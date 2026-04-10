use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use crate::foundation::{
    ExecutionLane, HostOsControlApprovalDecision, HostOsControlApprovalStatus, RequestPlane,
    TaskContextMode, TaskScheduleType, TaskStatus,
};

use super::dev_environment::DigitalOceanDevEnvironment;
use super::executor::{
    run_worker_daemon, run_worker_from_paths, run_worker_stdio, ExecutionLaneRouter,
};
use super::github_webhook::{handle_github_webhook, GithubWebhookPayload};
use super::host_os_control::{
    approval_notification_text, build_default_context, replay_approved_host_os_control_request,
    resolution_notification_text, resolve_host_os_control_request, run_host_os_control_task,
    HostOsControlAction, RunHostOsControlTaskInput, RunHostOsControlTaskResult,
};
use super::linear::{
    run_linear_issue_comment_upsert_task, run_linear_issue_quality_task,
    run_linear_issue_transition_task, run_linear_pm_memory_task, run_linear_teams_task,
    LinearIssueCommentUpsertTaskInput, LinearIssueQualityTaskInput, LinearIssueTransitionTaskInput,
    LinearPmMemoryTaskInput, LinearTeamsTaskInput,
};
use super::local_channel::LocalInboundEnvelope;
use super::observability::{
    ingest_observability_event, ObservabilityEventStatus, ObservabilitySeverity,
};
use super::openclaw_gateway::{describe_openclaw_gateway_readiness, start_openclaw_gateway_server};
use super::pm_automation::start_pm_automation_loop;
use super::remote_control::{build_remote_control_context, describe_remote_control};
use super::runtime::LocalRuntime;
use super::scheduler::TaskScheduleInput;
use super::service_slack::{ensure_registered_group, send_recorded_slack_message};
use super::slack::SlackChannel;
use super::slack_runtime::SlackRuntime;
use super::swarm::{
    cancel_swarm_objective_run, create_swarm_objective_run, get_swarm_run_details,
    list_swarm_run_details, pump_swarm_once, CreateSwarmObjectiveRunInput,
};
use super::webhook_server::start_webhook_server;
use super::{NanoclawApp, NanoclawConfig};

fn print_usage() {
    eprintln!(
        "usage: cargo run -- [bootstrap|show-config|gateway <show-config>|provenance <list|show>|approval <list|show|resolve>|host-os <run|replay>|swarm <create|list|show|cancel|pump>|observability <ingest|list|show>|remote-control <status|run|replay>|task <list|due|add|pause|resume|delete|complete|run-due>|local <send|run|outbox>|slack <run|import-groups>|linear <teams|issue-quality|pm-memory|comment-upsert|transition>|github-webhook <event-type> <payload-file>|show-dev-env|prepare-dev-env|seed-cargo-cache|sync-dev-env|exec-dev-env <command...>]"
    );
}

fn parse_lane_override<I>(args: &mut I) -> Result<Option<ExecutionLane>>
where
    I: Iterator<Item = String>,
{
    let Some(flag) = args.next() else {
        return Ok(None);
    };
    if flag != "--lane" {
        anyhow::bail!("unexpected argument '{}'; expected '--lane <lane>'", flag);
    }
    let Some(value) = args.next() else {
        anyhow::bail!("missing lane value after --lane");
    };
    Ok(Some(ExecutionLane::parse(&value)))
}

fn parse_request_plane(value: Option<String>) -> RequestPlane {
    value
        .map(|value| RequestPlane::parse(&value))
        .unwrap_or(RequestPlane::None)
}

fn parse_host_os_action(action_kind: &str, args: &[String]) -> Result<HostOsControlAction> {
    match action_kind {
        "open-application" => {
            let application = args.join(" ");
            if application.trim().is_empty() {
                anyhow::bail!("missing application name for open-application");
            }
            Ok(HostOsControlAction::OpenApplication { application })
        }
        "activate-application" => {
            let application = args.join(" ");
            if application.trim().is_empty() {
                anyhow::bail!("missing application name for activate-application");
            }
            Ok(HostOsControlAction::ActivateApplication { application })
        }
        "open-url" => {
            let url = args.join(" ");
            if url.trim().is_empty() {
                anyhow::bail!("missing URL for open-url");
            }
            Ok(HostOsControlAction::OpenUrl { url })
        }
        "reveal-in-finder" => {
            let path = args.join(" ");
            if path.trim().is_empty() {
                anyhow::bail!("missing path for reveal-in-finder");
            }
            Ok(HostOsControlAction::RevealInFinder { path })
        }
        "shell" => {
            let mut cwd = None;
            let mut command_parts = Vec::new();
            let mut remaining = args.iter().peekable();
            while let Some(arg) = remaining.next() {
                match arg.as_str() {
                    "--cwd" if command_parts.is_empty() => {
                        let Some(value) = remaining.next() else {
                            anyhow::bail!("missing path after --cwd");
                        };
                        cwd = Some(value.clone());
                    }
                    "--" if command_parts.is_empty() => {
                        command_parts.extend(remaining.cloned());
                        break;
                    }
                    _ => {
                        command_parts.push(arg.clone());
                        command_parts.extend(remaining.cloned());
                        break;
                    }
                }
            }
            let command = command_parts.join(" ");
            if command.trim().is_empty() {
                anyhow::bail!("missing shell command");
            }
            Ok(HostOsControlAction::ShellCommand { command, cwd })
        }
        other => anyhow::bail!("unsupported host-os action '{}'", other),
    }
}

fn build_control_capability_manifest(
    action: &HostOsControlAction,
    request_plane: &RequestPlane,
    allow_ssh: bool,
) -> crate::foundation::CapabilityManifest {
    match action {
        HostOsControlAction::ShellCommand { .. } => {
            super::security_profile::derive_capability_manifest(
                request_plane,
                super::security_profile::DeriveCapabilityManifestInput {
                    allow_ssh,
                    allow_host_command: true,
                    allow_os_control: true,
                    ..Default::default()
                },
            )
        }
        _ => super::security_profile::derive_capability_manifest(
            request_plane,
            super::security_profile::DeriveCapabilityManifestInput {
                allow_ssh,
                allow_os_control: true,
                ..Default::default()
            },
        ),
    }
}

fn maybe_notify_host_os_approval(
    app: &mut NanoclawApp,
    result: &RunHostOsControlTaskResult,
) -> Result<()> {
    if !result.approval_required {
        return Ok(());
    }
    let Some(request_id) = result.approval_request_id.as_deref() else {
        return Ok(());
    };
    let Some(record) = app.db.get_host_os_control_approval_request(request_id)? else {
        return Ok(());
    };
    let notify_chat_jid = app
        .config
        .host_os_approval_chat_jid
        .clone()
        .or(record.chat_jid.clone());
    if let Err(error) = notify_host_os_record(
        app,
        notify_chat_jid.as_deref(),
        &approval_notification_text(&record),
    ) {
        eprintln!("host-os approval notify failed: {error:#}");
    }
    Ok(())
}

fn print_host_os_result(result: &RunHostOsControlTaskResult) {
    println!("ok: {}", result.ok);
    println!("executed: {}", result.executed);
    println!("action_summary: {}", result.action_summary);
    println!("action_scope: {}", result.action_scope);
    println!("approval_required: {}", result.approval_required);
    println!(
        "approval_request_id: {}",
        result.approval_request_id.as_deref().unwrap_or("-")
    );
    println!(
        "provenance_id: {}",
        result.provenance_id.as_deref().unwrap_or("-")
    );
    if let Some(output) = result.output.as_deref() {
        println!("output:\n{}", output);
    }
    if let Some(error) = result.error.as_deref() {
        println!("error: {}", error);
    }
}

fn notify_host_os_record(app: &mut NanoclawApp, chat_jid: Option<&str>, body: &str) -> Result<()> {
    let Some(chat_jid) = chat_jid.filter(|value| value.starts_with("slack:")) else {
        return Ok(());
    };
    let mut channel = SlackChannel::from_config(&app.config, false)?;
    let group = ensure_registered_group(app, chat_jid, Some("Host OS Approvals"))?;
    let assistant_name = app.config.assistant_name.clone();
    let _ = send_recorded_slack_message(
        app,
        &mut channel,
        &group.jid,
        Some(&group.name),
        body,
        &assistant_name,
        Some(&assistant_name),
        true,
        true,
    )?;
    Ok(())
}

pub fn run_cli(args: impl IntoIterator<Item = String>) -> Result<()> {
    let mut args = args.into_iter();
    let command = args.next().unwrap_or_else(|| "bootstrap".to_string());
    if command == "exec-worker" {
        let Some(request_path) = args.next() else {
            print_usage();
            std::process::exit(2);
        };
        let Some(response_path) = args.next() else {
            print_usage();
            std::process::exit(2);
        };
        return run_worker_from_paths(Path::new(&request_path), Path::new(&response_path));
    }
    if command == "exec-worker-daemon" {
        let Some(session_root) = args.next() else {
            print_usage();
            std::process::exit(2);
        };
        return run_worker_daemon(Path::new(&session_root));
    }
    if command == "exec-worker-stdio" {
        return run_worker_stdio();
    }

    let config = NanoclawConfig::from_env();
    let dev_environment = DigitalOceanDevEnvironment::from_config(&config);

    match command.as_str() {
        "bootstrap" => {
            let (_app, summary) = NanoclawApp::bootstrap(config)?;
            println!("nanoclaw-rs bootstrap complete");
            println!("db: {}", summary.db_path);
            println!("groups: {}", summary.groups_dir);
            println!("trigger: {}", summary.default_trigger);
            println!("timezone: {}", summary.timezone);
            println!(
                "counts: chats={} messages={} scheduled_tasks={} registered_groups={}",
                summary.counts.chats,
                summary.counts.messages,
                summary.counts.scheduled_tasks,
                summary.counts.registered_groups
            );
            println!(
                "development_environment: {}",
                summary.development_environment
            );
        }
        "show-config" => {
            println!("project_root: {}", config.project_root.display());
            println!("data_dir: {}", config.data_dir.display());
            println!("groups_dir: {}", config.groups_dir.display());
            println!("store_dir: {}", config.store_dir.display());
            println!("db_path: {}", config.db_path.display());
            println!("assistant_name: {}", config.assistant_name);
            println!("default_trigger: {}", config.default_trigger);
            println!("timezone: {}", config.timezone);
            println!("max_concurrent_groups: {}", config.max_concurrent_groups);
            println!("execution_lane: {}", config.execution_lane.as_str());
            println!("container_image: {}", config.container_image);
            println!(
                "container_runtime: {}",
                config.container_runtime.as_deref().unwrap_or("auto")
            );
            println!(
                "container_groups: {}",
                if config.container_groups.is_empty() {
                    "-".to_string()
                } else {
                    config.container_groups.join(",")
                }
            );
            println!("droplet_ssh_host: {}", config.droplet_ssh_host);
            println!("droplet_ssh_user: {}", config.droplet_ssh_user);
            println!("droplet_ssh_port: {}", config.droplet_ssh_port);
            println!("droplet_repo_root: {}", config.droplet_repo_root);
            println!("remote_worker_mode: {}", config.remote_worker_mode.as_str());
            println!("remote_worker_root: {}", config.remote_worker_root);
            println!("remote_worker_binary: {}", config.remote_worker_binary);
            println!(
                "openclaw_gateway_bind_host: {}",
                config.openclaw_gateway_bind_host
            );
            println!(
                "openclaw_gateway_public_host: {}",
                config.openclaw_gateway_public_host
            );
            println!("openclaw_gateway_port: {}", config.openclaw_gateway_port);
            println!(
                "openclaw_gateway_ws_url: {}",
                config
                    .openclaw_gateway_public_ws_url()
                    .unwrap_or_else(|| "-".to_string())
            );
            println!(
                "openclaw_gateway_health_url: {}",
                config
                    .openclaw_gateway_public_health_url()
                    .unwrap_or_else(|| "-".to_string())
            );
            println!(
                "openclaw_gateway_execution_lane: {}",
                config.openclaw_gateway_execution_lane.as_str()
            );
            println!(
                "openclaw_gateway_token_configured: {}",
                !config.openclaw_gateway_token.trim().is_empty()
            );
            println!(
                "slack_env_file: {}",
                config
                    .slack_env_file
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "-".to_string())
            );
            println!("slack_poll_interval_ms: {}", config.slack_poll_interval_ms);
            println!("linear_webhook_port: {}", config.linear_webhook_port);
            println!(
                "observability_chat_jid: {}",
                if config.observability_chat_jid.is_empty() {
                    "-".to_string()
                } else {
                    config.observability_chat_jid.clone()
                }
            );
            println!(
                "observability_group_folder: {}",
                config.observability_group_folder
            );
            println!(
                "observability_webhook_token_configured: {}",
                !config.observability_webhook_token.trim().is_empty()
            );
            println!(
                "observability_auto_blue_team: {}",
                config.observability_auto_blue_team
            );
            println!(
                "observability_adapters_path: {}",
                config.observability_adapters_path.display()
            );
            println!(
                "linear_chat_jid: {}",
                if config.linear_chat_jid.is_empty() {
                    "-".to_string()
                } else {
                    config.linear_chat_jid.clone()
                }
            );
            println!(
                "linear_pm_team_keys: {}",
                if config.linear_pm_team_keys.is_empty() {
                    "-".to_string()
                } else {
                    config.linear_pm_team_keys.join(",")
                }
            );
            println!(
                "host_os_control_policy_path: {}",
                config.host_os_control_policy_path.display()
            );
            println!(
                "host_os_approval_chat_jid: {}",
                config.host_os_approval_chat_jid.as_deref().unwrap_or("-")
            );
            println!(
                "remote_control_ssh_host: {}",
                config.remote_control_ssh_host.as_deref().unwrap_or("-")
            );
            println!(
                "remote_control_ssh_user: {}",
                config.remote_control_ssh_user.as_deref().unwrap_or("-")
            );
            println!(
                "remote_control_ssh_port: {}",
                config.remote_control_ssh_port
            );
            println!(
                "remote_control_workspace_root: {}",
                config
                    .remote_control_workspace_root
                    .as_deref()
                    .unwrap_or("-")
            );
        }
        "gateway" => {
            let subcommand = args.next().unwrap_or_else(|| "show-config".to_string());
            match subcommand.as_str() {
                "show-config" => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&describe_openclaw_gateway_readiness(
                            &config
                        ))?
                    );
                }
                other => anyhow::bail!("unsupported gateway command '{}'", other),
            }
        }
        "provenance" => {
            let Some(provenance_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let app = NanoclawApp::open(config)?;
            match provenance_command.as_str() {
                "list" => {
                    let group_folder = args.next();
                    let limit = args
                        .next()
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(25);
                    for record in app
                        .db
                        .list_execution_provenance(group_folder.as_deref(), limit)?
                    {
                        println!(
                            "{}\t{}\t{}\t{}\t{}",
                            record.id,
                            record.group_folder,
                            record.run_kind.as_str(),
                            record.execution_location.as_str(),
                            record.status.as_str()
                        );
                    }
                }
                "show" => {
                    let Some(id) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let record = app.db.get_execution_provenance(&id)?;
                    if let Some(record) = record {
                        println!("{}", serde_json::to_string_pretty(&record)?);
                    } else {
                        println!("provenance_not_found: {}", id);
                    }
                }
                _ => {
                    print_usage();
                    std::process::exit(2);
                }
            }
        }
        "approval" => {
            let Some(approval_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let mut app = NanoclawApp::open(config)?;
            match approval_command.as_str() {
                "list" => {
                    let status = args
                        .next()
                        .map(|value| HostOsControlApprovalStatus::parse(&value));
                    let source_group = args.next();
                    let limit = args
                        .next()
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(25);
                    for record in app.db.list_host_os_control_approval_requests(
                        status,
                        source_group.as_deref(),
                        limit,
                    )? {
                        println!(
                            "{}\t{}\t{}\t{}",
                            record.id,
                            record.source_group,
                            record.action_kind,
                            record.status.as_str()
                        );
                    }
                }
                "show" => {
                    let Some(id) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let record = app.db.get_host_os_control_approval_request(&id)?;
                    if let Some(record) = record {
                        println!("{}", serde_json::to_string_pretty(&record)?);
                    } else {
                        println!("approval_not_found: {}", id);
                    }
                }
                "resolve" => {
                    let Some(id) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let Some(decision) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let record = resolve_host_os_control_request(
                        &app.db,
                        &app.config.host_os_control_policy_path,
                        &id,
                        HostOsControlApprovalDecision::parse(&decision),
                    )?;
                    let notify_chat_jid = app
                        .config
                        .host_os_approval_chat_jid
                        .clone()
                        .or(record.chat_jid.clone());
                    if let Err(error) = notify_host_os_record(
                        &mut app,
                        notify_chat_jid.as_deref(),
                        &resolution_notification_text(&record),
                    ) {
                        eprintln!("host-os approval notify failed: {error:#}");
                    }
                    println!("{}", serde_json::to_string_pretty(&record)?);
                }
                _ => {
                    print_usage();
                    std::process::exit(2);
                }
            }
        }
        "host-os" => {
            let Some(host_os_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let mut app = NanoclawApp::open(config)?;
            match host_os_command.as_str() {
                "run" => {
                    let Some(source_group) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let Some(action_kind) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let mut request_plane = RequestPlane::None;
                    let mut action_args = Vec::new();
                    let mut remaining = args.peekable();
                    while let Some(arg) = remaining.next() {
                        if arg == "--request-plane" {
                            request_plane = parse_request_plane(remaining.next());
                        } else {
                            action_args.push(arg);
                            action_args.extend(remaining);
                            break;
                        }
                    }
                    let action = parse_host_os_action(&action_kind, &action_args)?;
                    let mut context = build_default_context(
                        &source_group,
                        &app.config.host_os_control_policy_path,
                    );
                    context.request_plane = request_plane;
                    context.chat_jid = app.config.host_os_approval_chat_jid.clone();
                    context.capability_manifest =
                        build_control_capability_manifest(&action, &context.request_plane, false);
                    let result = run_host_os_control_task(
                        &app.db,
                        RunHostOsControlTaskInput { action },
                        &context,
                    )?;
                    maybe_notify_host_os_approval(&mut app, &result)?;
                    print_host_os_result(&result);
                }
                "replay" => {
                    let Some(request_id) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let mut context =
                        build_default_context("main", &app.config.host_os_control_policy_path);
                    if let Some(record) =
                        app.db.get_host_os_control_approval_request(&request_id)?
                    {
                        context.source_group = record.source_group.clone();
                        context.chat_jid = record.chat_jid.clone();
                    }
                    let result =
                        replay_approved_host_os_control_request(&app.db, &request_id, &context)?;
                    print_host_os_result(&result);
                }
                _ => {
                    print_usage();
                    std::process::exit(2);
                }
            }
        }
        "swarm" => {
            let Some(swarm_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let mut app = NanoclawApp::open(config)?;
            match swarm_command.as_str() {
                "create" => {
                    let Some(group_folder) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let mut created_by = "cli".to_string();
                    let mut requested_lane = None;
                    let mut max_concurrency = None;
                    let mut objective_parts = Vec::new();
                    let mut remaining = args.peekable();
                    while let Some(arg) = remaining.next() {
                        match arg.as_str() {
                            "--created-by" => {
                                let Some(value) = remaining.next() else {
                                    print_usage();
                                    std::process::exit(2);
                                };
                                created_by = value;
                            }
                            "--lane" => {
                                let Some(value) = remaining.next() else {
                                    print_usage();
                                    std::process::exit(2);
                                };
                                requested_lane =
                                    Some(crate::foundation::SwarmRequestedLane::parse(&value));
                            }
                            "--max-concurrency" => {
                                let Some(value) = remaining.next() else {
                                    print_usage();
                                    std::process::exit(2);
                                };
                                max_concurrency = value.parse::<i64>().ok();
                            }
                            other => {
                                objective_parts.push(other.to_string());
                                objective_parts.extend(remaining);
                                break;
                            }
                        }
                    }
                    let objective = objective_parts.join(" ");
                    if objective.trim().is_empty() {
                        print_usage();
                        std::process::exit(2);
                    }
                    let created = create_swarm_objective_run(
                        &mut app,
                        CreateSwarmObjectiveRunInput {
                            objective,
                            group_folder: group_folder.clone(),
                            chat_jid: group_folder,
                            created_by,
                            requested_lane,
                            tasks: Vec::new(),
                            max_concurrency,
                        },
                    )?;
                    println!("run_id: {}", created.run.id);
                    println!("status: {}", created.run.status.as_str());
                    println!("tasks: {}", created.tasks.len());
                }
                "list" => {
                    let limit = args
                        .next()
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(25);
                    for details in list_swarm_run_details(&app.db, limit)? {
                        println!(
                            "{}\t{}\t{}\t{}",
                            details.run.id,
                            details.run.group_folder,
                            details.run.status.as_str(),
                            details.run.summary.as_deref().unwrap_or("-")
                        );
                    }
                }
                "show" => {
                    let Some(run_id) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    if let Some(details) = get_swarm_run_details(&app.db, &run_id)? {
                        println!("{}", serde_json::to_string_pretty(&details.run)?);
                        println!("tasks:");
                        for task in details.tasks {
                            println!(
                                "{}\t{}\t{}\t{}",
                                task.id,
                                task.task_key,
                                task.status.as_str(),
                                task.resolved_lane
                                    .as_ref()
                                    .map(crate::foundation::SwarmResolvedLane::as_str)
                                    .unwrap_or_else(|| task.requested_lane.as_str())
                            );
                        }
                    } else {
                        println!("swarm_not_found: {}", run_id);
                    }
                }
                "cancel" => {
                    let Some(run_id) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    cancel_swarm_objective_run(&app, &run_id)?;
                    println!("canceled: {}", run_id);
                }
                "pump" => {
                    let lane_override = parse_lane_override(&mut args)?;
                    let executor = ExecutionLaneRouter::from_config(&app.config, lane_override)?;
                    let summary = pump_swarm_once(&mut app, &executor)?;
                    println!(
                        "expired_leases_released: {}",
                        summary.expired_leases_released
                    );
                    println!("runs_considered: {}", summary.runs_considered);
                    println!("tasks_claimed: {}", summary.tasks_claimed);
                    println!("tasks_completed: {}", summary.tasks_completed);
                    println!("tasks_failed: {}", summary.tasks_failed);
                }
                _ => {
                    print_usage();
                    std::process::exit(2);
                }
            }
        }
        "observability" => {
            let Some(observability_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let mut app = NanoclawApp::open(config)?;
            match observability_command.as_str() {
                "ingest" => {
                    let Some(payload_path) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let payload = serde_json::from_str(
                        &fs::read_to_string(&payload_path)
                            .with_context(|| format!("failed to read {}", payload_path))?,
                    )
                    .with_context(|| {
                        format!(
                            "failed to parse observability payload from {}",
                            payload_path
                        )
                    })?;
                    let result = ingest_observability_event(&mut app, None, payload)?;
                    println!("event_id: {}", result.event.id);
                    println!("fingerprint: {}", result.event.fingerprint);
                    println!("created: {}", result.created);
                    println!("target_jid: {}", result.target_jid);
                    println!(
                        "blue_team_run_id: {}",
                        result.blue_team_run_id.as_deref().unwrap_or("-")
                    );
                }
                "list" => {
                    let mut limit = 25usize;
                    let mut status = None::<ObservabilityEventStatus>;
                    let mut severity = None::<ObservabilitySeverity>;
                    while let Some(arg) = args.next() {
                        match arg.as_str() {
                            "--status" => {
                                let Some(value) = args.next() else {
                                    print_usage();
                                    std::process::exit(2);
                                };
                                status = Some(ObservabilityEventStatus::parse(&value));
                            }
                            "--severity" => {
                                let Some(value) = args.next() else {
                                    print_usage();
                                    std::process::exit(2);
                                };
                                severity = Some(ObservabilitySeverity::parse(&value));
                            }
                            other => {
                                limit = other.parse::<usize>().with_context(|| {
                                    format!("invalid observability list limit '{}'", other)
                                })?;
                            }
                        }
                    }
                    for event in app.db.list_observability_events(
                        limit,
                        status.as_ref(),
                        severity.as_ref(),
                    )? {
                        println!(
                            "{}\t{}\t{}\t{}\t{}\t{}",
                            event.id,
                            event.severity.as_str(),
                            event.status.as_str(),
                            event.source,
                            event.title,
                            event.last_seen_at
                        );
                    }
                }
                "show" => {
                    let Some(identifier) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let event = app
                        .db
                        .get_observability_event_by_id(&identifier)?
                        .or(app.db.get_observability_event_by_fingerprint(&identifier)?);
                    if let Some(event) = event {
                        println!("{}", serde_json::to_string_pretty(&event)?);
                    } else {
                        println!("observability_not_found: {}", identifier);
                    }
                }
                _ => {
                    print_usage();
                    std::process::exit(2);
                }
            }
        }
        "remote-control" => {
            let Some(remote_control_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            match remote_control_command.as_str() {
                "status" => {
                    let status = describe_remote_control(&config);
                    println!("configured: {}", status.configured);
                    println!("reachable: {}", status.reachable);
                    println!("target: {}", status.target.as_deref().unwrap_or("-"));
                    println!(
                        "workspace_root: {}",
                        status.workspace_root.as_deref().unwrap_or("-")
                    );
                    if let Some(error) = status.error.as_deref() {
                        println!("error: {}", error);
                    }
                }
                "run" => {
                    let Some(source_group) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let Some(action_kind) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let mut request_plane = RequestPlane::None;
                    let mut action_args = Vec::new();
                    let mut remaining = args.peekable();
                    while let Some(arg) = remaining.next() {
                        if arg == "--request-plane" {
                            request_plane = parse_request_plane(remaining.next());
                        } else {
                            action_args.push(arg);
                            action_args.extend(remaining);
                            break;
                        }
                    }
                    let action = parse_host_os_action(&action_kind, &action_args)?;
                    let mut app = NanoclawApp::open(config)?;
                    let mut context = build_remote_control_context(&source_group, &app.config)?;
                    context.request_plane = request_plane;
                    context.capability_manifest =
                        build_control_capability_manifest(&action, &context.request_plane, true);
                    let result = run_host_os_control_task(
                        &app.db,
                        RunHostOsControlTaskInput { action },
                        &context,
                    )?;
                    maybe_notify_host_os_approval(&mut app, &result)?;
                    print_host_os_result(&result);
                }
                "replay" => {
                    let Some(request_id) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let app = NanoclawApp::open(config)?;
                    let mut context = build_remote_control_context("main", &app.config)?;
                    if let Some(record) =
                        app.db.get_host_os_control_approval_request(&request_id)?
                    {
                        context.source_group = record.source_group.clone();
                        context.chat_jid = record.chat_jid.clone().or(context.chat_jid.clone());
                    }
                    context.capability_manifest =
                        super::security_profile::derive_capability_manifest(
                            &context.request_plane,
                            super::security_profile::DeriveCapabilityManifestInput {
                                allow_ssh: true,
                                allow_host_command: true,
                                allow_os_control: true,
                                ..Default::default()
                            },
                        );
                    let result =
                        replay_approved_host_os_control_request(&app.db, &request_id, &context)?;
                    print_host_os_result(&result);
                }
                _ => {
                    print_usage();
                    std::process::exit(2);
                }
            }
        }
        "task" => {
            let Some(task_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let mut app = NanoclawApp::open(config)?;
            match task_command.as_str() {
                "list" => {
                    for task in app.list_tasks()? {
                        println!(
                            "{}\t{}\t{}\t{}\t{}",
                            task.id,
                            task.group_folder,
                            task.status.as_str(),
                            task.schedule_type.as_str(),
                            task.next_run.as_deref().unwrap_or("-")
                        );
                    }
                }
                "due" => {
                    for task in app.due_tasks()? {
                        println!(
                            "{}\t{}\t{}",
                            task.id,
                            task.group_folder,
                            task.next_run.as_deref().unwrap_or("-")
                        );
                    }
                }
                "add" => {
                    let Some(group_folder) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let Some(schedule_type_raw) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let Some(schedule_value) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let mut script = None;
                    let mut prompt_parts = Vec::new();
                    let mut remaining = args.peekable();
                    while let Some(arg) = remaining.next() {
                        if arg == "--script" {
                            let Some(value) = remaining.next() else {
                                print_usage();
                                std::process::exit(2);
                            };
                            script = Some(value);
                        } else {
                            prompt_parts.push(arg);
                            prompt_parts.extend(remaining);
                            break;
                        }
                    }
                    let prompt = if prompt_parts.is_empty() {
                        if script.is_some() {
                            format!("Run scheduled script for group {}", group_folder)
                        } else {
                            String::new()
                        }
                    } else {
                        prompt_parts.join(" ")
                    };
                    if prompt.trim().is_empty() && script.is_none() {
                        print_usage();
                        std::process::exit(2);
                    }

                    let task = app.schedule_task(TaskScheduleInput {
                        group_folder: group_folder.clone(),
                        chat_jid: group_folder,
                        prompt,
                        script,
                        request_plane: None,
                        schedule_type: TaskScheduleType::parse(&schedule_type_raw),
                        schedule_value,
                        context_mode: TaskContextMode::Isolated,
                    })?;
                    println!("task_id: {}", task.id);
                    println!("status: {}", task.status.as_str());
                    println!("next_run: {}", task.next_run.as_deref().unwrap_or("-"));
                }
                "pause" => {
                    let Some(task_id) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    app.set_task_status(&task_id, TaskStatus::Paused)?;
                    println!("paused: {}", task_id);
                }
                "resume" => {
                    let Some(task_id) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    app.set_task_status(&task_id, TaskStatus::Active)?;
                    println!("resumed: {}", task_id);
                }
                "delete" => {
                    let Some(task_id) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    app.delete_task(&task_id)?;
                    println!("deleted: {}", task_id);
                }
                "complete" => {
                    let Some(task_id) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let result = args.collect::<Vec<_>>().join(" ");
                    let updated = app.complete_task_run(
                        &task_id,
                        0,
                        if result.trim().is_empty() {
                            None
                        } else {
                            Some(result)
                        },
                        None,
                    )?;
                    if let Some(task) = updated {
                        println!("completed: {}", task.id);
                        println!("status: {}", task.status.as_str());
                        println!("next_run: {}", task.next_run.as_deref().unwrap_or("-"));
                    } else {
                        println!("task_not_found: {}", task_id);
                    }
                }
                "run-due" => {
                    let lane_override = parse_lane_override(&mut args)?;
                    let executor = ExecutionLaneRouter::from_config(&app.config, lane_override)?;
                    let mut runtime = LocalRuntime::new(app, executor)?;
                    let summary = runtime.poll_once()?;
                    println!("due_tasks_run: {}", summary.scheduled_tasks_run);
                    println!("due_task_errors: {}", summary.scheduled_task_errors);
                    println!("swarm_tasks_run: {}", summary.swarm_tasks_run);
                    println!("swarm_task_errors: {}", summary.swarm_task_errors);
                    println!("outbound_messages: {}", summary.outbound_messages);
                }
                _ => {
                    print_usage();
                    std::process::exit(2);
                }
            }
        }
        "local" => {
            let Some(local_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let app = NanoclawApp::open(config)?;
            match local_command.as_str() {
                "send" => {
                    let executor = ExecutionLaneRouter::from_config(&app.config, None)?;
                    let runtime = LocalRuntime::new(app, executor)?;
                    let Some(chat_jid) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let Some(sender) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let text = args.collect::<Vec<_>>().join(" ");
                    if text.trim().is_empty() {
                        print_usage();
                        std::process::exit(2);
                    }

                    let path = runtime.enqueue_local_message(LocalInboundEnvelope {
                        id: None,
                        chat_jid,
                        sender: sender.clone(),
                        sender_name: Some(sender),
                        content: text,
                        timestamp: None,
                    })?;
                    println!("enqueued: {}", path.display());
                }
                "run" => {
                    let lane_override = parse_lane_override(&mut args)?;
                    let executor = ExecutionLaneRouter::from_config(&app.config, lane_override)?;
                    let mut runtime = LocalRuntime::new(app, executor)?;
                    let summary = runtime.poll_once()?;
                    println!("inbound_messages: {}", summary.inbound_messages);
                    println!("processed_groups: {}", summary.processed_groups);
                    println!("scheduled_tasks_run: {}", summary.scheduled_tasks_run);
                    println!("scheduled_task_errors: {}", summary.scheduled_task_errors);
                    println!("swarm_tasks_run: {}", summary.swarm_tasks_run);
                    println!("swarm_task_errors: {}", summary.swarm_task_errors);
                    println!("outbound_messages: {}", summary.outbound_messages);
                }
                "outbox" => {
                    let executor = ExecutionLaneRouter::from_config(&app.config, None)?;
                    let runtime = LocalRuntime::new(app, executor)?;
                    for envelope in runtime.read_outbox()? {
                        println!(
                            "{}\t{}\t{}",
                            envelope.timestamp,
                            envelope.chat_jid,
                            envelope.text.replace('\n', "\\n")
                        );
                    }
                }
                _ => {
                    print_usage();
                    std::process::exit(2);
                }
            }
        }
        "slack" => {
            let Some(slack_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let mut app = NanoclawApp::open(config)?;
            match slack_command.as_str() {
                "import-groups" => {
                    let Some(source_db_path) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let mut source_groups_dir = None::<String>;
                    while let Some(arg) = args.next() {
                        if arg == "--groups-dir" {
                            let Some(value) = args.next() else {
                                print_usage();
                                std::process::exit(2);
                            };
                            source_groups_dir = Some(value);
                        } else {
                            anyhow::bail!("unexpected argument '{}' for slack import-groups", arg);
                        }
                    }
                    let imported = app.import_registered_groups(
                        Path::new(&source_db_path),
                        source_groups_dir.as_deref().map(Path::new),
                    )?;
                    println!("imported_groups: {}", imported);
                }
                "run" => {
                    let mut lane_override = None::<ExecutionLane>;
                    let mut read_only = false;
                    while let Some(arg) = args.next() {
                        match arg.as_str() {
                            "--lane" => {
                                let Some(value) = args.next() else {
                                    print_usage();
                                    std::process::exit(2);
                                };
                                lane_override = Some(ExecutionLane::parse(&value));
                            }
                            "--read-only" => read_only = true,
                            _ => anyhow::bail!("unexpected slack run argument '{}'", arg),
                        }
                    }
                    if !read_only {
                        start_webhook_server(app.config.clone())?;
                        start_pm_automation_loop(app.config.clone())?;
                        start_openclaw_gateway_server(app.config.clone())?;
                    }
                    let executor = ExecutionLaneRouter::from_config(&app.config, lane_override)?;
                    let channel = SlackChannel::from_config(&app.config, read_only)?;
                    let mut runtime = SlackRuntime::new(app, channel, executor);
                    runtime.run_forever()?;
                }
                _ => {
                    print_usage();
                    std::process::exit(2);
                }
            }
        }
        "linear" => {
            let Some(linear_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let app = NanoclawApp::open(config)?;
            match linear_command.as_str() {
                "teams" => {
                    let limit = args
                        .next()
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(25);
                    let result = run_linear_teams_task(&app.config, LinearTeamsTaskInput { limit });
                    if !result.ok {
                        anyhow::bail!(
                            "{}",
                            result
                                .error
                                .unwrap_or_else(|| "Linear teams task failed".to_string())
                        );
                    }
                    for team in result.teams.unwrap_or_default() {
                        println!("{}\t{}\t{}", team.id, team.key, team.name);
                    }
                }
                "issue-quality" => {
                    let Some(identifier) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let apply = args.any(|arg| arg == "--apply");
                    let result = run_linear_issue_quality_task(
                        &app.db,
                        &app.config,
                        LinearIssueQualityTaskInput { identifier, apply },
                    );
                    if !result.ok {
                        anyhow::bail!(
                            "{}",
                            result.error.unwrap_or_else(|| {
                                "Linear issue quality task failed".to_string()
                            })
                        );
                    }
                    println!("identifier: {}", result.identifier);
                    println!("score: {}", result.score.unwrap_or_default());
                    println!("gaps: {}", result.gaps.as_ref().map(Vec::len).unwrap_or(0));
                    if let Some(body) = result.body {
                        println!("body:\n{}", body);
                    }
                }
                "pm-memory" => {
                    let Some(identifier) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let mut summary = None::<String>;
                    let mut next_action = None::<String>;
                    let mut current_state = None::<String>;
                    let mut repo_hint = None::<String>;
                    let mut last_source = None::<String>;
                    let mut blockers = Vec::new();
                    let mut details = None;
                    let mut merge = true;
                    while let Some(arg) = args.next() {
                        match arg.as_str() {
                            "--summary" => summary = args.next(),
                            "--next-action" => next_action = args.next(),
                            "--current-state" => current_state = args.next(),
                            "--repo-hint" => repo_hint = args.next(),
                            "--last-source" => last_source = args.next(),
                            "--blocker" => {
                                if let Some(value) = args.next() {
                                    blockers.push(value);
                                }
                            }
                            "--details-json" => {
                                let Some(value) = args.next() else {
                                    anyhow::bail!("missing value for --details-json");
                                };
                                details =
                                    Some(serde_json::from_str(&value).with_context(|| {
                                        "failed to parse --details-json as JSON"
                                    })?);
                            }
                            "--replace" => merge = false,
                            _ => anyhow::bail!("unexpected linear pm-memory argument '{}'", arg),
                        }
                    }
                    let result = run_linear_pm_memory_task(
                        &app.db,
                        &app.config,
                        LinearPmMemoryTaskInput {
                            identifier,
                            summary,
                            next_action,
                            blockers: (!blockers.is_empty()).then_some(blockers),
                            current_state,
                            repo_hint,
                            last_source,
                            details,
                            merge,
                        },
                    );
                    if !result.ok {
                        anyhow::bail!(
                            "{}",
                            result
                                .error
                                .unwrap_or_else(|| { "Linear PM memory task failed".to_string() })
                        );
                    }
                    println!("identifier: {}", result.identifier);
                    if let Some(issue_key) = result.issue_key {
                        println!("issue_key: {}", issue_key);
                    }
                    if let Some(memory) = result.memory {
                        println!("summary: {}", memory.summary.unwrap_or_default());
                        println!("next_action: {}", memory.next_action.unwrap_or_default());
                        println!(
                            "current_state: {}",
                            memory.current_state.unwrap_or_default()
                        );
                        println!("repo_hint: {}", memory.repo_hint.unwrap_or_default());
                    }
                }
                "comment-upsert" => {
                    let Some(identifier) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let Some(comment_kind) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let body = args.collect::<Vec<_>>().join(" ");
                    if body.trim().is_empty() {
                        print_usage();
                        std::process::exit(2);
                    }
                    let result = run_linear_issue_comment_upsert_task(
                        &app.db,
                        &app.config,
                        LinearIssueCommentUpsertTaskInput {
                            identifier,
                            body,
                            comment_kind,
                        },
                    );
                    if !result.ok {
                        anyhow::bail!(
                            "{}",
                            result
                                .error
                                .unwrap_or_else(|| { "Linear comment upsert failed".to_string() })
                        );
                    }
                    println!("identifier: {}", result.identifier);
                    println!("comment_id: {}", result.comment_id.unwrap_or_default());
                    println!(
                        "updated: {}",
                        result
                            .updated
                            .map(|value| value.to_string())
                            .unwrap_or_default()
                    );
                    println!(
                        "deduplicated: {}",
                        result
                            .deduplicated
                            .map(|value| value.to_string())
                            .unwrap_or_default()
                    );
                }
                "transition" => {
                    let Some(identifier) = args.next() else {
                        print_usage();
                        std::process::exit(2);
                    };
                    let mut state_name = None::<String>;
                    let mut state_id = None::<String>;
                    while let Some(arg) = args.next() {
                        match arg.as_str() {
                            "--state" => state_name = args.next(),
                            "--state-id" => state_id = args.next(),
                            _ => anyhow::bail!("unexpected linear transition argument '{}'", arg),
                        }
                    }
                    let result = run_linear_issue_transition_task(
                        &app.config,
                        LinearIssueTransitionTaskInput {
                            identifier,
                            state_name,
                            state_id,
                        },
                    );
                    if !result.ok {
                        anyhow::bail!(
                            "{}",
                            result.error.unwrap_or_else(|| {
                                "Linear issue transition failed".to_string()
                            })
                        );
                    }
                    println!("identifier: {}", result.identifier);
                    println!("changed: {}", result.changed.unwrap_or(false));
                    println!(
                        "previous_state: {}",
                        result
                            .previous_state
                            .map(|state| state.name)
                            .unwrap_or_default()
                    );
                    println!(
                        "next_state: {}",
                        result
                            .next_state
                            .map(|state| state.name)
                            .unwrap_or_default()
                    );
                }
                _ => {
                    print_usage();
                    std::process::exit(2);
                }
            }
        }
        "github-webhook" => {
            let Some(event_type) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let Some(payload_path) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let app = NanoclawApp::open(config)?;
            let payload = serde_json::from_str::<GithubWebhookPayload>(
                &fs::read_to_string(&payload_path)
                    .with_context(|| format!("failed to read {}", payload_path))?,
            )
            .with_context(|| format!("failed to parse GitHub payload from {}", payload_path))?;
            let result = handle_github_webhook(&app.db, &app.config, &payload, &event_type);
            println!("ignored: {}", result.ignored);
            if let Some(reason) = result.reason {
                println!("reason: {}", reason);
            }
            println!(
                "handled_identifiers: {}",
                if result.handled_identifiers.is_empty() {
                    "-".to_string()
                } else {
                    result.handled_identifiers.join(",")
                }
            );
            for notification in result.notifications {
                println!(
                    "notify\t{}\t{}\t{}",
                    notification.identifier,
                    notification.target_chat_jid.unwrap_or_default(),
                    notification.body.replace('\n', "\\n")
                );
            }
            for error in result.errors {
                println!("error: {}", error);
            }
        }
        "show-dev-env" => {
            let environment = dev_environment.environment();
            println!("name: {}", environment.name);
            println!("kind: {:?}", environment.kind);
            if let Some(ssh) = &environment.ssh {
                println!("ssh_host: {}", ssh.host);
                if let Some(user) = &ssh.user {
                    println!("ssh_user: {}", user);
                }
                println!("ssh_port: {}", ssh.port);
            }
            if let Some(repo_root) = &environment.repo_root {
                println!("repo_root: {}", repo_root);
            }
            if let Some(remote_root) = &environment.remote_worker_root {
                println!("remote_worker_root: {}", remote_root);
            }
            println!(
                "remote_worker_mode: {}",
                environment.remote_worker_mode.as_str()
            );
            println!("execution_lane: {}", config.execution_lane.as_str());
            println!(
                "container_groups: {}",
                if config.container_groups.is_empty() {
                    "-".to_string()
                } else {
                    config.container_groups.join(",")
                }
            );
            if let Some(timeout_ms) = environment.bootstrap_timeout_ms {
                println!("bootstrap_timeout_ms: {}", timeout_ms);
            }
            if let Some(sync_interval_ms) = environment.sync_interval_ms {
                println!("sync_interval_ms: {}", sync_interval_ms);
            }
            if let Some(remote_repo_path) = dev_environment.remote_repo_path().ok() {
                println!("remote_project_path: {}", remote_repo_path);
            }
        }
        "prepare-dev-env" => {
            let result = dev_environment.prepare_dev_environment()?;
            println!("remote_repo_path: {}", result.remote_repo_path);
            print!("{}", result.stdout);
        }
        "seed-cargo-cache" => {
            for result in dev_environment.seed_cargo_cache()? {
                println!("seeded_from: {}", result.local_source);
                println!("seeded_to: {}", result.remote_target);
            }
        }
        "sync-dev-env" => {
            let result = dev_environment.sync_project()?;
            println!("synced_from: {}", result.local_source);
            println!("synced_to: {}", result.remote_target);
            println!("command: {}", result.command);
        }
        "exec-dev-env" => {
            let remote_command = args.collect::<Vec<_>>().join(" ");
            if remote_command.trim().is_empty() {
                print_usage();
                std::process::exit(2);
            }
            let sync_result = dev_environment.sync_project()?;
            let exec_result = dev_environment.exec(&remote_command)?;
            println!("synced_from: {}", sync_result.local_source);
            println!("synced_to: {}", sync_result.remote_target);
            println!("remote_repo_path: {}", exec_result.remote_repo_path);
            print!("{}", exec_result.stdout);
        }
        _ => {
            print_usage();
            std::process::exit(2);
        }
    }

    Ok(())
}
