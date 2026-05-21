use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::foundation::{
    ExecutionLane, Group, HostOsControlApprovalDecision, HostOsControlApprovalStatus, RequestPlane,
    TaskContextMode, TaskScheduleType, TaskStatus,
};

use super::dev_environment::DigitalOceanDevEnvironment;
use super::executor::{
    build_execution_session, run_worker_daemon, run_worker_from_paths, run_worker_stdio,
    ExecutionLaneRouter,
};
use super::github_webhook::{handle_github_webhook, GithubWebhookPayload};
use super::group_runtime_config::GroupRuntimeConfig;
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
use super::local_channel::{LocalChannel, LocalInboundEnvelope};
use super::observability::{
    ingest_observability_event, ObservabilityEventStatus, ObservabilitySeverity,
};
use super::openclaw_gateway::{describe_openclaw_gateway_readiness, start_openclaw_gateway_server};
use super::pm_automation::start_pm_automation_loop;
use super::remote_control::{build_remote_control_context, describe_remote_control};
use super::runtime::LocalRuntime;
use super::runtime_channels::{
    runtime_channel_registry, RuntimeChannelDescriptor, RuntimeChannelRegistry,
    RuntimeChannelStatus,
};
use super::scheduler::TaskScheduleInput;
use super::service_slack::{ensure_registered_group, send_recorded_slack_message};
use super::session_storage::{
    ensure_session_sidecars, record_on_wake_message, session_sidecar_paths,
};
use super::slack::SlackChannel;
use super::slack_runtime::SlackRuntime;
use super::swarm::{
    cancel_swarm_objective_run, create_swarm_objective_run, get_swarm_run_details,
    list_swarm_run_details, pump_swarm_once, CreateSwarmObjectiveRunInput,
};
use super::tool_registry::{
    built_in_tool_adapter_contracts, load_external_tool_adapter_contracts,
    validate_built_in_tool_adapter_contracts,
};
use super::webhook_server::start_webhook_server;
use super::{NanoclawApp, NanoclawConfig};

fn print_usage() {
    eprintln!(
        "usage: cargo run -- [bootstrap|show-config|runtime <status|state|inspect|health|cleanup|poll|serve|stop|reload>|group-runtime <show|set>|session <show|wake>|gateway <show-config|serve>|provenance <list|show>|approval <list|show|resolve>|host-os <run|replay>|swarm <create|list|show|cancel|pump>|observability <ingest|list|show>|remote-control <status|run|replay>|task <list|due|add|pause|resume|delete|complete --manual-override|run-due>|local <send|run|outbox>|slack <run|import-groups>|linear <legacy>|github-webhook <event-type> <payload-file>|show-dev-env|prepare-dev-env|seed-cargo-cache|sync-dev-env|exec-dev-env <command...>]"
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeServeProfile {
    Full,
    Gateway,
    Webhook,
    Pm,
    Slack,
}

impl RuntimeServeProfile {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "full" => Ok(Self::Full),
            "gateway" => Ok(Self::Gateway),
            "webhook" => Ok(Self::Webhook),
            "pm" => Ok(Self::Pm),
            "slack" => Ok(Self::Slack),
            other => anyhow::bail!("unsupported runtime serve profile '{}'", other),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Gateway => "gateway",
            Self::Webhook => "webhook",
            Self::Pm => "pm",
            Self::Slack => "slack",
        }
    }

    fn all() -> [Self; 5] {
        [
            Self::Full,
            Self::Gateway,
            Self::Webhook,
            Self::Pm,
            Self::Slack,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeServeArgs {
    profile: RuntimeServeProfile,
    lane_override: Option<ExecutionLane>,
    read_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RuntimeCleanupArgs {
    apply: bool,
    include_state_residue: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeHealthArgs {
    limit: usize,
    strict: bool,
    notify_local: Option<String>,
    notify_always: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskCompleteArgs {
    task_id: String,
    result: Option<String>,
}

fn parse_task_complete_args<I>(args: &mut I) -> Result<TaskCompleteArgs>
where
    I: Iterator<Item = String>,
{
    let mut manual_override = false;
    let mut values = Vec::<String>::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--manual-override" => manual_override = true,
            other if other.starts_with("--") => {
                anyhow::bail!("unexpected task complete argument '{}'", other)
            }
            _ => values.push(arg),
        }
    }

    if !manual_override {
        anyhow::bail!(
            "task complete requires --manual-override; execution-driven completion must use structured execution evidence"
        );
    }
    let Some(task_id) = values.first().cloned() else {
        anyhow::bail!("task complete requires a task id");
    };
    let result = values
        .get(1..)
        .unwrap_or_default()
        .join(" ")
        .trim()
        .to_string();

    Ok(TaskCompleteArgs {
        task_id,
        result: (!result.is_empty()).then_some(result),
    })
}

fn parse_runtime_serve_args<I>(args: &mut I) -> Result<RuntimeServeArgs>
where
    I: Iterator<Item = String>,
{
    let mut profile = RuntimeServeProfile::Full;
    let mut lane_override = None::<ExecutionLane>;
    let mut read_only = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--profile" => {
                let Some(value) = args.next() else {
                    print_usage();
                    std::process::exit(2);
                };
                profile = RuntimeServeProfile::parse(&value)?;
            }
            "--lane" => {
                let Some(value) = args.next() else {
                    print_usage();
                    std::process::exit(2);
                };
                lane_override = Some(ExecutionLane::parse(&value));
            }
            "--read-only" => read_only = true,
            other => anyhow::bail!("unexpected runtime serve argument '{}'", other),
        }
    }

    Ok(RuntimeServeArgs {
        profile,
        lane_override,
        read_only,
    })
}

fn parse_runtime_control_args<I>(args: &mut I, label: &str) -> Result<RuntimeServeProfile>
where
    I: Iterator<Item = String>,
{
    let mut profile = RuntimeServeProfile::Full;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--profile" => {
                let Some(value) = args.next() else {
                    print_usage();
                    std::process::exit(2);
                };
                profile = RuntimeServeProfile::parse(&value)?;
            }
            other => anyhow::bail!("unexpected runtime {label} argument '{}'", other),
        }
    }

    Ok(profile)
}

fn parse_runtime_cleanup_args<I>(args: &mut I) -> Result<RuntimeCleanupArgs>
where
    I: Iterator<Item = String>,
{
    let mut apply = false;
    let mut include_state_residue = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--apply" => apply = true,
            "--dry-run" => apply = false,
            "--state-residue" => include_state_residue = true,
            other => anyhow::bail!("unexpected runtime cleanup argument '{}'", other),
        }
    }

    Ok(RuntimeCleanupArgs {
        apply,
        include_state_residue,
    })
}

fn parse_runtime_health_args<I>(args: &mut I) -> Result<RuntimeHealthArgs>
where
    I: Iterator<Item = String>,
{
    let mut limit = 10;
    let mut strict = false;
    let mut notify_local = None::<String>;
    let mut notify_always = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--limit" => {
                let Some(value) = args.next() else {
                    print_usage();
                    std::process::exit(2);
                };
                limit = value
                    .parse::<usize>()
                    .with_context(|| format!("invalid runtime health --limit value {value}"))?
                    .max(1);
            }
            "--strict" => strict = true,
            "--notify-local" => {
                let Some(value) = args.next() else {
                    print_usage();
                    std::process::exit(2);
                };
                let value = value.trim().to_string();
                if value.is_empty() {
                    anyhow::bail!("runtime health --notify-local requires a non-empty chat id");
                }
                notify_local = Some(value);
            }
            "--notify-always" => notify_always = true,
            other => anyhow::bail!("unexpected runtime health argument '{}'", other),
        }
    }

    Ok(RuntimeHealthArgs {
        limit,
        strict,
        notify_local,
        notify_always,
    })
}

fn parse_limit_args<I>(args: &mut I, default_limit: usize, label: &str) -> Result<usize>
where
    I: Iterator<Item = String>,
{
    let mut limit = default_limit;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--limit" => {
                let Some(value) = args.next() else {
                    print_usage();
                    std::process::exit(2);
                };
                limit = value
                    .parse::<usize>()
                    .with_context(|| format!("invalid {label} --limit value {value}"))?
                    .max(1);
            }
            other => anyhow::bail!("unexpected {label} argument '{}'", other),
        }
    }
    Ok(limit)
}

fn runtime_control_dir(config: &NanoclawConfig) -> PathBuf {
    config.data_dir.join("runtime")
}

fn runtime_pid_path(config: &NanoclawConfig, profile: RuntimeServeProfile) -> PathBuf {
    runtime_control_dir(config).join(format!("{}.pid", profile.as_str()))
}

fn runtime_startup_events_path(config: &NanoclawConfig) -> PathBuf {
    runtime_control_dir(config).join("startup-events.jsonl")
}

fn runtime_pid_status_json(config: &NanoclawConfig) -> serde_json::Value {
    let mut profiles = serde_json::Map::new();
    for profile in RuntimeServeProfile::all() {
        let profile_state = runtime_pid_profile_state_json(config, profile);
        profiles.insert(
            profile.as_str().to_string(),
            json!({
                "pidFile": profile_state["pidFile"],
                "pid": profile_state["pid"],
                "pidFileExists": profile_state["pidFileExists"],
            }),
        );
    }
    json!({
        "controlDir": runtime_control_dir(config).display().to_string(),
        "profiles": profiles,
    })
}

struct RuntimePidFileGuard {
    path: PathBuf,
}

impl Drop for RuntimePidFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn write_runtime_pid_file(
    config: &NanoclawConfig,
    profile: RuntimeServeProfile,
) -> Result<RuntimePidFileGuard> {
    let control_dir = runtime_control_dir(config);
    fs::create_dir_all(&control_dir)
        .with_context(|| format!("failed to create {}", control_dir.display()))?;
    let path = runtime_pid_path(config, profile);
    fs::write(&path, format!("{}\n", std::process::id()))
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(RuntimePidFileGuard { path })
}

fn read_runtime_pid(path: &Path) -> Result<u32> {
    let value =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    value
        .trim()
        .parse::<u32>()
        .with_context(|| format!("invalid runtime pid file {}", path.display()))
}

fn signal_runtime_profile(
    config: &NanoclawConfig,
    profile: RuntimeServeProfile,
    signal: &str,
    remove_pid_file_on_success: bool,
) -> Result<()> {
    let path = runtime_pid_path(config, profile);
    let pid = read_runtime_pid(&path)?;
    let status = Command::new("kill")
        .arg(signal)
        .arg(pid.to_string())
        .status()
        .with_context(|| format!("failed to invoke kill for pid {pid}"))?;
    if !status.success() {
        anyhow::bail!(
            "failed to signal runtime profile '{}' pid {} with {}: {}",
            profile.as_str(),
            pid,
            signal,
            status
        );
    }
    if remove_pid_file_on_success {
        let _ = fs::remove_file(&path);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "ok": true,
            "profile": profile.as_str(),
            "pid": pid,
            "signal": signal,
            "pidFile": path.display().to_string(),
        }))?
    );
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeHealthCheckStatus {
    Pass,
    Warn,
    Fail,
}

impl RuntimeHealthCheckStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Fail => "fail",
        }
    }

    fn severity(self) -> &'static str {
        match self {
            Self::Pass => "info",
            Self::Warn => "warning",
            Self::Fail => "error",
        }
    }
}

#[derive(Debug, Clone)]
struct RuntimeHealthCheck {
    id: &'static str,
    status: RuntimeHealthCheckStatus,
    message: String,
    evidence: Value,
}

fn runtime_health_check(
    id: &'static str,
    status: RuntimeHealthCheckStatus,
    message: impl Into<String>,
    evidence: Value,
) -> RuntimeHealthCheck {
    RuntimeHealthCheck {
        id,
        status,
        message: message.into(),
        evidence,
    }
}

fn runtime_health_check_json(check: &RuntimeHealthCheck) -> Value {
    json!({
        "id": check.id,
        "status": check.status.as_str(),
        "severity": check.status.severity(),
        "message": check.message,
        "evidence": check.evidence,
    })
}

fn process_is_running(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn runtime_pid_profile_state_json(config: &NanoclawConfig, profile: RuntimeServeProfile) -> Value {
    let pid_file = runtime_pid_path(config, profile);
    if !pid_file.exists() {
        return json!({
            "profile": profile.as_str(),
            "pidFile": pid_file.display().to_string(),
            "pidFileExists": false,
            "pid": null,
            "state": "absent",
            "running": false,
        });
    }

    match fs::read_to_string(&pid_file)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
    {
        Some(pid) => {
            let running = process_is_running(pid);
            json!({
                "profile": profile.as_str(),
                "pidFile": pid_file.display().to_string(),
                "pidFileExists": true,
                "pid": pid,
                "state": if running { "running" } else { "stale" },
                "running": running,
            })
        }
        None => json!({
            "profile": profile.as_str(),
            "pidFile": pid_file.display().to_string(),
            "pidFileExists": true,
            "pid": null,
            "state": "invalid",
            "running": false,
        }),
    }
}

fn append_runtime_startup_event(config: &NanoclawConfig, event: &Value) -> Result<()> {
    let control_dir = runtime_control_dir(config);
    fs::create_dir_all(&control_dir)
        .with_context(|| format!("failed to create {}", control_dir.display()))?;
    let path = runtime_startup_events_path(config);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    writeln!(file, "{}", serde_json::to_string(event)?)
        .with_context(|| format!("failed to append {}", path.display()))?;
    Ok(())
}

fn record_runtime_startup_event(
    config: &NanoclawConfig,
    serve_args: &RuntimeServeArgs,
    phase: &str,
    status: &str,
    message: impl Into<String>,
    evidence: Value,
) -> Result<Value> {
    let event = json!({
        "schemaVersion": "2026-05-20",
        "eventId": Uuid::new_v4().to_string(),
        "timestamp": Utc::now().to_rfc3339(),
        "profile": serve_args.profile.as_str(),
        "phase": phase,
        "status": status,
        "readOnly": serve_args.read_only,
        "laneOverride": serve_args.lane_override.as_ref().map(ExecutionLane::as_str),
        "processId": std::process::id(),
        "message": message.into(),
        "evidence": evidence,
    });
    append_runtime_startup_event(config, &event)?;
    Ok(event)
}

fn runtime_startup_events_json(config: &NanoclawConfig, limit: usize) -> Value {
    let path = runtime_startup_events_path(config);
    match fs::read_to_string(&path) {
        Ok(content) => {
            let mut total_records = 0usize;
            let mut invalid_records = 0usize;
            let mut recent = Vec::<Value>::new();
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                total_records += 1;
                match serde_json::from_str::<Value>(line) {
                    Ok(value) => {
                        recent.push(value);
                    }
                    Err(error) => {
                        invalid_records += 1;
                        recent.push(json!({
                            "schemaVersion": "2026-05-20",
                            "status": "invalid",
                            "message": "runtime startup event record could not be parsed",
                            "error": error.to_string(),
                        }));
                    }
                }
                if recent.len() > limit {
                    recent.remove(0);
                }
            }
            json!({
                "file": path_state_json(&path),
                "totalRecords": total_records,
                "invalidRecords": invalid_records,
                "shown": recent.len(),
                "recent": recent,
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => json!({
            "file": path_state_json(&path),
            "totalRecords": 0,
            "invalidRecords": 0,
            "shown": 0,
            "recent": [],
        }),
        Err(error) => json!({
            "file": path_state_json(&path),
            "totalRecords": null,
            "invalidRecords": null,
            "shown": 0,
            "recent": [],
            "error": error.to_string(),
        }),
    }
}

fn runtime_startup_failure_summary(events: &Value) -> (usize, BTreeMap<String, usize>, Value) {
    let mut failed = 0usize;
    let mut by_profile = BTreeMap::<String, usize>::new();
    let mut latest_failed = Value::Null;

    if let Some(recent_events) = events.get("recent").and_then(Value::as_array) {
        for event in recent_events {
            if event.get("status").and_then(Value::as_str) != Some("failed") {
                continue;
            }
            failed += 1;
            let profile = event
                .get("profile")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            *by_profile.entry(profile).or_default() += 1;
            latest_failed = event.clone();
        }
    }

    (failed, by_profile, latest_failed)
}

fn runtime_cleanup_json(config: &NanoclawConfig, args: RuntimeCleanupArgs) -> Value {
    let mut candidates = Vec::<Value>::new();
    let mut removed = Vec::<Value>::new();
    let mut errors = Vec::<Value>::new();

    for profile in RuntimeServeProfile::all() {
        let state = runtime_pid_profile_state_json(config, profile);
        let state_name = state
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        if !matches!(state_name, "stale" | "invalid") {
            continue;
        }

        let pid_file = runtime_pid_path(config, profile);
        let mut candidate = json!({
            "profile": profile.as_str(),
            "state": state_name,
            "pid": state.get("pid").cloned().unwrap_or(Value::Null),
            "pidFile": pid_file.display().to_string(),
            "removed": false,
        });

        if args.apply {
            match fs::remove_file(&pid_file) {
                Ok(()) => {
                    candidate["removed"] = Value::Bool(true);
                    removed.push(candidate.clone());
                }
                Err(error) => {
                    errors.push(json!({
                        "profile": profile.as_str(),
                        "pidFile": pid_file.display().to_string(),
                        "error": error.to_string(),
                    }));
                }
            }
        }

        candidates.push(candidate);
    }

    let mut report = json!({
        "ok": errors.is_empty(),
        "applied": args.apply,
        "summary": {
            "candidates": candidates.len(),
            "removed": removed.len(),
            "errors": errors.len(),
        },
        "controlDir": runtime_control_dir(config).display().to_string(),
        "candidates": candidates,
        "removed": removed,
        "errors": errors,
    });

    if args.include_state_residue {
        report["stateResidue"] = runtime_state_residue_json(config);
    }

    report
}

fn runtime_health_json(app: &NanoclawApp, limit: usize) -> Result<Value> {
    let counts = app.db.counts()?;
    let tasks = app.list_tasks()?;
    let due_tasks = app.due_tasks()?;
    let recent_provenance = app.db.list_execution_provenance(None, limit)?;
    let recent_evidence = app.db.list_execution_evidence(None, limit)?;
    let failed_tasks = tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Failed)
        .count();
    let active_tasks = tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Active)
        .count();
    let failed_evidence = recent_evidence
        .iter()
        .filter(|record| record.status == "failed")
        .count();
    let mut checks = Vec::<RuntimeHealthCheck>::new();

    checks.push(runtime_health_check(
        "data_dir",
        if app.config.data_dir.is_dir() {
            RuntimeHealthCheckStatus::Pass
        } else {
            RuntimeHealthCheckStatus::Fail
        },
        if app.config.data_dir.is_dir() {
            "data directory is present"
        } else {
            "data directory is missing"
        },
        json!({ "path": app.config.data_dir.display().to_string() }),
    ));

    checks.push(runtime_health_check(
        "store_dir",
        if app.config.store_dir.is_dir() {
            RuntimeHealthCheckStatus::Pass
        } else {
            RuntimeHealthCheckStatus::Fail
        },
        if app.config.store_dir.is_dir() {
            "store directory is present"
        } else {
            "store directory is missing"
        },
        json!({ "path": app.config.store_dir.display().to_string() }),
    ));

    let profile_states = RuntimeServeProfile::all()
        .iter()
        .map(|profile| runtime_pid_profile_state_json(&app.config, *profile))
        .collect::<Vec<_>>();
    let stale_or_invalid_profiles = profile_states
        .iter()
        .filter(|profile| {
            matches!(
                profile.get("state").and_then(Value::as_str),
                Some("stale" | "invalid")
            )
        })
        .count();
    checks.push(runtime_health_check(
        "runtime_pid_files",
        if stale_or_invalid_profiles == 0 {
            RuntimeHealthCheckStatus::Pass
        } else {
            RuntimeHealthCheckStatus::Warn
        },
        if stale_or_invalid_profiles == 0 {
            "runtime PID files are clean"
        } else {
            "one or more runtime PID files are stale or invalid"
        },
        json!({
            "controlDir": runtime_control_dir(&app.config).display().to_string(),
            "profiles": profile_states,
            "staleOrInvalid": stale_or_invalid_profiles,
        }),
    ));

    let startup_events = runtime_startup_events_json(&app.config, limit);
    let (failed_startup_events, failed_startup_profiles, latest_failed_startup) =
        runtime_startup_failure_summary(&startup_events);
    let startup_event_status = if failed_startup_events >= 3 {
        RuntimeHealthCheckStatus::Fail
    } else if failed_startup_events > 0 {
        RuntimeHealthCheckStatus::Warn
    } else {
        RuntimeHealthCheckStatus::Pass
    };
    checks.push(runtime_health_check(
        "runtime_startup_events",
        startup_event_status,
        match startup_event_status {
            RuntimeHealthCheckStatus::Pass => {
                "no recent runtime startup or preflight failures are recorded"
            }
            RuntimeHealthCheckStatus::Warn => {
                "recent runtime startup or preflight failure needs operator review"
            }
            RuntimeHealthCheckStatus::Fail => {
                "repeated runtime startup or preflight failures need operator action"
            }
        },
        json!({
            "failedRecent": failed_startup_events,
            "failedProfiles": failed_startup_profiles,
            "latestFailed": latest_failed_startup,
            "startupEvents": startup_events,
            "repeatFailureThreshold": 3,
        }),
    ));

    let gateway_token_configured = !app.config.openclaw_gateway_token.trim().is_empty();
    let gateway_check = if app.config.openclaw_gateway_port > 0 && !gateway_token_configured {
        runtime_health_check(
            "openclaw_gateway_config",
            RuntimeHealthCheckStatus::Fail,
            "gateway port is enabled without a gateway token",
            describe_openclaw_gateway_readiness(&app.config),
        )
    } else if app.config.openclaw_gateway_port == 0 && gateway_token_configured {
        runtime_health_check(
            "openclaw_gateway_config",
            RuntimeHealthCheckStatus::Warn,
            "gateway token is configured but the gateway port is disabled",
            describe_openclaw_gateway_readiness(&app.config),
        )
    } else {
        runtime_health_check(
            "openclaw_gateway_config",
            RuntimeHealthCheckStatus::Pass,
            if gateway_token_configured {
                "gateway configuration is complete"
            } else {
                "gateway is disabled"
            },
            describe_openclaw_gateway_readiness(&app.config),
        )
    };
    checks.push(gateway_check);

    let webhook_missing_auth = app.config.linear_webhook_port > 0
        && (!app.config.linear_legacy_enabled
            || app.config.linear_webhook_secret.trim().is_empty())
        && app.config.github_webhook_secret.trim().is_empty()
        && app.config.observability_webhook_token.trim().is_empty();
    checks.push(runtime_health_check(
        "webhook_auth",
        if webhook_missing_auth {
            RuntimeHealthCheckStatus::Fail
        } else {
            RuntimeHealthCheckStatus::Pass
        },
        if app.config.linear_webhook_port == 0 {
            "webhook server is disabled"
        } else if webhook_missing_auth {
            "webhook server is enabled without any configured webhook auth material"
        } else {
            "webhook auth material is configured"
        },
        json!({
            "port": app.config.linear_webhook_port,
            "linearLegacyEnabled": app.config.linear_legacy_enabled,
            "linearSignatureRequired": app.config.linear_legacy_enabled && !app.config.linear_webhook_secret.trim().is_empty(),
            "githubSignatureRequired": !app.config.github_webhook_secret.trim().is_empty(),
            "observabilityTokenConfigured": !app.config.observability_webhook_token.trim().is_empty(),
        }),
    ));

    checks.push(runtime_health_check(
        "scheduled_task_backlog",
        if due_tasks.is_empty() {
            RuntimeHealthCheckStatus::Pass
        } else {
            RuntimeHealthCheckStatus::Warn
        },
        if due_tasks.is_empty() {
            "no scheduled tasks are currently due"
        } else {
            "scheduled tasks are due and need a runtime poll"
        },
        json!({
            "total": tasks.len(),
            "active": active_tasks,
            "due": due_tasks.len(),
            "failed": failed_tasks,
        }),
    ));

    checks.push(runtime_health_check(
        "failed_tasks",
        if failed_tasks == 0 {
            RuntimeHealthCheckStatus::Pass
        } else {
            RuntimeHealthCheckStatus::Warn
        },
        if failed_tasks == 0 {
            "no failed scheduled tasks are recorded"
        } else {
            "failed scheduled tasks are recorded"
        },
        json!({ "failed": failed_tasks, "total": tasks.len() }),
    ));

    let evidence_status = if !recent_provenance.is_empty() && recent_evidence.is_empty() {
        RuntimeHealthCheckStatus::Warn
    } else if failed_evidence > 0 {
        RuntimeHealthCheckStatus::Warn
    } else {
        RuntimeHealthCheckStatus::Pass
    };
    checks.push(runtime_health_check(
        "execution_evidence",
        evidence_status,
        match evidence_status {
            RuntimeHealthCheckStatus::Pass => "recent execution evidence is consistent",
            RuntimeHealthCheckStatus::Warn => {
                "recent execution history needs operator review for missing or failed evidence"
            }
            RuntimeHealthCheckStatus::Fail => "recent execution evidence is invalid",
        },
        json!({
            "recentProvenance": recent_provenance.len(),
            "recentEvidence": recent_evidence.len(),
            "failedEvidence": failed_evidence,
        }),
    ));

    let tool_adapters = runtime_tool_adapter_registry_json(&app.config);
    let tool_adapters_ok = tool_adapters.get("ok").and_then(Value::as_bool) == Some(true);
    checks.push(runtime_health_check(
        "tool_adapter_registry",
        if tool_adapters_ok {
            RuntimeHealthCheckStatus::Pass
        } else {
            RuntimeHealthCheckStatus::Fail
        },
        if tool_adapters_ok {
            "tool adapter registry contracts are valid"
        } else {
            "tool adapter registry has invalid contracts"
        },
        tool_adapters,
    ));

    let runtime_channels = runtime_channel_registry(&app.config);
    let runtime_channel_status = if runtime_channels.summary.misconfigured > 0 {
        RuntimeHealthCheckStatus::Fail
    } else if runtime_channels.summary.degraded > 0 {
        RuntimeHealthCheckStatus::Warn
    } else {
        RuntimeHealthCheckStatus::Pass
    };
    checks.push(runtime_health_check(
        "runtime_channels",
        runtime_channel_status,
        match runtime_channel_status {
            RuntimeHealthCheckStatus::Pass => "runtime channels have declared ownership",
            RuntimeHealthCheckStatus::Warn => {
                "runtime channels have declared ownership with degraded configuration"
            }
            RuntimeHealthCheckStatus::Fail => {
                "runtime channels have declared ownership but some channels are misconfigured"
            }
        },
        json!(runtime_channels),
    ));

    let failing = checks
        .iter()
        .filter(|check| check.status == RuntimeHealthCheckStatus::Fail)
        .count();
    let warning = checks
        .iter()
        .filter(|check| check.status == RuntimeHealthCheckStatus::Warn)
        .count();
    let status = if failing > 0 {
        "unhealthy"
    } else if warning > 0 {
        "degraded"
    } else {
        "healthy"
    };

    Ok(json!({
        "ok": failing == 0,
        "status": status,
        "summary": {
            "checks": checks.len(),
            "passing": checks.len().saturating_sub(failing + warning),
            "warnings": warning,
            "failing": failing,
        },
        "runtime": runtime_status_json(&app.config),
        "counts": {
            "chats": counts.chats,
            "messages": counts.messages,
            "scheduledTasks": counts.scheduled_tasks,
            "registeredGroups": counts.registered_groups,
        },
        "checks": checks.iter().map(runtime_health_check_json).collect::<Vec<_>>(),
    }))
}

fn runtime_tool_adapter_registry_json(config: &NanoclawConfig) -> Value {
    let built_in_contracts = built_in_tool_adapter_contracts();
    let built_in_reports = validate_built_in_tool_adapter_contracts();
    let external_path = config.tool_adapter_manifest_path();
    let external_exists = external_path.exists();
    let external_result = load_external_tool_adapter_contracts(&external_path);

    let (external_ok, external_status, external_count, external_ids, external_error) =
        match external_result {
            Ok(contracts) => {
                let status = if external_exists {
                    if contracts.is_empty() {
                        "empty"
                    } else {
                        "loaded"
                    }
                } else {
                    "missing"
                };
                (
                    true,
                    status,
                    contracts.len(),
                    contracts
                        .iter()
                        .map(|contract| contract.id.clone())
                        .collect::<Vec<_>>(),
                    Value::Null,
                )
            }
            Err(error) => (
                false,
                "invalid",
                0,
                Vec::new(),
                Value::String(error.to_string()),
            ),
        };

    let built_in_ok = built_in_reports.is_empty();
    json!({
        "ok": built_in_ok && external_ok,
        "builtIn": {
            "ok": built_in_ok,
            "count": built_in_contracts.len(),
            "ids": built_in_contracts
                .iter()
                .map(|contract| contract.id.clone())
                .collect::<Vec<_>>(),
            "validationReports": validation_reports_json(&built_in_reports),
        },
        "external": {
            "ok": external_ok,
            "path": external_path.display().to_string(),
            "exists": external_exists,
            "status": external_status,
            "count": external_count,
            "ids": external_ids,
            "error": external_error,
        },
    })
}

fn validation_reports_json(
    reports: &[super::tool_registry::ToolAdapterContractValidationReport],
) -> Vec<Value> {
    reports
        .iter()
        .map(|report| {
            json!({
                "id": report.id,
                "violations": report.violations.iter().map(|violation| {
                    json!({
                        "field": violation.field,
                        "message": violation.message,
                    })
                }).collect::<Vec<_>>(),
            })
        })
        .collect()
}

fn runtime_status_json(config: &NanoclawConfig) -> serde_json::Value {
    json!({
        "ok": true,
        "activeBinary": "nanoclaw",
        "dataDir": config.data_dir.display().to_string(),
        "storeDir": config.store_dir.display().to_string(),
        "control": runtime_pid_status_json(config),
        "local": {
            "enabled": true,
            "mode": "poll",
            "inboxDir": config.data_dir.join("channels").join("local").join("inbox").display().to_string(),
            "outboxDir": config.data_dir.join("channels").join("local").join("outbox").display().to_string(),
        },
        "slack": {
            "envFile": config
                .slack_env_file
                .as_ref()
                .map(|path| path.display().to_string()),
            "pollIntervalMs": config.slack_poll_interval_ms,
        },
        "webhook": {
            "enabled": config.linear_webhook_port > 0,
            "port": config.linear_webhook_port,
            "linearLegacyEnabled": config.linear_legacy_enabled,
            "linearSignatureRequired": config.linear_legacy_enabled && !config.linear_webhook_secret.trim().is_empty(),
            "githubSignatureRequired": !config.github_webhook_secret.trim().is_empty(),
            "observabilityTokenConfigured": !config.observability_webhook_token.trim().is_empty(),
        },
        "pmAutomation": {
            "enabled": config.linear_legacy_enabled && !config.linear_chat_jid.trim().is_empty(),
            "status": if config.linear_legacy_enabled { "legacy-enabled" } else { "discontinued" },
            "linearChatJidConfigured": !config.linear_chat_jid.trim().is_empty(),
            "teamKeysConfigured": !config.linear_pm_team_keys.is_empty(),
        },
        "openclawGateway": describe_openclaw_gateway_readiness(config),
        "execution": {
            "defaultLane": config.execution_lane.as_str(),
            "gatewayLane": config.openclaw_gateway_execution_lane.as_str(),
        },
        "toolAdapters": runtime_tool_adapter_registry_json(config),
        "runtimeChannels": json!(runtime_channel_registry(config)),
        "serveProfiles": ["full", "gateway", "webhook", "pm", "slack"],
    })
}

fn path_state_json(path: &Path) -> Value {
    match fs::metadata(path) {
        Ok(metadata) => json!({
            "path": path.display().to_string(),
            "exists": true,
            "isDir": metadata.is_dir(),
            "isFile": metadata.is_file(),
            "bytes": if metadata.is_file() {
                Value::from(metadata.len())
            } else {
                Value::Null
            },
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => json!({
            "path": path.display().to_string(),
            "exists": false,
            "isDir": false,
            "isFile": false,
            "bytes": null,
        }),
        Err(error) => json!({
            "path": path.display().to_string(),
            "exists": false,
            "isDir": false,
            "isFile": false,
            "bytes": null,
            "error": error.to_string(),
        }),
    }
}

fn dir_inventory_json(path: &Path) -> Value {
    let mut files = 0usize;
    let mut dirs = 0usize;
    let mut json_files = 0usize;
    let mut other = 0usize;
    let mut errors = Vec::<Value>::new();

    match fs::read_dir(path) {
        Ok(entries) => {
            for entry in entries {
                match entry {
                    Ok(entry) => {
                        let entry_path = entry.path();
                        match entry.file_type() {
                            Ok(file_type) if file_type.is_dir() => dirs += 1,
                            Ok(file_type) if file_type.is_file() => {
                                files += 1;
                                if entry_path.extension().and_then(|ext| ext.to_str())
                                    == Some("json")
                                {
                                    json_files += 1;
                                } else {
                                    other += 1;
                                }
                            }
                            Ok(_) => other += 1,
                            Err(error) => errors.push(json!({
                                "path": entry_path.display().to_string(),
                                "error": error.to_string(),
                            })),
                        }
                    }
                    Err(error) => errors.push(json!({ "error": error.to_string() })),
                }
            }
            json!({
                "path": path.display().to_string(),
                "exists": true,
                "files": files,
                "dirs": dirs,
                "jsonFiles": json_files,
                "otherEntries": other,
                "errors": errors,
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => json!({
            "path": path.display().to_string(),
            "exists": false,
            "files": 0,
            "dirs": 0,
            "jsonFiles": 0,
            "otherEntries": 0,
            "errors": [],
        }),
        Err(error) => json!({
            "path": path.display().to_string(),
            "exists": false,
            "files": 0,
            "dirs": 0,
            "jsonFiles": 0,
            "otherEntries": 0,
            "errors": [{ "error": error.to_string() }],
        }),
    }
}

fn runtime_state_residue_item_json(
    key: &str,
    classification: &str,
    path: PathBuf,
    active: bool,
    recommendation: &str,
) -> Value {
    let inventory = if path.is_dir() {
        dir_inventory_json(&path)
    } else {
        Value::Null
    };

    json!({
        "key": key,
        "classification": classification,
        "activeRuntimePath": active,
        "recommendation": recommendation,
        "state": path_state_json(&path),
        "inventory": inventory,
    })
}

fn runtime_state_residue_operator_action_json(item: &Value) -> Value {
    let key = item.get("key").and_then(Value::as_str).unwrap_or("unknown");
    let classification = item
        .get("classification")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let path = item
        .get("state")
        .and_then(|state| state.get("path"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let exists = item
        .get("state")
        .and_then(|state| state.get("exists"))
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let (action, destructive, next_step) = match classification {
        "legacy_vector_cache" => (
            "purge_candidate",
            true,
            "confirm no active runtime contract references this cache before any separate operator-approved purge",
        ),
        "legacy_vector_store" | "legacy_embedding_db" | "legacy_history_jsonl" => (
            "migration_candidate",
            false,
            "migrate only through an explicit runtime contract, then archive or purge in a separate operator-approved step",
        ),
        "legacy_source_reference" => (
            "preserve_reference",
            false,
            "keep parked as source/reference material; do not delete through runtime cleanup",
        ),
        _ => (
            "review_candidate",
            false,
            "review manually before deciding whether this belongs to active runtime state",
        ),
    };

    json!({
        "key": key,
        "classification": classification,
        "path": path,
        "exists": exists,
        "status": if exists { "needs_operator_decision" } else { "not_present" },
        "action": action,
        "approvalRequired": true,
        "destructive": destructive,
        "safeDefault": "leave_in_place",
        "nextStep": next_step,
    })
}

fn runtime_state_residue_json(config: &NanoclawConfig) -> Value {
    let active_roots = vec![
        runtime_state_residue_item_json(
            "central_db",
            "active_central_db",
            config.db_path.clone(),
            true,
            "preserve; active runtime database",
        ),
        runtime_state_residue_item_json(
            "data_dir",
            "active_runtime_data",
            config.data_dir.clone(),
            true,
            "preserve; active runtime data root",
        ),
        runtime_state_residue_item_json(
            "groups_dir",
            "active_group_roots",
            config.groups_dir.clone(),
            true,
            "preserve; active group runtime roots",
        ),
        runtime_state_residue_item_json(
            "store_dir",
            "active_store_root",
            config.store_dir.clone(),
            true,
            "preserve; active store root containing the central DB",
        ),
        runtime_state_residue_item_json(
            "runtime_control_dir",
            "active_runtime_control",
            runtime_control_dir(config),
            true,
            "preserve; PID cleanup is handled separately by runtime cleanup",
        ),
        runtime_state_residue_item_json(
            "executor_sessions_dir",
            "active_execution_sidecars",
            config.data_dir.join("executor").join("sessions"),
            true,
            "preserve; active execution sidecar root",
        ),
        runtime_state_residue_item_json(
            "container_cache_dir",
            "active_execution_cache",
            config.data_dir.join("executor").join("container-cache"),
            true,
            "preserve unless an operator explicitly purges execution cache",
        ),
    ];

    let legacy_candidates = vec![
        runtime_state_residue_item_json(
            "fastembed_cache",
            "legacy_vector_cache",
            config.project_root.join(".fastembed_cache"),
            false,
            "legacy Agency vector cache; ignore or delete only after explicit operator approval",
        ),
        runtime_state_residue_item_json(
            "root_memory_json",
            "legacy_vector_store",
            config.project_root.join("memory.json"),
            false,
            "legacy memory server store; migrate only if a runtime contract needs it",
        ),
        runtime_state_residue_item_json(
            "store_memory_json",
            "legacy_vector_store",
            config.store_dir.join("memory.json"),
            false,
            "legacy memory store candidate; not used by the active NanoClaw runtime",
        ),
        runtime_state_residue_item_json(
            "store_agency_memory_json",
            "legacy_vector_store",
            config.store_dir.join("agency_memory.json"),
            false,
            "legacy memory store candidate; not used by the active NanoClaw runtime",
        ),
        runtime_state_residue_item_json(
            "store_embeddings_db",
            "legacy_embedding_db",
            config.store_dir.join("embeddings.db"),
            false,
            "legacy embedding DB candidate; not used by the active NanoClaw runtime",
        ),
        runtime_state_residue_item_json(
            "agency_history_jsonl",
            "legacy_history_jsonl",
            config.data_dir.join("agency_history.jsonl"),
            false,
            "legacy history file; migrate into session sidecars only through an explicit migration",
        ),
        runtime_state_residue_item_json(
            "src_memory",
            "legacy_source_reference",
            config.project_root.join("src").join("memory"),
            false,
            "source reference only; do not runtime-delete from cleanup commands",
        ),
        runtime_state_residue_item_json(
            "src_services_memory",
            "legacy_source_reference",
            config
                .project_root
                .join("src")
                .join("services")
                .join("memory.rs"),
            false,
            "source reference only; do not runtime-delete from cleanup commands",
        ),
    ];
    let present_legacy_candidates = legacy_candidates
        .iter()
        .filter(|item| {
            item.get("state")
                .and_then(|state| state.get("exists"))
                .and_then(Value::as_bool)
                == Some(true)
        })
        .count();
    let operator_actions = legacy_candidates
        .iter()
        .map(runtime_state_residue_operator_action_json)
        .collect::<Vec<_>>();

    json!({
        "policy": {
            "destructiveCleanup": "manual-only",
            "cleanupCommandDeletesState": false,
            "operatorActionRequired": true,
            "note": "This inventory is report-only; runtime cleanup only removes stale or invalid PID files.",
        },
        "summary": {
            "activeRoots": active_roots.len(),
            "legacyCandidates": legacy_candidates.len(),
            "presentLegacyCandidates": present_legacy_candidates,
            "operatorActions": operator_actions.len(),
        },
        "activeRoots": active_roots,
        "legacyCandidates": legacy_candidates,
        "operatorActions": operator_actions,
    })
}

fn sqlite_table_count_json(path: &Path, table: &str) -> Value {
    if !path.exists() {
        return json!({
            "path": path.display().to_string(),
            "exists": false,
            "table": table,
            "count": null,
        });
    }

    match Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY) {
        Ok(conn) => {
            let sql = format!("SELECT COUNT(*) FROM {table}");
            match conn.query_row(&sql, [], |row| row.get::<_, i64>(0)) {
                Ok(count) => json!({
                    "path": path.display().to_string(),
                    "exists": true,
                    "table": table,
                    "count": count,
                }),
                Err(error) => json!({
                    "path": path.display().to_string(),
                    "exists": true,
                    "table": table,
                    "count": null,
                    "error": error.to_string(),
                }),
            }
        }
        Err(error) => json!({
            "path": path.display().to_string(),
            "exists": true,
            "table": table,
            "count": null,
            "error": error.to_string(),
        }),
    }
}

fn runtime_session_sidecar_state_json(
    config: &NanoclawConfig,
    group_folder: &str,
    session_id: &str,
) -> Value {
    let session = build_execution_session(
        &config.data_dir,
        group_folder,
        session_id,
        &config.groups_dir.join(group_folder),
    );
    let paths = session_sidecar_paths(&session);

    json!({
        "groupFolder": group_folder,
        "sessionId": session_id,
        "sessionRoot": path_state_json(&paths.session_root),
        "inboundDb": {
            "file": path_state_json(&paths.inbound_db),
            "messagesIn": sqlite_table_count_json(&paths.inbound_db, "messages_in"),
            "destinations": sqlite_table_count_json(&paths.inbound_db, "destinations"),
            "sessionRouting": sqlite_table_count_json(&paths.inbound_db, "session_routing"),
        },
        "outboundDb": {
            "file": path_state_json(&paths.outbound_db),
            "messagesOut": sqlite_table_count_json(&paths.outbound_db, "messages_out"),
            "processingAck": sqlite_table_count_json(&paths.outbound_db, "processing_ack"),
        },
    })
}

fn runtime_state_json(app: &NanoclawApp, limit: usize) -> Result<Value> {
    let counts = app.db.counts()?;
    let groups = app.groups()?;
    let tasks = app.list_tasks()?;
    let due_tasks = app.due_tasks()?;
    let recent_tasks = tasks.iter().take(limit).cloned().collect::<Vec<_>>();
    let mut task_status_counts = BTreeMap::<String, usize>::new();
    for task in &tasks {
        *task_status_counts
            .entry(task.status.as_str().to_string())
            .or_default() += 1;
    }

    let local_root = app.config.data_dir.join("channels").join("local");
    let executor_sessions_dir = app.config.data_dir.join("executor").join("sessions");
    let mut linked_session_ids = BTreeSet::<String>::new();
    let mut group_sessions = Vec::<Option<String>>::new();
    let mut group_roots = Vec::<Value>::new();
    let mut session_sidecars = Vec::<Value>::new();

    for group in &groups {
        let session_id = app.db.session_for_group(&group.folder)?;
        if let Some(session_id) = session_id.as_deref() {
            linked_session_ids.insert(session_id.to_string());
        }
        group_sessions.push(session_id);
    }

    for (group, session_id) in groups.iter().zip(group_sessions.iter()).take(limit) {
        let group_root = app.config.groups_dir.join(&group.folder);
        if let Some(session_id) = session_id.as_deref() {
            session_sidecars.push(runtime_session_sidecar_state_json(
                &app.config,
                &group.folder,
                session_id,
            ));
        }
        group_roots.push(json!({
            "folder": group.folder,
            "jid": group.jid,
            "name": group.name,
            "isMain": group.is_main,
            "root": path_state_json(&group_root),
            "template": path_state_json(&group_root.join("CLAUDE.md")),
            "sessionId": session_id,
        }));
    }

    let mut orphan_session_dirs = Vec::<Value>::new();
    if let Ok(entries) = fs::read_dir(&executor_sessions_dir) {
        let mut dirs = entries
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                let file_type = entry.file_type().ok()?;
                if !file_type.is_dir() {
                    return None;
                }
                let session_id = entry.file_name().to_string_lossy().to_string();
                if linked_session_ids.contains(&session_id) {
                    return None;
                }
                Some((session_id, path))
            })
            .collect::<Vec<_>>();
        dirs.sort_by(|left, right| left.0.cmp(&right.0));
        orphan_session_dirs = dirs
            .into_iter()
            .take(limit)
            .map(|(session_id, path)| {
                json!({
                    "sessionId": session_id,
                    "root": path_state_json(&path),
                    "inventory": dir_inventory_json(&path),
                })
            })
            .collect();
    }

    Ok(json!({
        "ok": true,
        "runtime": {
            "activeBinary": "nanoclaw",
            "control": runtime_pid_status_json(&app.config),
            "startupEvents": runtime_startup_events_json(&app.config, limit),
        },
        "roots": {
            "projectRoot": path_state_json(&app.config.project_root),
            "dataDir": path_state_json(&app.config.data_dir),
            "storeDir": path_state_json(&app.config.store_dir),
            "groupsDir": path_state_json(&app.config.groups_dir),
            "runtimeControlDir": path_state_json(&runtime_control_dir(&app.config)),
            "executorSessionsDir": path_state_json(&executor_sessions_dir),
        },
        "centralDb": {
            "file": path_state_json(app.db.path()),
            "tables": {
                "chats": counts.chats,
                "messages": counts.messages,
                "scheduledTasks": counts.scheduled_tasks,
                "registeredGroups": counts.registered_groups,
                "sessions": sqlite_table_count_json(app.db.path(), "sessions"),
                "executionProvenance": sqlite_table_count_json(app.db.path(), "execution_provenance"),
                "executionEvidence": sqlite_table_count_json(app.db.path(), "execution_evidence"),
                "swarmRuns": sqlite_table_count_json(app.db.path(), "swarm_runs"),
                "swarmTasks": sqlite_table_count_json(app.db.path(), "swarm_tasks"),
                "observabilityEvents": sqlite_table_count_json(app.db.path(), "observability_events"),
            },
        },
        "localChannel": {
            "root": path_state_json(&local_root),
            "inbox": dir_inventory_json(&local_root.join("inbox")),
            "outbox": dir_inventory_json(&local_root.join("outbox")),
            "processed": dir_inventory_json(&local_root.join("processed")),
        },
        "groupRoots": {
            "root": path_state_json(&app.config.groups_dir),
            "inventory": dir_inventory_json(&app.config.groups_dir),
            "registeredTotal": groups.len(),
            "shown": group_roots.len(),
            "items": group_roots,
        },
        "sessionSidecars": {
            "root": path_state_json(&executor_sessions_dir),
            "linkedShown": session_sidecars.len(),
            "linkedItems": session_sidecars,
            "orphanShown": orphan_session_dirs.len(),
            "orphanItems": orphan_session_dirs,
        },
        "stateResidue": runtime_state_residue_json(&app.config),
        "queuedTasks": {
            "total": tasks.len(),
            "due": due_tasks.len(),
            "byStatus": task_status_counts,
            "recent": recent_tasks,
        },
    }))
}

fn print_runtime_status(config: &NanoclawConfig) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&runtime_status_json(config))?
    );
    Ok(())
}

fn print_runtime_state(config: NanoclawConfig, limit: usize) -> Result<()> {
    let app = NanoclawApp::open(config)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&runtime_state_json(&app, limit)?)?
    );
    Ok(())
}

fn runtime_inspect_json(app: &NanoclawApp, limit: usize) -> Result<serde_json::Value> {
    let counts = app.db.counts()?;
    let tasks = app.list_tasks()?;
    let due_tasks = app.due_tasks()?;
    let recent_tasks = tasks.iter().take(limit).cloned().collect::<Vec<_>>();
    let recent_provenance = app.db.list_execution_provenance(None, limit)?;
    let recent_evidence = app.db.list_execution_evidence(None, limit)?;
    let mut task_status_counts = BTreeMap::<String, usize>::new();
    for task in &tasks {
        *task_status_counts
            .entry(task.status.as_str().to_string())
            .or_default() += 1;
    }

    Ok(json!({
        "ok": true,
        "runtime": runtime_status_json(&app.config),
        "counts": {
            "chats": counts.chats,
            "messages": counts.messages,
            "scheduledTasks": counts.scheduled_tasks,
            "registeredGroups": counts.registered_groups,
        },
        "tasks": {
            "total": tasks.len(),
            "due": due_tasks.len(),
            "byStatus": task_status_counts,
            "recent": recent_tasks,
        },
        "recentExecutionProvenance": recent_provenance,
        "recentExecutionEvidence": recent_evidence,
    }))
}

fn runtime_health_alert_text(health: &Value) -> String {
    let status = health
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let summary = health.get("summary").unwrap_or(&Value::Null);
    let passing = summary
        .get("passing")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let warnings = summary
        .get("warnings")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let failing = summary
        .get("failing")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let mut lines = vec![
        format!("NanoClaw runtime health: {status}"),
        format!("checks: {passing} passing, {warnings} warnings, {failing} failing"),
    ];

    let mut surfaced = 0usize;
    if let Some(checks) = health.get("checks").and_then(Value::as_array) {
        for check in checks {
            let check_status = check
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            if check_status == "pass" {
                continue;
            }
            surfaced += 1;
            let id = check.get("id").and_then(Value::as_str).unwrap_or("unknown");
            let message = check
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("no message");
            lines.push(format!("- {id}: {check_status} - {message}"));
        }
    }

    if surfaced == 0 {
        lines.push("- all checks passing".to_string());
    }

    lines.join("\n")
}

fn maybe_send_runtime_health_notification(
    config: &NanoclawConfig,
    args: &RuntimeHealthArgs,
    health: &Value,
) -> Result<Value> {
    let Some(chat_jid) = args.notify_local.as_deref() else {
        return Ok(json!({
            "sent": false,
            "reason": "not_configured",
        }));
    };

    let status = health
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    if status == "healthy" && !args.notify_always {
        return Ok(json!({
            "sent": false,
            "reason": "healthy",
            "chatJid": chat_jid,
        }));
    }

    let channel = LocalChannel::new(&config.data_dir)?;
    let envelope = channel.send_message(chat_jid, &runtime_health_alert_text(health))?;
    Ok(json!({
        "sent": true,
        "chatJid": chat_jid,
        "outboxId": envelope.id,
        "timestamp": envelope.timestamp,
    }))
}

fn print_runtime_inspect(config: NanoclawConfig, limit: usize) -> Result<()> {
    let app = NanoclawApp::open(config)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&runtime_inspect_json(&app, limit)?)?
    );
    Ok(())
}

fn print_runtime_health(config: NanoclawConfig, args: RuntimeHealthArgs) -> Result<()> {
    let app = NanoclawApp::open(config)?;
    let mut health = runtime_health_json(&app, args.limit)?;
    let notification = maybe_send_runtime_health_notification(&app.config, &args, &health)?;
    if let Value::Object(fields) = &mut health {
        fields.insert("notification".to_string(), notification);
    }
    println!("{}", serde_json::to_string_pretty(&health)?);
    if args.strict && health.get("ok").and_then(Value::as_bool) != Some(true) {
        anyhow::bail!("runtime health strict check failed");
    }
    Ok(())
}

fn print_runtime_cleanup(config: NanoclawConfig, args: RuntimeCleanupArgs) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&runtime_cleanup_json(&config, args))?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use tempfile::tempdir;

    #[test]
    fn parses_default_runtime_serve_profile() {
        let mut args = Vec::<String>::new().into_iter();
        let parsed = parse_runtime_serve_args(&mut args).unwrap();

        assert_eq!(parsed.profile, RuntimeServeProfile::Full);
        assert_eq!(parsed.lane_override, None);
        assert!(!parsed.read_only);
    }

    #[test]
    fn parses_runtime_serve_profile_lane_and_read_only() {
        let mut args = vec![
            "--profile".to_string(),
            "gateway".to_string(),
            "--lane".to_string(),
            "omx".to_string(),
            "--read-only".to_string(),
        ]
        .into_iter();
        let parsed = parse_runtime_serve_args(&mut args).unwrap();

        assert_eq!(parsed.profile, RuntimeServeProfile::Gateway);
        assert_eq!(parsed.lane_override, Some(ExecutionLane::Omx));
        assert!(parsed.read_only);
    }

    #[test]
    fn rejects_unknown_runtime_serve_profile() {
        let mut args = vec!["--profile".to_string(), "legacy".to_string()].into_iter();
        let error = parse_runtime_serve_args(&mut args).unwrap_err();

        assert!(error
            .to_string()
            .contains("unsupported runtime serve profile"));
    }

    #[test]
    fn parses_runtime_control_profile() {
        let mut args = vec!["--profile".to_string(), "gateway".to_string()].into_iter();
        assert_eq!(
            parse_runtime_control_args(&mut args, "stop").unwrap(),
            RuntimeServeProfile::Gateway
        );
    }

    #[test]
    fn rejects_unknown_runtime_control_arg() {
        let mut args = vec!["--force".to_string()].into_iter();
        let error = parse_runtime_control_args(&mut args, "reload").unwrap_err();
        assert!(error
            .to_string()
            .contains("unexpected runtime reload argument"));
    }

    #[test]
    fn parses_runtime_inspect_limit() {
        let mut args = vec!["--limit".to_string(), "5".to_string()].into_iter();
        assert_eq!(
            parse_limit_args(&mut args, 10, "runtime inspect").unwrap(),
            5
        );
    }

    #[test]
    fn parses_runtime_state_limit() {
        let mut args = vec!["--limit".to_string(), "4".to_string()].into_iter();
        assert_eq!(parse_limit_args(&mut args, 10, "runtime state").unwrap(), 4);
    }

    #[test]
    fn parses_runtime_cleanup_apply() {
        let mut args = vec!["--apply".to_string()].into_iter();
        assert_eq!(
            parse_runtime_cleanup_args(&mut args).unwrap(),
            RuntimeCleanupArgs {
                apply: true,
                include_state_residue: false,
            }
        );
    }

    #[test]
    fn parses_runtime_cleanup_state_residue_report() {
        let mut args = vec!["--state-residue".to_string()].into_iter();
        assert_eq!(
            parse_runtime_cleanup_args(&mut args).unwrap(),
            RuntimeCleanupArgs {
                apply: false,
                include_state_residue: true,
            }
        );
    }

    #[test]
    fn parses_runtime_health_strict_limit() {
        let mut args = vec![
            "--limit".to_string(),
            "3".to_string(),
            "--strict".to_string(),
        ]
        .into_iter();
        assert_eq!(
            parse_runtime_health_args(&mut args).unwrap(),
            RuntimeHealthArgs {
                limit: 3,
                strict: true,
                notify_local: None,
                notify_always: false,
            }
        );
    }

    #[test]
    fn parses_runtime_health_notification_target() {
        let mut args = vec![
            "--notify-local".to_string(),
            "ops".to_string(),
            "--notify-always".to_string(),
        ]
        .into_iter();
        assert_eq!(
            parse_runtime_health_args(&mut args).unwrap(),
            RuntimeHealthArgs {
                limit: 10,
                strict: false,
                notify_local: Some("ops".to_string()),
                notify_always: true,
            }
        );
    }

    #[test]
    fn task_complete_requires_manual_override() {
        let mut args = vec!["task-1".to_string(), "done".to_string()].into_iter();
        let error = parse_task_complete_args(&mut args).unwrap_err();

        assert!(error.to_string().contains("requires --manual-override"));
    }

    #[test]
    fn parses_task_complete_manual_override() {
        let mut args = vec![
            "--manual-override".to_string(),
            "task-1".to_string(),
            "operator".to_string(),
            "verified".to_string(),
        ]
        .into_iter();
        let parsed = parse_task_complete_args(&mut args).unwrap();

        assert_eq!(parsed.task_id, "task-1");
        assert_eq!(parsed.result.as_deref(), Some("operator verified"));
    }

    #[test]
    fn detects_stale_runtime_pid_file() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        fs::create_dir_all(runtime_control_dir(&config))?;
        fs::write(
            runtime_pid_path(&config, RuntimeServeProfile::Full),
            "999999999\n",
        )?;

        let state = runtime_pid_profile_state_json(&config, RuntimeServeProfile::Full);

        assert_eq!(state["state"], "stale");
        assert_eq!(state["pid"], 999999999);
        assert_eq!(state["running"], false);
        Ok(())
    }

    #[test]
    fn detects_invalid_runtime_pid_file() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        fs::create_dir_all(runtime_control_dir(&config))?;
        fs::write(
            runtime_pid_path(&config, RuntimeServeProfile::Pm),
            "not-a-pid\n",
        )?;

        let state = runtime_pid_profile_state_json(&config, RuntimeServeProfile::Pm);

        assert_eq!(state["state"], "invalid");
        assert_eq!(state["pid"], Value::Null);
        assert_eq!(state["running"], false);
        Ok(())
    }

    #[test]
    fn runtime_status_reports_tool_adapter_registry() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.slack_env_file = None;

        let status = runtime_status_json(&config);

        assert_eq!(status["toolAdapters"]["ok"], true);
        assert_eq!(status["toolAdapters"]["external"]["status"], "missing");
        assert!(
            status["toolAdapters"]["builtIn"]["count"]
                .as_u64()
                .unwrap_or_default()
                >= 7
        );
        Ok(())
    }

    #[test]
    fn runtime_status_reports_runtime_channel_registry() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.slack_env_file = None;
        config.linear_webhook_port = 0;
        config.openclaw_gateway_port = 0;
        config.openclaw_gateway_token.clear();
        config.linear_legacy_enabled = false;

        let status = runtime_status_json(&config);
        let channels = &status["runtimeChannels"];

        assert_eq!(channels["ok"], true);
        assert_eq!(channels["summary"]["total"], 6);
        assert!(channels["channels"]
            .as_array()
            .unwrap()
            .iter()
            .any(|channel| {
                channel["id"] == "local"
                    && channel["status"] == "ready"
                    && channel["operatorVisible"] == true
            }));
        assert!(channels["channels"]
            .as_array()
            .unwrap()
            .iter()
            .any(|channel| {
                channel["id"] == "pm_automation"
                    && channel["status"] == "legacy_disabled"
                    && channel["legacy"] == true
            }));
        Ok(())
    }

    #[test]
    fn runtime_health_flags_runtime_channel_misconfiguration() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.groups_dir = temp.path().join("groups");
        config.store_dir = temp.path().join("store");
        config.db_path = config.store_dir.join("messages.db");
        config.slack_env_file = None;
        config.linear_webhook_port = 8789;
        config.github_webhook_secret.clear();
        config.observability_webhook_token.clear();
        config.openclaw_gateway_port = 8788;
        config.openclaw_gateway_token.clear();
        let app = NanoclawApp::open(config)?;

        let health = runtime_health_json(&app, 5)?;
        let channel_check = health["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["id"] == "runtime_channels")
            .expect("runtime channel health check");

        assert_eq!(channel_check["status"], "fail");
        assert_eq!(channel_check["evidence"]["ok"], false);
        assert_eq!(channel_check["evidence"]["summary"]["misconfigured"], 2);
        Ok(())
    }

    #[test]
    fn runtime_serve_preflight_blocks_gateway_without_token() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.slack_env_file = None;
        config.openclaw_gateway_port = 8788;
        config.openclaw_gateway_token.clear();

        let error = runtime_serve_preflight(
            &config,
            &RuntimeServeArgs {
                profile: RuntimeServeProfile::Gateway,
                lane_override: None,
                read_only: false,
            },
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("runtime_channel_misconfigured"));
        assert!(error.contains("profile=gateway"));
        assert!(error.contains("channel=openclaw_gateway"));
        assert!(error.contains("NANOCLAW_OPENCLAW_GATEWAY_TOKEN"));

        let events = runtime_startup_events_json(&config, 5);
        assert_eq!(events["totalRecords"], 1);
        assert_eq!(events["recent"][0]["phase"], "preflight");
        assert_eq!(events["recent"][0]["status"], "failed");
        assert_eq!(events["recent"][0]["profile"], "gateway");
        assert!(events["recent"][0]["evidence"]["failures"]
            .to_string()
            .contains("NANOCLAW_OPENCLAW_GATEWAY_TOKEN"));
        Ok(())
    }

    #[test]
    fn runtime_serve_preflight_blocks_slack_without_required_env_keys() -> Result<()> {
        let temp = tempdir()?;
        let env_file = temp.path().join(".env");
        fs::write(&env_file, "SLACK_BOT_TOKEN=xoxb-test\n")?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.slack_env_file = Some(env_file);

        let error = runtime_serve_preflight(
            &config,
            &RuntimeServeArgs {
                profile: RuntimeServeProfile::Slack,
                lane_override: None,
                read_only: false,
            },
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("runtime_channel_misconfigured"));
        assert!(error.contains("profile=slack"));
        assert!(error.contains("channel=slack"));
        assert!(error.contains("SLACK_APP_TOKEN"));
        Ok(())
    }

    #[test]
    fn runtime_serve_preflight_blocks_full_for_enabled_misconfigured_channel() -> Result<()> {
        let temp = tempdir()?;
        let env_file = temp.path().join(".env");
        fs::write(
            &env_file,
            "SLACK_BOT_TOKEN=xoxb-test\nSLACK_APP_TOKEN=xapp-test\n",
        )?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.slack_env_file = Some(env_file);
        config.openclaw_gateway_port = 8788;
        config.openclaw_gateway_token.clear();

        let error = runtime_serve_preflight(
            &config,
            &RuntimeServeArgs {
                profile: RuntimeServeProfile::Full,
                lane_override: None,
                read_only: false,
            },
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("profile=full"));
        assert!(error.contains("channel=openclaw_gateway"));
        assert!(error.contains("NANOCLAW_OPENCLAW_GATEWAY_TOKEN"));
        Ok(())
    }

    #[test]
    fn runtime_serve_preflight_allows_full_read_only_to_ignore_optional_servers() -> Result<()> {
        let temp = tempdir()?;
        let env_file = temp.path().join(".env");
        fs::write(
            &env_file,
            "SLACK_BOT_TOKEN=xoxb-test\nSLACK_APP_TOKEN=xapp-test\n",
        )?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.slack_env_file = Some(env_file);
        config.openclaw_gateway_port = 8788;
        config.openclaw_gateway_token.clear();

        runtime_serve_preflight(
            &config,
            &RuntimeServeArgs {
                profile: RuntimeServeProfile::Full,
                lane_override: None,
                read_only: true,
            },
        )?;
        Ok(())
    }

    #[test]
    fn runtime_serve_preflight_allows_ready_gateway() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.slack_env_file = None;
        config.openclaw_gateway_port = 8788;
        config.openclaw_gateway_token = "gateway-token".to_string();

        runtime_serve_preflight(
            &config,
            &RuntimeServeArgs {
                profile: RuntimeServeProfile::Gateway,
                lane_override: None,
                read_only: false,
            },
        )?;
        Ok(())
    }

    #[test]
    fn runtime_tool_adapter_registry_reports_invalid_external_manifest() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        fs::write(
            config.tool_adapter_manifest_path(),
            r#"[{
                "id": "bad_shell",
                "runtime_name": "bad-shell",
                "mode": "shell",
                "request_plane": "None",
                "capabilities": {
                    "web_request": false,
                    "email_request": false,
                    "browser": false,
                    "repo_sync": false,
                    "ssh": false,
                    "host_command": false,
                    "secret_broker": false,
                    "os_control": false
                },
                "approval_policy": "not_required",
                "artifact_kinds_required": [],
                "verification_kinds_required": [],
                "blockers_required_on_failure": false,
                "workspace_required": false,
                "operator_visible": false,
                "source_material": null
            }]"#,
        )?;

        let registry = runtime_tool_adapter_registry_json(&config);

        assert_eq!(registry["ok"], false);
        assert_eq!(registry["external"]["status"], "invalid");
        assert!(registry["external"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("failed validation: bad_shell"));
        Ok(())
    }

    #[test]
    fn runtime_cleanup_dry_run_keeps_invalid_pid_file() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        fs::create_dir_all(runtime_control_dir(&config))?;
        let pid_path = runtime_pid_path(&config, RuntimeServeProfile::Slack);
        fs::write(&pid_path, "not-a-pid\n")?;

        let report = runtime_cleanup_json(
            &config,
            RuntimeCleanupArgs {
                apply: false,
                include_state_residue: false,
            },
        );

        assert_eq!(report["ok"], true);
        assert_eq!(report["applied"], false);
        assert_eq!(report["summary"]["candidates"], 1);
        assert!(pid_path.exists());
        Ok(())
    }

    #[test]
    fn runtime_cleanup_apply_removes_invalid_pid_file() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        fs::create_dir_all(runtime_control_dir(&config))?;
        let pid_path = runtime_pid_path(&config, RuntimeServeProfile::Webhook);
        fs::write(&pid_path, "not-a-pid\n")?;

        let report = runtime_cleanup_json(
            &config,
            RuntimeCleanupArgs {
                apply: true,
                include_state_residue: false,
            },
        );

        assert_eq!(report["ok"], true);
        assert_eq!(report["applied"], true);
        assert_eq!(report["summary"]["removed"], 1);
        assert!(!pid_path.exists());
        Ok(())
    }

    #[test]
    fn runtime_state_residue_inventory_is_report_only() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.groups_dir = temp.path().join("groups");
        config.store_dir = temp.path().join("store");
        config.db_path = config.store_dir.join("messages.db");
        fs::create_dir_all(temp.path().join(".fastembed_cache"))?;
        fs::create_dir_all(&config.data_dir)?;
        fs::write(config.data_dir.join("agency_history.jsonl"), "{}\n")?;

        let report = runtime_state_residue_json(&config);

        assert_eq!(report["policy"]["cleanupCommandDeletesState"], false);
        assert_eq!(report["policy"]["operatorActionRequired"], true);
        assert_eq!(report["summary"]["presentLegacyCandidates"], 2);
        assert_eq!(report["summary"]["operatorActions"], 8);
        let legacy_keys = report["legacyCandidates"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|item| item["key"].as_str())
            .collect::<BTreeSet<_>>();
        assert!(legacy_keys.contains("fastembed_cache"));
        assert!(legacy_keys.contains("agency_history_jsonl"));
        let actions = report["operatorActions"].as_array().unwrap();
        let fastembed_action = actions
            .iter()
            .find(|item| item["key"] == "fastembed_cache")
            .expect("fastembed cache action should exist");
        assert_eq!(fastembed_action["action"], "purge_candidate");
        assert_eq!(fastembed_action["destructive"], true);
        assert_eq!(fastembed_action["approvalRequired"], true);
        assert_eq!(fastembed_action["exists"], true);

        let history_action = actions
            .iter()
            .find(|item| item["key"] == "agency_history_jsonl")
            .expect("history action should exist");
        assert_eq!(history_action["action"], "migration_candidate");
        assert_eq!(history_action["destructive"], false);
        assert_eq!(history_action["exists"], true);

        let source_action = actions
            .iter()
            .find(|item| item["key"] == "src_memory")
            .expect("source reference action should exist");
        assert_eq!(source_action["action"], "preserve_reference");
        assert_eq!(source_action["destructive"], false);
        assert!(temp.path().join(".fastembed_cache").exists());
        assert!(config.data_dir.join("agency_history.jsonl").exists());
        Ok(())
    }

    #[test]
    fn runtime_health_notification_skips_healthy_report_by_default() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        let health = json!({
            "ok": true,
            "status": "healthy",
            "summary": {
                "passing": 1,
                "warnings": 0,
                "failing": 0,
            },
            "checks": [],
        });
        let notification = maybe_send_runtime_health_notification(
            &config,
            &RuntimeHealthArgs {
                limit: 10,
                strict: false,
                notify_local: Some("ops".to_string()),
                notify_always: false,
            },
            &health,
        )?;

        assert_eq!(notification["sent"], false);
        assert_eq!(notification["reason"], "healthy");
        Ok(())
    }

    #[test]
    fn runtime_health_notification_writes_local_outbox_when_requested() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        let health = json!({
            "ok": false,
            "status": "unhealthy",
            "summary": {
                "passing": 0,
                "warnings": 0,
                "failing": 1,
            },
            "checks": [{
                "id": "webhook_auth",
                "status": "fail",
                "message": "webhook server is enabled without auth",
            }],
        });
        let notification = maybe_send_runtime_health_notification(
            &config,
            &RuntimeHealthArgs {
                limit: 10,
                strict: false,
                notify_local: Some("ops".to_string()),
                notify_always: false,
            },
            &health,
        )?;
        let channel = LocalChannel::new(&config.data_dir)?;
        let outbox = channel.read_outbox()?;

        assert_eq!(notification["sent"], true);
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0].chat_jid, "ops");
        assert!(outbox[0]
            .text
            .contains("NanoClaw runtime health: unhealthy"));
        assert!(outbox[0].text.contains("webhook_auth"));
        Ok(())
    }

    #[test]
    fn runtime_health_warns_on_recent_startup_failure() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.groups_dir = temp.path().join("groups");
        config.store_dir = temp.path().join("store");
        config.db_path = config.store_dir.join("messages.db");
        let serve_args = RuntimeServeArgs {
            profile: RuntimeServeProfile::Gateway,
            lane_override: None,
            read_only: false,
        };
        record_runtime_startup_event(
            &config,
            &serve_args,
            "preflight",
            "failed",
            "synthetic gateway preflight failure",
            json!({
                "reason": "unit_test",
            }),
        )?;

        let app = NanoclawApp::open(config)?;
        let health = runtime_health_json(&app, 5)?;
        let check = health["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["id"] == "runtime_startup_events")
            .expect("runtime startup event health check should exist");

        assert_eq!(health["status"], "degraded");
        assert_eq!(check["status"], "warn");
        assert_eq!(check["evidence"]["failedRecent"], 1);
        assert_eq!(check["evidence"]["failedProfiles"]["gateway"], 1);
        Ok(())
    }

    #[test]
    fn runtime_health_fails_on_repeated_startup_failures_and_notifies() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.groups_dir = temp.path().join("groups");
        config.store_dir = temp.path().join("store");
        config.db_path = config.store_dir.join("messages.db");
        let serve_args = RuntimeServeArgs {
            profile: RuntimeServeProfile::Gateway,
            lane_override: None,
            read_only: false,
        };
        for index in 0..3 {
            record_runtime_startup_event(
                &config,
                &serve_args,
                "preflight",
                "failed",
                format!("synthetic gateway preflight failure {index}"),
                json!({
                    "reason": "unit_test",
                    "index": index,
                }),
            )?;
        }

        let app = NanoclawApp::open(config.clone())?;
        let health = runtime_health_json(&app, 5)?;
        let notification = maybe_send_runtime_health_notification(
            &config,
            &RuntimeHealthArgs {
                limit: 5,
                strict: false,
                notify_local: Some("ops".to_string()),
                notify_always: false,
            },
            &health,
        )?;
        let channel = LocalChannel::new(&config.data_dir)?;
        let outbox = channel.read_outbox()?;

        assert_eq!(health["status"], "unhealthy");
        assert_eq!(notification["sent"], true);
        assert_eq!(outbox.len(), 1);
        assert!(outbox[0].text.contains("runtime_startup_events"));
        assert!(outbox[0]
            .text
            .contains("repeated runtime startup or preflight failures"));
        Ok(())
    }

    #[test]
    fn runtime_state_reports_active_files_and_sidecars() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.groups_dir = temp.path().join("groups");
        config.store_dir = temp.path().join("store");
        config.db_path = config.store_dir.join("messages.db");
        let app = NanoclawApp::open(config.clone())?;
        let group = group_by_folder(&app, "main")?;
        let session = ensure_cli_session(&app, &group)?;
        let paths = ensure_session_sidecars(&session)?;
        record_on_wake_message(&paths, &group, "wake")?;
        let channel = LocalChannel::new(&config.data_dir)?;
        channel.enqueue_inbound(LocalInboundEnvelope {
            id: Some("local-in-1".to_string()),
            chat_jid: "main".to_string(),
            sender: "operator".to_string(),
            sender_name: None,
            content: "state probe".to_string(),
            timestamp: Some("2026-05-20T00:00:00Z".to_string()),
        })?;
        channel.send_message("main", "state reply")?;

        let state = runtime_state_json(&app, 5)?;

        assert_eq!(state["ok"], true);
        assert_eq!(state["centralDb"]["file"]["exists"], true);
        assert_eq!(state["localChannel"]["inbox"]["jsonFiles"], 1);
        assert_eq!(state["localChannel"]["outbox"]["jsonFiles"], 1);
        assert_eq!(state["groupRoots"]["registeredTotal"], 1);
        assert_eq!(state["sessionSidecars"]["linkedShown"], 1);
        assert_eq!(
            state["sessionSidecars"]["linkedItems"][0]["inboundDb"]["messagesIn"]["count"],
            1
        );
        Ok(())
    }

    #[test]
    fn runtime_state_reports_startup_event_ledger() -> Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.groups_dir = temp.path().join("groups");
        config.store_dir = temp.path().join("store");
        config.db_path = config.store_dir.join("messages.db");
        let serve_args = RuntimeServeArgs {
            profile: RuntimeServeProfile::Gateway,
            lane_override: Some(ExecutionLane::Host),
            read_only: false,
        };
        record_runtime_startup_event(
            &config,
            &serve_args,
            "startup",
            "failed",
            "synthetic startup failure",
            json!({
                "reason": "unit_test",
            }),
        )?;

        let app = NanoclawApp::open(config)?;
        let state = runtime_state_json(&app, 5)?;

        assert_eq!(state["runtime"]["startupEvents"]["totalRecords"], 1);
        assert_eq!(
            state["runtime"]["startupEvents"]["recent"][0]["profile"],
            "gateway"
        );
        assert_eq!(
            state["runtime"]["startupEvents"]["recent"][0]["status"],
            "failed"
        );
        assert_eq!(
            state["runtime"]["startupEvents"]["recent"][0]["evidence"]["reason"],
            "unit_test"
        );
        Ok(())
    }
}

fn print_runtime_poll_summary(summary: super::runtime::RuntimePumpSummary) {
    println!("inbound_messages: {}", summary.inbound_messages);
    println!("processed_groups: {}", summary.processed_groups);
    println!("scheduled_tasks_run: {}", summary.scheduled_tasks_run);
    println!("scheduled_task_errors: {}", summary.scheduled_task_errors);
    println!("swarm_tasks_run: {}", summary.swarm_tasks_run);
    println!("swarm_task_errors: {}", summary.swarm_task_errors);
    println!("outbound_messages: {}", summary.outbound_messages);
}

fn run_runtime_poll(config: NanoclawConfig, lane_override: Option<ExecutionLane>) -> Result<()> {
    let app = NanoclawApp::open(config)?;
    let executor = ExecutionLaneRouter::from_config(&app.config, lane_override)?;
    let mut runtime = LocalRuntime::new(app, executor)?;
    let summary = runtime.poll_once()?;
    print_runtime_poll_summary(summary);
    Ok(())
}

fn park_runtime(profile: RuntimeServeProfile) -> ! {
    eprintln!("nanoclaw runtime profile '{}' is running", profile.as_str());
    loop {
        std::thread::park();
    }
}

fn runtime_serve_required_channel_ids(profile: RuntimeServeProfile) -> &'static [&'static str] {
    match profile {
        RuntimeServeProfile::Full => &["slack"],
        RuntimeServeProfile::Gateway => &["openclaw_gateway"],
        RuntimeServeProfile::Webhook => &["webhook"],
        RuntimeServeProfile::Pm => &["pm_automation", "slack"],
        RuntimeServeProfile::Slack => &["slack"],
    }
}

fn runtime_serve_channel_evidence(
    registry: &RuntimeChannelRegistry,
    profile: RuntimeServeProfile,
) -> Value {
    let profile_name = profile.as_str();
    let required_channel_ids = runtime_serve_required_channel_ids(profile);
    let channels = registry
        .channels
        .iter()
        .filter(|channel| {
            required_channel_ids.contains(&channel.id.as_str())
                || channel
                    .serve_profiles
                    .iter()
                    .any(|candidate| candidate == profile_name)
        })
        .collect::<Vec<_>>();
    json!({
        "summary": &registry.summary,
        "requiredChannelIds": required_channel_ids,
        "profileChannels": channels,
    })
}

fn runtime_serve_preflight(config: &NanoclawConfig, serve_args: &RuntimeServeArgs) -> Result<()> {
    let registry = runtime_channel_registry(config);
    let failures = runtime_serve_preflight_failures(&registry, serve_args);
    if failures.is_empty() {
        return Ok(());
    }

    let message = format_runtime_channel_preflight_error(serve_args.profile, &failures);
    if let Err(error) = record_runtime_startup_event(
        config,
        serve_args,
        "preflight",
        "failed",
        message.clone(),
        json!({
            "runtimeChannels": runtime_serve_channel_evidence(&registry, serve_args.profile),
            "failures": failures,
        }),
    ) {
        eprintln!("warning: failed to record runtime startup preflight evidence: {error}");
    }

    anyhow::bail!("{}", message)
}

fn runtime_serve_preflight_failures<'a>(
    registry: &'a RuntimeChannelRegistry,
    serve_args: &RuntimeServeArgs,
) -> Vec<&'a RuntimeChannelDescriptor> {
    let mut seen = BTreeSet::<String>::new();
    let mut failures = Vec::<&RuntimeChannelDescriptor>::new();

    for id in runtime_serve_required_channel_ids(serve_args.profile) {
        if let Some(channel) = registry.channels.iter().find(|channel| channel.id == *id) {
            if channel.status != RuntimeChannelStatus::Ready && seen.insert(channel.id.clone()) {
                failures.push(channel);
            }
        }
    }

    if !serve_args.read_only {
        let profile = serve_args.profile.as_str();
        for channel in registry.channels.iter().filter(|channel| {
            channel
                .serve_profiles
                .iter()
                .any(|candidate| candidate == profile)
                && channel.status == RuntimeChannelStatus::Misconfigured
        }) {
            if seen.insert(channel.id.clone()) {
                failures.push(channel);
            }
        }
    }

    failures
}

fn format_runtime_channel_preflight_error(
    profile: RuntimeServeProfile,
    failures: &[&RuntimeChannelDescriptor],
) -> String {
    let details = failures
        .iter()
        .map(|channel| {
            let missing = if channel.missing_config.is_empty() {
                "none".to_string()
            } else {
                channel.missing_config.join(",")
            };
            format!(
                "channel={} status={} missing=[{}] configSource={} authRequired={} authConfigured={}",
                channel.id,
                channel.status_message,
                missing,
                channel.config_source,
                channel.auth.required,
                channel.auth.configured
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "runtime_channel_misconfigured profile={} {}",
        profile.as_str(),
        details
    )
}

fn record_runtime_profile_running(
    config: &NanoclawConfig,
    serve_args: &RuntimeServeArgs,
) -> Result<()> {
    let registry = runtime_channel_registry(config);
    record_runtime_startup_event(
        config,
        serve_args,
        "startup",
        "running",
        format!(
            "runtime profile '{}' entered its serving loop",
            serve_args.profile.as_str()
        ),
        json!({
            "control": runtime_pid_profile_state_json(config, serve_args.profile),
            "runtimeChannels": runtime_serve_channel_evidence(&registry, serve_args.profile),
        }),
    )?;
    Ok(())
}

fn run_runtime_serve_profile(config: NanoclawConfig, serve_args: RuntimeServeArgs) -> Result<()> {
    let _pid_guard = write_runtime_pid_file(&config, serve_args.profile)?;
    match serve_args.profile {
        RuntimeServeProfile::Full => {
            let app = NanoclawApp::open(config.clone())?;
            if !serve_args.read_only {
                start_webhook_server(app.config.clone())?;
                start_pm_automation_loop(app.config.clone())?;
                start_openclaw_gateway_server(app.config.clone())?;
            }
            let executor =
                ExecutionLaneRouter::from_config(&app.config, serve_args.lane_override.clone())?;
            let channel = SlackChannel::from_config(&app.config, serve_args.read_only)?;
            let running_config = app.config.clone();
            let mut runtime = SlackRuntime::new(app, channel, executor);
            record_runtime_profile_running(&running_config, &serve_args)?;
            runtime.run_forever()?;
        }
        RuntimeServeProfile::Gateway => {
            let running_config = config.clone();
            let _app = NanoclawApp::open(config.clone())?;
            start_openclaw_gateway_server(config)?;
            record_runtime_profile_running(&running_config, &serve_args)?;
            park_runtime(RuntimeServeProfile::Gateway);
        }
        RuntimeServeProfile::Webhook => {
            let running_config = config.clone();
            start_webhook_server(config)?;
            record_runtime_profile_running(&running_config, &serve_args)?;
            park_runtime(RuntimeServeProfile::Webhook);
        }
        RuntimeServeProfile::Pm => {
            let running_config = config.clone();
            start_pm_automation_loop(config)?;
            record_runtime_profile_running(&running_config, &serve_args)?;
            park_runtime(RuntimeServeProfile::Pm);
        }
        RuntimeServeProfile::Slack => {
            let app = NanoclawApp::open(config)?;
            let executor =
                ExecutionLaneRouter::from_config(&app.config, serve_args.lane_override.clone())?;
            let channel = SlackChannel::from_config(&app.config, serve_args.read_only)?;
            let running_config = app.config.clone();
            let mut runtime = SlackRuntime::new(app, channel, executor);
            record_runtime_profile_running(&running_config, &serve_args)?;
            runtime.run_forever()?;
        }
    }
    Ok(())
}

fn run_runtime_serve(config: NanoclawConfig, serve_args: RuntimeServeArgs) -> Result<()> {
    runtime_serve_preflight(&config, &serve_args)?;
    let registry = runtime_channel_registry(&config);
    record_runtime_startup_event(
        &config,
        &serve_args,
        "startup",
        "starting",
        format!(
            "runtime profile '{}' passed preflight and is starting",
            serve_args.profile.as_str()
        ),
        json!({
            "runtimeChannels": runtime_serve_channel_evidence(&registry, serve_args.profile),
        }),
    )?;

    let result = run_runtime_serve_profile(config.clone(), serve_args.clone());
    if let Err(error) = &result {
        if let Err(record_error) = record_runtime_startup_event(
            &config,
            &serve_args,
            "startup",
            "failed",
            error.to_string(),
            json!({
                "control": runtime_pid_profile_state_json(&config, serve_args.profile),
                "runtimeChannels": runtime_serve_channel_evidence(
                    &runtime_channel_registry(&config),
                    serve_args.profile
                ),
            }),
        ) {
            eprintln!("warning: failed to record runtime startup failure evidence: {record_error}");
        }
    }
    result
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

fn parse_group_runtime_set_args<I>(
    existing: GroupRuntimeConfig,
    args: &mut I,
) -> Result<GroupRuntimeConfig>
where
    I: Iterator<Item = String>,
{
    let mut config = existing;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--provider" => {
                config.provider = Some(
                    args.next()
                        .context("missing value after --provider")?
                        .trim()
                        .to_string(),
                );
            }
            "--backend" => {
                config.backend = Some(
                    args.next()
                        .context("missing value after --backend")?
                        .trim()
                        .to_string(),
                );
            }
            "--model" => {
                config.model = Some(
                    args.next()
                        .context("missing value after --model")?
                        .trim()
                        .to_string(),
                );
            }
            "--effort" => {
                config.effort = Some(
                    args.next()
                        .context("missing value after --effort")?
                        .trim()
                        .to_string(),
                );
            }
            "--assistant-name" => {
                config.assistant_name = Some(
                    args.next()
                        .context("missing value after --assistant-name")?
                        .trim()
                        .to_string(),
                );
            }
            "--max-messages-per-prompt" => {
                let value = args
                    .next()
                    .context("missing value after --max-messages-per-prompt")?;
                let parsed = value
                    .parse::<usize>()
                    .with_context(|| format!("invalid --max-messages-per-prompt value {value}"))?;
                if parsed == 0 {
                    anyhow::bail!("--max-messages-per-prompt must be greater than zero");
                }
                config.max_messages_per_prompt = Some(parsed);
            }
            "--image-tag" => {
                config.image_tag = Some(
                    args.next()
                        .context("missing value after --image-tag")?
                        .trim()
                        .to_string(),
                );
            }
            "--cli-scope" => {
                config.cli_scope = Some(
                    args.next()
                        .context("missing value after --cli-scope")?
                        .trim()
                        .to_string(),
                );
            }
            "--clear-provider" => config.provider = None,
            "--clear-backend" => config.backend = None,
            "--clear-model" => config.model = None,
            "--clear-effort" => config.effort = None,
            "--clear-assistant-name" => config.assistant_name = None,
            "--clear-max-messages-per-prompt" => config.max_messages_per_prompt = None,
            "--clear-image-tag" => config.image_tag = None,
            "--clear-cli-scope" => config.cli_scope = None,
            other => anyhow::bail!("unexpected group-runtime set argument '{}'", other),
        }
    }
    Ok(config)
}

fn group_by_folder(app: &NanoclawApp, group_folder: &str) -> Result<Group> {
    app.groups()?
        .into_iter()
        .find(|group| group.folder == group_folder)
        .with_context(|| format!("registered group not found: {group_folder}"))
}

fn ensure_cli_session(
    app: &NanoclawApp,
    group: &Group,
) -> Result<super::executor::ExecutionSession> {
    let session_id = app
        .db
        .session_for_group(&group.folder)?
        .unwrap_or_else(|| format!("session-{}", uuid::Uuid::new_v4()));
    app.db.upsert_session(&group.folder, &session_id)?;
    let session = build_execution_session(
        &app.config.data_dir,
        &group.folder,
        &session_id,
        &app.config.groups_dir.join(&group.folder),
    );
    session.ensure_layout()?;
    ensure_session_sidecars(&session)?;
    Ok(session)
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
    if matches!(command.as_str(), "--help" | "-h" | "help") {
        print_usage();
        return Ok(());
    }
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
            println!("linear_legacy_enabled: {}", config.linear_legacy_enabled);
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
                "tool_adapters_path: {}",
                config.tool_adapter_manifest_path().display()
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
        "runtime" => {
            let Some(runtime_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            match runtime_command.as_str() {
                "status" => print_runtime_status(&config)?,
                "state" => {
                    let limit = parse_limit_args(&mut args, 10, "runtime state")?;
                    print_runtime_state(config, limit)?;
                }
                "inspect" => {
                    let limit = parse_limit_args(&mut args, 10, "runtime inspect")?;
                    print_runtime_inspect(config, limit)?;
                }
                "health" => {
                    let health_args = parse_runtime_health_args(&mut args)?;
                    print_runtime_health(config, health_args)?;
                }
                "cleanup" => {
                    let cleanup_args = parse_runtime_cleanup_args(&mut args)?;
                    print_runtime_cleanup(config, cleanup_args)?;
                }
                "poll" => {
                    let lane_override = parse_lane_override(&mut args)?;
                    run_runtime_poll(config, lane_override)?;
                }
                "serve" => {
                    let serve_args = parse_runtime_serve_args(&mut args)?;
                    run_runtime_serve(config, serve_args)?;
                }
                "stop" => {
                    let profile = parse_runtime_control_args(&mut args, "stop")?;
                    signal_runtime_profile(&config, profile, "-TERM", true)?;
                }
                "reload" => {
                    let profile = parse_runtime_control_args(&mut args, "reload")?;
                    signal_runtime_profile(&config, profile, "-HUP", false)?;
                }
                other => anyhow::bail!("unsupported runtime command '{}'", other),
            }
        }
        "group-runtime" => {
            let Some(group_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let Some(group_folder) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let app = NanoclawApp::open(config)?;
            match group_command.as_str() {
                "show" => {
                    let runtime_config = app.group_runtime_config(&group_folder)?;
                    println!("{}", serde_json::to_string_pretty(&runtime_config)?);
                }
                "set" => {
                    let existing = app.group_runtime_config(&group_folder)?;
                    let runtime_config = parse_group_runtime_set_args(existing, &mut args)?;
                    app.set_group_runtime_config(&group_folder, &runtime_config)?;
                    println!("{}", serde_json::to_string_pretty(&runtime_config)?);
                }
                other => anyhow::bail!("unsupported group-runtime command '{}'", other),
            }
        }
        "session" => {
            let Some(session_command) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let Some(group_folder) = args.next() else {
                print_usage();
                std::process::exit(2);
            };
            let app = NanoclawApp::open(config)?;
            let group = group_by_folder(&app, &group_folder)?;
            let session = ensure_cli_session(&app, &group)?;
            let paths = ensure_session_sidecars(&session)?;
            match session_command.as_str() {
                "show" => {
                    println!("session_id: {}", session.id);
                    println!("session_root: {}", session.session_root);
                    println!("inbound_db: {}", paths.inbound_db.display());
                    println!("outbound_db: {}", paths.outbound_db.display());
                }
                "wake" => {
                    let content = args.collect::<Vec<_>>().join(" ");
                    let content = if content.trim().is_empty() {
                        "Session wake event.".to_string()
                    } else {
                        content
                    };
                    let id = record_on_wake_message(&paths, &group, &content)?;
                    app.db.record_destination_projection(
                        &group.folder,
                        &session.id,
                        &paths.inbound_db,
                        "session_on_wake",
                    )?;
                    println!("wake_message_id: {}", id);
                    println!("inbound_db: {}", paths.inbound_db.display());
                }
                other => anyhow::bail!("unsupported session command '{}'", other),
            }
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
                "serve" => {
                    let _app = NanoclawApp::open(config.clone())?;
                    start_openclaw_gateway_server(config)?;
                    loop {
                        std::thread::park();
                    }
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
                    let parsed = parse_task_complete_args(&mut args)?;
                    let updated =
                        app.complete_task_run_manual_override(&parsed.task_id, parsed.result)?;
                    if let Some(task) = updated {
                        println!("completed: {}", task.id);
                        println!("status: {}", task.status.as_str());
                        println!("next_run: {}", task.next_run.as_deref().unwrap_or("-"));
                    } else {
                        println!("task_not_found: {}", parsed.task_id);
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
            if !config.linear_legacy_enabled {
                anyhow::bail!(
                    "Linear integration is discontinued for this Nexus runtime; set NANOCLAW_LINEAR_LEGACY_ENABLED=true only for controlled legacy migration"
                );
            }
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
