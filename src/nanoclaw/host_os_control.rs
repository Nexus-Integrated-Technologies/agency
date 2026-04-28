use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::foundation::{
    BoundaryClaim, BoundaryClaimSource, BoundaryQuadrant, CapabilityManifest, ExecutionLocation,
    ExecutionMountKind, ExecutionMountSummaryEntry, ExecutionRunKind, ExecutionStatus, GateAspect,
    GateCheck, GateDecision, GateEvaluation, GateScope, HostOsControlApprovalDecision,
    HostOsControlApprovalRequestRecord, HostOsControlApprovalStatus, RequestPlane, SshEndpoint,
};

use super::db::NanoclawDb;
use super::fpf_bridge::{derive_assurance, derive_task_signature};
use super::remote_control::{run_remote_command, shell_quote, ssh_target_display};
use super::security_profile::{
    build_execution_provenance_record, derive_capability_manifest, BuildExecutionProvenanceInput,
    DeriveCapabilityManifestInput,
};

static ONE_TIME_GRANTS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static SESSION_GRANTS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static SESSION_DENIALS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HostOsControlAction {
    OpenApplication {
        application: String,
    },
    ActivateApplication {
        application: String,
    },
    OpenUrl {
        url: String,
    },
    RevealInFinder {
        path: String,
    },
    ShellCommand {
        command: String,
        cwd: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct RunHostOsControlTaskInput {
    pub action: HostOsControlAction,
}

#[derive(Debug, Clone)]
pub struct HostOsControlContext {
    pub source_group: String,
    pub chat_jid: Option<String>,
    pub request_plane: RequestPlane,
    pub capability_manifest: CapabilityManifest,
    pub project_environment_id: Option<String>,
    pub policy_path: PathBuf,
    pub execution_location: ExecutionLocation,
    pub ssh_target: Option<SshEndpoint>,
}

#[derive(Debug, Clone)]
pub struct RunHostOsControlTaskResult {
    pub ok: bool,
    pub executed: bool,
    pub exit_code: i32,
    pub action: HostOsControlAction,
    pub action_summary: String,
    pub action_scope: String,
    pub output: Option<String>,
    pub verified: bool,
    pub verification_details: Option<String>,
    pub approval_required: bool,
    pub approval_request_id: Option<String>,
    pub allowed_decisions: Vec<HostOsControlApprovalDecision>,
    pub error: Option<String>,
    pub provenance_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct HostOsControlPolicyGrant {
    fingerprint: String,
    action_kind: String,
    action_scope: String,
    created_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct HostOsControlGroupPolicy {
    default_mode: Option<HostOsControlDefaultMode>,
    grants: Vec<HostOsControlPolicyGrant>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct HostOsControlPolicyFile {
    version: u8,
    default_mode: HostOsControlDefaultMode,
    groups: std::collections::BTreeMap<String, HostOsControlGroupPolicy>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum HostOsControlDefaultMode {
    Ask,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApprovalState {
    Ask,
    Allow,
    Deny,
}

pub fn build_default_context(
    source_group: impl Into<String>,
    policy_path: impl Into<PathBuf>,
) -> HostOsControlContext {
    HostOsControlContext {
        source_group: source_group.into(),
        chat_jid: None,
        request_plane: RequestPlane::None,
        capability_manifest: derive_capability_manifest(
            &RequestPlane::None,
            DeriveCapabilityManifestInput {
                allow_os_control: true,
                allow_host_command: true,
                ..Default::default()
            },
        ),
        project_environment_id: None,
        policy_path: policy_path.into(),
        execution_location: ExecutionLocation::Host,
        ssh_target: None,
    }
}

pub fn run_host_os_control_task(
    db: &NanoclawDb,
    input: RunHostOsControlTaskInput,
    context: &HostOsControlContext,
) -> Result<RunHostOsControlTaskResult> {
    let action = normalize_action(input.action);
    let action_scope = action_scope(&action);
    let action_summary = summarize_action(&action);
    let fingerprint = action_fingerprint(&action, context);
    let allowed_decisions = allowed_decisions(&action);
    let approval_state =
        resolve_approval_state(&context.policy_path, &context.source_group, &fingerprint)?;

    if approval_state == ApprovalState::Deny {
        return Ok(RunHostOsControlTaskResult {
            ok: false,
            executed: false,
            exit_code: 1,
            action,
            action_summary,
            action_scope,
            output: None,
            verified: false,
            verification_details: None,
            approval_required: false,
            approval_request_id: None,
            allowed_decisions,
            error: Some("Host OS control is denied by policy for this action.".to_string()),
            provenance_id: None,
        });
    }

    if approval_state == ApprovalState::Ask {
        let record = if let Some(existing) =
            db.find_pending_host_os_control_approval_request(&context.source_group, &fingerprint)?
        {
            existing
        } else {
            let created_at = Utc::now().to_rfc3339();
            let record = HostOsControlApprovalRequestRecord {
                id: format!("host-os-approval-{}", Uuid::new_v4()),
                source_group: context.source_group.clone(),
                chat_jid: context.chat_jid.clone(),
                action_kind: action_kind(&action).to_string(),
                action_scope: action_scope.clone(),
                action_fingerprint: fingerprint.clone(),
                action_summary: action_summary.clone(),
                action_payload: Some(serde_json::to_value(&action)?),
                allowed_decisions: allowed_decisions.clone(),
                boundary_claim: Some(build_host_os_boundary_claim(
                    &context.source_group,
                    &action_summary,
                    &action_scope,
                    &created_at,
                )),
                gate_evaluation: Some(build_host_os_gate_evaluation(
                    &fingerprint,
                    &action_summary,
                    false,
                    &created_at,
                )),
                status: HostOsControlApprovalStatus::Pending,
                resolved_decision: None,
                created_at,
                resolved_at: None,
            };
            db.create_host_os_control_approval_request(&record)?;
            record
        };

        return Ok(RunHostOsControlTaskResult {
            ok: false,
            executed: false,
            exit_code: 1,
            action,
            action_summary,
            action_scope,
            output: None,
            verified: false,
            verification_details: None,
            approval_required: true,
            approval_request_id: Some(record.id),
            allowed_decisions,
            error: Some(
                "Host OS control approval is required. Resolve the request before replaying it."
                    .to_string(),
            ),
            provenance_id: None,
        });
    }

    execute_host_os_control_action(db, action, context, Some(fingerprint), None)
}

pub fn list_pending_host_os_control_requests(
    db: &NanoclawDb,
    limit: usize,
) -> Result<Vec<HostOsControlApprovalRequestRecord>> {
    db.list_host_os_control_approval_requests(
        Some(HostOsControlApprovalStatus::Pending),
        None,
        limit,
    )
}

pub fn resolve_host_os_control_request(
    db: &NanoclawDb,
    policy_path: &Path,
    request_id: &str,
    decision: HostOsControlApprovalDecision,
) -> Result<HostOsControlApprovalRequestRecord> {
    let record = db
        .get_host_os_control_approval_request(request_id)?
        .with_context(|| format!("host OS control request {} was not found", request_id))?;
    if record.status != HostOsControlApprovalStatus::Pending {
        bail!(
            "host OS control request {} is already {}",
            request_id,
            record.status.as_str()
        );
    }
    if !record.allowed_decisions.contains(&decision) {
        bail!(
            "decision {} is not allowed for {}",
            decision.as_str(),
            record.action_kind
        );
    }

    let key = grant_key(&record.source_group, &record.action_fingerprint);
    match decision {
        HostOsControlApprovalDecision::Once => {
            one_time_grants().lock().unwrap().insert(key.clone());
            session_denials().lock().unwrap().remove(&key);
            db.resolve_host_os_control_approval_request(
                request_id,
                HostOsControlApprovalStatus::Approved,
                Some(HostOsControlApprovalDecision::Once),
                None,
            )?;
        }
        HostOsControlApprovalDecision::Session => {
            session_grants().lock().unwrap().insert(key.clone());
            session_denials().lock().unwrap().remove(&key);
            db.resolve_host_os_control_approval_request(
                request_id,
                HostOsControlApprovalStatus::Approved,
                Some(HostOsControlApprovalDecision::Session),
                None,
            )?;
        }
        HostOsControlApprovalDecision::Always => {
            persist_always_grant(
                policy_path,
                &record.source_group,
                HostOsControlPolicyGrant {
                    fingerprint: record.action_fingerprint.clone(),
                    action_kind: record.action_kind.clone(),
                    action_scope: record.action_scope.clone(),
                    created_at: Utc::now().to_rfc3339(),
                },
            )?;
            session_denials().lock().unwrap().remove(&key);
            db.resolve_host_os_control_approval_request(
                request_id,
                HostOsControlApprovalStatus::Approved,
                Some(HostOsControlApprovalDecision::Always),
                None,
            )?;
        }
        HostOsControlApprovalDecision::Deny | HostOsControlApprovalDecision::Custom(_) => {
            session_denials().lock().unwrap().insert(key);
            db.resolve_host_os_control_approval_request(
                request_id,
                HostOsControlApprovalStatus::Denied,
                Some(HostOsControlApprovalDecision::Deny),
                None,
            )?;
        }
    }

    db.get_host_os_control_approval_request(request_id)?
        .with_context(|| {
            format!(
                "host OS control request {} disappeared after resolve",
                request_id
            )
        })
}

pub fn replay_approved_host_os_control_request(
    db: &NanoclawDb,
    request_id: &str,
    context: &HostOsControlContext,
) -> Result<RunHostOsControlTaskResult> {
    let record = db
        .get_host_os_control_approval_request(request_id)?
        .with_context(|| format!("host OS control request {} was not found", request_id))?;
    if record.status != HostOsControlApprovalStatus::Approved {
        bail!(
            "host OS control request {} is {}, not approved",
            request_id,
            record.status.as_str()
        );
    }
    let action = reconstruct_action(&record)
        .with_context(|| format!("host OS control request {} cannot be replayed", request_id))?;
    let replay_context = HostOsControlContext {
        source_group: record.source_group.clone(),
        chat_jid: record.chat_jid.clone().or_else(|| context.chat_jid.clone()),
        request_plane: context.request_plane.clone(),
        capability_manifest: context.capability_manifest.clone(),
        project_environment_id: context.project_environment_id.clone(),
        policy_path: context.policy_path.clone(),
        execution_location: context.execution_location.clone(),
        ssh_target: context.ssh_target.clone(),
    };
    execute_host_os_control_action(db, action, &replay_context, None, Some(&record))
}

pub fn reconstruct_action(
    record: &HostOsControlApprovalRequestRecord,
) -> Result<HostOsControlAction> {
    let payload = record
        .action_payload
        .clone()
        .context("request does not contain an action payload")?;
    serde_json::from_value(payload).context("failed to decode host OS action payload")
}

pub fn approval_notification_text(record: &HostOsControlApprovalRequestRecord) -> String {
    let decisions = record
        .allowed_decisions
        .iter()
        .map(HostOsControlApprovalDecision::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "Host OS approval needed\nRequest: `{}`\nSource group: `{}`\nAction: {}\nAllowed: {}\nResolve with: `approval resolve {} <decision>`",
        record.id, record.source_group, record.action_summary, decisions, record.id
    )
}

pub fn resolution_notification_text(record: &HostOsControlApprovalRequestRecord) -> String {
    format!(
        "Host OS request `{}` is now {} ({})\nAction: {}",
        record.id,
        record.status.as_str(),
        record
            .resolved_decision
            .as_ref()
            .map(HostOsControlApprovalDecision::as_str)
            .unwrap_or("-"),
        record.action_summary
    )
}

#[cfg(test)]
pub fn reset_host_os_control_state_for_test() {
    one_time_grants().lock().unwrap().clear();
    session_grants().lock().unwrap().clear();
    session_denials().lock().unwrap().clear();
}

fn normalize_action(action: HostOsControlAction) -> HostOsControlAction {
    match action {
        HostOsControlAction::OpenApplication { application } => {
            HostOsControlAction::OpenApplication {
                application: application.trim().to_string(),
            }
        }
        HostOsControlAction::ActivateApplication { application } => {
            HostOsControlAction::ActivateApplication {
                application: application.trim().to_string(),
            }
        }
        HostOsControlAction::OpenUrl { url } => HostOsControlAction::OpenUrl {
            url: url.trim().to_string(),
        },
        HostOsControlAction::RevealInFinder { path } => HostOsControlAction::RevealInFinder {
            path: path.trim().to_string(),
        },
        HostOsControlAction::ShellCommand { command, cwd } => HostOsControlAction::ShellCommand {
            command: command.trim().to_string(),
            cwd: cwd
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
        },
    }
}

fn action_kind(action: &HostOsControlAction) -> &'static str {
    match action {
        HostOsControlAction::OpenApplication { .. } => "open_application",
        HostOsControlAction::ActivateApplication { .. } => "activate_application",
        HostOsControlAction::OpenUrl { .. } => "open_url",
        HostOsControlAction::RevealInFinder { .. } => "reveal_in_finder",
        HostOsControlAction::ShellCommand { .. } => "shell_command",
    }
}

fn action_scope(action: &HostOsControlAction) -> String {
    match action {
        HostOsControlAction::OpenApplication { application }
        | HostOsControlAction::ActivateApplication { application } => application.clone(),
        HostOsControlAction::OpenUrl { url } => url.clone(),
        HostOsControlAction::RevealInFinder { path } => path.clone(),
        HostOsControlAction::ShellCommand { command, cwd } => match cwd {
            Some(cwd) => format!("cwd={cwd}|command={command}"),
            None => command.clone(),
        },
    }
}

fn summarize_action(action: &HostOsControlAction) -> String {
    match action {
        HostOsControlAction::OpenApplication { application } => {
            format!("Open application \"{application}\"")
        }
        HostOsControlAction::ActivateApplication { application } => {
            format!("Activate application \"{application}\"")
        }
        HostOsControlAction::OpenUrl { url } => format!("Open URL {url}"),
        HostOsControlAction::RevealInFinder { path } => format!("Reveal in Finder {path}"),
        HostOsControlAction::ShellCommand { command, cwd } => match cwd {
            Some(cwd) => format!("Run shell command in {cwd}: {command}"),
            None => format!("Run shell command: {command}"),
        },
    }
}

fn build_host_os_boundary_claim(
    source_group: &str,
    action_summary: &str,
    action_scope: &str,
    created_at: &str,
) -> BoundaryClaim {
    BoundaryClaim {
        id: format!("boundary-{}", Uuid::new_v4()),
        provenance_id: None,
        group_id: source_group.to_string(),
        task_id: None,
        quadrant: BoundaryQuadrant::Deontic,
        source: BoundaryClaimSource::Approval,
        content: action_summary.to_string(),
        witness: Some(action_scope.to_string()),
        created_at: created_at.to_string(),
    }
}

fn build_host_os_gate_evaluation(
    id_seed: &str,
    action_summary: &str,
    approved: bool,
    created_at: &str,
) -> GateEvaluation {
    let outcome = if approved {
        GateDecision::Pass
    } else {
        GateDecision::Block
    };
    GateEvaluation {
        id: format!("gate-{id_seed}"),
        requested_lane: None,
        resolved_lane: None,
        decision: outcome,
        checks: vec![GateCheck {
            aspect: GateAspect::Approval,
            scope: GateScope::Locus,
            name: "host-os-control-approval".to_string(),
            outcome,
            rationale: if approved {
                "Host OS control is proceeding through the explicit approval path.".to_string()
            } else {
                "Host OS control requires explicit approval before execution.".to_string()
            },
            evidence: Some(action_summary.to_string()),
        }],
        created_at: created_at.to_string(),
    }
}

fn execute_host_os_control_action(
    db: &NanoclawDb,
    action: HostOsControlAction,
    context: &HostOsControlContext,
    one_time_fingerprint: Option<String>,
    approval_record: Option<&HostOsControlApprovalRequestRecord>,
) -> Result<RunHostOsControlTaskResult> {
    let action_scope = action_scope(&action);
    let action_summary = summarize_action(&action);
    let allowed_decisions = allowed_decisions(&action);
    let provenance_id = format!("host-os-{}", Uuid::new_v4());
    let now = Utc::now().to_rfc3339();
    let task_signature = derive_task_signature(
        &action_summary,
        &[],
        match &action {
            HostOsControlAction::ShellCommand { command, .. } => Some(command.as_str()),
            _ => None,
        },
        &context.request_plane,
        false,
        &now,
    );
    let boundary_claim = approval_record
        .and_then(|record| record.boundary_claim.clone())
        .unwrap_or_else(|| {
            build_host_os_boundary_claim(
                &context.source_group,
                &action_summary,
                &action_scope,
                &now,
            )
        });
    let gate_evaluation = approval_record
        .and_then(|record| record.gate_evaluation.clone())
        .map(|mut evaluation| {
            evaluation.decision = GateDecision::Pass;
            evaluation.checks = vec![GateCheck {
                aspect: GateAspect::Approval,
                scope: GateScope::Locus,
                name: "host-os-control-approved".to_string(),
                outcome: GateDecision::Pass,
                rationale: "Host OS control request was approved before execution.".to_string(),
                evidence: Some(action_summary.clone()),
            }];
            evaluation
        })
        .unwrap_or_else(|| {
            build_host_os_gate_evaluation(&provenance_id, &action_summary, true, &now)
        });
    let assurance = derive_assurance(
        "host_os_control",
        &task_signature,
        Some(&gate_evaluation),
        matches!(action, HostOsControlAction::ShellCommand { .. }),
    );
    db.create_execution_provenance(&build_execution_provenance_record(
        BuildExecutionProvenanceInput {
            id: provenance_id.clone(),
            run_kind: ExecutionRunKind::HostOsControl,
            group_folder: context.source_group.clone(),
            chat_jid: context.chat_jid.clone(),
            execution_location: context.execution_location.clone(),
            request_plane: context.request_plane.clone(),
            effective_capabilities: context.capability_manifest.clone(),
            project_environment_id: context.project_environment_id.clone(),
            mount_summary: mount_summary_for_action(&action),
            secret_handles_used: Vec::new(),
            fallback_reason: None,
            sync_scope: None,
            task_signature: Some(task_signature),
            boundary_claims: vec![boundary_claim],
            gate_evaluation: Some(gate_evaluation),
            assurance: Some(assurance),
            symbol_carriers: Vec::new(),
            provenance_edges: Vec::new(),
            status: ExecutionStatus::Started,
            created_at: now.clone(),
            updated_at: now,
            completed_at: None,
        },
    ))?;

    let execution = execute_action(&action, context.ssh_target.as_ref());
    let completed_at = Utc::now().to_rfc3339();
    consume_transient_approval(
        db,
        approval_record,
        &context.source_group,
        one_time_fingerprint.as_deref(),
    )?;
    match execution {
        Ok(output) => {
            db.update_execution_provenance(
                &provenance_id,
                Some(ExecutionStatus::Success),
                None,
                Some(Some(completed_at)),
            )?;
            Ok(RunHostOsControlTaskResult {
                ok: true,
                executed: true,
                exit_code: 0,
                action,
                action_summary,
                action_scope,
                output: Some(output),
                verified: false,
                verification_details: None,
                approval_required: false,
                approval_request_id: None,
                allowed_decisions,
                error: None,
                provenance_id: Some(provenance_id),
            })
        }
        Err(error) => {
            db.update_execution_provenance(
                &provenance_id,
                Some(ExecutionStatus::Error),
                Some(Some(error.to_string())),
                Some(Some(completed_at)),
            )?;
            Ok(RunHostOsControlTaskResult {
                ok: false,
                executed: true,
                exit_code: 1,
                action,
                action_summary,
                action_scope,
                output: None,
                verified: false,
                verification_details: None,
                approval_required: false,
                approval_request_id: None,
                allowed_decisions,
                error: Some(error.to_string()),
                provenance_id: Some(provenance_id),
            })
        }
    }
}

fn action_fingerprint(action: &HostOsControlAction, context: &HostOsControlContext) -> String {
    let mut hasher = Sha256::new();
    hasher.update(context.source_group.as_bytes());
    hasher.update(b"\n");
    hasher.update(action_kind(action).as_bytes());
    hasher.update(b"\n");
    hasher.update(action_scope(action).as_bytes());
    hex_string(&hasher.finalize())
}

fn allowed_decisions(action: &HostOsControlAction) -> Vec<HostOsControlApprovalDecision> {
    match action {
        HostOsControlAction::ShellCommand { .. } => vec![
            HostOsControlApprovalDecision::Once,
            HostOsControlApprovalDecision::Session,
            HostOsControlApprovalDecision::Deny,
        ],
        _ => vec![
            HostOsControlApprovalDecision::Once,
            HostOsControlApprovalDecision::Session,
            HostOsControlApprovalDecision::Always,
            HostOsControlApprovalDecision::Deny,
        ],
    }
}

fn execute_action(
    action: &HostOsControlAction,
    ssh_target: Option<&SshEndpoint>,
) -> Result<String> {
    if let Some(ssh_target) = ssh_target {
        return execute_remote_action(action, ssh_target);
    }
    execute_local_action(action)
}

fn execute_local_action(action: &HostOsControlAction) -> Result<String> {
    match action {
        HostOsControlAction::OpenApplication { application } => {
            run_command(open_application_command(application)?)
        }
        HostOsControlAction::ActivateApplication { application } => {
            run_command(activate_application_command(application)?)
        }
        HostOsControlAction::OpenUrl { url } => run_command(open_url_command(url)?),
        HostOsControlAction::RevealInFinder { path } => {
            run_command(reveal_in_finder_command(path)?)
        }
        HostOsControlAction::ShellCommand { command, cwd } => {
            let mut cmd = Command::new("/bin/sh");
            cmd.arg("-lc").arg(command);
            if let Some(cwd) = cwd {
                cmd.current_dir(cwd);
            }
            run_command(cmd)
        }
    }
}

fn execute_remote_action(action: &HostOsControlAction, ssh_target: &SshEndpoint) -> Result<String> {
    let command = remote_shell_command_for_action(action)?;
    run_remote_command(ssh_target, &command).with_context(|| {
        format!(
            "failed to execute remote control action on {}",
            ssh_target_display(ssh_target)
        )
    })
}

fn remote_shell_command_for_action(action: &HostOsControlAction) -> Result<String> {
    match action {
        HostOsControlAction::OpenApplication { application } => Ok(format!(
            "if [ \"$(uname -s)\" = \"Darwin\" ]; then /usr/bin/open -a {application}; else echo \"open-application is only supported on macOS remote targets\" >&2; exit 1; fi",
            application = shell_quote(application)
        )),
        HostOsControlAction::ActivateApplication { application } => Ok(format!(
            "if [ \"$(uname -s)\" = \"Darwin\" ]; then /usr/bin/osascript -e {script}; else echo \"activate-application is only supported on macOS remote targets\" >&2; exit 1; fi",
            script = shell_quote(&format!(
                "tell application \"{}\" to activate",
                application.replace('\\', "\\\\").replace('"', "\\\"")
            ))
        )),
        HostOsControlAction::OpenUrl { url } => Ok(format!(
            "if [ \"$(uname -s)\" = \"Darwin\" ]; then /usr/bin/open {url}; elif command -v xdg-open >/dev/null 2>&1; then xdg-open {url}; else echo \"open-url is not supported on this remote target\" >&2; exit 1; fi",
            url = shell_quote(url)
        )),
        HostOsControlAction::RevealInFinder { path } => Ok(format!(
            "if [ \"$(uname -s)\" = \"Darwin\" ]; then /usr/bin/open -R {path}; elif command -v xdg-open >/dev/null 2>&1; then xdg-open {directory}; else echo \"reveal-in-finder is not supported on this remote target\" >&2; exit 1; fi",
            path = shell_quote(path),
            directory = shell_quote(
                Path::new(path)
                    .parent()
                    .unwrap_or_else(|| Path::new(path))
                    .to_string_lossy()
                    .as_ref()
            )
        )),
        HostOsControlAction::ShellCommand { command, cwd } => Ok(match cwd {
            Some(cwd) => format!("cd {} && /bin/sh -lc {}", shell_quote(cwd), shell_quote(command)),
            None => format!("/bin/sh -lc {}", shell_quote(command)),
        }),
    }
}

fn run_command(mut command: Command) -> Result<String> {
    let output = command
        .output()
        .with_context(|| format!("failed to execute {:?}", command))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        bail!(
            "command failed with status {}{}",
            output.status,
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        );
    }
    Ok(if stdout.is_empty() {
        "ok".to_string()
    } else {
        stdout
    })
}

fn mount_summary_for_action(action: &HostOsControlAction) -> Vec<ExecutionMountSummaryEntry> {
    match action {
        HostOsControlAction::RevealInFinder { path } => vec![ExecutionMountSummaryEntry {
            host_path: Some(path.clone()),
            container_path: None,
            readonly: false,
            kind: ExecutionMountKind::Additional,
        }],
        HostOsControlAction::ShellCommand { cwd, .. } => cwd
            .as_ref()
            .map(|cwd| {
                vec![ExecutionMountSummaryEntry {
                    host_path: Some(cwd.clone()),
                    container_path: None,
                    readonly: false,
                    kind: ExecutionMountKind::Project,
                }]
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn resolve_approval_state(
    policy_path: &Path,
    source_group: &str,
    fingerprint: &str,
) -> Result<ApprovalState> {
    let key = grant_key(source_group, fingerprint);
    if one_time_grants().lock().unwrap().contains(&key)
        || session_grants().lock().unwrap().contains(&key)
    {
        return Ok(ApprovalState::Allow);
    }
    if session_denials().lock().unwrap().contains(&key) {
        return Ok(ApprovalState::Deny);
    }

    let policy = load_policy_file(policy_path);
    if policy
        .groups
        .get(source_group)
        .map(|group| {
            group
                .grants
                .iter()
                .any(|grant| grant.fingerprint == fingerprint)
        })
        .unwrap_or(false)
    {
        return Ok(ApprovalState::Allow);
    }

    let default_mode = policy
        .groups
        .get(source_group)
        .and_then(|group| group.default_mode)
        .unwrap_or(policy.default_mode);
    Ok(match default_mode {
        HostOsControlDefaultMode::Ask => ApprovalState::Ask,
        HostOsControlDefaultMode::Deny => ApprovalState::Deny,
    })
}

fn load_policy_file(path: &Path) -> HostOsControlPolicyFile {
    let Ok(raw) = fs::read_to_string(path) else {
        return HostOsControlPolicyFile {
            version: 1,
            default_mode: HostOsControlDefaultMode::Ask,
            groups: std::collections::BTreeMap::new(),
        };
    };
    serde_json::from_str(&raw).unwrap_or(HostOsControlPolicyFile {
        version: 1,
        default_mode: HostOsControlDefaultMode::Ask,
        groups: std::collections::BTreeMap::new(),
    })
}

fn save_policy_file(path: &Path, policy: &HostOsControlPolicyFile) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(path, serde_json::to_string_pretty(policy)?)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn persist_always_grant(
    policy_path: &Path,
    source_group: &str,
    grant: HostOsControlPolicyGrant,
) -> Result<()> {
    let mut policy = load_policy_file(policy_path);
    let group = policy
        .groups
        .entry(source_group.to_string())
        .or_insert_with(HostOsControlGroupPolicy::default);
    group
        .grants
        .retain(|existing| existing.fingerprint != grant.fingerprint);
    group.grants.push(grant);
    save_policy_file(policy_path, &policy)
}

fn one_time_grants() -> &'static Mutex<HashSet<String>> {
    ONE_TIME_GRANTS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn session_grants() -> &'static Mutex<HashSet<String>> {
    SESSION_GRANTS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn session_denials() -> &'static Mutex<HashSet<String>> {
    SESSION_DENIALS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn grant_key(source_group: &str, fingerprint: &str) -> String {
    format!("{source_group}:{fingerprint}")
}

fn consume_one_time_grant(source_group: &str, fingerprint: &str) {
    one_time_grants()
        .lock()
        .unwrap()
        .remove(&grant_key(source_group, fingerprint));
}

fn consume_transient_approval(
    db: &NanoclawDb,
    approval_record: Option<&HostOsControlApprovalRequestRecord>,
    source_group: &str,
    one_time_fingerprint: Option<&str>,
) -> Result<()> {
    if let Some(record) = approval_record {
        if record.resolved_decision == Some(HostOsControlApprovalDecision::Once) {
            db.resolve_host_os_control_approval_request(
                &record.id,
                HostOsControlApprovalStatus::Consumed,
                Some(HostOsControlApprovalDecision::Once),
                record.resolved_at.as_deref(),
            )?;
        }
    }
    if let Some(fingerprint) = one_time_fingerprint {
        consume_one_time_grant(source_group, fingerprint);
    }
    Ok(())
}

fn open_application_command(application: &str) -> Result<Command> {
    if cfg!(target_os = "macos") {
        let mut command = Command::new("/usr/bin/open");
        command.arg("-a").arg(application);
        Ok(command)
    } else {
        bail!("open_application is currently only supported on macOS")
    }
}

fn activate_application_command(application: &str) -> Result<Command> {
    if cfg!(target_os = "macos") {
        let mut command = Command::new("/usr/bin/osascript");
        command
            .arg("-e")
            .arg(format!("tell application \"{}\" to activate", application));
        Ok(command)
    } else {
        bail!("activate_application is currently only supported on macOS")
    }
}

fn open_url_command(url: &str) -> Result<Command> {
    if cfg!(target_os = "macos") {
        let mut command = Command::new("/usr/bin/open");
        command.arg(url);
        Ok(command)
    } else {
        let mut command = Command::new("xdg-open");
        command.arg(url);
        Ok(command)
    }
}

fn reveal_in_finder_command(path: &str) -> Result<Command> {
    if cfg!(target_os = "macos") {
        let mut command = Command::new("/usr/bin/open");
        command.arg("-R").arg(path);
        Ok(command)
    } else {
        let directory = Path::new(path)
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(path));
        let mut command = Command::new("xdg-open");
        command.arg(directory);
        Ok(command)
    }
}

fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempfile::tempdir;

    use super::{
        build_default_context, replay_approved_host_os_control_request,
        reset_host_os_control_state_for_test, resolve_host_os_control_request,
        run_host_os_control_task, HostOsControlAction, HostOsControlApprovalDecision,
        HostOsControlApprovalStatus, RunHostOsControlTaskInput,
    };
    use crate::nanoclaw::db::NanoclawDb;

    #[test]
    fn host_os_requests_require_approval_first() -> Result<()> {
        reset_host_os_control_state_for_test();
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        let policy_path = dir.path().join("host-os-control-policy.json");
        let context = build_default_context("main-requires-approval", &policy_path);

        let result = run_host_os_control_task(
            &db,
            RunHostOsControlTaskInput {
                action: HostOsControlAction::ShellCommand {
                    command: "printf ok".to_string(),
                    cwd: None,
                },
            },
            &context,
        )?;

        assert!(!result.executed);
        assert!(result.approval_required);
        assert_eq!(
            db.list_host_os_control_approval_requests(None, None, 10)?
                .len(),
            1
        );
        Ok(())
    }

    #[test]
    fn approved_host_os_request_can_be_replayed() -> Result<()> {
        reset_host_os_control_state_for_test();
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        let policy_path = dir.path().join("host-os-control-policy.json");
        let context = build_default_context("main-replay-once", &policy_path);

        let initial = run_host_os_control_task(
            &db,
            RunHostOsControlTaskInput {
                action: HostOsControlAction::ShellCommand {
                    command: "printf ok".to_string(),
                    cwd: None,
                },
            },
            &context,
        )?;
        let request_id = initial.approval_request_id.unwrap();
        let resolved = resolve_host_os_control_request(
            &db,
            &policy_path,
            &request_id,
            HostOsControlApprovalDecision::Once,
        )?;
        assert_eq!(resolved.status.as_str(), "approved");

        reset_host_os_control_state_for_test();

        let replay = replay_approved_host_os_control_request(&db, &request_id, &context)?;
        assert!(replay.executed);
        assert_eq!(replay.output.as_deref(), Some("ok"));
        assert_eq!(
            db.list_execution_provenance(Some("main-replay-once"), 10)?
                .len(),
            1
        );
        let consumed = db
            .get_host_os_control_approval_request(&request_id)?
            .expect("approval request should still exist");
        assert_eq!(consumed.status, HostOsControlApprovalStatus::Consumed);
        Ok(())
    }

    #[test]
    fn once_approval_cannot_be_replayed_twice() -> Result<()> {
        reset_host_os_control_state_for_test();
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        let policy_path = dir.path().join("host-os-control-policy.json");
        let context = build_default_context("main-replay-consumed", &policy_path);

        let initial = run_host_os_control_task(
            &db,
            RunHostOsControlTaskInput {
                action: HostOsControlAction::ShellCommand {
                    command: "printf ok".to_string(),
                    cwd: None,
                },
            },
            &context,
        )?;
        let request_id = initial.approval_request_id.unwrap();
        resolve_host_os_control_request(
            &db,
            &policy_path,
            &request_id,
            HostOsControlApprovalDecision::Once,
        )?;
        reset_host_os_control_state_for_test();
        let replay = replay_approved_host_os_control_request(&db, &request_id, &context)?;
        assert!(replay.executed);
        let second = replay_approved_host_os_control_request(&db, &request_id, &context);
        assert!(second.is_err());
        Ok(())
    }
}
