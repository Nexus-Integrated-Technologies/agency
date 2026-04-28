use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::foundation::{
    AssuranceTuple, BoundaryClaim, CapabilityManifest, ContextLoader, ExecutionBoundary,
    ExecutionBoundaryKind, ExecutionLane, ExecutionLocation, ExecutionMountKind,
    ExecutionMountSummaryEntry, ExecutionProvenanceRecord, ExecutionRunKind, ExecutionStatus,
    GateEvaluation, Group, HarnessRouter, MessageRecord, Objective, Plan, PlanStep, PlanStepStatus,
    RemoteWorkerMode, RequestPlane, RoutingDecision, RoutingInput, SessionRole, SessionState,
    SessionStore, TaskKind, TaskSignature,
};

use super::config::NanoclawConfig;
use super::dev_environment::DigitalOceanDevEnvironment;
use super::fpf_bridge::{
    build_boundary_claims, derive_assurance, derive_provenance_edges, derive_symbol_carriers,
    derive_task_signature, evaluate_execution_gate, now_iso,
};
use super::model_router::{resolve_worker_backend, ResolvedWorkerBackend, WorkerBackend};
use super::omx::{OmxExecutionOptions, OmxRunnerClient};
use super::request_plane::{get_request_plane_env, get_request_plane_text_error};
use super::security_profile::{
    build_execution_provenance_record, derive_capability_manifest, get_capability_manifest_env,
    BuildExecutionProvenanceInput, DeriveCapabilityManifestInput,
};

const WORKER_START_TIMEOUT: Duration = Duration::from_secs(5);
const WORKER_CONNECT_RETRY: Duration = Duration::from_millis(50);
const DEFAULT_WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_WORKER_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
const SOURCE_CRATE_ROOT: &str = env!("CARGO_MANIFEST_DIR");
const DEFAULT_CONTAINER_CACHE_DIR: &str = "/tmp/nanoclaw-target";
const DEFAULT_CODEX_SANDBOX: &str = "workspace-write";
const DEFAULT_WORKERS_AI_PROXY_URL: &str = "https://lab.bybuddha.dev/paperclip.ai";

fn summarize_workers_ai_body(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return "empty response body".to_string();
    }
    const MAX_BYTES: usize = 1_024;
    if trimmed.len() <= MAX_BYTES {
        return trimmed.to_string();
    }
    let mut end = MAX_BYTES;
    while end > 0 && !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}... [truncated {} bytes]",
        &trimmed[..end],
        trimmed.len() - end
    )
}

fn normalize_workers_ai_proxy_url(raw_proxy_url: &str) -> Result<String> {
    let raw = raw_proxy_url.trim();
    let mut parsed =
        reqwest::Url::parse(raw).context("PAPERCLIP_WORKERS_AI_PROXY_URL must be a valid URL")?;

    let path_segments = parsed
        .path()
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let mut normalized_segments = path_segments;

    while normalized_segments.len() > 1
        && normalized_segments.last() == Some(&"tasks")
        && normalized_segments[normalized_segments.len() - 2] == "tasks"
    {
        normalized_segments.pop();
    }

    if normalized_segments.last() != Some(&"tasks") {
        normalized_segments.push("tasks");
    }

    let next_path = if normalized_segments.is_empty() {
        "/tasks".to_string()
    } else {
        format!("/{}", normalized_segments.join("/"))
    };
    parsed.set_path(&next_path);

    Ok(parsed.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionSession {
    pub id: String,
    pub group_folder: String,
    pub workspace_root: String,
    pub session_root: String,
    pub ipc_root: String,
    pub state_root: String,
    pub logs_root: String,
}

impl ExecutionSession {
    pub fn ensure_layout(&self) -> Result<()> {
        for dir in [
            &self.session_root,
            &self.ipc_root,
            &self.state_root,
            &self.logs_root,
        ] {
            fs::create_dir_all(Path::new(dir))
                .with_context(|| format!("failed to create {}", dir))?;
        }
        Ok(())
    }

    pub fn socket_path(&self) -> PathBuf {
        std::env::temp_dir().join(format!("ncl-{}.sock", compact_session_suffix(&self.id)))
    }

    pub fn pid_path(&self) -> PathBuf {
        Path::new(&self.state_root).join("worker.pid")
    }

    pub fn daemon_stdout_path(&self) -> PathBuf {
        Path::new(&self.logs_root).join("daemon.stdout.log")
    }

    pub fn daemon_stderr_path(&self) -> PathBuf {
        Path::new(&self.logs_root).join("daemon.stderr.log")
    }
}

fn compact_session_suffix(session_id: &str) -> String {
    let compact = session_id.replace('-', "");
    let compact = compact
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>();
    if compact.len() <= 12 {
        compact
    } else {
        compact[compact.len() - 12..].to_string()
    }
}

pub fn build_execution_session(
    data_dir: &Path,
    group_folder: &str,
    session_id: &str,
    workspace_root: &Path,
) -> ExecutionSession {
    let session_root = data_dir.join("executor").join("sessions").join(session_id);
    ExecutionSession {
        id: session_id.to_string(),
        group_folder: group_folder.to_string(),
        workspace_root: workspace_root.display().to_string(),
        session_root: session_root.display().to_string(),
        ipc_root: session_root.join("ipc").display().to_string(),
        state_root: session_root.join("state").display().to_string(),
        logs_root: session_root.join("logs").display().to_string(),
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    pub group: Group,
    pub prompt: String,
    pub paperclip_overlay_context: Option<String>,
    pub messages: Vec<MessageRecord>,
    pub task_id: Option<String>,
    pub script: Option<String>,
    pub omx: Option<OmxExecutionOptions>,
    pub assistant_name: String,
    pub request_plane: RequestPlane,
    pub env: BTreeMap<String, String>,
    pub session: ExecutionSession,
    pub backend_override: Option<WorkerBackend>,
    pub task_signature: Option<TaskSignature>,
    pub routing_decision: Option<RoutingDecision>,
    pub objective: Option<Objective>,
    pub plan: Option<Plan>,
    pub boundary_claims: Vec<BoundaryClaim>,
    pub gate_evaluation: Option<GateEvaluation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ExecutionArtifactRef {
    pub kind: String,
    pub title: String,
    pub location: Option<String>,
    pub body: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ExecutionUsageSummary {
    pub input_tokens: usize,
    pub cached_input_tokens: usize,
    pub output_tokens: usize,
}

impl ExecutionUsageSummary {
    fn is_empty(&self) -> bool {
        self.input_tokens == 0 && self.cached_input_tokens == 0 && self.output_tokens == 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ExecutionMetadata {
    pub backend: Option<String>,
    pub provider: Option<String>,
    pub biller: Option<String>,
    pub billing_type: Option<String>,
    pub model: Option<String>,
    pub usage: Option<ExecutionUsageSummary>,
    pub cost_usd: Option<f64>,
    pub routing_decision: Option<RoutingDecision>,
    pub objective: Option<Objective>,
    pub plan: Option<Plan>,
    pub session_state: Option<SessionState>,
    pub gate_evaluation: Option<GateEvaluation>,
    pub assurance: Option<AssuranceTuple>,
    pub status: Option<String>,
    pub question: Option<String>,
    pub tmux_session: Option<String>,
    pub team_name: Option<String>,
    pub summary: Option<String>,
    pub artifacts: Vec<ExecutionArtifactRef>,
    pub external_run_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionResponse {
    pub text: String,
    pub boundary: ExecutionBoundary,
    pub session_id: String,
    pub log_path: Option<String>,
    pub log_body: Option<String>,
    pub provenance: Option<ExecutionProvenanceRecord>,
    pub metadata: Option<ExecutionMetadata>,
}

pub trait ExecutorBoundary {
    fn execute(&self, request: ExecutionRequest) -> Result<ExecutionResponse>;
}

#[derive(Debug, Clone)]
pub struct RustSubprocessExecutor {
    worker_binary: PathBuf,
}

impl RustSubprocessExecutor {
    pub fn new() -> Result<Self> {
        let worker_binary = std::env::var_os("NANOCLAW_EXECUTOR_BINARY")
            .map(PathBuf::from)
            .unwrap_or(std::env::current_exe().context("failed to resolve current executable")?);
        Ok(Self { worker_binary })
    }

    pub fn with_worker_binary(worker_binary: impl Into<PathBuf>) -> Self {
        Self {
            worker_binary: worker_binary.into(),
        }
    }

    fn connect_or_start_daemon(&self, session: &ExecutionSession) -> Result<UnixStream> {
        if let Ok(stream) = connect_to_worker_socket(session) {
            return Ok(stream);
        }

        cleanup_stale_worker_state(session)?;
        self.spawn_daemon(session)?;
        wait_for_worker_socket(session)
    }

    fn spawn_daemon(&self, session: &ExecutionSession) -> Result<()> {
        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(session.daemon_stdout_path())
            .with_context(|| {
                format!("failed to open {}", session.daemon_stdout_path().display())
            })?;
        let stderr = OpenOptions::new()
            .create(true)
            .append(true)
            .open(session.daemon_stderr_path())
            .with_context(|| {
                format!("failed to open {}", session.daemon_stderr_path().display())
            })?;

        Command::new(&self.worker_binary)
            .arg("exec-worker-daemon")
            .arg(&session.session_root)
            .current_dir(Path::new(&session.workspace_root))
            .env("NANOCLAW_EXECUTION_LOCATION", "host")
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .with_context(|| format!("failed to spawn worker {}", self.worker_binary.display()))?;
        Ok(())
    }
}

impl ExecutorBoundary for RustSubprocessExecutor {
    fn execute(&self, request: ExecutionRequest) -> Result<ExecutionResponse> {
        request.session.ensure_layout()?;
        let payload = build_worker_request(request);
        let mut stream = self.connect_or_start_daemon(&payload.session)?;
        write_json(&mut stream, &payload)?;
        stream
            .shutdown(Shutdown::Write)
            .context("failed to close worker socket write side")?;
        let outcome: WorkerOutcome = read_json(&mut stream)?;
        decode_worker_outcome(outcome)
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionLaneRouter {
    configured_lane: ExecutionLane,
    container_groups: Vec<String>,
    remote_worker_mode: RemoteWorkerMode,
    host: RustSubprocessExecutor,
    container: ContainerExecutor,
    remote: RemoteWorkerExecutor,
    omx: OmxExecutor,
}

impl ExecutionLaneRouter {
    pub fn from_config(
        config: &NanoclawConfig,
        lane_override: Option<ExecutionLane>,
    ) -> Result<Self> {
        Ok(Self {
            configured_lane: lane_override.unwrap_or_else(|| config.execution_lane.clone()),
            container_groups: config.container_groups.clone(),
            remote_worker_mode: config.remote_worker_mode.clone(),
            host: RustSubprocessExecutor::new()?,
            container: ContainerExecutor::from_config(config),
            remote: RemoteWorkerExecutor::from_config(config),
            omx: OmxExecutor::from_config(config),
        })
    }

    fn lane_for_group(&self, group: &Group) -> ExecutionLane {
        match &self.configured_lane {
            ExecutionLane::Auto => {
                if should_use_container_lane(&self.container_groups, group) {
                    ExecutionLane::Container
                } else if should_use_remote_lane(&self.remote_worker_mode, group) {
                    ExecutionLane::RemoteWorker
                } else {
                    ExecutionLane::Host
                }
            }
            lane => lane.clone(),
        }
    }

    fn requested_lane_for_request(&self, request: &ExecutionRequest) -> ExecutionLane {
        if request.omx.is_some() {
            ExecutionLane::Omx
        } else if matches!(self.configured_lane, ExecutionLane::Auto) {
            if should_use_container_lane(&self.container_groups, &request.group) {
                ExecutionLane::Container
            } else if should_use_remote_lane(&self.remote_worker_mode, &request.group) {
                ExecutionLane::RemoteWorker
            } else {
                routing_preferred_lane(request.routing_decision.as_ref())
                    .unwrap_or_else(|| self.lane_for_group(&request.group))
            }
        } else {
            self.lane_for_group(&request.group)
        }
    }

    fn decorate_request(&self, mut request: ExecutionRequest) -> ExecutionRequest {
        let created_at = now_iso();
        if request.routing_decision.is_none() {
            request.routing_decision = Some(derive_routing_decision(
                &request.prompt,
                &request.request_plane,
                Path::new(&request.session.workspace_root),
                request.omx.as_ref().map(|_| ExecutionLane::Omx),
            ));
        }
        if request.task_signature.is_none() {
            request.task_signature = Some(derive_task_signature(
                &request.prompt,
                &request.messages,
                request.script.as_deref(),
                &request.request_plane,
                request.omx.is_some(),
                &created_at,
            ));
        }
        if request.objective.is_none() {
            if let (Some(signature), Some(routing_decision)) = (
                request.task_signature.as_ref(),
                request.routing_decision.as_ref(),
            ) {
                request.objective = Some(build_execution_objective(
                    &request.prompt,
                    &request.assistant_name,
                    signature,
                    routing_decision,
                ));
            }
        }
        if request.plan.is_none() {
            if let (Some(objective), Some(routing_decision)) = (
                request.objective.as_ref(),
                request.routing_decision.as_ref(),
            ) {
                request.plan = Some(build_execution_plan(objective, routing_decision));
            }
        }
        if request.boundary_claims.is_empty() {
            request.boundary_claims = build_boundary_claims(
                &request.group.folder,
                request.task_id.as_deref(),
                &request.prompt,
                &request.request_plane,
                &created_at,
            );
        }
        if request.gate_evaluation.is_none() {
            let requested_lane = self.requested_lane_for_request(&request);
            if let Some(signature) = request.task_signature.as_ref() {
                request.gate_evaluation = Some(evaluate_execution_gate(
                    signature,
                    &request.boundary_claims,
                    requested_lane,
                    Path::new(&request.session.workspace_root),
                    request.omx.is_some(),
                    &created_at,
                ));
            }
        }
        request
    }

    fn lane_for_request(&self, request: &ExecutionRequest) -> ExecutionLane {
        request
            .gate_evaluation
            .as_ref()
            .and_then(|evaluation| evaluation.resolved_lane.clone())
            .unwrap_or_else(|| self.requested_lane_for_request(request))
    }
}

impl ExecutorBoundary for ExecutionLaneRouter {
    fn execute(&self, request: ExecutionRequest) -> Result<ExecutionResponse> {
        let request = self.decorate_request(request);
        match self.lane_for_request(&request) {
            ExecutionLane::Auto | ExecutionLane::Host => self.host.execute(request),
            ExecutionLane::Container => self.container.execute(request),
            ExecutionLane::RemoteWorker => self.remote.execute(request),
            ExecutionLane::Omx => self.omx.execute(request),
            ExecutionLane::Custom(value) => {
                bail!("unsupported execution lane '{}'", value)
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ContainerExecutor {
    source_crate_root: PathBuf,
    container_image: String,
    container_runtime: Option<String>,
    target_cache_root: PathBuf,
}

impl ContainerExecutor {
    fn from_config(config: &NanoclawConfig) -> Self {
        Self {
            source_crate_root: PathBuf::from(SOURCE_CRATE_ROOT),
            container_image: config.container_image.clone(),
            container_runtime: config.container_runtime.clone(),
            target_cache_root: config
                .data_dir
                .join("executor")
                .join("container-cache")
                .join("target"),
        }
    }
}

impl ExecutorBoundary for ContainerExecutor {
    fn execute(&self, request: ExecutionRequest) -> Result<ExecutionResponse> {
        request.session.ensure_layout()?;
        fs::create_dir_all(&self.target_cache_root).with_context(|| {
            format!(
                "failed to create container cache {}",
                self.target_cache_root.display()
            )
        })?;

        let payload = build_worker_request(request);
        let runtime = resolve_container_runtime(self.container_runtime.as_deref())?;
        let mut command = Command::new(&runtime);
        command
            .arg("run")
            .arg("--rm")
            .arg("-i")
            .arg("-v")
            .arg(format!(
                "{}:{}:ro",
                self.source_crate_root.display(),
                self.source_crate_root.display()
            ))
            .arg("-v")
            .arg(format!(
                "{}:{}",
                payload.session.workspace_root, payload.session.workspace_root
            ))
            .arg("-v")
            .arg(format!(
                "{}:{}",
                payload.session.session_root, payload.session.session_root
            ))
            .arg("-v")
            .arg(format!(
                "{}:{}",
                self.target_cache_root.display(),
                DEFAULT_CONTAINER_CACHE_DIR
            ))
            .arg("-e")
            .arg(format!("CARGO_TARGET_DIR={DEFAULT_CONTAINER_CACHE_DIR}"))
            .arg("-w")
            .arg(self.source_crate_root.display().to_string())
            .arg("-e")
            .arg("NANOCLAW_EXECUTION_LOCATION=local_container")
            .arg(&self.container_image)
            .arg("cargo")
            .arg("run")
            .arg("--quiet")
            .arg("--manifest-path")
            .arg(self.source_crate_root.join("Cargo.toml"))
            .arg("--bin")
            .arg("nanoclaw")
            .arg("--")
            .arg("exec-worker-stdio");

        let mut response = run_worker_command(&mut command, &payload)?;
        response.boundary = ExecutionBoundary {
            kind: ExecutionBoundaryKind::Container,
            root: Some(payload.session.workspace_root.clone()),
            isolated: true,
        };
        Ok(response)
    }
}

#[derive(Debug, Clone)]
struct RemoteWorkerExecutor {
    dev_environment: DigitalOceanDevEnvironment,
    remote_worker_root: String,
    remote_worker_binary: String,
    remote_manifest_path: String,
}

impl RemoteWorkerExecutor {
    fn from_config(config: &NanoclawConfig) -> Self {
        let remote_manifest_path = detect_remote_manifest_path(&config.project_root)
            .unwrap_or_else(|| "Cargo.toml".to_string());
        Self {
            dev_environment: DigitalOceanDevEnvironment::from_config(config),
            remote_worker_root: config.remote_worker_root.clone(),
            remote_worker_binary: config.remote_worker_binary.clone(),
            remote_manifest_path,
        }
    }
}

impl ExecutorBoundary for RemoteWorkerExecutor {
    fn execute(&self, request: ExecutionRequest) -> Result<ExecutionResponse> {
        request.session.ensure_layout()?;
        self.dev_environment.sync_project()?;
        self.dev_environment.sync_group_workspace(
            Path::new(&request.session.workspace_root),
            &request.group.folder,
        )?;

        let payload = build_worker_request(remoteize_request(&self.remote_worker_root, request));
        let command = format!(
            "if [ -x {binary} ]; then NANOCLAW_EXECUTION_LOCATION=remote_worker {binary} exec-worker-stdio; else NANOCLAW_EXECUTION_LOCATION=remote_worker cargo run --quiet --manifest-path {manifest} --bin nanoclaw -- exec-worker-stdio; fi",
            binary = shell_quote(&self.remote_worker_binary),
            manifest = shell_quote(&self.remote_manifest_path)
        );
        let stdin =
            serde_json::to_vec(&payload).context("failed to encode remote worker request")?;
        let result = self.dev_environment.exec_with_stdin(&command, &stdin)?;
        let outcome: WorkerOutcome = serde_json::from_str(&result.stdout)
            .context("failed to parse remote worker response")?;
        let mut response = decode_worker_outcome(outcome)?;
        response.boundary = ExecutionBoundary {
            kind: ExecutionBoundaryKind::RemoteWorker,
            root: Some(payload.session.workspace_root.clone()),
            isolated: true,
        };
        Ok(response)
    }
}

#[derive(Debug, Clone)]
struct OmxExecutor {
    client: OmxRunnerClient,
}

impl OmxExecutor {
    fn from_config(config: &NanoclawConfig) -> Self {
        Self {
            client: OmxRunnerClient::from_config(config),
        }
    }
}

impl ExecutorBoundary for OmxExecutor {
    fn execute(&self, request: ExecutionRequest) -> Result<ExecutionResponse> {
        request.session.ensure_layout()?;
        let workspace_root = PathBuf::from(&request.session.workspace_root);
        let created_at = now_iso();
        let session_store = SessionStore::new(session_state_path(&request.session));
        let mut session_state = session_store.load().unwrap_or_default();
        record_execution_request_session_turn(&mut session_state, &request);
        let task_signature = request.task_signature.clone().unwrap_or_else(|| {
            derive_task_signature(
                &request.prompt,
                &request.messages,
                request.script.as_deref(),
                &request.request_plane,
                true,
                &created_at,
            )
        });
        let boundary_claims = if request.boundary_claims.is_empty() {
            build_boundary_claims(
                &request.group.folder,
                request.task_id.as_deref(),
                &request.prompt,
                &request.request_plane,
                &created_at,
            )
        } else {
            request.boundary_claims.clone()
        };
        let gate_evaluation = request.gate_evaluation.clone().or_else(|| {
            Some(evaluate_execution_gate(
                &task_signature,
                &boundary_claims,
                ExecutionLane::Omx,
                workspace_root.as_path(),
                true,
                &created_at,
            ))
        });
        let response = self.client.invoke(
            &request.group.folder,
            Some(&request.group.jid),
            None,
            &request.prompt,
            workspace_root.as_path(),
            request.omx.as_ref(),
        )?;
        record_response_session_turn(&mut session_state, &response.summary);
        session_state.last_plan = request.plan.clone();
        session_store.save(&session_state)?;
        let now = Utc::now().to_rfc3339();
        let assurance = derive_assurance("omx", &task_signature, gate_evaluation.as_ref(), false);
        let symbol_carriers = derive_symbol_carriers(
            &response.session_id,
            &request.session,
            response.log_path.as_deref().map(Path::new),
        );
        let provenance_edges =
            derive_provenance_edges(&response.session_id, &symbol_carriers, &boundary_claims);
        let provenance = build_execution_provenance_record(BuildExecutionProvenanceInput {
            id: format!("omx-{}", Uuid::new_v4()),
            run_kind: ExecutionRunKind::Omx,
            group_folder: request.group.folder.clone(),
            chat_jid: Some(request.group.jid.clone()),
            execution_location: ExecutionLocation::Omx,
            request_plane: request.request_plane.clone(),
            effective_capabilities: derive_capability_manifest(
                &request.request_plane,
                DeriveCapabilityManifestInput {
                    allow_repo_sync: true,
                    allow_ssh: true,
                    ..Default::default()
                },
            ),
            project_environment_id: None,
            mount_summary: vec![ExecutionMountSummaryEntry {
                host_path: Some(workspace_root.display().to_string()),
                container_path: None,
                readonly: false,
                kind: ExecutionMountKind::Project,
            }],
            secret_handles_used: Vec::new(),
            fallback_reason: Some("omx".to_string()),
            sync_scope: None,
            task_signature: Some(task_signature.clone()),
            boundary_claims: boundary_claims.clone(),
            gate_evaluation: gate_evaluation.clone(),
            assurance: Some(assurance.clone()),
            symbol_carriers: symbol_carriers.clone(),
            provenance_edges: provenance_edges.clone(),
            status: if matches!(response.status, super::omx::OmxSessionStatus::Failed) {
                ExecutionStatus::Error
            } else {
                ExecutionStatus::Success
            },
            created_at: now.clone(),
            updated_at: now.clone(),
            completed_at: response.status.is_terminal().then_some(now.clone()),
        });

        Ok(ExecutionResponse {
            text: response.summary.clone(),
            boundary: ExecutionBoundary {
                kind: ExecutionBoundaryKind::Omx,
                root: Some(response.workspace_root.clone()),
                isolated: true,
            },
            session_id: response.session_id,
            log_path: response.log_path,
            log_body: response
                .log_body
                .clone()
                .or_else(|| Some(response.summary.clone())),
            provenance: Some(provenance),
            metadata: Some(ExecutionMetadata {
                backend: Some("omx".to_string()),
                provider: None,
                biller: None,
                billing_type: None,
                model: None,
                usage: None,
                cost_usd: None,
                routing_decision: request.routing_decision.clone(),
                objective: request.objective.clone(),
                plan: request.plan.clone(),
                session_state: Some(compact_session_state(&session_state, 6)),
                gate_evaluation,
                assurance: Some(assurance),
                status: Some(response.status.as_str().to_string()),
                question: response.question.clone(),
                tmux_session: response.tmux_session.clone(),
                team_name: response.team_name.clone(),
                summary: Some(response.summary.clone()),
                artifacts: response
                    .artifacts
                    .into_iter()
                    .map(|artifact| ExecutionArtifactRef {
                        kind: artifact.kind,
                        title: artifact.title,
                        location: artifact.location,
                        body: artifact.body,
                    })
                    .collect(),
                external_run_id: response.external_run_id.clone(),
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct InProcessEchoExecutor {
    assistant_name: String,
}

impl InProcessEchoExecutor {
    pub fn new(assistant_name: impl Into<String>) -> Self {
        Self {
            assistant_name: assistant_name.into(),
        }
    }
}

impl ExecutorBoundary for InProcessEchoExecutor {
    fn execute(&self, request: ExecutionRequest) -> Result<ExecutionResponse> {
        let latest = request.messages.last();
        let latest_summary = latest
            .map(|message| {
                format!(
                    "Latest from {} at {}: {}",
                    message.sender_name.as_deref().unwrap_or(&message.sender),
                    message.timestamp,
                    message.content
                )
            })
            .unwrap_or_else(|| "No inbound messages were provided.".to_string());

        Ok(ExecutionResponse {
            text: format!(
                "{} received {} message(s) for group '{}'.\n{}\nPrompt bytes: {}\nHandled at {}.",
                self.assistant_name,
                request.messages.len(),
                request.group.name,
                latest_summary,
                request.prompt.len(),
                Utc::now().to_rfc3339()
            ),
            boundary: ExecutionBoundary {
                kind: ExecutionBoundaryKind::InProcess,
                root: None,
                isolated: false,
            },
            session_id: request.session.id,
            log_path: None,
            log_body: None,
            provenance: None,
            metadata: None,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerRequest {
    invocation_id: String,
    requested_at: String,
    group: Group,
    prompt: String,
    paperclip_overlay_context: Option<String>,
    messages: Vec<MessageRecord>,
    task_id: Option<String>,
    script: Option<String>,
    omx: Option<OmxExecutionOptions>,
    assistant_name: String,
    request_plane: RequestPlane,
    env: BTreeMap<String, String>,
    session: ExecutionSession,
    backend_override: Option<WorkerBackend>,
    task_signature: Option<TaskSignature>,
    routing_decision: Option<RoutingDecision>,
    objective: Option<Objective>,
    plan: Option<Plan>,
    boundary_claims: Vec<BoundaryClaim>,
    gate_evaluation: Option<GateEvaluation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerResponse {
    text: String,
    boundary: ExecutionBoundary,
    session_id: String,
    log_path: Option<String>,
    log_body: Option<String>,
    provenance: Option<ExecutionProvenanceRecord>,
    metadata: Option<ExecutionMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerOutcome {
    response: Option<WorkerResponse>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionHistoryEntry {
    invocation_id: String,
    requested_at: String,
    message_count: usize,
    prompt_bytes: usize,
    script: Option<String>,
    success: bool,
}

#[derive(Debug, Clone)]
struct BackendExecutionMetadata {
    backend: WorkerBackend,
    provider: Option<String>,
    biller: Option<String>,
    billing_type: Option<String>,
    model: Option<String>,
    usage: Option<ExecutionUsageSummary>,
    cost_usd: Option<f64>,
    effective_capabilities: CapabilityManifest,
    project_environment_id: Option<String>,
    secret_handles: Vec<String>,
    mount_summary: Vec<ExecutionMountSummaryEntry>,
    fallback_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct BackendExecutionResult {
    text: String,
    log_body: String,
    metadata: BackendExecutionMetadata,
}

#[derive(Debug, Default)]
struct CodexJsonlParseResult {
    session_id: Option<String>,
    model: Option<String>,
    summary: String,
    usage: Option<ExecutionUsageSummary>,
    cost_usd: Option<f64>,
    error_message: Option<String>,
}

pub fn run_worker_from_paths(request_path: &Path, response_path: &Path) -> Result<()> {
    let bytes = fs::read(request_path)
        .with_context(|| format!("failed to read {}", request_path.display()))?;
    let request: WorkerRequest = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", request_path.display()))?;
    let response = execute_worker_request(request)?;
    fs::write(response_path, serde_json::to_vec_pretty(&response)?)
        .with_context(|| format!("failed to write {}", response_path.display()))?;
    Ok(())
}

pub fn run_worker_stdio() -> Result<()> {
    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let request: WorkerRequest = read_json(&mut reader)?;
    let outcome = match execute_worker_request(request) {
        Ok(response) => WorkerOutcome {
            response: Some(response),
            error: None,
        },
        Err(error) => WorkerOutcome {
            response: None,
            error: Some(error.to_string()),
        },
    };
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    write_json(&mut writer, &outcome)
}

pub fn run_worker_daemon(session_root: &Path) -> Result<()> {
    run_worker_daemon_with_idle_timeout(session_root, worker_idle_timeout())
}

fn run_worker_daemon_with_idle_timeout(session_root: &Path, idle_timeout: Duration) -> Result<()> {
    let session_root = session_root.to_path_buf();
    let ipc_root = session_root.join("ipc");
    let state_root = session_root.join("state");
    let session_id = session_root
        .file_name()
        .and_then(|value| value.to_str())
        .context("failed to derive session id from session root")?;
    let socket_path =
        std::env::temp_dir().join(format!("ncl-{}.sock", compact_session_suffix(session_id)));
    let pid_path = state_root.join("worker.pid");

    fs::create_dir_all(&ipc_root)
        .with_context(|| format!("failed to create {}", ipc_root.display()))?;
    fs::create_dir_all(&state_root)
        .with_context(|| format!("failed to create {}", state_root.display()))?;
    if socket_path.exists() {
        let _ = fs::remove_file(&socket_path);
    }

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("failed to bind {}", socket_path.display()))?;
    listener
        .set_nonblocking(true)
        .context("failed to set worker listener nonblocking")?;
    fs::write(&pid_path, format!("{}\n", std::process::id()))
        .with_context(|| format!("failed to write {}", pid_path.display()))?;

    let mut last_activity = Instant::now();
    loop {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .context("failed to configure worker stream blocking mode")?;
                last_activity = Instant::now();
                handle_daemon_connection(&mut stream)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if last_activity.elapsed() >= idle_timeout {
                    break;
                }
                thread::sleep(WORKER_CONNECT_RETRY);
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("worker listener failed at {}", socket_path.display())
                })
            }
        }
    }

    let _ = fs::remove_file(&socket_path);
    let _ = fs::remove_file(&pid_path);
    Ok(())
}

fn execute_worker_request(request: WorkerRequest) -> Result<WorkerResponse> {
    let mut request = request;
    request.session.ensure_layout()?;

    let workspace_root = PathBuf::from(&request.session.workspace_root);
    let history_path = Path::new(&request.session.state_root).join("history.jsonl");
    let prior_turns = count_history_entries(&history_path)?;
    let log_path =
        Path::new(&request.session.logs_root).join(format!("{}.log", request.invocation_id));
    let instruction_hint = read_workspace_hint(&workspace_root)?;
    let execution_location = current_execution_location();
    let resolved_backend = resolve_worker_backend(&workspace_root, &request.session.group_folder)?;
    let mut resolved_backend = resolved_backend;
    if let Some(backend_override) = request.backend_override.clone() {
        resolved_backend.backend = backend_override;
    }
    if request.task_signature.is_none() {
        request.task_signature = Some(derive_task_signature(
            &request.prompt,
            &request.messages,
            request.script.as_deref(),
            &request.request_plane,
            request.omx.is_some(),
            &request.requested_at,
        ));
    }
    if request.routing_decision.is_none() {
        request.routing_decision = Some(derive_routing_decision(
            &request.prompt,
            &request.request_plane,
            workspace_root.as_path(),
            request.omx.as_ref().map(|_| ExecutionLane::Omx),
        ));
    }
    if request.objective.is_none() {
        if let (Some(signature), Some(routing_decision)) = (
            request.task_signature.as_ref(),
            request.routing_decision.as_ref(),
        ) {
            request.objective = Some(build_execution_objective(
                &request.prompt,
                &request.assistant_name,
                signature,
                routing_decision,
            ));
        }
    }
    if request.plan.is_none() {
        if let (Some(objective), Some(routing_decision)) = (
            request.objective.as_ref(),
            request.routing_decision.as_ref(),
        ) {
            request.plan = Some(build_execution_plan(objective, routing_decision));
        }
    }
    if request.boundary_claims.is_empty() {
        request.boundary_claims = build_boundary_claims(
            &request.group.folder,
            request.task_id.as_deref(),
            &request.prompt,
            &request.request_plane,
            &request.requested_at,
        );
    }
    if request.gate_evaluation.is_none() {
        if let Some(signature) = request.task_signature.as_ref() {
            request.gate_evaluation = Some(evaluate_execution_gate(
                signature,
                &request.boundary_claims,
                ExecutionLane::Host,
                workspace_root.as_path(),
                request.omx.is_some(),
                &request.requested_at,
            ));
        }
    }
    let session_store = SessionStore::new(session_state_path(&request.session));
    let mut session_state = session_store.load().unwrap_or_default();
    record_request_session_turn(&mut session_state, &request);
    if request.plan.is_some() {
        session_state.last_plan = request.plan.clone();
    }

    let result = if let Some(script) = request
        .script
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        run_script_request(
            &request,
            script,
            &workspace_root,
            prior_turns,
            &instruction_hint,
            execution_location.clone(),
        )
    } else {
        match &resolved_backend.backend {
            WorkerBackend::Summary => Ok(build_message_response(
                &request,
                prior_turns,
                &instruction_hint,
                workspace_root.as_path(),
                execution_location.clone(),
            )),
            WorkerBackend::Codex => {
                match run_codex_request(
                    &request,
                    workspace_root.as_path(),
                    prior_turns,
                    &instruction_hint,
                    &session_state,
                    &resolved_backend,
                    execution_location.clone(),
                ) {
                    Ok(res) => Ok(res),
                    Err(err) => {
                        let err_text = err.to_string();
                        if is_codex_usage_limit_error(&err_text) {
                            run_workers_ai_request(
                                &request,
                                workspace_root.as_path(),
                                prior_turns,
                                &instruction_hint,
                                &session_state,
                                &resolved_backend,
                                execution_location.clone(),
                                Some(format!("codex_fallback: {}", err_text)),
                            )
                        } else {
                            Err(err)
                        }
                    }
                }
            }
            WorkerBackend::Claude => run_claude_request(
                &request,
                workspace_root.as_path(),
                prior_turns,
                &instruction_hint,
                &session_state,
                &resolved_backend,
                execution_location.clone(),
            ),
            WorkerBackend::WorkersAI => run_workers_ai_request(
                &request,
                workspace_root.as_path(),
                prior_turns,
                &instruction_hint,
                &session_state,
                &resolved_backend,
                execution_location.clone(),
                None,
            ),
            WorkerBackend::Custom(value) => {
                bail!("unsupported worker backend '{}'", value)
            }
        }
    };

    match result {
        Ok(result) => {
            record_response_session_turn(&mut session_state, &result.text);
            session_store.save(&session_state)?;
            fs::write(&log_path, &result.log_body)
                .with_context(|| format!("failed to write {}", log_path.display()))?;
            append_history_entry(
                &history_path,
                SessionHistoryEntry {
                    invocation_id: request.invocation_id.clone(),
                    requested_at: request.requested_at.clone(),
                    message_count: request.messages.len(),
                    prompt_bytes: request.prompt.len(),
                    script: request.script.clone(),
                    success: true,
                },
            )?;
            let completed_at = Utc::now().to_rfc3339();
            let task_signature = request.task_signature.clone();
            let boundary_claims = request.boundary_claims.clone();
            let gate_evaluation = request.gate_evaluation.clone();
            let assurance = task_signature.as_ref().map(|signature| {
                derive_assurance(
                    result.metadata.backend.as_str(),
                    signature,
                    gate_evaluation.as_ref(),
                    request.script.is_some(),
                )
            });
            let symbol_carriers = derive_symbol_carriers(
                &request.invocation_id,
                &request.session,
                Some(log_path.as_path()),
            );
            let provenance_edges =
                derive_provenance_edges(&request.invocation_id, &symbol_carriers, &boundary_claims);
            let provenance = build_execution_provenance_record(BuildExecutionProvenanceInput {
                id: request.invocation_id.clone(),
                run_kind: run_kind_for_backend(&result.metadata.backend, request.script.is_some()),
                group_folder: request.session.group_folder.clone(),
                chat_jid: Some(request.group.jid.clone()),
                execution_location: execution_location.clone(),
                request_plane: request.request_plane.clone(),
                effective_capabilities: result.metadata.effective_capabilities.clone(),
                project_environment_id: result.metadata.project_environment_id.clone(),
                mount_summary: result.metadata.mount_summary.clone(),
                secret_handles_used: result.metadata.secret_handles.clone(),
                fallback_reason: result.metadata.fallback_reason.clone(),
                sync_scope: None,
                task_signature,
                boundary_claims,
                gate_evaluation: gate_evaluation.clone(),
                assurance: assurance.clone(),
                symbol_carriers,
                provenance_edges,
                status: ExecutionStatus::Success,
                created_at: request.requested_at.clone(),
                updated_at: completed_at.clone(),
                completed_at: Some(completed_at),
            });
            let execution_metadata = build_execution_metadata(
                &result,
                request.routing_decision.clone(),
                request.objective.clone(),
                request.plan.clone(),
                Some(compact_session_state(&session_state, 6)),
                gate_evaluation,
                assurance,
            );
            Ok(WorkerResponse {
                text: result.text,
                boundary: ExecutionBoundary {
                    kind: ExecutionBoundaryKind::Host,
                    root: Some(request.session.workspace_root.clone()),
                    isolated: true,
                },
                session_id: request.session.id.clone(),
                log_path: Some(log_path.display().to_string()),
                log_body: Some(result.log_body),
                provenance: Some(provenance),
                metadata: Some(execution_metadata),
            })
        }
        Err(error) => {
            let _ = session_store.save(&session_state);
            let log_body = format!(
                "invocation_id={}\nsession_id={}\nworkspace={}\nstatus=error\nerror={}\n",
                request.invocation_id, request.session.id, request.session.workspace_root, error
            );
            let _ = fs::write(&log_path, log_body);
            let _ = append_history_entry(
                &history_path,
                SessionHistoryEntry {
                    invocation_id: request.invocation_id,
                    requested_at: request.requested_at,
                    message_count: request.messages.len(),
                    prompt_bytes: request.prompt.len(),
                    script: request.script,
                    success: false,
                },
            );
            Err(error)
        }
    }
}

fn handle_daemon_connection(stream: &mut UnixStream) -> Result<()> {
    let request: WorkerRequest = read_json(stream)?;
    let outcome = match execute_worker_request(request) {
        Ok(response) => WorkerOutcome {
            response: Some(response),
            error: None,
        },
        Err(error) => WorkerOutcome {
            response: None,
            error: Some(error.to_string()),
        },
    };
    write_json(stream, &outcome)
}

fn build_worker_request(request: ExecutionRequest) -> WorkerRequest {
    WorkerRequest {
        invocation_id: format!("exec-{}", Uuid::new_v4()),
        requested_at: Utc::now().to_rfc3339(),
        group: request.group,
        prompt: request.prompt,
        paperclip_overlay_context: request.paperclip_overlay_context,
        messages: request.messages,
        task_id: request.task_id,
        script: request.script,
        omx: request.omx,
        assistant_name: request.assistant_name,
        request_plane: request.request_plane,
        env: request.env,
        session: request.session,
        backend_override: request.backend_override,
        task_signature: request.task_signature,
        routing_decision: request.routing_decision,
        objective: request.objective,
        plan: request.plan,
        boundary_claims: request.boundary_claims,
        gate_evaluation: request.gate_evaluation,
    }
}

fn decode_worker_outcome(outcome: WorkerOutcome) -> Result<ExecutionResponse> {
    let response = match (outcome.response, outcome.error) {
        (Some(response), None) => response,
        (_, Some(error)) => bail!("worker execution failed: {}", error),
        _ => bail!("worker returned an empty response"),
    };

    Ok(ExecutionResponse {
        text: response.text,
        boundary: response.boundary,
        session_id: response.session_id,
        log_path: response.log_path,
        log_body: response.log_body,
        provenance: response.provenance,
        metadata: response.metadata,
    })
}

fn run_worker_command(command: &mut Command, payload: &WorkerRequest) -> Result<ExecutionResponse> {
    let stdin = serde_json::to_vec(payload).context("failed to encode worker request")?;
    let output = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to spawn {}", describe_command(command)))?;
    let output = write_and_wait_for_output(output, &stdin, command)?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!(
            "{} exited with status {}{}",
            describe_command(command),
            output.status,
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        );
    }
    let outcome: WorkerOutcome =
        serde_json::from_slice(&output.stdout).context("failed to parse worker outcome")?;
    decode_worker_outcome(outcome)
}

fn write_and_wait_for_output(
    mut child: std::process::Child,
    stdin: &[u8],
    command: &Command,
) -> Result<std::process::Output> {
    if !stdin.is_empty() {
        child
            .stdin
            .as_mut()
            .context("worker command stdin was not available")?
            .write_all(stdin)
            .with_context(|| format!("failed to write stdin to {}", describe_command(command)))?;
    }
    child
        .wait_with_output()
        .with_context(|| format!("failed to wait for {}", describe_command(command)))
}

fn describe_command(command: &Command) -> String {
    let program = command.get_program().to_string_lossy().to_string();
    let args = command
        .get_args()
        .map(|arg| arg.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    if args.is_empty() {
        program
    } else {
        format!("{} {}", program, args.join(" "))
    }
}

fn build_execution_metadata(
    result: &BackendExecutionResult,
    routing_decision: Option<RoutingDecision>,
    objective: Option<Objective>,
    plan: Option<Plan>,
    session_state: Option<SessionState>,
    gate_evaluation: Option<GateEvaluation>,
    assurance: Option<AssuranceTuple>,
) -> ExecutionMetadata {
    ExecutionMetadata {
        backend: Some(result.metadata.backend.as_str().to_string()),
        provider: result.metadata.provider.clone(),
        biller: result.metadata.biller.clone(),
        billing_type: result.metadata.billing_type.clone(),
        model: result.metadata.model.clone(),
        usage: result.metadata.usage.clone(),
        cost_usd: result.metadata.cost_usd,
        routing_decision,
        objective,
        plan,
        session_state,
        gate_evaluation,
        assurance,
        status: None,
        question: None,
        tmux_session: None,
        team_name: None,
        summary: None,
        artifacts: Vec::new(),
        external_run_id: None,
    }
}

fn connect_to_worker_socket(session: &ExecutionSession) -> Result<UnixStream> {
    let socket_path = session.socket_path();
    let stream = UnixStream::connect(&socket_path)
        .with_context(|| format!("failed to connect {}", socket_path.display()))?;
    let request_timeout = worker_request_timeout();
    stream
        .set_read_timeout(Some(request_timeout))
        .context("failed to set worker read timeout")?;
    stream
        .set_write_timeout(Some(request_timeout))
        .context("failed to set worker write timeout")?;
    Ok(stream)
}

fn cleanup_stale_worker_state(session: &ExecutionSession) -> Result<()> {
    for path in [session.socket_path(), session.pid_path()] {
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove stale {}", path.display()))?;
        }
    }
    Ok(())
}

fn wait_for_worker_socket(session: &ExecutionSession) -> Result<UnixStream> {
    let deadline = Instant::now() + WORKER_START_TIMEOUT;
    loop {
        match connect_to_worker_socket(session) {
            Ok(stream) => return Ok(stream),
            Err(_) if Instant::now() < deadline => thread::sleep(WORKER_CONNECT_RETRY),
            Err(error) => return Err(error),
        }
    }
}

fn worker_idle_timeout() -> Duration {
    std::env::var("NANOCLAW_WORKER_IDLE_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value >= 50)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_WORKER_IDLE_TIMEOUT)
}

fn worker_request_timeout() -> Duration {
    std::env::var("NANOCLAW_WORKER_REQUEST_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value >= 1000)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_WORKER_REQUEST_TIMEOUT)
}

fn parse_json_string_field(value: Option<&Value>) -> Option<String> {
    let value = value.and_then(Value::as_str)?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_json_usize_field(value: Option<&Value>) -> Option<usize> {
    value
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
}

fn parse_json_f64_field(value: Option<&Value>) -> Option<f64> {
    let value = value.and_then(Value::as_f64)?;
    if value.is_finite() && value >= 0.0 {
        Some(value)
    } else {
        None
    }
}

fn parse_codex_jsonl(stdout: &str) -> CodexJsonlParseResult {
    let mut parsed = CodexJsonlParseResult::default();
    let mut messages = Vec::new();
    let mut usage = ExecutionUsageSummary::default();

    for raw_line in stdout.split('\n') {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        let Ok(event) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let event_type = parse_json_string_field(event.get("type")).unwrap_or_default();
        match event_type.as_str() {
            "thread.started" => {
                parsed.session_id =
                    parse_json_string_field(event.get("thread_id")).or(parsed.session_id);
                parsed.model = parse_json_string_field(event.get("model")).or(parsed.model);
            }
            "error" => {
                parsed.error_message =
                    parse_json_string_field(event.get("message")).or(parsed.error_message);
            }
            "item.completed" => {
                let Some(item) = event.get("item").and_then(Value::as_object) else {
                    continue;
                };
                if parse_json_string_field(item.get("type")) != Some("agent_message".to_string()) {
                    continue;
                }
                if let Some(text) = parse_json_string_field(item.get("text")) {
                    messages.push(text);
                }
            }
            "turn.completed" | "turn.failed" => {
                let usage_object = event.get("usage").and_then(Value::as_object);
                if let Some(usage_object) = usage_object {
                    usage.input_tokens = parse_json_usize_field(usage_object.get("input_tokens"))
                        .unwrap_or(usage.input_tokens);
                    usage.cached_input_tokens = parse_json_usize_field(
                        usage_object
                            .get("cached_input_tokens")
                            .or_else(|| usage_object.get("cache_read_input_tokens")),
                    )
                    .unwrap_or(usage.cached_input_tokens);
                    usage.output_tokens = parse_json_usize_field(usage_object.get("output_tokens"))
                        .unwrap_or(usage.output_tokens);
                }
                parsed.cost_usd =
                    parse_json_f64_field(event.get("total_cost_usd")).or(parsed.cost_usd);
                parsed.model = parse_json_string_field(event.get("model")).or(parsed.model);
                if event_type == "turn.failed" {
                    parsed.error_message = event
                        .get("error")
                        .and_then(Value::as_object)
                        .and_then(|error| parse_json_string_field(error.get("message")))
                        .or_else(|| parse_json_string_field(event.get("message")))
                        .or(parsed.error_message);
                }
            }
            _ => {}
        }
    }

    parsed.summary = messages.join("\n\n").trim().to_string();
    if !usage.is_empty() {
        parsed.usage = Some(usage);
    }
    parsed
}

fn is_paperclip_gateway_request(request: &WorkerRequest) -> bool {
    request.group.folder.starts_with("paperclip_agent_")
        || request.group.jid.starts_with("paperclip:")
        || request.messages.iter().any(|message| {
            message.sender == "paperclip" || message.chat_jid.starts_with("paperclip:")
        })
}

fn default_codex_sandbox_for_request(request: &WorkerRequest) -> &'static str {
    if is_paperclip_gateway_request(request) {
        "danger-full-access"
    } else {
        DEFAULT_CODEX_SANDBOX
    }
}

fn is_codex_usage_limit_error(err_text: &str) -> bool {
    let normalized = err_text.to_ascii_lowercase();
    normalized.contains("usage limit") || normalized.contains("usage_limit")
}

fn should_use_container_lane(container_groups: &[String], group: &Group) -> bool {
    container_groups.iter().any(|value| {
        matches!(value.trim(), "*" | "all") || value == &group.folder || value == &group.jid
    })
}

fn should_use_remote_lane(mode: &RemoteWorkerMode, group: &Group) -> bool {
    match mode {
        RemoteWorkerMode::Off => false,
        RemoteWorkerMode::Main => group.is_main || group.folder == "main" || group.jid == "main",
        RemoteWorkerMode::All => true,
        RemoteWorkerMode::Custom(value) => value == &group.folder || value == &group.jid,
    }
}

fn resolve_container_runtime(preferred: Option<&str>) -> Result<String> {
    if let Some(binary) = preferred.filter(|value| !value.trim().is_empty()) {
        return Ok(binary.trim().to_string());
    }

    for candidate in ["docker", "podman"] {
        if Command::new(candidate)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
        {
            return Ok(candidate.to_string());
        }
    }

    bail!("no container runtime was found; set NANOCLAW_CONTAINER_RUNTIME to docker or podman")
}

fn detect_remote_manifest_path(project_root: &Path) -> Option<String> {
    if project_root.join("Cargo.toml").exists() {
        Some("Cargo.toml".to_string())
    } else if project_root.join("agency").join("Cargo.toml").exists() {
        Some("agency/Cargo.toml".to_string())
    } else {
        None
    }
}

fn remoteize_request(remote_worker_root: &str, request: ExecutionRequest) -> ExecutionRequest {
    let remote_workspace_root = format!("{remote_worker_root}/groups/{}", request.group.folder);
    let remote_session_root = format!("{remote_worker_root}/sessions/{}", request.session.id);
    ExecutionRequest {
        group: request.group,
        prompt: request.prompt,
        paperclip_overlay_context: request.paperclip_overlay_context,
        messages: request.messages,
        task_id: request.task_id,
        script: request.script,
        assistant_name: request.assistant_name,
        request_plane: request.request_plane,
        env: request.env,
        backend_override: request.backend_override,
        task_signature: request.task_signature,
        routing_decision: request.routing_decision,
        objective: request.objective,
        plan: request.plan,
        boundary_claims: request.boundary_claims,
        gate_evaluation: request.gate_evaluation,
        session: ExecutionSession {
            id: request.session.id,
            group_folder: request.session.group_folder,
            workspace_root: remote_workspace_root.clone(),
            session_root: remote_session_root.clone(),
            ipc_root: format!("{remote_session_root}/ipc"),
            state_root: format!("{remote_session_root}/state"),
            logs_root: format!("{remote_session_root}/logs"),
        },
        omx: request.omx,
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn write_json<T: Serialize>(writer: &mut impl Write, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value).context("failed to encode worker payload")?;
    writer
        .write_all(&bytes)
        .context("failed to write worker payload")?;
    writer.flush().context("failed to flush worker payload")?;
    Ok(())
}

fn read_json<T: DeserializeOwned>(reader: &mut impl Read) -> Result<T> {
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .context("failed to read worker payload")?;
    serde_json::from_slice(&bytes).context("failed to parse worker payload")
}

fn build_message_response(
    request: &WorkerRequest,
    prior_turns: usize,
    instruction_hint: &str,
    workspace_root: &Path,
    execution_location: ExecutionLocation,
) -> BackendExecutionResult {
    let latest = request.messages.last();
    let latest_summary = latest
        .map(|message| {
            format!(
                "Latest from {} at {}: {}",
                message.sender_name.as_deref().unwrap_or(&message.sender),
                message.timestamp,
                message.content
            )
        })
        .unwrap_or_else(|| "No inbound messages were provided.".to_string());

    let text = format!(
        "{} handled session {} for group '{}'.\nSession turn: {}\nWorkspace: {}\nInstructions: {}\n{}\nPrompt bytes: {}\nHandled at {}.",
        request.assistant_name,
        request.session.id,
        request.group.name,
        prior_turns + 1,
        workspace_root.display(),
        instruction_hint,
        latest_summary,
        request.prompt.len(),
        Utc::now().to_rfc3339()
    );
    let log_body = format!(
        "invocation_id={}\nsession_id={}\nworkspace={}\nmessage_count={}\nprompt_bytes={}\ninstruction_hint={}\n",
        request.invocation_id,
        request.session.id,
        workspace_root.display(),
        request.messages.len(),
        request.prompt.len(),
        instruction_hint
    );
    BackendExecutionResult {
        text,
        log_body,
        metadata: BackendExecutionMetadata {
            backend: WorkerBackend::Summary,
            provider: None,
            biller: None,
            billing_type: None,
            model: None,
            usage: None,
            cost_usd: None,
            effective_capabilities: derive_capability_manifest(
                &request.request_plane,
                DeriveCapabilityManifestInput::default(),
            ),
            project_environment_id: None,
            secret_handles: Vec::new(),
            mount_summary: vec![ExecutionMountSummaryEntry {
                host_path: Some(workspace_root.display().to_string()),
                container_path: None,
                readonly: matches!(execution_location, ExecutionLocation::LocalContainer),
                kind: ExecutionMountKind::Project,
            }],
            fallback_reason: None,
        },
    }
}

fn run_script_request(
    request: &WorkerRequest,
    script: &str,
    workspace_root: &Path,
    prior_turns: usize,
    instruction_hint: &str,
    execution_location: ExecutionLocation,
) -> Result<BackendExecutionResult> {
    let output = Command::new("/bin/sh")
        .arg("-lc")
        .arg(script)
        .current_dir(workspace_root)
        .output()
        .with_context(|| format!("failed to execute script in {}", workspace_root.display()))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let log_body = format!(
        "invocation_id={}\nsession_id={}\nworkspace={}\nsession_turn={}\ninstruction_hint={}\nscript={}\nstatus={}\nstdout=\n{}\nstderr=\n{}\n",
        request.invocation_id,
        request.session.id,
        workspace_root.display(),
        prior_turns + 1,
        instruction_hint,
        script,
        output.status,
        stdout,
        stderr
    );

    if !output.status.success() {
        bail!(
            "script execution failed with status {}{}",
            output.status,
            if stderr.trim().is_empty() {
                String::new()
            } else {
                format!(": {}", summarize_for_chat(&stderr, 400))
            }
        );
    }

    let stdout_summary = summarize_for_chat(&stdout, 1200);
    let stderr_summary = summarize_for_chat(&stderr, 400);
    let text = if stdout_summary.is_empty() {
        format!(
            "{} executed script in session {} for group '{}'.\nSession turn: {}\nWorkspace: {}\nInstructions: {}\nScript completed without stdout.\nHandled at {}.",
            request.assistant_name,
            request.session.id,
            request.group.name,
            prior_turns + 1,
            workspace_root.display(),
            instruction_hint,
            Utc::now().to_rfc3339()
        )
    } else {
        let mut text = format!(
            "{} executed script in session {} for group '{}'.\nSession turn: {}\nWorkspace: {}\nInstructions: {}\nStdout:\n{}",
            request.assistant_name,
            request.session.id,
            request.group.name,
            prior_turns + 1,
            workspace_root.display(),
            instruction_hint,
            stdout_summary
        );
        if !stderr_summary.is_empty() {
            text.push_str("\nStderr:\n");
            text.push_str(&stderr_summary);
        }
        text
    };

    Ok(BackendExecutionResult {
        text,
        log_body,
        metadata: BackendExecutionMetadata {
            backend: WorkerBackend::Summary,
            provider: None,
            biller: None,
            billing_type: None,
            model: None,
            usage: None,
            cost_usd: None,
            effective_capabilities: derive_capability_manifest(
                &request.request_plane,
                DeriveCapabilityManifestInput {
                    allow_host_command: true,
                    ..Default::default()
                },
            ),
            project_environment_id: None,
            secret_handles: Vec::new(),
            mount_summary: vec![ExecutionMountSummaryEntry {
                host_path: Some(workspace_root.display().to_string()),
                container_path: None,
                readonly: matches!(execution_location, ExecutionLocation::LocalContainer),
                kind: ExecutionMountKind::Project,
            }],
            fallback_reason: Some("script".to_string()),
        },
    })
}

fn run_codex_request(
    request: &WorkerRequest,
    workspace_root: &Path,
    prior_turns: usize,
    instruction_hint: &str,
    session_state: &SessionState,
    resolved_backend: &ResolvedWorkerBackend,
    _execution_location: ExecutionLocation,
) -> Result<BackendExecutionResult> {
    if let Some(error) = get_request_plane_text_error(&request.prompt, &request.request_plane) {
        bail!(error);
    }

    let codex_bin = std::env::var("NANOCLAW_CODEX_BIN").unwrap_or_else(|_| "codex".to_string());
    let project_environment = resolved_backend.project_environment.as_ref();
    let project_codex = project_environment
        .as_ref()
        .and_then(|resolved| resolved.project.codex.as_ref());
    let cwd = non_empty_env("NANOCLAW_CODEX_CWD")
        .map(PathBuf::from)
        .or_else(|| {
            project_codex
                .and_then(|config| config.cwd.as_deref())
                .map(|value| resolve_runtime_dir(value, workspace_root))
        })
        .unwrap_or_else(|| workspace_root.to_path_buf());
    ensure_directory(&cwd)?;

    let sandbox = non_empty_env("NANOCLAW_CODEX_SANDBOX")
        .or_else(|| project_codex.and_then(|config| config.sandbox.clone()))
        .unwrap_or_else(|| default_codex_sandbox_for_request(request).to_string());
    let profile = non_empty_env("NANOCLAW_CODEX_PROFILE")
        .or_else(|| project_codex.and_then(|config| config.profile.clone()));
    let model = non_empty_env("NANOCLAW_CODEX_MODEL")
        .or_else(|| project_codex.and_then(|config| config.model.clone()));
    let search_enabled = parse_bool_env("NANOCLAW_CODEX_SEARCH")
        .or_else(|| project_codex.and_then(|config| config.search))
        .unwrap_or(false);
    let project_env_state = resolved_backend.runtime_state.as_ref();
    let unset_keys = &resolved_backend.unset_keys;
    let capability_manifest = derive_capability_manifest(
        &request.request_plane,
        DeriveCapabilityManifestInput {
            allow_host_command: true,
            ..Default::default()
        },
    );

    let mut add_dirs = Vec::new();
    if cwd != workspace_root {
        push_path_if_missing(&mut add_dirs, workspace_root.to_path_buf());
    }
    if let Some(project_codex) = project_codex {
        for add_dir in &project_codex.add_dirs {
            let resolved = resolve_runtime_dir(add_dir, workspace_root);
            ensure_directory(&resolved)?;
            push_path_if_missing(&mut add_dirs, resolved);
        }
    }
    let prompt = build_worker_prompt(request, workspace_root, session_state)?;
    let output_path =
        Path::new(&request.session.state_root).join(format!("{}.codex.out", request.invocation_id));
    let mut command = Command::new(&codex_bin);
    command.arg("-a").arg("never");
    if search_enabled {
        command.arg("--search");
    }
    command
        .arg("exec")
        .arg("--json")
        .arg("--skip-git-repo-check")
        .arg("--color")
        .arg("never");
    if let Some(profile) = profile.as_deref().filter(|value| !value.trim().is_empty()) {
        command.arg("-p").arg(profile);
    }
    if let Some(model) = model.as_deref().filter(|value| !value.trim().is_empty()) {
        command.arg("-m").arg(model);
    }
    if sandbox == "danger-full-access" {
        command.arg("--dangerously-bypass-approvals-and-sandbox");
    } else {
        command.arg("-s").arg(&sandbox);
    }
    command.arg("-C").arg(&cwd).arg("-o").arg(&output_path);
    for add_dir in &add_dirs {
        command.arg("--add-dir").arg(add_dir);
    }
    for (key, value) in get_request_plane_env(&request.request_plane) {
        command.env(key, value);
    }
    for (key, value) in get_capability_manifest_env(&capability_manifest) {
        command.env(key, value);
    }
    if let Some(state) = project_env_state.as_ref() {
        for (key, value) in &state.env {
            command.env(key, value);
        }
    }
    for key in unset_keys {
        command.env_remove(key);
    }
    for (key, value) in &request.env {
        command.env(key, value);
    }
    if let Some(project_environment) = project_environment.as_ref() {
        command.env(
            "NANOCLAW_PROJECT_ENVIRONMENT_ID",
            &project_environment.project.id,
        );
        command.env(
            "NANOCLAW_PROJECT_ENVIRONMENT_MATCHED_BY",
            match project_environment.matched_by {
                super::project_environments::ProjectEnvironmentMatchKind::Id => "id",
                super::project_environments::ProjectEnvironmentMatchKind::ResolvedPath => {
                    "resolvedPath"
                }
                super::project_environments::ProjectEnvironmentMatchKind::RequestedPath => {
                    "requestedPath"
                }
                super::project_environments::ProjectEnvironmentMatchKind::GroupFolder => {
                    "groupFolder"
                }
            },
        );
        command.env(
            "NANOCLAW_PROJECT_ENVIRONMENT_MATCHED_VALUE",
            &project_environment.matched_value,
        );
    }
    command.arg(&prompt);

    let output = command
        .output()
        .with_context(|| format!("failed to execute {}", describe_command(&command)))?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let parsed = parse_codex_jsonl(&stdout);
    let file_text = fs::read_to_string(&output_path)
        .unwrap_or_default()
        .trim()
        .to_string();
    let _ = fs::remove_file(&output_path);
    let text = if file_text.is_empty() {
        parsed.summary.clone()
    } else {
        file_text
    };
    let resolved_model = parsed.model.clone().or(model.clone());

    if !output.status.success() {
        let error_detail = parsed
            .error_message
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| {
                if stderr.trim().is_empty() {
                    None
                } else {
                    Some(summarize_for_chat(&stderr, 600))
                }
            });
        bail!(
            "codex execution failed with status {}{}",
            output.status,
            if let Some(detail) = error_detail {
                format!(": {}", detail)
            } else if stderr.trim().is_empty() {
                String::new()
            } else {
                format!(": {}", summarize_for_chat(&stderr, 600))
            }
        );
    }

    if text.trim().is_empty() {
        bail!("codex execution produced empty output");
    }

    let usage = parsed.usage.clone().unwrap_or_default();
    let log_body = format!(
        "invocation_id={}\nsession_id={}\nworkspace={}\ncwd={}\nsession_turn={}\ninstruction_hint={}\nprompt_bytes={}\nbackend=codex\nprovider=openai\nbiller={}\nbilling_type={}\nmodel={}\ninput_tokens={}\ncached_input_tokens={}\noutput_tokens={}\ncost_usd={}\nrequest_plane={}\nproject_environment_id={}\nproject_environment_match={}\napplied_env_keys={}\nblocked_secret_env_keys={}\nsecret_handles={}\nstdout=\n{}\nstderr=\n{}\nresponse=\n{}\n",
        request.invocation_id,
        request.session.id,
        workspace_root.display(),
        cwd.display(),
        prior_turns + 1,
        instruction_hint,
        prompt.len(),
        resolve_codex_biller(),
        resolve_codex_billing_type(),
        resolved_model.as_deref().unwrap_or("-"),
        usage.input_tokens,
        usage.cached_input_tokens,
        usage.output_tokens,
        parsed
            .cost_usd
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        request.request_plane.as_str(),
        project_environment
            .as_ref()
            .map(|resolved| resolved.project.id.as_str())
            .unwrap_or("-"),
        project_environment
            .as_ref()
            .map(|resolved| resolved.matched_value.as_str())
            .unwrap_or("-"),
        project_env_state
            .as_ref()
            .map(|state| state.applied_env_keys.join(","))
            .unwrap_or_default(),
        project_env_state
            .as_ref()
            .map(|state| state.blocked_secret_env_keys.join(","))
            .unwrap_or_default(),
        project_env_state
            .as_ref()
            .map(|state| state.secret_handles.join(","))
            .unwrap_or_default(),
        stdout,
        stderr,
        text
    );

    Ok(BackendExecutionResult {
        text,
        log_body,
        metadata: BackendExecutionMetadata {
            backend: WorkerBackend::Codex,
            provider: Some("openai".to_string()),
            biller: Some(resolve_codex_biller()),
            billing_type: Some(resolve_codex_billing_type().to_string()),
            model: resolved_model,
            usage: parsed.usage.clone(),
            cost_usd: parsed.cost_usd,
            effective_capabilities: capability_manifest,
            project_environment_id: project_environment.map(|resolved| resolved.project.id.clone()),
            secret_handles: project_env_state
                .map(|state| state.secret_handles.clone())
                .unwrap_or_default(),
            mount_summary: build_host_mount_summary(workspace_root, &cwd, &add_dirs, &sandbox),
            fallback_reason: None,
        },
    })
}

fn run_claude_request(
    request: &WorkerRequest,
    workspace_root: &Path,
    prior_turns: usize,
    instruction_hint: &str,
    session_state: &SessionState,
    resolved_backend: &ResolvedWorkerBackend,
    _execution_location: ExecutionLocation,
) -> Result<BackendExecutionResult> {
    if let Some(error) = get_request_plane_text_error(&request.prompt, &request.request_plane) {
        bail!(error);
    }

    let claude_bin = std::env::var("NANOCLAW_CLAUDE_BIN").unwrap_or_else(|_| "claude".to_string());
    let cwd = non_empty_env("NANOCLAW_CLAUDE_CWD")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root.to_path_buf());
    ensure_directory(&cwd)?;

    let project_environment = resolved_backend.project_environment.as_ref();
    let project_env_state = resolved_backend.runtime_state.as_ref();
    let capability_manifest = derive_capability_manifest(
        &request.request_plane,
        DeriveCapabilityManifestInput {
            allow_host_command: true,
            ..Default::default()
        },
    );
    let prompt = build_worker_prompt(request, workspace_root, session_state)?;
    let mut command = Command::new(&claude_bin);
    command
        .arg("-p")
        .arg(&prompt)
        .arg("--output-format")
        .arg("text");
    if let Some(model) = non_empty_env("NANOCLAW_CLAUDE_MODEL") {
        command.arg("--model").arg(model);
    }
    if let Some(permission_mode) = non_empty_env("NANOCLAW_CLAUDE_PERMISSION_MODE") {
        command.arg("--permission-mode").arg(permission_mode);
    }
    command.current_dir(&cwd);
    for (key, value) in get_request_plane_env(&request.request_plane) {
        command.env(key, value);
    }
    for (key, value) in get_capability_manifest_env(&capability_manifest) {
        command.env(key, value);
    }
    if let Some(state) = project_env_state {
        for (key, value) in &state.env {
            command.env(key, value);
        }
    }
    for key in &resolved_backend.unset_keys {
        command.env_remove(key);
    }
    for (key, value) in &request.env {
        command.env(key, value);
    }
    if let Some(project_environment) = project_environment {
        command.env(
            "NANOCLAW_PROJECT_ENVIRONMENT_ID",
            &project_environment.project.id,
        );
        command.env(
            "NANOCLAW_PROJECT_ENVIRONMENT_MATCHED_VALUE",
            &project_environment.matched_value,
        );
    }

    let output = command
        .output()
        .with_context(|| format!("failed to execute {}", describe_command(&command)))?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let text = stdout.trim().to_string();

    if !output.status.success() {
        bail!(
            "claude execution failed with status {}{}",
            output.status,
            if stderr.trim().is_empty() {
                String::new()
            } else {
                format!(": {}", summarize_for_chat(&stderr, 600))
            }
        );
    }

    if text.is_empty() {
        bail!("claude execution produced empty output");
    }

    let log_body = format!(
        "invocation_id={}\nsession_id={}\nworkspace={}\ncwd={}\nsession_turn={}\ninstruction_hint={}\nprompt_bytes={}\nbackend=claude\nrequest_plane={}\nproject_environment_id={}\nproject_environment_match={}\napplied_env_keys={}\nblocked_secret_env_keys={}\nsecret_handles={}\nstdout=\n{}\nstderr=\n{}\nresponse=\n{}\n",
        request.invocation_id,
        request.session.id,
        workspace_root.display(),
        cwd.display(),
        prior_turns + 1,
        instruction_hint,
        prompt.len(),
        request.request_plane.as_str(),
        project_environment
            .map(|resolved| resolved.project.id.as_str())
            .unwrap_or("-"),
        project_environment
            .map(|resolved| resolved.matched_value.as_str())
            .unwrap_or("-"),
        project_env_state
            .map(|state| state.applied_env_keys.join(","))
            .unwrap_or_default(),
        project_env_state
            .map(|state| state.blocked_secret_env_keys.join(","))
            .unwrap_or_default(),
        project_env_state
            .map(|state| state.secret_handles.join(","))
            .unwrap_or_default(),
        stdout,
        stderr,
        text
    );

    Ok(BackendExecutionResult {
        text,
        log_body,
        metadata: BackendExecutionMetadata {
            backend: WorkerBackend::Claude,
            provider: None,
            biller: None,
            billing_type: None,
            model: None,
            usage: None,
            cost_usd: None,
            effective_capabilities: capability_manifest,
            project_environment_id: project_environment.map(|resolved| resolved.project.id.clone()),
            secret_handles: project_env_state
                .map(|state| state.secret_handles.clone())
                .unwrap_or_default(),
            mount_summary: build_host_mount_summary(workspace_root, &cwd, &[], "workspace-write"),
            fallback_reason: None,
        },
    })
}

fn run_workers_ai_request(
    request: &WorkerRequest,
    workspace_root: &Path,
    _prior_turns: usize,
    _instruction_hint: &str,
    session_state: &SessionState,
    resolved_backend: &ResolvedWorkerBackend,
    _execution_location: ExecutionLocation,
    fallback_reason: Option<String>,
) -> Result<BackendExecutionResult> {
    if let Some(error) = get_request_plane_text_error(&request.prompt, &request.request_plane) {
        bail!(error);
    }

    let proxy_url = std::env::var("PAPERCLIP_WORKERS_AI_PROXY_URL")
        .unwrap_or_else(|_| DEFAULT_WORKERS_AI_PROXY_URL.to_string());
    let proxy_url = normalize_workers_ai_proxy_url(&proxy_url)?;
    let proxy_token = std::env::var("PAPERCLIP_AI_PROXY_TOKEN")
        .or_else(|_| std::env::var("PAPERCLIP_WORKERS_AI_PROXY_TOKEN"))
        .ok();
    let model = std::env::var("NANOCLAW_WORKERS_AI_MODEL")
        .unwrap_or_else(|_| "@cf/meta/llama-3-8b-instruct".to_string());

    let prompt = build_worker_prompt(request, workspace_root, session_state)?;

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .context("failed to build reqwest client")?;

    let mut rb = client.post(&proxy_url).json(&serde_json::json!({
        "taskType": "lightweight_planning",
        "input": prompt,
        "model": model,
        "correlationId": request.invocation_id
    }));

    if let Some(token) = proxy_token {
        rb = rb.header("x-paperclip-ai-proxy-token", token);
    }

    let response = rb
        .send()
        .context("failed to send request to Workers AI proxy")?;
    let status = response.status();
    let body = response
        .text()
        .context("failed to read Workers AI response body")?;

    if !status.is_success() {
        bail!(
            "Workers AI execution failed with status {} for proxy URL {}: {}",
            status,
            proxy_url,
            summarize_workers_ai_body(&body)
        );
    }

    let result: serde_json::Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "failed to parse Workers AI response: {}",
            summarize_workers_ai_body(&body)
        )
    })?;
    if !result["ok"].as_bool().unwrap_or(false) {
        let error_msg = result["error"].as_str().unwrap_or("unknown error");
        bail!(
            "Workers AI proxy returned unsuccessful result: {}",
            error_msg
        );
    }

    let text = result["text"].as_str().unwrap_or_default().to_string();

    if text.is_empty() {
        bail!("Workers AI execution produced empty output");
    }

    let capability_manifest = derive_capability_manifest(
        &request.request_plane,
        DeriveCapabilityManifestInput::default(),
    );

    let log_body = format!(
        "invocation_id={}\nsession_id={}\nbackend=workers-ai\nmodel={}\nfallback_reason={:?}\nresponse=\n{}\n",
        request.invocation_id,
        request.session.id,
        model,
        fallback_reason,
        text
    );

    Ok(BackendExecutionResult {
        text,
        log_body,
        metadata: BackendExecutionMetadata {
            backend: WorkerBackend::WorkersAI,
            provider: Some("cloudflare".to_string()),
            biller: Some("cloudflare".to_string()),
            billing_type: None,
            model: Some(model),
            usage: None,
            cost_usd: None,
            effective_capabilities: capability_manifest,
            project_environment_id: resolved_backend
                .project_environment
                .as_ref()
                .map(|resolved| resolved.project.id.clone()),
            secret_handles: Vec::new(),
            mount_summary: Vec::new(),
            fallback_reason,
        },
    })
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn resolve_openai_compatible_biller() -> String {
    if non_empty_env("OPENROUTER_API_KEY").is_some() {
        return "openrouter".to_string();
    }
    let base_url = non_empty_env("OPENAI_BASE_URL")
        .or_else(|| non_empty_env("OPENAI_API_BASE"))
        .or_else(|| non_empty_env("OPENAI_API_BASE_URL"));
    if let Some(base_url) = base_url {
        if base_url.to_ascii_lowercase().contains("openrouter.ai") {
            return "openrouter".to_string();
        }
    }
    "openai".to_string()
}

fn resolve_codex_billing_type() -> &'static str {
    if non_empty_env("OPENAI_API_KEY").is_some() {
        "api"
    } else {
        "subscription"
    }
}

fn resolve_codex_biller() -> String {
    let openai_compatible_biller = resolve_openai_compatible_biller();
    if resolve_codex_billing_type() == "subscription" {
        "chatgpt".to_string()
    } else {
        openai_compatible_biller
    }
}

fn parse_bool_env(key: &str) -> Option<bool> {
    non_empty_env(key).map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

fn resolve_runtime_dir(value: &str, workspace_root: &Path) -> PathBuf {
    let path = PathBuf::from(value);
    let resolved = if path.is_absolute() {
        path
    } else {
        workspace_root.join(path)
    };
    fs::canonicalize(&resolved).unwrap_or(resolved)
}

fn ensure_directory(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("codex runtime path does not exist: {}", path.display()))?;
    if !metadata.is_dir() {
        bail!("codex runtime path is not a directory: {}", path.display());
    }
    Ok(())
}

fn push_path_if_missing(paths: &mut Vec<PathBuf>, candidate: PathBuf) {
    if !paths.iter().any(|existing| existing == &candidate) {
        paths.push(candidate);
    }
}

fn current_execution_location() -> ExecutionLocation {
    std::env::var("NANOCLAW_EXECUTION_LOCATION")
        .ok()
        .map(|value| ExecutionLocation::parse(&value))
        .unwrap_or(ExecutionLocation::Host)
}

fn run_kind_for_backend(backend: &WorkerBackend, is_script: bool) -> ExecutionRunKind {
    if is_script {
        return ExecutionRunKind::Custom("script".to_string());
    }

    match backend {
        WorkerBackend::Summary => ExecutionRunKind::Summary,
        WorkerBackend::Codex => ExecutionRunKind::Codex,
        WorkerBackend::Claude => ExecutionRunKind::Claude,
        WorkerBackend::WorkersAI => ExecutionRunKind::WorkersAI,
        WorkerBackend::Custom(value) => ExecutionRunKind::Custom(value.clone()),
    }
}

fn build_host_mount_summary(
    workspace_root: &Path,
    cwd: &Path,
    add_dirs: &[PathBuf],
    sandbox: &str,
) -> Vec<ExecutionMountSummaryEntry> {
    let readonly = sandbox == "read-only";
    let mut mounts = vec![ExecutionMountSummaryEntry {
        host_path: Some(cwd.display().to_string()),
        container_path: if cwd != workspace_root {
            Some(cwd.display().to_string())
        } else {
            None
        },
        readonly,
        kind: ExecutionMountKind::Project,
    }];
    for add_dir in add_dirs {
        mounts.push(ExecutionMountSummaryEntry {
            host_path: Some(add_dir.display().to_string()),
            container_path: None,
            readonly,
            kind: ExecutionMountKind::Additional,
        });
    }
    mounts
}

fn build_worker_prompt(
    request: &WorkerRequest,
    workspace_root: &Path,
    session_state: &SessionState,
) -> Result<String> {
    let project_context = ContextLoader::load_from(workspace_root)
        .map(|context| context.render())
        .unwrap_or_default();
    let paperclip_managed_context = request
        .paperclip_overlay_context
        .as_deref()
        .unwrap_or_default();
    let objective = request
        .objective
        .as_ref()
        .map(Objective::format_for_prompt)
        .unwrap_or_default();
    let routing = request
        .routing_decision
        .as_ref()
        .map(render_routing_decision)
        .unwrap_or_default();
    let plan = request.plan.as_ref().map(Plan::summary).unwrap_or_default();
    let session = render_session_state(session_state);

    Ok(format!(
        "You are {assistant} acting for the NanoClaw group '{group}'.\n\
\n\
Follow the Paperclip-managed context first when it is supplied for this run. Then follow the project context, execution objective, routing brief, and plan below when they are relevant.\n\
<paperclip_managed_context>\n\
{paperclip_managed_context}\n\
</paperclip_managed_context>\n\
\n\
<project_context>\n\
{project_context}\n\
</project_context>\n\
\n\
<objective>\n\
{objective}\n\
</objective>\n\
\n\
<routing>\n\
{routing}\n\
</routing>\n\
\n\
<plan>\n\
{plan}\n\
</plan>\n\
\n\
<session_state>\n\
{session}\n\
</session_state>\n\
\n\
Respond only with the message that should be sent back to the chat user.\n\
Do not include session metadata, prompt diagnostics, execution summaries, or tool logs.\n\
If the user message is simple conversational text, reply naturally and briefly.\n\
\n\
Current conversation context:\n\
{prompt}\n",
        assistant = request.assistant_name,
        group = request.group.name,
        paperclip_managed_context = paperclip_managed_context,
        project_context = project_context,
        objective = objective,
        routing = routing,
        plan = plan,
        session = session,
        prompt = request.prompt
    ))
}

fn read_workspace_hint(workspace_root: &Path) -> Result<String> {
    let context = ContextLoader::load_from(workspace_root)?;
    if context.documents.is_empty() {
        return Ok("No AGENTS.md/CLAUDE.md found.".to_string());
    }

    for document in context.documents.iter().rev() {
        if let Some(line) = document
            .body
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
        {
            return Ok(line.to_string());
        }
    }
    Ok("Project context files are present but empty.".to_string())
}

fn append_history_entry(path: &Path, entry: SessionHistoryEntry) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    writeln!(file, "{}", serde_json::to_string(&entry)?)
        .with_context(|| format!("failed to append {}", path.display()))?;
    Ok(())
}

fn count_history_entries(path: &Path) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let body =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(body.lines().count())
}

fn summarize_for_chat(input: &str, max_chars: usize) -> String {
    let trimmed = input.trim();
    if trimmed.len() <= max_chars {
        return trimmed.to_string();
    }
    format!("{}...", &trimmed[..max_chars])
}

fn derive_routing_decision(
    prompt: &str,
    request_plane: &RequestPlane,
    workspace_root: &Path,
    preferred_lane: Option<ExecutionLane>,
) -> RoutingDecision {
    HarnessRouter::route(&RoutingInput {
        prompt: prompt.to_string(),
        request_plane: request_plane.clone(),
        has_repo: workspace_root.join(".git").exists(),
        preferred_lane,
    })
}

fn build_execution_objective(
    prompt: &str,
    assistant_name: &str,
    signature: &TaskSignature,
    routing_decision: &RoutingDecision,
) -> Objective {
    let mut objective =
        Objective::new(prompt.trim().to_string()).with_budget(signature.budget.clone().into());
    objective.service_clause.provider_role = assistant_name.to_string();
    objective.service_clause.consumer_role = "Requester".to_string();
    if routing_decision.should_load_project_context {
        objective = objective.with_acceptance("Use the local project context and workspace rules.");
    }
    if routing_decision.should_verify_assumptions {
        objective =
            objective.with_acceptance("Verify completion and note residual risk or uncertainty.");
    }
    if matches!(routing_decision.task_kind, TaskKind::Coding) {
        objective = objective
            .with_acceptance("Work directly against the workspace when changes are needed.");
    }
    objective
}

fn build_execution_plan(objective: &Objective, routing_decision: &RoutingDecision) -> Plan {
    let mut plan = Plan::new(objective.goal.clone());
    let mut step_num = 1usize;
    if routing_decision.should_load_project_context {
        plan.push_step(PlanStep {
            step_num,
            description: "Load project context and inspect workspace instructions.".to_string(),
            task_kind: TaskKind::Planning,
            preferred_lane: Some(ExecutionLane::Host),
            suggested_tools: vec!["project_context".to_string()],
            expected_output: "Relevant local constraints and repo context.".to_string(),
            depends_on: Vec::new(),
            status: PlanStepStatus::Pending,
            output: None,
        });
        step_num += 1;
    }
    let dependency = if step_num > 1 {
        vec![step_num - 1]
    } else {
        Vec::new()
    };
    plan.push_step(PlanStep {
        step_num,
        description: format!("Execute the objective: {}", objective.goal),
        task_kind: routing_decision.task_kind.clone(),
        preferred_lane: Some(routing_decision.scale.preferred_lane.clone()),
        suggested_tools: Vec::new(),
        expected_output: "A concrete answer or workspace change that satisfies the objective."
            .to_string(),
        depends_on: dependency,
        status: PlanStepStatus::Pending,
        output: None,
    });
    if routing_decision.should_verify_assumptions {
        plan.push_step(PlanStep {
            step_num: step_num + 1,
            description: "Verify the result against acceptance criteria and residual risk."
                .to_string(),
            task_kind: TaskKind::Planning,
            preferred_lane: Some(ExecutionLane::Host),
            suggested_tools: vec!["verification".to_string()],
            expected_output: "Concise confidence statement and remaining risk.".to_string(),
            depends_on: vec![step_num],
            status: PlanStepStatus::Pending,
            output: None,
        });
    }
    plan
}

fn routing_preferred_lane(routing_decision: Option<&RoutingDecision>) -> Option<ExecutionLane> {
    match routing_decision?.scale.preferred_lane {
        ExecutionLane::Host
        | ExecutionLane::Container
        | ExecutionLane::RemoteWorker
        | ExecutionLane::Omx => Some(routing_decision?.scale.preferred_lane.clone()),
        ExecutionLane::Auto | ExecutionLane::Custom(_) => None,
    }
}

fn render_routing_decision(routing: &RoutingDecision) -> String {
    format!(
        "task_kind={}\ndata_shape={}\npreferred_lane={}\ncandidate_lanes={}\nconfidence={:.2}\nreason={}\nverify_assumptions={}\nload_project_context={}",
        routing.task_kind.as_str(),
        routing.data_shape.as_str(),
        routing.scale.preferred_lane.as_str(),
        routing
            .candidate_lanes
            .iter()
            .map(ExecutionLane::as_str)
            .collect::<Vec<_>>()
            .join(","),
        routing.confidence.0,
        routing.reason,
        routing.should_verify_assumptions,
        routing.should_load_project_context
    )
}

fn render_session_state(session_state: &SessionState) -> String {
    if session_state.turns.is_empty() {
        return "No persisted session turns yet.".to_string();
    }
    session_state
        .turns
        .iter()
        .rev()
        .take(6)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|turn| format!("[{}] {}", turn.role.as_str(), turn.content))
        .collect::<Vec<_>>()
        .join("\n")
}

fn session_state_path(session: &ExecutionSession) -> PathBuf {
    Path::new(&session.state_root).join("session.json")
}

fn record_request_session_turn(state: &mut SessionState, request: &WorkerRequest) {
    if let Some((role, content)) = derive_request_session_turn(
        request.task_id.as_deref(),
        &request.messages,
        &request.prompt,
    ) {
        push_session_turn_if_new(state, role, content);
    }
}

fn record_execution_request_session_turn(state: &mut SessionState, request: &ExecutionRequest) {
    if let Some((role, content)) = derive_request_session_turn(
        request.task_id.as_deref(),
        &request.messages,
        &request.prompt,
    ) {
        push_session_turn_if_new(state, role, content);
    }
}

fn record_response_session_turn(state: &mut SessionState, response_text: &str) {
    let content = summarize_for_chat(response_text, 2000);
    if content.is_empty() {
        return;
    }
    push_session_turn_if_new(state, SessionRole::Assistant, content);
}

fn push_session_turn_if_new(state: &mut SessionState, role: SessionRole, content: String) {
    let is_duplicate = state
        .turns
        .last()
        .map(|turn| turn.role == role && turn.content == content)
        .unwrap_or(false);
    if !is_duplicate {
        state.push_turn(role, content);
    }
}

fn derive_request_session_turn(
    task_id: Option<&str>,
    messages: &[MessageRecord],
    prompt: &str,
) -> Option<(SessionRole, String)> {
    let content = if let Some(task_id) = task_id {
        format!(
            "Scheduled task {}: {}",
            task_id,
            summarize_for_chat(prompt, 1200)
        )
    } else if let Some(message) = messages.last() {
        message.content.trim().to_string()
    } else {
        summarize_for_chat(prompt, 1200)
    };
    if content.is_empty() {
        return None;
    }
    let role = if task_id.is_some() {
        SessionRole::System
    } else {
        SessionRole::User
    };
    Some((role, content))
}

fn compact_session_state(state: &SessionState, max_turns: usize) -> SessionState {
    let mut compact = state.clone();
    if compact.turns.len() > max_turns {
        compact.turns = compact.turns[compact.turns.len() - max_turns..].to_vec();
    }
    compact
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::Shutdown;
    use std::path::Path;
    use std::path::PathBuf;
    use std::thread;
    use std::time::Duration;

    use tempfile::tempdir;

    use crate::foundation::CapabilityManifest;
    use crate::foundation::{ExecutionLane, MessageRecord, RemoteWorkerMode, RequestPlane};
    use crate::nanoclaw::config::NanoclawConfig;
    use crate::nanoclaw::model_router::WorkerBackend;

    use super::{
        build_execution_metadata, build_execution_session, connect_to_worker_socket,
        default_codex_sandbox_for_request, is_codex_usage_limit_error, parse_codex_jsonl,
        read_json, run_worker_daemon_with_idle_timeout, run_worker_from_paths,
        should_use_container_lane, should_use_remote_lane, wait_for_worker_socket, write_json,
        BackendExecutionMetadata, BackendExecutionResult, ContainerExecutor,
        DigitalOceanDevEnvironment, ExecutionLaneRouter, ExecutionRequest, ExecutionSession,
        ExecutionUsageSummary, ExecutorBoundary, InProcessEchoExecutor, OmxExecutor,
        RemoteWorkerExecutor, RustSubprocessExecutor, WorkerOutcome, WorkerRequest, WorkerResponse,
    };

    #[test]
    fn normalizes_workers_ai_proxy_url_to_tasks_suffix() {
        let cases = [
            (
                "https://lab.bybuddha.dev/paperclip.ai",
                "https://lab.bybuddha.dev/paperclip.ai/tasks",
            ),
            (
                "https://lab.bybuddha.dev/paperclip.ai/",
                "https://lab.bybuddha.dev/paperclip.ai/tasks",
            ),
            (
                "https://lab.bybuddha.dev/paperclip.ai/tasks",
                "https://lab.bybuddha.dev/paperclip.ai/tasks",
            ),
            (
                "https://lab.bybuddha.dev/paperclip.ai/tasks/",
                "https://lab.bybuddha.dev/paperclip.ai/tasks",
            ),
            (
                "https://lab.bybuddha.dev/paperclip.ai/tasks/tasks",
                "https://lab.bybuddha.dev/paperclip.ai/tasks",
            ),
            (
                "https://lab.bybuddha.dev/paperclip.ai/tasks/tasks/tasks",
                "https://lab.bybuddha.dev/paperclip.ai/tasks",
            ),
            ("http://paperclip.ai", "http://paperclip.ai/tasks"),
            ("https://lab.bybuddha.dev", "https://lab.bybuddha.dev/tasks"),
            (
                "https://lab.bybuddha.dev/",
                "https://lab.bybuddha.dev/tasks",
            ),
        ];

        for (input, expected) in cases {
            let output = super::normalize_workers_ai_proxy_url(input).expect("should normalize");
            assert_eq!(
                output, expected,
                "input {input} should normalize to {expected}"
            );
        }
    }

    #[test]
    fn codex_fallback_only_accepts_usage_limit_errors() {
        assert!(is_codex_usage_limit_error("Codex usage limit reached"));
        assert!(is_codex_usage_limit_error("usage_limit exceeded"));
        assert!(!is_codex_usage_limit_error(
            "codex execution failed with exit status: 1"
        ));
    }

    #[test]
    fn echo_executor_returns_summary() {
        let temp = tempdir().unwrap();
        let session = build_execution_session(temp.path(), "main", "session-1", temp.path());
        session.ensure_layout().unwrap();
        let executor = InProcessEchoExecutor::new("Andy");
        let response = executor
            .execute(ExecutionRequest {
                group: crate::foundation::Group::main("Andy", "2026-04-05T00:00:00Z"),
                prompt: "<messages />".to_string(),
                paperclip_overlay_context: None,
                messages: vec![MessageRecord {
                    id: "m1".to_string(),
                    chat_jid: "main".to_string(),
                    sender: "user".to_string(),
                    sender_name: Some("User".to_string()),
                    content: "hello".to_string(),
                    timestamp: "2026-04-05T00:00:00Z".to_string(),
                    is_from_me: false,
                    is_bot_message: false,
                }],
                task_id: None,
                script: None,
                omx: None,
                assistant_name: "Andy".to_string(),
                request_plane: RequestPlane::Web,
                env: Default::default(),
                session,
                backend_override: None,
                task_signature: None,
                routing_decision: None,
                objective: None,
                plan: None,
                boundary_claims: Vec::new(),
                gate_evaluation: None,
            })
            .unwrap();

        assert!(response.text.contains("Andy received 1 message"));
    }

    #[test]
    fn worker_executes_script_and_writes_response() {
        let temp = tempdir().unwrap();
        let workspace_root = temp.path().join("workspace");
        fs::create_dir_all(&workspace_root).unwrap();
        fs::write(workspace_root.join("CLAUDE.md"), "# Andy\n").unwrap();
        let session = build_execution_session(temp.path(), "main", "session-1", &workspace_root);
        session.ensure_layout().unwrap();

        let request_path = Path::new(&session.ipc_root).join("request.json");
        let response_path = Path::new(&session.ipc_root).join("response.json");
        let request = WorkerRequest {
            invocation_id: "exec-1".to_string(),
            requested_at: "2026-04-05T00:00:00Z".to_string(),
            group: crate::foundation::Group::main("Andy", "2026-04-05T00:00:00Z"),
            prompt: "<task />".to_string(),
            paperclip_overlay_context: None,
            messages: Vec::new(),
            task_id: None,
            script: Some("printf 'hello from script'".to_string()),
            omx: None,
            assistant_name: "Andy".to_string(),
            request_plane: RequestPlane::Web,
            env: Default::default(),
            session: session.clone(),
            backend_override: None,
            task_signature: None,
            routing_decision: None,
            objective: None,
            plan: None,
            boundary_claims: Vec::new(),
            gate_evaluation: None,
        };
        fs::write(&request_path, serde_json::to_vec_pretty(&request).unwrap()).unwrap();

        run_worker_from_paths(&request_path, &response_path).unwrap();

        let response: WorkerResponse =
            serde_json::from_slice(&fs::read(&response_path).unwrap()).unwrap();
        assert_eq!(response.session_id, "session-1");
        assert!(response.text.contains("hello from script"));
        assert!(Path::new(response.log_path.as_deref().unwrap()).exists());
    }

    #[test]
    fn worker_daemon_reuses_session_across_connections() {
        let temp = tempdir().unwrap();
        let workspace_root = temp.path().join("workspace");
        fs::create_dir_all(&workspace_root).unwrap();
        fs::write(workspace_root.join("CLAUDE.md"), "# Andy\n").unwrap();
        let session = build_execution_session(temp.path(), "main", "session-1", &workspace_root);
        session.ensure_layout().unwrap();

        let session_root = PathBuf::from(&session.session_root);
        let daemon = thread::spawn(move || {
            run_worker_daemon_with_idle_timeout(&session_root, Duration::from_millis(150)).unwrap();
        });

        let send_request = |session: &ExecutionSession, invocation_id: &str, content: &str| {
            let mut stream = if invocation_id == "exec-1" {
                wait_for_worker_socket(session).unwrap()
            } else {
                connect_to_worker_socket(session).unwrap()
            };
            let request = WorkerRequest {
                invocation_id: invocation_id.to_string(),
                requested_at: "2026-04-05T00:00:00Z".to_string(),
                group: crate::foundation::Group::main("Andy", "2026-04-05T00:00:00Z"),
                prompt: "<messages />".to_string(),
                paperclip_overlay_context: None,
                messages: vec![MessageRecord {
                    id: invocation_id.to_string(),
                    chat_jid: "main".to_string(),
                    sender: "user".to_string(),
                    sender_name: Some("User".to_string()),
                    content: content.to_string(),
                    timestamp: "2026-04-05T00:00:00Z".to_string(),
                    is_from_me: false,
                    is_bot_message: false,
                }],
                task_id: None,
                script: None,
                omx: None,
                assistant_name: "Andy".to_string(),
                request_plane: RequestPlane::Web,
                env: Default::default(),
                session: session.clone(),
                backend_override: None,
                task_signature: None,
                routing_decision: None,
                objective: None,
                plan: None,
                boundary_claims: Vec::new(),
                gate_evaluation: None,
            };
            write_json(&mut stream, &request).unwrap();
            stream.shutdown(Shutdown::Write).unwrap();
            let outcome: WorkerOutcome = read_json(&mut stream).unwrap();
            outcome.response.unwrap()
        };

        let first = send_request(&session, "exec-1", "first");
        assert!(first.text.contains("Session turn: 1"));
        assert!(session.socket_path().exists());
        assert!(session.pid_path().exists());

        let second = send_request(&session, "exec-2", "second");
        assert!(second.text.contains("Session turn: 2"));

        thread::sleep(Duration::from_millis(250));
        daemon.join().unwrap();
        assert!(!session.socket_path().exists());
        assert!(!session.pid_path().exists());
    }

    #[test]
    fn container_lane_matches_group_rules() {
        let main = crate::foundation::Group::main("Andy", "2026-04-05T00:00:00Z");
        assert!(should_use_container_lane(&["all".to_string()], &main));
        assert!(should_use_container_lane(&["main".to_string()], &main));
        assert!(!should_use_container_lane(&["ops".to_string()], &main));
    }

    #[test]
    fn remote_lane_matches_mode_rules() {
        let main = crate::foundation::Group::main("Andy", "2026-04-05T00:00:00Z");
        let side = crate::foundation::Group {
            jid: "room-1".to_string(),
            name: "Room 1".to_string(),
            folder: "room_1".to_string(),
            trigger: "@Andy".to_string(),
            added_at: "2026-04-05T00:00:00Z".to_string(),
            requires_trigger: true,
            is_main: false,
        };
        assert!(should_use_remote_lane(&RemoteWorkerMode::Main, &main));
        assert!(!should_use_remote_lane(&RemoteWorkerMode::Main, &side));
        assert!(should_use_remote_lane(&RemoteWorkerMode::All, &side));
        assert!(should_use_remote_lane(
            &RemoteWorkerMode::Custom("room_1".to_string()),
            &side,
        ));
    }

    #[test]
    fn auto_lane_prefers_container_before_remote() {
        let main = crate::foundation::Group::main("Andy", "2026-04-05T00:00:00Z");
        let router = ExecutionLaneRouter {
            configured_lane: ExecutionLane::Auto,
            container_groups: vec!["main".to_string()],
            remote_worker_mode: RemoteWorkerMode::All,
            host: RustSubprocessExecutor::with_worker_binary("/tmp/unused"),
            container: ContainerExecutor {
                source_crate_root: PathBuf::from("/tmp/src"),
                container_image: "rust:1.75-slim".to_string(),
                container_runtime: Some("docker".to_string()),
                target_cache_root: PathBuf::from("/tmp/cache"),
            },
            remote: RemoteWorkerExecutor {
                dev_environment: DigitalOceanDevEnvironment::from_config(&NanoclawConfig {
                    project_root: PathBuf::from("/tmp"),
                    data_dir: PathBuf::from("/tmp/data"),
                    groups_dir: PathBuf::from("/tmp/groups"),
                    store_dir: PathBuf::from("/tmp/store"),
                    db_path: PathBuf::from("/tmp/store/messages.db"),
                    assistant_name: "Andy".to_string(),
                    default_trigger: "@Andy".to_string(),
                    timezone: "UTC".to_string(),
                    max_concurrent_groups: 1,
                    execution_lane: ExecutionLane::Auto,
                    container_image: "rust:1.75-slim".to_string(),
                    container_runtime: None,
                    container_groups: Vec::new(),
                    droplet_ssh_host: "127.0.0.1".to_string(),
                    droplet_ssh_user: "root".to_string(),
                    droplet_ssh_port: 22,
                    droplet_repo_root: "/srv/code-mirror".to_string(),
                    remote_worker_mode: RemoteWorkerMode::Off,
                    remote_worker_root: "/root/.nanoclaw-worker".to_string(),
                    remote_worker_binary: "/usr/local/bin/nanoclaw-rs".to_string(),
                    remote_worker_bootstrap_timeout_ms: 30_000,
                    remote_worker_sync_interval_ms: 500,
                    remote_worker_tunnel_port_base: 13_000,
                    omx_runner_path: "/usr/local/bin/omx-paperclip-runner".to_string(),
                    omx_state_root: "/root/.nanoclaw-omx".to_string(),
                    omx_callback_url: "http://127.0.0.1:8788/webhook/omx".to_string(),
                    omx_callback_token: String::new(),
                    omx_default_mode: "team".to_string(),
                    omx_default_max_workers: 3,
                    omx_poll_interval_ms: 5_000,
                    openclaw_gateway_bind_host: "127.0.0.1".to_string(),
                    openclaw_gateway_public_host: "127.0.0.1".to_string(),
                    openclaw_gateway_public_ws_url: None,
                    openclaw_gateway_public_health_url: None,
                    openclaw_gateway_port: 0,
                    openclaw_gateway_token: String::new(),
                    openclaw_gateway_execution_lane: ExecutionLane::Host,
                    slack_env_file: None,
                    slack_poll_interval_ms: 500,
                    linear_webhook_port: 0,
                    linear_webhook_secret: String::new(),
                    github_webhook_secret: String::new(),
                    linear_chat_jid: String::new(),
                    linear_api_key: String::new(),
                    linear_write_api_key: String::new(),
                    linear_pm_team_keys: Vec::new(),
                    linear_pm_policy_interval_minutes: 60,
                    linear_pm_digest_interval_hours: 6,
                    linear_pm_guardrail_max_automations: 10,
                    observability_chat_jid: String::new(),
                    observability_group_folder: "observability".to_string(),
                    observability_webhook_token: String::new(),
                    observability_auto_blue_team: true,
                    observability_adapters_path: PathBuf::from("/tmp/observability-adapters.json"),
                    sender_allowlist_path: PathBuf::from("/tmp/sender-allowlist.json"),
                    project_environments_path: PathBuf::from("/tmp/project-environments.json"),
                    host_os_control_policy_path: PathBuf::from("/tmp/host-os-control-policy.json"),
                    host_os_approval_chat_jid: None,
                    remote_control_ssh_host: None,
                    remote_control_ssh_user: None,
                    remote_control_ssh_port: 22,
                    remote_control_workspace_root: None,
                    development_environment: crate::foundation::DevelopmentEnvironment {
                        id: "digitalocean-droplet".to_string(),
                        name: "DigitalOcean VM".to_string(),
                        kind: crate::foundation::DevelopmentEnvironmentKind::DigitalOceanDroplet,
                        workspace_root: Some("/tmp".to_string()),
                        repo_root: Some("/srv/code-mirror".to_string()),
                        ssh: Some(crate::foundation::SshEndpoint {
                            host: "127.0.0.1".to_string(),
                            user: Some("root".to_string()),
                            port: 22,
                        }),
                        remote_worker_mode: RemoteWorkerMode::Off,
                        remote_worker_root: Some("/root/.nanoclaw-worker".to_string()),
                        bootstrap_timeout_ms: Some(30_000),
                        sync_interval_ms: Some(500),
                        tunnel_port_base: Some(13_000),
                    },
                }),
                remote_worker_root: "/root/.nanoclaw-worker".to_string(),
                remote_worker_binary: "/usr/local/bin/nanoclaw-rs".to_string(),
                remote_manifest_path: "Cargo.toml".to_string(),
            },
            omx: OmxExecutor::from_config(&NanoclawConfig::from_env()),
        };
        assert_eq!(router.lane_for_group(&main), ExecutionLane::Container);
    }

    #[test]
    fn omx_request_forces_omx_lane() {
        let temp = tempdir().unwrap();
        let workspace_root = temp.path().join("workspace");
        fs::create_dir_all(&workspace_root).unwrap();
        let session = build_execution_session(temp.path(), "main", "session-1", &workspace_root);
        let main = crate::foundation::Group::main("Andy", "2026-04-05T00:00:00Z");
        let router = ExecutionLaneRouter {
            configured_lane: ExecutionLane::Auto,
            container_groups: vec!["main".to_string()],
            remote_worker_mode: RemoteWorkerMode::All,
            host: RustSubprocessExecutor::with_worker_binary("/tmp/unused"),
            container: ContainerExecutor {
                source_crate_root: PathBuf::from("/tmp/src"),
                container_image: "rust:1.75-slim".to_string(),
                container_runtime: Some("docker".to_string()),
                target_cache_root: PathBuf::from("/tmp/cache"),
            },
            remote: RemoteWorkerExecutor {
                dev_environment: DigitalOceanDevEnvironment::from_config(&NanoclawConfig {
                    project_root: PathBuf::from("/tmp"),
                    data_dir: PathBuf::from("/tmp/data"),
                    groups_dir: PathBuf::from("/tmp/groups"),
                    store_dir: PathBuf::from("/tmp/store"),
                    db_path: PathBuf::from("/tmp/store/messages.db"),
                    assistant_name: "Andy".to_string(),
                    default_trigger: "@Andy".to_string(),
                    timezone: "UTC".to_string(),
                    max_concurrent_groups: 1,
                    execution_lane: ExecutionLane::Auto,
                    container_image: "rust:1.75-slim".to_string(),
                    container_runtime: None,
                    container_groups: Vec::new(),
                    droplet_ssh_host: "127.0.0.1".to_string(),
                    droplet_ssh_user: "root".to_string(),
                    droplet_ssh_port: 22,
                    droplet_repo_root: "/srv/code-mirror".to_string(),
                    remote_worker_mode: RemoteWorkerMode::Off,
                    remote_worker_root: "/root/.nanoclaw-worker".to_string(),
                    remote_worker_binary: "/usr/local/bin/nanoclaw-rs".to_string(),
                    remote_worker_bootstrap_timeout_ms: 30_000,
                    remote_worker_sync_interval_ms: 500,
                    remote_worker_tunnel_port_base: 13_000,
                    omx_runner_path: "/usr/local/bin/omx-paperclip-runner".to_string(),
                    omx_state_root: "/root/.nanoclaw-omx".to_string(),
                    omx_callback_url: "http://127.0.0.1:8788/webhook/omx".to_string(),
                    omx_callback_token: String::new(),
                    omx_default_mode: "team".to_string(),
                    omx_default_max_workers: 3,
                    omx_poll_interval_ms: 5_000,
                    openclaw_gateway_bind_host: "127.0.0.1".to_string(),
                    openclaw_gateway_public_host: "127.0.0.1".to_string(),
                    openclaw_gateway_public_ws_url: None,
                    openclaw_gateway_public_health_url: None,
                    openclaw_gateway_port: 0,
                    openclaw_gateway_token: String::new(),
                    openclaw_gateway_execution_lane: ExecutionLane::Host,
                    slack_env_file: None,
                    slack_poll_interval_ms: 500,
                    linear_webhook_port: 0,
                    linear_webhook_secret: String::new(),
                    github_webhook_secret: String::new(),
                    linear_chat_jid: String::new(),
                    linear_api_key: String::new(),
                    linear_write_api_key: String::new(),
                    linear_pm_team_keys: Vec::new(),
                    linear_pm_policy_interval_minutes: 60,
                    linear_pm_digest_interval_hours: 6,
                    linear_pm_guardrail_max_automations: 10,
                    observability_chat_jid: String::new(),
                    observability_group_folder: "observability".to_string(),
                    observability_webhook_token: String::new(),
                    observability_auto_blue_team: true,
                    observability_adapters_path: PathBuf::from("/tmp/observability-adapters.json"),
                    sender_allowlist_path: PathBuf::from("/tmp/sender-allowlist.json"),
                    project_environments_path: PathBuf::from("/tmp/project-environments.json"),
                    host_os_control_policy_path: PathBuf::from("/tmp/host-os-control-policy.json"),
                    host_os_approval_chat_jid: None,
                    remote_control_ssh_host: None,
                    remote_control_ssh_user: None,
                    remote_control_ssh_port: 22,
                    remote_control_workspace_root: None,
                    development_environment: crate::foundation::DevelopmentEnvironment {
                        id: "digitalocean-droplet".to_string(),
                        name: "DigitalOcean VM".to_string(),
                        kind: crate::foundation::DevelopmentEnvironmentKind::DigitalOceanDroplet,
                        workspace_root: Some("/tmp".to_string()),
                        repo_root: Some("/srv/code-mirror".to_string()),
                        ssh: Some(crate::foundation::SshEndpoint {
                            host: "127.0.0.1".to_string(),
                            user: Some("root".to_string()),
                            port: 22,
                        }),
                        remote_worker_mode: RemoteWorkerMode::Off,
                        remote_worker_root: Some("/root/.nanoclaw-worker".to_string()),
                        bootstrap_timeout_ms: Some(30_000),
                        sync_interval_ms: Some(500),
                        tunnel_port_base: Some(13_000),
                    },
                }),
                remote_worker_root: "/root/.nanoclaw-worker".to_string(),
                remote_worker_binary: "/usr/local/bin/nanoclaw-rs".to_string(),
                remote_manifest_path: "Cargo.toml".to_string(),
            },
            omx: OmxExecutor::from_config(&NanoclawConfig::from_env()),
        };
        let request = ExecutionRequest {
            group: main,
            prompt: "launch omx".to_string(),
            paperclip_overlay_context: None,
            messages: Vec::new(),
            task_id: None,
            script: None,
            omx: Some(crate::nanoclaw::omx::OmxExecutionOptions::default()),
            assistant_name: "Andy".to_string(),
            request_plane: RequestPlane::Web,
            env: Default::default(),
            session,
            backend_override: None,
            task_signature: None,
            routing_decision: None,
            objective: None,
            plan: None,
            boundary_claims: Vec::new(),
            gate_evaluation: None,
        };

        assert_eq!(router.lane_for_request(&request), ExecutionLane::Omx);
    }

    #[test]
    fn auto_lane_promotes_parallel_coding_requests_to_omx() {
        let temp = tempdir().unwrap();
        let workspace_root = temp.path().join("workspace");
        fs::create_dir_all(workspace_root.join(".git")).unwrap();
        let session = build_execution_session(temp.path(), "main", "session-1", &workspace_root);
        let router = ExecutionLaneRouter {
            configured_lane: ExecutionLane::Auto,
            container_groups: Vec::new(),
            remote_worker_mode: RemoteWorkerMode::Off,
            host: RustSubprocessExecutor::with_worker_binary("/tmp/unused"),
            container: ContainerExecutor {
                source_crate_root: PathBuf::from("/tmp/src"),
                container_image: "rust:1.75-slim".to_string(),
                container_runtime: Some("docker".to_string()),
                target_cache_root: PathBuf::from("/tmp/cache"),
            },
            remote: RemoteWorkerExecutor {
                dev_environment: DigitalOceanDevEnvironment::from_config(
                    &NanoclawConfig::from_env(),
                ),
                remote_worker_root: "/root/.nanoclaw-worker".to_string(),
                remote_worker_binary: "/usr/local/bin/nanoclaw-rs".to_string(),
                remote_manifest_path: "Cargo.toml".to_string(),
            },
            omx: OmxExecutor::from_config(&NanoclawConfig::from_env()),
        };
        let request = router.decorate_request(ExecutionRequest {
            group: crate::foundation::Group::main("Andy", "2026-04-05T00:00:00Z"),
            prompt: "refactor the rust repo and use a multi-agent codex swarm to finish it"
                .to_string(),
            paperclip_overlay_context: None,
            messages: Vec::new(),
            task_id: None,
            script: None,
            omx: None,
            assistant_name: "Andy".to_string(),
            request_plane: RequestPlane::Web,
            env: Default::default(),
            session,
            backend_override: None,
            task_signature: None,
            routing_decision: None,
            objective: None,
            plan: None,
            boundary_claims: Vec::new(),
            gate_evaluation: None,
        });

        assert_eq!(router.lane_for_request(&request), ExecutionLane::Omx);
        assert_eq!(
            request
                .routing_decision
                .as_ref()
                .map(|value| value.task_kind.clone()),
            Some(crate::foundation::TaskKind::Coding)
        );
    }

    #[test]
    fn paperclip_gateway_requests_default_codex_to_danger_full_access() {
        let temp = tempdir().unwrap();
        let workspace_root = temp.path().join("workspace");
        fs::create_dir_all(&workspace_root).unwrap();
        let session = build_execution_session(
            temp.path(),
            "paperclip_agent_ceo",
            "gateway-run",
            &workspace_root,
        );
        let request = WorkerRequest {
            invocation_id: "exec-paperclip".to_string(),
            requested_at: "2026-04-08T00:00:00Z".to_string(),
            group: crate::foundation::Group {
                jid: "paperclip:agent:ceo:paperclip".to_string(),
                name: "CEO".to_string(),
                folder: "paperclip_agent_ceo".to_string(),
                trigger: "@Andy".to_string(),
                added_at: "2026-04-08T00:00:00Z".to_string(),
                requires_trigger: true,
                is_main: false,
            },
            prompt: "Paperclip wake".to_string(),
            paperclip_overlay_context: None,
            messages: vec![MessageRecord {
                id: "paperclip:run".to_string(),
                chat_jid: "paperclip:agent:ceo:paperclip".to_string(),
                sender: "paperclip".to_string(),
                sender_name: Some("CEO".to_string()),
                content: "wake".to_string(),
                timestamp: "2026-04-08T00:00:00Z".to_string(),
                is_from_me: false,
                is_bot_message: true,
            }],
            task_id: None,
            script: None,
            omx: None,
            assistant_name: "Andy".to_string(),
            request_plane: RequestPlane::Web,
            env: Default::default(),
            session,
            backend_override: None,
            task_signature: None,
            routing_decision: None,
            objective: None,
            plan: None,
            boundary_claims: Vec::new(),
            gate_evaluation: None,
        };

        assert_eq!(
            default_codex_sandbox_for_request(&request),
            "danger-full-access"
        );
    }

    #[test]
    fn parses_codex_jsonl_usage_and_cost_metadata() {
        let parsed = parse_codex_jsonl(
            r#"{"type":"thread.started","thread_id":"thread-1","model":"gpt-5.4"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"hello"}}
{"type":"turn.completed","usage":{"input_tokens":123,"cached_input_tokens":45,"output_tokens":67},"total_cost_usd":0.123}"#,
        );

        assert_eq!(parsed.session_id.as_deref(), Some("thread-1"));
        assert_eq!(parsed.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(parsed.summary, "hello");
        assert_eq!(
            parsed.usage,
            Some(ExecutionUsageSummary {
                input_tokens: 123,
                cached_input_tokens: 45,
                output_tokens: 67,
            })
        );
        assert_eq!(parsed.cost_usd, Some(0.123));
    }

    #[test]
    fn builds_execution_metadata_from_backend_result() {
        let metadata = build_execution_metadata(
            &BackendExecutionResult {
                text: "ok".to_string(),
                log_body: "log".to_string(),
                metadata: BackendExecutionMetadata {
                    backend: WorkerBackend::Codex,
                    provider: Some("openai".to_string()),
                    biller: Some("chatgpt".to_string()),
                    billing_type: Some("subscription".to_string()),
                    model: Some("gpt-5.4".to_string()),
                    usage: Some(ExecutionUsageSummary {
                        input_tokens: 100,
                        cached_input_tokens: 25,
                        output_tokens: 50,
                    }),
                    cost_usd: Some(0.42),
                    effective_capabilities: CapabilityManifest::default(),
                    project_environment_id: None,
                    secret_handles: Vec::new(),
                    mount_summary: Vec::new(),
                    fallback_reason: None,
                },
            },
            None,
            None,
            None,
            None,
            None,
            None,
        );

        assert_eq!(metadata.backend.as_deref(), Some("codex"));
        assert_eq!(metadata.provider.as_deref(), Some("openai"));
        assert_eq!(metadata.biller.as_deref(), Some("chatgpt"));
        assert_eq!(metadata.billing_type.as_deref(), Some("subscription"));
        assert_eq!(metadata.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(
            metadata.usage,
            Some(ExecutionUsageSummary {
                input_tokens: 100,
                cached_input_tokens: 25,
                output_tokens: 50,
            })
        );
        assert_eq!(metadata.cost_usd, Some(0.42));
    }
}
