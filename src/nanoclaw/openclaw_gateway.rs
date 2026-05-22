use std::collections::{BTreeMap, HashMap};
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};
use tungstenite::{accept, Message, WebSocket};
use uuid::Uuid;

use crate::foundation::{
    ExecutionBoundary, ExecutionBoundaryKind, ExecutionLane, MessageRecord, RequestPlane,
};

use super::app::NanoclawApp;
use super::config::NanoclawConfig;
use super::executor::{
    build_execution_evidence, build_execution_session, BuildExecutionEvidenceInput,
    ExecutionArtifactRef, ExecutionEvidenceMode, ExecutionEvidenceStatus, ExecutionLaneRouter,
    ExecutionMetadata, ExecutionRequest, ExecutionResponse, ExecutionVerificationRef,
    ExecutorBoundary,
};
use super::model_router::WorkerBackend;
use super::omx::{
    apply_omx_webhook_payload, describe_omx_readiness, is_valid_omx_token,
    parse_omx_webhook_payload, OmxArtifactRef, OmxExecutionOptions, OmxMode, OmxRunnerClient,
    OmxWebhookPayload,
};

const PROTOCOL_VERSION: u64 = 3;
const DEFAULT_WAIT_TIMEOUT_MS: u64 = 30_000;
const RUN_POLL_INTERVAL_MS: u64 = 250;
const WAIT_KEEPALIVE_INTERVAL_MS: u64 = 2_000;

#[derive(Debug, Clone)]
pub struct OpenClawGatewayReadiness {
    pub enabled: bool,
    pub websocket_url: Option<String>,
    pub health_url: Option<String>,
    pub execution_lane: String,
}

#[derive(Debug, Clone)]
struct GatewayServerState {
    config: NanoclawConfig,
    runs: Arc<Mutex<HashMap<String, GatewayRunRecord>>>,
}

#[derive(Debug, Clone)]
struct GatewayRunRecord {
    run_id: String,
    status: GatewayRunStatus,
    accepted_at: String,
    updated_at: String,
    session_id: Option<String>,
    group_folder: Option<String>,
    lane: String,
    summary: Option<String>,
    result_text: Option<String>,
    metadata: Option<Value>,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewayRunStatus {
    Accepted,
    Running,
    Ok,
    Error,
}

impl GatewayRunStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }

    fn is_terminal(self) -> bool {
        matches!(self, Self::Ok | Self::Error)
    }
}

#[derive(Debug, Deserialize)]
struct GatewayEnvelope {
    #[serde(rename = "type")]
    frame_type: String,
    id: Option<String>,
    method: Option<String>,
    params: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayConnectParams {
    min_protocol: Option<u64>,
    max_protocol: Option<u64>,
    auth: Option<GatewayAuth>,
}

#[derive(Debug, Deserialize, Default)]
struct GatewayAuth {
    token: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayAgentParams {
    message: Option<String>,
    session_key: Option<String>,
    idempotency_key: Option<String>,
    paperclip: Option<GatewayPaperclipPayload>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayPaperclipPayload {
    run_id: Option<String>,
    agent_id: Option<String>,
    agent_name: Option<String>,
    task_id: Option<String>,
    issue_id: Option<String>,
    runtime_env: Option<HashMap<String, String>>,
    workspace: Option<GatewayPaperclipWorkspace>,
    workspaces: Option<Vec<GatewayPaperclipWorkspaceHint>>,
    wake: Option<GatewayPaperclipWake>,
    managed_context: Option<GatewayManagedContext>,
    gateway: Option<GatewayHints>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayPaperclipWorkspace {
    cwd: Option<String>,
    source: Option<String>,
    mode: Option<String>,
    strategy: Option<String>,
    provider_type: Option<String>,
    provider_ref: Option<String>,
    remote_provider: Option<String>,
    remote_workspace_ref: Option<String>,
    project_id: Option<String>,
    workspace_id: Option<String>,
    repo_url: Option<String>,
    repo_ref: Option<String>,
    branch_name: Option<String>,
    worktree_path: Option<String>,
    agent_home: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayPaperclipWorkspaceHint {
    workspace_id: Option<String>,
    cwd: Option<String>,
    repo_url: Option<String>,
    repo_ref: Option<String>,
    source_type: Option<String>,
    remote_provider: Option<String>,
    remote_workspace_ref: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayPaperclipWake {
    reason: Option<String>,
    issue: Option<GatewayPaperclipIssue>,
    comment_ids: Option<Vec<String>>,
    latest_comment_id: Option<String>,
    comments: Option<Vec<GatewayPaperclipComment>>,
    truncated: Option<bool>,
    fallback_fetch_needed: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayPaperclipIssue {
    id: Option<String>,
    identifier: Option<String>,
    title: Option<String>,
    status: Option<Value>,
    priority: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayPaperclipComment {
    id: Option<String>,
    issue_id: Option<String>,
    body: Option<String>,
    body_truncated: Option<bool>,
    created_at: Option<String>,
    author: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayManagedContext {
    instructions_bundle: Option<GatewayManagedInstructionsBundle>,
    skills: Option<Vec<GatewayManagedSkill>>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayManagedInstructionsBundle {
    entry_file: Option<String>,
    files: Option<Vec<GatewayManagedFile>>,
    notices: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayManagedSkill {
    key: Option<String>,
    runtime_name: Option<String>,
    required: Option<bool>,
    files: Option<Vec<GatewayManagedFile>>,
    notices: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayManagedFile {
    path: Option<String>,
    content: Option<String>,
    truncated: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayHints {
    execution_lane: Option<String>,
    omx_mode: Option<String>,
    max_workers: Option<usize>,
    #[serde(alias = "worker_backend")]
    worker_backend: Option<String>,
    model: Option<String>,
    #[serde(alias = "zai_model")]
    zai_model: Option<String>,
    #[serde(alias = "zai_base_url")]
    zai_base_url: Option<String>,
    #[serde(alias = "azure_model")]
    azure_model: Option<String>,
    #[serde(alias = "azure_deployment", alias = "azureDeploymentName")]
    azure_deployment: Option<String>,
    #[serde(alias = "azure_endpoint", alias = "azure_base_url")]
    azure_endpoint: Option<String>,
    #[serde(alias = "azure_api_version")]
    azure_api_version: Option<String>,
    #[serde(alias = "azure_fallback_backend", alias = "azureOpenAiFallbackBackend")]
    azure_fallback_backend: Option<String>,
    #[serde(
        alias = "codex_usage_fallback_backend",
        alias = "codexUsageFallbackBackend"
    )]
    codex_usage_fallback_backend: Option<String>,
    #[serde(alias = "github_copilot_repo")]
    github_copilot_repo: Option<String>,
    #[serde(alias = "github_copilot_base")]
    github_copilot_base: Option<String>,
    #[serde(alias = "github_copilot_custom_agent")]
    github_copilot_custom_agent: Option<String>,
    #[serde(alias = "github_copilot_follow")]
    github_copilot_follow: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GatewayWaitParams {
    run_id: Option<String>,
    timeout_ms: Option<u64>,
}

struct PlainHttpRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

struct ConnectionState {
    connected: bool,
    challenge_nonce: String,
}

pub fn describe_openclaw_gateway_readiness(config: &NanoclawConfig) -> Value {
    let readiness = gateway_readiness(config);
    json!({
        "ok": readiness.enabled,
        "enabled": readiness.enabled,
        "websocketUrl": readiness.websocket_url,
        "healthUrl": readiness.health_url,
        "executionLane": readiness.execution_lane,
        "tokenConfigured": !config.openclaw_gateway_token.trim().is_empty(),
    })
}

pub fn gateway_readiness(config: &NanoclawConfig) -> OpenClawGatewayReadiness {
    OpenClawGatewayReadiness {
        enabled: config.openclaw_gateway_port > 0
            && !config.openclaw_gateway_token.trim().is_empty(),
        websocket_url: config.openclaw_gateway_public_ws_url(),
        health_url: config.openclaw_gateway_public_health_url(),
        execution_lane: config.openclaw_gateway_execution_lane.as_str().to_string(),
    }
}

pub fn start_openclaw_gateway_server(config: NanoclawConfig) -> Result<()> {
    if config.openclaw_gateway_port == 0 {
        return Ok(());
    }
    if config.openclaw_gateway_token.trim().is_empty() {
        eprintln!("openclaw gateway disabled: missing NANOCLAW_OPENCLAW_GATEWAY_TOKEN");
        return Ok(());
    }

    let address = format!(
        "{}:{}",
        config.openclaw_gateway_bind_host, config.openclaw_gateway_port
    );
    let listener = TcpListener::bind(&address)
        .with_context(|| format!("failed to bind OpenClaw gateway on {address}"))?;
    let state = GatewayServerState {
        config: config.clone(),
        runs: Arc::new(Mutex::new(HashMap::new())),
    };

    thread::spawn(move || {
        eprintln!(
            "openclaw gateway listening on {} ws={} health={}",
            address,
            config
                .openclaw_gateway_public_ws_url()
                .unwrap_or_else(|| format!("ws://{address}")),
            config
                .openclaw_gateway_public_health_url()
                .unwrap_or_else(|| format!("http://{address}/health")),
        );
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let state = state.clone();
                    thread::spawn(move || {
                        if let Err(error) = handle_connection(stream, state) {
                            eprintln!("openclaw gateway connection failed: {error:#}");
                        }
                    });
                }
                Err(error) => eprintln!("openclaw gateway accept failed: {error:#}"),
            }
        }
    });

    Ok(())
}

fn handle_connection(mut stream: TcpStream, state: GatewayServerState) -> Result<()> {
    if is_plain_http_request(&stream)? {
        return handle_http_request(&mut stream, &state);
    }

    let mut socket = accept(stream).context("failed to accept websocket")?;
    let mut connection = ConnectionState {
        connected: false,
        challenge_nonce: Uuid::new_v4().to_string(),
    };
    write_json_frame(
        &mut socket,
        &json!({
            "type": "event",
            "event": "connect.challenge",
            "payload": {
                "nonce": connection.challenge_nonce,
                "protocol": PROTOCOL_VERSION,
            }
        }),
    )?;

    loop {
        let message = match socket.read() {
            Ok(message) => message,
            Err(tungstenite::Error::ConnectionClosed) => return Ok(()),
            Err(tungstenite::Error::AlreadyClosed) => return Ok(()),
            Err(error) => return Err(error).context("failed to read gateway websocket frame"),
        };

        match message {
            Message::Text(text) => {
                handle_text_frame(&mut socket, &state, &mut connection, &text)?;
            }
            Message::Binary(bytes) => {
                let text = String::from_utf8(bytes.to_vec())
                    .context("gateway binary frame was not valid utf8")?;
                handle_text_frame(&mut socket, &state, &mut connection, &text)?;
            }
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload))?;
            }
            Message::Close(_) => return Ok(()),
            _ => {}
        }
    }
}

fn handle_text_frame(
    socket: &mut WebSocket<TcpStream>,
    state: &GatewayServerState,
    connection: &mut ConnectionState,
    text: &str,
) -> Result<()> {
    let envelope: GatewayEnvelope =
        serde_json::from_str(text).context("failed to decode gateway frame")?;
    if envelope.frame_type != "req" {
        return Ok(());
    }
    let request_id = envelope.id.unwrap_or_else(|| Uuid::new_v4().to_string());
    let method = envelope.method.unwrap_or_default();
    let params = envelope.params.unwrap_or_else(|| json!({}));

    match method.as_str() {
        "connect" => handle_connect(socket, state, connection, &request_id, params),
        "agent" => {
            if !connection.connected {
                return write_error_frame(socket, &request_id, "unauthorized", "connect first");
            }
            handle_agent(socket, state, &request_id, params)
        }
        "agent.wait" => {
            if !connection.connected {
                return write_error_frame(socket, &request_id, "unauthorized", "connect first");
            }
            handle_agent_wait(socket, state, &request_id, params)
        }
        "device.pair.list" => write_ok_frame(
            socket,
            &request_id,
            json!({
                "pending": [],
                "approved": [],
            }),
        ),
        "device.pair.approve" => write_ok_frame(
            socket,
            &request_id,
            json!({
                "approved": true,
                "status": "ok",
            }),
        ),
        other => write_error_frame(
            socket,
            &request_id,
            "method_not_supported",
            &format!("unsupported method {other}"),
        ),
    }
}

fn handle_connect(
    socket: &mut WebSocket<TcpStream>,
    state: &GatewayServerState,
    connection: &mut ConnectionState,
    request_id: &str,
    params: Value,
) -> Result<()> {
    let parsed: GatewayConnectParams = serde_json::from_value(params).unwrap_or_default();
    let min_protocol = parsed.min_protocol.unwrap_or(PROTOCOL_VERSION);
    let max_protocol = parsed.max_protocol.unwrap_or(PROTOCOL_VERSION);
    if min_protocol > PROTOCOL_VERSION || max_protocol < PROTOCOL_VERSION {
        return write_error_frame(
            socket,
            request_id,
            "unsupported_protocol",
            "protocol version 3 is required",
        );
    }

    let provided_token = parsed
        .auth
        .as_ref()
        .and_then(|auth| auth.token.as_ref().or(auth.password.as_ref()))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if provided_token.as_deref() != Some(state.config.openclaw_gateway_token.trim()) {
        return write_error_frame(socket, request_id, "unauthorized", "invalid gateway token");
    }

    connection.connected = true;
    write_ok_frame(
        socket,
        request_id,
        json!({
            "protocol": PROTOCOL_VERSION,
            "status": "ok",
            "server": {
                "id": "nanoclaw-rust-openclaw-gateway",
                "version": env!("CARGO_PKG_VERSION"),
            }
        }),
    )
}

fn handle_agent(
    socket: &mut WebSocket<TcpStream>,
    state: &GatewayServerState,
    request_id: &str,
    params: Value,
) -> Result<()> {
    let parsed: GatewayAgentParams =
        serde_json::from_value(params).context("failed to decode agent params")?;
    let prompt = parsed
        .message
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .context("agent request is missing message")?;
    let run_id = parsed
        .idempotency_key
        .clone()
        .or_else(|| {
            parsed
                .paperclip
                .as_ref()
                .and_then(|paperclip| paperclip.run_id.clone())
        })
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    if let Some(existing) = find_run(&state.runs, &run_id)? {
        return write_ok_frame(socket, request_id, run_record_payload(&existing));
    }

    let now = Utc::now().to_rfc3339();
    let lane = resolve_gateway_execution_lane(&state.config, &parsed);
    upsert_run(
        &state.runs,
        GatewayRunRecord {
            run_id: run_id.clone(),
            status: GatewayRunStatus::Accepted,
            accepted_at: now.clone(),
            updated_at: now,
            session_id: None,
            group_folder: None,
            lane: lane.as_str().to_string(),
            summary: Some("Gateway run accepted.".to_string()),
            result_text: None,
            metadata: None,
            error: None,
        },
    )?;

    let state_clone = state.clone();
    let background_run_id = run_id.clone();
    thread::spawn(move || {
        if let Err(error) =
            execute_gateway_run(&state_clone, &background_run_id, &prompt, &parsed, lane)
        {
            let _ = mark_run_error(&state_clone.runs, &background_run_id, &error.to_string());
        }
    });

    if let Some(existing) = find_run(&state.runs, &run_id)? {
        return write_ok_frame(socket, request_id, run_record_payload(&existing));
    }

    write_ok_frame(
        socket,
        request_id,
        json!({
            "runId": run_id,
            "status": "accepted",
        }),
    )
}

fn handle_agent_wait(
    socket: &mut WebSocket<TcpStream>,
    state: &GatewayServerState,
    request_id: &str,
    params: Value,
) -> Result<()> {
    let parsed: GatewayWaitParams =
        serde_json::from_value(params).context("failed to decode agent.wait params")?;
    let run_id = parsed
        .run_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .context("agent.wait requires runId")?;
    let timeout_ms = parsed.timeout_ms.unwrap_or(DEFAULT_WAIT_TIMEOUT_MS);
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let keepalive_interval = Duration::from_millis(WAIT_KEEPALIVE_INTERVAL_MS);
    let mut last_keepalive = Instant::now()
        .checked_sub(keepalive_interval)
        .unwrap_or_else(Instant::now);
    socket
        .get_mut()
        .set_read_timeout(Some(Duration::from_millis(1)))
        .context("failed to set gateway wait read timeout")?;

    loop {
        if !drain_gateway_wait_control_frames(socket)? {
            return Ok(());
        }

        if let Some(existing) = find_run(&state.runs, &run_id)? {
            if existing.status.is_terminal() {
                emit_gateway_run_terminal_event(socket, &existing)?;
                return write_ok_frame(socket, request_id, run_record_payload(&existing));
            }

            if last_keepalive.elapsed() >= keepalive_interval {
                write_gateway_agent_event(
                    socket,
                    &run_id,
                    "lifecycle",
                    json!({
                        "phase": "running",
                        "status": existing.status.as_str(),
                        "summary": existing.summary,
                        "updatedAt": existing.updated_at,
                    }),
                )?;
                last_keepalive = Instant::now();
            }
        } else {
            return write_error_frame(socket, request_id, "not_found", "run not found");
        }

        if Instant::now() >= deadline {
            return write_ok_frame(
                socket,
                request_id,
                json!({
                    "runId": run_id,
                    "status": "timeout",
                }),
            );
        }
        thread::sleep(Duration::from_millis(RUN_POLL_INTERVAL_MS));
    }
}

fn write_gateway_agent_event(
    socket: &mut WebSocket<TcpStream>,
    run_id: &str,
    stream: &str,
    data: Value,
) -> Result<()> {
    write_json_frame(
        socket,
        &json!({
            "type": "event",
            "event": "agent",
            "payload": {
                "runId": run_id,
                "stream": stream,
                "data": data,
            },
        }),
    )
}

fn emit_gateway_run_terminal_event(
    socket: &mut WebSocket<TcpStream>,
    record: &GatewayRunRecord,
) -> Result<()> {
    let phase = if record.status == GatewayRunStatus::Ok {
        "completed"
    } else {
        "failed"
    };
    write_gateway_agent_event(
        socket,
        &record.run_id,
        "lifecycle",
        json!({
            "phase": phase,
            "status": record.status.as_str(),
            "summary": record.summary,
            "error": record.error,
            "sessionId": record.session_id,
            "updatedAt": record.updated_at,
        }),
    )?;

    if record.status == GatewayRunStatus::Ok {
        if let Some(text) = record
            .result_text
            .as_ref()
            .or(record.summary.as_ref())
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            write_gateway_agent_event(
                socket,
                &record.run_id,
                "assistant",
                json!({
                    "text": text,
                }),
            )?;
        }
    } else if let Some(error) = record.error.as_ref().filter(|value| !value.is_empty()) {
        write_gateway_agent_event(
            socket,
            &record.run_id,
            "error",
            json!({
                "error": error,
            }),
        )?;
    }

    Ok(())
}

fn drain_gateway_wait_control_frames(socket: &mut WebSocket<TcpStream>) -> Result<bool> {
    loop {
        match socket.read() {
            Ok(Message::Ping(payload)) => {
                socket.send(Message::Pong(payload))?;
            }
            Ok(Message::Close(_)) => return Ok(false),
            Ok(Message::Text(_)) | Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => {}
            Ok(_) => {}
            Err(tungstenite::Error::ConnectionClosed) => return Ok(false),
            Err(tungstenite::Error::AlreadyClosed) => return Ok(false),
            Err(tungstenite::Error::Io(error))
                if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
            {
                return Ok(true);
            }
            Err(error) => return Err(error).context("failed to read gateway wait control frame"),
        }
    }
}

fn execute_gateway_run(
    state: &GatewayServerState,
    run_id: &str,
    prompt: &str,
    params: &GatewayAgentParams,
    lane: ExecutionLane,
) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    {
        let mut runs = state
            .runs
            .lock()
            .map_err(|_| anyhow::anyhow!("failed to lock gateway run state"))?;
        if let Some(existing) = runs.get_mut(run_id) {
            existing.status = GatewayRunStatus::Running;
            existing.updated_at = now;
            existing.summary = Some("Gateway run started.".to_string());
        }
    }

    let mut app = NanoclawApp::open(state.config.clone())?;
    let identity = derive_group_identity(run_id, params);
    let group = app.ensure_group_for_chat(&identity.chat_jid, Some(&identity.group_name))?;
    let session_id = format!("gateway-{}", slug(run_id));
    let backend_override = select_gateway_worker_backend(
        forced_gateway_worker_backend_from_env(),
        gateway_backend_override(params),
        default_gateway_worker_backend_from_env(),
    );
    let effective_backend = backend_override.clone();
    if !matches!(effective_backend, Some(WorkerBackend::GithubCopilot)) {
        if let Some(target) = resolve_codespaces_gateway_target(params)? {
            let session = build_execution_session(
                &state.config.data_dir,
                &group.folder,
                &session_id,
                &PathBuf::from(&target.remote_cwd),
            );
            let mut runtime_env = paperclip_runtime_env(params.paperclip.as_ref());
            runtime_env.extend(gateway_runtime_env(params, backend_override.as_ref()));
            let runtime_env = codespaces_runtime_env(runtime_env);
            let prompt = render_codespaces_gateway_prompt(&render_gateway_remote_prompt(
                prompt,
                params.paperclip.as_ref(),
            ));
            let execution =
                execute_codespaces_gateway_run(&session, &target, &prompt, runtime_env)?;
            return mark_run_success(
                &state.runs,
                run_id,
                &group.folder,
                &execution,
                ExecutionLane::RemoteWorker,
            );
        }
    }

    let workspace_root = if matches!(effective_backend, Some(WorkerBackend::GithubCopilot)) {
        state.config.groups_dir.join(&group.folder)
    } else {
        resolve_gateway_workspace_root(&state.config, params, &group.folder)?
    };
    let session = build_execution_session(
        &state.config.data_dir,
        &group.folder,
        &session_id,
        &workspace_root,
    );
    let executor = ExecutionLaneRouter::from_config(&state.config, Some(lane.clone()))?;
    let mut runtime_env = paperclip_runtime_env(params.paperclip.as_ref());
    runtime_env.extend(gateway_runtime_env(params, backend_override.as_ref()));

    let request = ExecutionRequest {
        group: group.clone(),
        prompt: prompt.to_string(),
        paperclip_overlay_context: render_paperclip_managed_context(params.paperclip.as_ref()),
        messages: vec![MessageRecord {
            id: format!("paperclip:{run_id}"),
            chat_jid: identity.chat_jid.clone(),
            sender: "paperclip".to_string(),
            sender_name: Some(
                params
                    .paperclip
                    .as_ref()
                    .and_then(|paperclip| paperclip.agent_name.clone())
                    .unwrap_or_else(|| "Paperclip".to_string()),
            ),
            content: prompt.to_string(),
            timestamp: Utc::now().to_rfc3339(),
            is_from_me: false,
            is_bot_message: true,
        }],
        task_id: None,
        script: None,
        omx: gateway_omx_options(&state.config, params, &lane),
        assistant_name: state.config.assistant_name.clone(),
        request_plane: RequestPlane::Web,
        env: runtime_env,
        session,
        backend_override,
        task_signature: None,
        routing_decision: None,
        objective: None,
        plan: None,
        boundary_claims: Vec::new(),
        gate_evaluation: None,
    };

    let execution = executor.execute(request)?;
    mark_run_success(&state.runs, run_id, &group.folder, &execution, lane)
}

fn resolve_gateway_workspace_root(
    config: &NanoclawConfig,
    params: &GatewayAgentParams,
    group_folder: &str,
) -> Result<PathBuf> {
    if let Some(workspace) = paperclip_cloud_sandbox_workspace(params) {
        let provider = non_empty(workspace.remote_provider.as_deref())
            .or_else(|| non_empty(workspace.provider_type.as_deref()))
            .unwrap_or("cloud_sandbox");
        let reference = non_empty(workspace.remote_workspace_ref.as_deref())
            .or_else(|| non_empty(workspace.provider_ref.as_deref()))
            .unwrap_or("unconfigured");
        return Err(anyhow::anyhow!(
            "Paperclip requested remote {provider} workspace \"{reference}\", but this local OpenClaw gateway cannot execute inside remote sandboxes yet. Start the gateway inside that Codespace or enable a Codespaces SSH runner before waking this agent."
        ));
    }

    for (label, raw_path) in paperclip_workspace_candidates(params) {
        let path = PathBuf::from(raw_path.trim());
        if !path.is_absolute() {
            return Err(anyhow::anyhow!(
                "Paperclip {label} path must be absolute: {}",
                path.display()
            ));
        }
        if !path.is_dir() {
            if should_provision_paperclip_workspace(params, label, &path) {
                std::fs::create_dir_all(&path).with_context(|| {
                    format!(
                        "failed to provision Paperclip {label} directory: {}",
                        path.display()
                    )
                })?;
                eprintln!(
                    "openclaw gateway provisioned Paperclip {label} directory: {}",
                    path.display()
                );
                return Ok(path);
            }
            return Err(anyhow::anyhow!(
                "Paperclip {label} path is not a directory: {}",
                path.display()
            ));
        }
        return Ok(path);
    }

    Ok(config.groups_dir.join(group_folder))
}

fn should_provision_paperclip_workspace(
    params: &GatewayAgentParams,
    label: &str,
    path: &Path,
) -> bool {
    if label != "workspace.cwd" {
        return false;
    }
    let Some(workspace) = params
        .paperclip
        .as_ref()
        .and_then(|paperclip| paperclip.workspace.as_ref())
    else {
        return false;
    };

    let is_agent_home_source = non_empty(workspace.source.as_deref())
        .map(|source| source == "agent_home")
        .unwrap_or(false);
    let is_declared_agent_home = non_empty(workspace.agent_home.as_deref())
        .map(|agent_home| Path::new(agent_home) == path)
        .unwrap_or(false);
    if !is_agent_home_source && !is_declared_agent_home {
        return false;
    }

    non_empty(workspace.repo_url.as_deref()).is_none()
        && non_empty(workspace.worktree_path.as_deref()).is_none()
        && non_empty(workspace.workspace_id.as_deref()).is_none()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GatewayCodespacesTarget {
    provider: String,
    reference: String,
    remote_cwd: String,
    repo_url: Option<String>,
    repo_ref: Option<String>,
}

fn resolve_codespaces_gateway_target(
    params: &GatewayAgentParams,
) -> Result<Option<GatewayCodespacesTarget>> {
    let Some(workspace) = paperclip_cloud_sandbox_workspace(params) else {
        return Ok(None);
    };
    let provider = non_empty(workspace.remote_provider.as_deref())
        .or_else(|| non_empty(workspace.provider_type.as_deref()))
        .unwrap_or("cloud_sandbox")
        .to_string();
    if provider != "github_codespaces" {
        return Err(anyhow::anyhow!(
            "Paperclip requested remote {provider} workspace, but OpenClaw only supports github_codespaces cloud_sandbox execution today."
        ));
    }

    let reference = non_empty(workspace.remote_workspace_ref.as_deref())
        .or_else(|| non_empty(workspace.provider_ref.as_deref()))
        .map(str::to_string)
        .context("Paperclip requested github_codespaces execution without a Codespace name")?;
    let remote_cwd = non_empty(workspace.cwd.as_deref())
        .or_else(|| non_empty(workspace.worktree_path.as_deref()))
        .map(str::to_string)
        .or_else(|| {
            workspace
                .repo_url
                .as_deref()
                .and_then(default_codespaces_cwd_for_repo)
        })
        .context("Paperclip requested github_codespaces execution without a remote cwd")?;

    Ok(Some(GatewayCodespacesTarget {
        provider,
        reference,
        remote_cwd,
        repo_url: non_empty(workspace.repo_url.as_deref()).map(str::to_string),
        repo_ref: non_empty(workspace.repo_ref.as_deref()).map(str::to_string),
    }))
}

fn default_codespaces_cwd_for_repo(repo_url: &str) -> Option<String> {
    let trimmed = repo_url
        .trim()
        .trim_end_matches(".git")
        .trim_end_matches('/');
    let repo_name = trimmed.rsplit('/').next()?.trim();
    if repo_name.is_empty() {
        None
    } else {
        Some(format!("/workspaces/{repo_name}"))
    }
}

fn paperclip_cloud_sandbox_workspace(
    params: &GatewayAgentParams,
) -> Option<&GatewayPaperclipWorkspace> {
    let workspace = params.paperclip.as_ref()?.workspace.as_ref()?;
    let strategy = non_empty(workspace.strategy.as_deref());
    let mode = non_empty(workspace.mode.as_deref());
    let provider_type = non_empty(workspace.provider_type.as_deref());
    let remote_provider = non_empty(workspace.remote_provider.as_deref());
    if strategy == Some("cloud_sandbox")
        || mode == Some("cloud_sandbox")
        || provider_type == Some("cloud_sandbox")
        || remote_provider == Some("github_codespaces")
    {
        return Some(workspace);
    }
    None
}

fn paperclip_workspace_candidates(params: &GatewayAgentParams) -> Vec<(&'static str, String)> {
    let mut candidates = Vec::new();
    let Some(paperclip) = params.paperclip.as_ref() else {
        return candidates;
    };

    if let Some(workspace) = paperclip.workspace.as_ref() {
        push_non_empty_candidate(&mut candidates, "workspace.cwd", workspace.cwd.as_deref());
        push_non_empty_candidate(
            &mut candidates,
            "workspace.worktreePath",
            workspace.worktree_path.as_deref(),
        );
    }
    if let Some(workspaces) = paperclip.workspaces.as_ref() {
        for workspace in workspaces {
            push_non_empty_candidate(
                &mut candidates,
                "workspaces[].cwd",
                workspace.cwd.as_deref(),
            );
        }
    }

    candidates
}

fn push_non_empty_candidate(
    candidates: &mut Vec<(&'static str, String)>,
    label: &'static str,
    value: Option<&str>,
) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    candidates.push((label, value.to_string()));
}

fn render_gateway_remote_prompt(
    prompt: &str,
    paperclip: Option<&GatewayPaperclipPayload>,
) -> String {
    let overlay = render_paperclip_managed_context(paperclip);
    match overlay {
        Some(overlay) if !overlay.trim().is_empty() => format!(
            "{}\n\nPaperclip request:\n{}",
            overlay.trim(),
            prompt.trim()
        ),
        _ => prompt.to_string(),
    }
}

fn render_codespaces_gateway_prompt(prompt: &str) -> String {
    format!(
        r#"GitHub Codespaces remote execution handoff:
- You are running inside a GitHub Codespace, not inside the local Paperclip control plane.
- Treat the embedded Paperclip wake/context below as the source of truth for the issue and instructions.
- Do not call PAPERCLIP_API_URL, /api/agents/me, checkout, comment, patch, or status endpoints from this remote workspace.
- Do not mark the issue done or blocked through the API from this remote workspace.
- Return one concise final operator-facing result with evidence, blockers, and whether files changed. The local OpenClaw gateway will write that result back to Paperclip.
- If the issue request is only a smoke or trace, do only the requested inspection and avoid file edits.

{}"#,
        prompt.trim()
    )
}

fn execute_codespaces_gateway_run(
    session: &super::executor::ExecutionSession,
    target: &GatewayCodespacesTarget,
    prompt: &str,
    runtime_env: BTreeMap<String, String>,
) -> Result<ExecutionResponse> {
    session.ensure_layout()?;
    let gh_bin = std::env::var("NANOCLAW_CODESPACES_GH_BIN").unwrap_or_else(|_| "gh".to_string());
    let codex_auth_sync_status = sync_codespaces_codex_auth(&gh_bin, target)?;
    let remote_script = build_codespaces_codex_script(&target.remote_cwd, &runtime_env);
    let remote_command = build_codespaces_ssh_remote_command(&remote_script);
    let mut command = Command::new(&gh_bin);
    configure_codespaces_gh_auth(&mut command);
    command
        .arg("codespace")
        .arg("ssh")
        .arg("-c")
        .arg(&target.reference)
        .arg("--")
        .arg(&remote_command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn {}", describe_process_command(&command)))?;
    let write_result = match child.stdin.as_mut() {
        Some(stdin) => stdin.write_all(prompt.as_bytes()),
        None => Err(std::io::Error::new(
            ErrorKind::BrokenPipe,
            "gh codespace ssh stdin was unavailable",
        )),
    };
    drop(child.stdin.take());
    let output = child
        .wait_with_output()
        .with_context(|| format!("failed to wait for {}", describe_process_command(&command)))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let log_body = format!(
        "provider=github_codespaces\ncodespace={}\nremote_cwd={}\nrepo_url={}\nrepo_ref={}\ncodex_auth_sync={}\nstatus={}\nstdout=\n{}\nstderr=\n{}\n",
        target.reference,
        target.remote_cwd,
        target.repo_url.as_deref().unwrap_or("-"),
        target.repo_ref.as_deref().unwrap_or("-"),
        codex_auth_sync_status,
        output.status,
        stdout,
        stderr
    );
    let log_path = PathBuf::from(&session.logs_root).join("github-codespaces-codex.log");
    std::fs::write(&log_path, &log_body)
        .with_context(|| format!("failed to write {}", log_path.display()))?;

    if let Err(error) = write_result {
        return Err(anyhow::anyhow!(
            "failed to stream prompt to GitHub Codespace \"{}\": {}{}{}",
            target.reference,
            error,
            if stderr.is_empty() { "" } else { ": " },
            summarize_gateway_text(&stderr, 800)
        ));
    }
    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "GitHub Codespaces execution failed in \"{}\" with status {}{}",
            target.reference,
            output.status,
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {}", summarize_gateway_text(&stderr, 800))
            }
        ));
    }
    if stdout.trim().is_empty() {
        return Err(anyhow::anyhow!(
            "GitHub Codespaces execution in \"{}\" produced empty output{}",
            target.reference,
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {}", summarize_gateway_text(&stderr, 800))
            }
        ));
    }

    let boundary = ExecutionBoundary {
        kind: ExecutionBoundaryKind::RemoteWorker,
        root: Some(target.remote_cwd.clone()),
        isolated: true,
    };
    let log_path_string = log_path.display().to_string();
    let metadata = ExecutionMetadata {
        backend: Some("codex".to_string()),
        provider: Some("github_codespaces".to_string()),
        status: Some("completed".to_string()),
        summary: Some(stdout.clone()),
        ..Default::default()
    };
    let evidence = build_execution_evidence(BuildExecutionEvidenceInput {
        adapter_type: "openclaw_gateway",
        mode: ExecutionEvidenceMode::Gateway,
        run_id: session.id.as_str(),
        status: ExecutionEvidenceStatus::Succeeded,
        session_id: session.id.as_str(),
        group_folder: Some(session.group_folder.as_str()),
        workspace_root: Some(target.remote_cwd.as_str()),
        boundary: &boundary,
        log_path: Some(log_path_string.as_str()),
        log_body: Some(log_body.as_str()),
        metadata: Some(&metadata),
        provenance_id: None,
        verification: vec![ExecutionVerificationRef {
            kind: "command".to_string(),
            command: Some(describe_process_command(&command)),
            status: ExecutionEvidenceStatus::Succeeded.as_str().to_string(),
            summary: Some("GitHub Codespaces command completed successfully".to_string()),
        }],
        blockers: Vec::new(),
    });

    Ok(ExecutionResponse {
        text: stdout.clone(),
        boundary,
        session_id: session.id.clone(),
        log_path: Some(log_path_string),
        log_body: Some(log_body),
        provenance: None,
        metadata: Some(metadata),
        evidence: Some(evidence),
    })
}

fn sync_codespaces_codex_auth(
    gh_bin: &str,
    target: &GatewayCodespacesTarget,
) -> Result<&'static str> {
    let Some(codex_home) = local_codespaces_codex_home() else {
        return Ok("skipped_no_codex_home");
    };
    let auth_files = codespaces_codex_auth_files(&codex_home);
    if auth_files.is_empty() {
        return Ok("skipped_no_codex_auth");
    }

    let mut tar = Command::new("tar");
    tar.arg("-C").arg(&codex_home).arg("-cf").arg("-");
    for file_name in &auth_files {
        tar.arg(file_name);
    }
    tar.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut tar_child = tar
        .spawn()
        .with_context(|| format!("failed to spawn {}", describe_process_command(&tar)))?;
    let mut tar_stdout = tar_child.stdout.take().ok_or_else(|| {
        anyhow::anyhow!("tar stdout was unavailable while preparing Codex auth sync")
    })?;

    let remote_script = r#"set -eo pipefail
mkdir -p "$HOME/.codex"
tar -C "$HOME/.codex" -xf -
chmod 700 "$HOME/.codex"
find "$HOME/.codex" -maxdepth 1 -type f -exec chmod 600 {} \;
"#;
    let remote_command = build_codespaces_ssh_remote_command(remote_script);
    let mut command = Command::new(gh_bin);
    configure_codespaces_gh_auth(&mut command);
    command
        .arg("codespace")
        .arg("ssh")
        .arg("-c")
        .arg(&target.reference)
        .arg("--")
        .arg(&remote_command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut gh_child = command
        .spawn()
        .with_context(|| format!("failed to spawn {}", describe_process_command(&command)))?;
    let mut gh_stdin = gh_child.stdin.take().ok_or_else(|| {
        anyhow::anyhow!("gh codespace ssh stdin was unavailable while syncing Codex auth")
    })?;
    let copy_result = std::io::copy(&mut tar_stdout, &mut gh_stdin);
    drop(gh_stdin);

    let tar_output = tar_child
        .wait_with_output()
        .context("failed to wait for Codex auth tar process")?;
    let gh_output = gh_child
        .wait_with_output()
        .context("failed to wait for GitHub Codespaces Codex auth sync")?;

    if let Err(error) = copy_result {
        return Err(anyhow::anyhow!(
            "failed to stream local Codex auth into GitHub Codespace \"{}\": {}",
            target.reference,
            error
        ));
    }
    if !tar_output.status.success() {
        let stderr = String::from_utf8_lossy(&tar_output.stderr);
        return Err(anyhow::anyhow!(
            "failed to package local Codex auth from {} with status {}{}",
            codex_home.display(),
            tar_output.status,
            if stderr.trim().is_empty() {
                String::new()
            } else {
                format!(": {}", summarize_gateway_text(&stderr, 400))
            }
        ));
    }
    if !gh_output.status.success() {
        let stderr = String::from_utf8_lossy(&gh_output.stderr);
        return Err(anyhow::anyhow!(
            "failed to sync Codex auth into GitHub Codespace \"{}\" with status {}{}",
            target.reference,
            gh_output.status,
            if stderr.trim().is_empty() {
                String::new()
            } else {
                format!(": {}", summarize_gateway_text(&stderr, 400))
            }
        ));
    }

    Ok("synced")
}

fn local_codespaces_codex_home() -> Option<PathBuf> {
    for key in ["NANOCLAW_CODESPACES_CODEX_HOME", "CODEX_HOME"] {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(PathBuf::from(trimmed));
            }
        }
    }
    std::env::var("HOME")
        .ok()
        .map(|home| PathBuf::from(home).join(".codex"))
}

fn codespaces_codex_auth_files(codex_home: &Path) -> Vec<&'static str> {
    ["auth.json", "config.toml"]
        .into_iter()
        .filter(|file_name| codex_home.join(file_name).is_file())
        .collect()
}

fn configure_codespaces_gh_auth(command: &mut Command) {
    if let Ok(gh_config_dir) = std::env::var("NANOCLAW_CODESPACES_GH_CONFIG_DIR")
        .or_else(|_| std::env::var("GH_CONFIG_DIR"))
    {
        let trimmed = gh_config_dir.trim();
        if !trimmed.is_empty() {
            command.env("GH_CONFIG_DIR", trimmed);
        }
    }
    if let Ok(token) = std::env::var("GH_TOKEN").or_else(|_| std::env::var("GITHUB_TOKEN")) {
        let trimmed = token.trim();
        if !trimmed.is_empty() {
            command.env("GH_TOKEN", trimmed);
        }
    }
}

fn build_codespaces_ssh_remote_command(remote_script: &str) -> String {
    // gh codespace ssh forwards trailing args through an SSH shell; keep the
    // bash command as one quoted argument so multi-line scripts are not split.
    format!("bash -lc {}", shell_quote_gateway(remote_script))
}

fn build_codespaces_codex_script(
    remote_cwd: &str,
    runtime_env: &BTreeMap<String, String>,
) -> String {
    let codex_bin =
        std::env::var("NANOCLAW_CODESPACES_CODEX_BIN").unwrap_or_else(|_| "codex".to_string());
    let sandbox = std::env::var("NANOCLAW_CODESPACES_CODEX_SANDBOX")
        .unwrap_or_else(|_| "workspace-write".to_string());
    let sandbox_args = if sandbox == "danger-full-access" {
        "--dangerously-bypass-approvals-and-sandbox".to_string()
    } else {
        format!("-s {}", shell_quote_gateway(&sandbox))
    };
    let exports = runtime_env
        .iter()
        .filter(|(key, _)| is_codespaces_export_env_key(key))
        .map(|(key, value)| format!("export {key}={}", shell_quote_gateway(value)))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"set -eo pipefail
cd {remote_cwd}
if ! command -v {codex_bin} >/dev/null 2>&1; then
  if ! command -v npx >/dev/null 2>&1; then
    echo "codex CLI is not installed in this Codespace and npx is unavailable; install Codex in the devcontainer or set NANOCLAW_CODESPACES_CODEX_BIN." >&2
    exit 127
  fi
fi
run_codex() {{
  if command -v {codex_bin} >/dev/null 2>&1; then
    {codex_bin} "$@"
  else
    npx -y @openai/codex "$@"
  fi
}}
prompt_file="$(mktemp)"
output_file="$(mktemp)"
trap 'rm -f "$prompt_file" "$output_file"' EXIT
cat > "$prompt_file"
{exports}
run_codex -a never exec --json --skip-git-repo-check --color never {sandbox_args} -C {remote_cwd} -o "$output_file" "$(cat "$prompt_file")" >&2
if [ ! -s "$output_file" ]; then
  echo "codex execution produced an empty output file" >&2
  exit 65
fi
cat "$output_file"
"#,
        remote_cwd = shell_quote_gateway(remote_cwd),
        codex_bin = shell_quote_gateway(&codex_bin),
        exports = exports,
        sandbox_args = sandbox_args
    )
}

fn is_safe_env_key(key: &str) -> bool {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn is_codespaces_export_env_key(key: &str) -> bool {
    if !is_safe_env_key(key) {
        return false;
    }
    let upper = key.to_ascii_uppercase();
    !upper.contains("KEY")
        && !upper.contains("TOKEN")
        && !upper.contains("SECRET")
        && !upper.contains("PASSWORD")
        && !upper.ends_with("_AUTH")
}

fn shell_quote_gateway(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn describe_process_command(command: &Command) -> String {
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

fn summarize_gateway_text(value: &str, max_bytes: usize) -> String {
    let trimmed = value.trim();
    if trimmed.len() <= max_bytes {
        return trimmed.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}... [truncated {} bytes]",
        &trimmed[..end],
        trimmed.len() - end
    )
}

fn paperclip_runtime_env(paperclip: Option<&GatewayPaperclipPayload>) -> BTreeMap<String, String> {
    const ALLOWED_KEYS: &[&str] = &[
        "PAPERCLIP_RUN_ID",
        "PAPERCLIP_AGENT_ID",
        "PAPERCLIP_COMPANY_ID",
        "PAPERCLIP_API_URL",
        "PAPERCLIP_API_KEY",
        "PAPERCLIP_TASK_ID",
        "PAPERCLIP_WAKE_REASON",
        "PAPERCLIP_WAKE_COMMENT_ID",
        "PAPERCLIP_APPROVAL_ID",
        "PAPERCLIP_APPROVAL_STATUS",
        "PAPERCLIP_LINKED_ISSUE_IDS",
        "ZAI_ANTHROPIC_AUTH_TOKEN",
        "NANOCLAW_ZAI_ANTHROPIC_AUTH_TOKEN",
        "ZAI_API_KEY",
        "NANOCLAW_ZAI_API_KEY",
        "ZAI_ANTHROPIC_BASE_URL",
        "NANOCLAW_ZAI_ANTHROPIC_BASE_URL",
        "ZAI_ANTHROPIC_MODEL",
        "NANOCLAW_ZAI_MODEL",
        "AZURE_OPENAI_API_KEY",
        "NANOCLAW_AZURE_OPENAI_API_KEY",
        "AZURE_AI_FOUNDRY_API_KEY",
        "NANOCLAW_AZURE_AI_FOUNDRY_API_KEY",
        "AZURE_FOUNDRY_API_KEY",
        "NANOCLAW_AZURE_FOUNDRY_API_KEY",
        "AZURE_AI_API_KEY",
        "NANOCLAW_AZURE_AI_API_KEY",
        "AZURE_OPENAI_ENDPOINT",
        "NANOCLAW_AZURE_OPENAI_ENDPOINT",
        "AZURE_AI_FOUNDRY_ENDPOINT",
        "NANOCLAW_AZURE_AI_FOUNDRY_ENDPOINT",
        "AZURE_FOUNDRY_ENDPOINT",
        "NANOCLAW_AZURE_FOUNDRY_ENDPOINT",
        "AZURE_OPENAI_BASE_URL",
        "NANOCLAW_AZURE_OPENAI_BASE_URL",
        "AZURE_AI_FOUNDRY_BASE_URL",
        "NANOCLAW_AZURE_AI_FOUNDRY_BASE_URL",
        "AZURE_FOUNDRY_BASE_URL",
        "NANOCLAW_AZURE_FOUNDRY_BASE_URL",
        "AZURE_OPENAI_DEPLOYMENT",
        "NANOCLAW_AZURE_OPENAI_DEPLOYMENT",
        "AZURE_AI_FOUNDRY_DEPLOYMENT",
        "NANOCLAW_AZURE_AI_FOUNDRY_DEPLOYMENT",
        "AZURE_FOUNDRY_DEPLOYMENT",
        "NANOCLAW_AZURE_FOUNDRY_DEPLOYMENT",
        "AZURE_OPENAI_MODEL",
        "NANOCLAW_AZURE_OPENAI_MODEL",
        "AZURE_AI_FOUNDRY_MODEL",
        "NANOCLAW_AZURE_AI_FOUNDRY_MODEL",
        "AZURE_FOUNDRY_MODEL",
        "NANOCLAW_AZURE_FOUNDRY_MODEL",
        "AZURE_OPENAI_DEPLOYMENT_NAME",
        "NANOCLAW_AZURE_OPENAI_DEPLOYMENT_NAME",
        "AZURE_AI_FOUNDRY_DEPLOYMENT_NAME",
        "NANOCLAW_AZURE_AI_FOUNDRY_DEPLOYMENT_NAME",
        "AZURE_OPENAI_API_VERSION",
        "NANOCLAW_AZURE_OPENAI_API_VERSION",
        "AZURE_AI_FOUNDRY_API_VERSION",
        "NANOCLAW_AZURE_AI_FOUNDRY_API_VERSION",
        "AZURE_FOUNDRY_API_VERSION",
        "NANOCLAW_AZURE_FOUNDRY_API_VERSION",
        "NANOCLAW_AZURE_OPENAI_RATE_CARD_JSON",
        "AZURE_OPENAI_RATE_CARD_JSON",
        "NANOCLAW_AZURE_AI_FOUNDRY_RATE_CARD_JSON",
        "AZURE_AI_FOUNDRY_RATE_CARD_JSON",
        "NANOCLAW_AZURE_FOUNDRY_RATE_CARD_JSON",
        "AZURE_FOUNDRY_RATE_CARD_JSON",
        "NANOCLAW_AZURE_OPENAI_FALLBACK_BACKEND",
        "NANOCLAW_AZURE_FALLBACK_BACKEND",
    ];

    let mut env = BTreeMap::new();
    let Some(runtime_env) = paperclip.and_then(|value| value.runtime_env.as_ref()) else {
        return env;
    };

    for (key, value) in runtime_env {
        if !ALLOWED_KEYS.iter().any(|allowed| allowed == key) {
            continue;
        }
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            env.insert(key.clone(), trimmed.to_string());
        }
    }
    env
}

fn codespaces_runtime_env(mut env: BTreeMap<String, String>) -> BTreeMap<String, String> {
    // Remote Codespaces agents return their final output to this local gateway.
    // They should not receive local-only Paperclip API credentials for 127.0.0.1,
    // nor provider secrets that are not needed by the Codex SSH runner.
    env.remove("PAPERCLIP_API_URL");
    env.remove("PAPERCLIP_API_KEY");
    env.remove("ZAI_ANTHROPIC_AUTH_TOKEN");
    env.remove("NANOCLAW_ZAI_ANTHROPIC_AUTH_TOKEN");
    env.remove("ZAI_API_KEY");
    env.remove("NANOCLAW_ZAI_API_KEY");
    env.remove("ZAI_ANTHROPIC_BASE_URL");
    env.remove("NANOCLAW_ZAI_ANTHROPIC_BASE_URL");
    env.remove("ZAI_ANTHROPIC_MODEL");
    env.remove("NANOCLAW_ZAI_MODEL");
    env.remove("AZURE_OPENAI_API_KEY");
    env.remove("NANOCLAW_AZURE_OPENAI_API_KEY");
    env.remove("AZURE_AI_FOUNDRY_API_KEY");
    env.remove("NANOCLAW_AZURE_AI_FOUNDRY_API_KEY");
    env.remove("AZURE_FOUNDRY_API_KEY");
    env.remove("NANOCLAW_AZURE_FOUNDRY_API_KEY");
    env.remove("AZURE_AI_API_KEY");
    env.remove("NANOCLAW_AZURE_AI_API_KEY");
    env.remove("AZURE_OPENAI_ENDPOINT");
    env.remove("NANOCLAW_AZURE_OPENAI_ENDPOINT");
    env.remove("AZURE_AI_FOUNDRY_ENDPOINT");
    env.remove("NANOCLAW_AZURE_AI_FOUNDRY_ENDPOINT");
    env.remove("AZURE_FOUNDRY_ENDPOINT");
    env.remove("NANOCLAW_AZURE_FOUNDRY_ENDPOINT");
    env.remove("AZURE_OPENAI_BASE_URL");
    env.remove("NANOCLAW_AZURE_OPENAI_BASE_URL");
    env.remove("AZURE_AI_FOUNDRY_BASE_URL");
    env.remove("NANOCLAW_AZURE_AI_FOUNDRY_BASE_URL");
    env.remove("AZURE_FOUNDRY_BASE_URL");
    env.remove("NANOCLAW_AZURE_FOUNDRY_BASE_URL");
    env.remove("AZURE_OPENAI_DEPLOYMENT");
    env.remove("NANOCLAW_AZURE_OPENAI_DEPLOYMENT");
    env.remove("AZURE_AI_FOUNDRY_DEPLOYMENT");
    env.remove("NANOCLAW_AZURE_AI_FOUNDRY_DEPLOYMENT");
    env.remove("AZURE_FOUNDRY_DEPLOYMENT");
    env.remove("NANOCLAW_AZURE_FOUNDRY_DEPLOYMENT");
    env.remove("AZURE_OPENAI_MODEL");
    env.remove("NANOCLAW_AZURE_OPENAI_MODEL");
    env.remove("AZURE_AI_FOUNDRY_MODEL");
    env.remove("NANOCLAW_AZURE_AI_FOUNDRY_MODEL");
    env.remove("AZURE_FOUNDRY_MODEL");
    env.remove("NANOCLAW_AZURE_FOUNDRY_MODEL");
    env.remove("AZURE_OPENAI_DEPLOYMENT_NAME");
    env.remove("NANOCLAW_AZURE_OPENAI_DEPLOYMENT_NAME");
    env.remove("AZURE_AI_FOUNDRY_DEPLOYMENT_NAME");
    env.remove("NANOCLAW_AZURE_AI_FOUNDRY_DEPLOYMENT_NAME");
    env.remove("AZURE_OPENAI_API_VERSION");
    env.remove("NANOCLAW_AZURE_OPENAI_API_VERSION");
    env.remove("AZURE_AI_FOUNDRY_API_VERSION");
    env.remove("NANOCLAW_AZURE_AI_FOUNDRY_API_VERSION");
    env.remove("AZURE_FOUNDRY_API_VERSION");
    env.remove("NANOCLAW_AZURE_FOUNDRY_API_VERSION");
    env.remove("NANOCLAW_AZURE_OPENAI_RATE_CARD_JSON");
    env.remove("AZURE_OPENAI_RATE_CARD_JSON");
    env.remove("NANOCLAW_AZURE_AI_FOUNDRY_RATE_CARD_JSON");
    env.remove("AZURE_AI_FOUNDRY_RATE_CARD_JSON");
    env.remove("NANOCLAW_AZURE_FOUNDRY_RATE_CARD_JSON");
    env.remove("AZURE_FOUNDRY_RATE_CARD_JSON");
    env.insert(
        "PAPERCLIP_REMOTE_HANDOFF".to_string(),
        "gateway_writeback".to_string(),
    );
    env
}

fn gateway_backend_override(params: &GatewayAgentParams) -> Option<WorkerBackend> {
    params
        .paperclip
        .as_ref()
        .and_then(|paperclip| paperclip.gateway.as_ref())
        .and_then(|gateway| gateway.worker_backend.as_deref())
        .map(WorkerBackend::parse)
}

fn forced_gateway_worker_backend_from_env() -> Option<WorkerBackend> {
    std::env::var("NANOCLAW_FORCE_WORKER_BACKEND")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(|value| WorkerBackend::parse(&value))
}

fn default_gateway_worker_backend_from_env() -> Option<WorkerBackend> {
    std::env::var("NANOCLAW_WORKER_BACKEND")
        .or_else(|_| std::env::var("NANOCLAW_MODEL_BACKEND"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(|value| WorkerBackend::parse(&value))
}

fn select_gateway_worker_backend(
    forced: Option<WorkerBackend>,
    hinted: Option<WorkerBackend>,
    default: Option<WorkerBackend>,
) -> Option<WorkerBackend> {
    forced.or(hinted).or(default)
}

fn gateway_runtime_env(
    params: &GatewayAgentParams,
    effective_backend: Option<&WorkerBackend>,
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    let gateway = params
        .paperclip
        .as_ref()
        .and_then(|paperclip| paperclip.gateway.as_ref());
    let payload_backend_hint = gateway
        .and_then(|gateway| gateway.worker_backend.as_deref())
        .map(WorkerBackend::parse);
    let effective_backend = effective_backend.cloned();
    let suppress_generic_model = matches!(
        (effective_backend.as_ref(), payload_backend_hint.as_ref()),
        (Some(effective), Some(payload)) if effective != payload
    );
    let backend_hint = effective_backend.or(payload_backend_hint);

    if let Some(gateway) = gateway {
        if let Some(model) = gateway
            .zai_model
            .as_deref()
            .or_else(|| {
                (!suppress_generic_model
                    && !matches!(backend_hint.as_ref(), Some(WorkerBackend::AzureOpenAI)))
                .then_some(gateway.model.as_deref())
                .flatten()
            })
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            env.insert("NANOCLAW_ZAI_MODEL".to_string(), model.to_string());
        }
        if let Some(base_url) = gateway
            .zai_base_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            env.insert(
                "NANOCLAW_ZAI_ANTHROPIC_BASE_URL".to_string(),
                base_url.to_string(),
            );
        }
        if let Some(deployment) = gateway
            .azure_deployment
            .as_deref()
            .or(gateway.azure_model.as_deref())
            .or_else(|| {
                (!suppress_generic_model
                    && matches!(backend_hint.as_ref(), Some(WorkerBackend::AzureOpenAI)))
                .then_some(gateway.model.as_deref())
                .flatten()
            })
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            env.insert(
                "NANOCLAW_AZURE_OPENAI_DEPLOYMENT".to_string(),
                deployment.to_string(),
            );
        }
        if let Some(endpoint) = gateway
            .azure_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            env.insert(
                "NANOCLAW_AZURE_OPENAI_ENDPOINT".to_string(),
                endpoint.to_string(),
            );
        }
        if let Some(api_version) = gateway
            .azure_api_version
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            env.insert(
                "NANOCLAW_AZURE_OPENAI_API_VERSION".to_string(),
                api_version.to_string(),
            );
        }
        if let Some(fallback_backend) = gateway
            .azure_fallback_backend
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            env.insert(
                "NANOCLAW_AZURE_OPENAI_FALLBACK_BACKEND".to_string(),
                fallback_backend.to_string(),
            );
        }
        if let Some(fallback_backend) = gateway
            .codex_usage_fallback_backend
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            env.insert(
                "NANOCLAW_CODEX_USAGE_FALLBACK_BACKEND".to_string(),
                fallback_backend.to_string(),
            );
        }
        if let Some(custom_agent) = gateway
            .github_copilot_custom_agent
            .as_deref()
            .and_then(|value| non_empty(Some(value)))
        {
            env.insert(
                "NANOCLAW_GITHUB_COPILOT_CUSTOM_AGENT".to_string(),
                custom_agent.to_string(),
            );
        }
        if let Some(follow) = gateway.github_copilot_follow {
            env.insert(
                "NANOCLAW_GITHUB_COPILOT_FOLLOW".to_string(),
                follow.to_string(),
            );
        }
    }

    if let Some(repo) = gateway
        .and_then(|gateway| gateway.github_copilot_repo.as_deref())
        .and_then(|value| non_empty(Some(value)))
        .or_else(|| gateway_github_copilot_repo(params))
    {
        env.insert("NANOCLAW_GITHUB_COPILOT_REPO".to_string(), repo.to_string());
    }
    if let Some(base) = gateway
        .and_then(|gateway| gateway.github_copilot_base.as_deref())
        .and_then(|value| non_empty(Some(value)))
        .or_else(|| gateway_github_copilot_base(params))
    {
        env.insert("NANOCLAW_GITHUB_COPILOT_BASE".to_string(), base.to_string());
    }

    env
}

fn gateway_github_copilot_repo(params: &GatewayAgentParams) -> Option<&str> {
    let paperclip = params.paperclip.as_ref()?;
    paperclip
        .workspace
        .as_ref()
        .and_then(|workspace| non_empty(workspace.repo_url.as_deref()))
        .or_else(|| {
            paperclip
                .workspaces
                .as_ref()?
                .iter()
                .find_map(|workspace| non_empty(workspace.repo_url.as_deref()))
        })
}

fn gateway_github_copilot_base(params: &GatewayAgentParams) -> Option<&str> {
    let paperclip = params.paperclip.as_ref()?;
    paperclip
        .workspace
        .as_ref()
        .and_then(|workspace| {
            non_empty(workspace.branch_name.as_deref())
                .or_else(|| non_empty(workspace.repo_ref.as_deref()))
        })
        .or_else(|| {
            paperclip
                .workspaces
                .as_ref()?
                .iter()
                .find_map(|workspace| non_empty(workspace.repo_ref.as_deref()))
        })
}

fn render_paperclip_managed_context(paperclip: Option<&GatewayPaperclipPayload>) -> Option<String> {
    let mut sections: Vec<String> = Vec::new();

    if let Some(wake_context) = render_paperclip_wake_context(paperclip) {
        sections.push(wake_context);
    }

    if let Some(workspace_context) = render_paperclip_workspace_context(paperclip) {
        sections.push(workspace_context);
    }

    if let Some(managed) = paperclip.and_then(|value| value.managed_context.as_ref()) {
        if let Some(bundle) = managed.instructions_bundle.as_ref() {
            let files = bundle.files.as_ref().map(Vec::as_slice).unwrap_or(&[]);
            let notices = bundle.notices.as_ref().map(Vec::as_slice).unwrap_or(&[]);
            if !files.is_empty() || !notices.is_empty() {
                let mut section = vec!["Paperclip managed instructions bundle:".to_string()];
                if let Some(entry_file) = bundle.entry_file.as_deref() {
                    section.push(format!("Entry file: {entry_file}"));
                }
                for notice in notices {
                    let trimmed = notice.trim();
                    if !trimmed.is_empty() {
                        section.push(format!("Note: {trimmed}"));
                    }
                }
                for file in files {
                    if let Some(rendered) = render_managed_file(file) {
                        section.push(rendered);
                    }
                }
                sections.push(section.join("\n\n"));
            }
        }

        if let Some(skills) = managed.skills.as_ref() {
            let mut skill_sections: Vec<String> = Vec::new();
            for skill in skills {
                let files = skill.files.as_ref().map(Vec::as_slice).unwrap_or(&[]);
                let notices = skill.notices.as_ref().map(Vec::as_slice).unwrap_or(&[]);
                if files.is_empty() && notices.is_empty() {
                    continue;
                }
                let skill_name = skill
                    .runtime_name
                    .as_deref()
                    .or(skill.key.as_deref())
                    .unwrap_or("paperclip-skill");
                let mut section = vec![format!(
                    "Skill: {}{}",
                    skill_name,
                    if skill.required.unwrap_or(false) {
                        " (required)"
                    } else {
                        ""
                    }
                )];
                if let Some(key) = skill.key.as_deref() {
                    section.push(format!("Key: {key}"));
                }
                for notice in notices {
                    let trimmed = notice.trim();
                    if !trimmed.is_empty() {
                        section.push(format!("Note: {trimmed}"));
                    }
                }
                for file in files {
                    if let Some(rendered) = render_managed_file(file) {
                        section.push(rendered);
                    }
                }
                skill_sections.push(section.join("\n\n"));
            }
            if !skill_sections.is_empty() {
                sections.push(format!(
                    "Paperclip managed skills:\n\n{}",
                    skill_sections.join("\n\n")
                ));
            }
        }
    }

    if sections.is_empty() {
        None
    } else {
        Some(sections.join("\n\n"))
    }
}

fn render_paperclip_wake_context(paperclip: Option<&GatewayPaperclipPayload>) -> Option<String> {
    let paperclip = paperclip?;
    let mut lines = vec!["Paperclip issue packet:".to_string()];
    let mut wrote = false;

    push_packet_field(&mut lines, "runId", paperclip.run_id.as_deref(), &mut wrote);
    push_packet_field(
        &mut lines,
        "agentId",
        paperclip.agent_id.as_deref(),
        &mut wrote,
    );
    push_packet_field(
        &mut lines,
        "agentName",
        paperclip.agent_name.as_deref(),
        &mut wrote,
    );
    push_packet_field(
        &mut lines,
        "taskId",
        paperclip.task_id.as_deref(),
        &mut wrote,
    );
    push_packet_field(
        &mut lines,
        "issueId",
        paperclip.issue_id.as_deref(),
        &mut wrote,
    );

    if let Some(wake) = paperclip.wake.as_ref() {
        push_packet_field(&mut lines, "wakeReason", wake.reason.as_deref(), &mut wrote);
        push_packet_field(
            &mut lines,
            "latestCommentId",
            wake.latest_comment_id.as_deref(),
            &mut wrote,
        );
        if let Some(issue) = wake.issue.as_ref() {
            let identifier = non_empty(issue.identifier.as_deref()).unwrap_or("-");
            let title = non_empty(issue.title.as_deref()).unwrap_or("-");
            let status = issue
                .status
                .as_ref()
                .and_then(json_scalar_to_string)
                .unwrap_or_else(|| "-".to_string());
            let priority = issue
                .priority
                .as_ref()
                .and_then(json_scalar_to_string)
                .unwrap_or_else(|| "-".to_string());
            lines.push(format!(
                "- issue: {} {} status={} priority={}",
                identifier, title, status, priority
            ));
            if let Some(id) = non_empty(issue.id.as_deref()) {
                lines.push(format!("  id: {id}"));
            }
            wrote = true;
        }
        if let Some(comment_ids) = wake.comment_ids.as_ref().filter(|ids| !ids.is_empty()) {
            lines.push(format!("- commentIds: {}", comment_ids.join(",")));
            wrote = true;
        }
        if wake.truncated.unwrap_or(false) {
            lines.push("- wake payload was truncated by Paperclip".to_string());
            wrote = true;
        }
        if wake.fallback_fetch_needed.unwrap_or(false) {
            lines.push("- Paperclip indicated fallback fetch was needed".to_string());
            wrote = true;
        }
        if let Some(comments) = wake
            .comments
            .as_ref()
            .filter(|comments| !comments.is_empty())
        {
            lines.push("- comments:".to_string());
            for comment in comments.iter().take(8) {
                let id = non_empty(comment.id.as_deref()).unwrap_or("-");
                let created_at = non_empty(comment.created_at.as_deref()).unwrap_or("-");
                let author = comment
                    .author
                    .as_ref()
                    .and_then(json_author_label)
                    .unwrap_or_else(|| "-".to_string());
                let issue_id = non_empty(comment.issue_id.as_deref()).unwrap_or("-");
                let body = comment
                    .body
                    .as_deref()
                    .map(|value| summarize_gateway_text(value, 2400))
                    .unwrap_or_default();
                lines.push(format!(
                    "  - id={} issueId={} createdAt={} author={} truncated={}",
                    id,
                    issue_id,
                    created_at,
                    author,
                    comment.body_truncated.unwrap_or(false)
                ));
                if !body.is_empty() {
                    lines.push(format!("    body: {body}"));
                }
            }
            if comments.len() > 8 {
                lines.push(format!(
                    "  - [omitted {} older comments]",
                    comments.len() - 8
                ));
            }
            wrote = true;
        }
    }

    wrote.then(|| lines.join("\n"))
}

fn push_packet_field(lines: &mut Vec<String>, label: &str, value: Option<&str>, wrote: &mut bool) {
    if let Some(value) = non_empty(value) {
        lines.push(format!("- {label}: {value}"));
        *wrote = true;
    }
}

fn json_scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => non_empty(Some(value.as_str())).map(str::to_string),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn json_author_label(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => non_empty(Some(value.as_str())).map(str::to_string),
        Value::Object(map) => map
            .get("name")
            .or_else(|| map.get("login"))
            .or_else(|| map.get("id"))
            .and_then(json_scalar_to_string),
        _ => json_scalar_to_string(value),
    }
}

fn render_paperclip_workspace_context(
    paperclip: Option<&GatewayPaperclipPayload>,
) -> Option<String> {
    let paperclip = paperclip?;
    let mut lines = vec!["Paperclip execution workspace:".to_string()];
    let mut wrote = false;

    if let Some(workspace) = paperclip.workspace.as_ref() {
        if let Some(cwd) = non_empty(workspace.cwd.as_deref()) {
            lines.push(format!("- cwd: {cwd}"));
            wrote = true;
        }
        if let Some(path) = non_empty(workspace.worktree_path.as_deref()) {
            lines.push(format!("- worktreePath: {path}"));
            wrote = true;
        }
        push_workspace_field(&mut lines, "source", workspace.source.as_deref());
        push_workspace_field(&mut lines, "mode", workspace.mode.as_deref());
        push_workspace_field(&mut lines, "strategy", workspace.strategy.as_deref());
        push_workspace_field(
            &mut lines,
            "providerType",
            workspace.provider_type.as_deref(),
        );
        push_workspace_field(&mut lines, "providerRef", workspace.provider_ref.as_deref());
        push_workspace_field(
            &mut lines,
            "remoteProvider",
            workspace.remote_provider.as_deref(),
        );
        push_workspace_field(
            &mut lines,
            "remoteWorkspaceRef",
            workspace.remote_workspace_ref.as_deref(),
        );
        push_workspace_field(&mut lines, "projectId", workspace.project_id.as_deref());
        push_workspace_field(&mut lines, "workspaceId", workspace.workspace_id.as_deref());
        push_workspace_field(&mut lines, "repoUrl", workspace.repo_url.as_deref());
        push_workspace_field(&mut lines, "repoRef", workspace.repo_ref.as_deref());
        push_workspace_field(&mut lines, "branchName", workspace.branch_name.as_deref());
        push_workspace_field(&mut lines, "agentHome", workspace.agent_home.as_deref());
    }

    if let Some(workspaces) = paperclip.workspaces.as_ref() {
        let mut additional: Vec<String> = Vec::new();
        for workspace in workspaces {
            let cwd = non_empty(workspace.cwd.as_deref());
            let repo_url = non_empty(workspace.repo_url.as_deref());
            let repo_ref = non_empty(workspace.repo_ref.as_deref());
            let workspace_id = non_empty(workspace.workspace_id.as_deref());
            let source_type = non_empty(workspace.source_type.as_deref());
            let remote_provider = non_empty(workspace.remote_provider.as_deref());
            let remote_workspace_ref = non_empty(workspace.remote_workspace_ref.as_deref());
            if cwd.is_none()
                && repo_url.is_none()
                && repo_ref.is_none()
                && workspace_id.is_none()
                && source_type.is_none()
                && remote_provider.is_none()
                && remote_workspace_ref.is_none()
            {
                continue;
            }
            additional.push(format!(
                "- workspaceId={} sourceType={} cwd={} repo={} ref={} remoteProvider={} remoteWorkspaceRef={}",
                workspace_id.unwrap_or("-"),
                source_type.unwrap_or("-"),
                cwd.unwrap_or("-"),
                repo_url.unwrap_or("-"),
                repo_ref.unwrap_or("-"),
                remote_provider.unwrap_or("-"),
                remote_workspace_ref.unwrap_or("-")
            ));
        }
        if !additional.is_empty() {
            lines.push("Additional Paperclip workspace hints:".to_string());
            lines.extend(additional);
            wrote = true;
        }
    }

    wrote.then(|| lines.join("\n"))
}

fn push_workspace_field(lines: &mut Vec<String>, label: &str, value: Option<&str>) {
    if let Some(value) = non_empty(value) {
        lines.push(format!("- {label}: {value}"));
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn render_managed_file(file: &GatewayManagedFile) -> Option<String> {
    let relative_path = file.path.as_deref()?.trim();
    if relative_path.is_empty() {
        return None;
    }
    let content = file.content.as_deref()?.trim();
    if content.is_empty() {
        return None;
    }
    let language = markdown_language_hint(relative_path);
    let mut rendered = format!("File: {relative_path}\n```{language}\n{content}\n```");
    if file.truncated.unwrap_or(false) {
        rendered
            .push_str("\nThis file was truncated by Paperclip before it was sent to the gateway.");
    }
    Some(rendered)
}

fn markdown_language_hint(relative_path: &str) -> &'static str {
    match relative_path
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "md" | "mdx" => "md",
        "json" => "json",
        "yaml" | "yml" => "yaml",
        "sh" => "bash",
        "py" => "python",
        "rs" => "rust",
        "ts" | "tsx" => "ts",
        "js" | "jsx" | "mjs" | "cjs" => "js",
        "toml" => "toml",
        _ => "",
    }
}

fn extract_omx_team_status(artifacts: &[ExecutionArtifactRef]) -> Option<Value> {
    for artifact in artifacts {
        if artifact.kind != "team-status" {
            continue;
        }
        let Some(body) = artifact.body.as_ref() else {
            continue;
        };
        if let Ok(value) = serde_json::from_str::<Value>(body) {
            return Some(value);
        }
        return Some(Value::String(body.clone()));
    }
    None
}

fn mark_run_success(
    runs: &Arc<Mutex<HashMap<String, GatewayRunRecord>>>,
    run_id: &str,
    group_folder: &str,
    execution: &ExecutionResponse,
    lane: ExecutionLane,
) -> Result<()> {
    let metadata = execution.metadata.as_ref();
    let artifacts = metadata
        .map(|value| value.artifacts.clone())
        .unwrap_or_default();
    let artifacts_value = serde_json::to_value(&artifacts).unwrap_or(Value::Null);
    let omx_stats = extract_omx_team_status(&artifacts).unwrap_or(Value::Null);
    let omx_stats_for_gateway = omx_stats.clone();
    let gateway_status = classify_gateway_execution_status(
        &lane,
        metadata.and_then(|value| value.status.as_deref()),
        &omx_stats,
    );
    let provider = metadata
        .and_then(|value| value.provider.clone())
        .or_else(|| metadata.and_then(|value| value.backend.clone()))
        .unwrap_or_else(|| "nanoclaw".to_string());
    let biller = metadata.and_then(|value| value.biller.clone());
    let billing_type = metadata.and_then(|value| value.billing_type.clone());
    let model = metadata.and_then(|value| value.model.clone());
    let usage_value = metadata
        .and_then(|value| value.usage.as_ref())
        .and_then(|value| serde_json::to_value(value).ok())
        .unwrap_or(Value::Null);
    let cost_usd = metadata.and_then(|value| value.cost_usd);
    let routing_value = metadata
        .and_then(|value| value.routing_decision.as_ref())
        .and_then(|value| serde_json::to_value(value).ok())
        .unwrap_or(Value::Null);
    let objective_value = metadata
        .and_then(|value| value.objective.as_ref())
        .and_then(|value| serde_json::to_value(value).ok())
        .unwrap_or(Value::Null);
    let plan_value = metadata
        .and_then(|value| value.plan.as_ref())
        .and_then(|value| serde_json::to_value(value).ok())
        .unwrap_or(Value::Null);
    let session_value = metadata
        .and_then(|value| value.session_state.as_ref())
        .and_then(|value| serde_json::to_value(value).ok())
        .unwrap_or(Value::Null);
    let agent_meta = json!({
        "provider": provider,
        "biller": biller,
        "billingType": billing_type,
        "model": model,
        "usage": usage_value.clone(),
        "costUsd": cost_usd,
        "routingDecision": routing_value.clone(),
        "objective": objective_value.clone(),
        "plan": plan_value.clone(),
        "sessionState": session_value.clone(),
    });
    let result_meta = json!({
        "provider": agent_meta.get("provider").cloned().unwrap_or(Value::Null),
        "biller": agent_meta.get("biller").cloned().unwrap_or(Value::Null),
        "billingType": agent_meta.get("billingType").cloned().unwrap_or(Value::Null),
        "model": agent_meta.get("model").cloned().unwrap_or(Value::Null),
        "usage": agent_meta.get("usage").cloned().unwrap_or(Value::Null),
        "costUsd": agent_meta.get("costUsd").cloned().unwrap_or(Value::Null),
        "artifacts": artifacts_value.clone(),
        "agentMeta": agent_meta,
        "foundation": {
            "routingDecision": routing_value,
            "objective": objective_value,
            "plan": plan_value,
            "sessionState": session_value,
        },
        "omx": {
            "teamStatus": omx_stats,
            "artifacts": artifacts_value,
        },
        "gateway": {
            "lane": lane.as_str(),
            "sessionId": execution.session_id,
            "tmuxSession": metadata.and_then(|value| value.tmux_session.clone()),
            "teamName": metadata.and_then(|value| value.team_name.clone()),
            "summary": metadata.and_then(|value| value.summary.clone()),
            "question": metadata.and_then(|value| value.question.clone()),
            "omxStats": omx_stats_for_gateway,
        }
    });
    let mut guard = runs
        .lock()
        .map_err(|_| anyhow::anyhow!("failed to lock gateway run state"))?;
    let existing = guard
        .get_mut(run_id)
        .context("gateway run disappeared before completion")?;
    existing.status = gateway_status;
    existing.updated_at = Utc::now().to_rfc3339();
    existing.session_id = Some(execution.session_id.clone());
    existing.group_folder = Some(group_folder.to_string());
    let summary = metadata
        .and_then(|value| value.summary.clone())
        .unwrap_or_else(|| execution.text.clone());
    existing.summary = Some(summary.clone());
    existing.result_text = Some(execution.text.clone());
    existing.metadata = Some(result_meta);
    existing.error = matches!(gateway_status, GatewayRunStatus::Error).then_some(summary);
    Ok(())
}

fn classify_gateway_execution_status(
    lane: &ExecutionLane,
    execution_status: Option<&str>,
    omx_team_status: &Value,
) -> GatewayRunStatus {
    if !matches!(lane, ExecutionLane::Omx) {
        return GatewayRunStatus::Ok;
    }

    if is_failure_status(execution_status) {
        return GatewayRunStatus::Error;
    }
    if let Some(team_status) = classify_omx_team_status(omx_team_status) {
        return team_status;
    }
    if is_success_status(execution_status) {
        return GatewayRunStatus::Ok;
    }

    // OMX launches are asynchronous. A non-terminal runner response means the
    // Paperclip run must keep waiting for the local callback watcher.
    GatewayRunStatus::Running
}

fn is_failure_status(status: Option<&str>) -> bool {
    matches!(
        normalize_status(status).as_deref(),
        Some("failed" | "error" | "stopped" | "cancelled" | "canceled")
    )
}

fn is_success_status(status: Option<&str>) -> bool {
    matches!(
        normalize_status(status).as_deref(),
        Some("completed" | "success" | "ok")
    )
}

fn normalize_status(status: Option<&str>) -> Option<String> {
    let normalized = status?.trim().to_ascii_lowercase().replace('-', "_");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn classify_omx_team_status(team_status: &Value) -> Option<GatewayRunStatus> {
    let tasks = team_status.get("tasks")?.as_object()?;
    let total = value_count(tasks.get("total"));
    if total == 0 {
        return None;
    }

    let pending = value_count(tasks.get("pending"));
    let blocked = value_count(tasks.get("blocked"));
    let in_progress = value_count(tasks.get("in_progress"));
    if pending + blocked + in_progress > 0 {
        return Some(GatewayRunStatus::Running);
    }

    let failed = value_count(tasks.get("failed"));
    let completed = value_count(tasks.get("completed"));
    if completed + failed >= total {
        if failed > 0 {
            Some(GatewayRunStatus::Error)
        } else {
            Some(GatewayRunStatus::Ok)
        }
    } else {
        Some(GatewayRunStatus::Running)
    }
}

fn value_count(value: Option<&Value>) -> u64 {
    match value {
        Some(Value::Number(number)) => number.as_u64().unwrap_or(0),
        Some(Value::String(text)) => text.trim().parse::<u64>().unwrap_or(0),
        _ => 0,
    }
}

fn mark_run_error(
    runs: &Arc<Mutex<HashMap<String, GatewayRunRecord>>>,
    run_id: &str,
    error: &str,
) -> Result<()> {
    let mut guard = runs
        .lock()
        .map_err(|_| anyhow::anyhow!("failed to lock gateway run state"))?;
    let existing = guard
        .get_mut(run_id)
        .context("gateway run disappeared before failure")?;
    existing.status = GatewayRunStatus::Error;
    existing.updated_at = Utc::now().to_rfc3339();
    existing.error = Some(error.to_string());
    existing.summary = Some(error.to_string());
    Ok(())
}

fn gateway_omx_options(
    config: &NanoclawConfig,
    params: &GatewayAgentParams,
    lane: &ExecutionLane,
) -> Option<OmxExecutionOptions> {
    if !matches!(lane, ExecutionLane::Omx) {
        return None;
    }

    let gateway = params
        .paperclip
        .as_ref()
        .and_then(|paperclip| paperclip.gateway.as_ref());
    Some(OmxExecutionOptions {
        mode: gateway
            .and_then(|value| value.omx_mode.as_deref())
            .map(OmxMode::parse)
            .unwrap_or_else(|| OmxMode::parse(&config.omx_default_mode)),
        max_workers: gateway.and_then(|value| value.max_workers),
        external_run_id: params.idempotency_key.clone().or_else(|| {
            params
                .paperclip
                .as_ref()
                .and_then(|paperclip| paperclip.run_id.clone())
        }),
    })
}

fn resolve_gateway_execution_lane(
    config: &NanoclawConfig,
    params: &GatewayAgentParams,
) -> ExecutionLane {
    params
        .paperclip
        .as_ref()
        .and_then(|paperclip| paperclip.gateway.as_ref())
        .and_then(|gateway| gateway.execution_lane.as_deref())
        .map(ExecutionLane::parse)
        .unwrap_or_else(|| config.openclaw_gateway_execution_lane.clone())
}

fn derive_group_identity(run_id: &str, params: &GatewayAgentParams) -> GroupIdentity {
    let paperclip = params.paperclip.as_ref();
    let agent_id = paperclip
        .and_then(|value| value.agent_id.as_deref())
        .unwrap_or("paperclip");
    let session_key = params
        .session_key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| paperclip.and_then(|value| value.issue_id.as_deref()))
        .or_else(|| paperclip.and_then(|value| value.task_id.as_deref()))
        .unwrap_or(run_id);
    let issue_id = paperclip.and_then(|value| value.issue_id.as_deref());
    let group_name = paperclip
        .and_then(|value| value.agent_name.clone())
        .unwrap_or_else(|| "Paperclip Gateway".to_string());
    let chat_jid = issue_id
        .map(|issue_id| format!("paperclip:issue:{issue_id}"))
        .unwrap_or_else(|| format!("paperclip:agent:{agent_id}:{session_key}"));
    GroupIdentity {
        chat_jid,
        group_name,
    }
}

#[derive(Debug, Clone)]
struct GroupIdentity {
    chat_jid: String,
    group_name: String,
}

fn find_run(
    runs: &Arc<Mutex<HashMap<String, GatewayRunRecord>>>,
    run_id: &str,
) -> Result<Option<GatewayRunRecord>> {
    let guard = runs
        .lock()
        .map_err(|_| anyhow::anyhow!("failed to lock gateway run state"))?;
    Ok(guard.get(run_id).cloned())
}

fn upsert_run(
    runs: &Arc<Mutex<HashMap<String, GatewayRunRecord>>>,
    record: GatewayRunRecord,
) -> Result<()> {
    let mut guard = runs
        .lock()
        .map_err(|_| anyhow::anyhow!("failed to lock gateway run state"))?;
    guard.insert(record.run_id.clone(), record);
    Ok(())
}

fn run_record_payload(record: &GatewayRunRecord) -> Value {
    let result = record.result_text.as_ref().map(|text| {
        json!({
            "text": text,
            "summary": record.summary,
            "meta": record.metadata.clone().unwrap_or_else(|| json!({})),
        })
    });
    json!({
        "runId": record.run_id,
        "status": record.status.as_str(),
        "summary": record.summary,
        "error": record.error,
        "result": result,
        "sessionId": record.session_id,
        "groupFolder": record.group_folder,
        "lane": record.lane,
        "acceptedAt": record.accepted_at,
        "updatedAt": record.updated_at,
    })
}

fn write_ok_frame(
    socket: &mut WebSocket<TcpStream>,
    request_id: &str,
    payload: Value,
) -> Result<()> {
    write_json_frame(
        socket,
        &json!({
            "type": "res",
            "id": request_id,
            "ok": true,
            "payload": payload,
        }),
    )
}

fn write_error_frame(
    socket: &mut WebSocket<TcpStream>,
    request_id: &str,
    code: &str,
    message: &str,
) -> Result<()> {
    write_json_frame(
        socket,
        &json!({
            "type": "res",
            "id": request_id,
            "ok": false,
            "error": {
                "code": code,
                "message": message,
            }
        }),
    )
}

fn write_json_frame(socket: &mut WebSocket<TcpStream>, value: &Value) -> Result<()> {
    socket
        .send(Message::Text(value.to_string().into()))
        .context("failed to write websocket frame")
}

fn is_plain_http_request(stream: &TcpStream) -> Result<bool> {
    let mut peek = [0u8; 2048];
    let bytes = stream
        .peek(&mut peek)
        .context("failed to peek incoming stream")?;
    if bytes == 0 {
        return Ok(false);
    }
    let preview = String::from_utf8_lossy(&peek[..bytes]).to_string();
    let lower = preview.to_ascii_lowercase();
    if !(lower.starts_with("get ") || lower.starts_with("head ") || lower.starts_with("post ")) {
        return Ok(false);
    }
    Ok(!lower.contains("upgrade: websocket"))
}

fn handle_http_request(stream: &mut TcpStream, state: &GatewayServerState) -> Result<()> {
    let request = read_plain_http_request(stream)?;
    let method = request.method.to_ascii_uppercase();
    let path = logical_openclaw_path(&request.path);
    let head_only = method == "HEAD";

    if matches!(method.as_str(), "GET" | "HEAD") && (path == "/" || path == "/health") {
        return respond_http_health(stream, &state.config, head_only);
    }

    if matches!(method.as_str(), "GET" | "HEAD") {
        if let Some(session_id) = path.strip_prefix("/omx/sessions/") {
            return respond_http_omx_session(stream, state, &request, session_id, head_only);
        }
    }

    if method == "POST" && path == "/webhook/omx" {
        return respond_http_omx_webhook(stream, state, &request);
    }

    respond_http_json(
        stream,
        404,
        json!({
            "error": "not_found",
            "path": path,
        }),
        head_only,
    )
}

fn read_plain_http_request(stream: &mut TcpStream) -> Result<PlainHttpRequest> {
    const MAX_HTTP_BYTES: usize = 2 * 1024 * 1024;

    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut data = Vec::new();
    let mut buffer = [0u8; 8192];
    let mut header_end = None;
    let mut content_length = 0usize;

    loop {
        let bytes = stream
            .read(&mut buffer)
            .context("failed to read HTTP request")?;
        if bytes == 0 {
            break;
        }
        data.extend_from_slice(&buffer[..bytes]);
        if data.len() > MAX_HTTP_BYTES {
            anyhow::bail!("HTTP request exceeded maximum size");
        }

        if header_end.is_none() {
            if let Some(index) = find_header_end(&data) {
                header_end = Some(index);
                content_length = parse_content_length(&data[..index]).unwrap_or(0);
            }
        }

        if let Some(index) = header_end {
            if data.len() >= index + 4 + content_length {
                break;
            }
        }
    }

    let header_end = header_end.context("HTTP request missing header terminator")?;
    let header_text = String::from_utf8_lossy(&data[..header_end]);
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().context("HTTP request missing request line")?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .context("HTTP request missing method")?
        .to_string();
    let path = request_parts
        .next()
        .context("HTTP request missing path")?
        .to_string();
    let mut headers = BTreeMap::new();
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    let body_start = header_end + 4;
    let body_end = body_start.saturating_add(content_length).min(data.len());
    let body = data[body_start..body_end].to_vec();

    Ok(PlainHttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_content_length(header_bytes: &[u8]) -> Option<usize> {
    let header_text = String::from_utf8_lossy(header_bytes);
    for line in header_text.split("\r\n").skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("content-length") {
            return value.trim().parse().ok();
        }
    }
    None
}

fn logical_openclaw_path(path: &str) -> String {
    let path = path.split('?').next().unwrap_or("/");
    let stripped = path
        .strip_prefix("/openclaw")
        .filter(|rest| rest.is_empty() || rest.starts_with('/'))
        .unwrap_or(path);
    if stripped.is_empty() {
        "/".to_string()
    } else {
        stripped.to_string()
    }
}

fn respond_http_health(
    stream: &mut TcpStream,
    config: &NanoclawConfig,
    head_only: bool,
) -> Result<()> {
    respond_http_json(
        stream,
        200,
        json!({
            "status": "ok",
            "service": "nanoclaw-openclaw-gateway",
            "websocketUrl": config.openclaw_gateway_public_ws_url(),
            "executionLane": config.openclaw_gateway_execution_lane.as_str(),
            "omx": describe_omx_readiness(config),
        }),
        head_only,
    )
}

fn respond_http_omx_session(
    stream: &mut TcpStream,
    state: &GatewayServerState,
    request: &PlainHttpRequest,
    session_id: &str,
    head_only: bool,
) -> Result<()> {
    let session_id = session_id.trim_matches('/');
    if session_id.is_empty() {
        return respond_http_json(
            stream,
            400,
            json!({ "error": "missing_session_id" }),
            head_only,
        );
    }
    if !is_authorized_http_gateway_request(&state.config, request) {
        return respond_http_json(stream, 401, json!({ "error": "unauthorized" }), head_only);
    }

    let client = OmxRunnerClient::from_config(&state.config);
    match client.status(session_id, state.config.project_root.as_path()) {
        Ok(status) => respond_http_json(
            stream,
            200,
            json!({
                "ok": true,
                "session": status,
            }),
            head_only,
        ),
        Err(error) => respond_http_json(
            stream,
            404,
            json!({
                "ok": false,
                "error": error.to_string(),
                "sessionId": session_id,
            }),
            head_only,
        ),
    }
}

fn respond_http_omx_webhook(
    stream: &mut TcpStream,
    state: &GatewayServerState,
    request: &PlainHttpRequest,
) -> Result<()> {
    let header_token = request
        .headers
        .get("x-nanoclaw-omx-token")
        .map(String::as_str)
        .or_else(|| request.headers.get("x-openclaw-token").map(String::as_str));
    if !is_valid_omx_token(&state.config, header_token) {
        return respond_http_json(stream, 401, json!({ "error": "unauthorized" }), false);
    }

    let payload = match parse_omx_webhook_payload(&request.body) {
        Ok(payload) => payload,
        Err(error) => {
            return respond_http_json(
                stream,
                400,
                json!({
                    "error": "invalid_omx_payload",
                    "message": error.to_string(),
                }),
                false,
            );
        }
    };
    if !is_valid_omx_token(&state.config, payload.token.as_deref().or(header_token)) {
        return respond_http_json(stream, 401, json!({ "error": "unauthorized" }), false);
    }

    update_gateway_run_from_omx_webhook(&state.runs, &payload)?;

    let mut app = NanoclawApp::open(state.config.clone())?;
    let mut channel = None;
    let body = apply_omx_webhook_payload(&mut app, &mut channel, payload)?;
    respond_http_json(stream, 200, body, false)
}

fn update_gateway_run_from_omx_webhook(
    runs: &Arc<Mutex<HashMap<String, GatewayRunRecord>>>,
    payload: &OmxWebhookPayload,
) -> Result<()> {
    let Some(run_id) = payload
        .external_run_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };

    let mut guard = runs
        .lock()
        .map_err(|_| anyhow::anyhow!("failed to lock gateway run state"))?;
    let Some(existing) = guard.get_mut(run_id) else {
        return Ok(());
    };

    let status = payload.status.as_str();
    let terminal = payload.status.is_terminal();
    let failed = matches!(status, "failed" | "stopped");
    let summary = payload
        .summary
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("OMX session {status}."));
    let artifacts_value = serde_json::to_value(&payload.artifacts).unwrap_or(Value::Null);
    let team_status = extract_omx_artifact_team_status(&payload.artifacts).unwrap_or(Value::Null);

    existing.status = if terminal {
        if failed {
            GatewayRunStatus::Error
        } else {
            GatewayRunStatus::Ok
        }
    } else {
        GatewayRunStatus::Running
    };
    existing.updated_at = Utc::now().to_rfc3339();
    existing.session_id = Some(payload.session_id.clone());
    existing.group_folder = Some(payload.group_folder.clone());
    existing.summary = Some(summary.clone());
    existing.result_text = Some(summary.clone());
    existing.metadata = Some(json!({
        "provider": "omx",
        "artifacts": artifacts_value,
        "omx": {
            "teamStatus": team_status,
            "artifacts": artifacts_value,
        },
        "gateway": {
            "lane": existing.lane,
            "sessionId": payload.session_id,
            "tmuxSession": payload.tmux_session,
            "teamName": payload.team_name,
            "summary": summary,
            "question": payload.question,
            "omxStatus": status,
        }
    }));
    existing.error = failed.then(|| {
        payload
            .summary
            .clone()
            .unwrap_or_else(|| format!("OMX session {status}."))
    });

    Ok(())
}

fn extract_omx_artifact_team_status(artifacts: &[OmxArtifactRef]) -> Option<Value> {
    for artifact in artifacts {
        if artifact.kind != "team-status" {
            continue;
        }
        let Some(body) = artifact.body.as_ref() else {
            continue;
        };
        if let Ok(value) = serde_json::from_str::<Value>(body) {
            return Some(value);
        }
        return Some(Value::String(body.clone()));
    }
    None
}

fn is_authorized_http_gateway_request(config: &NanoclawConfig, request: &PlainHttpRequest) -> bool {
    let Some(token) = http_gateway_token(request) else {
        return false;
    };
    token == config.openclaw_gateway_token.trim()
}

fn http_gateway_token(request: &PlainHttpRequest) -> Option<&str> {
    request
        .headers
        .get("x-openclaw-token")
        .map(String::as_str)
        .or_else(|| {
            request
                .headers
                .get("authorization")
                .and_then(|value| value.strip_prefix("Bearer "))
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn respond_http_json(
    stream: &mut TcpStream,
    status: u16,
    value: Value,
    head_only: bool,
) -> Result<()> {
    let body = serde_json::to_string(&value)?;
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Internal Server Error",
    };
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status,
        status_text,
        if head_only { 0 } else { body.len() },
    );
    stream
        .write_all(response.as_bytes())
        .context("failed to write gateway HTTP response headers")?;
    if !head_only {
        stream
            .write_all(body.as_bytes())
            .context("failed to write gateway HTTP response body")?;
    }
    let _ = stream.flush();
    let _ = stream.shutdown(Shutdown::Both);
    Ok(())
}

fn slug(value: &str) -> String {
    let slug = value
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    let trimmed = slug.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "gateway".to_string()
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paperclip_runtime_env_filters_to_allowed_paperclip_keys() {
        let payload = GatewayPaperclipPayload {
            runtime_env: Some(HashMap::from([
                ("PAPERCLIP_RUN_ID".to_string(), "run-1".to_string()),
                ("PAPERCLIP_API_KEY".to_string(), "jwt-token".to_string()),
                ("ZAI_API_KEY".to_string(), "zai-token".to_string()),
                (
                    "AZURE_OPENAI_API_KEY".to_string(),
                    "azure-token".to_string(),
                ),
                ("OPENAI_API_KEY".to_string(), "provider-secret".to_string()),
                ("PAPERCLIP_TASK_ID".to_string(), "  ".to_string()),
            ])),
            ..Default::default()
        };

        let env = paperclip_runtime_env(Some(&payload));

        assert_eq!(
            env.get("PAPERCLIP_RUN_ID").map(String::as_str),
            Some("run-1")
        );
        assert_eq!(
            env.get("PAPERCLIP_API_KEY").map(String::as_str),
            Some("jwt-token")
        );
        assert_eq!(
            env.get("ZAI_API_KEY").map(String::as_str),
            Some("zai-token")
        );
        assert_eq!(
            env.get("AZURE_OPENAI_API_KEY").map(String::as_str),
            Some("azure-token")
        );
        assert!(!env.contains_key("OPENAI_API_KEY"));
        assert!(!env.contains_key("PAPERCLIP_TASK_ID"));
    }

    #[test]
    fn gateway_hints_can_select_zai_backend_and_model() {
        let params = GatewayAgentParams {
            paperclip: Some(GatewayPaperclipPayload {
                gateway: Some(GatewayHints {
                    execution_lane: Some("host".to_string()),
                    worker_backend: Some("zai".to_string()),
                    model: Some("glm-4.7".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(gateway_backend_override(&params), Some(WorkerBackend::Zai));
        let env = gateway_runtime_env(&params, None);
        assert_eq!(
            env.get("NANOCLAW_ZAI_MODEL").map(String::as_str),
            Some("glm-4.7")
        );
    }

    #[test]
    fn gateway_hints_can_select_azure_openai_backend_and_endpoint() {
        let params = GatewayAgentParams {
            paperclip: Some(GatewayPaperclipPayload {
                gateway: Some(GatewayHints {
                    execution_lane: Some("host".to_string()),
                    worker_backend: Some("azure-openai".to_string()),
                    model: Some("cto-deployment".to_string()),
                    azure_endpoint: Some("https://example.openai.azure.com/openai/v1/".to_string()),
                    azure_api_version: Some("v1".to_string()),
                    azure_fallback_backend: Some("codex".to_string()),
                    codex_usage_fallback_backend: Some("zai".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            gateway_backend_override(&params),
            Some(WorkerBackend::AzureOpenAI)
        );
        let env = gateway_runtime_env(&params, None);
        assert_eq!(
            env.get("NANOCLAW_AZURE_OPENAI_DEPLOYMENT")
                .map(String::as_str),
            Some("cto-deployment")
        );
        assert_eq!(
            env.get("NANOCLAW_AZURE_OPENAI_ENDPOINT")
                .map(String::as_str),
            Some("https://example.openai.azure.com/openai/v1/")
        );
        assert_eq!(
            env.get("NANOCLAW_AZURE_OPENAI_API_VERSION")
                .map(String::as_str),
            Some("v1")
        );
        assert_eq!(
            env.get("NANOCLAW_AZURE_OPENAI_FALLBACK_BACKEND")
                .map(String::as_str),
            Some("codex")
        );
        assert_eq!(
            env.get("NANOCLAW_CODEX_USAGE_FALLBACK_BACKEND")
                .map(String::as_str),
            Some("zai")
        );
        assert!(!env.contains_key("NANOCLAW_ZAI_MODEL"));
    }

    #[test]
    fn forced_gateway_backend_wins_over_payload_hint() {
        let selected = select_gateway_worker_backend(
            Some(WorkerBackend::AzureOpenAI),
            Some(WorkerBackend::Zai),
            Some(WorkerBackend::Codex),
        );

        assert_eq!(selected, Some(WorkerBackend::AzureOpenAI));
    }

    #[test]
    fn forced_gateway_backend_suppresses_mismatched_generic_model_env() {
        let params = GatewayAgentParams {
            paperclip: Some(GatewayPaperclipPayload {
                gateway: Some(GatewayHints {
                    worker_backend: Some("zai".to_string()),
                    model: Some("glm-4.7".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let env = gateway_runtime_env(&params, Some(&WorkerBackend::AzureOpenAI));

        assert!(!env.contains_key("NANOCLAW_ZAI_MODEL"));
        assert!(!env.contains_key("NANOCLAW_AZURE_OPENAI_DEPLOYMENT"));
    }

    #[test]
    fn forced_gateway_backend_keeps_explicit_azure_deployment_env() {
        let params = GatewayAgentParams {
            paperclip: Some(GatewayPaperclipPayload {
                gateway: Some(GatewayHints {
                    worker_backend: Some("zai".to_string()),
                    model: Some("glm-4.7".to_string()),
                    azure_deployment: Some("nanoclaw-gpt-4-1-mini".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let env = gateway_runtime_env(&params, Some(&WorkerBackend::AzureOpenAI));

        assert_eq!(
            env.get("NANOCLAW_AZURE_OPENAI_DEPLOYMENT")
                .map(String::as_str),
            Some("nanoclaw-gpt-4-1-mini")
        );
        assert!(!env.contains_key("NANOCLAW_ZAI_MODEL"));
    }

    #[test]
    fn gateway_hints_deserialize_provider_fallback_aliases() {
        let params: GatewayAgentParams = serde_json::from_value(serde_json::json!({
            "paperclip": {
                "gateway": {
                    "workerBackend": "azure-openai",
                    "azureFallbackBackend": "codex",
                    "codexUsageFallbackBackend": "zai"
                }
            }
        }))
        .unwrap();

        assert_eq!(
            gateway_backend_override(&params),
            Some(WorkerBackend::AzureOpenAI)
        );
        let env = gateway_runtime_env(&params, None);
        assert_eq!(
            env.get("NANOCLAW_AZURE_OPENAI_FALLBACK_BACKEND")
                .map(String::as_str),
            Some("codex")
        );
        assert_eq!(
            env.get("NANOCLAW_CODEX_USAGE_FALLBACK_BACKEND")
                .map(String::as_str),
            Some("zai")
        );
    }

    #[test]
    fn gateway_hints_can_select_github_copilot_backend_and_repo() {
        let params = GatewayAgentParams {
            paperclip: Some(GatewayPaperclipPayload {
                workspace: Some(GatewayPaperclipWorkspace {
                    repo_url: Some(
                        "https://github.com/Nexus-Integrated-Technologies/paperclip-cloudflare"
                            .to_string(),
                    ),
                    repo_ref: Some("main".to_string()),
                    ..Default::default()
                }),
                gateway: Some(GatewayHints {
                    execution_lane: Some("host".to_string()),
                    worker_backend: Some("github-copilot".to_string()),
                    github_copilot_custom_agent: Some("cto".to_string()),
                    github_copilot_follow: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            gateway_backend_override(&params),
            Some(WorkerBackend::GithubCopilot)
        );
        let env = gateway_runtime_env(&params, None);
        assert_eq!(
            env.get("NANOCLAW_GITHUB_COPILOT_REPO").map(String::as_str),
            Some("https://github.com/Nexus-Integrated-Technologies/paperclip-cloudflare")
        );
        assert_eq!(
            env.get("NANOCLAW_GITHUB_COPILOT_BASE").map(String::as_str),
            Some("main")
        );
        assert_eq!(
            env.get("NANOCLAW_GITHUB_COPILOT_CUSTOM_AGENT")
                .map(String::as_str),
            Some("cto")
        );
    }

    #[test]
    fn paperclip_wake_packet_renders_issue_and_comments() {
        let payload = GatewayPaperclipPayload {
            run_id: Some("run-1".to_string()),
            agent_name: Some("CTO".to_string()),
            wake: Some(GatewayPaperclipWake {
                reason: Some("issue_assigned".to_string()),
                issue: Some(GatewayPaperclipIssue {
                    id: Some("issue-1".to_string()),
                    identifier: Some("NEX-234".to_string()),
                    title: Some("Delegate via Copilot".to_string()),
                    status: Some(serde_json::json!("todo")),
                    priority: Some(serde_json::json!("medium")),
                }),
                comments: Some(vec![GatewayPaperclipComment {
                    id: Some("comment-1".to_string()),
                    issue_id: Some("issue-1".to_string()),
                    body: Some("Use GitHub Copilot for this repo-scoped change.".to_string()),
                    body_truncated: Some(false),
                    created_at: Some("2026-05-07T10:00:00Z".to_string()),
                    author: Some(serde_json::json!({ "id": "local-board" })),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let context = render_paperclip_managed_context(Some(&payload)).unwrap();

        assert!(context.contains("Paperclip issue packet:"));
        assert!(context.contains("runId: run-1"));
        assert!(context.contains("issue: NEX-234 Delegate via Copilot"));
        assert!(context.contains("Use GitHub Copilot for this repo-scoped change."));
    }

    #[test]
    fn gateway_prefers_paperclip_workspace_cwd() {
        let workspace = tempfile::tempdir().unwrap();
        let fallback = tempfile::tempdir().unwrap();
        let mut config = NanoclawConfig::from_env();
        config.groups_dir = fallback.path().to_path_buf();
        let params = GatewayAgentParams {
            paperclip: Some(GatewayPaperclipPayload {
                workspace: Some(GatewayPaperclipWorkspace {
                    cwd: Some(workspace.path().display().to_string()),
                    worktree_path: Some(fallback.path().join("unused").display().to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let resolved = resolve_gateway_workspace_root(&config, &params, "paperclip_issue_1")
            .expect("workspace should resolve");

        assert_eq!(resolved, workspace.path());
    }

    #[test]
    fn gateway_provisions_missing_agent_home_workspace_cwd() {
        let temp = tempfile::tempdir().unwrap();
        let fallback = temp.path().join("groups");
        let workspace = temp.path().join("instances/default/workspaces/agent-1");
        let mut config = NanoclawConfig::from_env();
        config.groups_dir = fallback;
        let params = GatewayAgentParams {
            paperclip: Some(GatewayPaperclipPayload {
                workspace: Some(GatewayPaperclipWorkspace {
                    cwd: Some(workspace.display().to_string()),
                    source: Some("agent_home".to_string()),
                    agent_home: Some(workspace.display().to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let resolved = resolve_gateway_workspace_root(&config, &params, "paperclip_issue_1")
            .expect("agent home fallback should be provisioned");

        assert_eq!(resolved, workspace);
        assert!(resolved.is_dir());
    }

    #[test]
    fn gateway_does_not_provision_missing_repo_workspace_cwd() {
        let temp = tempfile::tempdir().unwrap();
        let fallback = temp.path().join("groups");
        let workspace = temp.path().join("projects/repo");
        let mut config = NanoclawConfig::from_env();
        config.groups_dir = fallback;
        let params = GatewayAgentParams {
            paperclip: Some(GatewayPaperclipPayload {
                workspace: Some(GatewayPaperclipWorkspace {
                    cwd: Some(workspace.display().to_string()),
                    source: Some("project_primary".to_string()),
                    repo_url: Some(
                        "https://github.com/Nexus-Integrated-Technologies/example".to_string(),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let error = resolve_gateway_workspace_root(&config, &params, "paperclip_issue_1")
            .expect_err("missing repo workspace should stay a provisioning failure");

        assert!(error.to_string().contains("path is not a directory"));
        assert!(!workspace.exists());
    }

    #[test]
    fn gateway_local_workspace_resolver_does_not_treat_codespace_path_as_local() {
        let fallback = tempfile::tempdir().unwrap();
        let mut config = NanoclawConfig::from_env();
        config.groups_dir = fallback.path().to_path_buf();
        let params = GatewayAgentParams {
            paperclip: Some(GatewayPaperclipPayload {
                workspace: Some(GatewayPaperclipWorkspace {
                    cwd: Some("/workspaces/paperclip-cloudflare".to_string()),
                    mode: Some("cloud_sandbox".to_string()),
                    strategy: Some("cloud_sandbox".to_string()),
                    provider_type: Some("cloud_sandbox".to_string()),
                    remote_provider: Some("github_codespaces".to_string()),
                    remote_workspace_ref: Some("paperclip-cloudflare-codespace".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let error = resolve_gateway_workspace_root(&config, &params, "paperclip_issue_1")
            .expect_err("local gateway should not pretend a Codespace path is local");

        assert!(error
            .to_string()
            .contains("remote github_codespaces workspace"));
        assert!(error.to_string().contains("Codespaces SSH runner"));
    }

    #[test]
    fn gateway_resolves_github_codespaces_remote_target() {
        let params = GatewayAgentParams {
            paperclip: Some(GatewayPaperclipPayload {
                workspace: Some(GatewayPaperclipWorkspace {
                    mode: Some("cloud_sandbox".to_string()),
                    strategy: Some("cloud_sandbox".to_string()),
                    provider_type: Some("cloud_sandbox".to_string()),
                    remote_provider: Some("github_codespaces".to_string()),
                    remote_workspace_ref: Some("paperclip-cloudflare-codespace".to_string()),
                    cwd: Some("/workspaces/paperclip-cloudflare".to_string()),
                    repo_url: Some(
                        "https://github.com/Nexus-Integrated-Technologies/paperclip-cloudflare"
                            .to_string(),
                    ),
                    repo_ref: Some("main".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let target = resolve_codespaces_gateway_target(&params)
            .expect("target resolution should not fail")
            .expect("cloud sandbox should resolve to a target");

        assert_eq!(target.provider, "github_codespaces");
        assert_eq!(target.reference, "paperclip-cloudflare-codespace");
        assert_eq!(target.remote_cwd, "/workspaces/paperclip-cloudflare");
        assert_eq!(
            target.repo_url.as_deref(),
            Some("https://github.com/Nexus-Integrated-Technologies/paperclip-cloudflare")
        );
        assert_eq!(target.repo_ref.as_deref(), Some("main"));
    }

    #[test]
    fn github_codespaces_codex_script_exports_only_non_secret_env_keys() {
        let script = build_codespaces_codex_script(
            "/workspaces/paperclip-cloudflare",
            &BTreeMap::from([
                ("PAPERCLIP_RUN_ID".to_string(), "run-1".to_string()),
                ("ZAI_API_KEY".to_string(), "secret'quoted".to_string()),
                ("PAPERCLIP_API_KEY".to_string(), "jwt-token".to_string()),
                ("BAD-KEY".to_string(), "do-not-export".to_string()),
            ]),
        );

        assert!(script.contains("cd '/workspaces/paperclip-cloudflare'"));
        assert!(script.contains("export PAPERCLIP_RUN_ID='run-1'"));
        assert!(!script.contains("ZAI_API_KEY"));
        assert!(!script.contains("PAPERCLIP_API_KEY"));
        assert!(!script.contains("BAD-KEY"));
        assert!(script.contains("codex CLI is not installed in this Codespace"));
        assert!(script.contains("npx -y @openai/codex"));
        assert!(script.contains("exec --json --skip-git-repo-check"));
    }

    #[test]
    fn github_codespaces_handoff_prompt_keeps_gateway_as_writeback_authority() {
        let prompt =
            render_codespaces_gateway_prompt("Paperclip wake event.\nCall /api/agents/me.");

        assert!(prompt.contains("GitHub Codespaces remote execution handoff"));
        assert!(prompt.contains("Do not call PAPERCLIP_API_URL"));
        assert!(prompt.contains("local OpenClaw gateway will write that result back"));
        assert!(prompt.contains("Paperclip wake event."));
    }

    #[test]
    fn github_codespaces_runtime_env_removes_local_control_plane_credentials() {
        let env = codespaces_runtime_env(BTreeMap::from([
            ("PAPERCLIP_RUN_ID".to_string(), "run-1".to_string()),
            (
                "PAPERCLIP_API_URL".to_string(),
                "http://127.0.0.1:3100".to_string(),
            ),
            ("PAPERCLIP_API_KEY".to_string(), "secret".to_string()),
            ("ZAI_API_KEY".to_string(), "provider-secret".to_string()),
            ("NANOCLAW_ZAI_MODEL".to_string(), "glm-4.7".to_string()),
            (
                "AZURE_OPENAI_API_KEY".to_string(),
                "azure-secret".to_string(),
            ),
            (
                "NANOCLAW_AZURE_OPENAI_DEPLOYMENT".to_string(),
                "cto-deployment".to_string(),
            ),
        ]));

        assert_eq!(
            env.get("PAPERCLIP_RUN_ID").map(String::as_str),
            Some("run-1")
        );
        assert_eq!(
            env.get("PAPERCLIP_REMOTE_HANDOFF").map(String::as_str),
            Some("gateway_writeback")
        );
        assert!(!env.contains_key("PAPERCLIP_API_URL"));
        assert!(!env.contains_key("PAPERCLIP_API_KEY"));
        assert!(!env.contains_key("ZAI_API_KEY"));
        assert!(!env.contains_key("NANOCLAW_ZAI_MODEL"));
        assert!(!env.contains_key("AZURE_OPENAI_API_KEY"));
        assert!(!env.contains_key("NANOCLAW_AZURE_OPENAI_DEPLOYMENT"));
    }

    #[test]
    fn github_codespaces_codex_auth_files_selects_codex_credentials() {
        let dir =
            std::env::temp_dir().join(format!("nanoclaw-codex-auth-files-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("auth.json"), "{}").unwrap();
        std::fs::write(dir.join("config.toml"), "").unwrap();
        std::fs::write(dir.join("history.jsonl"), "ignore").unwrap();

        let files = codespaces_codex_auth_files(&dir);
        std::fs::remove_dir_all(&dir).unwrap();

        assert_eq!(files, vec!["auth.json", "config.toml"]);
    }

    #[test]
    fn github_codespaces_ssh_command_quotes_multiline_script_as_single_remote_arg() {
        let command =
            build_codespaces_ssh_remote_command("set -eo pipefail\ncat > \"$prompt_file\"");

        assert!(command.starts_with("bash -lc '"));
        assert!(command.contains("set -eo pipefail\ncat > \"$prompt_file\""));
        assert!(!command.contains("bash -lc set -eo pipefail"));
    }

    #[test]
    fn gateway_workspace_context_mentions_execution_cwd() {
        let payload = GatewayPaperclipPayload {
            workspace: Some(GatewayPaperclipWorkspace {
                cwd: Some("/tmp/paperclip-worktree".to_string()),
                mode: Some("isolated_workspace".to_string()),
                strategy: Some("git_worktree".to_string()),
                repo_url: Some("https://example.test/repo.git".to_string()),
                ..Default::default()
            }),
            workspaces: Some(vec![GatewayPaperclipWorkspaceHint {
                workspace_id: Some("workspace-1".to_string()),
                cwd: Some("/tmp/project-primary".to_string()),
                repo_url: Some("https://example.test/repo.git".to_string()),
                repo_ref: Some("main".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        };

        let context = render_paperclip_managed_context(Some(&payload)).unwrap();

        assert!(context.contains("Paperclip execution workspace"));
        assert!(context.contains("/tmp/paperclip-worktree"));
        assert!(context.contains("git_worktree"));
        assert!(context.contains("workspace-1"));
    }
}
