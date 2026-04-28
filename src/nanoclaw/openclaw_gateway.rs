use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};
use tungstenite::{accept, Message, WebSocket};
use uuid::Uuid;

use crate::foundation::{ExecutionLane, MessageRecord, RequestPlane};

use super::app::NanoclawApp;
use super::config::NanoclawConfig;
use super::executor::{
    build_execution_session, ExecutionArtifactRef, ExecutionLaneRouter, ExecutionRequest,
    ExecutionResponse, ExecutorBoundary,
};
use super::omx::{
    apply_omx_webhook_payload, describe_omx_readiness, is_valid_omx_token,
    parse_omx_webhook_payload, OmxArtifactRef, OmxExecutionOptions, OmxMode, OmxRunnerClient,
    OmxWebhookPayload,
};

const PROTOCOL_VERSION: u64 = 3;
const DEFAULT_WAIT_TIMEOUT_MS: u64 = 30_000;
const RUN_POLL_INTERVAL_MS: u64 = 250;
const WAIT_KEEPALIVE_INTERVAL_MS: u64 = 10_000;

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
    managed_context: Option<GatewayManagedContext>,
    gateway: Option<GatewayHints>,
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
    let mut last_keepalive = Instant::now();

    loop {
        if let Some(existing) = find_run(&state.runs, &run_id)? {
            if existing.status.is_terminal() {
                return write_ok_frame(socket, request_id, run_record_payload(&existing));
            }

            if last_keepalive.elapsed() >= Duration::from_millis(WAIT_KEEPALIVE_INTERVAL_MS) {
                write_json_frame(
                    socket,
                    &json!({
                        "type": "event",
                        "event": "agent.wait.keepalive",
                        "payload": run_record_payload(&existing),
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
    let workspace_root = state.config.groups_dir.join(&group.folder);
    let session_id = format!("gateway-{}", slug(run_id));
    let session = build_execution_session(
        &state.config.data_dir,
        &group.folder,
        &session_id,
        &workspace_root,
    );
    let executor = ExecutionLaneRouter::from_config(&state.config, Some(lane.clone()))?;
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
        omx: gateway_omx_options(params, &lane),
        assistant_name: state.config.assistant_name.clone(),
        request_plane: RequestPlane::Web,
        env: paperclip_runtime_env(params.paperclip.as_ref()),
        session,
        backend_override: None,
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

fn render_paperclip_managed_context(paperclip: Option<&GatewayPaperclipPayload>) -> Option<String> {
    let managed = paperclip.and_then(|value| value.managed_context.as_ref())?;
    let mut sections: Vec<String> = Vec::new();

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

    if sections.is_empty() {
        None
    } else {
        Some(sections.join("\n\n"))
    }
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
            .unwrap_or(OmxMode::Team),
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
        assert!(!env.contains_key("OPENAI_API_KEY"));
        assert!(!env.contains_key("PAPERCLIP_TASK_ID"));
    }
}
