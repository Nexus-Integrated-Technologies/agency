use std::collections::HashMap;
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
    build_execution_session, ExecutionLaneRouter, ExecutionRequest, ExecutionResponse,
    ExecutorBoundary,
};
use super::omx::{OmxExecutionOptions, OmxMode};

const PROTOCOL_VERSION: u64 = 3;
const DEFAULT_WAIT_TIMEOUT_MS: u64 = 30_000;
const RUN_POLL_INTERVAL_MS: u64 = 250;

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
    gateway: Option<GatewayHints>,
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
        return respond_http_health(&mut stream, &state.config);
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

    loop {
        if let Some(existing) = find_run(&state.runs, &run_id)? {
            if existing.status.is_terminal() {
                return write_ok_frame(socket, request_id, run_record_payload(&existing));
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
        script: None,
        omx: gateway_omx_options(params, &lane),
        assistant_name: state.config.assistant_name.clone(),
        request_plane: RequestPlane::Web,
        session,
        backend_override: None,
    };

    let execution = executor.execute(request)?;
    mark_run_success(&state.runs, run_id, &group.folder, &execution, lane)
}

fn mark_run_success(
    runs: &Arc<Mutex<HashMap<String, GatewayRunRecord>>>,
    run_id: &str,
    group_folder: &str,
    execution: &ExecutionResponse,
    lane: ExecutionLane,
) -> Result<()> {
    let metadata = execution.metadata.as_ref();
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
    let agent_meta = json!({
        "provider": provider,
        "biller": biller,
        "billingType": billing_type,
        "model": model,
        "usage": usage_value.clone(),
        "costUsd": cost_usd,
    });
    let result_meta = json!({
        "provider": agent_meta.get("provider").cloned().unwrap_or(Value::Null),
        "biller": agent_meta.get("biller").cloned().unwrap_or(Value::Null),
        "billingType": agent_meta.get("billingType").cloned().unwrap_or(Value::Null),
        "model": agent_meta.get("model").cloned().unwrap_or(Value::Null),
        "usage": agent_meta.get("usage").cloned().unwrap_or(Value::Null),
        "costUsd": agent_meta.get("costUsd").cloned().unwrap_or(Value::Null),
        "agentMeta": agent_meta,
        "gateway": {
            "lane": lane.as_str(),
            "sessionId": execution.session_id,
            "tmuxSession": metadata.and_then(|value| value.tmux_session.clone()),
            "teamName": metadata.and_then(|value| value.team_name.clone()),
            "summary": metadata.and_then(|value| value.summary.clone()),
            "question": metadata.and_then(|value| value.question.clone()),
        }
    });
    let mut guard = runs
        .lock()
        .map_err(|_| anyhow::anyhow!("failed to lock gateway run state"))?;
    let existing = guard
        .get_mut(run_id)
        .context("gateway run disappeared before completion")?;
    existing.status = GatewayRunStatus::Ok;
    existing.updated_at = Utc::now().to_rfc3339();
    existing.session_id = Some(execution.session_id.clone());
    existing.group_folder = Some(group_folder.to_string());
    existing.summary = Some(
        metadata
            .and_then(|value| value.summary.clone())
            .unwrap_or_else(|| execution.text.clone()),
    );
    existing.result_text = Some(execution.text.clone());
    existing.metadata = Some(result_meta);
    existing.error = None;
    Ok(())
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
    if !(lower.starts_with("get ") || lower.starts_with("head ")) {
        return Ok(false);
    }
    Ok(!lower.contains("upgrade: websocket"))
}

fn respond_http_health(stream: &mut TcpStream, config: &NanoclawConfig) -> Result<()> {
    let mut buffer = [0u8; 4096];
    let _ = stream.read(&mut buffer);
    let body = serde_json::to_string(&json!({
        "status": "ok",
        "service": "nanoclaw-openclaw-gateway",
        "websocketUrl": config.openclaw_gateway_public_ws_url(),
        "executionLane": config.openclaw_gateway_execution_lane.as_str(),
    }))?;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .context("failed to write gateway health response")?;
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
