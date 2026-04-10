use std::thread;

use anyhow::{Context, Result};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

use super::app::NanoclawApp;
use super::config::NanoclawConfig;
use super::github_webhook::{handle_github_webhook, GithubWebhookPayload, GithubWebhookResult};
use super::linear::{run_linear_issue_quality_task, LinearIssueQualityTaskInput};
use super::observability::{
    describe_observability_readiness, ingest_observability_event, verify_observability_token,
};
use super::omx::{
    apply_omx_webhook_payload, describe_omx_readiness, is_valid_omx_token,
    parse_omx_webhook_payload,
};
use super::pm::PmAuditEvent;
use super::service_slack::{
    ensure_registered_group, record_slack_message, send_recorded_slack_message,
};
use super::slack::SlackChannel;
use super::slack_threading::build_slack_thread_jid;

type HmacSha256 = Hmac<Sha256>;

const STALE_WEBHOOK_WINDOW_MS: i64 = 5 * 60 * 1_000;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LinearWebhookPayload {
    #[serde(rename = "type")]
    kind: Option<String>,
    action: Option<String>,
    webhook_timestamp: Option<i64>,
    data: Option<LinearWebhookIssueData>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LinearWebhookIssueData {
    id: Option<String>,
    identifier: Option<String>,
    title: Option<String>,
    description: Option<String>,
    url: Option<String>,
    state: Option<LinearWebhookState>,
    assignee: Option<LinearWebhookAssignee>,
    labels: Option<LinearWebhookLabelConnection>,
    priority: Option<i64>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct LinearWebhookState {
    name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct LinearWebhookAssignee {
    name: Option<String>,
    email: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct LinearWebhookLabelConnection {
    nodes: Option<Vec<LinearWebhookLabelNode>>,
}

#[derive(Debug, Clone, Deserialize)]
struct LinearWebhookLabelNode {
    name: Option<String>,
}

pub fn start_webhook_server(config: NanoclawConfig) -> Result<()> {
    if config.linear_webhook_port == 0 {
        return Ok(());
    }

    let address = format!("0.0.0.0:{}", config.linear_webhook_port);
    let server = Server::http(&address)
        .map_err(|error| anyhow::anyhow!("failed to bind webhook server on {address}: {error}"))?;

    thread::spawn(move || {
        let mut app = match NanoclawApp::open(config.clone()) {
            Ok(app) => app,
            Err(error) => {
                eprintln!("webhook server failed to open app: {error:#}");
                return;
            }
        };
        let mut channel = match SlackChannel::from_config(&config, false) {
            Ok(channel) => Some(channel),
            Err(error) => {
                eprintln!("webhook server Slack channel unavailable: {error:#}");
                None
            }
        };

        eprintln!("webhook server listening on {address}");
        for mut request in server.incoming_requests() {
            let response = match route_request(&config, &mut app, &mut channel, &mut request) {
                Ok((status, body)) => json_response(status, body),
                Err(error) => {
                    eprintln!("webhook request failed: {error:#}");
                    json_response(
                        500,
                        json!({
                            "ok": false,
                            "error": error.to_string(),
                        }),
                    )
                }
            };
            if let Err(error) = request.respond(response) {
                eprintln!("webhook response write failed: {error:#}");
            }
        }
    });

    Ok(())
}

fn route_request(
    config: &NanoclawConfig,
    app: &mut NanoclawApp,
    channel: &mut Option<SlackChannel>,
    request: &mut Request,
) -> Result<(u16, Value)> {
    let path = request
        .url()
        .split('?')
        .next()
        .unwrap_or(request.url())
        .to_string();

    match (request.method(), path.as_str()) {
        (&Method::Get, "/health") => Ok((200, json!({ "status": "ok" }))),
        (&Method::Get, "/webhook/linear") => Ok((
            200,
            json!({
                "ok": true,
                "endpoint": "linear-webhook",
                "status": "ready",
                "method": "POST",
                "signatureRequired": !config.linear_webhook_secret.trim().is_empty(),
                "hint": "Send a signed POST request to this endpoint from Linear."
            }),
        )),
        (&Method::Get, "/webhook/github") => Ok((
            200,
            json!({
                "ok": true,
                "endpoint": "github-webhook",
                "status": "ready",
                "method": "POST",
                "signatureRequired": !config.github_webhook_secret.trim().is_empty(),
                "hint": "Send a signed GitHub webhook POST request to this endpoint."
            }),
        )),
        (&Method::Get, "/webhook/observability") => {
            Ok((200, describe_observability_readiness(config)))
        }
        (&Method::Get, "/webhook/omx") => Ok((200, describe_omx_readiness(config))),
        (&Method::Post, "/webhook/linear") => handle_linear_webhook(config, app, channel, request),
        (&Method::Post, "/webhook/github") => {
            handle_github_webhook_request(config, app, channel, request)
        }
        (&Method::Post, "/webhook/observability") => {
            handle_observability_webhook(config, app, channel, request)
        }
        (&Method::Post, "/webhook/omx") => handle_omx_webhook(config, app, channel, request),
        _ => Ok((
            404,
            json!({
                "ok": false,
                "error": format!("Unknown route {}", path),
            }),
        )),
    }
}

fn handle_linear_webhook(
    config: &NanoclawConfig,
    app: &mut NanoclawApp,
    channel: &mut Option<SlackChannel>,
    request: &mut Request,
) -> Result<(u16, Value)> {
    let body = read_request_body(request)?;
    let signature = request_header(request, "x-linear-signature").unwrap_or_default();
    if !verify_hmac_signature(&config.linear_webhook_secret, &signature, &body) {
        return Ok((401, json!({ "error": "Invalid signature" })));
    }

    let payload = match serde_json::from_slice::<LinearWebhookPayload>(&body) {
        Ok(payload) => payload,
        Err(_) => return Ok((400, json!({ "error": "Invalid JSON" }))),
    };

    if let Some(timestamp) = payload.webhook_timestamp {
        let now_ms = chrono::Utc::now().timestamp_millis();
        if (now_ms - timestamp).abs() > STALE_WEBHOOK_WINDOW_MS {
            return Ok((400, json!({ "error": "Stale webhook" })));
        }
    }

    let action = payload
        .action
        .as_deref()
        .unwrap_or("unknown")
        .trim()
        .to_string();
    let event_type = request_header(request, "linear-event")
        .filter(|value| !value.trim().is_empty())
        .or_else(|| payload.kind.clone())
        .unwrap_or_default();
    let issue_key = resolve_linear_issue_thread_key(&payload);
    let issue_identifier = payload
        .data
        .as_ref()
        .and_then(|data| data.identifier.as_ref())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    if !event_type.trim().is_empty() && event_type.trim() != "Issue" {
        let _ = app.db.record_pm_audit_event(&PmAuditEvent {
            issue_key: issue_key.clone(),
            issue_identifier: issue_identifier.clone(),
            thread_jid: None,
            phase: "webhook_ignored".to_string(),
            status: "ignored".to_string(),
            tool: Some("linear-webhook".to_string()),
            error_code: Some("non-issue-event".to_string()),
            blocking: false,
            metadata: Some(json!({
                "action": action,
                "eventType": event_type,
            })),
            created_at: None,
        });
        return Ok((
            200,
            json!({ "ok": true, "ignored": true, "reason": "non-issue-event" }),
        ));
    }

    if !is_actionable_linear_issue_payload(&payload) {
        let _ = app.db.record_pm_audit_event(&PmAuditEvent {
            issue_key: issue_key.clone(),
            issue_identifier: issue_identifier.clone(),
            thread_jid: None,
            phase: "webhook_ignored".to_string(),
            status: "ignored".to_string(),
            tool: Some("linear-webhook".to_string()),
            error_code: Some("incomplete-payload".to_string()),
            blocking: false,
            metadata: Some(json!({
                "action": action,
                "eventType": if event_type.trim().is_empty() { Value::Null } else { Value::String(event_type.clone()) },
                "dataKeys": payload
                    .data
                    .as_ref()
                    .map(observed_linear_payload_keys)
                    .unwrap_or_default(),
            })),
            created_at: None,
        });
        return Ok((200, json!({ "ok": true, "ignored": true })));
    }

    let _ = app.db.record_pm_audit_event(&PmAuditEvent {
        issue_key: issue_key.clone(),
        issue_identifier: issue_identifier.clone(),
        thread_jid: None,
        phase: "webhook_received".to_string(),
        status: "started".to_string(),
        tool: Some("linear-webhook".to_string()),
        error_code: None,
        blocking: false,
        metadata: Some(json!({
            "action": action,
            "title": payload.data.as_ref().and_then(|data| data.title.clone()),
            "eventType": if event_type.trim().is_empty() { "Issue".to_string() } else { event_type.clone() },
        })),
        created_at: None,
    });

    if let Err(error) = deliver_linear_webhook(
        config,
        app,
        channel,
        &payload,
        issue_key.as_deref(),
        issue_identifier.as_deref(),
    ) {
        let _ = app.db.record_pm_audit_event(&PmAuditEvent {
            issue_key: issue_key.clone(),
            issue_identifier: issue_identifier.clone(),
            thread_jid: if config.linear_chat_jid.trim().is_empty() {
                None
            } else {
                Some(config.linear_chat_jid.clone())
            },
            phase: "webhook_delivery_failed".to_string(),
            status: "failed".to_string(),
            tool: Some("linear-webhook".to_string()),
            error_code: Some("chat-delivery-failed".to_string()),
            blocking: true,
            metadata: Some(json!({
                "action": action,
                "message": error.to_string(),
            })),
            created_at: None,
        });
        eprintln!("linear webhook delivery failed: {error:#}");
    }

    if let Some(identifier) = issue_identifier.as_deref() {
        let quality = run_linear_issue_quality_task(
            &app.db,
            config,
            LinearIssueQualityTaskInput {
                identifier: identifier.to_string(),
                apply: true,
            },
        );
        let _ = app.db.record_pm_audit_event(&PmAuditEvent {
            issue_key,
            issue_identifier: Some(identifier.to_string()),
            thread_jid: None,
            phase: if quality.ok {
                "quality_check_succeeded".to_string()
            } else {
                "quality_check_failed".to_string()
            },
            status: if quality.ok {
                "succeeded".to_string()
            } else {
                "failed".to_string()
            },
            tool: Some("linear-quality-check".to_string()),
            error_code: if quality.ok {
                None
            } else {
                Some("linear-quality-check-failed".to_string())
            },
            blocking: false,
            metadata: Some(if quality.ok {
                json!({
                    "score": quality.score,
                    "gapCount": quality.gaps.as_ref().map(Vec::len).unwrap_or(0),
                })
            } else {
                json!({
                    "message": quality.error,
                })
            }),
            created_at: None,
        });
    }

    Ok((200, json!({ "ok": true })))
}

fn deliver_linear_webhook(
    config: &NanoclawConfig,
    app: &mut NanoclawApp,
    channel: &mut Option<SlackChannel>,
    payload: &LinearWebhookPayload,
    issue_key: Option<&str>,
    issue_identifier: Option<&str>,
) -> Result<()> {
    let Some(channel) = channel.as_mut() else {
        return Ok(());
    };

    let existing_thread = match issue_key {
        Some(issue_key) => app.db.get_linear_issue_thread(issue_key)?,
        None => None,
    }
    .or(match issue_identifier {
        Some(identifier) => app.db.get_linear_issue_thread_by_identifier(identifier)?,
        None => None,
    });

    let thread_name = build_linear_thread_name(payload);
    let message = format_linear_issue_event(payload);

    if let Some(existing_thread) = existing_thread {
        ensure_registered_group(app, &existing_thread.chat_jid, Some(&thread_name))?;
        if let Some(issue_key) = issue_key {
            app.db.set_linear_issue_thread(
                issue_key,
                &existing_thread.chat_jid,
                &existing_thread.thread_ts,
                issue_identifier,
            )?;
        }

        let _ = app.db.record_pm_audit_event(&PmAuditEvent {
            issue_key: issue_key.map(str::to_string),
            issue_identifier: issue_identifier.map(str::to_string),
            thread_jid: Some(existing_thread.chat_jid.clone()),
            phase: "thread_reused".to_string(),
            status: "succeeded".to_string(),
            tool: Some("linear-webhook".to_string()),
            error_code: None,
            blocking: false,
            metadata: Some(json!({
                "threadTs": existing_thread.thread_ts,
            })),
            created_at: None,
        });

        send_recorded_slack_message(
            app,
            channel,
            &existing_thread.chat_jid,
            Some(&thread_name),
            &message,
            "linear-webhook",
            Some("Linear Webhook"),
            false,
            false,
        )?;
    } else if !config.linear_chat_jid.trim().is_empty() {
        ensure_registered_group(app, &config.linear_chat_jid, Some("Linear Issues"))?;
        if let Some(outbound) = channel.send_message(&config.linear_chat_jid, &message)? {
            let target_jid = build_slack_thread_jid(&config.linear_chat_jid, &outbound.id);
            ensure_registered_group(app, &target_jid, Some(&thread_name))?;
            let thread_key = issue_key
                .map(str::to_string)
                .or_else(|| issue_identifier.map(|identifier| format!("identifier:{identifier}")));
            if let Some(thread_key) = thread_key.as_deref() {
                app.db.set_linear_issue_thread(
                    thread_key,
                    &target_jid,
                    &outbound.id,
                    issue_identifier,
                )?;
            }
            record_slack_message(
                app,
                &outbound.timestamp,
                &target_jid,
                Some(&thread_name),
                &outbound.id,
                &message,
                "linear-webhook",
                Some("Linear Webhook"),
                false,
                false,
            )?;
            let _ = app.db.record_pm_audit_event(&PmAuditEvent {
                issue_key: thread_key,
                issue_identifier: issue_identifier.map(str::to_string),
                thread_jid: Some(target_jid),
                phase: "thread_created".to_string(),
                status: "succeeded".to_string(),
                tool: Some("linear-webhook".to_string()),
                error_code: None,
                blocking: false,
                metadata: Some(json!({
                    "threadTs": outbound.id,
                })),
                created_at: None,
            });
        }
    }

    if let Some(issue_key) = issue_key {
        if is_closed_linear_issue(payload) {
            app.db
                .mark_linear_issue_thread_closed(issue_key, &chrono::Utc::now().to_rfc3339())?;
            let _ = app.db.record_pm_audit_event(&PmAuditEvent {
                issue_key: Some(issue_key.to_string()),
                issue_identifier: issue_identifier.map(str::to_string),
                thread_jid: None,
                phase: "issue_closed".to_string(),
                status: "succeeded".to_string(),
                tool: Some("linear-webhook".to_string()),
                error_code: None,
                blocking: false,
                metadata: Some(json!({
                    "state": payload
                        .data
                        .as_ref()
                        .and_then(|data| data.state.as_ref())
                        .and_then(|state| state.name.clone()),
                    "action": payload.action,
                })),
                created_at: None,
            });
        } else {
            app.db.reopen_linear_issue_thread(issue_key)?;
        }
    }

    Ok(())
}

fn handle_github_webhook_request(
    config: &NanoclawConfig,
    app: &mut NanoclawApp,
    channel: &mut Option<SlackChannel>,
    request: &mut Request,
) -> Result<(u16, Value)> {
    let body = read_request_body(request)?;
    let signature = request_header(request, "x-hub-signature-256").unwrap_or_default();
    if !verify_hmac_signature(&config.github_webhook_secret, &signature, &body) {
        return Ok((401, json!({ "error": "Invalid signature" })));
    }

    let payload = match serde_json::from_slice::<GithubWebhookPayload>(&body) {
        Ok(payload) => payload,
        Err(_) => return Ok((400, json!({ "error": "Invalid JSON" }))),
    };
    let event_type = request_header(request, "x-github-event").unwrap_or_default();
    let mut result = handle_github_webhook(&app.db, config, &payload, &event_type);
    deliver_github_notifications(config, app, channel, &mut result);
    Ok((200, serde_json::to_value(result)?))
}

fn handle_observability_webhook(
    config: &NanoclawConfig,
    app: &mut NanoclawApp,
    channel: &mut Option<SlackChannel>,
    request: &mut Request,
) -> Result<(u16, Value)> {
    if config.observability_webhook_token.trim().is_empty() {
        return Ok((
            503,
            json!({
                "ok": false,
                "error": "Observability webhook token is not configured",
            }),
        ));
    }

    let authorization_header = request_header(request, "authorization");
    let token_header = request_header(request, "x-observability-token");
    if !verify_observability_token(
        &config.observability_webhook_token,
        authorization_header.as_deref(),
        token_header.as_deref(),
    ) {
        return Ok((401, json!({ "ok": false, "error": "Invalid token" })));
    }

    let body = read_request_body(request)?;
    let payload = match serde_json::from_slice::<Value>(&body) {
        Ok(payload) => payload,
        Err(_) => return Ok((400, json!({ "ok": false, "error": "Invalid JSON" }))),
    };
    let result = ingest_observability_event(app, channel.as_mut(), payload)?;
    Ok((
        200,
        json!({
            "ok": true,
            "created": result.created,
            "eventId": result.event.id,
            "fingerprint": result.event.fingerprint,
            "targetJid": result.target_jid,
            "blueTeamRunId": result.blue_team_run_id,
        }),
    ))
}

fn handle_omx_webhook(
    config: &NanoclawConfig,
    app: &mut NanoclawApp,
    channel: &mut Option<SlackChannel>,
    request: &mut Request,
) -> Result<(u16, Value)> {
    let authorization_header = request_header(request, "authorization");
    let token_header = request_header(request, "x-nanoclaw-omx-token");
    let header_token = authorization_header
        .as_deref()
        .and_then(|value| value.strip_prefix("Bearer "))
        .or(token_header.as_deref());
    if !is_valid_omx_token(config, header_token) {
        return Ok((401, json!({ "ok": false, "error": "Invalid token" })));
    }

    let body = read_request_body(request)?;
    let payload = match parse_omx_webhook_payload(&body) {
        Ok(payload) => payload,
        Err(_) => return Ok((400, json!({ "ok": false, "error": "Invalid JSON" }))),
    };
    if !is_valid_omx_token(config, payload.token.as_deref().or(header_token)) {
        return Ok((401, json!({ "ok": false, "error": "Invalid token" })));
    }

    let body = apply_omx_webhook_payload(app, channel, payload)?;
    Ok((200, body))
}

fn deliver_github_notifications(
    config: &NanoclawConfig,
    app: &mut NanoclawApp,
    channel: &mut Option<SlackChannel>,
    result: &mut GithubWebhookResult,
) {
    let Some(channel) = channel.as_mut() else {
        return;
    };

    for notification in &result.notifications {
        let target_chat_jid = notification
            .target_chat_jid
            .clone()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                (!config.linear_chat_jid.trim().is_empty())
                    .then_some(config.linear_chat_jid.clone())
            });
        let Some(target_chat_jid) = target_chat_jid else {
            continue;
        };
        let suggested_name = if target_chat_jid.contains("::thread:") {
            format!("Linear Issues • {}", notification.identifier)
        } else {
            "Linear PM".to_string()
        };
        if let Err(error) = ensure_registered_group(app, &target_chat_jid, Some(&suggested_name))
            .and_then(|_| {
                send_recorded_slack_message(
                    app,
                    channel,
                    &target_chat_jid,
                    Some(&suggested_name),
                    &notification.body,
                    "github-webhook",
                    Some("GitHub Webhook"),
                    false,
                    false,
                )
                .map(|_| ())
            })
        {
            result.errors.push(format!(
                "{}: failed to deliver Slack notification: {}",
                notification.identifier, error
            ));
        }
    }

    if !result.errors.is_empty() {
        result.ok = false;
    }
}

fn request_header(request: &Request, name: &str) -> Option<String> {
    request
        .headers()
        .iter()
        .find(|header| header.field.to_string().eq_ignore_ascii_case(name))
        .map(|header| header.value.as_str().trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_request_body(request: &mut Request) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    request
        .as_reader()
        .read_to_end(&mut body)
        .context("failed to read webhook request body")?;
    Ok(body)
}

fn json_response(status: u16, body: Value) -> Response<std::io::Cursor<Vec<u8>>> {
    Response::from_string(body.to_string())
        .with_status_code(StatusCode(status))
        .with_header(json_header())
}

fn json_header() -> Header {
    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
        .expect("valid content-type header")
}

fn verify_hmac_signature(secret: &str, signature: &str, body: &[u8]) -> bool {
    if secret.trim().is_empty() || signature.trim().is_empty() {
        return true;
    }
    let Some(signature_hex) = signature.strip_prefix("sha256=") else {
        return false;
    };
    let Some(signature_bytes) = decode_hex(signature_hex) else {
        return false;
    };
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(mac) => mac,
        Err(_) => return false,
    };
    mac.update(body);
    mac.verify_slice(&signature_bytes).is_ok()
}

fn decode_hex(input: &str) -> Option<Vec<u8>> {
    if input.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    let chars = input.as_bytes();
    let mut index = 0usize;
    while index < chars.len() {
        let high = decode_hex_nibble(chars[index])?;
        let low = decode_hex_nibble(chars[index + 1])?;
        bytes.push((high << 4) | low);
        index += 2;
    }
    Some(bytes)
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn resolve_linear_issue_thread_key(payload: &LinearWebhookPayload) -> Option<String> {
    if let Some(issue_id) = payload
        .data
        .as_ref()
        .and_then(|data| data.id.as_ref())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        return Some(format!("linear:{issue_id}"));
    }

    payload
        .data
        .as_ref()
        .and_then(|data| data.identifier.as_ref())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|identifier| format!("identifier:{identifier}"))
}

fn observed_linear_payload_keys(data: &LinearWebhookIssueData) -> Vec<&'static str> {
    let mut keys = Vec::new();
    if data.id.as_deref().is_some() {
        keys.push("id");
    }
    if data.identifier.as_deref().is_some() {
        keys.push("identifier");
    }
    if data.title.as_deref().is_some() {
        keys.push("title");
    }
    if data.description.as_deref().is_some() {
        keys.push("description");
    }
    if data.url.as_deref().is_some() {
        keys.push("url");
    }
    if data
        .state
        .as_ref()
        .and_then(|state| state.name.as_deref())
        .is_some()
    {
        keys.push("state");
    }
    if data
        .assignee
        .as_ref()
        .and_then(|assignee| assignee.name.as_deref().or(assignee.email.as_deref()))
        .is_some()
    {
        keys.push("assignee");
    }
    if data
        .labels
        .as_ref()
        .and_then(|labels| labels.nodes.as_ref())
        .map(|nodes| nodes.iter().any(|label| label.name.as_deref().is_some()))
        .unwrap_or(false)
    {
        keys.push("labels");
    }
    if data.priority.is_some() {
        keys.push("priority");
    }
    if data.created_at.as_deref().is_some() {
        keys.push("createdAt");
    }
    if data.updated_at.as_deref().is_some() {
        keys.push("updatedAt");
    }
    keys
}

fn is_actionable_linear_issue_payload(payload: &LinearWebhookPayload) -> bool {
    let Some(data) = payload.data.as_ref() else {
        return false;
    };
    data.identifier
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
        || data
            .title
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some()
        || data
            .url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some()
        || data
            .state
            .as_ref()
            .and_then(|state| state.name.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some()
        || data
            .assignee
            .as_ref()
            .and_then(|assignee| assignee.name.as_deref().or(assignee.email.as_deref()))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some()
        || data
            .description
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some()
        || data
            .labels
            .as_ref()
            .and_then(|labels| labels.nodes.as_ref())
            .map(|nodes| {
                nodes.iter().any(|label| {
                    label
                        .name
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .is_some()
                })
            })
            .unwrap_or(false)
        || data.priority.is_some()
}

fn is_closed_linear_issue(payload: &LinearWebhookPayload) -> bool {
    if payload.action.as_deref() == Some("remove") {
        return true;
    }

    payload
        .data
        .as_ref()
        .and_then(|data| data.state.as_ref())
        .and_then(|state| state.name.as_ref())
        .map(|value| value.trim().to_ascii_lowercase())
        .map(|value| {
            [
                "done",
                "canceled",
                "cancelled",
                "closed",
                "complete",
                "completed",
            ]
            .contains(&value.as_str())
        })
        .unwrap_or(false)
}

fn build_linear_thread_name(payload: &LinearWebhookPayload) -> String {
    payload
        .data
        .as_ref()
        .and_then(|data| data.identifier.as_ref())
        .map(|identifier| format!("Linear Issues • {}", identifier.trim()))
        .unwrap_or_else(|| "Linear Issues Thread".to_string())
}

fn format_linear_issue_event(payload: &LinearWebhookPayload) -> String {
    let action = payload
        .action
        .as_deref()
        .unwrap_or("unknown")
        .to_uppercase();
    let data = payload.data.as_ref();
    let identifier = data
        .and_then(|data| data.identifier.as_deref())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("???");
    let title = data
        .and_then(|data| data.title.as_deref())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("(no title)");
    let state = data
        .and_then(|data| data.state.as_ref())
        .and_then(|state| state.name.as_deref())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("???");
    let assignee = data
        .and_then(|data| data.assignee.as_ref())
        .and_then(|assignee| assignee.name.as_deref().or(assignee.email.as_deref()))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("unassigned");
    let labels = data
        .and_then(|data| data.labels.as_ref())
        .and_then(|labels| labels.nodes.as_ref())
        .map(|nodes| {
            nodes
                .iter()
                .filter_map(|label| label.name.as_ref())
                .map(|name| name.trim())
                .filter(|name| !name.is_empty())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    let url = data
        .and_then(|data| data.url.as_deref())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("");
    let description = data
        .and_then(|data| data.description.as_deref())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| {
            let mut shortened = value.chars().take(300).collect::<String>();
            if value.chars().count() > 300 {
                shortened.push_str("...");
            }
            shortened
        })
        .unwrap_or_else(|| "(no description)".to_string());

    let mut message = format!("*Linear Webhook - {action}*\n");
    message.push_str(&format!("*Issue:* {identifier} - {title}\n"));
    message.push_str(&format!("*State:* {state}\n"));
    message.push_str(&format!("*Assignee:* {assignee}\n"));
    if !labels.is_empty() {
        message.push_str(&format!("*Labels:* {labels}\n"));
    }
    if !url.is_empty() {
        message.push_str(&format!("*URL:* {url}\n"));
    }
    message.push_str(&format!("*Description:* {description}"));
    message
}

#[cfg(test)]
mod tests {
    use super::{
        format_linear_issue_event, is_actionable_linear_issue_payload, is_closed_linear_issue,
        verify_hmac_signature, LinearWebhookIssueData, LinearWebhookPayload, LinearWebhookState,
    };
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type TestHmacSha256 = Hmac<Sha256>;

    #[test]
    fn verifies_hmac_signature_round_trip() {
        let body = br#"{"hello":"world"}"#;
        let mut mac = TestHmacSha256::new_from_slice(b"secret").unwrap();
        mac.update(body);
        let signature = mac.finalize().into_bytes();
        let header = format!(
            "sha256={}",
            signature
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        assert!(verify_hmac_signature("secret", &header, body));
        assert!(!verify_hmac_signature("other", &header, body));
    }

    #[test]
    fn detects_actionable_payloads() {
        let empty = LinearWebhookPayload {
            kind: Some("Issue".to_string()),
            action: Some("create".to_string()),
            webhook_timestamp: None,
            data: Some(LinearWebhookIssueData {
                id: None,
                identifier: None,
                title: None,
                description: None,
                url: None,
                state: None,
                assignee: None,
                labels: None,
                priority: None,
                created_at: None,
                updated_at: None,
            }),
        };
        assert!(!is_actionable_linear_issue_payload(&empty));

        let actionable = LinearWebhookPayload {
            kind: Some("Issue".to_string()),
            action: Some("update".to_string()),
            webhook_timestamp: None,
            data: Some(LinearWebhookIssueData {
                id: Some("issue-1".to_string()),
                identifier: Some("BYB-23".to_string()),
                title: Some("Ship Rust PM automation".to_string()),
                description: None,
                url: None,
                state: Some(LinearWebhookState {
                    name: Some("In Progress".to_string()),
                }),
                assignee: None,
                labels: None,
                priority: None,
                created_at: None,
                updated_at: None,
            }),
        };
        assert!(is_actionable_linear_issue_payload(&actionable));
        assert!(format_linear_issue_event(&actionable).contains("BYB-23"));
    }

    #[test]
    fn detects_closed_states() {
        let payload = LinearWebhookPayload {
            kind: Some("Issue".to_string()),
            action: Some("update".to_string()),
            webhook_timestamp: None,
            data: Some(LinearWebhookIssueData {
                id: Some("issue-1".to_string()),
                identifier: Some("BYB-23".to_string()),
                title: Some("Done".to_string()),
                description: None,
                url: None,
                state: Some(LinearWebhookState {
                    name: Some("Done".to_string()),
                }),
                assignee: None,
                labels: None,
                priority: None,
                created_at: None,
                updated_at: None,
            }),
        };
        assert!(is_closed_linear_issue(&payload));
    }
}
