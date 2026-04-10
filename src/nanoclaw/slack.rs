use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::ErrorKind;
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use crate::foundation::{Group, MessageRecord};
use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use regex::Regex;
use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Error as WebSocketError, Message as WebSocketMessage, WebSocket};

use super::config::NanoclawConfig;
use super::slack_threading::{
    build_slack_thread_jid, derive_slack_thread_group, get_slack_base_jid, parse_slack_thread_jid,
};

const MAX_MESSAGE_LENGTH: usize = 4000;

#[derive(Debug, Clone)]
pub struct SlackOutboundEnvelope {
    pub id: String,
    pub chat_jid: String,
    pub text: String,
    pub timestamp: String,
}

#[derive(Debug, Clone)]
pub struct SlackInboundEvent {
    pub message: MessageRecord,
    pub base_jid: String,
    pub base_name: Option<String>,
    pub effective_name: Option<String>,
    pub is_group: bool,
    pub derived_group: Option<Group>,
}

pub struct SlackChannel {
    client: Client,
    bot_token: String,
    app_token: String,
    bot_user_id: Option<String>,
    assistant_name: String,
    default_trigger: String,
    read_only: bool,
    user_name_cache: HashMap<String, String>,
}

pub type SlackSocket = WebSocket<MaybeTlsStream<TcpStream>>;

impl SlackChannel {
    pub fn from_config(config: &NanoclawConfig, read_only: bool) -> Result<Self> {
        let env_file = config
            .slack_env_file
            .as_ref()
            .context("Slack runtime needs NANOCLAW_SLACK_ENV_FILE or a readable .env")?;
        let env = read_env_file(env_file)?;
        let bot_token = env
            .get("SLACK_BOT_TOKEN")
            .cloned()
            .context("SLACK_BOT_TOKEN is missing from Slack env file")?;
        let app_token = env
            .get("SLACK_APP_TOKEN")
            .cloned()
            .context("SLACK_APP_TOKEN is missing from Slack env file")?;

        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .context("failed to build Slack HTTP client")?,
            bot_token,
            app_token,
            bot_user_id: None,
            assistant_name: config.assistant_name.clone(),
            default_trigger: config.default_trigger.clone(),
            read_only,
            user_name_cache: HashMap::new(),
        })
    }

    pub fn connect_socket(&mut self) -> Result<SlackSocket> {
        self.ensure_bot_user_id()?;
        let response = self
            .client
            .post("https://slack.com/api/apps.connections.open")
            .header(AUTHORIZATION, format!("Bearer {}", self.app_token))
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .send()
            .context("failed to open Slack Socket Mode connection")?;
        let body = response
            .json::<Value>()
            .context("failed to decode Slack socket open response")?;
        ensure_slack_ok(&body, "apps.connections.open")?;
        let url = body
            .get("url")
            .and_then(Value::as_str)
            .context("Slack socket open response did not include websocket url")?;
        let (mut socket, _) = connect(url).context("failed to connect to Slack websocket")?;
        configure_socket(&mut socket)?;
        eprintln!("slack socket connected");
        Ok(socket)
    }

    pub fn sync_channel_metadata(&self) -> Result<Vec<(String, String)>> {
        let mut cursor = None::<String>;
        let mut channels = Vec::new();
        loop {
            let mut url =
                "https://slack.com/api/conversations.list?types=public_channel,private_channel&exclude_archived=true&limit=200"
                    .to_string();
            if let Some(current_cursor) = cursor.as_deref() {
                url.push_str("&cursor=");
                url.push_str(current_cursor);
            }
            let body = self
                .client
                .get(url)
                .header(AUTHORIZATION, format!("Bearer {}", self.bot_token))
                .send()
                .context("failed to list Slack conversations")?
                .json::<Value>()
                .context("failed to decode Slack conversations.list response")?;
            ensure_slack_ok(&body, "conversations.list")?;
            if let Some(entries) = body.get("channels").and_then(Value::as_array) {
                for entry in entries {
                    let is_member = entry
                        .get("is_member")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let Some(id) = entry.get("id").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(name) = entry.get("name").and_then(Value::as_str) else {
                        continue;
                    };
                    if is_member {
                        channels.push((format!("slack:{id}"), name.to_string()));
                    }
                }
            }
            cursor = body
                .get("response_metadata")
                .and_then(|value| value.get("next_cursor"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            if cursor.is_none() {
                break;
            }
        }
        Ok(channels)
    }

    pub fn read_event(
        &mut self,
        socket: &mut SlackSocket,
        registered_groups: &BTreeMap<String, Group>,
    ) -> Result<Option<SlackInboundEvent>> {
        match socket.read() {
            Ok(WebSocketMessage::Text(payload)) => {
                self.handle_socket_text(socket, &payload, registered_groups)
            }
            Ok(WebSocketMessage::Ping(payload)) => {
                socket
                    .send(WebSocketMessage::Pong(payload))
                    .context("failed to answer Slack websocket ping")?;
                Ok(None)
            }
            Ok(WebSocketMessage::Close(_)) => anyhow::bail!("Slack websocket closed"),
            Ok(_) => Ok(None),
            Err(WebSocketError::Io(error)) if error.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(WebSocketError::AlreadyClosed | WebSocketError::ConnectionClosed) => {
                anyhow::bail!("Slack websocket disconnected")
            }
            Err(error) => Err(error).context("failed to read Slack websocket event"),
        }
    }

    pub fn send_message(&mut self, jid: &str, text: &str) -> Result<Option<SlackOutboundEnvelope>> {
        if self.read_only {
            return Ok(None);
        }
        let formatted = format_slack_mrkdwn(text);
        if formatted.trim().is_empty() {
            return Ok(None);
        }

        let parsed_thread = parse_slack_thread_jid(jid);
        let base_jid = get_slack_base_jid(jid).unwrap_or_else(|| jid.to_string());
        let channel_id = base_jid.trim_start_matches("slack:");

        let mut first_timestamp = None::<String>;
        for chunk in split_for_slack(&formatted) {
            let mut payload = json!({
                "channel": channel_id,
                "mrkdwn": true,
                "text": chunk,
            });
            if let Some(parsed_thread) = parsed_thread.as_ref() {
                payload["thread_ts"] = Value::String(parsed_thread.thread_ts.clone());
            }
            let body = self
                .client
                .post("https://slack.com/api/chat.postMessage")
                .header(AUTHORIZATION, format!("Bearer {}", self.bot_token))
                .json(&payload)
                .send()
                .with_context(|| format!("failed to send Slack message to {jid}"))?
                .json::<Value>()
                .context("failed to decode Slack chat.postMessage response")?;
            ensure_slack_ok(&body, "chat.postMessage")?;
            if first_timestamp.is_none() {
                first_timestamp = body.get("ts").and_then(Value::as_str).map(str::to_string);
            }
        }

        Ok(first_timestamp
            .map(|timestamp| SlackOutboundEnvelope {
                id: timestamp.clone(),
                chat_jid: jid.to_string(),
                text: text.to_string(),
                timestamp: slack_ts_to_iso(&timestamp),
            })
            .map(|envelope| {
                eprintln!(
                    "slack outbound sent jid={} timestamp={}",
                    envelope.chat_jid, envelope.id
                );
                envelope
            }))
    }

    fn handle_socket_text(
        &mut self,
        socket: &mut SlackSocket,
        payload: &str,
        registered_groups: &BTreeMap<String, Group>,
    ) -> Result<Option<SlackInboundEvent>> {
        let body = serde_json::from_str::<Value>(payload)
            .with_context(|| format!("failed to parse Slack websocket payload: {payload}"))?;
        if let Some(envelope_id) = body.get("envelope_id").and_then(Value::as_str) {
            let ack = json!({ "envelope_id": envelope_id }).to_string();
            socket
                .send(WebSocketMessage::Text(ack))
                .context("failed to acknowledge Slack websocket envelope")?;
        }

        match body.get("type").and_then(Value::as_str) {
            Some("hello") => {
                eprintln!("slack socket hello");
                Ok(None)
            }
            Some("disconnect") => {
                let reason = body
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                anyhow::bail!("Slack requested reconnect ({reason})")
            }
            Some("events_api") => self.decode_events_api(body.get("payload"), registered_groups),
            other => {
                eprintln!(
                    "slack socket ignored envelope type={}",
                    other.unwrap_or("unknown")
                );
                Ok(None)
            }
        }
    }

    fn decode_events_api(
        &mut self,
        payload: Option<&Value>,
        registered_groups: &BTreeMap<String, Group>,
    ) -> Result<Option<SlackInboundEvent>> {
        let Some(event) = payload.and_then(|value| value.get("event")) else {
            return Ok(None);
        };

        let subtype = event.get("subtype").and_then(Value::as_str);
        if subtype.is_some() && subtype != Some("bot_message") {
            return Ok(None);
        }

        let Some(text) = event.get("text").and_then(Value::as_str) else {
            return Ok(None);
        };
        let Some(channel_id) = event.get("channel").and_then(Value::as_str) else {
            return Ok(None);
        };
        let Some(timestamp) = event.get("ts").and_then(Value::as_str) else {
            return Ok(None);
        };

        let base_jid = format!("slack:{channel_id}");
        let effective_jid = match event.get("thread_ts").and_then(Value::as_str) {
            Some(thread_ts) if thread_ts != timestamp => {
                build_slack_thread_jid(&base_jid, thread_ts)
            }
            _ => base_jid.clone(),
        };

        let base_group = registered_groups.get(&base_jid);
        let exact_group = registered_groups.get(&effective_jid);
        if base_group.is_none() && exact_group.is_none() {
            eprintln!(
                "slack event ignored chat_jid={} base_jid={} reason=unregistered",
                effective_jid, base_jid
            );
            return Ok(None);
        }

        let derived_group = if exact_group.is_none() && effective_jid != base_jid {
            derive_slack_thread_group(&effective_jid, registered_groups, None)
        } else {
            None
        };

        let is_bot_message = event.get("bot_id").and_then(Value::as_str).is_some()
            || event.get("user").and_then(Value::as_str) == self.bot_user_id.as_deref();
        let sender = event
            .get("user")
            .or_else(|| event.get("bot_id"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let sender_name = if is_bot_message {
            Some(self.assistant_name.clone())
        } else {
            self.resolve_user_name(event.get("user").and_then(Value::as_str))?
        };
        let mut content = text.to_string();
        if !is_bot_message {
            if let Some(bot_user_id) = self.bot_user_id.as_deref() {
                let mention = format!("<@{bot_user_id}>");
                if content.contains(&mention) && !content.contains(&self.default_trigger) {
                    content = format!("{} {}", self.default_trigger, content);
                }
            }
        }

        let inbound = SlackInboundEvent {
            message: MessageRecord {
                id: timestamp.to_string(),
                chat_jid: effective_jid.clone(),
                sender,
                sender_name,
                content,
                timestamp: slack_ts_to_iso(timestamp),
                is_from_me: is_bot_message,
                is_bot_message,
            },
            base_jid,
            base_name: base_group.map(|group| group.name.clone()),
            effective_name: exact_group
                .map(|group| group.name.clone())
                .or_else(|| derived_group.as_ref().map(|group| group.name.clone())),
            is_group: event.get("channel_type").and_then(Value::as_str) != Some("im"),
            derived_group,
        };
        eprintln!(
            "slack event accepted chat_jid={} base_jid={} sender={} bot={} text={}",
            inbound.message.chat_jid,
            inbound.base_jid,
            inbound.message.sender,
            inbound.message.is_bot_message,
            inbound
                .message
                .content
                .chars()
                .take(120)
                .collect::<String>()
                .replace('\n', "\\n")
        );
        Ok(Some(inbound))
    }

    fn ensure_bot_user_id(&mut self) -> Result<()> {
        if self.bot_user_id.is_some() {
            return Ok(());
        }
        let body = self
            .client
            .post("https://slack.com/api/auth.test")
            .header(AUTHORIZATION, format!("Bearer {}", self.bot_token))
            .send()
            .context("failed to call Slack auth.test")?
            .json::<Value>()
            .context("failed to decode Slack auth.test response")?;
        ensure_slack_ok(&body, "auth.test")?;
        self.bot_user_id = body
            .get("user_id")
            .and_then(Value::as_str)
            .map(str::to_string);
        Ok(())
    }

    fn resolve_user_name(&mut self, user_id: Option<&str>) -> Result<Option<String>> {
        let Some(user_id) = user_id.filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        if let Some(name) = self.user_name_cache.get(user_id) {
            return Ok(Some(name.clone()));
        }
        let body = self
            .client
            .get("https://slack.com/api/users.info")
            .query(&[("user", user_id)])
            .header(AUTHORIZATION, format!("Bearer {}", self.bot_token))
            .send()
            .with_context(|| format!("failed to resolve Slack user {user_id}"))?
            .json::<Value>()
            .context("failed to decode Slack users.info response")?;
        ensure_slack_ok(&body, "users.info")?;
        let name = body
            .get("user")
            .and_then(|user| user.get("real_name").or_else(|| user.get("name")))
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(name) = name.as_ref() {
            self.user_name_cache
                .insert(user_id.to_string(), name.clone());
        }
        Ok(name)
    }
}

fn configure_socket(socket: &mut SlackSocket) -> Result<()> {
    match socket.get_mut() {
        MaybeTlsStream::Plain(stream) => {
            stream
                .set_nonblocking(true)
                .context("failed to set Slack socket nonblocking")?;
        }
        MaybeTlsStream::Rustls(stream) => {
            stream
                .get_mut()
                .set_nonblocking(true)
                .context("failed to set Slack rustls socket nonblocking")?;
        }
        _ => {}
    }
    Ok(())
}

fn ensure_slack_ok(body: &Value, operation: &str) -> Result<()> {
    if body.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(());
    }
    let error = body
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("unknown_error");
    anyhow::bail!("Slack {operation} failed: {error}")
}

fn slack_ts_to_iso(ts: &str) -> String {
    let mut parts = ts.split('.');
    let seconds = parts
        .next()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or_default();
    let nanos = parts
        .next()
        .and_then(|fraction| {
            let padded = if fraction.len() >= 9 {
                fraction[..9].to_string()
            } else {
                format!("{fraction:0<9}")
            };
            padded.parse::<u32>().ok()
        })
        .unwrap_or_default();
    Utc.timestamp_opt(seconds, nanos)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339()
}

fn split_for_slack(text: &str) -> Vec<String> {
    if text.len() <= MAX_MESSAGE_LENGTH {
        return vec![text.to_string()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        let extra_len = if current.is_empty() {
            line.len()
        } else {
            line.len() + 1
        };
        if !current.is_empty() && current.len() + extra_len > MAX_MESSAGE_LENGTH {
            chunks.push(current);
            current = String::new();
        }
        if !current.is_empty() {
            current.push('\n');
        }
        current.push_str(line);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    if chunks.is_empty() {
        vec![text.to_string()]
    } else {
        chunks
    }
}

fn read_env_file(path: &Path) -> Result<HashMap<String, String>> {
    let body =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut values = HashMap::new();
    for raw_line in body.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let cleaned = value
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        values.insert(key.trim().to_string(), cleaned);
    }
    Ok(values)
}

pub fn format_slack_mrkdwn(text: &str) -> String {
    let heading_re = Regex::new(r"(?m)^(#{1,6})\s+(.+)$").expect("valid heading regex");
    let bold_star_re = Regex::new(r"\*\*([^\n]+?)\*\*").expect("valid bold-star regex");
    let bold_underscore_re = Regex::new(r"__([^\n]+?)__").expect("valid bold-underscore regex");
    let strike_re = Regex::new(r"~~([^\n]+?)~~").expect("valid strike regex");
    let image_re = Regex::new(r"!\[([^\]]*)\]\((https?://[^\s)]+)\)").expect("valid image regex");
    let link_re = Regex::new(r"\[([^\]]+)\]\((https?://[^\s)]+)\)").expect("valid link regex");

    let mut output = heading_re
        .replace_all(text, |caps: &regex::Captures<'_>| format!("*{}*", &caps[2]))
        .to_string();
    output = bold_star_re
        .replace_all(&output, |caps: &regex::Captures<'_>| {
            format!("*{}*", &caps[1])
        })
        .to_string();
    output = bold_underscore_re
        .replace_all(&output, |caps: &regex::Captures<'_>| {
            format!("*{}*", &caps[1])
        })
        .to_string();
    output = strike_re
        .replace_all(&output, |caps: &regex::Captures<'_>| {
            format!("~{}~", &caps[1])
        })
        .to_string();
    output = image_re
        .replace_all(&output, |caps: &regex::Captures<'_>| {
            if caps[1].is_empty() {
                format!("<{}>", &caps[2])
            } else {
                format!("<{}|{}>", &caps[2], &caps[1])
            }
        })
        .to_string();
    link_re
        .replace_all(&output, |caps: &regex::Captures<'_>| {
            format!("<{}|{}>", &caps[2], &caps[1])
        })
        .to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use crate::foundation::Group;
    use reqwest::blocking::Client;

    use super::{format_slack_mrkdwn, SlackChannel};

    #[test]
    fn formats_markdown_for_slack() {
        let formatted =
            format_slack_mrkdwn("## Title\nSee [docs](https://example.com) and **bold**.");
        assert_eq!(
            formatted,
            "*Title*\nSee <https://example.com|docs> and *bold*."
        );
    }

    #[test]
    fn slack_decoder_ignores_unregistered_rooms() {
        let mut channel = SlackChannel {
            client: Client::builder().build().unwrap(),
            bot_token: "xoxb-test".to_string(),
            app_token: "xapp-test".to_string(),
            bot_user_id: Some("UANDY".to_string()),
            assistant_name: "Andy".to_string(),
            default_trigger: "@Andy".to_string(),
            read_only: false,
            user_name_cache: HashMap::new(),
        };
        let payload = serde_json::json!({
            "event": {
                "type": "message",
                "channel": "C0TEST",
                "channel_type": "channel",
                "user": "U123",
                "text": "hello",
                "ts": "1774956749.012119"
            }
        });
        let groups = BTreeMap::<String, Group>::new();
        assert!(channel
            .decode_events_api(Some(&payload), &groups)
            .unwrap()
            .is_none());
    }
}
