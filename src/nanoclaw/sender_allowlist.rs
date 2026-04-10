use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SenderAllowlistMode {
    Trigger,
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllowedSenders {
    Any,
    Only(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatAllowlistEntry {
    pub allow: AllowedSenders,
    pub mode: SenderAllowlistMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderAllowlistConfig {
    pub default: ChatAllowlistEntry,
    pub chats: BTreeMap<String, ChatAllowlistEntry>,
    pub log_denied: bool,
}

impl Default for SenderAllowlistConfig {
    fn default() -> Self {
        Self {
            default: ChatAllowlistEntry {
                allow: AllowedSenders::Any,
                mode: SenderAllowlistMode::Trigger,
            },
            chats: BTreeMap::new(),
            log_denied: true,
        }
    }
}

pub fn load_sender_allowlist(path: &Path) -> SenderAllowlistConfig {
    let Ok(raw) = fs::read_to_string(path) else {
        return SenderAllowlistConfig::default();
    };
    let Ok(parsed) = serde_json::from_str::<Value>(&raw) else {
        return SenderAllowlistConfig::default();
    };

    let Some(default_entry) = parsed.get("default").and_then(parse_entry) else {
        return SenderAllowlistConfig::default();
    };

    let chats = parsed
        .get("chats")
        .and_then(Value::as_object)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|(jid, value)| parse_entry(value).map(|entry| (jid.clone(), entry)))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    SenderAllowlistConfig {
        default: default_entry,
        chats,
        log_denied: parsed
            .get("logDenied")
            .and_then(Value::as_bool)
            .unwrap_or(true),
    }
}

pub fn is_sender_allowed(chat_jid: &str, sender: &str, config: &SenderAllowlistConfig) -> bool {
    match &entry_for_chat(chat_jid, config).allow {
        AllowedSenders::Any => true,
        AllowedSenders::Only(entries) => entries.iter().any(|value| value == sender),
    }
}

pub fn should_drop_message(chat_jid: &str, config: &SenderAllowlistConfig) -> bool {
    matches!(
        entry_for_chat(chat_jid, config).mode,
        SenderAllowlistMode::Drop
    )
}

pub fn is_trigger_allowed(chat_jid: &str, sender: &str, config: &SenderAllowlistConfig) -> bool {
    is_sender_allowed(chat_jid, sender, config)
}

fn entry_for_chat<'a>(chat_jid: &str, config: &'a SenderAllowlistConfig) -> &'a ChatAllowlistEntry {
    config.chats.get(chat_jid).unwrap_or(&config.default)
}

fn parse_entry(value: &Value) -> Option<ChatAllowlistEntry> {
    let object = value.as_object()?;
    let allow = match object.get("allow")? {
        Value::String(value) if value == "*" => AllowedSenders::Any,
        Value::Array(entries) => AllowedSenders::Only(
            entries
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>(),
        ),
        _ => return None,
    };
    let mode = match object.get("mode").and_then(Value::as_str)? {
        "trigger" => SenderAllowlistMode::Trigger,
        "drop" => SenderAllowlistMode::Drop,
        _ => return None,
    };
    Some(ChatAllowlistEntry { allow, mode })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{
        is_sender_allowed, is_trigger_allowed, load_sender_allowlist, should_drop_message,
        AllowedSenders, SenderAllowlistConfig,
    };

    #[test]
    fn missing_file_returns_default_allowlist() {
        let temp = tempdir().unwrap();
        let config = load_sender_allowlist(&temp.path().join("missing.json"));
        assert_eq!(config, SenderAllowlistConfig::default());
    }

    #[test]
    fn parses_chat_specific_drop_entries() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("sender-allowlist.json");
        fs::write(
            &path,
            r#"{
              "default": {"allow": "*", "mode": "trigger"},
              "chats": {
                "slack:C123": {"allow": ["alice"], "mode": "drop"}
              }
            }"#,
        )
        .unwrap();

        let config = load_sender_allowlist(&path);
        assert!(should_drop_message("slack:C123", &config));
        assert!(is_sender_allowed("slack:C123", "alice", &config));
        assert!(!is_sender_allowed("slack:C123", "eve", &config));
        assert!(is_trigger_allowed("slack:C123", "alice", &config));
        assert!(!is_trigger_allowed("slack:C123", "eve", &config));
        assert!(matches!(config.default.allow, AllowedSenders::Any));
    }
}
