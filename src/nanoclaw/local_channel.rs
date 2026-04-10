use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::foundation::MessageRecord;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalInboundEnvelope {
    pub id: Option<String>,
    pub chat_jid: String,
    pub sender: String,
    pub sender_name: Option<String>,
    pub content: String,
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalOutboundEnvelope {
    pub id: String,
    pub chat_jid: String,
    pub text: String,
    pub timestamp: String,
}

#[derive(Debug, Clone)]
pub struct LocalChannel {
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollResult {
    pub consumed: usize,
}

impl LocalChannel {
    pub fn new(data_dir: &Path) -> Result<Self> {
        let root = data_dir.join("channels").join("local");
        fs::create_dir_all(root.join("inbox"))
            .with_context(|| format!("failed to create {}", root.join("inbox").display()))?;
        fs::create_dir_all(root.join("outbox"))
            .with_context(|| format!("failed to create {}", root.join("outbox").display()))?;
        fs::create_dir_all(root.join("processed"))
            .with_context(|| format!("failed to create {}", root.join("processed").display()))?;
        Ok(Self { root })
    }

    pub fn inbox_path(&self) -> PathBuf {
        self.root.join("inbox")
    }

    pub fn outbox_path(&self) -> PathBuf {
        self.root.join("outbox")
    }

    pub fn enqueue_inbound(&self, envelope: LocalInboundEnvelope) -> Result<PathBuf> {
        let timestamp = envelope
            .timestamp
            .clone()
            .unwrap_or_else(|| Utc::now().to_rfc3339());
        let id = envelope
            .id
            .clone()
            .unwrap_or_else(|| format!("local-in-{}", Uuid::new_v4()));
        let payload = LocalInboundEnvelope {
            id: Some(id.clone()),
            timestamp: Some(timestamp),
            ..envelope
        };
        let file_path = self.inbox_path().join(format!("{}.json", id));
        fs::write(&file_path, serde_json::to_vec_pretty(&payload)?)
            .with_context(|| format!("failed to write {}", file_path.display()))?;
        Ok(file_path)
    }

    pub fn poll_inbound(&self) -> Result<Vec<MessageRecord>> {
        let mut files = fs::read_dir(self.inbox_path())
            .with_context(|| format!("failed to read {}", self.inbox_path().display()))?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
            .collect::<Vec<_>>();
        files.sort();

        let mut messages = Vec::new();
        for file in files {
            let bytes = fs::read(&file)
                .with_context(|| format!("failed to read inbound file {}", file.display()))?;
            let envelope: LocalInboundEnvelope = serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to parse inbound file {}", file.display()))?;
            let message = MessageRecord {
                id: envelope
                    .id
                    .clone()
                    .unwrap_or_else(|| format!("local-in-{}", Uuid::new_v4())),
                chat_jid: envelope.chat_jid,
                sender: envelope.sender,
                sender_name: envelope.sender_name,
                content: envelope.content,
                timestamp: envelope
                    .timestamp
                    .unwrap_or_else(|| Utc::now().to_rfc3339()),
                is_from_me: false,
                is_bot_message: false,
            };
            let processed_path = self.root.join("processed").join(
                file.file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("message.json"),
            );
            fs::rename(&file, &processed_path).with_context(|| {
                format!(
                    "failed to move inbound file {} to {}",
                    file.display(),
                    processed_path.display()
                )
            })?;
            messages.push(message);
        }

        Ok(messages)
    }

    pub fn send_message(&self, chat_jid: &str, text: &str) -> Result<LocalOutboundEnvelope> {
        let envelope = LocalOutboundEnvelope {
            id: format!("local-out-{}", Uuid::new_v4()),
            chat_jid: chat_jid.to_string(),
            text: text.to_string(),
            timestamp: Utc::now().to_rfc3339(),
        };
        let file_path = self.outbox_path().join(format!("{}.json", envelope.id));
        fs::write(&file_path, serde_json::to_vec_pretty(&envelope)?)
            .with_context(|| format!("failed to write {}", file_path.display()))?;
        Ok(envelope)
    }

    pub fn read_outbox(&self) -> Result<Vec<LocalOutboundEnvelope>> {
        let mut files = fs::read_dir(self.outbox_path())
            .with_context(|| format!("failed to read {}", self.outbox_path().display()))?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
            .collect::<Vec<_>>();
        files.sort();

        let mut envelopes = Vec::new();
        for file in files {
            let bytes = fs::read(&file)
                .with_context(|| format!("failed to read outbox file {}", file.display()))?;
            let envelope: LocalOutboundEnvelope = serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to parse outbox file {}", file.display()))?;
            envelopes.push(envelope);
        }
        Ok(envelopes)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{LocalChannel, LocalInboundEnvelope};

    #[test]
    fn local_channel_round_trips_inbox_and_outbox() {
        let temp = tempdir().unwrap();
        let channel = LocalChannel::new(temp.path()).unwrap();
        channel
            .enqueue_inbound(LocalInboundEnvelope {
                id: Some("message-1".to_string()),
                chat_jid: "main".to_string(),
                sender: "user".to_string(),
                sender_name: Some("User".to_string()),
                content: "hello".to_string(),
                timestamp: Some("2026-04-05T12:00:00Z".to_string()),
            })
            .unwrap();

        let inbound = channel.poll_inbound().unwrap();
        assert_eq!(inbound.len(), 1);
        assert!(channel
            .send_message("main", "reply")
            .unwrap()
            .text
            .contains("reply"));
        assert_eq!(channel.read_outbox().unwrap().len(), 1);
    }
}
