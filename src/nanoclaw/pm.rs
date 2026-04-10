use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PmAuditEvent {
    pub issue_key: Option<String>,
    pub issue_identifier: Option<String>,
    pub thread_jid: Option<String>,
    pub phase: String,
    pub status: String,
    pub tool: Option<String>,
    pub error_code: Option<String>,
    pub blocking: bool,
    pub metadata: Option<Value>,
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredPmAuditEvent {
    pub id: i64,
    pub issue_key: Option<String>,
    pub issue_identifier: Option<String>,
    pub thread_jid: Option<String>,
    pub phase: String,
    pub status: String,
    pub tool: Option<String>,
    pub error_code: Option<String>,
    pub blocking: bool,
    pub metadata: Option<Value>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PmIssueMemory {
    pub issue_key: String,
    pub issue_identifier: String,
    pub summary: Option<String>,
    pub next_action: Option<String>,
    pub blockers: Option<Vec<String>>,
    pub current_state: Option<String>,
    pub repo_hint: Option<String>,
    pub last_source: Option<String>,
    pub memory: Option<Value>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PmIssueTrackedComment {
    pub issue_key: String,
    pub issue_identifier: Option<String>,
    pub comment_kind: String,
    pub comment_id: String,
    pub body_hash: Option<String>,
    pub body: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinearIssueThread {
    pub chat_jid: String,
    pub thread_ts: String,
    pub issue_identifier: Option<String>,
    pub closed_at: Option<String>,
}

pub fn build_pm_issue_key(issue_id: &str) -> String {
    format!("linear:{issue_id}")
}
