use serde::{Deserialize, Serialize};

use super::domain::RequestPlane;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Coding,
    Planning,
    HostControl,
    Observability,
    Messaging,
    Pm,
    Research,
    Custom(String),
}

impl TaskKind {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "coding" => Self::Coding,
            "planning" => Self::Planning,
            "host_control" | "host-control" => Self::HostControl,
            "observability" => Self::Observability,
            "messaging" => Self::Messaging,
            "pm" => Self::Pm,
            "research" => Self::Research,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Coding => "coding",
            Self::Planning => "planning",
            Self::HostControl => "host_control",
            Self::Observability => "observability",
            Self::Messaging => "messaging",
            Self::Pm => "pm",
            Self::Research => "research",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskDataShape {
    Text,
    Codebase,
    Command,
    Web,
    Structured,
    Mixed,
    Unknown,
}

impl TaskDataShape {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "text" => Self::Text,
            "codebase" => Self::Codebase,
            "command" => Self::Command,
            "web" => Self::Web,
            "structured" => Self::Structured,
            "mixed" => Self::Mixed,
            "" | "unknown" => Self::Unknown,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Text => "text",
            Self::Codebase => "codebase",
            Self::Command => "command",
            Self::Web => "web",
            Self::Structured => "structured",
            Self::Mixed => "mixed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TaskBudget {
    pub time_limit_ms: Option<u64>,
    pub token_budget: Option<u64>,
    pub cost_ceiling_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskSignature {
    pub id: String,
    pub task_kind: TaskKind,
    pub data_shape: TaskDataShape,
    pub requires_repo: bool,
    pub requires_network: bool,
    pub requires_secrets: bool,
    pub requires_host_control: bool,
    pub request_plane: RequestPlane,
    pub constraints: Vec<String>,
    pub budget: TaskBudget,
    pub created_at: String,
}
