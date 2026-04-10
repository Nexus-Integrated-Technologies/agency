use serde::{Deserialize, Serialize};

use super::domain::ExecutionLane;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GateAspect {
    Capability,
    RequestPlane,
    Boundary,
    Risk,
    Approval,
    Environment,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GateScope {
    Lane,
    Locus,
    Subflow,
    Profile,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum GateDecision {
    Pass,
    Degrade,
    Block,
}

impl GateDecision {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Pass => "pass",
            Self::Degrade => "degrade",
            Self::Block => "block",
        }
    }

    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "pass" => Self::Pass,
            "degrade" => Self::Degrade,
            "block" => Self::Block,
            _ => Self::Pass,
        }
    }

    pub fn join(self, other: Self) -> Self {
        if self > other {
            self
        } else {
            other
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GateCheck {
    pub aspect: GateAspect,
    pub scope: GateScope,
    pub name: String,
    pub outcome: GateDecision,
    pub rationale: String,
    pub evidence: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GateEvaluation {
    pub id: String,
    pub requested_lane: Option<ExecutionLane>,
    pub resolved_lane: Option<ExecutionLane>,
    pub decision: GateDecision,
    pub checks: Vec<GateCheck>,
    pub created_at: String,
}
