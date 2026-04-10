use serde::{Deserialize, Serialize};

use super::domain::{GroupId, TaskId};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryQuadrant {
    Law,
    Admissibility,
    Deontic,
    Evidence,
}

impl BoundaryQuadrant {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "law" => Self::Law,
            "admissibility" | "gate" => Self::Admissibility,
            "deontic" | "duty" => Self::Deontic,
            "evidence" | "fact" => Self::Evidence,
            _ => Self::Law,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Law => "law",
            Self::Admissibility => "admissibility",
            Self::Deontic => "deontic",
            Self::Evidence => "evidence",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryClaimSource {
    Prompt,
    Policy,
    Approval,
    Result,
    Custom(String),
}

impl BoundaryClaimSource {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Prompt => "prompt",
            Self::Policy => "policy",
            Self::Approval => "approval",
            Self::Result => "result",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoundaryClaim {
    pub id: String,
    pub provenance_id: Option<String>,
    pub group_id: GroupId,
    pub task_id: Option<TaskId>,
    pub quadrant: BoundaryQuadrant,
    pub source: BoundaryClaimSource,
    pub content: String,
    pub witness: Option<String>,
    pub created_at: String,
}

pub fn classify_boundary_text(text: &str) -> BoundaryQuadrant {
    let normalized = text.trim().to_ascii_lowercase();
    if normalized.contains("must")
        || normalized.contains("shall")
        || normalized.contains("approve")
        || normalized.contains("deny")
        || normalized.contains("owed")
    {
        BoundaryQuadrant::Deontic
    } else if normalized.contains("allow")
        || normalized.contains("block")
        || normalized.contains("forbid")
        || normalized.contains("require")
        || normalized.contains("permission")
        || normalized.contains("gate")
    {
        BoundaryQuadrant::Admissibility
    } else if normalized.contains("observed")
        || normalized.contains("result")
        || normalized.contains("evidence")
        || normalized.contains("output")
        || normalized.contains("measured")
        || normalized.contains("verified")
    {
        BoundaryQuadrant::Evidence
    } else {
        BoundaryQuadrant::Law
    }
}
