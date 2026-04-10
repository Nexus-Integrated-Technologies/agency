use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RefreshTrigger {
    PolicyChange,
    IlluminationIncrease,
    EditionBump,
    BridgeChange,
    FreshnessExpiry,
    MaturityChange,
    DominancePolicyChange,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RefreshAction {
    RecomputeSelection,
    UpdateArchive,
    RebindBridge,
    RepublishBundle,
    RebuildPortfolioSurface,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RefreshPlan {
    pub id: String,
    pub path_slice_ids: Vec<String>,
    pub triggers: Vec<RefreshTrigger>,
    pub actions: Vec<RefreshAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RefreshReport {
    pub plan_id: String,
    pub path_ids: Vec<String>,
    pub deltas: Vec<String>,
}
