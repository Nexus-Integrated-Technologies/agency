use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SymbolCarrier {
    pub id: String,
    pub kind: String,
    pub location: Option<String>,
    pub checksum: Option<String>,
    pub source: String,
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProvenanceEdgeKind {
    DerivedFrom,
    UsedCarrier,
    MeasuredBy,
    InterpretedBy,
    HappenedBefore,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProvenanceEdge {
    pub from: String,
    pub to: String,
    pub kind: ProvenanceEdgeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvidenceGraph {
    pub path_id: String,
    pub evidence_map: BTreeMap<String, String>,
}

impl Default for EvidenceGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl EvidenceGraph {
    pub fn new() -> Self {
        Self {
            path_id: Uuid::new_v4().to_string(),
            evidence_map: BTreeMap::new(),
        }
    }

    pub fn record_evidence(&mut self, claim: impl Into<String>, carrier: impl Into<String>) {
        self.evidence_map.insert(claim.into(), carrier.into());
    }

    pub fn format_for_audit(&self) -> String {
        let mut output = format!("EVIDENCE GRAPH (Path: {})\n", self.path_id);
        if self.evidence_map.is_empty() {
            output.push_str("  - No physical evidence carriers recorded.");
        } else {
            for (claim, carrier) in &self.evidence_map {
                output.push_str(&format!(
                    "  - Claim: '{}' -> Carrier: '{}'\n",
                    claim, carrier
                ));
            }
        }
        output
    }
}
