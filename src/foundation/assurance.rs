use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Formality {
    Informal,
    Structured,
    Formalizable,
    MachineCheckable,
    VerifiedSpecification,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum CongruenceLevel {
    WeakGuess,
    Plausible,
    Validated,
    Verified,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AssuranceTuple {
    pub formality: Formality,
    pub congruence: CongruenceLevel,
    pub reliability: f64,
    pub notes: String,
}

impl AssuranceTuple {
    pub fn new(
        formality: Formality,
        congruence: CongruenceLevel,
        reliability: f64,
        notes: impl Into<String>,
    ) -> Self {
        Self {
            formality,
            congruence,
            reliability: reliability.clamp(0.0, 1.0),
            notes: notes.into(),
        }
    }
}
