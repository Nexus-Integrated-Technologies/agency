use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ScopeSlice {
    pub dimension: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ScopeSet {
    pub id: String,
    pub slices: HashSet<ScopeSlice>,
}

impl ScopeSet {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            slices: HashSet::new(),
        }
    }

    pub fn with_slice(mut self, dimension: impl Into<String>, value: impl Into<String>) -> Self {
        self.slices.insert(ScopeSlice {
            dimension: dimension.into(),
            value: value.into(),
        });
        self
    }

    pub fn covers(&self, other: &ScopeSet) -> bool {
        other.slices.is_subset(&self.slices)
    }
}
