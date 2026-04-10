use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RoleAlgebra {
    pub context_id: String,
    pub specializations: HashSet<(String, String)>,
    pub incompatibilities: HashSet<(String, String)>,
    pub bundles: HashMap<String, HashSet<String>>,
}

impl RoleAlgebra {
    pub fn new(context_id: impl Into<String>) -> Self {
        Self {
            context_id: context_id.into(),
            ..Default::default()
        }
    }

    pub fn add_specialization(&mut self, child: impl Into<String>, parent: impl Into<String>) {
        self.specializations.insert((child.into(), parent.into()));
    }

    pub fn add_incompatibility(&mut self, a: impl Into<String>, b: impl Into<String>) {
        let a = a.into();
        let b = b.into();
        self.incompatibilities.insert((a.clone(), b.clone()));
        self.incompatibilities.insert((b, a));
    }

    pub fn add_bundle(&mut self, name: impl Into<String>, roles: impl IntoIterator<Item = String>) {
        self.bundles
            .insert(name.into(), roles.into_iter().collect());
    }

    pub fn is_incompatible(&self, a: &str, b: &str) -> bool {
        self.incompatibilities
            .contains(&(a.to_string(), b.to_string()))
    }
}
