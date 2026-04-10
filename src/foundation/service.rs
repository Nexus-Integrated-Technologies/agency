use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceClause {
    pub name: String,
    pub provider_role: String,
    pub consumer_role: String,
    pub acceptance_spec: Vec<String>,
    pub status: ServiceStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServiceStatus {
    Proposed,
    Active,
    Fulfilled,
    Breached,
}

impl ServiceClause {
    pub fn new(
        name: impl Into<String>,
        provider: impl Into<String>,
        consumer: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            provider_role: provider.into(),
            consumer_role: consumer.into(),
            acceptance_spec: Vec::new(),
            status: ServiceStatus::Proposed,
        }
    }

    pub fn with_acceptance(mut self, criteria: impl Into<String>) -> Self {
        self.acceptance_spec.push(criteria.into());
        self
    }

    pub fn format_for_audit(&self) -> String {
        format!(
            "SERVICE CLAUSE: {}\nPROVIDER: {} -> CONSUMER: {}\nSTATUS: {:?}\nCRITERIA:\n  - {}",
            self.name,
            self.provider_role,
            self.consumer_role,
            self.status,
            if self.acceptance_spec.is_empty() {
                "none".to_string()
            } else {
                self.acceptance_spec.join("\n  - ")
            }
        )
    }
}
