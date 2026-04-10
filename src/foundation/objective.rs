use serde::{Deserialize, Serialize};

use super::{ServiceClause, TaskBudget};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Objective {
    pub goal: String,
    pub service_clause: ServiceClause,
    pub resource_budget: ResourceBudget,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourceBudget {
    pub max_cycles: usize,
    pub max_tokens: Option<u32>,
    pub max_time_seconds: u64,
    pub cost_ceiling_usd: Option<f64>,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            max_cycles: 5,
            max_tokens: None,
            max_time_seconds: 300,
            cost_ceiling_usd: None,
        }
    }
}

impl ResourceBudget {
    pub fn as_task_budget(&self) -> TaskBudget {
        TaskBudget {
            time_limit_ms: Some(self.max_time_seconds.saturating_mul(1000)),
            token_budget: self.max_tokens.map(u64::from),
            cost_ceiling_usd: self.cost_ceiling_usd,
        }
    }
}

impl From<TaskBudget> for ResourceBudget {
    fn from(value: TaskBudget) -> Self {
        Self {
            max_cycles: 1,
            max_tokens: value
                .token_budget
                .map(|tokens| tokens.min(u64::from(u32::MAX)) as u32),
            max_time_seconds: value.time_limit_ms.unwrap_or(300_000) / 1000,
            cost_ceiling_usd: value.cost_ceiling_usd,
        }
    }
}

impl Objective {
    pub fn new(goal: impl Into<String>) -> Self {
        let goal = goal.into();
        Self {
            goal: goal.clone(),
            service_clause: ServiceClause::new(goal.clone(), "NanoClaw", "User"),
            resource_budget: ResourceBudget::default(),
        }
    }

    pub fn with_acceptance(mut self, criteria: impl Into<String>) -> Self {
        self.service_clause = self.service_clause.with_acceptance(criteria);
        self
    }

    pub fn with_budget(mut self, budget: ResourceBudget) -> Self {
        self.resource_budget = budget;
        self
    }

    pub fn format_for_prompt(&self) -> String {
        let mut output = format!("## OBJECTIVE\nGOAL: {}\n", self.goal);
        if !self.service_clause.acceptance_spec.is_empty() {
            output.push_str("ACCEPTANCE CRITERIA:\n");
            for (index, criteria) in self.service_clause.acceptance_spec.iter().enumerate() {
                output.push_str(&format!("  {}. {}\n", index + 1, criteria));
            }
        }
        output.push_str(&format!(
            "RESOURCE BUDGET: max_cycles={} max_time={}s max_tokens={}\n",
            self.resource_budget.max_cycles,
            self.resource_budget.max_time_seconds,
            self.resource_budget
                .max_tokens
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unbounded".to_string())
        ));
        output
    }
}

#[cfg(test)]
mod tests {
    use super::{Objective, ResourceBudget};

    #[test]
    fn objective_formats_acceptance_criteria() {
        let prompt = Objective::new("ship the fix")
            .with_acceptance("all tests pass")
            .with_budget(ResourceBudget {
                max_cycles: 3,
                max_tokens: Some(1000),
                max_time_seconds: 120,
                cost_ceiling_usd: Some(5.0),
            })
            .format_for_prompt();

        assert!(prompt.contains("ship the fix"));
        assert!(prompt.contains("all tests pass"));
        assert!(prompt.contains("max_cycles=3"));
    }
}
