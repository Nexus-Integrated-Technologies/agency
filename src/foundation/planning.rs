use serde::{Deserialize, Serialize};

use super::{ExecutionLane, TaskKind};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanStepStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanStep {
    pub step_num: usize,
    pub description: String,
    pub task_kind: TaskKind,
    pub preferred_lane: Option<ExecutionLane>,
    pub suggested_tools: Vec<String>,
    pub expected_output: String,
    pub depends_on: Vec<usize>,
    pub status: PlanStepStatus,
    pub output: Option<String>,
}

impl PlanStep {
    pub fn is_ready(&self, plan: &Plan) -> bool {
        self.status == PlanStepStatus::Pending
            && self.depends_on.iter().all(|dependency| {
                plan.steps.iter().any(|step| {
                    step.step_num == *dependency && step.status == PlanStepStatus::Completed
                })
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Plan {
    pub goal: String,
    pub steps: Vec<PlanStep>,
    pub current_step: usize,
    pub is_complete: bool,
}

impl Plan {
    pub fn new(goal: impl Into<String>) -> Self {
        Self {
            goal: goal.into(),
            steps: Vec::new(),
            current_step: 0,
            is_complete: false,
        }
    }

    pub fn push_step(&mut self, step: PlanStep) {
        self.steps.push(step);
        self.current_step = self
            .steps
            .iter()
            .position(|step| step.status != PlanStepStatus::Completed)
            .unwrap_or(self.steps.len());
        self.is_complete = self
            .steps
            .iter()
            .all(|step| step.status == PlanStepStatus::Completed);
    }

    pub fn ready_steps(&self) -> Vec<&PlanStep> {
        self.steps
            .iter()
            .filter(|step| step.is_ready(self))
            .collect()
    }

    pub fn start_step(&mut self, step_num: usize) {
        if let Some(step) = self.steps.iter_mut().find(|step| step.step_num == step_num) {
            step.status = PlanStepStatus::Running;
        }
    }

    pub fn complete_step(&mut self, step_num: usize, output: impl Into<String>) {
        if let Some(step) = self.steps.iter_mut().find(|step| step.step_num == step_num) {
            step.status = PlanStepStatus::Completed;
            step.output = Some(output.into());
        }
        self.refresh_status();
    }

    pub fn fail_step(&mut self, step_num: usize, output: impl Into<String>) {
        if let Some(step) = self.steps.iter_mut().find(|step| step.step_num == step_num) {
            step.status = PlanStepStatus::Failed;
            step.output = Some(output.into());
        }
        self.refresh_status();
    }

    pub fn progress(&self) -> f32 {
        if self.steps.is_empty() {
            return 0.0;
        }
        let completed = self
            .steps
            .iter()
            .filter(|step| step.status == PlanStepStatus::Completed)
            .count();
        (completed as f32 / self.steps.len() as f32) * 100.0
    }

    pub fn summary(&self) -> String {
        let summary = self
            .steps
            .iter()
            .map(|step| {
                let marker = match step.status {
                    PlanStepStatus::Pending => " ",
                    PlanStepStatus::Running => "~",
                    PlanStepStatus::Completed => "x",
                    PlanStepStatus::Failed => "!",
                };
                format!(
                    "[{}] {}. {} ({})",
                    marker,
                    step.step_num,
                    step.description,
                    step.task_kind.as_str()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        format!(
            "Goal: {}\nProgress: {:.0}%\nSteps:\n{}",
            self.goal,
            self.progress(),
            summary
        )
    }

    fn refresh_status(&mut self) {
        self.current_step = self
            .steps
            .iter()
            .position(|step| step.status != PlanStepStatus::Completed)
            .unwrap_or(self.steps.len());
        self.is_complete = !self.steps.is_empty()
            && self
                .steps
                .iter()
                .all(|step| step.status == PlanStepStatus::Completed);
    }
}

#[cfg(test)]
mod tests {
    use super::{Plan, PlanStep, PlanStepStatus};
    use crate::foundation::{ExecutionLane, TaskKind};

    #[test]
    fn plan_tracks_dependency_ready_steps() {
        let mut plan = Plan::new("ship release");
        plan.push_step(PlanStep {
            step_num: 1,
            description: "inspect repo".to_string(),
            task_kind: TaskKind::Coding,
            preferred_lane: Some(ExecutionLane::Host),
            suggested_tools: vec!["rg".to_string()],
            expected_output: "repo notes".to_string(),
            depends_on: Vec::new(),
            status: PlanStepStatus::Pending,
            output: None,
        });
        plan.push_step(PlanStep {
            step_num: 2,
            description: "implement fix".to_string(),
            task_kind: TaskKind::Coding,
            preferred_lane: Some(ExecutionLane::Omx),
            suggested_tools: vec!["codex".to_string()],
            expected_output: "patch".to_string(),
            depends_on: vec![1],
            status: PlanStepStatus::Pending,
            output: None,
        });

        assert_eq!(plan.ready_steps().len(), 1);
        plan.complete_step(1, "repo inspected");
        assert_eq!(plan.ready_steps().len(), 1);
        assert_eq!(plan.ready_steps()[0].step_num, 2);
    }
}
