use serde::{Deserialize, Serialize};

use super::{ExecutionLane, RequestPlane, TaskDataShape, TaskKind};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ScaleElasticity {
    Flat,
    Rising,
    Knee,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScaleClass {
    Logic,
    Tiny,
    Standard,
    Heavy,
}

impl ScaleClass {
    pub fn escalate(self) -> Self {
        match self {
            Self::Logic => Self::Tiny,
            Self::Tiny => Self::Standard,
            Self::Standard => Self::Heavy,
            Self::Heavy => Self::Heavy,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct RouteConfidence(pub f32);

impl RouteConfidence {
    pub fn new(value: f32) -> Self {
        Self(value.clamp(0.0, 1.0))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScaleProfile {
    pub class: ScaleClass,
    pub predicted_complexity: f32,
    pub elasticity: ScaleElasticity,
    pub preferred_lane: ExecutionLane,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoutingInput {
    pub prompt: String,
    pub request_plane: RequestPlane,
    pub has_repo: bool,
    pub preferred_lane: Option<ExecutionLane>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoutingDecision {
    pub task_kind: TaskKind,
    pub data_shape: TaskDataShape,
    pub candidate_lanes: Vec<ExecutionLane>,
    pub should_load_project_context: bool,
    pub should_verify_assumptions: bool,
    pub confidence: RouteConfidence,
    pub reason: String,
    pub scale: ScaleProfile,
}

pub struct HarnessRouter;

impl HarnessRouter {
    pub fn route(input: &RoutingInput) -> RoutingDecision {
        let prompt = input.prompt.trim();
        let lower = prompt.to_ascii_lowercase();

        let is_host_control = contains_any(
            &lower,
            &[
                "systemctl",
                "launchctl",
                "brew ",
                "apt ",
                "ssh ",
                "tmux",
                "kill ",
                "chmod ",
            ],
        );
        let is_observability = contains_any(
            &lower,
            &[
                "logs",
                "metrics",
                "incident",
                "runbook",
                "trace",
                "observability",
                "alert",
            ],
        );
        let is_planning = contains_any(
            &lower,
            &[
                "plan",
                "roadmap",
                "break down",
                "steps",
                "architecture",
                "design",
                "hiring plan",
            ],
        );
        let is_research = lower.contains("http://")
            || lower.contains("https://")
            || contains_any(
                &lower,
                &["latest", "docs", "research", "investigate", "compare"],
            );
        let wants_parallel_coding =
            contains_any(&lower, &["parallel", "multi-agent", "swarm", "omx"]);
        let is_coding = input.has_repo
            || contains_any(
                &lower,
                &[
                    "code", "repo", "refactor", "fix", "bug", "tests", "rust", "cargo", "git",
                    "pr ", "feature", ".rs", ".ts", ".tsx", ".py",
                ],
            );

        let task_kind = if is_host_control {
            TaskKind::HostControl
        } else if is_observability {
            TaskKind::Observability
        } else if is_coding {
            TaskKind::Coding
        } else if is_planning {
            TaskKind::Planning
        } else if is_research {
            TaskKind::Research
        } else {
            TaskKind::Messaging
        };

        let data_shape = if is_host_control {
            TaskDataShape::Command
        } else if is_coding {
            TaskDataShape::Codebase
        } else if is_research {
            TaskDataShape::Web
        } else if prompt.contains('{') || prompt.contains('[') {
            TaskDataShape::Structured
        } else {
            TaskDataShape::Text
        };

        let complexity = estimate_complexity(
            &lower,
            is_coding,
            is_planning,
            is_research,
            wants_parallel_coding,
        );
        let mut scale = scale_profile(task_kind.clone(), complexity, input.preferred_lane.clone());
        if task_kind == TaskKind::Coding
            && wants_parallel_coding
            && scale.class >= ScaleClass::Standard
        {
            scale.preferred_lane = ExecutionLane::Omx;
        }

        let mut candidate_lanes = Vec::new();
        push_lane(&mut candidate_lanes, input.preferred_lane.clone());
        match task_kind {
            TaskKind::HostControl | TaskKind::Observability => {
                push_lane(&mut candidate_lanes, Some(ExecutionLane::Host));
            }
            TaskKind::Coding => {
                if scale.class >= ScaleClass::Heavy
                    || (scale.class >= ScaleClass::Standard && wants_parallel_coding)
                {
                    push_lane(&mut candidate_lanes, Some(ExecutionLane::Omx));
                }
                push_lane(&mut candidate_lanes, Some(ExecutionLane::Host));
                push_lane(&mut candidate_lanes, Some(ExecutionLane::Container));
            }
            TaskKind::Planning => {
                if scale.class >= ScaleClass::Standard {
                    push_lane(&mut candidate_lanes, Some(ExecutionLane::Omx));
                }
                push_lane(&mut candidate_lanes, Some(ExecutionLane::Container));
            }
            TaskKind::Research => {
                push_lane(&mut candidate_lanes, Some(ExecutionLane::Container));
                push_lane(&mut candidate_lanes, Some(ExecutionLane::Host));
            }
            _ => {
                push_lane(&mut candidate_lanes, Some(ExecutionLane::Container));
            }
        }

        if candidate_lanes.is_empty() {
            candidate_lanes.push(ExecutionLane::Auto);
        }

        let should_load_project_context = input.has_repo || is_coding || is_planning;
        let should_verify_assumptions = is_research || is_planning || complexity > 0.6;
        let confidence = if is_host_control || is_observability || is_coding || is_planning {
            RouteConfidence::new(0.85)
        } else {
            RouteConfidence::new(0.65)
        };
        let reason = format!(
            "task_kind={} data_shape={} complexity={:.2}",
            task_kind.as_str(),
            data_shape.as_str(),
            complexity
        );

        RoutingDecision {
            task_kind,
            data_shape,
            candidate_lanes,
            should_load_project_context,
            should_verify_assumptions,
            confidence,
            reason,
            scale,
        }
    }
}

fn estimate_complexity(
    prompt: &str,
    is_coding: bool,
    is_planning: bool,
    is_research: bool,
    wants_parallel_coding: bool,
) -> f32 {
    let mut complexity: f32 = 0.1;
    if prompt.len() > 120 {
        complexity += 0.2;
    }
    if prompt.len() > 320 {
        complexity += 0.2;
    }
    if is_coding {
        complexity += 0.25;
    }
    if is_planning {
        complexity += 0.2;
    }
    if is_research {
        complexity += 0.15;
    }
    if wants_parallel_coding {
        complexity += 0.2;
    }
    complexity.clamp(0.0, 1.0)
}

fn scale_profile(
    task_kind: TaskKind,
    complexity: f32,
    preferred_lane: Option<ExecutionLane>,
) -> ScaleProfile {
    let (class, elasticity) = if complexity < 0.15 {
        (ScaleClass::Logic, ScaleElasticity::Flat)
    } else if complexity < 0.35 {
        (ScaleClass::Tiny, ScaleElasticity::Flat)
    } else if complexity < 0.7 {
        (ScaleClass::Standard, ScaleElasticity::Rising)
    } else {
        (ScaleClass::Heavy, ScaleElasticity::Knee)
    };

    let preferred_lane = preferred_lane.unwrap_or_else(|| match task_kind {
        TaskKind::HostControl | TaskKind::Observability => ExecutionLane::Host,
        TaskKind::Coding if class >= ScaleClass::Heavy => ExecutionLane::Omx,
        TaskKind::Coding => ExecutionLane::Host,
        TaskKind::Planning if class >= ScaleClass::Standard => ExecutionLane::Omx,
        TaskKind::Planning => ExecutionLane::Container,
        TaskKind::Research => ExecutionLane::Container,
        _ => ExecutionLane::Container,
    });

    ScaleProfile {
        class,
        predicted_complexity: complexity,
        elasticity,
        preferred_lane,
    }
}

fn push_lane(target: &mut Vec<ExecutionLane>, lane: Option<ExecutionLane>) {
    if let Some(lane) = lane {
        if !target.iter().any(|existing| existing == &lane) {
            target.push(lane);
        }
    }
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::{HarnessRouter, RoutingInput, ScaleClass};
    use crate::foundation::{ExecutionLane, RequestPlane, TaskDataShape, TaskKind};

    #[test]
    fn routes_heavy_coding_to_omx_first() {
        let decision = HarnessRouter::route(&RoutingInput {
            prompt: "refactor the rust repo, split modules, run tests, and coordinate a multi-agent codex session".to_string(),
            request_plane: RequestPlane::None,
            has_repo: true,
            preferred_lane: None,
        });

        assert_eq!(decision.task_kind, TaskKind::Coding);
        assert_eq!(decision.data_shape, TaskDataShape::Codebase);
        assert_eq!(decision.candidate_lanes[0], ExecutionLane::Omx);
        assert!(decision.scale.class >= ScaleClass::Standard);
    }

    #[test]
    fn routes_host_commands_to_host_lane() {
        let decision = HarnessRouter::route(&RoutingInput {
            prompt: "check systemctl logs and restart the service".to_string(),
            request_plane: RequestPlane::None,
            has_repo: false,
            preferred_lane: None,
        });

        assert_eq!(decision.task_kind, TaskKind::HostControl);
        assert_eq!(decision.candidate_lanes[0], ExecutionLane::Host);
    }
}
