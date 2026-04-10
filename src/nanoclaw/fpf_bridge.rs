use std::path::Path;

use chrono::Utc;
use uuid::Uuid;

use crate::foundation::{
    classify_boundary_text, AssuranceTuple, BoundaryClaim, BoundaryClaimSource, BoundaryQuadrant,
    CongruenceLevel, ExecutionLane, Formality, GateAspect, GateCheck, GateDecision, GateEvaluation,
    HarnessRouter, MessageRecord, ProvenanceEdge, ProvenanceEdgeKind, RequestPlane, RoleAlgebra,
    RoutingInput, ScaleClass, SwarmTask, SymbolCarrier, TaskBudget, TaskKind, TaskSignature,
};

use super::executor::ExecutionSession;

pub fn derive_task_signature(
    prompt: &str,
    _messages: &[MessageRecord],
    script: Option<&str>,
    request_plane: &RequestPlane,
    omx_requested: bool,
    created_at: &str,
) -> TaskSignature {
    let prompt_lower = prompt.to_ascii_lowercase();
    let routing = HarnessRouter::route(&RoutingInput {
        prompt: prompt.to_string(),
        request_plane: request_plane.clone(),
        has_repo: contains_any(
            &prompt_lower,
            &[
                "repo",
                "repository",
                "git ",
                "cargo",
                "npm",
                "pnpm",
                "src/",
                ".rs",
                ".ts",
                ".tsx",
                ".py",
                "workspace",
            ],
        ) || script.is_some(),
        preferred_lane: omx_requested.then_some(ExecutionLane::Omx),
    });
    let requires_repo = script.is_some()
        || contains_any(
            &prompt_lower,
            &[
                "repo",
                "repository",
                "git ",
                "cargo",
                "npm",
                "pnpm",
                "src/",
                "file ",
                "diff",
                "refactor",
                "implement",
                "fix ",
                "test ",
            ],
        );
    let requires_network = !matches!(request_plane, RequestPlane::None)
        || contains_any(
            &prompt_lower,
            &[
                "http", "https", "api", "webhook", "fetch", "browser", "slack", "linear", "github",
                "openai",
            ],
        );
    let requires_secrets = contains_any(
        &prompt_lower,
        &[
            "token",
            "secret",
            "apikey",
            "api key",
            "credential",
            "password",
            "sign in",
            "oauth",
        ],
    );
    let requires_host_control = contains_any(
        &prompt_lower,
        &[
            "open application",
            "open app",
            "activate application",
            "finder",
            "open url",
            "shell command",
            "osascript",
        ],
    );
    let mut constraints = Vec::new();
    if requires_repo {
        constraints.push("requires_repo".to_string());
    }
    if requires_network {
        constraints.push("requires_network".to_string());
    }
    if requires_secrets {
        constraints.push("requires_secrets".to_string());
    }
    if requires_host_control {
        constraints.push("requires_host_control".to_string());
    }
    if omx_requested {
        constraints.push("omx_requested".to_string());
    }
    if routing.should_load_project_context {
        constraints.push("load_project_context".to_string());
    }
    if routing.should_verify_assumptions {
        constraints.push("verify_assumptions".to_string());
    }
    if matches!(routing.scale.preferred_lane, ExecutionLane::Omx) {
        constraints.push("router_prefers_omx".to_string());
    }

    let task_kind = routing.task_kind.clone();
    let data_shape = routing.data_shape.clone();
    let budget = match (&task_kind, routing.scale.class) {
        (TaskKind::Observability, _) => TaskBudget {
            time_limit_ms: Some(2 * 60 * 1000),
            token_budget: Some(40_000),
            cost_ceiling_usd: None,
        },
        (_, ScaleClass::Logic) => TaskBudget {
            time_limit_ms: Some(60 * 1000),
            token_budget: Some(20_000),
            cost_ceiling_usd: None,
        },
        (_, ScaleClass::Tiny) => TaskBudget {
            time_limit_ms: Some(3 * 60 * 1000),
            token_budget: Some(40_000),
            cost_ceiling_usd: None,
        },
        (_, ScaleClass::Standard) => TaskBudget {
            time_limit_ms: Some(5 * 60 * 1000),
            token_budget: Some(80_000),
            cost_ceiling_usd: None,
        },
        (_, ScaleClass::Heavy) => TaskBudget {
            time_limit_ms: Some(
                if omx_requested || matches!(routing.scale.preferred_lane, ExecutionLane::Omx) {
                    15 * 60 * 1000
                } else {
                    10 * 60 * 1000
                },
            ),
            token_budget: Some(200_000),
            cost_ceiling_usd: None,
        },
    };

    TaskSignature {
        id: format!("task-signature-{}", Uuid::new_v4()),
        task_kind,
        data_shape,
        requires_repo,
        requires_network,
        requires_secrets,
        requires_host_control,
        request_plane: request_plane.clone(),
        constraints,
        budget,
        created_at: created_at.to_string(),
    }
}

pub fn build_boundary_claims(
    group_id: &str,
    task_id: Option<&str>,
    prompt: &str,
    request_plane: &RequestPlane,
    created_at: &str,
) -> Vec<BoundaryClaim> {
    let mut claims = vec![BoundaryClaim {
        id: format!("boundary-{}", Uuid::new_v4()),
        provenance_id: None,
        group_id: group_id.to_string(),
        task_id: task_id.map(ToOwned::to_owned),
        quadrant: classify_boundary_text(prompt),
        source: BoundaryClaimSource::Prompt,
        content: truncate(prompt, 400),
        witness: None,
        created_at: created_at.to_string(),
    }];
    if !matches!(request_plane, RequestPlane::None) {
        claims.push(BoundaryClaim {
            id: format!("boundary-{}", Uuid::new_v4()),
            provenance_id: None,
            group_id: group_id.to_string(),
            task_id: task_id.map(ToOwned::to_owned),
            quadrant: BoundaryQuadrant::Admissibility,
            source: BoundaryClaimSource::Policy,
            content: format!("Request plane is {}", request_plane.as_str()),
            witness: Some("execution request plane".to_string()),
            created_at: created_at.to_string(),
        });
    }
    claims
}

pub fn evaluate_execution_gate(
    signature: &TaskSignature,
    boundary_claims: &[BoundaryClaim],
    requested_lane: ExecutionLane,
    workspace_root: &Path,
    omx_requested: bool,
    created_at: &str,
) -> GateEvaluation {
    let mut checks = Vec::new();
    let mut resolved_lane = requested_lane.clone();

    if signature.requires_host_control {
        checks.push(GateCheck {
            aspect: GateAspect::Approval,
            scope: crate::foundation::GateScope::Lane,
            name: "host-control-requires-approval".to_string(),
            outcome: GateDecision::Block,
            rationale: "Host control work must use the explicit approval path instead of generic execution.".to_string(),
            evidence: Some("task signature requires_host_control".to_string()),
        });
        resolved_lane = ExecutionLane::Host;
    }

    if signature.requires_secrets
        && matches!(
            requested_lane,
            ExecutionLane::Container | ExecutionLane::RemoteWorker
        )
    {
        checks.push(GateCheck {
            aspect: GateAspect::Capability,
            scope: crate::foundation::GateScope::Lane,
            name: "secret-bearing-requests-stay-on-trusted-lanes".to_string(),
            outcome: GateDecision::Degrade,
            rationale: "Secret-bearing requests should prefer trusted host execution over container or remote lanes.".to_string(),
            evidence: Some("task signature requires_secrets".to_string()),
        });
        resolved_lane = ExecutionLane::Host;
    }

    if signature.requires_repo
        && matches!(requested_lane, ExecutionLane::Container)
        && !workspace_root.join(".git").exists()
    {
        checks.push(GateCheck {
            aspect: GateAspect::Environment,
            scope: crate::foundation::GateScope::Lane,
            name: "container-lane-needs-repo-context".to_string(),
            outcome: GateDecision::Degrade,
            rationale:
                "Repo-oriented work without a local git root should fall back to host execution."
                    .to_string(),
            evidence: Some(workspace_root.display().to_string()),
        });
        resolved_lane = ExecutionLane::Host;
    }

    if omx_requested && !matches!(requested_lane, ExecutionLane::Omx) {
        checks.push(GateCheck {
            aspect: GateAspect::Boundary,
            scope: crate::foundation::GateScope::Lane,
            name: "explicit-omx-request".to_string(),
            outcome: GateDecision::Degrade,
            rationale: "The caller explicitly requested OMX execution for this run.".to_string(),
            evidence: Some("omx execution options present".to_string()),
        });
        resolved_lane = ExecutionLane::Omx;
    }

    if boundary_claims
        .iter()
        .any(|claim| claim.quadrant == BoundaryQuadrant::Deontic)
        && matches!(
            requested_lane,
            ExecutionLane::Container | ExecutionLane::RemoteWorker
        )
    {
        checks.push(GateCheck {
            aspect: GateAspect::Boundary,
            scope: crate::foundation::GateScope::Lane,
            name: "deontic-requests-prefer-trusted-lanes".to_string(),
            outcome: GateDecision::Degrade,
            rationale:
                "Normative or approval-bearing requests should stay on trusted host or OMX lanes."
                    .to_string(),
            evidence: Some("boundary quadrant is deontic".to_string()),
        });
        resolved_lane = ExecutionLane::Host;
    }

    let decision = checks
        .iter()
        .fold(GateDecision::Pass, |acc, check| acc.join(check.outcome));

    GateEvaluation {
        id: format!("gate-{}", Uuid::new_v4()),
        requested_lane: Some(requested_lane),
        resolved_lane: Some(resolved_lane),
        decision,
        checks,
        created_at: created_at.to_string(),
    }
}

pub fn derive_assurance(
    backend_label: &str,
    signature: &TaskSignature,
    gate: Option<&GateEvaluation>,
    script_used: bool,
) -> AssuranceTuple {
    let base = match backend_label {
        "codex" => {
            if script_used {
                (
                    Formality::MachineCheckable,
                    CongruenceLevel::Validated,
                    0.82,
                )
            } else {
                (Formality::Structured, CongruenceLevel::Plausible, 0.64)
            }
        }
        "omx" => (Formality::Structured, CongruenceLevel::Validated, 0.7),
        "summary" => (Formality::Informal, CongruenceLevel::WeakGuess, 0.28),
        "claude" => (Formality::Structured, CongruenceLevel::Plausible, 0.58),
        _ => (Formality::Structured, CongruenceLevel::Plausible, 0.5),
    };
    let mut reliability = base.2;
    if signature.requires_host_control {
        reliability -= 0.08;
    }
    if gate
        .map(|evaluation| evaluation.decision == GateDecision::Degrade)
        .unwrap_or(false)
    {
        reliability -= 0.05;
    }
    if gate
        .map(|evaluation| evaluation.decision == GateDecision::Block)
        .unwrap_or(false)
    {
        reliability -= 0.25;
    }
    AssuranceTuple::new(
        base.0,
        base.1,
        reliability,
        format!(
            "Derived for backend {} handling {} work.",
            backend_label,
            signature.task_kind.as_str()
        ),
    )
}

pub fn derive_symbol_carriers(
    invocation_id: &str,
    session: &ExecutionSession,
    log_path: Option<&Path>,
) -> Vec<SymbolCarrier> {
    let mut carriers = vec![SymbolCarrier {
        id: format!("carrier:{}:workspace", invocation_id),
        kind: "workspace_root".to_string(),
        location: Some(session.workspace_root.clone()),
        checksum: None,
        source: "nanoclaw".to_string(),
        version: None,
    }];
    if let Some(log_path) = log_path {
        carriers.push(SymbolCarrier {
            id: format!("carrier:{}:log", invocation_id),
            kind: "execution_log".to_string(),
            location: Some(log_path.display().to_string()),
            checksum: None,
            source: "nanoclaw".to_string(),
            version: None,
        });
    }
    carriers
}

pub fn derive_provenance_edges(
    invocation_id: &str,
    carriers: &[SymbolCarrier],
    boundary_claims: &[BoundaryClaim],
) -> Vec<ProvenanceEdge> {
    let mut edges = Vec::new();
    for claim in boundary_claims {
        for carrier in carriers {
            edges.push(ProvenanceEdge {
                from: carrier.id.clone(),
                to: claim.id.clone(),
                kind: ProvenanceEdgeKind::UsedCarrier,
            });
        }
    }
    if carriers.len() > 1 {
        edges.push(ProvenanceEdge {
            from: carriers[0].id.clone(),
            to: carriers[1].id.clone(),
            kind: ProvenanceEdgeKind::HappenedBefore,
        });
    }
    edges.push(ProvenanceEdge {
        from: format!("run:{invocation_id}"),
        to: format!("artifact:{invocation_id}:summary"),
        kind: ProvenanceEdgeKind::DerivedFrom,
    });
    edges
}

pub fn evaluate_swarm_role_requirements(
    task: &SwarmTask,
    role_algebra: Option<&RoleAlgebra>,
    created_at: &str,
) -> Option<GateEvaluation> {
    let mut seen = std::collections::HashSet::new();
    for role in &task.required_roles {
        if !seen.insert(role.clone()) {
            return Some(GateEvaluation {
                id: format!("gate-{}", Uuid::new_v4()),
                requested_lane: None,
                resolved_lane: None,
                decision: GateDecision::Block,
                checks: vec![GateCheck {
                    aspect: GateAspect::Approval,
                    scope: crate::foundation::GateScope::Profile,
                    name: "duplicate-required-role".to_string(),
                    outcome: GateDecision::Block,
                    rationale: format!("Required role '{}' appears more than once.", role),
                    evidence: None,
                }],
                created_at: created_at.to_string(),
            });
        }
    }

    let role_algebra = role_algebra?;
    for (index, role) in task.required_roles.iter().enumerate() {
        for other in task.required_roles.iter().skip(index + 1) {
            if role_algebra.is_incompatible(role, other) {
                return Some(GateEvaluation {
                    id: format!("gate-{}", Uuid::new_v4()),
                    requested_lane: None,
                    resolved_lane: None,
                    decision: GateDecision::Block,
                    checks: vec![GateCheck {
                        aspect: GateAspect::Approval,
                        scope: crate::foundation::GateScope::Profile,
                        name: "role-incompatibility".to_string(),
                        outcome: GateDecision::Block,
                        rationale: format!(
                            "Required roles '{}' and '{}' are incompatible in this swarm context.",
                            role, other
                        ),
                        evidence: Some(role_algebra.context_id.clone()),
                    }],
                    created_at: created_at.to_string(),
                });
            }
        }
    }
    None
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (index, ch) in value.chars().enumerate() {
        if index >= max_chars {
            out.push('…');
            break;
        }
        out.push(ch);
    }
    out
}

pub fn now_iso() -> String {
    Utc::now().to_rfc3339()
}
