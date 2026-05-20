use rust_agency::fpf::bridge::{AlignmentBridge, BridgeRelation, BridgeCAL};
use rust_agency::fpf::cg_spec::{CGSpec, CGScope, DescribedEntity, ReferencePlane, ScaleComplianceProfile};
use rust_agency::fpf::ee_log::{EELOG, BitterLessonPreference};
use rust_agency::fpf::ethics::{EthicsCAL, EthicalDuty, EthicalScale};
use rust_agency::fpf::mvpk::{MVPK, FaceKind, PublicationScope};
use rust_agency::fpf::uts::{UTS, ConceptSet, SenseCell, SenseFamily};
use rust_agency::fpf::assurance::{CongruenceLevel};
use rust_agency::fpf::mm_chr::{ScaleType, Polarity};
use rust_agency::fpf::q_bundle::{Scope};
use rust_agency::fpf::sos_log::{SoSLOG, MethodFamily, MaturityCard, MaturityRung};
use rust_agency::fpf::task_signature::{TaskSignature, DataShape, NoiseModel, ObjectiveProfile, SizeScale, Missingness, DominanceRegime, PortfolioMode, Budgeting};
use rust_agency::fpf::commitment::{Commitment, Modality, CommitmentStatus};
use rust_agency::fpf::service::{ServiceClause, ServiceSituation, AccessSpec, AcceptanceSpec};
use rust_agency::orchestrator::{EvolutionEvent};
use rust_agency::fpf::role::Window;
use rust_agency::fpf::capability::{Capability, WorkScope, WorkMeasures};
use rust_agency::fpf::plan::{WorkPlan, PlanItem};
use rust_agency::fpf::transition::{PromotionRecord, MHTEventType, IdentityStance, PreConfig, PostHolon, BOSCTriggers};
use std::collections::{HashMap, HashSet};

#[test]
fn test_bridge_composition() {
    let b1 = AlignmentBridge {
        id: "b1".into(),
        left_cell: "c1".into(),
        right_cell: "c2".into(),
        relation: BridgeRelation::NearEquivalent,
        cl: CongruenceLevel::CL2Validated,
        loss_notes: "loss1".into(),
        fit_notes: "fit1".into(),
    };
    let b2 = AlignmentBridge {
        id: "b2".into(),
        left_cell: "c2".into(),
        right_cell: "c3".into(),
        relation: BridgeRelation::NearEquivalent,
        cl: CongruenceLevel::CL1Plausible,
        loss_notes: "loss2".into(),
        fit_notes: "fit2".into(),
    };

    let composed = BridgeCAL::compose(&b1, &b2).unwrap();
    assert_eq!(composed.cl, CongruenceLevel::CL1Plausible);
    assert_eq!(composed.left_cell, "c1");
    assert_eq!(composed.right_cell, "c3");
}

#[test]
fn test_cg_spec_legality() {
    let mut scp = HashMap::new();
    scp.insert("safety".to_string(), ScaleComplianceProfile {
        scale_types: vec![ScaleType::Ordinal],
        polarity: Polarity::Positive,
        unit_alignment_rules: vec![],
        guard_macros: vec![],
    });

    let spec = CGSpec {
        uts_id: "uts1".into(),
        edition: "v1".into(),
        context_id: "ctx1".into(),
        purpose: "test".into(),
        scope: CGScope { slice_id: "s1".into(), task_kinds: vec![], object_kinds: vec![] },
        described_entity: DescribedEntity { grounding_holon_id: "h1".into(), reference_plane: ReferencePlane::World },
        comparator_set: vec![],
        characteristics: vec!["safety".into()],
        scp,
        minimal_evidence: HashMap::new(),
        gamma_fold: rust_agency::fpf::cg_spec::GammaFold::WeakestLink,
        cl_routing: HashMap::new(),
        illumination: None,
    };

    assert!(!spec.verify_legality("safety", "mean"));
    assert!(spec.verify_legality("safety", "median"));
}

#[test]
fn test_ee_log_blp() {
    let blp = BitterLessonPreference {
        enabled: true,
        scale_probe_required: true,
        general_method_bonus: 0.1,
    };
    assert_eq!(EELOG::blp_check(true, &blp), 0.1);
    assert_eq!(EELOG::blp_check(false, &blp), 0.0);
}

#[test]
fn test_ethics_conflict_detection() {
    let d1 = EthicalDuty {
        id: "d1".into(),
        scale: EthicalScale::L3Planet,
        description: "Safety".into(),
        priority: 1,
    };
    let d2 = EthicalDuty {
        id: "d2".into(),
        scale: EthicalScale::L3Planet,
        description: "Survival".into(),
        priority: 1,
    };
    let conflict = EthicsCAL::detect_conflict(&d1, &d2).unwrap();
    assert_eq!(conflict.conflict_type, rust_agency::fpf::ethics::ConflictType::Goal);
}

#[test]
fn test_mvpk_view_emission() {
    let scope = PublicationScope {
        id: "ps1".into(),
        scope: Scope { slices: HashSet::new() },
    };
    let view = MVPK::emit_view("m1", FaceKind::TechCard, scope, vec![]);
    assert_eq!(view.kind, FaceKind::TechCard);
    assert_eq!(view.viewpoint_id, "vp_tech");
}

#[test]
fn test_uts_cell_lookup() {
    let cell = SenseCell {
        context_id: "ctx1".into(),
        tech_label: "process".into(),
        plain_label: "workflow".into(),
        gloss: "test".into(),
        sense_family: SenseFamily::Method,
        notes: None,
    };
    let cs = ConceptSet {
        id: "cs1".into(),
        u_type: "U.Method".into(),
        tech_name: "UnifiedProcess".into(),
        plain_name: "Process".into(),
        description: "".into(),
        cells: vec![cell],
        rationale: "".into(),
        nqd: None,
        autonomy: None,
    };
    let uts = UTS {
        id: "uts1".into(),
        context_cards: HashMap::new(),
        concept_sets: vec![cs],
        block_plan: vec![],
    };

    let found = uts.find_cell("ctx1", "process").unwrap();
    assert_eq!(found.plain_label, "workflow");
}

#[test]
fn test_commitment_creation() {
    let c = Commitment {
        id: "c1".into(),
        scope_id: "s1".into(),
        modality: Modality::Must,
        description: "test".into(),
        validity_window: Window::now_open(),
        evidence_refs: vec![],
        status: CommitmentStatus::Open,
    };
    assert_eq!(c.id, "c1");
}

#[test]
fn test_service_clause_acceptance() {
    let _clause = ServiceClause {
        id: "s1".into(),
        provider_role_id: "p1".into(),
        consumer_role_id: Some("c1".into()),
        promise_content: "promise".into(),
        access_spec: AccessSpec { endpoint: "/".into(), protocol: "https".into() },
        acceptance_spec: AcceptanceSpec { criteria: vec!["c1".into()] },
        slo: None,
        sla: None,
    };
    let sit = ServiceSituation {
        id: "sit1".into(),
        clause_id: "s1".into(),
        provider_principal_id: "p1".into(),
        access_point_id: "ap1".into(),
        delivery_system_id: "ds1".into(),
        access_spec_id: "as1".into(),
        commitment_id: None,
        promise_act_id: None,
        work_id: None,
    };
    assert_eq!(sit.clause_id, "s1");
}

#[test]
fn test_evolution_event() {
    let event = EvolutionEvent {
        id: "e1".into(),
        rationale_id: "r1".into(),
        target_id: "h1".into(),
        change_description: "improved".into(),
        version_delta: "0.1.0".into(),
    };
    assert_eq!(event.target_id, "h1");
}

#[test]
fn test_capability_creation() {
    let cap = Capability {
        id: "cap1".into(),
        holder_id: "agent1".into(),
        task_family: "reasoning".into(),
        work_scope: WorkScope { context_slices: vec![] },
        work_measures: WorkMeasures { characteristics: HashMap::new() },
        qualification_window: Window::now_open(),
    };
    assert_eq!(cap.id, "cap1");
}

#[test]
fn test_work_plan_creation() {
    let item = PlanItem {
        id: "i1".into(),
        method_id: "m1".into(),
        planned_window: Window::now_open(),
        required_roles: vec![],
        proposed_performer_id: None,
        budget_reservations: vec![],
        dependencies: vec![],
    };
    let plan = WorkPlan {
        id: "p1".into(),
        context_id: "ctx1".into(),
        items: vec![item],
        version: "1.0".into(),
    };
    assert_eq!(plan.id, "p1");
}

#[test]
fn test_promotion_record() {
    let record = PromotionRecord {
        id: "pr1".into(),
        event_type: MHTEventType::Fusion,
        transformer_role: "supervisor".into(),
        identity_stance: IdentityStance::Stance4D,
        pre_config: PreConfig { node_ids: vec![], edge_descriptions: vec![], bounded_context_id: "ctx1".into() },
        triggers: BOSCTriggers { boundary: None, objective: None, supervisor: None, capability: None, agency: None, temporal: None, context: None },
        post_holon: PostHolon { holon_id: "h1".into(), boundary_description: "".into(), objective: "".into(), supervisory_structure: "".into(), bounded_context_id: "ctx1".into() },
        identity_mapping: HashMap::new(),
        notes: "".into(),
    };
    assert_eq!(record.id, "pr1");
}

#[test]
fn test_sos_log_deduction() {
    let family = MethodFamily {
        id: "f1".into(),
        home_context_id: "ctx1".into(),
        eligibility_predicates: vec![],
    };
    let m1 = MaturityCard {
        family_id: "f1".into(),
        rung: MaturityRung::L1WorkedExamples,
        evidence_graph_path_ids: vec![],
    };
    let m2 = MaturityCard {
        family_id: "f1".into(),
        rung: MaturityRung::L3BenchmarkSevere,
        evidence_graph_path_ids: vec![],
    };
    
    let signature = TaskSignature {
        id: "task_1".to_string(),
        context_id: "R&D".to_string(),
        task_kind: "Inference".to_string(),
        kind_set: vec![],
        data_shape: DataShape::Tabular,
        noise_model: NoiseModel::IIDGaussian,
        objective_profile: ObjectiveProfile {
            heads: vec![],
            dominance_regime: DominanceRegime::ParetoOnly,
        },
        constraints: vec![],
        scope_slice_id: "S1".to_string(),
        evidence_graph_ref: "EG1".to_string(),
        size_scale: SizeScale { n: 100, m: None, complexity_proxy: 1.0, units: "rows".to_string() },
        freshness_window: "30d".to_string(),
        missingness: Missingness::None,
        shift_class: None,
        behavior_space_ref: None,
        archive_config: None,
        emitter_policy_ref: None,
        dominance_regime_qd: DominanceRegime::ParetoOnly,
        portfolio_mode: PortfolioMode::Pareto,
        budgeting: Budgeting { time_limit_ms: 1000, compute_budget: 1.0, cost_ceiling: 1.0, units: "USD".to_string() },
    };

    match SoSLOG::deduce(&family, &m1, &signature) {
        rust_agency::fpf::sos_log::AdmissibilityVerdict::Degrade { mode, .. } => {
            assert_eq!(mode, rust_agency::fpf::sos_log::DegradeMode::Sandbox);
        },
        _ => panic!("Expected Degrade"),
    }

    match SoSLOG::deduce(&family, &m2, &signature) {
        rust_agency::fpf::sos_log::AdmissibilityVerdict::Admit => (),
        _ => panic!("Expected Admit"),
    }
}
