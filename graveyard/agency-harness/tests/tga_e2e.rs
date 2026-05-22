use rust_agency::fpf::tga::{TGA, Node, NodeKind, CtxState, DesignRunTag, Transfer, GateDecision};
use rust_agency::fpf::dispatcher::{Dispatcher, MethodFamily, EligibilityStandard, AssuranceProfile};
use rust_agency::fpf::task_signature::{TaskSignature, DataShape, NoiseModel, ObjectiveProfile, SizeScale, Missingness, DominanceRegime, PortfolioMode, Budgeting};
use std::collections::HashMap;

#[test]
fn test_tga_full_transduction_path() {
    // 1. Setup CtxState
    let ctx_state = CtxState {
        locus: "R&D".to_string(),
        reference_plane: "world".to_string(),
        editions: HashMap::new(),
        tag: DesignRunTag::Design,
    };

    // 2. Create nodes for a simple path: Signature -> Mechanism -> Check
    let n1 = Node {
        id: "sig_1".to_string(),
        kind: NodeKind::Signature,
        species_id: "A.6.0".to_string(),
        ctx_state: ctx_state.clone(),
    };

    let n2 = Node {
        id: "mech_1".to_string(),
        kind: NodeKind::Mechanism,
        species_id: "A.6.1".to_string(),
        ctx_state: ctx_state.clone(),
    };

    let n3 = Node {
        id: "gate_1".to_string(),
        kind: NodeKind::Check,
        species_id: "OperationalGate".to_string(),
        ctx_state: ctx_state.clone(),
    };

    // 3. Create transfers (edges)
    let t1 = Transfer {
        id: "trans_1".to_string(),
        source_id: "sig_1".to_string(),
        target_id: "mech_1".to_string(),
        assurance_ops: vec![],
    };

    let t2 = Transfer {
        id: "trans_2".to_string(),
        source_id: "mech_1".to_string(),
        target_id: "gate_1".to_string(),
        assurance_ops: vec![],
    };

    // 4. Verify CtxState preservation across transfers
    assert!(TGA::verify_transfer_preservation(&t1, &n1, &n2));
    assert!(TGA::verify_transfer_preservation(&t2, &n2, &n3));

    // 5. Setup MethodFamily Registry for Dispatcher
    let mut registry = HashMap::new();
    let family_1 = MethodFamily {
        id: "family_1".to_string(),
        context_id: "R&D".to_string(),
        tradition: "Scientific".to_string(),
        version: "1.0.0".to_string(),
        eligibility_standard: EligibilityStandard {
            required_data_shapes: vec!["Tabular".to_string()],
            noise_tolerances: vec!["IIDGaussian".to_string()],
            resource_envelope: "low".to_string(),
            scope_prerequisites: vec![],
        },
        assurance_profile: AssuranceProfile {
            formality_level: "F4".to_string(),
            expected_lanes: vec!["LA".to_string()],
            cl_allowances: HashMap::new(),
        },
        cost_model: "O(n)".to_string(),
        method_description_ids: vec![],
    };
    registry.insert("family_1".to_string(), family_1);

    // 6. Create TaskSignature
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
        dominance_regime_qd: DominanceRegime::ParetoOnly, // Renamed to avoid collision
        portfolio_mode: PortfolioMode::Pareto,
        budgeting: Budgeting { time_limit_ms: 1000, compute_budget: 1.0, cost_ceiling: 1.0, units: "USD".to_string() },
    };

    // 7. Execute Dispatcher Selection
    let selection_result = Dispatcher::select(&registry, &signature, "policy_1");
    
    assert_eq!(selection_result.chosen_family, Some("family_1".to_string()));
    assert_eq!(selection_result.candidates.len(), 1);

    // 8. Join decisions at the gate
    let d1 = GateDecision::Pass;
    let d2 = GateDecision::Pass;
    let final_decision = TGA::join_decisions(d1, d2);
    assert_eq!(final_decision, GateDecision::Pass);
}