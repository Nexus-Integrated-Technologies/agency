use rust_agency::fpf::holon::{System, Boundary, BoundaryKind, Entity};
use rust_agency::fpf::context::BoundedContext;
use rust_agency::fpf::role::{RoleAssignment, Window};
use rust_agency::fpf::transformer::MethodDescription;
use rust_agency::fpf::agency::{AgencyCHR, AgencyGrade};
use rust_agency::fpf::mereology::{Portion, MereologicalRelation};
use rust_agency::fpf::traits::ReferencePlane;
use rust_agency::fpf::gate::GateDecision;

#[test]
fn test_holon_system_creation() {
    let boundary = Boundary {
        kind: BoundaryKind::Open,
        description: "Test boundary".to_string(),
    };
    
    let system = System {
        id: "test_system".to_string(),
        boundary,
        characteristics: std::collections::HashMap::new(),
    };
    
    assert_eq!(system.id(), "test_system");
}

#[test]
fn test_bounded_context() {
    let boundary = Boundary {
        kind: BoundaryKind::Closed,
        description: "Semantic boundary".to_string(),
    };
    
    let context = BoundedContext::new("Technical", boundary)
        .with_role("Coder")
        .with_invariant("No bugs permitted");
        
    assert!(context.roles.contains("Coder"));
    assert!(context.invariants.contains("No bugs permitted"));
}

#[test]
fn test_role_assignment() {
    let window = Window::now_open();
    let assignment = RoleAssignment {
        holder_id: "agent_1".to_string(),
        role_id: "Coder".to_string(),
        context_id: "Technical".to_string(),
        window,
        justification: None,
        provenance: None,
    };
    
    assert_eq!(assignment.holder_id, "agent_1");
}

#[test]
fn test_method_description() {
    let method = MethodDescription {
        id: "write_code".to_string(),
        content: "fn main() { println!(\"Hello\"); }".to_string(),
        version: "1.0.0".to_string(),
        required_roles: vec!["Coder".to_string()],
    };
    
    assert_eq!(method.required_roles[0], "Coder");
}

#[test]
fn test_agency_grade() {
    let chr = AgencyCHR {
        bmc: 1.0,
        ph: 1.0,
        mp: 1.0,
        per: 1.0,
        oc: 1.0,
    };
    let grade = AgencyGrade::from_chr(&chr);
    assert_eq!(grade, AgencyGrade::ReflectiveStrategic);
}

#[test]
fn test_mereology_portion() {
    let portion = Portion {
        whole_id: "tank_1".to_string(),
        measure_kind: "volume".to_string(),
        quantity: 50.0,
        unit: "L".to_string(),
    };
    let rel = MereologicalRelation::PortionOf(portion);
    match rel {
        MereologicalRelation::PortionOf(p) => assert_eq!(p.quantity, 50.0),
        _ => panic!("Expected PortionOf"),
    }
}

#[test]
fn test_gate_decision_join() {
    let d1 = GateDecision::Pass;
    let d2 = GateDecision::Block;
    assert_eq!(d1.join(d2), GateDecision::Block);
    
    let d3 = GateDecision::Abstain;
    let d4 = GateDecision::Degrade;
    assert_eq!(d3.join(d4), GateDecision::Degrade);
}

#[test]
fn test_reference_plane() {
    let plane = ReferencePlane::World;
    match plane {
        ReferencePlane::World => (),
        _ => panic!("Expected World plane"),
    }
}
