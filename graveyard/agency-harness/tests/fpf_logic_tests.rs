use rust_agency::fpf::aggregation::{SysCAL, EpistemeCAL, DependencyGraph, AttributeClass, DesignRunTag};
use rust_agency::fpf::holon::{System, Episteme, Boundary, BoundaryKind, CharacteristicValue};
use rust_agency::fpf::assurance::{AssuranceTuple, Reliability, Formality, ClaimScope, CongruenceLevel};
use rust_agency::fpf::nqd_cal::{NQDCAL};
use rust_agency::fpf::creativity_chr::NQDBundle;
use rust_agency::fpf::mm_chr::{Measure, CoordinateValue};
use std::collections::{HashMap, HashSet, BTreeMap};

#[test]
fn test_sys_aggregation_extensive() {
    let sys_cal = SysCAL;
    let mut parts = HashMap::new();
    
    let mut char1 = HashMap::new();
    char1.insert("mass".to_string(), CharacteristicValue::Numeric(10.0));
    parts.insert("p1".to_string(), System {
        id: "p1".to_string(),
        boundary: Boundary { kind: BoundaryKind::Open, description: "".to_string() },
        characteristics: char1,
    });

    let mut char2 = HashMap::new();
    char2.insert("mass".to_string(), CharacteristicValue::Numeric(20.0));
    parts.insert("p2".to_string(), System {
        id: "p2".to_string(),
        boundary: Boundary { kind: BoundaryKind::Open, description: "".to_string() },
        characteristics: char2,
    });

    let graph = DependencyGraph {
        nodes: vec!["p1".to_string(), "p2".to_string()],
        edges: vec![],
        design_run_tag: DesignRunTag::Design,
        notes: "".to_string(),
    };

    let mut attr_classes = HashMap::new();
    attr_classes.insert("mass".to_string(), AttributeClass::Extensive);

    let result = sys_cal.aggregate("agg_sys", &graph, &parts, &attr_classes);
    
    if let Some(CharacteristicValue::Numeric(val)) = result.characteristics.get("mass") {
        assert_eq!(*val, 30.0);
    } else {
        panic!("Expected numeric mass characteristic");
    }
}

use rust_agency::fpf::q_bundle::{ContextSlice, Scope};

#[test]
fn test_episteme_aggregation_reliability() {
    let epist_cal = EpistemeCAL;
    let mut parts = HashMap::new();
    let mut assurance_map = HashMap::new();

    let e1 = Episteme {
        id: "e1".to_string(),
        boundary: Boundary { kind: BoundaryKind::Permeable, description: "".to_string() },
        content: "Fact 1".to_string(),
        version: "1.0".to_string(),
        characteristics: HashMap::new(),
    };
    let t1 = AssuranceTuple {
        formality: Formality::F4VerifiedSpecification,
        scope: ClaimScope { span: HashSet::new(), description: "S1".to_string() },
        reliability: Reliability(0.9),
        notes: "".to_string(),
    };
    parts.insert("e1".to_string(), e1);
    assurance_map.insert("e1".to_string(), t1);

    let e2 = Episteme {
        id: "e2".to_string(),
        boundary: Boundary { kind: BoundaryKind::Permeable, description: "".to_string() },
        content: "Fact 2".to_string(),
        version: "1.0".to_string(),
        characteristics: HashMap::new(),
    };
    let t2 = AssuranceTuple {
        formality: Formality::F2Formalizable,
        scope: ClaimScope { span: HashSet::new(), description: "S2".to_string() },
        reliability: Reliability(0.8),
        notes: "".to_string(),
    };
    parts.insert("e2".to_string(), e2);
    assurance_map.insert("e2".to_string(), t2);

    let graph = DependencyGraph {
        nodes: vec!["e1".to_string(), "e2".to_string()],
        edges: vec![],
        design_run_tag: DesignRunTag::Design,
        notes: "".to_string(),
    };

    let (_result_epist, result_tuple) = epist_cal.aggregate(
        "agg_epist", 
        &graph, 
        &parts, 
        &assurance_map, 
        CongruenceLevel::CL3Verified // No penalty for CL3
    );

    // F_eff = min(F4, F2) = F2
    assert_eq!(result_tuple.formality, Formality::F2Formalizable);
    // R_eff = min(0.9, 0.8) - 0.0 = 0.8
    assert_eq!(result_tuple.reliability.0, 0.8);
}

#[test]
fn test_scope_coverage() {
    let slice = ContextSlice {
        context_id: "R&D".to_string(),
        standard_versions: BTreeMap::new(),
        environment_selectors: BTreeMap::new(),
        gamma_time: "2026-01-01".to_string(),
    };
    
    let mut slices = HashSet::new();
    slices.insert(slice.clone());
    
    let scope = Scope { slices };
    assert!(scope.covers(&slice));
}

#[test]
fn test_pareto_dominance() {
    let candidates = vec![
        NQDBundle {
            novelty: Measure { method_id: "m".into(), coordinate: CoordinateValue::Scalar(0.5), timestamp: chrono::Utc::now(), reliability: Reliability(1.0) },
            quality: Measure { method_id: "m".into(), coordinate: CoordinateValue::Scalar(0.5), timestamp: chrono::Utc::now(), reliability: Reliability(1.0) },
            diversity: Measure { method_id: "m".into(), coordinate: CoordinateValue::Scalar(0.1), timestamp: chrono::Utc::now(), reliability: Reliability(1.0) },
        },
        NQDBundle {
            novelty: Measure { method_id: "m".into(), coordinate: CoordinateValue::Scalar(0.6), timestamp: chrono::Utc::now(), reliability: Reliability(1.0) },
            quality: Measure { method_id: "m".into(), coordinate: CoordinateValue::Scalar(0.6), timestamp: chrono::Utc::now(), reliability: Reliability(1.0) },
            diversity: Measure { method_id: "m".into(), coordinate: CoordinateValue::Scalar(0.1), timestamp: chrono::Utc::now(), reliability: Reliability(1.0) },
        },
        NQDBundle {
            novelty: Measure { method_id: "m".into(), coordinate: CoordinateValue::Scalar(0.7), timestamp: chrono::Utc::now(), reliability: Reliability(1.0) },
            quality: Measure { method_id: "m".into(), coordinate: CoordinateValue::Scalar(0.4), timestamp: chrono::Utc::now(), reliability: Reliability(1.0) },
            diversity: Measure { method_id: "m".into(), coordinate: CoordinateValue::Scalar(0.1), timestamp: chrono::Utc::now(), reliability: Reliability(1.0) },
        },
    ];

    let front_indices = NQDCAL::compute_dominance(&candidates);
    
    // candidate 0 is dominated by candidate 1 (0.6 > 0.5, 0.6 > 0.5)
    // candidate 1 and 2 are non-dominated (1 is better in quality, 2 is better in novelty)
    assert_eq!(front_indices.len(), 2);
    assert!(front_indices.contains(&1));
    assert!(front_indices.contains(&2));
}