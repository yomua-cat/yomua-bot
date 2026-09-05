//! 关系模型的测试。

use crate::domain::relationship::*;

#[test]
fn new_relationship_defaults() {
    let rel = Relationship::new(1, 2);
    assert_eq!(rel.character_id, 1);
    assert_eq!(rel.participant_id, 2);
    assert_eq!(rel.familiarity, 0.0);
    assert_eq!(rel.affection, 0.2);
    assert_eq!(rel.trust, 0.1);
    assert_eq!(rel.interaction_count, 0);
}

#[test]
fn record_interaction_increments_count() {
    let mut rel = Relationship::new(1, 2);
    rel.record_interaction();
    assert_eq!(rel.interaction_count, 1);
    rel.record_interaction();
    assert_eq!(rel.interaction_count, 2);
}

#[test]
fn familiarity_grows_logarithmically() {
    let mut rel = Relationship::new(1, 2);

    // 第一次互动
    rel.record_interaction();
    let fam1 = rel.familiarity;

    // 经过多次互动之后
    for _ in 0..99 {
        rel.record_interaction();
    }
    let fam100 = rel.familiarity;

    // 熟悉度应增长但上限为 1.0
    assert!(fam100 > fam1);
    assert!(fam100 <= 1.0);
}

#[test]
fn relationship_serialization_roundtrip() {
    let rel = Relationship::new(1, 2);
    let json = serde_json::to_string(&rel).unwrap();
    let deserialized: Relationship = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.character_id, 1);
    assert_eq!(deserialized.participant_id, 2);
}
