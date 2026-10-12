//! 行为模型的测试。

use crate::domain::behavior::*;

#[test]
fn behavior_decision_creation() {
    let decision = BehaviorDecision {
        action: BehaviorAction::Reply,
        priority: Priority::Normal,
        cognition_level: CognitionLevel::Light,
        delay_ms: 0,
        reason: "User mentioned character".to_string(),
        decided_at: chrono::Utc::now(),
    };

    assert_eq!(decision.action, BehaviorAction::Reply);
    assert_eq!(decision.priority, Priority::Normal);
}

#[test]
fn priority_ordering() {
    assert!(Priority::Realtime < Priority::Urgent);
    assert!(Priority::Urgent < Priority::Normal);
    assert!(Priority::Normal < Priority::Background);
}

#[test]
fn behavior_action_variants() {
    let actions = [
        BehaviorAction::Reply,
        BehaviorAction::Ignore,
        BehaviorAction::Delay,
        BehaviorAction::InitiateProactive,
        BehaviorAction::UpdateState,
    ];

    // 仅确保它们全部可构造且互不相同
    for (i, a1) in actions.iter().enumerate() {
        for (j, a2) in actions.iter().enumerate() {
            if i != j {
                assert_ne!(a1, a2);
            }
        }
    }
}

#[test]
fn cognition_level_variants() {
    assert_ne!(CognitionLevel::None, CognitionLevel::Light);
    assert_ne!(CognitionLevel::Light, CognitionLevel::Deep);
}

#[test]
fn action_serialization() {
    let action = Action::SendMessage {
        conversation_id: 1,
        content: "hello".to_string(),
    };
    let json = serde_json::to_string(&action).unwrap();
    let deserialized: Action = serde_json::from_str(&json).unwrap();
    match deserialized {
        Action::SendMessage {
            conversation_id,
            content,
        } => {
            assert_eq!(conversation_id, 1);
            assert_eq!(content, "hello");
        }
        _ => panic!("expected SendMessage"),
    }
}
