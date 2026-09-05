//! 情绪模型的测试。

use crate::domain::emotion::*;

#[test]
fn default_emotion_state() {
    let state = EmotionState::default();
    assert_eq!(state.happiness, 0.5);
    assert_eq!(state.anger, 0.0);
    assert_eq!(state.sadness, 0.0);
    assert_eq!(state.fear, 0.0);
    assert_eq!(state.affection, 0.3);
    assert_eq!(state.stress, 0.1);
    assert_eq!(state.energy, 0.7);
}

#[test]
fn apply_event_adjusts_emotion() {
    let initial = EmotionState::default();
    let event = EmotionEvent {
        event_type: EmotionEventType::MessageReceived,
        adjustments: vec![
            EmotionAdjustment {
                dimension: "happiness".to_string(),
                value: 0.2,
            },
            EmotionAdjustment {
                dimension: "anger".to_string(),
                value: -0.1,
            },
        ],
        description: None,
    };

    let new_state = initial.apply_event(&event);
    assert!((new_state.happiness - 0.7).abs() < 0.001);
    assert_eq!(new_state.anger, 0.0); // 不能低于 0
}

#[test]
fn apply_event_clamps_values() {
    let initial = EmotionState {
        happiness: 0.9,
        ..Default::default()
    };

    let event = EmotionEvent {
        event_type: EmotionEventType::MessageReceived,
        adjustments: vec![EmotionAdjustment {
            dimension: "happiness".to_string(),
            value: 0.5, // 若不进行限制将会是 1.4
        }],
        description: None,
    };

    let new_state = initial.apply_event(&event);
    assert_eq!(new_state.happiness, 1.0); // 被限制为 1.0
}

#[test]
fn time_decay_moves_toward_baseline() {
    let mut state = EmotionState {
        happiness: 0.9, // 高于基线（0.5）
        anger: 0.5,     // 高于基线（0.0）
        ..Default::default()
    };

    state.apply_decay(100.0, 0.01);

    // 应向基线方向移动
    assert!(state.happiness < 0.9);
    assert!(state.happiness > 0.5); // 但不会完全到达
    assert!(state.anger < 0.5);
    assert!(state.anger > 0.0);
}

#[test]
fn time_decay_zero_duration_no_change() {
    let mut state = EmotionState::default();
    let original = state.clone();

    state.apply_decay(0.0, 0.01);

    assert_eq!(state.happiness, original.happiness);
    assert_eq!(state.anger, original.anger);
}

#[test]
fn emotion_serialization_roundtrip() {
    let state = EmotionState::default();
    let json = serde_json::to_string(&state).unwrap();
    let deserialized: EmotionState = serde_json::from_str(&json).unwrap();
    assert_eq!(state.happiness, deserialized.happiness);
    assert_eq!(state.energy, deserialized.energy);
}
