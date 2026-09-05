//! 领域模型的测试。

#[cfg(test)]
mod character_tests {
    use crate::domain::character::*;
    use chrono::Utc;

    #[test]
    fn character_state_default() {
        let state = CharacterState::default();
        assert_eq!(state.energy, 72.0);
        assert_eq!(state.attention, 50.0);
        assert_eq!(state.stress, 10.0);
        assert_eq!(state.social_mood.as_deref(), Some("calm"));
    }

    #[test]
    fn character_definition_fields() {
        let def = CharacterDefinition {
            name: "Alice".to_string(),
            description: Some("A friendly girl".to_string()),
            personality: Some("Cheerful, curious".to_string()),
            scenario: None,
            style: Some("Casual, uses emoji".to_string()),
            background: None,
            greetings: vec!["Hi there!".to_string()],
            example_messages: vec![],
            system_prompt: Some("You are Alice.".to_string()),
            post_history_instructions: None,
            lorebook: vec![],
            metadata: serde_json::json!({}),
        };

        assert_eq!(def.name, "Alice");
        assert!(def.greetings.contains(&"Hi there!".to_string()));
    }

    #[test]
    fn character_binding_reply_modes() {
        assert_ne!(ReplyMode::MentionOnly, ReplyMode::Natural);
        assert_eq!(ReplyMode::Occasionally, ReplyMode::Occasionally);
    }

    #[test]
    fn character_serialization_roundtrip() {
        let character = Character {
            id: 1,
            definition: CharacterDefinition {
                name: "Test".to_string(),
                description: None,
                personality: None,
                scenario: None,
                style: None,
                background: None,
                greetings: vec![],
                example_messages: vec![],
                system_prompt: None,
                post_history_instructions: None,
                lorebook: vec![],
                metadata: serde_json::json!({}),
            },
            state: CharacterState::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let json = serde_json::to_string(&character).unwrap();
        let deserialized: Character = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, 1);
        assert_eq!(deserialized.definition.name, "Test");
    }

    #[test]
    fn character_definition_validate_rejects_blank_name() {
        let def = CharacterDefinition {
            name: "   ".to_string(),
            description: None,
            personality: None,
            scenario: None,
            style: None,
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: None,
            post_history_instructions: None,
            lorebook: vec![],
            metadata: serde_json::json!({}),
        };
        assert!(def.validate().is_err());

        let mut valid = def.clone();
        valid.name = "Alice".to_string();
        assert!(valid.validate().is_ok());
    }

    #[test]
    fn character_state_clamped_bounds_numeric_fields() {
        let state = CharacterState {
            energy: 150.0,
            attention: -20.0,
            stress: 999.0,
            ..Default::default()
        };
        let clamped = state.clamped();
        assert_eq!(clamped.energy, 100.0);
        assert_eq!(clamped.attention, 0.0);
        assert_eq!(clamped.stress, 100.0);

        // 已在 [0,100] 内的值保持不变
        let in_range = CharacterState {
            energy: 50.0,
            attention: 60.0,
            stress: 30.0,
            ..Default::default()
        }
        .clamped();
        assert_eq!(in_range.energy, 50.0);
        assert_eq!(in_range.attention, 60.0);
        assert_eq!(in_range.stress, 30.0);
    }
}

#[cfg(test)]
mod conversation_tests {
    use crate::domain::conversation::*;

    #[test]
    fn conversation_type_variants() {
        assert_ne!(ConversationType::Private, ConversationType::Group);
    }

    #[test]
    fn participant_role_variants() {
        assert_ne!(ParticipantRole::User, ParticipantRole::Character);
        assert_ne!(ParticipantRole::User, ParticipantRole::System);
    }
}

#[cfg(test)]
mod message_tests {
    use crate::domain::message::*;

    #[test]
    fn message_content_variants() {
        let text = MessageContent::Text("hello".to_string());
        match text {
            MessageContent::Text(s) => assert_eq!(s, "hello"),
            _ => panic!("expected text"),
        }
    }

    #[test]
    fn message_serialization() {
        let msg = MessageContent::Text("test".to_string());
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: MessageContent = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, deserialized);
    }
}
