//! SQLite 存储集成测试。

#[cfg(test)]
mod storage_tests {
    use crate::domain::character::*;
    use crate::domain::conversation::*;
    use crate::domain::memory::*;
    use crate::domain::message::*;
    use crate::domain::relationship::*;
    use crate::domain::repository::*;
    use crate::infrastructure::storage::SqliteStorage;

    async fn setup_storage() -> SqliteStorage {
        let storage = SqliteStorage::open_in_memory()
            .await
            .expect("failed to open in-memory db");
        storage.migrate().await.expect("failed to run migrations");
        storage
    }

    #[tokio::test]
    async fn test_character_crud() {
        let storage = setup_storage().await;
        let repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );

        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "Alice".to_string(),
                description: Some("A test character".to_string()),
                personality: Some("Friendly".to_string()),
                scenario: None,
                style: None,
                background: None,
                greetings: vec!["Hello!".to_string()],
                example_messages: vec![],
                system_prompt: Some("You are Alice.".to_string()),
                post_history_instructions: None,
                lorebook: vec![],
                metadata: serde_json::json!({}),
            },
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        // 插入
        let id = repo.insert(&character).await.expect("insert failed");
        assert!(id > 0);

        // 按 ID 查找
        let found = repo.find_by_id(id).await.expect("find failed").unwrap();
        assert_eq!(found.definition.name, "Alice");
        assert_eq!(
            found.definition.system_prompt.as_deref(),
            Some("You are Alice.")
        );

        // 查找全部
        let all = repo.find_all().await.expect("find_all failed");
        assert_eq!(all.len(), 1);

        // 删除
        repo.delete(id).await.expect("delete failed");
        let found = repo.find_by_id(id).await.expect("find after delete failed");
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_conversation_crud() {
        let storage = setup_storage().await;
        let repo = crate::infrastructure::storage::repository::SqliteConversationRepository::new(
            storage.pool().clone(),
        );

        let conv = Conversation {
            id: 0,
            conversation_type: ConversationType::Group,
            external_id: "123456".to_string(),
            name: Some("Test Group".to_string()),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        let id = repo.insert(&conv).await.expect("insert failed");
        assert!(id > 0);

        // 按外部 ID 查找
        let found = repo
            .find_by_external_id("123456")
            .await
            .expect("find by external_id failed")
            .unwrap();
        assert_eq!(found.name.as_deref(), Some("Test Group"));

        // 删除
        repo.delete(id).await.expect("delete failed");
    }

    #[tokio::test]
    async fn test_message_crud() {
        let storage = setup_storage().await;
        let conv_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let part_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );
        let msg_repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );

        // 准备：创建会话和参与者
        let conv = Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "user42".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv_id = conv_repo.insert(&conv).await.unwrap();

        let participant = Participant {
            id: 0,
            conversation_id: conv_id,
            external_id: "user42".to_string(),
            display_name: "TestUser".to_string(),
            role: ParticipantRole::User,
            metadata: serde_json::json!({}),
        };
        let part_id = part_repo.insert(&participant).await.unwrap();

        // 插入消息
        let msg = Message {
            id: 0,
            conversation_id: conv_id,
            sender_id: part_id,
            content: MessageContent::Text("Hello world".to_string()),
            timestamp: chrono::Utc::now(),
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
        };
        let msg_id = msg_repo.insert(&msg).await.expect("insert message failed");
        assert!(msg_id > 0);

        // 查找最近消息
        let recent = msg_repo
            .find_recent(conv_id, 10)
            .await
            .expect("find recent failed");
        assert_eq!(recent.len(), 1);
        match &recent[0].content {
            MessageContent::Text(s) => assert_eq!(s, "Hello world"),
            _ => panic!("expected text content"),
        }
    }

    #[tokio::test]
    async fn test_memory_crud() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let mem_repo = crate::infrastructure::storage::repository::SqliteMemoryRepository::new(
            storage.pool().clone(),
        );

        // 先创建一个角色
        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "TestChar".to_string(),
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
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_id = char_repo.insert(&character).await.unwrap();

        // 插入记忆
        let memory = Memory::new(
            char_id,
            None,
            MemoryType::Episodic,
            "User likes cats".to_string(),
            0.8,
        );
        let mem_id = mem_repo
            .insert(&memory)
            .await
            .expect("insert memory failed");
        assert!(mem_id > 0);

        // 按角色查找
        let memories = mem_repo
            .find_by_character_id(char_id, None, 10)
            .await
            .expect("find memories failed");
        assert_eq!(memories.len(), 1);
        assert_eq!(memories[0].content, "User likes cats");
    }

    #[tokio::test]
    async fn test_memory_keyword_search() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let mem_repo = crate::infrastructure::storage::repository::SqliteMemoryRepository::new(
            storage.pool().clone(),
        );

        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "KW".to_string(),
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
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_id = char_repo.insert(&character).await.unwrap();

        mem_repo
            .insert(&Memory::new(
                char_id,
                None,
                MemoryType::Semantic,
                "用户喜欢猫，养了一只橘猫".to_string(),
                0.8,
            ))
            .await
            .unwrap();
        mem_repo
            .insert(&Memory::new(
                char_id,
                None,
                MemoryType::Episodic,
                "用户今天出去散步".to_string(),
                0.6,
            ))
            .await
            .unwrap();

        // 命中「猫」的记忆应被检索到。
        let hits = mem_repo
            .search_by_keywords(char_id, &["猫".to_string()], 10)
            .await
            .expect("检索应成功");
        assert_eq!(hits.len(), 1);
        assert!(hits[0].content.contains("猫"));

        // 无匹配关键词 → 空。
        let none = mem_repo
            .search_by_keywords(char_id, &["不存在".to_string()], 10)
            .await
            .unwrap();
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn test_relationship_upsert() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let conv_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let part_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );
        let rel_repo =
            crate::infrastructure::storage::repository::SqliteRelationshipRepository::new(
                storage.pool().clone(),
            );

        // 准备
        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "Char1".to_string(),
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
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_id = char_repo.insert(&character).await.unwrap();

        let conv = Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "user99".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv_id = conv_repo.insert(&conv).await.unwrap();

        let participant = Participant {
            id: 0,
            conversation_id: conv_id,
            external_id: "user99".to_string(),
            display_name: "User99".to_string(),
            role: ParticipantRole::User,
            metadata: serde_json::json!({}),
        };
        let part_id = part_repo.insert(&participant).await.unwrap();

        // 创建关系
        let mut rel = Relationship::new(char_id, part_id);
        rel.record_interaction();
        rel.record_interaction();

        rel_repo.upsert(&rel).await.expect("upsert failed");

        // 查找关系
        let found = rel_repo
            .find(char_id, part_id)
            .await
            .expect("find failed")
            .unwrap();
        assert_eq!(found.interaction_count, 2);
        assert!(found.familiarity > 0.0);

        // 再次 upsert（更新）
        let mut rel2 = found;
        rel2.record_interaction();
        rel_repo.upsert(&rel2).await.expect("second upsert failed");

        let found2 = rel_repo.find(char_id, part_id).await.unwrap().unwrap();
        assert_eq!(found2.interaction_count, 3);
    }

    #[tokio::test]
    async fn test_binding_find_all_and_insert() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let conv_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let binding_repo =
            crate::infrastructure::storage::repository::SqliteCharacterBindingRepository::new(
                storage.pool().clone(),
            );

        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "BindAll".to_string(),
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
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_id = char_repo.insert(&character).await.unwrap();

        let conv = Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "bind-user".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv_id = conv_repo.insert(&conv).await.unwrap();

        let b1 = CharacterBinding {
            id: 0,
            character_id: char_id,
            conversation_id: conv_id,
            reply_mode: crate::domain::character::ReplyMode::Natural,
            proactive_enabled: true,
            mute_schedule: Some("23:00-07:00".to_string()),
            behavior_overrides: serde_json::json!({}),
            context_policy: serde_json::json!({}),
            switched_at: None,
            cross_reply_enabled: false,
            created_at: chrono::Utc::now(),
        };
        binding_repo.insert(&b1).await.unwrap();

        // find_all 应返回已插入的全部绑定。
        let all = binding_repo.find_all().await.expect("find_all 应成功");
        assert_eq!(all.len(), 1);
        assert!(all[0].proactive_enabled, "proactive_enabled 应持久化");
        assert_eq!(
            all[0].mute_schedule.as_deref(),
            Some("23:00-07:00"),
            "mute_schedule 应持久化"
        );
    }

    #[tokio::test]
    async fn test_binding_switched_at_roundtrip() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let conv_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let binding_repo =
            crate::infrastructure::storage::repository::SqliteCharacterBindingRepository::new(
                storage.pool().clone(),
            );

        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "BindSwitch".to_string(),
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
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_id = char_repo.insert(&character).await.unwrap();

        let conv = Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "bind-switch-user".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv_id = conv_repo.insert(&conv).await.unwrap();

        // switched_at = Some(固定时间) → 应能往返持久化。
        let switched_at = chrono::DateTime::parse_from_rfc3339("2026-05-01T08:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let b1 = CharacterBinding {
            id: 0,
            character_id: char_id,
            conversation_id: conv_id,
            reply_mode: crate::domain::character::ReplyMode::Occasionally,
            proactive_enabled: true,
            mute_schedule: None,
            behavior_overrides: serde_json::json!({"tone": "cool"}),
            context_policy: serde_json::json!({"history": 30}),
            switched_at: Some(switched_at),
            cross_reply_enabled: false,
            created_at: chrono::Utc::now(),
        };
        let b1_id = binding_repo.insert(&b1).await.unwrap();
        assert!(b1_id > 0);

        let found = binding_repo.find_all().await.expect("find_all 应成功");
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].switched_at,
            Some(switched_at),
            "switched_at 应持久化为原值"
        );

        // switched_at = None → 回来仍为 None（G1：需换一个会话插入，同会话仅允许一个绑定）。
        let conv2 = Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "bind-switch-user-2".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv2_id = conv_repo.insert(&conv2).await.unwrap();
        let b2 = CharacterBinding {
            id: 0,
            character_id: char_id,
            conversation_id: conv2_id,
            reply_mode: crate::domain::character::ReplyMode::Natural,
            proactive_enabled: false,
            mute_schedule: None,
            behavior_overrides: serde_json::json!({}),
            context_policy: serde_json::json!({}),
            switched_at: None,
            cross_reply_enabled: false,
            created_at: chrono::Utc::now(),
        };
        let b2_id = binding_repo.insert(&b2).await.unwrap();
        let by_conv = binding_repo
            .find_by_conversation_id(conv2_id)
            .await
            .expect("find_by_conversation_id 应成功");
        assert_eq!(by_conv.len(), 1);
        let b2_back = by_conv.iter().find(|b| b.id == b2_id).unwrap();
        assert_eq!(b2_back.switched_at, None, "None 应保持 None");

        // update：整体替换字段（换角色 + 更新 switched_at）。
        let switched_at2 = chrono::DateTime::parse_from_rfc3339("2026-06-01T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let mut updated = found[0].clone();
        updated.character_id = char_id;
        updated.reply_mode = crate::domain::character::ReplyMode::MentionOnly;
        updated.proactive_enabled = false;
        updated.switched_at = Some(switched_at2);
        binding_repo.update(&updated).await.expect("update 应成功");

        let after = binding_repo
            .find_by_conversation_id(conv_id)
            .await
            .unwrap()
            .into_iter()
            .find(|b| b.id == b1_id)
            .unwrap();
        assert_eq!(after.character_id, char_id);
        assert_eq!(
            after.reply_mode,
            crate::domain::character::ReplyMode::MentionOnly
        );
        assert!(!after.proactive_enabled);
        assert_eq!(
            after.switched_at,
            Some(switched_at2),
            "update 应写入新 switched_at"
        );
    }

    #[tokio::test]
    async fn test_plugin_data_roundtrip() {
        let storage = setup_storage().await;
        let repo = crate::infrastructure::storage::repository::SqlitePluginDataRepository::new(
            storage.pool().clone(),
        );

        // 未设置 → None
        assert!(repo.get("alpha", "name").await.unwrap().is_none());

        // set 简单值
        repo.set("alpha", "name", &serde_json::json!("echo"))
            .await
            .unwrap();
        assert_eq!(
            repo.get("alpha", "name").await.unwrap(),
            Some(serde_json::json!("echo"))
        );

        // upsert 覆盖
        repo.set("alpha", "name", &serde_json::json!("echo-v2"))
            .await
            .unwrap();
        assert_eq!(
            repo.get("alpha", "name").await.unwrap(),
            Some(serde_json::json!("echo-v2"))
        );

        // JSON 复杂值
        let complex = serde_json::json!({ "list": [1, 2, 3], "obj": { "a": true } });
        repo.set("alpha", "cfg", &complex).await.unwrap();
        assert_eq!(repo.get("alpha", "cfg").await.unwrap(), Some(complex));

        // 跨插件隔离：别的插件读不到，也列不到
        assert!(repo.get("beta", "name").await.unwrap().is_none());
        assert!(repo.get("beta", "cfg").await.unwrap().is_none());
        let mut alpha_keys = repo.list_keys("alpha").await.unwrap();
        alpha_keys.sort();
        assert_eq!(alpha_keys, vec!["cfg", "name"]);

        // beta 的数据与 alpha 互不干扰
        repo.set("beta", "k1", &serde_json::json!(1)).await.unwrap();
        let beta_keys = repo.list_keys("beta").await.unwrap();
        assert_eq!(beta_keys, vec!["k1"]);
        let alpha_keys_again = repo.list_keys("alpha").await.unwrap();
        assert_eq!(alpha_keys_again.len(), 2, "beta 写入不得影响 alpha 的键");

        // delete：删除后读不到、列表不再含该键
        repo.delete("alpha", "name").await.unwrap();
        assert!(repo.get("alpha", "name").await.unwrap().is_none());
        let alpha_keys_after = repo.list_keys("alpha").await.unwrap();
        assert_eq!(alpha_keys_after, vec!["cfg"]);

        // 对不存在的键 delete 也不报错
        repo.delete("alpha", "missing").await.unwrap();
    }

    #[tokio::test]
    async fn test_character_state_upsert() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let state_repo =
            crate::infrastructure::storage::repository::SqliteCharacterStateRepository::new(
                storage.pool().clone(),
            );

        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "StateTest".to_string(),
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
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_id = char_repo.insert(&character).await.unwrap();

        // 默认状态无 last_proactive_at。
        let mut state = CharacterState {
            energy: 30.0,
            stress: 80.0,
            ..Default::default()
        };
        assert!(state.last_proactive_at.is_none(), "默认无主动时间");

        state_repo.upsert(char_id, &state).await.unwrap();

        let loaded = state_repo
            .find_by_character_id(char_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.energy, 30.0);
        assert_eq!(loaded.stress, 80.0);
        assert!(loaded.last_proactive_at.is_none());

        // 再次 upsert（更新），并写入 last_proactive_at 验证持久化往返。
        state.energy = 90.0;
        let proactive_time = chrono::Utc::now() - chrono::Duration::minutes(5);
        state.last_proactive_at = Some(proactive_time);
        state_repo.upsert(char_id, &state).await.unwrap();

        let loaded2 = state_repo
            .find_by_character_id(char_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded2.energy, 90.0);
        let saved = loaded2
            .last_proactive_at
            .expect("last_proactive_at 应被持久化");
        assert!(
            (saved - proactive_time).num_seconds().abs() <= 1,
            "last_proactive_at 应接近写入值"
        );
    }
}
