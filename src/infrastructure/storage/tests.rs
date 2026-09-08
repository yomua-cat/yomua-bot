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
            active_character_id: None,
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

        // 首次写入：energy / stress / current_activity 往返。
        let mut state = CharacterState {
            energy: 30.0,
            stress: 80.0,
            current_activity: Some("休息".to_string()),
            ..Default::default()
        };
        state_repo.upsert(char_id, &state).await.unwrap();

        let loaded = state_repo
            .find_by_character_id(char_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.energy, 30.0);
        assert_eq!(loaded.stress, 80.0);
        assert_eq!(loaded.current_activity.as_deref(), Some("休息"));

        // 再次 upsert（更新）验证覆盖语义。
        state.energy = 90.0;
        state.current_activity = None;
        state_repo.upsert(char_id, &state).await.unwrap();

        let loaded2 = state_repo
            .find_by_character_id(char_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded2.energy, 90.0);
        assert_eq!(loaded2.current_activity, None);
    }

    #[tokio::test]
    async fn test_mood_upsert_roundtrip_and_scope() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let conv_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let mood_repo = crate::infrastructure::storage::repository::SqliteMoodRepository::new(
            storage.pool().clone(),
        );

        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "MoodTest".to_string(),
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
        let conv1 = Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "u_mood_1".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv2 = Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "u_mood_2".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv1_id = conv_repo.insert(&conv1).await.unwrap();
        let conv2_id = conv_repo.insert(&conv2).await.unwrap();

        // 写入与读取往返。
        let mood = crate::domain::emotion::Mood {
            value: 68.0,
            last_updated: chrono::Utc::now(),
        };
        mood_repo.upsert(char_id, conv1_id, &mood).await.unwrap();
        let loaded = mood_repo
            .find_by_character_and_conversation(char_id, conv1_id)
            .await
            .unwrap()
            .expect("应能读回 mood");
        assert!((loaded.value - 68.0).abs() < 1e-6);

        // 范围隔离：另一个会话无 mood；写入后互不影响。
        assert!(mood_repo
            .find_by_character_and_conversation(char_id, conv2_id)
            .await
            .unwrap()
            .is_none());
        mood_repo
            .upsert(
                char_id,
                conv2_id,
                &crate::domain::emotion::Mood {
                    value: 30.0,
                    last_updated: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
        let v1 = mood_repo
            .find_by_character_and_conversation(char_id, conv1_id)
            .await
            .unwrap()
            .unwrap();
        let v2 = mood_repo
            .find_by_character_and_conversation(char_id, conv2_id)
            .await
            .unwrap()
            .unwrap();
        assert!((v1.value - 68.0).abs() < 1e-6);
        assert!((v2.value - 30.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_conversation_state_upsert_roundtrip() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let conv_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let cs_repo =
            crate::infrastructure::storage::repository::SqliteConversationStateRepository::new(
                storage.pool().clone(),
            );

        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "ConvStateTest".to_string(),
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
            external_id: "u_cs_1".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv_id = conv_repo.insert(&conv).await.unwrap();

        let state = crate::domain::character::ConversationState {
            energy: 42.0,
            stress: 65.0,
            last_updated: chrono::Utc::now(),
        };
        cs_repo.upsert(char_id, conv_id, &state).await.unwrap();
        let loaded = cs_repo
            .find_by_character_and_conversation(char_id, conv_id)
            .await
            .unwrap()
            .expect("应能读回 conversation state");
        assert!((loaded.energy - 42.0).abs() < 1e-6);
        assert!((loaded.stress - 65.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_behavior_state_upsert_roundtrip() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let conv_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let bs_repo =
            crate::infrastructure::storage::repository::SqliteBehaviorStateRepository::new(
                storage.pool().clone(),
            );

        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "BehaviorStateTest".to_string(),
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
            external_id: "u_bs_1".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv_id = conv_repo.insert(&conv).await.unwrap();

        // 默认不含主动时间。
        let state = crate::domain::character::BehaviorState::default();
        bs_repo.upsert(char_id, conv_id, &state).await.unwrap();
        let loaded = bs_repo
            .find_by_character_and_conversation(char_id, conv_id)
            .await
            .unwrap()
            .expect("应能读回 behavior state");
        assert!(loaded.last_proactive_at.is_none());

        // 主动时间往返。
        let t = chrono::Utc::now() - chrono::Duration::minutes(5);
        bs_repo
            .upsert(
                char_id,
                conv_id,
                &crate::domain::character::BehaviorState {
                    last_proactive_at: Some(t),
                    last_updated: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
        let loaded2 = bs_repo
            .find_by_character_and_conversation(char_id, conv_id)
            .await
            .unwrap()
            .unwrap();
        let saved = loaded2.last_proactive_at.expect("应能读回主动时间");
        assert!((saved - t).num_seconds().abs() <= 1, "主动时间应接近写入值");
    }

    #[tokio::test]
    async fn migration_006_backfills_legacy_emotion_and_proactive() {
        // 构造一个「旧库」：只有旧 emotion_states（无 conversation_id）与
        // character_states.last_proactive_at，尚无新三张表。
        let storage = SqliteStorage::open_in_memory().await.unwrap();
        let pool = storage.pool().clone();

        sqlx::query(
            r#"CREATE TABLE characters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL, description TEXT, personality TEXT, scenario TEXT,
                style TEXT, background TEXT, greetings TEXT NOT NULL DEFAULT '[]',
                example_messages TEXT NOT NULL DEFAULT '[]', system_prompt TEXT,
                post_history_instructions TEXT, lorebook TEXT NOT NULL DEFAULT '[]',
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            r#"CREATE TABLE character_states (
                character_id INTEGER PRIMARY KEY,
                energy REAL NOT NULL DEFAULT 72.0,
                stress REAL NOT NULL DEFAULT 10.0,
                current_activity TEXT,
                last_proactive_at TEXT,
                last_updated TEXT NOT NULL DEFAULT (datetime('now'))
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            r#"CREATE TABLE conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_type TEXT NOT NULL,
                external_id TEXT NOT NULL,
                name TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            r#"CREATE TABLE conversation_bindings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                character_id INTEGER NOT NULL,
                conversation_id INTEGER NOT NULL,
                reply_mode TEXT NOT NULL DEFAULT 'mention_only',
                proactive_enabled INTEGER NOT NULL DEFAULT 0,
                mute_schedule TEXT,
                behavior_overrides TEXT NOT NULL DEFAULT '{}',
                context_policy TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            r#"CREATE TABLE emotion_states (
                character_id INTEGER NOT NULL,
                happiness REAL NOT NULL DEFAULT 0.5,
                energy REAL NOT NULL DEFAULT 0.7,
                stress REAL NOT NULL DEFAULT 0.1,
                last_updated TEXT NOT NULL DEFAULT (datetime('now'))
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();

        // 制造数据：角色 1 绑定会话 10 / 20，全局情绪 happiness=0.8、精力 0.9、压力 0.2；
        // 全局角色状态带 last_proactive_at。
        sqlx::query("INSERT INTO characters (id, name) VALUES (1, 'Alice')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO conversations (id, conversation_type, external_id) VALUES (10, 'private', 'u10'), (20, 'private', 'u20')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO conversation_bindings (character_id, conversation_id) VALUES (1, 10), (1, 20)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO emotion_states (character_id, happiness, energy, stress) VALUES (1, 0.8, 0.9, 0.2)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO character_states (character_id, energy, stress, last_proactive_at) VALUES (1, 80.0, 10.0, '2026-01-01T10:00:00Z')",
        )
        .execute(&pool)
        .await
        .unwrap();

        // 运行迁移。
        storage.migrate().await.expect("旧库迁移应成功");

        // 校验 moods 回填：character 1 的每个会话都有 mood，值为 happiness(0.8)*100 ≈ 80。
        let mood_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM moods WHERE character_id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(mood_count, 2, "每个绑定会话都应回填一个 mood");
        let mood_val: f64 = sqlx::query_scalar(
            "SELECT value FROM moods WHERE character_id = 1 AND conversation_id = 10",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            (mood_val - 80.0).abs() < 1e-6,
            "mood 应取 happiness*100，实际 {mood_val}"
        );

        // conversation_states 回填：energy 90 / stress 20。
        let cs_val: (f64, f64) = sqlx::query_as(
            "SELECT energy, stress FROM conversation_states WHERE character_id = 1 AND conversation_id = 20",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            (cs_val.0 - 90.0).abs() < 1e-6 && (cs_val.1 - 20.0).abs() < 1e-6,
            "conversation_states 应回填 energy/stress，实际 ({}, {})",
            cs_val.0,
            cs_val.1
        );

        // behavior_states 回填：last_proactive_at 从 character_states 复制到每个会话。
        let bs_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM behavior_states WHERE character_id = 1 AND last_proactive_at IS NOT NULL",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(bs_count, 2, "每个绑定会话都应回填 last_proactive_at");

        // 幂等：再次运行迁移不报错、不产生重复。
        storage.migrate().await.expect("迁移应幂等");
        let mood_after: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM moods WHERE character_id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(mood_after, 2, "重复迁移不应重复回填");
    }
}
