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
    use std::sync::Arc;

    async fn setup_storage() -> SqliteStorage {
        let storage = SqliteStorage::open_in_memory()
            .await
            .expect("failed to open in-memory db");
        storage.migrate().await.expect("failed to run migrations");
        storage
    }

    /// 构造测试用消息（C2-3 字段使用默认值）
    fn make_test_message(
        conversation_id: i64,
        sender_id: i64,
        content: MessageContent,
        timestamp: chrono::DateTime<chrono::Utc>,
        active_character_id: Option<i64>,
    ) -> Message {
        Message {
            id: 0,
            conversation_id,
            sender_id,
            content,
            timestamp,
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
            active_character_id,
            // C2-3 字段
            platform_message_id: None,
            conversation_sequence: None,
            occurred_at: None,
            ingested_at: None,
        }
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
            enabled: true,
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
    async fn latest_message_time_returns_none_for_empty_conversation() {
        let storage = setup_storage().await;
        let repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );

        let latest = repo
            .latest_message_time(999)
            .await
            .expect("查询空会话的最新消息时间不应失败");
        assert!(latest.is_none());
    }

    #[tokio::test]
    async fn reply_delivery_requires_current_outbox_lease() {
        let storage = setup_storage().await;
        let repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        let character_repo =
            crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
                storage.pool().clone(),
            );
        let character_id = character_repo
            .insert(&Character {
                id: 0,
                definition: CharacterDefinition {
                    name: "Lease Test".into(),
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
                enabled: true,
                state: CharacterState::default(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .expect("插入测试角色失败");
        let conversation_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let conversation_id = conversation_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "lease-test-conversation".into(),
                name: Some("租约测试".into()),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .expect("插入测试会话失败");
        let participant_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );
        let sender_id = participant_repo
            .insert(&Participant {
                id: 0,
                conversation_id,
                external_id: "lease-test-user".into(),
                display_name: "测试用户".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform": "onebot_qq"}),
            })
            .await
            .expect("插入测试参与者失败");
        let message_id = repo
            .insert(&make_test_message(
                conversation_id,
                sender_id,
                MessageContent::Text("测试发送租约".into()),
                chrono::Utc::now(),
                Some(character_id),
            ))
            .await
            .expect("插入测试消息失败");
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            r#"INSERT INTO outbox_events(
                   event_uuid, entity_id, entity_type, event_type, event_version,
                   payload, ordering_key, ordering_seq, created_at, available_at,
                   status, attempts, last_error, locked_until, processed_at, lease_token
               ) VALUES (?, ?, 'messages', 'MessageAccepted', 1, '{}', ?, 1, ?, ?,
                         'processing', 1, NULL, ?, NULL, ?)"#,
        )
        .bind("delivery-lease-test")
        .bind(message_id)
        .bind(format!("conversation:{conversation_id}"))
        .bind(&now)
        .bind(&now)
        .bind((chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339())
        .bind("lease-a")
        .execute(storage.pool())
        .await
        .expect("插入测试 outbox 事件失败");

        assert!(!repo
            .begin_reply_task_delivery(message_id, character_id, "lease-b")
            .await
            .expect("失效租约检查失败"));
        assert!(repo
            .reply_task(message_id)
            .await
            .expect("查询回复任务失败")
            .is_none());

        assert!(repo
            .begin_reply_task_delivery(message_id, character_id, "lease-a")
            .await
            .expect("有效租约检查失败"));
        let task = repo
            .reply_task(message_id)
            .await
            .expect("查询回复任务失败")
            .expect("回复任务未创建");
        assert_eq!(task.status, ReplyTaskStatus::Sending);
    }

    #[tokio::test]
    async fn reply_delivery_result_cannot_be_overwritten_after_lease_changes() {
        let storage = setup_storage().await;
        let repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        let character_repo =
            crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
                storage.pool().clone(),
            );
        let character_id = character_repo
            .insert(&Character {
                id: 0,
                definition: CharacterDefinition {
                    name: "Lease Result Test".into(),
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
                enabled: true,
                state: CharacterState::default(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .expect("插入测试角色失败");
        let conversation_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let conversation_id = conversation_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "lease-result-conversation".into(),
                name: Some("租约结果测试".into()),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .expect("插入测试会话失败");
        let participant_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );
        let sender_id = participant_repo
            .insert(&Participant {
                id: 0,
                conversation_id,
                external_id: "lease-result-user".into(),
                display_name: "测试用户".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform": "onebot_qq"}),
            })
            .await
            .expect("插入测试参与者失败");
        let message_id = repo
            .insert(&make_test_message(
                conversation_id,
                sender_id,
                MessageContent::Text("租约结果".into()),
                chrono::Utc::now(),
                Some(character_id),
            ))
            .await
            .expect("插入测试消息失败");
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            r#"INSERT INTO outbox_events(
                   event_uuid, entity_id, entity_type, event_type, event_version,
                   payload, ordering_key, ordering_seq, created_at, available_at,
                   status, attempts, last_error, locked_until, processed_at, lease_token
               ) VALUES (?, ?, 'messages', 'MessageAccepted', 1, '{}', ?, 1, ?, ?,
                         'processing', 1, NULL, ?, NULL, ?)
            "#,
        )
        .bind("delivery-result-lease-test")
        .bind(message_id)
        .bind(format!("conversation:{conversation_id}"))
        .bind(&now)
        .bind(&now)
        .bind((chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339())
        .bind("lease-a")
        .execute(storage.pool())
        .await
        .expect("插入测试 outbox 事件失败");

        assert!(repo
            .begin_reply_task_delivery(message_id, character_id, "lease-a")
            .await
            .expect("取得发送栅栏失败"));
        assert!(repo
            .save_reply_task_delivery_owned(
                message_id,
                character_id,
                "lease-a",
                ReplyTaskStatus::Sent,
                Some("第一条回复"),
                None,
                Some(101),
            )
            .await
            .expect("当前租约写入失败"));

        sqlx::query(
            "UPDATE outbox_events SET lease_token = ?, locked_until = ? WHERE entity_id = ?",
        )
        .bind("lease-b")
        .bind((chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339())
        .bind(message_id)
        .execute(storage.pool())
        .await
        .expect("切换测试租约失败");

        assert!(!repo
            .save_reply_task_delivery_owned(
                message_id,
                character_id,
                "lease-a",
                ReplyTaskStatus::Unknown,
                Some("旧 worker 覆盖"),
                Some("stale"),
                None,
            )
            .await
            .expect("失效租约写入检查失败"));
        let task = repo
            .reply_task(message_id)
            .await
            .expect("查询回复任务失败")
            .expect("回复任务缺失");
        assert_eq!(task.status, ReplyTaskStatus::Sent);
        assert_eq!(task.content.as_deref(), Some("第一条回复"));
        assert_eq!(task.platform_message_id, Some(101));
    }

    #[tokio::test]
    async fn history_queries_hide_internal_messages() {
        let storage = setup_storage().await;
        let character_repo =
            crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
                storage.pool().clone(),
            );
        let character_id = character_repo
            .insert(&Character {
                id: 0,
                definition: CharacterDefinition {
                    name: "History Filter".into(),
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
                enabled: true,
                state: CharacterState::default(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .expect("插入测试角色失败");
        let conversation_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let conversation_id = conversation_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "history-filter-conversation".into(),
                name: Some("历史过滤测试".into()),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .expect("插入测试会话失败");
        let participant_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );
        let sender_id = participant_repo
            .insert(&Participant {
                id: 0,
                conversation_id,
                external_id: "history-filter-user".into(),
                display_name: "测试用户".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform": "onebot_qq"}),
            })
            .await
            .expect("插入测试参与者失败");
        let message_repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        let base = chrono::Utc::now();
        for (content, metadata, offset) in [
            ("用户内容", serde_json::json!({"author_kind": "user"}), 0),
            (
                "管理命令",
                serde_json::json!({"author_kind": "system_command"}),
                1,
            ),
            (
                "平台回声",
                serde_json::json!({"author_kind": "platform_echo"}),
                2,
            ),
            (
                "主动依据",
                serde_json::json!({"author_kind": "system", "proactive": true}),
                3,
            ),
        ] {
            let mut message = make_test_message(
                conversation_id,
                sender_id,
                MessageContent::Text(content.into()),
                base + chrono::Duration::seconds(offset),
                Some(character_id),
            );
            message.metadata = metadata;
            message_repo
                .insert(&message)
                .await
                .expect("插入历史消息失败");
        }

        let recent = message_repo
            .find_recent(conversation_id, 20)
            .await
            .expect("查询普通历史失败");
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].content, MessageContent::Text("用户内容".into()));

        let cognition = message_repo
            .find_cognition_batch(character_id, conversation_id, 0, 20)
            .await
            .expect("查询认知批次失败");
        assert_eq!(cognition.len(), 1);
        assert_eq!(
            cognition[0].content,
            MessageContent::Text("用户内容".into())
        );
    }

    #[tokio::test]
    async fn conversations_share_external_id_across_types_but_not_within_type() {
        let storage = setup_storage().await;
        let repo = crate::infrastructure::storage::repository::SqliteConversationRepository::new(
            storage.pool().clone(),
        );
        let now = chrono::Utc::now();
        let group_id = repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "same-platform-id".into(),
                name: Some("群聊".into()),
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        let private_id = repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Private,
                external_id: "same-platform-id".into(),
                name: Some("私聊".into()),
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        assert_ne!(group_id, private_id);

        let duplicate = repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "same-platform-id".into(),
                name: None,
                created_at: now,
                updated_at: now,
            })
            .await;
        assert!(duplicate.is_err());
    }

    #[tokio::test]
    async fn migration_rejects_duplicate_conversation_identity_before_replacing_index() {
        let storage = setup_storage().await;
        sqlx::query("DROP INDEX idx_conversations_external_type")
            .execute(storage.pool())
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO conversations(conversation_type,external_id) VALUES('group','duplicate-identity'),('group','duplicate-identity')",
        )
        .execute(storage.pool())
        .await
        .unwrap();

        let error = storage.migrate().await.unwrap_err().to_string();
        assert!(error.contains("重复会话身份"), "实际错误：{error}");
        let index_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_conversations_external_type'",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(index_count, 0, "迁移失败时不应伪造新索引已创建");
    }

    #[tokio::test]
    async fn test_allocate_sequence_uses_sqlite_atomic_update() {
        let storage = setup_storage().await;
        let repo = crate::infrastructure::storage::repository::SqliteConversationRepository::new(
            storage.pool().clone(),
        );
        let conversation_id = repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "sequence-test".into(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();

        assert_eq!(repo.allocate_sequence(conversation_id).await.unwrap(), 1);
        assert_eq!(repo.allocate_sequence(conversation_id).await.unwrap(), 2);
        let error = repo.allocate_sequence(i64::MAX).await.unwrap_err();
        assert!(matches!(error, crate::error::RepositoryError::NotFound(_)));
    }

    #[tokio::test]
    async fn test_platform_reply_id_resolves_to_local_reply_foreign_key() {
        let storage = setup_storage().await;
        let conversation_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let message_repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        let character_id: i64 =
            sqlx::query_scalar("INSERT INTO characters(name) VALUES('引用测试角色') RETURNING id")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let conversation_id = conversation_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "reply-reference-test".into(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let sender_id: i64 = sqlx::query_scalar(
            "INSERT INTO participants(conversation_id,external_id,display_name,role,metadata) VALUES(?,?,?,?,?) RETURNING id",
        )
        .bind(conversation_id)
        .bind("reply-user")
        .bind("引用用户")
        .bind("user")
        .bind("{}")
        .fetch_one(storage.pool())
        .await
        .unwrap();

        let timestamp = chrono::Utc::now();
        let parent = message_repo
            .accept_message(&AcceptedMessageInput {
                conversation_id,
                sender_id,
                content_json: serde_json::json!({"Text":"原消息"}).to_string(),
                timestamp: timestamp.to_rfc3339(),
                platform_message_id: Some(701),
                character_id,
                event_uuid: "reply-parent".into(),
                outbox_payload: serde_json::json!({
                    "event_uuid":"reply-parent",
                    "conversation_id":conversation_id,
                    "sender_id":sender_id,
                    "content":"原消息",
                    "timestamp":timestamp,
                    "character_id":character_id,
                    "metadata":{"author_kind":"user"}
                })
                .to_string(),
            })
            .await
            .unwrap();
        let child = message_repo
            .accept_message(&AcceptedMessageInput {
                conversation_id,
                sender_id,
                content_json: serde_json::json!({"Text":"回复原消息"}).to_string(),
                timestamp: (timestamp + chrono::Duration::seconds(1)).to_rfc3339(),
                platform_message_id: Some(702),
                character_id,
                event_uuid: "reply-child".into(),
                outbox_payload: serde_json::json!({
                    "event_uuid":"reply-child",
                    "conversation_id":conversation_id,
                    "sender_id":sender_id,
                    "content":"回复原消息",
                    "timestamp":timestamp,
                    "character_id":character_id,
                    "metadata":{"author_kind":"user","reply_to_platform_message_id":701}
                })
                .to_string(),
            })
            .await
            .unwrap();

        let local_reply_to: Option<i64> =
            sqlx::query_scalar("SELECT reply_to FROM messages WHERE id=?")
                .bind(child.0)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(local_reply_to, Some(parent.0));
        let metadata: String = sqlx::query_scalar("SELECT metadata FROM messages WHERE id=?")
            .bind(child.0)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&metadata).unwrap()
                ["reply_to_platform_message_id"],
            701
        );
    }

    #[tokio::test]
    async fn test_accept_message_is_idempotent_by_platform_identity() {
        let storage = setup_storage().await;
        let conv_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let msg_repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        sqlx::query("INSERT INTO characters(name) VALUES('测试角色')")
            .execute(storage.pool())
            .await
            .unwrap();
        let conversation_id = conv_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "dedup-group".into(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        sqlx::query("INSERT INTO participants(conversation_id,external_id,display_name,role,metadata) VALUES(?,?,?,?,?)")
            .bind(conversation_id).bind("1").bind("测试用户").bind("user").bind("{}")
            .execute(storage.pool()).await.unwrap();
        let now = chrono::Utc::now();
        let payload = serde_json::json!({"event_uuid":"dedup-event","conversation_id":conversation_id,"sender_id":1,"message_id":0,"content":"同一条","timestamp":now,"is_mentioned":false,"character_id":1,"conversation_sequence":0,"platform_message_id":Some(42i64),"ingested_at":now});
        let first = msg_repo
            .accept_message(&crate::domain::repository::AcceptedMessageInput {
                conversation_id,
                sender_id: 1,
                content_json: serde_json::json!({"Text":"同一条"}).to_string(),
                timestamp: now.to_rfc3339(),
                platform_message_id: Some(42),
                character_id: 1,
                event_uuid: "dedup-event".to_string(),
                outbox_payload: payload.to_string(),
            })
            .await
            .unwrap();
        let second = msg_repo
            .accept_message(&crate::domain::repository::AcceptedMessageInput {
                conversation_id,
                sender_id: 1,
                content_json: serde_json::json!({"Text":"同一条"}).to_string(),
                timestamp: now.to_rfc3339(),
                platform_message_id: Some(42),
                character_id: 1,
                event_uuid: "dedup-event-duplicate".to_string(),
                outbox_payload: payload.to_string(),
            })
            .await
            .unwrap();
        assert_eq!(first.0, second.0);
        assert_eq!(first.1, second.1);
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE conversation_id=?")
                .bind(conversation_id)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let events: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events WHERE ordering_key=?")
                .bind(conversation_id.to_string())
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(count, 1);
        assert_eq!(events, 1);
    }

    #[tokio::test]
    async fn find_after_sequence_is_not_displaced_by_old_history_noise() {
        let storage = setup_storage().await;
        let conversation_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let message_repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        let character_id: i64 =
            sqlx::query_scalar("INSERT INTO characters(name) VALUES('批次冻结角色') RETURNING id")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let conversation_id = conversation_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "batch-freeze-noise".into(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let sender_id: i64 = sqlx::query_scalar(
            "INSERT INTO participants(conversation_id,external_id,display_name,role,metadata) VALUES(?,?,?,?,?) RETURNING id",
        )
        .bind(conversation_id)
        .bind("batch-user")
        .bind("批次用户")
        .bind("user")
        .bind("{}")
        .fetch_one(storage.pool())
        .await
        .unwrap();

        let pool = storage.pool().clone();
        let accept = |sequence_hint: i64, text: &str, platform_message_id: i64| {
            let pool = pool.clone();
            let text = text.to_string();
            async move {
                let now = chrono::Utc::now();
                let repo =
                    crate::infrastructure::storage::repository::SqliteMessageRepository::new(pool);
                let metadata = serde_json::json!({
                    "author_kind": "user",
                    "external_sender_id": "batch-user",
                });
                let payload = serde_json::json!({
                    "event_uuid": format!("batch-freeze-{sequence_hint}"),
                    "conversation_id": conversation_id,
                    "sender_id": sender_id,
                    "message_id": 0,
                    "content": text,
                    "timestamp": now,
                    "is_mentioned": false,
                    "character_id": character_id,
                    "conversation_sequence": 0,
                    "platform_message_id": platform_message_id,
                    "ingested_at": now,
                    "metadata": metadata,
                });
                repo.accept_message(&AcceptedMessageInput {
                    conversation_id,
                    sender_id,
                    content_json: serde_json::json!({"Text": text}).to_string(),
                    timestamp: now.to_rfc3339(),
                    platform_message_id: Some(platform_message_id),
                    character_id,
                    event_uuid: format!("batch-freeze-event-{sequence_hint}"),
                    outbox_payload: payload.to_string(),
                })
                .await
                .unwrap()
            }
        };

        // 旧历史超过原先的“最近 20 条”窗口，不能影响当前消息之后的候选。
        for index in 0..30 {
            accept(index, &format!("旧历史 {index}"), 10_000 + index).await;
        }
        let current = accept(30, "当前消息", 10_030).await;
        let next = accept(31, "后续一", 10_031).await;
        let next_two = accept(32, "后续二", 10_032).await;

        let messages = message_repo
            .find_after_sequence(conversation_id, current.1, 20)
            .await
            .unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].id, next.0);
        assert_eq!(messages[1].id, next_two.0);
        assert!(messages
            .iter()
            .all(|message| message.conversation_sequence.unwrap() > current.1));
    }

    #[tokio::test]
    async fn message_acceptance_publishes_one_canonical_event_for_duplicate_delivery() {
        let storage = setup_storage().await;
        let conv_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let msg_repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        let character_id: i64 =
            sqlx::query_scalar("INSERT INTO characters(name) VALUES('事件角色') RETURNING id")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let conversation_id = conv_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Private,
                external_id: "canonical-event".into(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let participant_id: i64 = sqlx::query_scalar("INSERT INTO participants(conversation_id,external_id,display_name,role,metadata) VALUES(?,?,?,?,?) RETURNING id")
            .bind(conversation_id)
            .bind("user-1")
            .bind("用户")
            .bind("user")
            .bind("{}")
            .fetch_one(storage.pool())
            .await
            .unwrap();

        let bus = crate::application::event_bus::EventBus::new();
        let mut subscription = bus.subscribe();
        let service = crate::application::message_acceptance::MessageAcceptanceService::new(
            std::sync::Arc::new(msg_repo),
            bus,
        );
        let timestamp = chrono::Utc::now();
        let metadata = serde_json::json!({
            "author_kind": "user",
            "external_sender_id": "user-1",
            "display_name": "用户",
        });

        service
            .accept_inbound(
                conversation_id,
                participant_id,
                "你好".into(),
                timestamp,
                Some(9001),
                character_id,
                false,
                metadata.clone(),
            )
            .await
            .unwrap();
        service
            .accept_inbound(
                conversation_id,
                participant_id,
                "你好".into(),
                timestamp,
                Some(9001),
                character_id,
                false,
                metadata,
            )
            .await
            .unwrap();

        let event = subscription.recv().await.expect("首次接受应发布事件");
        match event {
            crate::domain::event::CoreEvent::MessageAccepted(event) => {
                assert!(event.message_id > 0);
                assert_eq!(event.conversation_sequence, 1);
                assert_eq!(event.content, "你好");
            }
            other => panic!("应发布 MessageAccepted，实际 {other:?}"),
        }
        assert!(
            subscription.try_recv().is_none(),
            "重复投递不得再次发布事件"
        );
    }

    #[tokio::test]
    async fn command_receipt_and_platform_echo_are_idempotent() {
        let storage = setup_storage().await;
        let repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        assert!(repo.claim_command_message(7, 100).await.unwrap());
        assert!(!repo.claim_command_message(7, 100).await.unwrap());
        assert!(repo.claim_command_message(8, 100).await.unwrap());

        let metadata = serde_json::json!({"author_kind":"platform_echo"});
        repo.record_platform_echo(7, 200, "bot-1", "已发送", &metadata)
            .await
            .unwrap();
        repo.record_platform_echo(7, 200, "bot-1", "重复回声", &metadata)
            .await
            .unwrap();
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM platform_message_echoes WHERE conversation_id=7 AND platform_message_id=200",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(count, 1);
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
        let msg = make_test_message(
            conv_id,
            part_id,
            MessageContent::Text("Hello world".to_string()),
            chrono::Utc::now(),
            None,
        );
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
    async fn test_reply_task_unknown_preserves_content_and_error() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let conv_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let msg_repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        let character_id = char_repo
            .insert(&Character {
                id: 0,
                definition: CharacterDefinition {
                    name: "ReplyTask".into(),
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
                enabled: true,
                state: CharacterState::default(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let conversation_id = conv_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Private,
                external_id: "reply-task-test".into(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let sender_id: i64 = sqlx::query_scalar("INSERT INTO participants(conversation_id,external_id,display_name,role,metadata) VALUES(?,?,?,?,?) RETURNING id")
            .bind(conversation_id).bind("u1").bind("用户").bind("user").bind("{}").fetch_one(storage.pool()).await.unwrap();
        let message_id = msg_repo
            .insert(&make_test_message(
                conversation_id,
                sender_id,
                MessageContent::Text("hello".into()),
                chrono::Utc::now(),
                Some(character_id),
            ))
            .await
            .unwrap();
        msg_repo
            .save_reply_task_error(
                message_id,
                character_id,
                ReplyTaskStatus::Unknown,
                Some("reply"),
                Some("发送结果未知"),
            )
            .await
            .unwrap();
        let task = msg_repo.reply_task(message_id).await.unwrap().unwrap();
        assert_eq!(task.status, ReplyTaskStatus::Unknown);
        assert_eq!(task.content.as_deref(), Some("reply"));
        assert_eq!(task.error.as_deref(), Some("发送结果未知"));

        assert!(msg_repo
            .confirm_unknown_reply_task(message_id, 7001)
            .await
            .unwrap());
        let confirmed = msg_repo.reply_task(message_id).await.unwrap().unwrap();
        assert_eq!(confirmed.status, ReplyTaskStatus::Sent);
        assert_eq!(confirmed.platform_message_id, Some(7001));
        assert!(msg_repo
            .assistant_history_exists(conversation_id, message_id)
            .await
            .unwrap());
        let assistant_sequence: Option<i64> = sqlx::query_scalar(
            "SELECT conversation_sequence FROM messages WHERE conversation_id = ? AND json_extract(metadata, '$.reply_task') = ? AND json_extract(metadata, '$.author_kind') = 'assistant'",
        )
        .bind(conversation_id)
        .bind(message_id)
        .fetch_optional(storage.pool())
        .await
        .unwrap();
        assert_eq!(
            assistant_sequence,
            Some(1),
            "确认路径必须分配新的会话顺序号"
        );
    }

    #[tokio::test]
    async fn test_reply_task_sending_recovery_is_atomic_and_preserves_content() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let conv_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let msg_repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        let character_id = char_repo
            .insert(&Character {
                id: 0,
                definition: CharacterDefinition {
                    name: "SendingRecovery".into(),
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
                enabled: true,
                state: CharacterState::default(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let conversation_id = conv_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Private,
                external_id: "sending-recovery-test".into(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let sender_id: i64 = sqlx::query_scalar("INSERT INTO participants(conversation_id,external_id,display_name,role,metadata) VALUES(?,?,?,?,?) RETURNING id")
            .bind(conversation_id)
            .bind("u1")
            .bind("用户")
            .bind("user")
            .bind("{}")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let message_id = msg_repo
            .insert(&make_test_message(
                conversation_id,
                sender_id,
                MessageContent::Text("hello".into()),
                chrono::Utc::now(),
                Some(character_id),
            ))
            .await
            .unwrap();
        msg_repo
            .save_reply_task(
                message_id,
                character_id,
                ReplyTaskStatus::Sending,
                Some("已经生成的回复"),
            )
            .await
            .unwrap();

        assert!(msg_repo
            .mark_reply_task_unknown_if_sending(
                message_id,
                character_id,
                "进程在发送平台回执前退出，发送结果未知，需要核查",
            )
            .await
            .unwrap());
        assert!(!msg_repo
            .mark_reply_task_unknown_if_sending(message_id, character_id, "不应覆盖")
            .await
            .unwrap());

        let task = msg_repo.reply_task(message_id).await.unwrap().unwrap();
        assert_eq!(task.status, ReplyTaskStatus::Unknown);
        assert_eq!(task.content.as_deref(), Some("已经生成的回复"));
        assert_eq!(
            task.error.as_deref(),
            Some("进程在发送平台回执前退出，发送结果未知，需要核查")
        );
    }

    #[tokio::test]
    async fn assistant_history_lookup_is_exact_not_window_limited() {
        let storage = setup_storage().await;
        let msg_repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        let conversation_id: i64 = sqlx::query_scalar(
            "INSERT INTO conversations(conversation_type,external_id) VALUES('private','history-lookup') RETURNING id",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let sender_id: i64 = sqlx::query_scalar(
            "INSERT INTO participants(conversation_id,external_id,display_name,role,metadata) VALUES(?,?,?,?,?) RETURNING id",
        )
        .bind(conversation_id)
        .bind("assistant").bind("角色").bind("character").bind("{}").fetch_one(storage.pool()).await.unwrap();
        sqlx::query(
            "INSERT INTO messages(conversation_id,sender_id,content,timestamp,metadata) VALUES(?,?,?,?,?)",
        )
        .bind(conversation_id)
        .bind(sender_id)
        .bind(serde_json::json!({"Text":"回复"}).to_string())
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(serde_json::json!({"author_kind":"assistant","reply_task":9001}).to_string())
        .execute(storage.pool())
        .await
        .unwrap();

        assert!(msg_repo
            .assistant_history_exists(conversation_id, 9001)
            .await
            .unwrap());
        assert!(!msg_repo
            .assistant_history_exists(conversation_id, 9002)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_requeue_unknown_reply_task_requires_unknown_and_requeues_event() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let conv_repo =
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                storage.pool().clone(),
            );
        let msg_repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        let character_id = char_repo
            .insert(&Character {
                id: 0,
                definition: CharacterDefinition {
                    name: "RetryTask".into(),
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
                enabled: true,
                state: CharacterState::default(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let conversation_id = conv_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Private,
                external_id: "retry-task-test".into(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let sender_id: i64 = sqlx::query_scalar("INSERT INTO participants(conversation_id,external_id,display_name,role,metadata) VALUES(?,?,?,?,?) RETURNING id")
            .bind(conversation_id).bind("u2").bind("用户").bind("user").bind("{}").fetch_one(storage.pool()).await.unwrap();
        let message_id = msg_repo
            .insert(&make_test_message(
                conversation_id,
                sender_id,
                MessageContent::Text("retry".into()),
                chrono::Utc::now(),
                Some(character_id),
            ))
            .await
            .unwrap();
        msg_repo
            .save_reply_task_error(
                message_id,
                character_id,
                ReplyTaskStatus::Unknown,
                Some("reply"),
                Some("超时"),
            )
            .await
            .unwrap();
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query("INSERT INTO outbox_events(event_uuid,entity_id,entity_type,event_type,event_version,payload,ordering_key,ordering_seq,created_at,available_at,status,attempts,last_error,locked_until,processed_at,lease_token) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind("retry-task-event")
            .bind(message_id)
            .bind("messages")
            .bind("MessageAccepted")
            .bind(1i32)
            .bind("{}")
            .bind(format!("conversation:{conversation_id}"))
            .bind(1i64)
            .bind(&now)
            .bind(&now)
            .bind("completed")
            .bind(3i32)
            .bind("unknown")
            .bind(Option::<String>::None)
            .bind(&now)
            .bind(Option::<String>::None)
            .execute(storage.pool())
            .await
            .unwrap();

        assert!(msg_repo
            .requeue_unknown_reply_task(message_id)
            .await
            .unwrap());
        let task = msg_repo.reply_task(message_id).await.unwrap().unwrap();
        assert_eq!(task.status, ReplyTaskStatus::Generated);
        assert_eq!(task.content.as_deref(), Some("reply"));
        let (status, attempts): (String, i32) = sqlx::query_as(
            "SELECT status, attempts FROM outbox_events WHERE event_uuid='retry-task-event'",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(status, "pending");
        assert_eq!(attempts, 0);
        assert!(!msg_repo
            .requeue_unknown_reply_task(message_id)
            .await
            .unwrap());
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
            enabled: true,
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

        // 乐观并发更新：旧快照不能覆盖已经提交的新内容。
        let expected = memories[0].clone();
        let mut committed = expected.clone();
        committed.content = "User likes dogs".to_string();
        assert!(mem_repo
            .update_if_unchanged(&committed, &expected)
            .await
            .unwrap());
        let mut stale = expected.clone();
        stale.content = "stale overwrite".to_string();
        assert!(!mem_repo
            .update_if_unchanged(&stale, &expected)
            .await
            .unwrap());
        assert_eq!(
            mem_repo.find_by_id(mem_id).await.unwrap().unwrap().content,
            "User likes dogs"
        );
    }

    #[tokio::test]
    async fn pending_memory_edit_requests_survive_restart_scan() {
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
                name: "PendingEditTest".to_string(),
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
            enabled: true,
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let character_id = char_repo.insert(&character).await.unwrap();
        let mut memory = Memory::new(
            character_id,
            None,
            MemoryType::Semantic,
            "用户喜欢猫".to_string(),
            0.8,
        );
        memory.metadata["edit_request"] = serde_json::json!({
            "instruction": "改成用户喜欢狗",
            "status": "pending",
            "requested_at": "2026-10-17T00:00:00Z"
        });
        let id = mem_repo.insert(&memory).await.unwrap();

        let pending = mem_repo.find_pending_edit_requests(10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id);
        assert_eq!(pending[0].content, "用户喜欢猫");
        assert_eq!(pending[0].metadata["edit_request"]["status"], "pending");
    }

    #[tokio::test]
    async fn memory_visibility_follows_global_user_across_conversations() {
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
        let memory_repo = crate::infrastructure::storage::repository::SqliteMemoryRepository::new(
            storage.pool().clone(),
        );
        let character_id = char_repo
            .insert(&Character {
                id: 0,
                definition: CharacterDefinition {
                    name: "跨会话记忆".into(),
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
                enabled: true,
                state: CharacterState::default(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let conversation_a = conv_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "memory-a".into(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let conversation_b = conv_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "memory-b".into(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let participant_a = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id: conversation_a,
                external_id: "800001".into(),
                display_name: "小明".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform":"onebot_qq"}),
            })
            .await
            .unwrap();
        let participant_b = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id: conversation_b,
                external_id: "800001".into(),
                display_name: "小明（群B）".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform":"onebot_qq"}),
            })
            .await
            .unwrap();
        let participant_c = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id: conversation_a,
                external_id: "800002".into(),
                display_name: "小红".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform":"onebot_qq"}),
            })
            .await
            .unwrap();
        let participant_d = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id: conversation_b,
                external_id: "800003".into(),
                display_name: "小红（群B）".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform":"onebot_qq"}),
            })
            .await
            .unwrap();
        let mut memory = Memory::new(
            character_id,
            Some(conversation_a),
            MemoryType::Semantic,
            "小明喜欢猫".into(),
            0.9,
        );
        memory.metadata =
            serde_json::json!({"source_participant_id": participant_a, "shared": true});
        memory_repo.insert(&memory).await.unwrap();

        let mut private_memory = Memory::new(
            character_id,
            Some(conversation_a),
            MemoryType::Episodic,
            "小明在群A提到的具体经历".into(),
            0.9,
        );
        private_memory.metadata =
            serde_json::json!({"source_participant_id": participant_a, "shared": false});
        memory_repo.insert(&private_memory).await.unwrap();

        let mut other_user_memory = Memory::new(
            character_id,
            Some(conversation_a),
            MemoryType::Semantic,
            "小红喜欢狗".into(),
            0.9,
        );
        other_user_memory.metadata =
            serde_json::json!({"source_participant_id": participant_c, "shared": true});
        memory_repo.insert(&other_user_memory).await.unwrap();

        let mut local_memory = Memory::new(
            character_id,
            Some(conversation_b),
            MemoryType::Episodic,
            "群B的共同经历".into(),
            0.9,
        );
        local_memory.metadata =
            serde_json::json!({"source_participant_id": participant_d, "shared": false});
        memory_repo.insert(&local_memory).await.unwrap();

        let visible = memory_repo
            .find_visible_for_participant(character_id, conversation_b, participant_b, 10)
            .await
            .unwrap();
        let visible_content = visible
            .iter()
            .map(|memory| memory.content.as_str())
            .collect::<Vec<_>>();
        assert_eq!(visible_content.len(), 2);
        assert!(visible_content.contains(&"小明喜欢猫"));
        assert!(visible_content.contains(&"群B的共同经历"));
        assert!(!visible_content.contains(&"小明在群A提到的具体经历"));
        assert!(!visible_content.contains(&"小红喜欢狗"));

        let other_user_visible = memory_repo
            .find_visible_for_participant(character_id, conversation_b, participant_d, 10)
            .await
            .unwrap();
        let other_user_content = other_user_visible
            .iter()
            .map(|memory| memory.content.as_str())
            .collect::<Vec<_>>();
        assert_eq!(other_user_content, vec!["群B的共同经历"]);

        // 没有用户身份上下文的接口只能读取本会话记忆。
        let plugin_visible = memory_repo
            .find_visible(character_id, conversation_b, 10)
            .await
            .unwrap();
        assert_eq!(plugin_visible.len(), 1);
        assert_eq!(plugin_visible[0].content, "群B的共同经历");
        let stored_user_id: Option<i64> =
            sqlx::query_scalar("SELECT user_id FROM memories WHERE content='小明喜欢猫'")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert!(stored_user_id.is_some());

        // 向量检索必须使用同一套会话与共享边界，不能因为走了 semantic_memories
        // 就绕过普通记忆查询的可见性规则。
        let user_a = stored_user_id.unwrap();
        memory_repo
            .insert_semantic(
                character_id,
                Some(conversation_a),
                "semantic",
                "向量共享事实",
                &[1.0, 0.0],
                0.9,
                &serde_json::json!({"shared": true, "user_id": user_a}).to_string(),
            )
            .await
            .unwrap();
        memory_repo
            .insert_semantic(
                character_id,
                Some(conversation_a),
                "semantic",
                "向量私密经历",
                &[1.0, 0.0],
                0.9,
                &serde_json::json!({"shared": false, "user_id": user_a}).to_string(),
            )
            .await
            .unwrap();

        let vector_cross_session = memory_repo
            .search_visible_by_embedding(
                character_id,
                conversation_b,
                Some(user_a),
                &[1.0, 0.0],
                None,
                10,
            )
            .await
            .unwrap();
        assert_eq!(
            vector_cross_session
                .iter()
                .map(|item| item.memory.content.as_str())
                .collect::<Vec<_>>(),
            vec!["向量共享事实"]
        );

        // 用户请求本身只留下待确认记录，不应立即改变跨会话读取范围。
        memory_repo
            .request_sharing_policy(character_id, 1, Some("不要带到其他会话"))
            .await
            .unwrap();
        let still_visible = memory_repo
            .find_visible_for_participant(character_id, conversation_b, participant_b, 10)
            .await
            .unwrap();
        assert!(still_visible
            .iter()
            .any(|memory| memory.content == "小明喜欢猫"));

        // 管理员确认后，只阻止未来的跨会话检索；来源会话仍然可读。
        memory_repo
            .confirm_sharing_policy(character_id, 1, Some("确认隐私禁用"))
            .await
            .unwrap();
        let blocked = memory_repo
            .find_visible_for_participant(character_id, conversation_b, participant_b, 10)
            .await
            .unwrap();
        assert!(!blocked.iter().any(|memory| memory.content == "小明喜欢猫"));
        let vector_blocked = memory_repo
            .search_visible_by_embedding(
                character_id,
                conversation_b,
                Some(user_a),
                &[1.0, 0.0],
                None,
                10,
            )
            .await
            .unwrap();
        assert!(vector_blocked.is_empty());
        let source_visible = memory_repo
            .find_visible_for_participant(character_id, conversation_a, participant_a, 10)
            .await
            .unwrap();
        assert!(source_visible
            .iter()
            .any(|memory| memory.content == "小明喜欢猫"));

        // 取消策略后重新允许未来跨会话读取。
        memory_repo
            .cancel_sharing_policy(character_id, 1, Some("用户撤回禁用"))
            .await
            .unwrap();
        let restored = memory_repo
            .find_visible_for_participant(character_id, conversation_b, participant_b, 10)
            .await
            .unwrap();
        assert!(restored.iter().any(|memory| memory.content == "小明喜欢猫"));
    }

    #[tokio::test]
    async fn test_candidate_memory_is_idempotent_and_hidden_from_normal_reads() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let memory_repo = Arc::new(
            crate::infrastructure::storage::repository::SqliteMemoryRepository::new(
                storage.pool().clone(),
            ),
        );
        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "CandidateTest".to_string(),
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
            enabled: true,
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let character_id = char_repo.insert(&character).await.unwrap();
        let service = crate::application::memory_service::MemoryService::new(memory_repo.clone());
        let occurred_at = chrono::Utc::now();

        assert_eq!(
            service
                .extract_and_store(
                    character_id,
                    None,
                    7,
                    "我喜欢喝咖啡，每天都要来一杯",
                    9001,
                    occurred_at,
                )
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            service
                .extract_and_store(
                    character_id,
                    None,
                    7,
                    "我喜欢喝咖啡，每天都要来一杯",
                    9001,
                    occurred_at,
                )
                .await
                .unwrap(),
            0,
            "同一来源消息重试不得重复生成候选"
        );

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM memories WHERE character_id = ? AND json_extract(metadata, '$.status') = 'candidate'",
        )
        .bind(character_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(count, 1);
        assert!(
            memory_repo
                .find_by_character_id(character_id, None, 10)
                .await
                .unwrap()
                .is_empty(),
            "候选记忆不得进入普通读取路径"
        );
    }

    #[tokio::test]
    async fn safe_statement_candidates_are_promoted_with_audit_metadata() {
        let storage = setup_storage().await;
        let memory_repo = crate::infrastructure::storage::repository::SqliteMemoryRepository::new(
            storage.pool().clone(),
        );
        let character_id: i64 =
            sqlx::query_scalar("INSERT INTO characters(name) VALUES('候选确认') RETURNING id")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let mut candidate = Memory::new(
            character_id,
            None,
            MemoryType::Semantic,
            "我喜欢喝咖啡".to_string(),
            0.7,
        );
        candidate.metadata = serde_json::json!({
            "kind": "statement",
            "status": "candidate",
            "source_message_id": 42,
            "candidate_confidence": 0.9,
            "shared": true,
        });
        memory_repo.insert(&candidate).await.unwrap();

        assert_eq!(memory_repo.promote_safe_candidates(10).await.unwrap(), 1);
        let stored = memory_repo
            .find_by_character_id(character_id, None, 10)
            .await
            .unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].metadata["status"], "formal");
        assert_eq!(stored[0].metadata["verification"], "program_confirmed");
        assert!(stored[0].metadata["confirmed_at"].is_string());

        // 推断候选不走默认确认路径。
        let mut inference = Memory::new(
            character_id,
            None,
            MemoryType::Semantic,
            "这可能说明角色更信任用户".to_string(),
            0.9,
        );
        inference.metadata = serde_json::json!({
            "kind": "inference",
            "status": "candidate",
            "source_message_id": 43,
        });
        memory_repo.insert(&inference).await.unwrap();
        assert_eq!(memory_repo.promote_safe_candidates(10).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn promoted_candidate_remains_idempotent_for_same_source() {
        let storage = setup_storage().await;
        let memory_repo = crate::infrastructure::storage::repository::SqliteMemoryRepository::new(
            storage.pool().clone(),
        );
        let character_id: i64 =
            sqlx::query_scalar("INSERT INTO characters(name) VALUES('来源幂等') RETURNING id")
                .fetch_one(storage.pool())
                .await
                .unwrap();

        let mut candidate = Memory::new(
            character_id,
            None,
            MemoryType::Semantic,
            "我喜欢茶".to_string(),
            0.8,
        );
        candidate.metadata = serde_json::json!({
            "kind": "statement",
            "status": "candidate",
            "source_message_id": 9001,
            "candidate_confidence": 0.95,
            "shared": true,
        });

        let first_id = memory_repo
            .insert_candidate_once(&candidate)
            .await
            .unwrap()
            .expect("首次处理应创建候选");
        assert_eq!(memory_repo.promote_safe_candidates(10).await.unwrap(), 1);

        let retry = memory_repo.insert_candidate_once(&candidate).await.unwrap();
        assert!(retry.is_none(), "正式记忆仍应阻止同来源重试");
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM memories WHERE character_id=? AND json_extract(metadata, '$.source_message_id')= ?",
        )
        .bind(character_id)
        .bind(9001_i64)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(count, 1);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT json_extract(metadata, '$.status') FROM memories WHERE id=?"
            )
            .bind(first_id)
            .fetch_one(storage.pool())
            .await
            .unwrap(),
            "formal"
        );
    }

    #[tokio::test]
    async fn relationship_interactions_are_idempotent_across_sessions_for_one_user() {
        let storage = setup_storage().await;
        let char_id: i64 =
            sqlx::query_scalar("INSERT INTO characters(name) VALUES('跨会话关系') RETURNING id")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let conv_a: i64 = sqlx::query_scalar("INSERT INTO conversations(conversation_type,external_id) VALUES('group','interaction-a') RETURNING id")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let conv_b: i64 = sqlx::query_scalar("INSERT INTO conversations(conversation_type,external_id) VALUES('group','interaction-b') RETURNING id")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let part_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );
        let rel_repo =
            crate::infrastructure::storage::repository::SqliteRelationshipRepository::new(
                storage.pool().clone(),
            );
        let msg_repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        let user_a = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id: conv_a,
                external_id: "interaction-user".into(),
                display_name: "用户 A".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform":"onebot_qq"}),
            })
            .await
            .unwrap();
        let user_b = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id: conv_b,
                external_id: "interaction-user".into(),
                display_name: "用户 B".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform":"onebot_qq"}),
            })
            .await
            .unwrap();
        let message_a = msg_repo
            .insert(&make_test_message(
                conv_a,
                user_a,
                MessageContent::Text("第一条".into()),
                chrono::Utc::now(),
                Some(char_id),
            ))
            .await
            .unwrap();
        let message_b = msg_repo
            .insert(&make_test_message(
                conv_b,
                user_b,
                MessageContent::Text("第二条".into()),
                chrono::Utc::now(),
                Some(char_id),
            ))
            .await
            .unwrap();

        assert!(rel_repo
            .record_interaction_once(char_id, user_a, message_a)
            .await
            .unwrap());
        assert!(rel_repo
            .record_interaction_once(char_id, user_b, message_b)
            .await
            .unwrap());
        assert!(!rel_repo
            .record_interaction_once(char_id, user_b, message_b)
            .await
            .unwrap());

        let relationship = rel_repo.find(char_id, user_b).await.unwrap().unwrap();
        assert_eq!(relationship.interaction_count, 2);
        let interaction_rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM relationship_interactions WHERE character_id=?",
        )
        .bind(char_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(interaction_rows, 2);
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
            enabled: true,
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
            enabled: true,
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
    async fn test_relationship_interaction_is_idempotent_by_source_message() {
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
        let msg_repo = crate::infrastructure::storage::repository::SqliteMessageRepository::new(
            storage.pool().clone(),
        );
        let rel_repo =
            crate::infrastructure::storage::repository::SqliteRelationshipRepository::new(
                storage.pool().clone(),
            );
        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "Idempotent".to_string(),
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
            enabled: true,
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let character_id = char_repo.insert(&character).await.unwrap();
        let conversation_id = conv_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Private,
                external_id: "idempotent-user".to_string(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let participant_id = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id,
                external_id: "idempotent-user".to_string(),
                display_name: "用户".to_string(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();
        let message_id = msg_repo
            .insert(&make_test_message(
                conversation_id,
                participant_id,
                MessageContent::Text("你好".to_string()),
                chrono::Utc::now(),
                Some(character_id),
            ))
            .await
            .unwrap();

        assert!(rel_repo
            .record_interaction_once(character_id, participant_id, message_id)
            .await
            .unwrap());
        assert!(!rel_repo
            .record_interaction_once(character_id, participant_id, message_id)
            .await
            .unwrap());
        assert_eq!(
            rel_repo
                .find(character_id, participant_id)
                .await
                .unwrap()
                .unwrap()
                .interaction_count,
            1
        );
    }

    #[tokio::test]
    async fn test_relationship_lookup_by_stable_user_key_across_conversations() {
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
        let character = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "StableUserChar".to_string(),
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
            enabled: true,
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let character_id = char_repo.insert(&character).await.unwrap();
        let conversation_a = conv_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "group-a".to_string(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let participant_a = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id: conversation_a,
                external_id: "900001".to_string(),
                display_name: "小明".to_string(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"stable_user_key":"900001"}),
            })
            .await
            .unwrap();
        rel_repo
            .create_for_participant(character_id, participant_a)
            .await
            .unwrap();
        let conversation_b = conv_repo
            .insert(&Conversation {
                id: 0,
                conversation_type: ConversationType::Group,
                external_id: "group-b".to_string(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let participant_b = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id: conversation_b,
                external_id: "900001".to_string(),
                display_name: "小明（群B）".to_string(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"stable_user_key":"900001"}),
            })
            .await
            .unwrap();
        let found = rel_repo
            .find_by_stable_user_key(character_id, "900001")
            .await
            .unwrap()
            .expect("同一平台用户应找到跨会话关系");
        let expected_user_id: i64 =
            sqlx::query_scalar("SELECT user_id FROM participants WHERE id=?")
                .bind(participant_a)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(found.user_id, expected_user_id);
        assert_ne!(participant_a, participant_b);

        // 首次通过稳定键归并时，即使当前会话的 participant 不同，也必须复用已有关系归属。
        let canonical = rel_repo
            .ensure_stable_relationship_participant(character_id, participant_b, "900001")
            .await
            .unwrap();
        assert_eq!(canonical, participant_a);

        let user_ids: Vec<i64> =
            sqlx::query_scalar("SELECT user_id FROM participants WHERE id IN (?, ?) ORDER BY id")
                .bind(participant_a)
                .bind(participant_b)
                .fetch_all(storage.pool())
                .await
                .unwrap();
        assert_eq!(user_ids.len(), 2);
        assert_eq!(user_ids[0], user_ids[1]);
        let user_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM users WHERE platform='onebot_qq' AND external_id='900001'",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(user_count, 1);
    }

    #[tokio::test]
    async fn global_user_identity_is_namespaced_by_platform() {
        let storage = setup_storage().await;
        let part_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );

        let qq_user = part_repo
            .ensure_user("onebot_qq", "same-external-id")
            .await
            .unwrap()
            .expect("QQ 用户应创建全域身份");
        let other_platform_user = part_repo
            .ensure_user("other_platform", "same-external-id")
            .await
            .unwrap()
            .expect("其他平台用户应创建独立全域身份");

        assert_ne!(qq_user, other_platform_user);
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE external_id='same-external-id'")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(count, 2);

        let conversation_id: i64 = sqlx::query_scalar(
            "INSERT INTO conversations(conversation_type,external_id) VALUES('group','identity-trigger-test') RETURNING id",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let participant_id: i64 = sqlx::query_scalar(
            r#"INSERT INTO participants(conversation_id,external_id,display_name,role,metadata)
               VALUES(?, 'trigger-user', '触发器用户', 'user', '{"platform":"other_platform"}')
               RETURNING id"#,
        )
        .bind(conversation_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let trigger_user_id: i64 =
            sqlx::query_scalar("SELECT user_id FROM participants WHERE id=?")
                .bind(participant_id)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let identity: (String, String) =
            sqlx::query_as("SELECT platform,external_id FROM users WHERE id=?")
                .bind(trigger_user_id)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(identity, ("other_platform".into(), "trigger-user".into()));
    }

    #[tokio::test]
    async fn migration_merges_legacy_relationships_by_global_user() {
        let storage = setup_storage().await;
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let part_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );
        let character_id = char_repo
            .insert(&Character {
                id: 0,
                definition: CharacterDefinition {
                    name: "迁移角色".into(),
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
                enabled: true,
                state: CharacterState::default(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let conversation_a: i64 = sqlx::query_scalar(
            "INSERT INTO conversations(conversation_type,external_id) VALUES('group','legacy-a') RETURNING id",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let conversation_b: i64 = sqlx::query_scalar(
            "INSERT INTO conversations(conversation_type,external_id) VALUES('group','legacy-b') RETURNING id",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let participant_a = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id: conversation_a,
                external_id: "700001".into(),
                display_name: "旧用户 A".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"stable_user_key":"700001"}),
            })
            .await
            .unwrap();
        let participant_b = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id: conversation_b,
                external_id: "700001".into(),
                display_name: "旧用户 B".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"stable_user_key":"700001"}),
            })
            .await
            .unwrap();

        // 模拟旧库：参与者没有 user_id，只有历史 stable_user_key 元数据。
        sqlx::query("UPDATE participants SET user_id=NULL, metadata=? WHERE id IN (?, ?)")
            .bind(serde_json::json!({"stable_user_key":"700001"}).to_string())
            .bind(participant_a)
            .bind(participant_b)
            .execute(storage.pool())
            .await
            .unwrap();

        sqlx::query("DROP TABLE relationships")
            .execute(storage.pool())
            .await
            .unwrap();
        sqlx::query(
            r#"CREATE TABLE relationships (
                character_id INTEGER NOT NULL,
                participant_id INTEGER NOT NULL,
                familiarity REAL NOT NULL,
                affection REAL NOT NULL,
                trust REAL NOT NULL,
                respect REAL NOT NULL,
                annoyance REAL NOT NULL,
                intimacy REAL NOT NULL,
                interaction_count INTEGER NOT NULL,
                last_interaction TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(character_id,participant_id)
            )"#,
        )
        .execute(storage.pool())
        .await
        .unwrap();
        for (participant_id, familiarity, count, updated) in [
            (participant_a, 0.3, 2_i64, "2026-01-01T00:00:00Z"),
            (participant_b, 0.8, 5_i64, "2026-02-01T00:00:00Z"),
        ] {
            sqlx::query(
                "INSERT INTO relationships(character_id,participant_id,familiarity,affection,trust,respect,annoyance,intimacy,interaction_count,last_interaction,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            )
            .bind(character_id)
            .bind(participant_id)
            .bind(familiarity)
            .bind(0.2_f64)
            .bind(0.1_f64)
            .bind(0.2_f64)
            .bind(0.0_f64)
            .bind(0.0_f64)
            .bind(count)
            .bind(updated)
            .bind("2025-01-01T00:00:00Z")
            .bind(updated)
            .execute(storage.pool())
            .await
            .unwrap();
        }
        storage.migrate().await.unwrap();
        storage.migrate().await.unwrap();
        let row: (i64, f64, i64) = sqlx::query_as(
            "SELECT user_id,familiarity,interaction_count FROM relationships WHERE character_id=?",
        )
        .bind(character_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(
            row.0,
            sqlx::query_scalar::<_, i64>("SELECT user_id FROM participants WHERE id=?")
                .bind(participant_a)
                .fetch_one(storage.pool())
                .await
                .unwrap()
        );
        assert_eq!(row.1, 0.8);
        assert_eq!(row.2, 7);
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM relationships WHERE character_id=?")
                .bind(character_id)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn migration_tolerates_invalid_legacy_metadata_without_wrong_user_mapping() {
        let storage = setup_storage().await;
        let conversation_id: i64 = sqlx::query_scalar(
            "INSERT INTO conversations(conversation_type,external_id) VALUES('group','invalid-metadata') RETURNING id",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let participant_id: i64 = sqlx::query_scalar(
            "INSERT INTO participants(conversation_id,external_id,display_name,role,metadata) VALUES(?, 'legacy-user', '旧用户', 'user', '{}') RETURNING id",
        )
        .bind(conversation_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        sqlx::query("DROP TRIGGER participants_fill_user_identity_on_update")
            .execute(storage.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE participants SET user_id=NULL, metadata=? WHERE id=?")
            .bind(serde_json::json!({"stable_user_key":"different-account"}).to_string())
            .bind(participant_id)
            .execute(storage.pool())
            .await
            .unwrap();
        storage.migrate().await.expect("可解析的稳定键应正常回填");
        let parsed_user_id: i64 = sqlx::query_scalar("SELECT user_id FROM participants WHERE id=?")
            .bind(participant_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();

        sqlx::query("UPDATE participants SET user_id=NULL, metadata='not-json' WHERE id=?")
            .bind(participant_id)
            .execute(storage.pool())
            .await
            .unwrap();
        storage
            .migrate()
            .await
            .expect("损坏的旧 metadata 不应中断迁移");

        let mapped_user_id: i64 = sqlx::query_scalar("SELECT user_id FROM participants WHERE id=?")
            .bind(participant_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let identity: (String, String) =
            sqlx::query_as("SELECT platform, external_id FROM users WHERE id=?")
                .bind(mapped_user_id)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_ne!(mapped_user_id, parsed_user_id);
        assert_eq!(
            identity,
            ("onebot_qq".to_string(), "legacy-user".to_string())
        );
    }

    #[tokio::test]
    async fn migration_merges_legacy_relationship_interactions_by_global_user() {
        let storage = setup_storage().await;
        let char_id: i64 =
            sqlx::query_scalar("INSERT INTO characters(name) VALUES('交互迁移') RETURNING id")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let conv_a: i64 = sqlx::query_scalar("INSERT INTO conversations(conversation_type,external_id) VALUES('group','legacy-interaction-a') RETURNING id")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let conv_b: i64 = sqlx::query_scalar("INSERT INTO conversations(conversation_type,external_id) VALUES('group','legacy-interaction-b') RETURNING id")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let part_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );
        let part_a = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id: conv_a,
                external_id: "legacy-interaction-user".into(),
                display_name: "旧 A".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform":"onebot_qq"}),
            })
            .await
            .unwrap();
        let part_b = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id: conv_b,
                external_id: "legacy-interaction-user".into(),
                display_name: "旧 B".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform":"onebot_qq"}),
            })
            .await
            .unwrap();
        let message_id: i64 = sqlx::query_scalar("INSERT INTO messages(conversation_id,sender_id,content,timestamp,active_character_id) VALUES(?,?,?,?,?) RETURNING id")
            .bind(conv_a)
            .bind(part_a)
            .bind("迁移来源")
            .bind("2026-01-01T00:00:00Z")
            .bind(char_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();

        sqlx::query("DROP TABLE relationship_interactions")
            .execute(storage.pool())
            .await
            .unwrap();
        sqlx::query(
            r#"CREATE TABLE relationship_interactions (
                character_id INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
                participant_id INTEGER NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
                source_message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY(character_id, participant_id, source_message_id)
            )"#,
        )
        .execute(storage.pool())
        .await
        .unwrap();
        for participant_id in [part_a, part_b] {
            sqlx::query("INSERT INTO relationship_interactions(character_id,participant_id,source_message_id) VALUES(?,?,?)")
                .bind(char_id)
                .bind(participant_id)
                .bind(message_id)
                .execute(storage.pool())
                .await
                .unwrap();
        }

        storage.migrate().await.unwrap();
        storage.migrate().await.unwrap();
        let has_user_id: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pragma_table_info('relationship_interactions') WHERE name='user_id'")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(has_user_id, 1);
        let rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM relationship_interactions WHERE character_id=?",
        )
        .bind(char_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(rows, 1);
        let user_id: i64 = sqlx::query_scalar(
            "SELECT user_id FROM relationship_interactions WHERE character_id=?",
        )
        .bind(char_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let expected_user_id: i64 =
            sqlx::query_scalar("SELECT user_id FROM participants WHERE id=?")
                .bind(part_b)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(user_id, expected_user_id);
    }

    #[tokio::test]
    async fn merge_users_moves_data_and_removes_source_identity() {
        let storage = setup_storage().await;
        let part_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );
        let character_id: i64 =
            sqlx::query_scalar("INSERT INTO characters(name) VALUES('合并测试角色') RETURNING id")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let conversation_id: i64 = sqlx::query_scalar(
            "INSERT INTO conversations(conversation_type,external_id) VALUES('group','merge-test') RETURNING id",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let source = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id,
                external_id: "910001".into(),
                display_name: "旧号".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();
        let target = part_repo
            .insert(&Participant {
                id: 0,
                conversation_id,
                external_id: "910002".into(),
                display_name: "新号".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();
        let source_user: i64 = sqlx::query_scalar("SELECT user_id FROM participants WHERE id=?")
            .bind(source)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let target_user: i64 = sqlx::query_scalar("SELECT user_id FROM participants WHERE id=?")
            .bind(target)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let source_message_id: i64 = sqlx::query_scalar(
            "INSERT INTO messages(conversation_id,sender_id,content,metadata) VALUES(?,?,?,?) RETURNING id",
        )
        .bind(conversation_id)
        .bind(source)
        .bind("{}")
        .bind(serde_json::json!({
            "external_sender_id": "910001",
            "display_name": "旧号",
            "mentions": ["910001", "910002"]
        }).to_string())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO relationship_interactions(character_id,user_id,source_message_id) VALUES(?,?,?)",
        )
        .bind(character_id)
        .bind(source_user)
        .bind(source_message_id)
        .execute(storage.pool())
        .await
        .unwrap();
        for (user_id, count) in [(source_user, 2_i64), (target_user, 5_i64)] {
            sqlx::query(
                "INSERT INTO relationships(character_id,user_id,interaction_count) VALUES(?,?,?)",
            )
            .bind(character_id)
            .bind(user_id)
            .bind(count)
            .execute(storage.pool())
            .await
            .unwrap();
        }
        part_repo
            .merge_users(source_user, target_user)
            .await
            .unwrap();
        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE id=?")
            .bind(source_user)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(remaining, 0);
        let merged_count: i64 = sqlx::query_scalar(
            "SELECT interaction_count FROM relationships WHERE character_id=? AND user_id=?",
        )
        .bind(character_id)
        .bind(target_user)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(merged_count, 7);
        let source_binding: i64 = sqlx::query_scalar("SELECT user_id FROM participants WHERE id=?")
            .bind(source)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(source_binding, target_user);
        let source_identity: (String, String, String) = sqlx::query_as(
            "SELECT external_id, display_name, metadata FROM participants WHERE id=?",
        )
        .bind(source)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert!(source_identity.0.starts_with("merged-user-"));
        assert_eq!(source_identity.1, "已合并用户");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&source_identity.2).unwrap(),
            serde_json::json!({"merged_into_user_id": target_user})
        );
        let merged_metadata: String =
            sqlx::query_scalar("SELECT metadata FROM messages WHERE id=?")
                .bind(source_message_id)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let merged_metadata: serde_json::Value = serde_json::from_str(&merged_metadata).unwrap();
        assert!(merged_metadata["external_sender_id"].is_null());
        assert!(merged_metadata["display_name"] == "已合并用户");
        assert_eq!(merged_metadata["mentions"], serde_json::json!(["910002"]));

        let interaction_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM relationship_interactions WHERE character_id=? AND user_id=? AND source_message_id=?",
        )
        .bind(character_id)
        .bind(target_user)
        .bind(source_message_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(interaction_count, 1);

        let relationship_repo =
            crate::infrastructure::storage::repository::SqliteRelationshipRepository::new(
                storage.pool().clone(),
            );
        let inserted_again = relationship_repo
            .record_interaction_once(character_id, source, source_message_id)
            .await
            .unwrap();
        assert!(!inserted_again);
        let merged_count_after_replay: i64 = sqlx::query_scalar(
            "SELECT interaction_count FROM relationships WHERE character_id=? AND user_id=?",
        )
        .bind(character_id)
        .bind(target_user)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(merged_count_after_replay, 7);
    }

    #[tokio::test]
    async fn merge_users_uses_explicit_source_when_source_id_is_greater() {
        let storage = setup_storage().await;
        let participant_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );
        let conversation_id: i64 = sqlx::query_scalar(
            "INSERT INTO conversations(conversation_type,external_id) VALUES('private','920001') RETURNING id",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();

        // 先创建目标，再创建来源，确保 source_user_id > target_user_id。
        let target_participant = participant_repo
            .insert(&Participant {
                id: 0,
                conversation_id,
                external_id: "920001".into(),
                display_name: "目标账号".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();
        let source_participant = participant_repo
            .insert(&Participant {
                id: 0,
                conversation_id,
                external_id: "920002".into(),
                display_name: "来源账号".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();
        let target_user: i64 = sqlx::query_scalar("SELECT user_id FROM participants WHERE id=?")
            .bind(target_participant)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let source_user: i64 = sqlx::query_scalar("SELECT user_id FROM participants WHERE id=?")
            .bind(source_participant)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert!(source_user > target_user);

        participant_repo
            .merge_users(source_user, target_user)
            .await
            .unwrap();

        let source_participant_identity: (String, String) =
            sqlx::query_as("SELECT external_id, display_name FROM participants WHERE id=?")
                .bind(source_participant)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert!(source_participant_identity.0.starts_with("merged-user-"));
        assert_eq!(source_participant_identity.1, "已合并用户");

        let target_participant_identity: (String, String) =
            sqlx::query_as("SELECT external_id, display_name FROM participants WHERE id=?")
                .bind(target_participant)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(target_participant_identity.0, "920001");
        assert_eq!(target_participant_identity.1, "目标账号");
    }

    #[tokio::test]
    async fn delete_user_removes_memories_and_anonymizes_history_atomically() {
        let storage = setup_storage().await;
        let participant_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );
        let character_id: i64 = sqlx::query_scalar(
            "INSERT INTO characters(name) VALUES('删除用户测试角色') RETURNING id",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let conversation_id: i64 = sqlx::query_scalar(
            "INSERT INTO conversations(conversation_type,external_id) VALUES('group','delete-user-test') RETURNING id",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let deleted_participant = participant_repo
            .insert(&Participant {
                id: 0,
                conversation_id,
                external_id: "920001".into(),
                display_name: "旧账号昵称".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform":"onebot_qq","profile":"private"}),
            })
            .await
            .unwrap();
        let other_participant = participant_repo
            .insert(&Participant {
                id: 0,
                conversation_id,
                external_id: "920002".into(),
                display_name: "另一位用户".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform":"onebot_qq"}),
            })
            .await
            .unwrap();
        let deleted_user_id: i64 =
            sqlx::query_scalar("SELECT user_id FROM participants WHERE id = ?")
                .bind(deleted_participant)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let other_user_id: i64 =
            sqlx::query_scalar("SELECT user_id FROM participants WHERE id = ?")
                .bind(other_participant)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let private_conversation_id: i64 = sqlx::query_scalar(
            "INSERT INTO conversations(conversation_type,external_id,name) VALUES('private','920001','旧账号昵称') RETURNING id",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO conversation_bindings(character_id,conversation_id,proactive_enabled) VALUES(?,?,1)",
        )
        .bind(character_id)
        .bind(private_conversation_id)
        .execute(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO blacklist_entries(target,target_type,reason) VALUES('920001','user','删除时应清理')",
        )
        .execute(storage.pool())
        .await
        .unwrap();

        let deleted_message_id: i64 = sqlx::query_scalar(
            r#"INSERT INTO messages(conversation_id,sender_id,content,metadata)
               VALUES(?,?,?,?) RETURNING id"#,
        )
        .bind(conversation_id)
        .bind(deleted_participant)
        .bind(serde_json::json!("历史正文保留，账号 920001 曾写下的内容").to_string())
        .bind(
            serde_json::json!({
                "author_kind":"user",
                "external_sender_id":"920001",
                "display_name":"旧账号昵称",
                "mentions":[920002,920001]
            })
            .to_string(),
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let other_message_id: i64 = sqlx::query_scalar(
            r#"INSERT INTO messages(conversation_id,sender_id,content,metadata)
               VALUES(?,?,?,?) RETURNING id"#,
        )
        .bind(conversation_id)
        .bind(other_participant)
        .bind(serde_json::json!("另一位用户的历史消息").to_string())
        .bind(
            serde_json::json!({
                "author_kind":"user",
                "external_sender_id":"920002",
                "display_name":"另一位用户",
                "mentions":[920001,920003]
            })
            .to_string(),
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();

        sqlx::query(
            r#"INSERT INTO outbox_events
               (event_uuid,entity_id,entity_type,event_type,event_version,payload,ordering_key,ordering_seq)
               VALUES('delete-user-event',?,'messages','MessageAccepted',1,?,'conversation:delete-user-test',1)"#,
        )
        .bind(deleted_message_id)
        .bind(serde_json::json!({
            "message_id":deleted_message_id,
            "content":"历史正文保留，账号 920001 曾写下的内容",
            "metadata":{
                "author_kind":"user",
                "external_sender_id":"920001",
                "display_name":"旧账号昵称",
                "mentions":[920001]
            }
        }).to_string())
        .execute(storage.pool())
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO memories(character_id,conversation_id,memory_type,content,metadata,user_id) VALUES(?,?,'semantic','需要删除的个人记忆',?,?)",
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(serde_json::json!({"source_participant_id":deleted_participant}).to_string())
        .bind(deleted_user_id)
        .execute(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO memories(character_id,conversation_id,memory_type,content,metadata,user_id) VALUES(?,?,'semantic','其他用户的记忆',?,?)",
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(serde_json::json!({"source_participant_id":other_participant}).to_string())
        .bind(other_user_id)
        .execute(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO memories(character_id,conversation_id,memory_type,content,metadata,user_id) VALUES(?,?,'semantic','由多条经历整理出的个人记忆',?,NULL)",
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(serde_json::json!({"source_participant_ids":[deleted_participant,other_participant]}).to_string())
        .execute(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO semantic_memories(character_id,conversation_id,memory_type,content,embedding,metadata) VALUES(?,?,'semantic','向量个人记忆','[0.1]',?)",
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(serde_json::json!({"source_participant_ids":[deleted_participant]}).to_string())
        .execute(storage.pool())
        .await
        .unwrap();
        sqlx::query("INSERT INTO relationships(character_id,user_id) VALUES(?,?)")
            .bind(character_id)
            .bind(deleted_user_id)
            .execute(storage.pool())
            .await
            .unwrap();

        participant_repo.delete_user(deleted_user_id).await.unwrap();

        let user_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE id=?")
            .bind(deleted_user_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(user_rows, 0);
        let participant: (String, String, String, Option<i64>) = sqlx::query_as(
            "SELECT external_id,display_name,metadata,user_id FROM participants WHERE id=?",
        )
        .bind(deleted_participant)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert!(participant.0.starts_with("deleted-user-"));
        assert_eq!(participant.1, "已删除用户");
        assert_eq!(participant.2, "{}");
        assert_eq!(participant.3, None);
        let private_conversation: (String, Option<String>, i64) = sqlx::query_as(
            r#"SELECT c.external_id,c.name,b.proactive_enabled
                 FROM conversations c
                 JOIN conversation_bindings b ON b.conversation_id=c.id
                WHERE c.id=?"#,
        )
        .bind(private_conversation_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert!(private_conversation
            .0
            .starts_with("deleted-user-conversation-"));
        assert_eq!(private_conversation.1.as_deref(), Some("已删除用户"));
        assert_eq!(private_conversation.2, 0);

        let messages: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT id,content,metadata FROM messages WHERE id IN (?,?) ORDER BY id",
        )
        .bind(deleted_message_id)
        .bind(other_message_id)
        .fetch_all(storage.pool())
        .await
        .unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages[0].1,
            serde_json::json!("历史正文保留，账号 920001 曾写下的内容").to_string()
        );
        assert!(!messages[0].2.contains("920001"));
        assert!(!messages[0].2.contains("旧账号昵称"));
        let other_metadata: serde_json::Value = serde_json::from_str(&messages[1].2).unwrap();
        assert_eq!(other_metadata["mentions"], serde_json::json!([920003]));
        assert_eq!(other_metadata["external_sender_id"], "920002");

        let outbox_payload: String = sqlx::query_scalar(
            "SELECT payload FROM outbox_events WHERE event_uuid='delete-user-event'",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let outbox_payload: serde_json::Value = serde_json::from_str(&outbox_payload).unwrap();
        assert_eq!(
            outbox_payload["content"],
            "历史正文保留，账号 920001 曾写下的内容"
        );
        assert!(outbox_payload["metadata"]["external_sender_id"].is_null());
        assert_eq!(outbox_payload["metadata"]["display_name"], "已删除用户");
        assert_eq!(
            outbox_payload["metadata"]["mentions"],
            serde_json::json!([])
        );

        let remaining_memories: Vec<String> =
            sqlx::query_scalar("SELECT content FROM memories ORDER BY id")
                .fetch_all(storage.pool())
                .await
                .unwrap();
        assert_eq!(remaining_memories, vec!["其他用户的记忆"]);
        let semantic_memory_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM semantic_memories")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(semantic_memory_count, 0);
        let blacklist_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM blacklist_entries WHERE target='920001' AND target_type='user'",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(blacklist_count, 0);
        let relationship_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM relationships WHERE user_id=?")
                .bind(deleted_user_id)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(relationship_count, 0);
        let foreign_key_violations: Vec<(String, i64, String, i64)> =
            sqlx::query_as("PRAGMA foreign_key_check")
                .fetch_all(storage.pool())
                .await
                .unwrap();
        assert!(foreign_key_violations.is_empty());

        let missing = participant_repo.delete_user(deleted_user_id).await;
        assert!(matches!(
            missing,
            Err(crate::error::RepositoryError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn delete_user_rolls_back_anonymization_if_database_delete_fails() {
        let storage = setup_storage().await;
        let participant_repo =
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                storage.pool().clone(),
            );
        let conversation_id: i64 = sqlx::query_scalar(
            "INSERT INTO conversations(conversation_type,external_id) VALUES('group','delete-rollback-test') RETURNING id",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let participant_id = participant_repo
            .insert(&Participant {
                id: 0,
                conversation_id,
                external_id: "930001".into(),
                display_name: "保留昵称".into(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({"platform":"onebot_qq"}),
            })
            .await
            .unwrap();
        let user_id: i64 = sqlx::query_scalar("SELECT user_id FROM participants WHERE id=?")
            .bind(participant_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        sqlx::query(
            "CREATE TRIGGER fail_user_delete BEFORE DELETE ON users BEGIN SELECT RAISE(ABORT, 'injected delete failure'); END",
        )
        .execute(storage.pool())
        .await
        .unwrap();

        assert!(participant_repo.delete_user(user_id).await.is_err());
        let participant: (String, String, Option<i64>) =
            sqlx::query_as("SELECT external_id,display_name,user_id FROM participants WHERE id=?")
                .bind(participant_id)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(
            participant,
            ("930001".into(), "保留昵称".into(), Some(user_id))
        );
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
            enabled: true,
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
            enabled: true,
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
            enabled: true,
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
            enabled: true,
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
            enabled: true,
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
            enabled: true,
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
                    proactive_count_date: Some(chrono::Utc::now().date_naive()),
                    proactive_count: 3,
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
        assert_eq!(loaded2.proactive_count, 3);

        // 额度预留必须在仓储层同时检查冷却和每日上限；第二次立即预留应失败。
        let reserve_now = chrono::Utc::now() + chrono::Duration::minutes(6);
        assert!(bs_repo
            .try_reserve_proactive(
                char_id,
                conv_id,
                reserve_now,
                chrono::Duration::minutes(10),
                10,
            )
            .await
            .unwrap());
        assert!(!bs_repo
            .try_reserve_proactive(
                char_id,
                conv_id,
                reserve_now + chrono::Duration::seconds(1),
                chrono::Duration::minutes(10),
                10,
            )
            .await
            .unwrap());
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

    // -------------------------------------------------------------------------
    // Phase 2: SQLite find_recent_attributed Integration Tests
    // -------------------------------------------------------------------------

    /// SQLite-A: A→B→A attribution
    /// 写入 M1(A), M2(B), M3(A)，验证 A 看到 M1,M3，B 看到 M2
    #[tokio::test]
    async fn sqlite_attributed_a_b_a() {
        let storage = setup_storage().await;

        // 创建角色 A 和 B（active_character_id FK 需要）
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let char_a = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "A".to_string(),
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
            enabled: true,
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_a_id = char_repo.insert(&char_a).await.unwrap();

        let char_b = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "B".to_string(),
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
            enabled: true,
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_b_id = char_repo.insert(&char_b).await.unwrap();

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

        // 创建会话和参与者
        let conv = Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "user1".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv_id = conv_repo.insert(&conv).await.unwrap();

        let participant = Participant {
            id: 0,
            conversation_id: conv_id,
            external_id: "user1".to_string(),
            display_name: "User1".to_string(),
            role: ParticipantRole::User,
            metadata: serde_json::json!({}),
        };
        let part_id = part_repo.insert(&participant).await.unwrap();

        let base = chrono::Utc::now();

        // M1: A 的消息
        let m1 = make_test_message(
            conv_id,
            part_id,
            MessageContent::Text("M1 from A".to_string()),
            base,
            Some(char_a_id),
        );
        let _m1_id = msg_repo.insert(&m1).await.unwrap();

        // M2: B 的消息
        let m2 = make_test_message(
            conv_id,
            part_id,
            MessageContent::Text("M2 from B".to_string()),
            base + chrono::Duration::minutes(1),
            Some(char_b_id),
        );
        let _m2_id = msg_repo.insert(&m2).await.unwrap();

        // M3: A 的消息
        let m3 = make_test_message(
            conv_id,
            part_id,
            MessageContent::Text("M3 from A".to_string()),
            base + chrono::Duration::minutes(2),
            Some(char_a_id),
        );
        let _m3_id = msg_repo.insert(&m3).await.unwrap();

        // A 调用 find_recent_attributed，应得到 M1, M3
        let a_msgs = msg_repo
            .find_recent_attributed(conv_id, char_a_id, 20)
            .await
            .unwrap();
        assert_eq!(a_msgs.len(), 2, "A 应看到 2 条消息");
        assert_eq!(a_msgs[0].id, 1, "A: M1 最旧在前");
        assert_eq!(a_msgs[1].id, 3, "A: M3 最新在后");

        // B 调用 find_recent_attributed，应得到 M2
        let b_msgs = msg_repo
            .find_recent_attributed(conv_id, char_b_id, 20)
            .await
            .unwrap();
        assert_eq!(b_msgs.len(), 1, "B 应看到 1 条消息");
        assert_eq!(b_msgs[0].id, 2, "B: M2");

        // Character 99 不存在，应得到空
        let c99_msgs = msg_repo
            .find_recent_attributed(conv_id, 99, 20)
            .await
            .unwrap();
        assert_eq!(c99_msgs.len(), 0, "不存在的角色应看到 0 条消息");
    }

    /// SQLite-B: Sparse history — 核心测试，证明 WHERE 在 LIMIT 之前
    /// 写入 M1(A), M2..M1000(B), M1001(A)，验证 A 的 limit=20 返回 [M1, M1001]
    #[tokio::test]
    async fn sqlite_attributed_sparse_history_where_before_limit() {
        let storage = setup_storage().await;

        // 创建角色 A 和 B（active_character_id FK 需要）
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let char_a = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "A".to_string(),
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
            enabled: true,
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_a_id = char_repo.insert(&char_a).await.unwrap();

        let char_b = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "B".to_string(),
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
            enabled: true,
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_b_id = char_repo.insert(&char_b).await.unwrap();

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

        // 创建会话和参与者
        let conv = Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "sparse".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv_id = conv_repo.insert(&conv).await.unwrap();

        let participant = Participant {
            id: 0,
            conversation_id: conv_id,
            external_id: "user1".to_string(),
            display_name: "User1".to_string(),
            role: ParticipantRole::User,
            metadata: serde_json::json!({}),
        };
        let part_id = part_repo.insert(&participant).await.unwrap();

        let base = chrono::Utc::now();

        // M1: A 的消息（最旧）
        let m1 = Message {
            id: 0,
            conversation_id: conv_id,
            sender_id: part_id,
            content: MessageContent::Text("M1 from A".to_string()),
            timestamp: base,
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
            active_character_id: Some(char_a_id),
            // C2-3 字段
            platform_message_id: None,
            conversation_sequence: None,
            occurred_at: None,
            ingested_at: None,
        };
        let m1_id = msg_repo.insert(&m1).await.unwrap();
        assert_eq!(m1_id, 1);

        // M2..M1000: B 的消息（共 999 条）
        for i in 2..=1000i64 {
            let msg = Message {
                id: 0,
                conversation_id: conv_id,
                sender_id: part_id,
                content: MessageContent::Text(format!("M{} from B", i)),
                timestamp: base + chrono::Duration::minutes(i - 1),
                reply_to: None,
                mentions: vec![],
                attachments: vec![],
                metadata: serde_json::json!({}),
                active_character_id: Some(char_b_id), // B
                // C2-3 字段
                platform_message_id: None,
                conversation_sequence: None,
                occurred_at: None,
                ingested_at: None,
            };
            msg_repo.insert(&msg).await.unwrap();
        }

        // M1001: A 的消息（最新）
        let m1001 = Message {
            id: 0,
            conversation_id: conv_id,
            sender_id: part_id,
            content: MessageContent::Text("M1001 from A".to_string()),
            timestamp: base + chrono::Duration::minutes(1000),
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
            active_character_id: Some(char_a_id),
            // C2-3 字段
            platform_message_id: None,
            conversation_sequence: None,
            occurred_at: None,
            ingested_at: None,
        };
        let m1001_id = msg_repo.insert(&m1001).await.unwrap();
        assert_eq!(m1001_id, 1001);

        // A 调用 find_recent_attributed(conv_id, A, limit=20)
        // 关键：即使有 999 条 B 消息在中间，A 仍然只看到 M1 和 M1001
        // 这证明 SQL 是: WHERE active_character_id=A 先过滤，然后 LIMIT 20 截断
        // 而不是: 先 LIMIT 20（取 conversation 最近 20 条），再从中过滤 A
        let a_msgs = msg_repo
            .find_recent_attributed(conv_id, char_a_id, 20)
            .await
            .unwrap();
        assert_eq!(
            a_msgs.len(),
            2,
            "A 应只看到 2 条消息（M1 和 M1001），证明 WHERE 在 LIMIT 之前"
        );
        assert_eq!(a_msgs[0].id, 1, "最旧在前: M1");
        assert_eq!(a_msgs[1].id, 1001, "最新在后: M1001");

        // B 应该看到 M2..M1000（共 999 条），limit=20 只返回最近 20 条
        let b_msgs = msg_repo
            .find_recent_attributed(conv_id, char_b_id, 20)
            .await
            .unwrap();
        assert_eq!(b_msgs.len(), 20, "B 的 limit=20 应返回 20 条");
        // B 的消息按 timestamp DESC，最新的在前，所以应该是 id 981-1000
        assert_eq!(b_msgs[0].id, 981, "B 最新的一条是 M981");
        assert_eq!(b_msgs[19].id, 1000, "B 第 20 条是 M1000");
    }

    /// SQLite-C: 超过 limit — 21 条 A 消息，limit=20 应精确返回最近 20 条
    #[tokio::test]
    async fn sqlite_attributed_limit_exact() {
        let storage = setup_storage().await;

        // 创建角色 A（active_character_id FK 需要）
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let char_a = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "A".to_string(),
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
            enabled: true,
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_a_id = char_repo.insert(&char_a).await.unwrap();

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

        let conv = Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "limit_test".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv_id = conv_repo.insert(&conv).await.unwrap();

        let participant = Participant {
            id: 0,
            conversation_id: conv_id,
            external_id: "user1".to_string(),
            display_name: "User1".to_string(),
            role: ParticipantRole::User,
            metadata: serde_json::json!({}),
        };
        let part_id = part_repo.insert(&participant).await.unwrap();

        let base = chrono::Utc::now();

        // 插入 21 条 A 消息（M1..M20 连续时间，M21 间隔很远）
        for i in 1..=20i64 {
            let msg = Message {
                id: 0,
                conversation_id: conv_id,
                sender_id: part_id,
                content: MessageContent::Text(format!("M{} from A", i)),
                timestamp: base + chrono::Duration::seconds(i),
                reply_to: None,
                mentions: vec![],
                attachments: vec![],
                metadata: serde_json::json!({}),
                active_character_id: Some(char_a_id),
                // C2-3 字段
                platform_message_id: None,
                conversation_sequence: None,
                occurred_at: None,
                ingested_at: None,
            };
            msg_repo.insert(&msg).await.unwrap();
        }

        // M21: 间隔很远
        let m21 = Message {
            id: 0,
            conversation_id: conv_id,
            sender_id: part_id,
            content: MessageContent::Text("M21 from A far".to_string()),
            timestamp: base + chrono::Duration::days(1),
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
            active_character_id: Some(char_a_id),
            // C2-3 字段
            platform_message_id: None,
            conversation_sequence: None,
            occurred_at: None,
            ingested_at: None,
        };
        let _m21_id = msg_repo.insert(&m21).await.unwrap();

        // limit=20 应返回 M2..M20, M21（最近 20 条 oldest-first）
        let msgs = msg_repo
            .find_recent_attributed(conv_id, char_a_id, 20)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 20, "limit=20 应返回 20 条");
        assert_eq!(msgs[0].id, 2, "最旧: M2");
        assert_eq!(msgs[19].id, 21, "最新: M21");

        // limit=10 应返回 M12..M20, M21
        let msgs10 = msg_repo
            .find_recent_attributed(conv_id, char_a_id, 10)
            .await
            .unwrap();
        assert_eq!(msgs10.len(), 10, "limit=10 应返回 10 条");
        assert_eq!(msgs10[0].id, 12, "最旧: M12");
        assert_eq!(msgs10[9].id, 21, "最新: M21");
    }

    /// SQLite-D: NULL legacy — active_character_id=NULL 不应被 Character-specific 查询返回
    #[tokio::test]
    async fn sqlite_attributed_null_legacy() {
        let storage = setup_storage().await;

        // 创建角色 A（active_character_id FK 需要）
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let char_a = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "A".to_string(),
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
            enabled: true,
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_a_id = char_repo.insert(&char_a).await.unwrap();

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

        let conv = Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "null_test".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv_id = conv_repo.insert(&conv).await.unwrap();

        let participant = Participant {
            id: 0,
            conversation_id: conv_id,
            external_id: "user1".to_string(),
            display_name: "User1".to_string(),
            role: ParticipantRole::User,
            metadata: serde_json::json!({}),
        };
        let part_id = part_repo.insert(&participant).await.unwrap();

        let base = chrono::Utc::now();

        // M1: NULL legacy 消息（无 attribution）
        let m1 = Message {
            id: 0,
            conversation_id: conv_id,
            sender_id: part_id,
            content: MessageContent::Text("Legacy message".to_string()),
            timestamp: base,
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
            active_character_id: None, // NULL — 旧数据
            // C2-3 字段
            platform_message_id: None,
            conversation_sequence: None,
            occurred_at: None,
            ingested_at: None,
        };
        let _m1_id = msg_repo.insert(&m1).await.unwrap();

        // M2: A 的消息
        let m2 = Message {
            id: 0,
            conversation_id: conv_id,
            sender_id: part_id,
            content: MessageContent::Text("M2 from A".to_string()),
            timestamp: base + chrono::Duration::minutes(1),
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
            active_character_id: Some(char_a_id),
            // C2-3 字段
            platform_message_id: None,
            conversation_sequence: None,
            occurred_at: None,
            ingested_at: None,
        };
        let _m2_id = msg_repo.insert(&m2).await.unwrap();

        // CharacterHistoryProvider(A) 不返回 NULL 消息
        let a_msgs = msg_repo
            .find_recent_attributed(conv_id, char_a_id, 20)
            .await
            .unwrap();
        assert_eq!(a_msgs.len(), 1, "A 应只看到 1 条消息（M2）");
        assert_eq!(a_msgs[0].id, 2, "A 看到的应是 M2，不是 NULL legacy");

        // raw find_recent 可以看到所有消息包括 NULL
        let raw_msgs = msg_repo.find_recent(conv_id, 20).await.unwrap();
        assert_eq!(raw_msgs.len(), 2, "Raw 应看到 2 条消息（包括 NULL legacy）");
    }

    /// SQLite-E: 相同 timestamp 时的 ordering — ORDER BY timestamp DESC, id DESC
    /// reverse 后最终 oldest-first，相同 timestamp 时 id ASC
    #[tokio::test]
    async fn sqlite_attributed_same_timestamp_ordering() {
        let storage = setup_storage().await;

        // 创建角色 A（active_character_id FK 需要）
        let char_repo = crate::infrastructure::storage::repository::SqliteCharacterRepository::new(
            storage.pool().clone(),
        );
        let char_a = Character {
            id: 0,
            definition: CharacterDefinition {
                name: "A".to_string(),
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
            enabled: true,
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let char_a_id = char_repo.insert(&char_a).await.unwrap();

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

        let conv = Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "order_test".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let conv_id = conv_repo.insert(&conv).await.unwrap();

        let participant = Participant {
            id: 0,
            conversation_id: conv_id,
            external_id: "user1".to_string(),
            display_name: "User1".to_string(),
            role: ParticipantRole::User,
            metadata: serde_json::json!({}),
        };
        let part_id = part_repo.insert(&participant).await.unwrap();

        // 相同 timestamp，不同 id
        let same_time = chrono::DateTime::parse_from_rfc3339("2026-01-01T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        let m10 = Message {
            id: 0,
            conversation_id: conv_id,
            sender_id: part_id,
            content: MessageContent::Text("M10".to_string()),
            timestamp: same_time,
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
            active_character_id: Some(char_a_id),
            // C2-3 字段
            platform_message_id: None,
            conversation_sequence: None,
            occurred_at: None,
            ingested_at: None,
        };
        let m10_id = msg_repo.insert(&m10).await.unwrap();

        let m11 = Message {
            id: 0,
            conversation_id: conv_id,
            sender_id: part_id,
            content: MessageContent::Text("M11".to_string()),
            timestamp: same_time,
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
            active_character_id: Some(char_a_id),
            // C2-3 字段
            platform_message_id: None,
            conversation_sequence: None,
            occurred_at: None,
            ingested_at: None,
        };
        let m11_id = msg_repo.insert(&m11).await.unwrap();

        let m12 = Message {
            id: 0,
            conversation_id: conv_id,
            sender_id: part_id,
            content: MessageContent::Text("M12".to_string()),
            timestamp: same_time,
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
            active_character_id: Some(char_a_id),
            // C2-3 字段
            platform_message_id: None,
            conversation_sequence: None,
            occurred_at: None,
            ingested_at: None,
        };
        let m12_id = msg_repo.insert(&m12).await.unwrap();

        let msgs = msg_repo
            .find_recent_attributed(conv_id, char_a_id, 20)
            .await
            .unwrap();

        assert_eq!(msgs.len(), 3, "应返回 3 条消息");
        // SQL: ORDER BY timestamp DESC, id DESC → [m12, m11, m10]
        // reverse 后: [m10, m11, m12] → id ASC
        assert_eq!(msgs[0].id, m10_id, "最终 oldest-first: M10 在前");
        assert_eq!(msgs[1].id, m11_id, "相同 timestamp: id ASC");
        assert_eq!(msgs[2].id, m12_id, "M12 在后（id 最大）");
    }

    /// Migration 011: 验证 idx_messages_attribution 索引在 migration 后存在
    #[tokio::test]
    async fn sqlite_migration_011_creates_attribution_index() {
        let storage = setup_storage().await;
        let pool = storage.pool();

        // 验证索引存在
        let idx_exists: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM sqlite_master
               WHERE type = 'index' AND name = 'idx_messages_attribution'"#,
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(idx_exists, 1, "idx_messages_attribution 索引应存在");

        // 验证索引定义
        let idx_sql: (String,) = sqlx::query_as(
            r#"SELECT sql FROM sqlite_master
               WHERE type = 'index' AND name = 'idx_messages_attribution'"#,
        )
        .fetch_one(pool)
        .await
        .unwrap();

        let sql = idx_sql.0.to_lowercase();
        assert!(sql.contains("messages"), "索引定义应包含 messages 表");
        assert!(
            sql.contains("conversation_id"),
            "索引应包含 conversation_id 列"
        );
        assert!(
            sql.contains("active_character_id"),
            "索引应包含 active_character_id 列"
        );
        assert!(sql.contains("timestamp"), "索引应包含 timestamp 列");
        assert!(sql.contains("id"), "索引应包含 id 列");
    }
}
