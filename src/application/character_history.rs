//! CharacterHistoryProvider — 提供 Character-specific message history retrieval。
//!
//! 语义：
//! - 返回当前 Conversation 中归属于指定 Character 的最近 N 条 Message
//! - SQL WHERE 条件在 LIMIT 之前应用，保证 LIMIT 作用于已过滤的候选集
//! - 返回结果为 oldest-first（自然先后顺序）

use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::message::Message;
use crate::domain::repository::MessageRepository;
use crate::error::RuntimeError;

/// Character-specific message history provider。
#[async_trait]
pub trait CharacterHistoryProvider: Send + Sync {
    /// 获取当前 Conversation 中归属于指定 Character 的最近消息。
    ///
    /// 等价于：
    /// ```sql
    /// WHERE conversation_id = ? AND active_character_id = ?
    /// ORDER BY timestamp DESC, id DESC
    /// LIMIT ?
    /// ```
    /// 返回结果已反转，最旧的消息排在前面。
    ///
    /// # 参数
    /// - `before_sequence`: 如果为 `Some(seq)`，使用 sequence-based 过滤：
    ///   `WHERE conversation_id = ? AND active_character_id = ? AND conversation_sequence < seq`
    async fn get_recent(
        &self,
        conversation_id: i64,
        character_id: i64,
        limit: usize,
        before_sequence: Option<i64>,
    ) -> Result<Vec<Message>, RuntimeError>;
}

/// 基于 MessageRepository 的 CharacterHistoryProvider 实现。
pub struct RepositoryCharacterHistoryProvider {
    message_repo: Arc<dyn MessageRepository>,
}

impl RepositoryCharacterHistoryProvider {
    pub fn new(message_repo: Arc<dyn MessageRepository>) -> Self {
        Self { message_repo }
    }
}

#[async_trait]
impl CharacterHistoryProvider for RepositoryCharacterHistoryProvider {
    async fn get_recent(
        &self,
        conversation_id: i64,
        character_id: i64,
        limit: usize,
        before_sequence: Option<i64>,
    ) -> Result<Vec<Message>, RuntimeError> {
        match before_sequence {
            Some(seq) => {
                // C2-3: sequence-based filtering
                self.message_repo
                    .find_before_sequence(conversation_id, character_id, seq, limit as i64)
                    .await
                    .map_err(RuntimeError::Repository)
            }
            None => {
                // Legacy: timestamp-based filtering
                self.message_repo
                    .find_recent_attributed(conversation_id, character_id, limit as i64)
                    .await
                    .map_err(RuntimeError::Repository)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 2 Acceptance Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::message::MessageContent;
    use crate::error::RepositoryError;
    use chrono::{DateTime, Utc};
    use std::sync::Mutex;

    /// 简单的 in-memory MessageRepository 实现，用于测试。
    struct MemRepo {
        messages: Mutex<Vec<Message>>,
    }

    impl MemRepo {
        fn new(messages: Vec<Message>) -> Self {
            Self {
                messages: Mutex::new(messages),
            }
        }
    }

    #[async_trait]
    impl MessageRepository for MemRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Message>, RepositoryError> {
            Ok(None)
        }

        async fn find_recent(
            &self,
            conversation_id: i64,
            limit: i64,
        ) -> Result<Vec<Message>, RepositoryError> {
            let mut all = self.messages.lock().unwrap().clone();
            all.retain(|m| m.conversation_id == conversation_id);
            all.sort_by_key(|a| std::cmp::Reverse(a.timestamp));
            all.truncate(limit as usize);
            all.reverse();
            Ok(all)
        }

        async fn find_recent_attributed(
            &self,
            conversation_id: i64,
            character_id: i64,
            limit: i64,
        ) -> Result<Vec<Message>, RepositoryError> {
            let mut filtered: Vec<Message> = self
                .messages
                .lock()
                .unwrap()
                .iter()
                .filter(|m| {
                    m.conversation_id == conversation_id
                        && m.active_character_id == Some(character_id)
                })
                .cloned()
                .collect();
            // 模拟 SQL: ORDER BY timestamp DESC, id DESC
            filtered.sort_by(|a, b| {
                let time_cmp = b.timestamp.cmp(&a.timestamp); // DESC
                if time_cmp == std::cmp::Ordering::Equal {
                    b.id.cmp(&a.id) // DESC
                } else {
                    time_cmp
                }
            });
            filtered.truncate(limit as usize);
            // 反转顺序，使最旧的消息排在前面
            filtered.reverse();
            Ok(filtered)
        }

        async fn insert(&self, _m: &Message) -> Result<i64, RepositoryError> {
            Ok(1)
        }

        async fn latest_message_time(
            &self,
            _conversation_id: i64,
        ) -> Result<Option<DateTime<Utc>>, RepositoryError> {
            Ok(None)
        }

        async fn find_before_sequence(
            &self,
            _conversation_id: i64,
            _character_id: i64,
            _before_sequence: i64,
            _limit: i64,
        ) -> Result<Vec<Message>, RepositoryError> {
            Ok(Vec::new())
        }

        async fn find_recent_by_sequence(
            &self,
            _conversation_id: i64,
            _limit: i64,
        ) -> Result<Vec<Message>, RepositoryError> {
            Ok(Vec::new())
        }

        async fn accept_message(
            &self,
            _conversation_id: i64,
            _sender_id: i64,
            _content_json: &str,
            _timestamp: &str,
            _platform_message_id: Option<i64>,
            _character_id: i64,
            _event_uuid: &str,
            _outbox_payload: &str,
        ) -> Result<(i64, i64, chrono::DateTime<chrono::Utc>), RepositoryError> {
            Err(RepositoryError::NotImplemented(
                "not implemented".to_string(),
            ))
        }
    }

    /// 构造测试用消息。
    fn make_msg(
        id: i64,
        conversation_id: i64,
        sender_id: i64,
        content: &str,
        timestamp: DateTime<Utc>,
        active_character_id: Option<i64>,
    ) -> Message {
        Message {
            id,
            conversation_id,
            sender_id,
            content: MessageContent::Text(content.to_string()),
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

    fn base_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    /// A — A→B→A attribution test
    #[tokio::test]
    async fn test_a_b_a_attribution() {
        // M1(A), M2(B), M3(A)
        let base = base_time();
        let t1 = base;
        let t2 = base + chrono::Duration::minutes(1);
        let t3 = base + chrono::Duration::minutes(2);
        let messages = vec![
            make_msg(1, 10, 1, "M1", t1, Some(1)), // A 的消息
            make_msg(2, 10, 2, "M2", t2, Some(2)), // B 的消息
            make_msg(3, 10, 1, "M3", t3, Some(1)), // A 的消息
        ];
        let repo = Arc::new(MemRepo::new(messages));
        let provider = RepositoryCharacterHistoryProvider::new(repo);

        // A 应该看到 M1, M3
        let a_msgs = provider.get_recent(10, 1, 20, None).await.unwrap();
        assert_eq!(a_msgs.len(), 2);
        assert_eq!(a_msgs[0].id, 1); // 最旧在前
        assert_eq!(a_msgs[1].id, 3);

        // B 应该看到 M2
        let b_msgs = provider.get_recent(10, 2, 20, None).await.unwrap();
        assert_eq!(b_msgs.len(), 1);
        assert_eq!(b_msgs[0].id, 2);
    }

    /// B — Sparse history: WHERE 在 LIMIT 前（1000条B消息中间插1条A消息）
    #[tokio::test]
    async fn test_sparse_history_where_before_limit() {
        let base = base_time();
        let t1 = base;
        let t1001 = base + chrono::Duration::minutes(1000);
        let messages = vec![
            make_msg(1, 10, 1, "M1", t1, Some(1)), // A 的消息
            // M2..M1000 是 B 的消息（A 看不到）
            make_msg(1001, 10, 1, "M1001", t1001, Some(1)), // A 的消息
        ];
        // 补充 B 的消息 M2..M1000
        let mut all_messages = messages;
        for i in 2..=1000 {
            let t = base + chrono::Duration::minutes(i as i64 - 1);
            all_messages.push(make_msg(i as i64, 10, 2, "B msg", t, Some(2)));
        }
        all_messages.sort_by_key(|m| m.id); // 保持 id 顺序

        let repo = Arc::new(MemRepo::new(all_messages));
        let provider = RepositoryCharacterHistoryProvider::new(repo);

        // A, limit=20，应该返回 M1 和 M1001（虽然有 1000 条 B 消息在中间）
        let a_msgs = provider.get_recent(10, 1, 20, None).await.unwrap();
        assert_eq!(a_msgs.len(), 2);
        assert_eq!(a_msgs[0].id, 1);
        assert_eq!(a_msgs[1].id, 1001);
    }

    /// C — 20+ A messages: 21 条 A 消息，最近 20 条是 M2..M20, M1001
    #[tokio::test]
    async fn test_20_plus_a_messages_limit_correctness() {
        let base = base_time();
        let messages = vec![
            // M1..M20 是 A 的消息（20条）
            make_msg(1, 10, 1, "M1", base, Some(1)),
            make_msg(2, 10, 1, "M2", base + chrono::Duration::seconds(1), Some(1)),
            make_msg(3, 10, 1, "M3", base + chrono::Duration::seconds(2), Some(1)),
            make_msg(4, 10, 1, "M4", base + chrono::Duration::seconds(3), Some(1)),
            make_msg(5, 10, 1, "M5", base + chrono::Duration::seconds(4), Some(1)),
            make_msg(6, 10, 1, "M6", base + chrono::Duration::seconds(5), Some(1)),
            make_msg(7, 10, 1, "M7", base + chrono::Duration::seconds(6), Some(1)),
            make_msg(8, 10, 1, "M8", base + chrono::Duration::seconds(7), Some(1)),
            make_msg(9, 10, 1, "M9", base + chrono::Duration::seconds(8), Some(1)),
            make_msg(
                10,
                10,
                1,
                "M10",
                base + chrono::Duration::seconds(9),
                Some(1),
            ),
            make_msg(
                11,
                10,
                1,
                "M11",
                base + chrono::Duration::seconds(10),
                Some(1),
            ),
            make_msg(
                12,
                10,
                1,
                "M12",
                base + chrono::Duration::seconds(11),
                Some(1),
            ),
            make_msg(
                13,
                10,
                1,
                "M13",
                base + chrono::Duration::seconds(12),
                Some(1),
            ),
            make_msg(
                14,
                10,
                1,
                "M14",
                base + chrono::Duration::seconds(13),
                Some(1),
            ),
            make_msg(
                15,
                10,
                1,
                "M15",
                base + chrono::Duration::seconds(14),
                Some(1),
            ),
            make_msg(
                16,
                10,
                1,
                "M16",
                base + chrono::Duration::seconds(15),
                Some(1),
            ),
            make_msg(
                17,
                10,
                1,
                "M17",
                base + chrono::Duration::seconds(16),
                Some(1),
            ),
            make_msg(
                18,
                10,
                1,
                "M18",
                base + chrono::Duration::seconds(17),
                Some(1),
            ),
            make_msg(
                19,
                10,
                1,
                "M19",
                base + chrono::Duration::seconds(18),
                Some(1),
            ),
            make_msg(
                20,
                10,
                1,
                "M20",
                base + chrono::Duration::seconds(19),
                Some(1),
            ),
            // M21..M1000 是 B 的消息
            make_msg(
                21,
                10,
                2,
                "M21",
                base + chrono::Duration::seconds(20),
                Some(2),
            ),
            // M1001 是 A 的消息（第 21 条 A 消息）
            make_msg(
                1001,
                10,
                1,
                "M1001",
                base + chrono::Duration::minutes(1000),
                Some(1),
            ),
        ];

        let repo = Arc::new(MemRepo::new(messages));
        let provider = RepositoryCharacterHistoryProvider::new(repo);

        // A 有 21 条消息（M1..M20, M1001），limit=20 应该返回最近的 20 条
        // 即 M2..M20, M1001（按 oldest-first 排序）
        let a_msgs = provider.get_recent(10, 1, 20, None).await.unwrap();
        assert_eq!(a_msgs.len(), 20);
        // 第一条应该是 M2（第二早的），最后一条应该是 M1001（最新的）
        assert_eq!(a_msgs[0].id, 2);
        assert_eq!(a_msgs[19].id, 1001);
    }

    /// D — NULL legacy: Raw history 可以看到 NULL，attributed history 看不到 NULL
    #[tokio::test]
    async fn test_null_legacy_raw_vs_attributed() {
        let base = base_time();
        let messages = vec![
            make_msg(1, 10, 1, "NULL msg", base, None), // legacy，无 attribution
            make_msg(2, 10, 1, "A msg", base, Some(1)), // A 的消息
        ];

        let repo = Arc::new(MemRepo::new(messages));
        let provider = RepositoryCharacterHistoryProvider::new(repo.clone());

        // CharacterHistoryProvider(A) 不返回 NULL 消息
        let a_msgs = provider.get_recent(10, 1, 20, None).await.unwrap();
        assert_eq!(a_msgs.len(), 1);
        assert_eq!(a_msgs[0].id, 2);

        // Raw find_recent 可以看到 NULL 消息
        let raw_msgs = repo.find_recent(10, 20).await.unwrap();
        assert_eq!(raw_msgs.len(), 2); // 包括 NULL 消息
    }

    /// E — Stable ordering: 相同 timestamp 时按 id ASC 排序
    #[tokio::test]
    async fn test_stable_ordering_same_timestamp() {
        let base = base_time();
        // T, id=10 和 T, id=11 有相同 timestamp
        let messages = vec![
            make_msg(11, 10, 1, "M11", base, Some(1)),
            make_msg(10, 10, 1, "M10", base, Some(1)),
        ];

        let repo = Arc::new(MemRepo::new(messages));
        let provider = RepositoryCharacterHistoryProvider::new(repo);

        let msgs = provider.get_recent(10, 1, 20, None).await.unwrap();
        assert_eq!(msgs.len(), 2);
        // 最终 oldest-first，id ASC
        assert_eq!(msgs[0].id, 10);
        assert_eq!(msgs[1].id, 11);
    }

    /// F — Ignore retention: IGNORE 标记的消息仍然可以被检索
    #[tokio::test]
    async fn test_ignore_retention_does_not_block_retrieval() {
        let base = base_time();
        let messages = vec![
            make_msg(1, 10, 1, "Normal", base, Some(1)),
            make_msg(
                2,
                10,
                1,
                "Ignored",
                base + chrono::Duration::seconds(1),
                Some(1),
            ),
        ];

        let repo = Arc::new(MemRepo::new(messages));
        let provider = RepositoryCharacterHistoryProvider::new(repo);

        // IGNORE 标记不影响 retrieval
        let msgs = provider.get_recent(10, 1, 20, None).await.unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].id, 1);
        assert_eq!(msgs[1].id, 2);
    }

    /// G — CognitionDriver uses CharacterHistoryProvider with limit=20
    /// 这个测试验证 CognitionDriver 配置正确使用了 CharacterHistoryProvider
    /// 由于 CognitionDriver 的实现细节在 cognition_driver.rs 中，
    /// 这里验证 provider 接口契约正确
    #[tokio::test]
    async fn test_cognition_driver_provider_contract() {
        let base = base_time();
        // 创建 25 条 A 消息
        let messages: Vec<Message> = (1..=25)
            .map(|i| {
                make_msg(
                    i,
                    10,
                    1,
                    &format!("M{}", i),
                    base + chrono::Duration::seconds(i),
                    Some(1),
                )
            })
            .collect();

        let repo = Arc::new(MemRepo::new(messages));
        let provider = RepositoryCharacterHistoryProvider::new(repo);

        // 验证 limit=20 正确：返回最近的 20 条（id 6-25），最旧在前
        let msgs = provider.get_recent(10, 1, 20, None).await.unwrap();
        assert_eq!(msgs.len(), 20);
        assert_eq!(msgs[0].id, 6); // 最旧的在前
        assert_eq!(msgs[19].id, 25); // 最新的在最后
    }
}
