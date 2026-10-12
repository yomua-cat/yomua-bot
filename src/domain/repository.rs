//! 仓储 trait——领域与存储之间的抽象边界。
//!
//! 领域代码仅依赖这些 trait。
//! 基础设施层提供 SQLite 实现。

// TODO(AUDIT-061): 所有 ID 字段（character_id、message_id 等）直接使用 i64，
//   没有 newtype 封装，存在类型安全风险（如 character_id 和 message_id 混用）。
//   建议 Phase 9 为每个 ID 类型创建独立 newtype（如 `CharacterId(i64)`），在 trait 签名中
//   使用这些类型替代裸 i64，防止传错 ID 类型导致的隐性 bug。

use async_trait::async_trait;

use crate::domain::character::{
    BehaviorState, Character, CharacterBinding, CharacterState, ConversationState,
};
use crate::domain::conversation::{Conversation, Participant};
use crate::domain::emotion::Mood;
use crate::domain::memory::{Memory, SemanticMatchResult};
use crate::domain::message::Message;
use crate::domain::relationship::Relationship;
use crate::error::RepositoryError;

// ---------------------------------------------------------------------------
// 角色
// ---------------------------------------------------------------------------

#[async_trait]
pub trait CharacterRepository: Send + Sync {
    async fn find_by_id(&self, id: i64) -> Result<Option<Character>, RepositoryError>;
    async fn find_all(&self) -> Result<Vec<Character>, RepositoryError>;
    async fn insert(&self, character: &Character) -> Result<i64, RepositoryError>;
    async fn update(&self, character: &Character) -> Result<(), RepositoryError>;
    async fn delete(&self, id: i64) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 角色状态
// ---------------------------------------------------------------------------

#[async_trait]
pub trait CharacterStateRepository: Send + Sync {
    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Option<CharacterState>, RepositoryError>;

    async fn upsert(
        &self,
        character_id: i64,
        state: &CharacterState,
    ) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 会话状态（Character × Conversation 范围）
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ConversationStateRepository: Send + Sync {
    async fn find_by_character_and_conversation(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<Option<ConversationState>, RepositoryError>;

    async fn upsert(
        &self,
        character_id: i64,
        conversation_id: i64,
        state: &ConversationState,
    ) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 行为状态（Character × Conversation 范围）
// ---------------------------------------------------------------------------

#[async_trait]
pub trait BehaviorStateRepository: Send + Sync {
    async fn find_by_character_and_conversation(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<Option<BehaviorState>, RepositoryError>;

    async fn upsert(
        &self,
        character_id: i64,
        conversation_id: i64,
        state: &BehaviorState,
    ) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 情绪（Mood，Character × Conversation 范围）
// ---------------------------------------------------------------------------

#[async_trait]
pub trait MoodRepository: Send + Sync {
    async fn find_by_character_and_conversation(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<Option<Mood>, RepositoryError>;

    async fn upsert(
        &self,
        character_id: i64,
        conversation_id: i64,
        mood: &Mood,
    ) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 角色绑定
// ---------------------------------------------------------------------------

#[async_trait]
pub trait CharacterBindingRepository: Send + Sync {
    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Vec<CharacterBinding>, RepositoryError>;
    async fn find_by_conversation_id(
        &self,
        conversation_id: i64,
    ) -> Result<Vec<CharacterBinding>, RepositoryError>;
    async fn find_all(&self) -> Result<Vec<CharacterBinding>, RepositoryError>;
    async fn find_all_enabled(&self) -> Result<Vec<CharacterBinding>, RepositoryError>;
    async fn insert(&self, binding: &CharacterBinding) -> Result<i64, RepositoryError>;
    async fn update(&self, binding: &CharacterBinding) -> Result<(), RepositoryError>;
    async fn delete(&self, id: i64) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 会话
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ConversationRepository: Send + Sync {
    async fn find_by_id(&self, id: i64) -> Result<Option<Conversation>, RepositoryError>;
    async fn find_by_external_id(
        &self,
        external_id: &str,
    ) -> Result<Option<Conversation>, RepositoryError>;
    async fn find_all(&self) -> Result<Vec<Conversation>, RepositoryError>;
    async fn insert(&self, conversation: &Conversation) -> Result<i64, RepositoryError>;
    async fn update(&self, conversation: &Conversation) -> Result<(), RepositoryError>;
    async fn delete(&self, id: i64) -> Result<(), RepositoryError>;
    /// 原子分配下一个 Conversation Sequence。
    ///
    /// 在一个事务内原子地递增 conversation 的 sequence_counter 并返回新值。
    /// 用于实现 QQBOT Application Ingress Order。
    async fn allocate_sequence(&self, conversation_id: i64) -> Result<i64, RepositoryError>;
}

// ---------------------------------------------------------------------------
// 参与者
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ParticipantRepository: Send + Sync {
    async fn find_by_id(&self, id: i64) -> Result<Option<Participant>, RepositoryError>;
    async fn find_by_external_id(
        &self,
        conversation_id: i64,
        external_id: &str,
    ) -> Result<Option<Participant>, RepositoryError>;
    async fn find_by_conversation_id(
        &self,
        conversation_id: i64,
    ) -> Result<Vec<Participant>, RepositoryError>;
    async fn insert(&self, participant: &Participant) -> Result<i64, RepositoryError>;
}

// ---------------------------------------------------------------------------
// 消息
// ---------------------------------------------------------------------------

#[async_trait]
pub trait MessageRepository: Send + Sync {
    async fn find_by_id(&self, id: i64) -> Result<Option<Message>, RepositoryError>;
    async fn find_recent(
        &self,
        conversation_id: i64,
        limit: i64,
    ) -> Result<Vec<Message>, RepositoryError>;
    /// 检索当前 Conversation 中归属于指定 Character 的最近消息。
    ///
    /// SQL 语义：
    /// ```sql
    /// WHERE conversation_id = ? AND active_character_id = ?
    /// ORDER BY timestamp DESC, id DESC
    /// LIMIT ?
    /// ```
    /// 然后反转顺序，使最旧的消息排在前面（自然先后顺序）。
    ///
    /// 注意：WHERE 条件在 LIMIT 之前应用，保证 LIMIT 作用于
    /// 已按 Character attribution 过滤后的候选集。
    async fn find_recent_attributed(
        &self,
        conversation_id: i64,
        character_id: i64,
        limit: i64,
    ) -> Result<Vec<Message>, RepositoryError>;
    async fn insert(&self, message: &Message) -> Result<i64, RepositoryError>;
    async fn latest_message_time(
        &self,
        conversation_id: i64,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, RepositoryError>;
    /// 回填某条消息的 active_character_id（在 reply_processor 确定 Active 角色后调用）。
    /// 对于不支持此功能的存储实现，默认 no-op。
    async fn update_active_character_id(
        &self,
        message_id: i64,
        active_character_id: i64,
    ) -> Result<(), RepositoryError> {
        let _ = (message_id, active_character_id);
        Ok(())
    }
    /// 按 (conversation_id, sender_id, timestamp, content) 精确去重查找，用于防止重复插入。
    /// 不存在时返回 Ok(None)。不支持此功能的实现返回 Ok(None)。
    async fn find_by_conversation_sender_time_content(
        &self,
        _conversation_id: i64,
        _sender_id: i64,
        _timestamp: chrono::DateTime<chrono::Utc>,
        _content: &str,
    ) -> Result<Option<Message>, RepositoryError> {
        Ok(None)
    }

    /// 检索指定 Conversation 中，在给定 sequence 之前的消息（按 character attribution 过滤）。
    ///
    /// SQL 语义：
    /// ```sql
    /// WHERE conversation_id = ?
    ///   AND active_character_id = ?
    ///   AND conversation_sequence < ?
    /// ORDER BY conversation_sequence ASC
    /// ```
    /// 返回升序排列的消息（自然先后顺序）。
    ///
    /// 用于 History boundary：A 的 History 应只包含 sequence < A.sequence 的已提交消息。
    async fn find_before_sequence(
        &self,
        conversation_id: i64,
        character_id: i64,
        before_sequence: i64,
        limit: i64,
    ) -> Result<Vec<Message>, RepositoryError> {
        let _ = (conversation_id, character_id, before_sequence, limit);
        Ok(Vec::new())
    }

    /// 检索指定 Conversation 中，最近的消息（按 sequence 排序）。
    ///
    /// SQL 语义：
    /// ```sql
    /// WHERE conversation_id = ?
    /// ORDER BY conversation_sequence DESC
    /// LIMIT ?
    /// ```
    /// 然后反转顺序，使最旧的消息排在前面。
    async fn find_recent_by_sequence(
        &self,
        conversation_id: i64,
        limit: i64,
    ) -> Result<Vec<Message>, RepositoryError> {
        let _ = (conversation_id, limit);
        Ok(Vec::new())
    }

    /// 接受一条消息：在单一事务内分配 sequence、生成 identity 并插入。
    ///
    /// 这是 C2-3 Temporal Architecture 的核心操作，保证：
    /// - sequence allocation 和 message insert 在同一事务内
    /// - 事务提交后，message 才对下游可见
    /// - ingested_at 在事务内生成，作为 authoritative timestamp 返回给调用方
    ///
    /// # 参数
    /// - `conversation_id`: 会话 ID
    /// - `sender_id`: 发送者 ID
    /// - `content_json`: 序列化的消息内容
    /// - `timestamp`: 平台时间
    /// - `platform_message_id`: 外部平台消息 ID
    /// - `character_id`: active character ID
    ///
    /// # 返回
    /// - `Ok((message_id, conversation_sequence, ingested_at))`
    ///   ingested_at 是事务内生成的 authoritative application timestamp
    /// - C2-4: outbox_payload_json 和 event_uuid 在同一事务内提交
    async fn accept_message(
        &self,
        conversation_id: i64,
        sender_id: i64,
        content_json: &str,
        timestamp: &str,
        platform_message_id: Option<i64>,
        character_id: i64,
        _event_uuid: &str,
        _outbox_payload: &str,
    ) -> Result<(i64, i64, chrono::DateTime<chrono::Utc>), RepositoryError> {
        let _ = (
            conversation_id,
            sender_id,
            content_json,
            timestamp,
            platform_message_id,
            character_id,
        );
        Err(RepositoryError::NotImplemented(
            "accept_message not implemented".to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// 记忆
// ---------------------------------------------------------------------------

#[async_trait]
pub trait MemoryRepository: Send + Sync {
    async fn find_by_id(&self, id: i64) -> Result<Option<Memory>, RepositoryError>;

    async fn find_by_character_id(
        &self,
        character_id: i64,
        memory_type: Option<crate::domain::memory::MemoryType>,
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError>;

    async fn search_by_keywords(
        &self,
        character_id: i64,
        keywords: &[String],
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError> {
        let _ = (character_id, keywords, limit);
        Ok(Vec::new())
    }

    async fn insert(&self, memory: &Memory) -> Result<i64, RepositoryError>;
    async fn update(&self, memory: &Memory) -> Result<(), RepositoryError>;
    async fn delete(&self, id: i64) -> Result<(), RepositoryError>;

    /// 查询所有记忆（全局列表）。
    /// 按 importance DESC, created_at DESC, id DESC 排序，确保确定性。
    async fn find_all(&self, limit: i64) -> Result<Vec<Memory>, RepositoryError>;

    /// 按会话查询记忆。
    async fn find_by_conversation_id(
        &self,
        conversation_id: i64,
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError>;

    /// 通用过滤查询。
    /// 所有过滤条件为 AND 关系，任一为 None 则不加入过滤。
    async fn find_filtered(
        &self,
        character_id: Option<i64>,
        conversation_id: Option<i64>,
        memory_type: Option<crate::domain::memory::MemoryType>,
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError>;

    async fn search_by_embedding(
        &self,
        character_id: i64,
        query_embedding: &[f32],
        memory_type: Option<&str>,
        limit: i64,
    ) -> Result<Vec<SemanticMatchResult>, RepositoryError> {
        let _ = (character_id, query_embedding, memory_type, limit);
        Ok(Vec::new())
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_semantic(
        &self,
        character_id: i64,
        conversation_id: Option<i64>,
        memory_type: &str,
        content: &str,
        embedding: &[f32],
        importance: f64,
        metadata: &str,
    ) -> Result<i64, RepositoryError> {
        let _ = (
            character_id,
            conversation_id,
            memory_type,
            content,
            embedding,
            importance,
            metadata,
        );
        Err(RepositoryError::Internal(
            "insert_semantic not implemented for in-memory repository".to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// 关系
// ---------------------------------------------------------------------------

#[async_trait]
pub trait RelationshipRepository: Send + Sync {
    async fn find(
        &self,
        character_id: i64,
        participant_id: i64,
    ) -> Result<Option<Relationship>, RepositoryError>;
    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Vec<Relationship>, RepositoryError>;
    async fn upsert(&self, relationship: &Relationship) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 插件数据
// ---------------------------------------------------------------------------

#[async_trait]
pub trait PluginDataRepository: Send + Sync {
    async fn get(
        &self,
        plugin_name: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>, RepositoryError>;
    async fn set(
        &self,
        plugin_name: &str,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), RepositoryError>;
    async fn delete(&self, plugin_name: &str, key: &str) -> Result<(), RepositoryError>;
    async fn list_keys(&self, plugin_name: &str) -> Result<Vec<String>, RepositoryError>;
}

// ---------------------------------------------------------------------------
// 黑名单
// ---------------------------------------------------------------------------

use crate::domain::blacklist::{BlacklistEntry, BlacklistType, NewBlacklistEntry};
use crate::domain::outbox::OutboxEvent;

#[async_trait]
pub trait BlacklistRepository: Send + Sync {
    /// 列出所有黑名单条目（含已过期的）。
    async fn find_all(&self) -> Result<Vec<BlacklistEntry>, RepositoryError>;

    /// 按 target 精确查找。
    async fn find_by_target(
        &self,
        target: &str,
        target_type: BlacklistType,
    ) -> Result<Option<BlacklistEntry>, RepositoryError>;

    /// 判断某 target 是否在有效黑名单中（未过期）。
    async fn is_blacklisted(
        &self,
        target: &str,
        target_type: BlacklistType,
    ) -> Result<bool, RepositoryError>;

    /// 添加新条目。
    async fn insert(&self, entry: &NewBlacklistEntry) -> Result<i64, RepositoryError>;

    /// 更新条目的 reason（不修改 expires_at）。
    async fn update_reason(&self, id: i64, reason: Option<String>) -> Result<(), RepositoryError>;

    /// 设置过期时间（用于临时封禁）。
    async fn set_expiration(
        &self,
        id: i64,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), RepositoryError>;

    /// 删除条目。
    async fn delete(&self, id: i64) -> Result<(), RepositoryError>;

    /// 按 target 删除（用于 remove 操作）。
    async fn delete_by_target(
        &self,
        target: &str,
        target_type: BlacklistType,
    ) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// Outbox（耐用事件投递）
// ---------------------------------------------------------------------------

/// Outbox 事件仓储。
///
/// 负责管理 outbox_events 表，提供：
/// - 原子 claim（带 lease）
/// - 状态转换（pending → processing → completed/retry_wait/dead_letter）
/// - 按 ordering_key + ordering_seq 查询（per-conversation 排序）
#[async_trait::async_trait]
pub trait OutboxRepository: Send + Sync {
    /// 尝试原子性地 claim 一个可处理的事件。
    ///
    /// 条件：
    /// - status IN (pending, retry_wait)
    /// - available_at <= now
    /// - locked_until IS NULL OR locked_until <= now
    /// - 前一个 ordering_seq 已完成（completed 或 dead_letter）或不存在
    ///
    /// 如果找到则原子性地：
    /// - 将 status 更新为 processing
    /// - 设置 locked_until = now + lease_duration
    /// - attempts += 1
    ///
    /// 返回 claim 的事件，如果没有任何可 claim 的事件则返回 None。
    async fn claim_next_event(
        &self,
        lease_duration_secs: i64,
    ) -> Result<Option<OutboxEvent>, RepositoryError>;

    /// 添加一个 outbox 事件（用于测试）。
    async fn add_outbox_event(&self, event: OutboxEvent) -> Result<(), RepositoryError>;

    /// 强制使指定事件的 lease 过期（用于测试）。
    async fn force_expire_lease(&self, event_uuid: &str) -> Result<(), RepositoryError>;

    /// 更新事件状态为已完成。
    ///
    /// 只能从 processing 状态转换。
    /// lease_token 必须匹配当前持有者的 token，否则静默跳过。
    async fn mark_completed(
        &self,
        event_uuid: &str,
        lease_token: &str,
    ) -> Result<(), RepositoryError>;

    /// 将事件状态更新为 retry_wait，并设置下次可用的 available_at 时间。
    ///
    /// 只能从 processing 状态转换。
    /// available_at 使用指数退避策略计算。
    /// lease_token 必须匹配当前持有者的 token，否则静默跳过。
    async fn mark_retry_wait(
        &self,
        event_uuid: &str,
        lease_token: &str,
        error: &str,
        next_available_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), RepositoryError>;

    /// 将事件状态更新为 dead_letter（永久失败）。
    ///
    /// 只能从 processing 状态转换。
    /// lease_token 必须匹配当前持有者的 token，否则静默跳过。
    async fn mark_dead_letter(
        &self,
        event_uuid: &str,
        lease_token: &str,
        error: &str,
    ) -> Result<(), RepositoryError>;

    /// 查询指定 ordering_key 中最大的已完成 ordering_seq。
    ///
    /// 用于判断下一个可处理的 seq。
    async fn max_completed_seq(&self, ordering_key: &str) -> Result<Option<i64>, RepositoryError>;

    /// 查询指定 ordering_key 中最大的 ordering_seq（无论状态）。
    ///
    /// 用于启动恢复。
    async fn max_seq(&self, ordering_key: &str) -> Result<Option<i64>, RepositoryError>;

    /// 恢复所有已过期的 processing 事件（进程崩溃恢复）。
    ///
    /// 将 locked_until <= now 的 processing 事件重置为 pending，
    /// 以便下次 poll 可以重新 claim。
    async fn recover_expired_processing(&self) -> Result<i64, RepositoryError>;

    /// 按 event_uuid 查询单个事件。
    async fn find_by_uuid(&self, event_uuid: &str) -> Result<Option<OutboxEvent>, RepositoryError>;

    /// 列出所有终态（completed 或 dead_letter）的事件。
    ///
    /// 用于调试和监控。
    async fn list_terminal_events(&self, limit: i64) -> Result<Vec<OutboxEvent>, RepositoryError>;

    /// 获取当前状态为 processing 的事件数量（用于监控）。
    async fn processing_count(&self) -> Result<i64, RepositoryError>;

    /// 获取当前状态为 pending 或 retry_wait 的事件数量（用于监控）。
    async fn pending_count(&self) -> Result<i64, RepositoryError>;
}
