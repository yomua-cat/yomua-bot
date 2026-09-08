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
}

// ---------------------------------------------------------------------------
// 记忆
// ---------------------------------------------------------------------------

#[async_trait]
pub trait MemoryRepository: Send + Sync {
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
