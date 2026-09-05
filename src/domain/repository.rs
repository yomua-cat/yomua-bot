//! 仓储 trait——领域与存储之间的抽象边界。
//!
//! 领域代码仅依赖这些 trait。
//! 基础设施层提供 SQLite 实现。

use async_trait::async_trait;

use crate::domain::character::{Character, CharacterBinding, CharacterState};
use crate::domain::conversation::{Conversation, Participant};
use crate::domain::emotion::EmotionState;
use crate::domain::memory::{Memory, SemanticMatchResult};
use crate::domain::message::Message;
use crate::domain::relationship::Relationship;
use crate::error::RepositoryError;

// ---------------------------------------------------------------------------
// 角色
// ---------------------------------------------------------------------------

#[async_trait]
pub trait CharacterRepository: Send + Sync {
    /// 按 ID 查找一个角色。
    async fn find_by_id(&self, id: i64) -> Result<Option<Character>, RepositoryError>;

    /// 查找所有角色（仅元数据，不含完整状态）。
    async fn find_all(&self) -> Result<Vec<Character>, RepositoryError>;

    /// 插入一个新角色。返回生成的角色 ID。
    async fn insert(&self, character: &Character) -> Result<i64, RepositoryError>;

    /// 更新一个已存在的角色。
    async fn update(&self, character: &Character) -> Result<(), RepositoryError>;

    /// 按 ID 删除一个角色。
    async fn delete(&self, id: i64) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 角色状态
// ---------------------------------------------------------------------------

#[async_trait]
pub trait CharacterStateRepository: Send + Sync {
    /// 加载一个角色的状态。
    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Option<CharacterState>, RepositoryError>;

    /// 对一个角色的状态执行 upsert（插入或更新）。
    async fn upsert(
        &self,
        character_id: i64,
        state: &CharacterState,
    ) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 角色绑定
// ---------------------------------------------------------------------------

#[async_trait]
pub trait CharacterBindingRepository: Send + Sync {
    /// 查找一个角色的所有绑定。
    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Vec<CharacterBinding>, RepositoryError>;

    /// 查找一个会话的所有绑定。
    async fn find_by_conversation_id(
        &self,
        conversation_id: i64,
    ) -> Result<Vec<CharacterBinding>, RepositoryError>;

    /// 查找所有绑定（供主动行为驱动枚举）。
    async fn find_all(&self) -> Result<Vec<CharacterBinding>, RepositoryError>;

    /// 查找所有启用了主动行为的绑定（供认知驱动枚举）。
    async fn find_all_enabled(&self) -> Result<Vec<CharacterBinding>, RepositoryError>;

    /// 插入一个新绑定。
    async fn insert(&self, binding: &CharacterBinding) -> Result<i64, RepositoryError>;

    /// 更新一个既有绑定的全部字段（按 id 定位）。
    async fn update(&self, binding: &CharacterBinding) -> Result<(), RepositoryError>;

    /// 按 ID 删除一个绑定。
    async fn delete(&self, id: i64) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 会话
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ConversationRepository: Send + Sync {
    /// 按 ID 查找一个会话。
    async fn find_by_id(&self, id: i64) -> Result<Option<Conversation>, RepositoryError>;

    /// 按外部平台 ID 查找一个会话。
    async fn find_by_external_id(
        &self,
        external_id: &str,
    ) -> Result<Option<Conversation>, RepositoryError>;

    /// 查找所有会话。
    async fn find_all(&self) -> Result<Vec<Conversation>, RepositoryError>;

    /// 插入一个新会话。返回生成的会话 ID。
    async fn insert(&self, conversation: &Conversation) -> Result<i64, RepositoryError>;

    /// 更新一个已存在的会话。
    async fn update(&self, conversation: &Conversation) -> Result<(), RepositoryError>;

    /// 按 ID 删除一个会话。
    async fn delete(&self, id: i64) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 参与者
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ParticipantRepository: Send + Sync {
    /// 按 ID 查找一个参与者。
    async fn find_by_id(&self, id: i64) -> Result<Option<Participant>, RepositoryError>;

    /// 在某个会话内按外部平台 ID 查找参与者。
    async fn find_by_external_id(
        &self,
        conversation_id: i64,
        external_id: &str,
    ) -> Result<Option<Participant>, RepositoryError>;

    /// 查找一个会话中的所有参与者。
    async fn find_by_conversation_id(
        &self,
        conversation_id: i64,
    ) -> Result<Vec<Participant>, RepositoryError>;

    /// 插入一个新参与者。返回生成的参与者 ID。
    async fn insert(&self, participant: &Participant) -> Result<i64, RepositoryError>;
}

// ---------------------------------------------------------------------------
// 消息
// ---------------------------------------------------------------------------

#[async_trait]
pub trait MessageRepository: Send + Sync {
    /// 按 ID 查找一条消息。
    async fn find_by_id(&self, id: i64) -> Result<Option<Message>, RepositoryError>;

    /// 查找一个会话中的最近消息（最新的在前，数量受限）。
    async fn find_recent(
        &self,
        conversation_id: i64,
        limit: i64,
    ) -> Result<Vec<Message>, RepositoryError>;

    /// 插入一条新消息。返回生成的消息 ID。
    async fn insert(&self, message: &Message) -> Result<i64, RepositoryError>;

    /// 获取一个会话中最新消息的时间戳（用于 idle 检测）。
    async fn latest_message_time(
        &self,
        conversation_id: i64,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, RepositoryError>;
}

// ---------------------------------------------------------------------------
// 记忆
// ---------------------------------------------------------------------------

#[async_trait]
pub trait MemoryRepository: Send + Sync {
    /// 查找一个角色的记忆，可选按类型过滤。
    async fn find_by_character_id(
        &self,
        character_id: i64,
        memory_type: Option<crate::domain::memory::MemoryType>,
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError>;

    /// 按关键词检索记忆（内容子串匹配，MVP 不使用向量检索）。
    ///
    /// 默认实现返回空集，方便测试桩；SQLite 实现按 `LIKE` 检索内容。
    async fn search_by_keywords(
        &self,
        character_id: i64,
        keywords: &[String],
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError> {
        let _ = (character_id, keywords, limit);
        Ok(Vec::new())
    }

    /// 插入一条新记忆。返回生成的记忆 ID。
    async fn insert(&self, memory: &Memory) -> Result<i64, RepositoryError>;

    /// 更新一条已存在的记忆。
    async fn update(&self, memory: &Memory) -> Result<(), RepositoryError>;

    /// 按 ID 删除一条记忆。
    async fn delete(&self, id: i64) -> Result<(), RepositoryError>;

    /// 语义检索：按向量相似度 TopK 返回（纯 Rust 余弦相似度）。
    ///
    /// 默认实现返回空集，方便测试桩；SQLite 实现从 `semantic_memories` 表读取。
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

    /// 插入语义记忆到 semantic_memories 表（embedding 必须）。
    ///
    /// 默认实现返回"未实现"错误，SQLite 实现提供完整功能。
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
    /// 按角色与参与者查找一段关系。
    async fn find(
        &self,
        character_id: i64,
        participant_id: i64,
    ) -> Result<Option<Relationship>, RepositoryError>;

    /// 查找一个角色的所有关系。
    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Vec<Relationship>, RepositoryError>;

    /// upsert 一段关系。
    async fn upsert(&self, relationship: &Relationship) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 情绪
// ---------------------------------------------------------------------------

#[async_trait]
pub trait EmotionStateRepository: Send + Sync {
    /// 按角色 ID 查找情绪状态。
    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Option<EmotionState>, RepositoryError>;

    /// 对一个角色的情绪状态执行 upsert（插入或更新）。
    async fn upsert(&self, character_id: i64, state: &EmotionState) -> Result<(), RepositoryError>;
}

// ---------------------------------------------------------------------------
// 插件数据
// ---------------------------------------------------------------------------

#[async_trait]
pub trait PluginDataRepository: Send + Sync {
    /// 读取一个插件的一项持久化数据；不存在返回 `None`。
    async fn get(
        &self,
        plugin_name: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>, RepositoryError>;

    /// 写入（upsert）一个插件的一项持久化数据。
    async fn set(
        &self,
        plugin_name: &str,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), RepositoryError>;

    /// 删除一个插件的一项持久化数据。
    async fn delete(&self, plugin_name: &str, key: &str) -> Result<(), RepositoryError>;

    /// 列出某个插件的全部键。
    async fn list_keys(&self, plugin_name: &str) -> Result<Vec<String>, RepositoryError>;
}
