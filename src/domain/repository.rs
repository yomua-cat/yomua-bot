//! 仓储 trait——领域与存储之间的抽象边界。
//!
//! 领域代码仅依赖这些 trait。
//! 基础设施层提供 SQLite 实现。

// 数据库主键当前统一使用 i64；跨表传递时由仓储和应用服务校验归属。

use async_trait::async_trait;

use crate::domain::character::{
    BehaviorState, Character, CharacterBinding, CharacterState, ConversationState,
};
use crate::domain::conversation::{Conversation, ConversationType, Participant};
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
    /// 按平台会话类型和外部 ID 查找会话。
    ///
    /// 外部数字 ID 在私聊和群聊命名空间中可能相同，调用方必须同时提供类型。
    /// 默认实现保留给内存测试仓储，生产 SQLite 仓储会使用带类型的 SQL 查询。
    async fn find_by_external_id_and_type(
        &self,
        external_id: &str,
        conversation_type: ConversationType,
    ) -> Result<Option<Conversation>, RepositoryError> {
        Ok(self
            .find_by_external_id(external_id)
            .await?
            .filter(|conversation| conversation.conversation_type == conversation_type))
    }
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
    /// 更新会话内显示名和元数据；旧实现默认不支持更新。
    async fn update(&self, _participant: &Participant) -> Result<(), RepositoryError> {
        Ok(())
    }

    /// 确保平台用户存在并返回全域用户 ID。
    /// 只由真实存储实现；测试仓储未实现时返回 `None`，不影响会话内身份测试。
    async fn ensure_user(
        &self,
        _platform: &str,
        _external_id: &str,
    ) -> Result<Option<i64>, RepositoryError> {
        Ok(None)
    }

    /// 将一个平台用户的历史归并到另一个用户，返回被保留的用户 ID。
    async fn merge_users(
        &self,
        _source_user_id: i64,
        _target_user_id: i64,
    ) -> Result<i64, RepositoryError> {
        Err(RepositoryError::NotImplemented("用户合并".into()))
    }

    /// 删除用户及其关联数据。调用方必须先完成显式确认。
    async fn delete_user(&self, _user_id: i64) -> Result<(), RepositoryError> {
        Err(RepositoryError::NotImplemented("用户删除".into()))
    }
}

// ---------------------------------------------------------------------------
// 消息
// ---------------------------------------------------------------------------

/// 消息接收入库所需的完整事务输入。
#[derive(Debug, Clone)]
pub struct AcceptedMessageInput {
    pub conversation_id: i64,
    pub sender_id: i64,
    pub content_json: String,
    pub timestamp: String,
    pub platform_message_id: Option<i64>,
    pub character_id: i64,
    pub event_uuid: String,
    pub outbox_payload: String,
}

/// 消息接受事务的结果。
///
/// `created` 用于区分首次投递和平台重复投递。两者都返回同一条消息的
/// canonical identity，但只有首次投递可以继续发布下游事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedMessageResult {
    pub message_id: i64,
    pub conversation_sequence: i64,
    pub ingested_at: chrono::DateTime<chrono::Utc>,
    pub created: bool,
}

#[async_trait]
pub trait MessageRepository: Send + Sync {
    async fn find_by_id(&self, id: i64) -> Result<Option<Message>, RepositoryError>;
    /// 持久化生成内容和发送状态；未知发送结果不自动补发。
    async fn find_cognition_batch(
        &self,
        _character_id: i64,
        _conversation_id: i64,
        _after_message_id: i64,
        _limit: i64,
    ) -> Result<Vec<Message>, RepositoryError> {
        Err(RepositoryError::NotImplemented("认知消息批次".into()))
    }
    async fn owns_outbox_lease(
        &self,
        _message_id: i64,
        _token: &str,
    ) -> Result<bool, RepositoryError> {
        Err(RepositoryError::NotImplemented("租约校验".into()))
    }
    async fn reply_task(
        &self,
        _message_id: i64,
    ) -> Result<Option<crate::domain::message::ReplyTask>, RepositoryError> {
        Err(RepositoryError::NotImplemented("回复任务存储".into()))
    }
    /// 判断某条入站消息是否已经有对应的 assistant 历史。
    ///
    /// 发送成功后本地历史可能因进程退出而延迟补写，恢复路径必须按任务
    /// 标识精确去重，不能依赖有限长度的最近历史窗口。
    async fn assistant_history_exists(
        &self,
        _conversation_id: i64,
        _reply_task_message_id: i64,
    ) -> Result<bool, RepositoryError> {
        Ok(false)
    }
    async fn find_unknown_reply_tasks(
        &self,
        _limit: i64,
    ) -> Result<Vec<crate::domain::message::UnknownReplyTask>, RepositoryError> {
        Err(RepositoryError::NotImplemented("未知发送任务查询".into()))
    }
    async fn find_unknown_reply_task(
        &self,
        _message_id: i64,
    ) -> Result<Option<crate::domain::message::UnknownReplyTask>, RepositoryError> {
        Err(RepositoryError::NotImplemented("未知发送任务查询".into()))
    }
    async fn save_reply_task(
        &self,
        _message_id: i64,
        _character_id: i64,
        _status: crate::domain::message::ReplyTaskStatus,
        _content: Option<&str>,
    ) -> Result<(), RepositoryError> {
        Err(RepositoryError::NotImplemented("回复任务存储".into()))
    }
    async fn save_reply_task_error(
        &self,
        message_id: i64,
        character_id: i64,
        status: crate::domain::message::ReplyTaskStatus,
        content: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), RepositoryError> {
        let _ = error;
        self.save_reply_task(message_id, character_id, status, content)
            .await
    }
    /// 在恢复路径中把尚未确认的发送任务原子转为 unknown。
    /// 默认实现兼容简单测试仓储；生产 SQLite 实现会限定原状态为 sending。
    async fn mark_reply_task_unknown_if_sending(
        &self,
        message_id: i64,
        character_id: i64,
        error: &str,
    ) -> Result<bool, RepositoryError> {
        self.save_reply_task_error(
            message_id,
            character_id,
            crate::domain::message::ReplyTaskStatus::Unknown,
            None,
            Some(error),
        )
        .await
        .map(|_| true)
    }
    async fn save_reply_task_delivery(
        &self,
        message_id: i64,
        character_id: i64,
        status: crate::domain::message::ReplyTaskStatus,
        content: Option<&str>,
        error: Option<&str>,
        platform_message_id: Option<i64>,
    ) -> Result<(), RepositoryError> {
        let _ = (error, platform_message_id);
        self.save_reply_task(message_id, character_id, status, content)
            .await
    }

    /// 在人工确认平台确实未发送后，将未知任务恢复为可安全重试状态。
    /// 实现必须同时重置对应的 MessageAccepted outbox 事件，保证重试仍沿原有链路执行。
    async fn requeue_unknown_reply_task(&self, _message_id: i64) -> Result<bool, RepositoryError> {
        Err(RepositoryError::NotImplemented("未知回复任务重试".into()))
    }

    /// 平台核查确认消息已经发送后，将 unknown 任务原子标记为 sent。
    async fn confirm_unknown_reply_task(
        &self,
        _message_id: i64,
        _platform_message_id: i64,
    ) -> Result<bool, RepositoryError> {
        Err(RepositoryError::NotImplemented("未知回复任务确认".into()))
    }

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
        _input: &AcceptedMessageInput,
    ) -> Result<(i64, i64, chrono::DateTime<chrono::Utc>), RepositoryError> {
        Err(RepositoryError::NotImplemented(
            "accept_message not implemented".to_string(),
        ))
    }

    /// 接受消息并返回是否创建了新记录。
    ///
    /// 默认实现兼容只实现基础接口的测试仓储；生产仓储应覆盖此方法，
    /// 以便平台重复投递不会再次触发事件。
    async fn accept_message_with_status(
        &self,
        input: &AcceptedMessageInput,
    ) -> Result<AcceptedMessageResult, RepositoryError> {
        let (message_id, conversation_sequence, ingested_at) = self.accept_message(input).await?;
        Ok(AcceptedMessageResult {
            message_id,
            conversation_sequence,
            ingested_at,
            created: true,
        })
    }
}

// ---------------------------------------------------------------------------
// 记忆
// ---------------------------------------------------------------------------

#[async_trait]
pub trait MemoryRepository: Send + Sync {
    async fn find_by_id(&self, id: i64) -> Result<Option<Memory>, RepositoryError>;

    /// 自动确认可以由程序安全判断的候选记忆。
    ///
    /// 只有带可靠来源且由确定性规则提取的 statement 候选适合走默认路径；
    /// 模型推断的 inference 候选必须继续等待更严格的复核。
    async fn promote_safe_candidates(&self, _limit: i64) -> Result<u64, RepositoryError> {
        Ok(0)
    }

    /// 判断某条来源消息是否已经生成候选记忆，抵御 Outbox 重试的重复副作用。
    async fn candidate_exists_for_source(
        &self,
        _character_id: i64,
        _conversation_id: Option<i64>,
        _source_message_id: i64,
    ) -> Result<bool, RepositoryError> {
        Ok(false)
    }

    /// 带来源的候选记忆幂等插入。已存在时返回 None。
    async fn insert_candidate_once(&self, memory: &Memory) -> Result<Option<i64>, RepositoryError> {
        let Some(source_message_id) = memory
            .metadata
            .get("source_message_id")
            .and_then(|value| value.as_i64())
        else {
            return Ok(Some(self.insert(memory).await?));
        };
        if self
            .candidate_exists_for_source(
                memory.character_id,
                memory.conversation_id,
                source_message_id,
            )
            .await?
        {
            return Ok(None);
        }
        Ok(Some(self.insert(memory).await?))
    }

    async fn cognition_progress(
        &self,
        _character_id: i64,
        _conversation_id: i64,
    ) -> Result<i64, RepositoryError> {
        Err(RepositoryError::NotImplemented("认知处理进度".into()))
    }
    async fn commit_cognition_batch(
        &self,
        _character_id: i64,
        _conversation_id: i64,
        _expected_progress: i64,
        _new_progress: i64,
        _memories: &[Memory],
    ) -> Result<bool, RepositoryError> {
        Err(RepositoryError::NotImplemented("认知事务提交".into()))
    }
    async fn find_visible(
        &self,
        character_id: i64,
        conversation_id: i64,
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError> {
        // 默认实现也必须先隔离，不能截断后才过滤。
        let mut items = self
            .find_by_character_id(character_id, None, i64::MAX)
            .await?;
        items.retain(|m| {
            m.metadata.get("status").and_then(|value| value.as_str()) != Some("candidate")
                && m.conversation_id == Some(conversation_id)
        });
        items.truncate(limit.max(0) as usize);
        Ok(items)
    }

    /// 按当前消息作者的全域用户身份读取可见记忆。
    /// 默认实现退化为会话范围查询，SQLite 实现只额外加入同一用户明确共享的记忆。
    async fn find_visible_for_participant(
        &self,
        character_id: i64,
        conversation_id: i64,
        _participant_id: i64,
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError> {
        self.find_visible(character_id, conversation_id, limit)
            .await
    }

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

    /// 仅在持久化内容仍与 `expected` 一致时更新记忆。
    ///
    /// 控制端的自然语言修改会经过一次模型调用；调用期间可能有后台整理或
    /// 另一个管理员修改同一条记忆。生产仓储应使用数据库条件更新保证不会
    /// 用旧快照覆盖新内容，测试仓储可以使用默认的比较实现。
    async fn update_if_unchanged(
        &self,
        memory: &Memory,
        expected: &Memory,
    ) -> Result<bool, RepositoryError> {
        let Some(current) = self.find_by_id(memory.id).await? else {
            return Err(RepositoryError::NotFound(format!(
                "memory {} not found",
                memory.id
            )));
        };
        if current.content != expected.content
            || current.importance != expected.importance
            || current.last_accessed != expected.last_accessed
            || current.metadata != expected.metadata
        {
            return Ok(false);
        }
        self.update(memory).await?;
        Ok(true)
    }
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
    /// 插入可选的向量记忆；该接口使用独立表，不代表默认上下文已启用向量检索。
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
    /// 按会话参与者解析其全域用户后查找关系。
    async fn find(
        &self,
        character_id: i64,
        participant_id: i64,
    ) -> Result<Option<Relationship>, RepositoryError>;
    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Vec<Relationship>, RepositoryError>;
    /// 按稳定平台用户键查找跨会话关系。
    async fn find_by_stable_user_key(
        &self,
        _character_id: i64,
        _stable_user_key: &str,
    ) -> Result<Option<Relationship>, RepositoryError> {
        Ok(None)
    }
    /// 原子地解析角色与稳定用户的关系归属。
    ///
    /// 会话内 participant 只是展示和消息归属，关系必须通过稳定用户键归并。
    /// 实现应在并发首次遇到同一用户时返回同一个 participant，避免产生两份关系。
    async fn ensure_stable_relationship_participant(
        &self,
        character_id: i64,
        participant_id: i64,
        stable_user_key: &str,
    ) -> Result<i64, RepositoryError> {
        if let Some(existing) = self
            .find_by_stable_user_key(character_id, stable_user_key)
            .await?
        {
            let _ = existing;
            return Ok(participant_id);
        }
        if self.find(character_id, participant_id).await?.is_none() {
            self.upsert(&Relationship::new(character_id, participant_id))
                .await?;
        }
        Ok(participant_id)
    }
    /// 按会话参与者创建默认关系。生产仓储必须先解析 participant.user_id。
    async fn create_for_participant(
        &self,
        _character_id: i64,
        _participant_id: i64,
    ) -> Result<Relationship, RepositoryError> {
        Err(RepositoryError::NotImplemented(
            "按参与者创建全域关系".into(),
        ))
    }
    async fn upsert(&self, relationship: &Relationship) -> Result<(), RepositoryError>;
    /// 以来源消息 ID 幂等记录一次交互，避免 Outbox 重试重复增加关系熟悉度。
    async fn record_interaction_once(
        &self,
        _character_id: i64,
        _participant_id: i64,
        _source_message_id: i64,
    ) -> Result<bool, RepositoryError> {
        Err(RepositoryError::NotImplemented("关系交互幂等记录".into()))
    }
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
    /// 使用调度器实际配置的最大尝试次数恢复过期事件。
    /// 默认实现兼容只提供固定默认值的测试仓储。
    async fn recover_expired_processing_with_limit(
        &self,
        _max_attempts: i64,
    ) -> Result<i64, RepositoryError> {
        self.recover_expired_processing().await
    }
    async fn renew_lease(
        &self,
        _event_uuid: &str,
        _token: &str,
        _duration_secs: i64,
    ) -> Result<bool, RepositoryError> {
        Err(RepositoryError::NotImplemented("租约续期".into()))
    }

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
