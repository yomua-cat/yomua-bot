//! 会话与参与者管理 —— 将平台外部 ID 解析为核心内部 ID。
//!
//! 适配器从平台事件中只得到外部 ID（例如 QQ 群号 / 用户号）。
//! 本模块负责将这些外部 ID 映射为核心内部的 `conversation_id` / `participant_id`，
//! 并在首次遇到时创建对应的行。核心本身不理解外部 ID 的语义。

use std::sync::Arc;

use chrono::Utc;

use crate::domain::conversation::{Conversation, ConversationType, Participant, ParticipantRole};
use crate::domain::repository::{ConversationRepository, ParticipantRepository};
use crate::error::RuntimeError;

/// 一条已解析的入站消息（外部 ID 已转换为核心 ID）。
#[derive(Debug, Clone)]
pub struct ResolvedMessage {
    /// 核心会话 ID。
    pub conversation_id: i64,
    /// 核心发送者（参与者）ID。
    pub sender_id: i64,
    /// 平台消息 ID（可选，用于去重/跟踪）。
    pub platform_message_id: Option<i64>,
    /// 消息内容。
    pub content: String,
    /// 消息时间戳。
    pub timestamp: chrono::DateTime<Utc>,
}

/// 会话管理器 —— 负责外部 ID → 核心 ID 的解析与按需创建。
#[derive(Clone)]
pub struct ConversationManager {
    conversation_repo: Arc<dyn ConversationRepository>,
    participant_repo: Arc<dyn ParticipantRepository>,
}

impl ConversationManager {
    /// 创建一个会话管理器。
    pub fn new(
        conversation_repo: Arc<dyn ConversationRepository>,
        participant_repo: Arc<dyn ParticipantRepository>,
    ) -> Self {
        Self {
            conversation_repo,
            participant_repo,
        }
    }

    /// 按外部 ID 与会话类型查找或创建会话，返回核心会话 ID。
    pub async fn resolve_or_create_conversation(
        &self,
        conversation_type: ConversationType,
        external_id: &str,
    ) -> Result<i64, RuntimeError> {
        if let Some(conv) = self
            .conversation_repo
            .find_by_external_id_and_type(external_id, conversation_type)
            .await?
        {
            return Ok(conv.id);
        }

        // 首次遇到 —— 创建会话。
        let now = Utc::now();
        let conversation = Conversation {
            id: 0,
            conversation_type,
            external_id: external_id.to_string(),
            name: None,
            created_at: now,
            updated_at: now,
        };
        let id = self.conversation_repo.insert(&conversation).await?;
        Ok(id)
    }

    /// 只查找已经存在的会话，不创建新记录。
    ///
    /// 该入口用于平台回声、对账和其他不会创建业务身份的事件。
    pub async fn find_existing_conversation(
        &self,
        conversation_type: ConversationType,
        external_id: &str,
    ) -> Result<Option<i64>, RuntimeError> {
        Ok(self
            .conversation_repo
            .find_by_external_id_and_type(external_id, conversation_type)
            .await?
            .map(|conversation| conversation.id))
    }

    /// 在指定会话内按外部发起者 ID 查找或创建参与者，返回核心参与者 ID。
    ///
    /// `display_name` 仅在首次创建参与者时使用。
    pub async fn resolve_or_create_participant(
        &self,
        conversation_id: i64,
        external_id: &str,
        display_name: &str,
    ) -> Result<i64, RuntimeError> {
        if let Some(participant) = self
            .participant_repo
            .find_by_external_id(conversation_id, external_id)
            .await?
        {
            let mut participant = participant;
            if participant.display_name != display_name {
                participant.display_name = display_name.to_string();
                participant.metadata["display_name_updated_at"] = serde_json::json!(Utc::now());
                self.participant_repo.update(&participant).await?;
            }
            return Ok(participant.id);
        }

        let participant = Participant {
            id: 0,
            conversation_id,
            external_id: external_id.to_string(),
            display_name: display_name.to_string(),
            role: ParticipantRole::User,
            metadata: serde_json::json!({
                "platform": "onebot_qq",
                "display_name_updated_at": Utc::now(),
            }),
        };
        let id = self.participant_repo.insert(&participant).await?;
        Ok(id)
    }

    /// 只查找会话内已有参与者，不创建身份记录。
    pub async fn find_existing_participant(
        &self,
        conversation_id: i64,
        external_id: &str,
    ) -> Result<Option<i64>, RuntimeError> {
        Ok(self
            .participant_repo
            .find_by_external_id(conversation_id, external_id)
            .await?
            .map(|participant| participant.id))
    }

    /// 解析一条入站消息的外部发起者 / 会话，并返回核心 ID 元组。
    ///
    /// 这是外部 ID → 核心 ID 的核心入口。
    pub async fn resolve(
        &self,
        conversation_type: ConversationType,
        external_conversation_id: &str,
        external_sender_id: &str,
        display_name: &str,
    ) -> Result<(i64, i64), RuntimeError> {
        let conversation_id = self
            .resolve_or_create_conversation(conversation_type, external_conversation_id)
            .await?;
        let sender_id = self
            .resolve_or_create_participant(conversation_id, external_sender_id, display_name)
            .await?;
        Ok((conversation_id, sender_id))
    }
}
