//! 入站消息事务入口：平台身份去重，消息与完整事件共同提交。
use crate::application::event_bus::EventBus;
use crate::domain::event::{Command, MessageAcceptedEvent};
use crate::domain::repository::{AcceptedMessageInput, MessageRepository};
use crate::error::RuntimeError;
use chrono::Utc;
use std::sync::Arc;

pub struct MessageAcceptanceService {
    message_repo: Arc<dyn MessageRepository>,
    event_bus: EventBus,
}
impl MessageAcceptanceService {
    pub fn new(message_repo: Arc<dyn MessageRepository>, event_bus: EventBus) -> Self {
        Self {
            message_repo,
            event_bus,
        }
    }
    pub async fn accept(
        &self,
        conversation_id: i64,
        sender_id: i64,
        content: String,
        timestamp: chrono::DateTime<Utc>,
        platform_message_id: Option<i64>,
        character_id: i64,
    ) -> Result<(i64, i64), RuntimeError> {
        self.accept_inbound(
            conversation_id,
            sender_id,
            content,
            timestamp,
            platform_message_id,
            character_id,
            false,
            serde_json::json!({}),
        )
        .await
    }

    /// 为不会进入消息 Outbox 的管理命令领取持久幂等凭证。
    pub async fn claim_command_message(
        &self,
        conversation_id: i64,
        platform_message_id: i64,
    ) -> Result<bool, RuntimeError> {
        self.message_repo
            .claim_command_message(conversation_id, platform_message_id)
            .await
            .map_err(RuntimeError::Repository)
    }

    pub async fn record_platform_echo(
        &self,
        conversation_id: i64,
        platform_message_id: i64,
        sender_external_id: &str,
        content: &str,
        metadata: &serde_json::Value,
    ) -> Result<(), RuntimeError> {
        self.message_repo
            .record_platform_echo(
                conversation_id,
                platform_message_id,
                sender_external_id,
                content,
                metadata,
            )
            .await
            .map_err(RuntimeError::Repository)
    }

    /// 将管理指令作为不可见于角色上下文的系统消息持久化。
    ///
    /// 指令仍需要本地消息身份和 Outbox 事件，才能在发送结果未知或进程重启
    /// 时恢复；`author_kind=system_command` 让普通回复链跳过它。
    #[allow(clippy::too_many_arguments)]
    pub async fn accept_command(
        &self,
        conversation_id: i64,
        sender_id: i64,
        content: String,
        timestamp: chrono::DateTime<Utc>,
        platform_message_id: i64,
        character_id: i64,
        command: Command,
        external_sender_id: String,
        binding_id: i64,
        binding_version: Option<chrono::DateTime<Utc>>,
    ) -> Result<(i64, i64), RuntimeError> {
        self.accept_inbound(
            conversation_id,
            sender_id,
            content,
            timestamp,
            Some(platform_message_id),
            character_id,
            false,
            serde_json::json!({
                "author_kind": "system_command",
                "command": command,
                "external_sender_id": external_sender_id,
                "binding_id": binding_id,
                "binding_version": binding_version,
            }),
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn accept_inbound(
        &self,
        conversation_id: i64,
        sender_id: i64,
        content: String,
        timestamp: chrono::DateTime<Utc>,
        platform_message_id: Option<i64>,
        character_id: i64,
        is_mentioned: bool,
        metadata: serde_json::Value,
    ) -> Result<(i64, i64), RuntimeError> {
        let event_uuid = uuid::Uuid::new_v4().to_string();
        let payload = serde_json::to_value(MessageAcceptedEvent {
            event_uuid: event_uuid.clone(),
            conversation_id,
            sender_id,
            message_id: 0,
            content: content.clone(),
            timestamp,
            is_mentioned,
            character_id,
            conversation_sequence: 0,
            platform_message_id,
            ingested_at: Utc::now(),
            metadata: metadata.clone(),
        })
        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
        let event_content = content.clone();
        let content_json =
            serde_json::to_string(&crate::domain::message::MessageContent::Text(content))
                .map_err(|e| RuntimeError::Internal(e.to_string()))?;
        let result = self
            .message_repo
            .accept_message_with_status(&AcceptedMessageInput {
                conversation_id,
                sender_id,
                content_json,
                timestamp: timestamp.to_rfc3339(),
                platform_message_id,
                character_id,
                event_uuid: event_uuid.clone(),
                outbox_payload: payload.to_string(),
            })
            .await?;
        if result.created {
            self.event_bus
                .publish(&crate::domain::event::CoreEvent::MessageAccepted(
                    MessageAcceptedEvent {
                        event_uuid,
                        conversation_id,
                        sender_id,
                        message_id: result.message_id,
                        content: event_content,
                        timestamp,
                        is_mentioned,
                        character_id,
                        conversation_sequence: result.conversation_sequence,
                        platform_message_id,
                        ingested_at: result.ingested_at,
                        metadata,
                    },
                ));
        }
        Ok((result.message_id, result.conversation_sequence))
    }
}
