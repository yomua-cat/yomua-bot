//! 入站消息事务入口：平台身份去重，消息与完整事件共同提交。
use crate::application::event_bus::EventBus;
use crate::domain::event::MessageAcceptedEvent;
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
