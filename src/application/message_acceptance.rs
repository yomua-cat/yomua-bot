//! 消息接受服务 —— C2-3 Temporal Architecture
//!
//! 负责将 OneBot 入站消息转换为 canonical Message 并保证事务边界：
//!
//! ```text
//! BEGIN
//!     allocate Conversation sequence
//!     create canonical Message (messages.id is canonical identity)
//!     persist Message
//! COMMIT
//!     ↓
//! publish MessageAccepted / downstream event
//! ```
//!
//! **注意**：此服务不承担去重职责。去重由 `MessagePersistence` 在接受到 `MessageReceived`
//! 事件后处理。但在新架构下，`MessageAccepted` 事件在 commit 之后发布，
//! 此时 message 已经持久化，去重已无意义（幂等性由 sequence 唯一性保证）。

use std::sync::Arc;

use chrono::Utc;
use serde_json::json;

use crate::application::event_bus::EventBus;
use crate::domain::event::{CoreEvent, MessageAcceptedEvent};
use crate::domain::repository::MessageRepository;
use crate::error::RuntimeError;

/// 消息接受服务。
///
/// 接收原始入站消息数据，执行：
/// 1. sequence allocation（原子递增）
/// 2. messages.id 作为 canonical identity
/// 3. Message 构造与持久化
/// 4. COMMIT
/// 5. 发布下游事件
pub struct MessageAcceptanceService {
    message_repo: Arc<dyn MessageRepository>,
    event_bus: EventBus,
}

impl MessageAcceptanceService {
    /// 创建消息接受服务。
    pub fn new(message_repo: Arc<dyn MessageRepository>, event_bus: EventBus) -> Self {
        Self {
            message_repo,
            event_bus,
        }
    }

    /// 接受一条入站消息，执行 commit-before-publish 事务。
    ///
    /// # 参数
    /// - `conversation_id`: 会话 ID
    /// - `sender_id`: 发送者 ID
    /// - `content`: 消息内容
    /// - `platform_timestamp`: 平台时间（OneBot/NapCat）
    /// - `platform_message_id`: 外部平台消息 ID
    /// - `character_id`: 消息接收时的 active character ID
    ///
    /// # 返回
    /// - `Ok((message_id, conversation_sequence))` - canonical identity 和 ingress order
    /// - `Err` - 事务失败
    pub async fn accept(
        &self,
        conversation_id: i64,
        sender_id: i64,
        content: String,
        platform_timestamp: chrono::DateTime<Utc>,
        platform_message_id: Option<i64>,
        character_id: i64,
    ) -> Result<(i64, i64), RuntimeError> {
        // 1. 生成稳定的 event_uuid 前缀（在事务外生成，用于 outbox payload）
        let event_uuid_prefix = uuid::Uuid::new_v4().to_string();

        // 2. 构建临时 event 用于 outbox payload（序列化为 JSON）
        let temp_event = MessageAcceptedEvent {
            event_uuid: format!("{}-MessageAccepted", event_uuid_prefix),
            conversation_id,
            sender_id,
            message_id: 0, // placeholder
            content: content.clone(),
            timestamp: platform_timestamp,
            is_mentioned: false,
            character_id,
            conversation_sequence: 0, // placeholder
            platform_message_id,
            ingested_at: chrono::Utc::now(),
        };
        let outbox_payload = serde_json::to_string(&temp_event)
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;

        // 3. 序列化为 content JSON
        let content_json = serde_json::to_string(&json!({ "Text": content.clone() }))
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;
        let timestamp_str = platform_timestamp.to_rfc3339();

        // 4. 调用 MessageRepository.accept_message（事务内完成 sequence + message + outbox event）
        let (message_id, conversation_sequence, ingested_at) = self
            .message_repo
            .accept_message(
                conversation_id,
                sender_id,
                &content_json,
                &timestamp_str,
                platform_message_id,
                character_id,
                &format!("{}-MessageAccepted", event_uuid_prefix),
                &outbox_payload,
            )
            .await
            .map_err(|e| RuntimeError::Repository(e))?;

        tracing::debug!(
            target: "storage",
            conversation_id,
            sender_id,
            message_id,
            conversation_sequence,
            "消息已接受并持久化"
        );

        // 5. 构建完整的 event_uuid
        let event_uuid = format!("{}-MessageAccepted", event_uuid_prefix);

        // 6. 构建 MessageAccepted event（C2-4: 仅用于写入 outbox，不发布到 EventBus）
        let _event = CoreEvent::MessageAccepted(MessageAcceptedEvent {
            event_uuid: event_uuid.clone(),
            conversation_id,
            sender_id,
            message_id,
            content,
            timestamp: platform_timestamp,
            is_mentioned: false,
            character_id,
            conversation_sequence,
            platform_message_id,
            ingested_at,
        });

        // 7. C2-4: MessageAccepted 不再通过 EventBus 发布。
        //     事件已写入 outbox_events 表（步骤 4-5），由 OutboxDispatcher 拾取并通过 MPSC 投递。
        //     这确保了 durable event delivery 的语义：commit 成功后，事件必然被处理。
        tracing::debug!(
            target: "outbox",
            conversation_id,
            sender_id,
            message_id,
            conversation_sequence,
            event_uuid = %event_uuid,
            "MessageAccepted 已写入 outbox（durable path）"
        );

        Ok((message_id, conversation_sequence))
    }
}
