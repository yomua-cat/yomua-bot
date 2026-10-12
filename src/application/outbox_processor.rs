//! Outbox Processor —— MPSC 消费者，处理来自 OutboxDispatcher 的事件。
//!
//! 职责：
//! - 从 MPSC 通道接收 OutboxEvent
//! - 反序列化事件 payload
//! - 调用 ReplyProcessor.process_for_outbox() 处理事件
//! - 通过 ACK 通道返回 ProcessingOutcome
//!
//! **注意**：此处理器不直接发布任何 EventBus 事件。下游事件（如 BehaviorDecided、
//! ResponseGenerated）由 ReplyProcessor 内部通过 EventBus 发布。

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

use crate::application::reply_processor::ReplyProcessor;
use crate::domain::outbox::{OutboxEvent, ProcessingOutcome};

/// Outbox 事件处理器。
///
/// 接收来自 OutboxDispatcher 的事件，执行处理，并返回结果。
pub struct OutboxProcessor {
    reply_processor: Arc<ReplyProcessor>,
}

impl OutboxProcessor {
    /// 创建 Outbox 处理器。
    pub fn new(reply_processor: Arc<ReplyProcessor>) -> Self {
        Self { reply_processor }
    }

    /// 启动处理循环。
    ///
    /// 从 MPSC 通道接收事件，处理并返回结果。
    /// 直到 `shutdown_rx` 收到关停信号或通道关闭。
    ///
    /// # Arguments
    /// * `event_rx` - MPSC receiver，接收 (OutboxEvent, ACK sender) 元组
    /// * `shutdown_rx` - 关停信号接收器
    pub async fn run(
        &self,
        mut event_rx: mpsc::Receiver<(OutboxEvent, oneshot::Sender<ProcessingOutcome>)>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        info!(target: "outbox", "OutboxProcessor 启动");

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!(target: "outbox", "OutboxProcessor 收到关停信号，正在停止...");
                        break;
                    }
                }
                item = event_rx.recv() => {
                    match item {
                        Some((event, response_tx)) => {
                            let outcome = self.process_event(&event).await;
                            if response_tx.send(outcome).is_err() {
                                warn!(
                                    target: "outbox",
                                    event_uuid = %event.event_uuid,
                                    "ACK 发送失败（dispatcher 已关闭）"
                                );
                            }
                        }
                        None => {
                            info!(target: "outbox", "MPSC 通道已关闭，OutboxProcessor 停止");
                            break;
                        }
                    }
                }
            }
        }

        info!(target: "outbox", "OutboxProcessor 已停止");
    }

    /// 处理单个事件。
    async fn process_event(&self, event: &OutboxEvent) -> ProcessingOutcome {
        let event_uuid = &event.event_uuid;
        debug!(
            target: "outbox",
            event_uuid = %event_uuid,
            event_type = %event.event_type,
            ordering_key = %event.ordering_key,
            ordering_seq = event.ordering_seq,
            "开始处理事件"
        );

        // 根据 event_type 分发
        match event.event_type.as_str() {
            "MessageAccepted" => self.process_message_accepted(event).await,
            other => {
                error!(
                    target: "outbox",
                    event_uuid = %event_uuid,
                    event_type = %other,
                    "未知的 outbox 事件类型"
                );
                ProcessingOutcome::PermanentFailure {
                    error: format!("未知的 event_type: {}", other),
                }
            }
        }
    }

    /// 处理 MessageAccepted 事件。
    async fn process_message_accepted(&self, event: &OutboxEvent) -> ProcessingOutcome {
        let event_uuid = &event.event_uuid;

        // 反序列化 payload
        let payload: crate::domain::outbox::OutboxMessageAcceptedPayload =
            match serde_json::from_str(&event.payload) {
                Ok(p) => p,
                Err(e) => {
                    error!(
                        target: "outbox",
                        event_uuid = %event_uuid,
                        error = %e,
                        "反序列化 MessageAccepted payload 失败"
                    );
                    return ProcessingOutcome::PermanentFailure {
                        error: format!("payload 反序列化失败: {}", e),
                    };
                }
            };

        debug!(
            target: "outbox",
            event_uuid = %event_uuid,
            message_id = payload.message_id,
            conversation_id = payload.conversation_id,
            conversation_sequence = payload.conversation_sequence,
            "MessageAccepted payload 已解析"
        );

        // 转换为 MessageReceivedEvent（复用 ReplyProcessor 的处理逻辑）
        let msg_received = crate::domain::event::MessageReceivedEvent {
            conversation_id: payload.conversation_id,
            sender_id: payload.sender_id,
            message_id: payload.message_id,
            content: payload.content.clone(),
            timestamp: payload.timestamp,
            is_mentioned: payload.is_mentioned,
            character_id: payload.character_id,
            conversation_sequence: payload.conversation_sequence,
            platform_message_id: payload.platform_message_id,
            ingested_at: payload.ingested_at,
        };

        // 调用 ReplyProcessor（使用 outbox 专用路径）
        match self.reply_processor.process_for_outbox(&msg_received).await {
            Ok(outcome) => {
                debug!(
                    target: "outbox",
                    event_uuid = %event_uuid,
                    outcome = %outcome,
                    "MessageAccepted 处理完成"
                );
                outcome
            }
            Err(e) => {
                // ReplyProcessor 返回错误（这不应该发生，因为它内部捕获了所有错误）
                // 将所有错误视为永久失败
                error!(
                    target: "outbox",
                    event_uuid = %event_uuid,
                    error = %e,
                    "ReplyProcessor 处理异常"
                );
                ProcessingOutcome::PermanentFailure {
                    error: format!("ReplyProcessor 异常: {}", e),
                }
            }
        }
    }
}
