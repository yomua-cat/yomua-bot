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

use futures_util::{stream::FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

use crate::application::reply_processor::ReplyProcessor;
use crate::domain::outbox::{OutboxEvent, ProcessingOutcome};

/// 单个 Outbox 事件允许占用处理器的最长时间。
///
/// 处理器不能无限等待外部模型、数据库或 OneBot。到达截止时间后，
/// 事件会返回可重试结果，调度器负责持久化退避状态。
pub const DEFAULT_EVENT_PROCESSING_TIMEOUT_SECS: u64 = 120;

async fn process_with_deadline<F>(future: F, timeout: std::time::Duration) -> ProcessingOutcome
where
    F: std::future::Future<Output = ProcessingOutcome>,
{
    tokio::time::timeout(timeout, future)
        .await
        .unwrap_or_else(|_| ProcessingOutcome::RetryableFailure {
            error: format!("Outbox 事件处理超过 {} 秒截止时间", timeout.as_secs()),
        })
}

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

        let mut work = FuturesUnordered::new();
        loop {
            tokio::select! {
                biased;
                changed=shutdown_rx.changed()=>{if changed.is_err()||*shutdown_rx.borrow(){break}},
                Some(_)=work.next(),if !work.is_empty()=>{},
                item=event_rx.recv(),if work.len()<16=>{
                    let Some((event,response_tx))=item else{break};
                    work.push(async move {
                        let outcome = process_with_deadline(
                            self.process_event(&event),
                            std::time::Duration::from_secs(DEFAULT_EVENT_PROCESSING_TIMEOUT_SECS),
                        )
                        .await;
                        if response_tx.send(outcome).is_err(){warn!(event_uuid=%event.event_uuid,"ACK 发送失败");}
                    });
                }
            }
        }

        // 关停时先给已领取的事件有限时间完成并发送 ACK，避免直接 drop
        // 正在生成或发送的任务，导致平台已有副作用却没有持久化结果。
        let drain = tokio::time::timeout(
            std::time::Duration::from_secs(DEFAULT_EVENT_PROCESSING_TIMEOUT_SECS),
            async { while work.next().await.is_some() {} },
        )
        .await;
        if drain.is_err() {
            warn!(target: "outbox", "关停等待处理中的事件超时，剩余任务交由租约恢复");
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
            metadata: payload.metadata,
        };

        let Some(token) = event.lease_token.as_deref() else {
            return ProcessingOutcome::PermanentFailure {
                error: "缺少任务租约".into(),
            };
        };
        self.reply_processor
            .process_for_outbox_owned(&msg_received, token)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn processing_deadline_turns_hung_work_into_retryable_failure() {
        let work = async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            ProcessingOutcome::Success
        };
        let result = process_with_deadline(work, std::time::Duration::from_secs(5));
        tokio::pin!(result);
        tokio::time::advance(std::time::Duration::from_secs(5)).await;

        assert_eq!(
            result.await,
            ProcessingOutcome::RetryableFailure {
                error: "Outbox 事件处理超过 5 秒截止时间".into()
            }
        );
    }
}
