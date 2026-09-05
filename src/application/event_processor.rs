//! 事件处理器 —— 将核心事件路由到相应的处理器。
//!
//! 作为事件总线的订阅者，持续消费 `CoreEvent` 并把事件路由到
//! 相应的处理器（消息回复编排、行为引擎、认知层等）。

use std::sync::Arc;

use crate::application::event_bus::EventBus;
use crate::application::reply_processor::ReplyProcessor;
use crate::domain::event::CoreEvent;
use crate::error::RuntimeError;

/// 处理核心事件并分发到相应的处理器。
pub struct EventProcessor {
    reply_processor: Arc<ReplyProcessor>,
}

impl EventProcessor {
    /// 创建一个事件处理器，路由到给出的回复处理器。
    pub fn new(reply_processor: Arc<ReplyProcessor>) -> Self {
        Self { reply_processor }
    }

    /// 从事件总线持续消费并处理事件，直到发送端全部关闭。
    pub async fn run(self, bus: &EventBus) {
        let mut subscription = bus.subscribe();
        while let Some(event) = subscription.recv().await {
            if let Err(e) = self.process(&event).await {
                tracing::warn!(target: "runtime", error = %e, "事件处理失败");
            }
        }
    }

    /// 处理一个核心事件。
    pub async fn process(&self, event: &CoreEvent) -> Result<(), RuntimeError> {
        match event {
            CoreEvent::MessageReceived(e) => {
                tracing::debug!(
                    conversation_id = e.conversation_id,
                    sender_id = e.sender_id,
                    is_mentioned = e.is_mentioned,
                    "收到消息，路由到回复处理器"
                );
                self.reply_processor.process(e).await?;
            }
            CoreEvent::MessageSent(e) => {
                tracing::debug!(conversation_id = e.conversation_id, "消息已发送");
            }
            CoreEvent::CharacterStateChanged(e) => {
                tracing::debug!(character_id = e.character_id, "角色状态已变更");
            }
            CoreEvent::EmotionChanged(e) => {
                tracing::debug!(character_id = e.character_id, "情感已变更");
            }
            CoreEvent::RelationshipChanged(e) => {
                tracing::debug!(
                    character_id = e.character_id,
                    participant_id = e.participant_id,
                    "关系已变更"
                );
            }
            CoreEvent::MemoryCreated(e) => {
                tracing::debug!(
                    character_id = e.character_id,
                    memory_id = e.memory_id,
                    "记忆已创建"
                );
            }
            CoreEvent::BehaviorDecided(e) => {
                tracing::debug!(
                    character_id = e.character_id,
                    action = %e.action,
                    reason = %e.reason,
                    "行为已决定"
                );
            }
            CoreEvent::ResponseGenerated(e) => {
                tracing::debug!(character_id = e.character_id, "已生成响应");
            }
            CoreEvent::AdapterConnected(e) => {
                tracing::info!(adapter = %e.adapter_name, "适配器已连接");
            }
            CoreEvent::AdapterDisconnected(e) => {
                tracing::warn!(
                    adapter = %e.adapter_name,
                    reason = ?e.reason,
                    "适配器已断开"
                );
            }
            CoreEvent::ScheduledTaskTriggered(e) => {
                tracing::debug!(
                    task_id = e.task_id,
                    task_type = %e.task_type,
                    "已触发计划任务"
                );
            }
        }

        Ok(())
    }
}
