//! 事件总线 —— 基于 `tokio::sync::broadcast` 的核心事件分发机制。
//!
//! 事件类型使用平台无关的 [`crate::domain::event::CoreEvent`]。
//! 生产者（例如 OneBot 适配器）通过 [`EventBus::publish`] 发布事件；
//! 消费者（例如日志、插件桥接和其他观察者）通过 [`EventBus::subscribe`] 订阅。
//!
//! 这是尽力而为的内存广播，不是持久化队列：没有订阅者时事件会被丢弃，
//! 订阅者处理滞后时超出容量的旧事件也会被丢弃。需要可靠处理的业务事件
//! 必须先写入 SQLite/Outbox，再由对应的持久化处理链路消费。

use std::sync::Arc;

use tokio::sync::broadcast;

use crate::domain::event::CoreEvent;

/// 默认的广播通道容量。
///
/// 当没有消费者时，事件会被丢弃（broadcast 语义）；订阅者处理滞后时，
/// 超出容量的旧事件也会被丢弃。这用于防止慢消费者造成内存无限增长。
const DEFAULT_CHANNEL_CAPACITY: usize = 256;

/// 事件总线。
///
/// 支持多个订阅者；每个订阅者都会收到发布的事件的一份克隆。
#[derive(Clone)]
pub struct EventBus {
    sender: Arc<broadcast::Sender<CoreEvent>>,
}

impl EventBus {
    /// 创建一个新的事件总线。
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CHANNEL_CAPACITY)
    }

    /// 创建一个指定通道容量的事件总线。
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender: Arc::new(sender),
        }
    }

    /// 发布一个核心事件。返回已接收该事件的订阅者数量。
    ///
    /// 返回值为零表示当前没有订阅者，事件已经被丢弃；事件不会在总线中暂存。
    /// 即使有订阅者，慢订阅者超出通道容量时也可能丢失旧事件。
    pub fn publish(&self, event: &CoreEvent) -> usize {
        self.sender.send(event.clone()).unwrap_or(0)
    }

    /// 订阅事件总线。返回一个随机的接收者。
    ///
    /// 每个调用都会创建一个全新的、从发布时刻起开始接收的订阅。
    pub fn subscribe(&self) -> EventSubscription {
        EventSubscription {
            receiver: self.sender.subscribe(),
        }
    }

    /// 当前激活的订阅者数量。
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

/// 事件总线的订阅句柄。
///
/// 使用 [`EventSubscription::recv`] 拉取一条事件。
pub struct EventSubscription {
    receiver: broadcast::Receiver<CoreEvent>,
}

impl EventSubscription {
    /// 等待并接收下一条核心事件。
    ///
    /// 当所有发送端都已关闭时返回 `None`。如果订阅者滞后，广播通道会丢弃
    /// 已无法保留的旧事件；方法会记录跳过数量并继续等待下一条仍可用的事件。
    pub async fn recv(&mut self) -> Option<CoreEvent> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => return Some(event),
                Err(broadcast::error::RecvError::Closed) => return None,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(
                        target = "event_bus",
                        skipped,
                        "事件订阅处理滞后，已跳过无法保留的旧事件"
                    );
                }
            }
        }
    }

    /// 尝试立即接收一条事件（非阻塞）。
    ///
    /// 返回 `None` 表示当前没有可用事件或总线已关闭。
    ///
    /// 如果订阅者滞后，广播通道会先返回 `Lagged`；本方法会记录跳过数量，
    /// 然后继续读取仍保留在缓冲区中的最早事件，而不是把滞后误当成空队列。
    pub fn try_recv(&mut self) -> Option<CoreEvent> {
        loop {
            match self.receiver.try_recv() {
                Ok(event) => return Some(event),
                Err(broadcast::error::TryRecvError::Empty) => return None,
                Err(broadcast::error::TryRecvError::Closed) => return None,
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    tracing::warn!(
                        target = "event_bus",
                        skipped,
                        "事件订阅处理滞后，已跳过无法保留的旧事件"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::event::{AdapterConnectedEvent, CoreEvent, MessageReceivedEvent};

    fn sample_event() -> CoreEvent {
        sample_event_with_content("你好")
    }

    fn sample_event_with_content(content: &str) -> CoreEvent {
        CoreEvent::MessageReceived(MessageReceivedEvent {
            conversation_id: 1,
            sender_id: 2,
            message_id: 3,
            content: content.to_string(),
            timestamp: chrono::Utc::now(),
            is_mentioned: false,
            character_id: 1,
            // C2-3 字段
            conversation_sequence: 0,
            platform_message_id: None,
            ingested_at: chrono::Utc::now(),
            metadata: serde_json::json!({}),
        })
    }

    #[tokio::test]
    async fn publish_delivers_to_all_subscribers() {
        let bus = EventBus::new();
        let mut sub1 = bus.subscribe();
        let mut sub2 = bus.subscribe();

        bus.publish(&sample_event());

        let e1 = sub1.recv().await.expect("sub1 应收到事件");
        let e2 = sub2.recv().await.expect("sub2 应收到事件");
        match (e1, e2) {
            (CoreEvent::MessageReceived(a), CoreEvent::MessageReceived(b)) => {
                assert_eq!(a.content, "你好");
                assert_eq!(b.content, "你好");
            }
            _ => panic!("期望 MessageReceived 事件"),
        }
    }

    #[tokio::test]
    async fn publish_without_receiver_does_not_fail() {
        let bus = EventBus::new();
        // 没有订阅者时发送不应导致错误，也不会为之后的新订阅者暂存事件。
        let n = bus.publish(&sample_event());
        assert_eq!(n, 0);

        let mut late_subscriber = bus.subscribe();
        assert!(late_subscriber.try_recv().is_none());
    }

    #[tokio::test]
    async fn try_recv_returns_none_when_empty() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe();
        assert!(sub.try_recv().is_none());
    }

    #[tokio::test]
    async fn try_recv_skips_lagged_events_and_returns_oldest_retained_event() {
        let bus = EventBus::with_capacity(2);
        let mut sub = bus.subscribe();

        bus.publish(&sample_event_with_content("第一条"));
        bus.publish(&sample_event_with_content("第二条"));
        bus.publish(&sample_event_with_content("第三条"));

        let event = sub.try_recv().expect("应返回仍保留的最早事件");
        match event {
            CoreEvent::MessageReceived(message) => assert_eq!(message.content, "第二条"),
            _ => panic!("期望 MessageReceived 事件"),
        }
    }

    #[tokio::test]
    async fn multi_events_deliver_in_order() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe();

        bus.publish(&sample_event());
        bus.publish(&CoreEvent::AdapterConnected(AdapterConnectedEvent {
            adapter_name: "onebot".to_string(),
            timestamp: chrono::Utc::now(),
        }));

        let first = sub.recv().await.expect("第一条");
        let second = sub.recv().await.expect("第二条");
        assert!(matches!(first, CoreEvent::MessageReceived(_)));
        assert!(matches!(second, CoreEvent::AdapterConnected(_)));
    }
}
