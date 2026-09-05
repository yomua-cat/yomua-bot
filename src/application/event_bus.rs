//! 事件总线 —— 基于 `tokio::sync::broadcast` 的核心事件分发机制。
//!
//! 事件类型使用平台无关的 [`crate::domain::event::CoreEvent`]。
//! 生产者（例如 OneBot 适配器）通过 [`EventBus::publish`] 发布事件；
//! 消费者（例如消息持久化、日志、未来的人设运行时）通过 [`EventBus::subscribe`] 订阅。

use std::sync::Arc;

use tokio::sync::broadcast;

use crate::domain::event::CoreEvent;

/// 默认的广播通道容量。
///
/// 当没有消费者时，生产者写入的最近事件会被丢弃（broadcast 语义），
/// 这用于防止慢消费者造成内存无限增长。
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
    /// 订阅者数量为零时并不表示失败 —— 事件仍会被暂存（或按容量丢弃）。
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
    /// 当所有发送端都已关闭时返回 `None`。
    pub async fn recv(&mut self) -> Option<CoreEvent> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => return Some(event),
                Err(broadcast::error::RecvError::Closed) => return None,
                // 滞后（Lagged）时跳过被丢弃的事件，继续等待。
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    }

    /// 尝试立即接收一条事件（非阻塞）。
    ///
    /// 返回 `None` 表示当前没有可用事件。
    pub fn try_recv(&mut self) -> Option<CoreEvent> {
        match self.receiver.try_recv() {
            Ok(event) => Some(event),
            Err(broadcast::error::TryRecvError::Empty) => None,
            Err(broadcast::error::TryRecvError::Closed) => None,
            Err(broadcast::error::TryRecvError::Lagged(_)) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::event::{AdapterConnectedEvent, CoreEvent, MessageReceivedEvent};

    fn sample_event() -> CoreEvent {
        CoreEvent::MessageReceived(MessageReceivedEvent {
            conversation_id: 1,
            sender_id: 2,
            message_id: 3,
            content: "你好".to_string(),
            timestamp: chrono::Utc::now(),
            is_mentioned: false,
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
        // 没有订阅者时发送不应导致错误。
        let n = bus.publish(&sample_event());
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn try_recv_returns_none_when_empty() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe();
        assert!(sub.try_recv().is_none());
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
