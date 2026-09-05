//! 事件订阅桥接 —— 把 Core 事件总线上的事件转发给已订阅的插件连接。
//!
//! 每条 [`CoreEvent`] 先映射为 [`EventType`]（dotted 事件名），再序列化为
//! JSON 载荷，随后逐个通知订阅了该事件类型的插件。通知一律 `try_send`：
//! 通道满则丢弃并记 warn（与 EventBus lagged 语义一致），事件不阻塞 Core。

use std::sync::Arc;

use crate::application::event_bus::EventSubscription;
use crate::infrastructure::plugin::protocol::{EventType, WireMessage};
use crate::infrastructure::plugin::registry::PluginRegistry;

/// 事件订阅桥接器。
pub struct EventBridge {
    registry: Arc<PluginRegistry>,
}

impl EventBridge {
    /// 创建一个事件桥接器。
    pub fn new(registry: Arc<PluginRegistry>) -> Self {
        Self { registry }
    }

    /// 运行桥接循环：订阅总线直到总线关闭（`None`）后退出。
    ///
    /// 每收到一条事件：映射 → 序列化 → 逐个通知订阅插件。
    pub async fn run(&self, mut subscription: EventSubscription) {
        while let Some(event) = subscription.recv().await {
            let event_type = EventType::from_core_event(&event);
            let data = match serde_json::to_value(&event) {
                Ok(data) => data,
                Err(e) => {
                    tracing::warn!("事件序列化失败（{}），跳过：{e}", event_type.name());
                    continue;
                }
            };
            self.dispatch_to_subscribers(event_type, data);
        }
        tracing::debug!("事件总线已关闭，事件桥接退出");
    }

    /// 把一条已序列化的事件通知给所有订阅插件。
    fn dispatch_to_subscribers(&self, event_type: EventType, data: serde_json::Value) {
        for name in self.registry.subscribed_plugins(event_type) {
            match self.registry.connected_plugin(&name) {
                Some(tx) => {
                    if let Err(e) = tx.try_send(WireMessage::Notify {
                        event: event_type.name().to_string(),
                        data: data.clone(),
                    }) {
                        tracing::warn!(
                            "事件 {} 通知插件 {name} 失败（通道满或已关闭）：{e}，丢弃",
                            event_type.name()
                        );
                    }
                }
                None => {
                    // 已订阅但暂无连接（如正在重连）：静默跳过。
                    tracing::debug!("插件 {name} 订阅了 {} 但未连接，跳过", event_type.name());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::event_bus::EventBus;
    use crate::domain::event::CoreEvent;
    use crate::domain::event::{AdapterConnectedEvent, MessageReceivedEvent, MessageSentEvent};
    use crate::infrastructure::plugin::protocol::WireMessage;
    use crate::infrastructure::plugin::registry::PluginRegistry;
    use crate::infrastructure::plugin::PluginManifest;
    use chrono::{TimeZone, Utc};
    use tokio::sync::mpsc;

    fn manifest(name: &str) -> PluginManifest {
        PluginManifest {
            name: name.to_string(),
            version: "0.1.0".to_string(),
            description: "示例插件".to_string(),
            permissions: vec![],
            executable: "plugin-bin".to_string(),
            config: serde_json::json!({}),
        }
    }

    fn ts() -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000, 0).unwrap()
    }

    /// 构造 registry，其中 alpha/beta 已注册、订阅指定事件、
    /// 并用 mpsc 通道模拟各自的连接。
    struct Harness {
        registry: Arc<PluginRegistry>,
        alpha_rx: mpsc::Receiver<WireMessage>,
        beta_rx: mpsc::Receiver<WireMessage>,
        bridge: EventBridge,
    }

    fn harness() -> Harness {
        let registry = Arc::new(PluginRegistry::new());
        registry.register(manifest("alpha")).unwrap();
        registry.register(manifest("beta")).unwrap();
        registry.register(manifest("gamma")).unwrap();

        registry.set_subscriptions(
            "alpha",
            vec![EventType::MessageReceived, EventType::MessageSent],
        );
        registry.set_subscriptions("beta", vec![EventType::MessageReceived]);
        // gamma 订阅了但无连接。

        let (alpha_tx, alpha_rx) = mpsc::channel::<WireMessage>(32);
        let (beta_tx, beta_rx) = mpsc::channel::<WireMessage>(32);
        registry.set_state("alpha", PluginState::Starting);
        registry.set_state("beta", PluginState::Starting);
        registry.attach_conn("alpha", alpha_tx).unwrap();
        registry.attach_conn("beta", beta_tx).unwrap();

        let bridge = EventBridge::new(registry.clone());
        Harness {
            registry,
            alpha_rx,
            beta_rx,
            bridge,
        }
    }

    use crate::infrastructure::plugin::PluginState;

    #[tokio::test]
    async fn bridge_forwards_to_subscribed_connected_plugins() {
        let mut h = harness();
        let bus = EventBus::with_capacity(16);

        // 事件 1：MessageReceived（alpha + beta 订阅）。
        let msg_event = CoreEvent::MessageReceived(MessageReceivedEvent {
            conversation_id: 1,
            sender_id: 2,
            message_id: 3,
            content: "你好".to_string(),
            timestamp: ts(),
            is_mentioned: false,
        });
        // 事件 2：MessageSent（仅 alpha 订阅）。
        let sent_event = CoreEvent::MessageSent(MessageSentEvent {
            conversation_id: 1,
            character_id: Some(4),
            message_id: 5,
            content: "回复".to_string(),
            timestamp: ts(),
        });
        // 事件 3：AdapterConnected（无人订阅）。
        let adapter_event = CoreEvent::AdapterConnected(AdapterConnectedEvent {
            adapter_name: "onebot".to_string(),
            timestamp: ts(),
        });
        // 事件 4：MessageReceived 第二条（覆盖同一事件多次转发）。
        let msg_event2 = CoreEvent::MessageReceived(MessageReceivedEvent {
            conversation_id: 10,
            sender_id: 20,
            message_id: 30,
            content: "第二条".to_string(),
            timestamp: ts(),
            is_mentioned: true,
        });

        // 在独立任务中运行桥接（订阅总线），主测试限量读取插件通道。
        let bridge_task = tokio::spawn({
            let bridge = EventBridge::new(h.registry.clone());
            let sub = bus.subscribe();
            async move {
                bridge.run(sub).await;
            }
        });

        bus.publish(&msg_event);
        bus.publish(&sent_event);
        bus.publish(&adapter_event);
        bus.publish(&msg_event2);
        // 给桥接一点时间消化（真实 IO 任务调度）。
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // alpha 应收到 message.received、message.sent、message.received（按发布序）。
        match h.alpha_rx.recv().await.expect("alpha 第 1 条") {
            WireMessage::Notify { event, data } => {
                assert_eq!(event, "message.received");
                assert_eq!(data["MessageReceived"]["content"], "你好");
                assert_eq!(data["MessageReceived"]["conversation_id"], 1);
            }
            other => panic!("期望 Notify，实际 {other:?}"),
        }
        match h.alpha_rx.recv().await.expect("alpha 第 2 条") {
            WireMessage::Notify { event, .. } => assert_eq!(event, "message.sent"),
            other => panic!("期望 Notify，实际 {other:?}"),
        }
        match h.alpha_rx.recv().await.expect("alpha 第 3 条") {
            WireMessage::Notify { event, data } => {
                assert_eq!(event, "message.received");
                assert_eq!(data["MessageReceived"]["content"], "第二条");
            }
            other => panic!("期望 Notify，实际 {other:?}"),
        }
        // alpha 不应收到 adapter.connected（无人订阅该事件）。
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), h.alpha_rx.recv())
                .await
                .is_err(),
            "alpha 不应收到未订阅事件"
        );

        // beta 只收到两条 message.received。
        for expected_content in ["你好", "第二条"] {
            match h.beta_rx.recv().await.expect("beta 应收事件") {
                WireMessage::Notify { event, data } => {
                    assert_eq!(event, "message.received");
                    assert_eq!(data["MessageReceived"]["content"], expected_content);
                }
                other => panic!("期望 Notify，实际 {other:?}"),
            }
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), h.beta_rx.recv())
                .await
                .is_err(),
            "beta 不应收到其余事件"
        );

        bridge_task.abort();
    }

    #[tokio::test]
    async fn bridge_skips_plugins_without_connection() {
        let mut h = harness();
        // gamma 订阅 message.received 但无连接 → 静默跳过，不影响 alpha/beta。
        h.registry
            .set_subscriptions("gamma", vec![EventType::MessageReceived]);

        let ev = CoreEvent::MessageReceived(MessageReceivedEvent {
            conversation_id: 9,
            sender_id: 8,
            message_id: 7,
            content: "hi".to_string(),
            timestamp: ts(),
            is_mentioned: false,
        });
        let ty = EventType::from_core_event(&ev);
        let data = serde_json::to_value(&ev).unwrap();
        h.bridge.dispatch_to_subscribers(ty, data);

        // gamma 无连接 → 不 panic；alpha/beta 仍收到。
        match h.alpha_rx.recv().await.expect("alpha 应收到") {
            WireMessage::Notify { event, .. } => assert_eq!(event, "message.received"),
            other => panic!("期望 Notify，实际 {other:?}"),
        }
        match h.beta_rx.recv().await.expect("beta 应收到") {
            WireMessage::Notify { event, .. } => assert_eq!(event, "message.received"),
            other => panic!("期望 Notify，实际 {other:?}"),
        }
    }

    #[tokio::test]
    async fn bridge_drops_when_channel_full_and_does_not_block() {
        // 专门构造：一个 1 容量的满通道插件 + 一个正常插件。
        let registry = Arc::new(PluginRegistry::new());
        registry.register(manifest("full")).unwrap();
        registry.register(manifest("ok")).unwrap();
        registry.set_subscriptions("full", vec![EventType::MessageReceived]);
        registry.set_subscriptions("ok", vec![EventType::MessageReceived]);
        registry.set_state("full", PluginState::Starting);
        registry.set_state("ok", PluginState::Starting);

        // full 的通道容量 1 且已塞满。
        let (full_tx, full_rx) = mpsc::channel::<WireMessage>(1);
        full_tx
            .try_send(WireMessage::Notify {
                event: "occupied".to_string(),
                data: serde_json::json!({}),
            })
            .unwrap();
        let _ = full_rx;
        // ok 的通道容量 1（空）。
        let (ok_tx, mut ok_rx) = mpsc::channel::<WireMessage>(1);
        registry.attach_conn("full", full_tx).unwrap();
        registry.attach_conn("ok", ok_tx).unwrap();

        let bridge = EventBridge::new(registry.clone());
        let ev = CoreEvent::MessageReceived(MessageReceivedEvent {
            conversation_id: 1,
            sender_id: 2,
            message_id: 3,
            content: "挤爆".to_string(),
            timestamp: ts(),
            is_mentioned: false,
        });
        let data = serde_json::to_value(&ev).unwrap();
        // 满通道丢弃 + warn，正常通道照常收到 → 事件不阻塞。
        bridge.dispatch_to_subscribers(EventType::MessageReceived, data);

        match ok_rx.recv().await.expect("ok 应收到") {
            WireMessage::Notify { event, .. } => assert_eq!(event, "message.received"),
            other => panic!("期望 Notify，实际 {other:?}"),
        }
    }
}
