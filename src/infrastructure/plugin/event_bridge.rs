//! 事件订阅桥接 —— 把 Core 事件总线上的事件转发给已订阅的插件连接。
//!
//! 每条 [`CoreEvent`] 先映射为 [`EventType`]（dotted 事件名），再序列化为
//! JSON 载荷，随后逐个通知订阅了该事件类型的插件。通知一律 `try_send`：
//! 通道满则丢弃并记 warn（与 EventBus lagged 语义一致），事件不阻塞 Core。

use std::sync::Arc;

use crate::application::event_bus::EventSubscription;
use crate::domain::event::{CoreEvent, ResponseSource};
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
            let Some((event_type, data)) = plugin_event_dto(&event) else {
                continue;
            };
            self.dispatch_to_subscribers(event_type, data);
        }
        tracing::debug!("事件总线已关闭，事件桥接退出");
    }

    /// 把一条已序列化的事件通知给所有订阅插件。
    fn dispatch_to_subscribers(&self, event_type: EventType, data: serde_json::Value) {
        for name in self.registry.subscribed_plugins(event_type) {
            let granted = self.registry.permissions_for(&name).unwrap_or_default();
            if crate::infrastructure::plugin::permissions::permission_for_event(event_type)
                .is_some_and(|required| !granted.contains(&required))
            {
                tracing::warn!(plugin = %name, event = %event_type.name(), "插件事件读取权限不足");
                continue;
            }
            match self.registry.connected_plugin(&name) {
                Some(tx) => {
                    if let Err(e) = tx.try_send(WireMessage::Notify {
                        event: event_type.name().to_string(),
                        data: data.clone(),
                    }) {
                        // 事件丢弃是严重问题：说明插件消费速度低于 Core 发送速度，
                        // 或插件已卡死/断开连接。需要 error 级别告警以便运维发现。
                        tracing::error!(
                            "事件 {} 通知插件 {name} 失败（通道满或已关闭）：{e}，事件已丢弃",
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

/// 将宿主事件投影为版本稳定的插件 DTO；插件看不到 CoreEvent 的枚举标签。
fn plugin_event_dto(event: &CoreEvent) -> Option<(EventType, serde_json::Value)> {
    let (kind, data) = match event {
        CoreEvent::MessageAccepted(e) => (
            EventType::MessageReceived,
            serde_json::json!({
                "conversation_id":e.conversation_id,"sender_id":e.sender_id,"message_id":e.message_id,
                "content":e.content,"timestamp":e.timestamp,"is_mentioned":e.is_mentioned,
                "character_id":e.character_id,"conversation_sequence":e.conversation_sequence,
                "platform_message_id":e.platform_message_id,"ingested_at":e.ingested_at,"metadata":e.metadata
            }),
        ),
        CoreEvent::MessageReceived(e) => (
            EventType::MessageReceived,
            serde_json::json!({
                "conversation_id":e.conversation_id,"sender_id":e.sender_id,"message_id":e.message_id,
                "content":e.content,"timestamp":e.timestamp,"is_mentioned":e.is_mentioned,
                "character_id":e.character_id,"conversation_sequence":e.conversation_sequence,
                "platform_message_id":e.platform_message_id,"ingested_at":e.ingested_at,"metadata":e.metadata
            }),
        ),
        CoreEvent::MessageSent(e) => (
            EventType::MessageSent,
            serde_json::json!({"conversation_id":e.conversation_id,"character_id":e.character_id,"message_id":e.message_id,"content":e.content,"timestamp":e.timestamp}),
        ),
        CoreEvent::CharacterStateChanged(e) => (
            EventType::CharacterStateChanged,
            serde_json::json!({"character_id":e.character_id,"timestamp":e.timestamp}),
        ),
        CoreEvent::EmotionChanged(e) => (
            EventType::EmotionChanged,
            serde_json::json!({"character_id":e.character_id,"conversation_id":e.conversation_id,"timestamp":e.timestamp}),
        ),
        CoreEvent::RelationshipChanged(e) => (
            EventType::RelationshipChanged,
            serde_json::json!({"character_id":e.character_id,"user_id":e.user_id,"timestamp":e.timestamp}),
        ),
        CoreEvent::MemoryCreated(e) => (
            EventType::MemoryCreated,
            serde_json::json!({"character_id":e.character_id,"memory_id":e.memory_id,"timestamp":e.timestamp}),
        ),
        CoreEvent::BehaviorDecided(e) => (
            EventType::BehaviorDecided,
            serde_json::json!({"character_id":e.character_id,"conversation_id":e.conversation_id,"action":e.action,"reason":e.reason,"timestamp":e.timestamp}),
        ),
        CoreEvent::ResponseGenerated(e) => (
            EventType::ResponseGenerated,
            serde_json::json!({"character_id":e.character_id,"conversation_id":e.conversation_id,"content":e.content,"source":match e.source {ResponseSource::Rule=>"rule",ResponseSource::Llm=>"llm",ResponseSource::Plugin=>"plugin"},"timestamp":e.timestamp}),
        ),
        CoreEvent::AdapterConnected(e) => (
            EventType::AdapterConnected,
            serde_json::json!({"adapter_name":e.adapter_name,"timestamp":e.timestamp}),
        ),
        CoreEvent::AdapterDisconnected(e) => (
            EventType::AdapterDisconnected,
            serde_json::json!({"adapter_name":e.adapter_name,"reason":e.reason,"timestamp":e.timestamp}),
        ),
        CoreEvent::ScheduledTaskTriggered(e) => (
            EventType::ScheduledTaskTriggered,
            serde_json::json!({"task_id":e.task_id,"task_type":e.task_type,"timestamp":e.timestamp}),
        ),
        CoreEvent::CommandReceived(_) => return None,
    };
    Some((kind, data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::event_bus::EventBus;
    use crate::domain::event::CoreEvent;
    use crate::domain::event::{
        AdapterConnectedEvent, MessageAcceptedEvent, MessageReceivedEvent, MessageSentEvent,
    };
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
            permissions: vec![crate::infrastructure::plugin::PluginPermission::MessageRead],
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
            character_id: 1,
            // C2-3 字段
            conversation_sequence: 0,
            platform_message_id: None,
            ingested_at: chrono::Utc::now(),
            metadata: serde_json::json!({}),
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
            character_id: 1,
            // C2-3 字段
            conversation_sequence: 0,
            platform_message_id: None,
            ingested_at: chrono::Utc::now(),
            metadata: serde_json::json!({}),
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
                assert_eq!(data["content"], "你好");
                assert_eq!(data["conversation_id"], 1);
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
                assert_eq!(data["content"], "第二条");
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
                    assert_eq!(data["content"], expected_content);
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
    async fn bridge_normalizes_message_accepted_to_one_message_received_notification() {
        let mut h = harness();
        let bus = EventBus::with_capacity(4);
        let bridge_task = tokio::spawn({
            let bridge = EventBridge::new(h.registry.clone());
            let sub = bus.subscribe();
            async move { bridge.run(sub).await }
        });

        bus.publish(&CoreEvent::MessageAccepted(MessageAcceptedEvent {
            event_uuid: "accepted-1".into(),
            conversation_id: 7,
            sender_id: 8,
            message_id: 9,
            content: "持久化消息".into(),
            timestamp: ts(),
            is_mentioned: true,
            character_id: 10,
            conversation_sequence: 11,
            platform_message_id: Some(12),
            ingested_at: ts(),
            metadata: serde_json::json!({"author_kind":"user"}),
        }));

        let notification =
            tokio::time::timeout(std::time::Duration::from_secs(1), h.alpha_rx.recv())
                .await
                .expect("插件应收到 canonical 消息事件")
                .expect("通知通道不应提前关闭");
        match notification {
            WireMessage::Notify { event, data } => {
                assert_eq!(event, "message.received");
                assert_eq!(data["message_id"], 9);
                assert_eq!(data["content"], "持久化消息");
                assert_eq!(data["metadata"]["author_kind"], "user");
            }
            other => panic!("期望 Notify，实际 {other:?}"),
        }
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
            character_id: 1,
            // C2-3 字段
            conversation_sequence: 0,
            platform_message_id: None,
            ingested_at: chrono::Utc::now(),
            metadata: serde_json::json!({}),
        });
        let (ty, data) = plugin_event_dto(&ev).expect("MessageReceived 应对插件可见");
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
            character_id: 1,
            // C2-3 字段
            conversation_sequence: 0,
            platform_message_id: None,
            ingested_at: chrono::Utc::now(),
            metadata: serde_json::json!({}),
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
