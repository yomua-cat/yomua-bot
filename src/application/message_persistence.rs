//! 消息持久化 —— 订阅事件总线，将收到的消息写入消息仓库。
//!
//! 主流程：OneBot 事件 → CoreEvent → 消息写入 `MessageRepository`。
//! 本模块作为事件总线的订阅者，只关心 `MessageReceived` 事件。

use std::sync::Arc;

use crate::application::event_bus::{EventBus, EventSubscription};
use crate::domain::event::CoreEvent;
use crate::domain::message::{Message, MessageContent};
use crate::domain::repository::MessageRepository;
use crate::error::RuntimeError;

/// 消息持久化订阅者。
///
/// 从事件总线持续消费 `MessageReceived` 事件并写入消息仓库。
pub struct MessagePersistence {
    message_repo: Arc<dyn MessageRepository>,
}

impl MessagePersistence {
    /// 创建一个消息持久化订阅者。
    pub fn new(message_repo: Arc<dyn MessageRepository>) -> Self {
        Self { message_repo }
    }

    /// 启动消费循环，持续监听事件总线直到发送端全部关闭。
    pub async fn run(self, bus: &EventBus) {
        let mut subscription = bus.subscribe();
        while let Some(event) = subscription.recv().await {
            if let Err(e) = self.handle(&event).await {
                tracing::warn!(target: "storage", error = %e, "消息持久化失败");
            }
        }
    }

    /// 处理单条事件：若为 `MessageReceived` 则持久化。
    pub async fn handle(&self, event: &CoreEvent) -> Result<(), RuntimeError> {
        let CoreEvent::MessageReceived(e) = event else {
            return Ok(());
        };

        let message = Message {
            id: 0, // 由数据库分配
            conversation_id: e.conversation_id,
            sender_id: e.sender_id,
            content: MessageContent::Text(e.content.clone()),
            timestamp: e.timestamp,
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
        };

        self.message_repo.insert(&message).await?;
        tracing::debug!(
            target: "storage",
            conversation_id = message.conversation_id,
            sender_id = message.sender_id,
            "消息已持久化"
        );
        Ok(())
    }

    /// 提供给需要手动驱动订阅的调用方。
    pub fn subscribe(&self, bus: &EventBus) -> EventSubscription {
        bus.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::onebot::connection::{WsConnector, WsTransport};
    use crate::adapters::onebot::{OneBotAdapter, OneBotAdapterImpl};
    use crate::application::conversation::ConversationManager;
    use crate::application::event_bus::EventBus;
    use crate::infrastructure::storage::SqliteStorage;
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

    /// 测试用连接器：按脚本吐出消息后关闭连接。
    struct ScriptedConnector {
        script: StdMutex<VecDeque<Vec<String>>>,
    }

    impl ScriptedConnector {
        fn new(messages: Vec<Vec<String>>) -> Self {
            Self {
                script: StdMutex::new(messages.into()),
            }
        }
    }

    #[async_trait]
    impl WsConnector for ScriptedConnector {
        async fn connect(
            &self,
            _config: &crate::adapters::onebot::OneBotConfig,
        ) -> Result<Box<dyn WsTransport>, crate::error::RuntimeError> {
            let batch = self.script.lock().unwrap().pop_front();
            Ok(Box::new(FakeTransport {
                messages: batch.unwrap_or_default().into(),
            }))
        }
    }

    /// 测试用传输：依次返回消息，然后报连接关闭（从而触发重连）。
    struct FakeTransport {
        messages: VecDeque<String>,
    }

    #[async_trait]
    impl WsTransport for FakeTransport {
        async fn read_text(&mut self) -> Result<Option<String>, crate::error::RuntimeError> {
            Ok(self.messages.pop_front())
        }

        async fn send_text(&mut self, _t: &str) -> Result<(), crate::error::RuntimeError> {
            Ok(())
        }

        async fn ping(&mut self) -> Result<(), crate::error::RuntimeError> {
            Ok(())
        }
    }

    /// 端到端主流程测试：
    /// OneBot 群消息 JSON → 适配器解析 → 事件总线 → 消息持久化写入仓库。
    #[tokio::test]
    async fn onebot_message_ends_up_persisted() {
        // 准备存储与仓库。
        let storage = SqliteStorage::open_in_memory()
            .await
            .expect("打开内存库失败");
        storage.migrate().await.expect("迁移失败");
        let pool = storage.pool().clone();

        let conv_repo: Arc<dyn crate::domain::repository::ConversationRepository> = Arc::new(
            crate::infrastructure::storage::repository::SqliteConversationRepository::new(
                pool.clone(),
            ),
        );
        let part_repo: Arc<dyn crate::domain::repository::ParticipantRepository> = Arc::new(
            crate::infrastructure::storage::repository::SqliteParticipantRepository::new(
                pool.clone(),
            ),
        );
        let msg_repo: Arc<dyn crate::domain::repository::MessageRepository> = Arc::new(
            crate::infrastructure::storage::repository::SqliteMessageRepository::new(pool.clone()),
        );

        let conversation_manager = ConversationManager::new(conv_repo.clone(), part_repo.clone());

        let bus = EventBus::new();

        // 启动消息持久化订阅者。
        let persistence = MessagePersistence::new(msg_repo.clone());
        let bus_for_persistence = bus.clone();
        tokio::spawn(async move { persistence.run(&bus_for_persistence).await });

        // 假连接：先吐出一条 OneBot 群消息，然后关闭（触发重连）。
        let group_message = r#"{
            "post_type": "message",
            "message_type": "group",
            "time": 1690000000,
            "self_id": 123456,
            "message_id": 9001,
            "group_id": 500000,
            "user_id": 900001,
            "message": [{"type":"text","data":{"text":"你好，世界！"}}],
            "sender": {"user_id": 900001, "nickname": "小明", "card": ""}
        }"#;
        let connector = ScriptedConnector::new(vec![
            vec![group_message.to_string()], // 第 1 次连接：发消息后关闭
            vec![],                          // 第 2 次连接：空，保持
        ]);

        let adapter = OneBotAdapterImpl::with_connector(
            crate::adapters::onebot::OneBotConfig {
                websocket_url: "ws://127.0.0.1:1".to_string(),
                access_token: None,
                reconnect_interval_secs: 1,
                max_reconnect_interval_secs: 4,
                heartbeat_interval_secs: 30,
            },
            bus.clone(),
            conversation_manager,
            Arc::new(connector),
        );

        adapter.start().await.expect("适配器启动失败");

        // 轮询：等待消息被持久化（带兜底超时）。
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut persisted = false;
        while std::time::Instant::now() < deadline {
            if let Some(conv) = conv_repo
                .find_by_external_id("500000")
                .await
                .expect("查询会话失败")
            {
                let recent = msg_repo
                    .find_recent(conv.id, 10)
                    .await
                    .expect("查询消息失败");
                if !recent.is_empty() {
                    persisted = true;
                    match &recent[0].content {
                        crate::domain::message::MessageContent::Text(t) => {
                            assert_eq!(t, "你好，世界！");
                        }
                        _ => panic!("期望文本消息"),
                    }
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        assert!(persisted, "消息应被持久化");

        adapter.stop().await.expect("停止适配器失败");
    }
}
