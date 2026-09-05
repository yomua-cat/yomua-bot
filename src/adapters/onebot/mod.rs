//! OneBot 11 适配器 — 通过 WebSocket 连接 NapCat。
//!
//! 将 OneBot JSON 事件转换为核心事件，并将核心动作转换为 OneBot API 调用。
//! 使用指数退避策略处理重连。适配器断开时核心不会退出。
//!
//! 模块划分：
//! - `mod` —— 配置、连接状态、适配器 trait 与 `OneBotAdapterImpl` 实现
//! - [`conversion`] —— OneBot JSON ↔ 平台无关消息的纯函数转换
//! - [`connection`] —— WebSocket 传输、断线重连与指数退避

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch, Mutex};

use crate::application::conversation::ConversationManager;
use crate::application::event_bus::EventBus;
use crate::domain::event::{CommandReceivedEvent, CoreEvent, MessageReceivedEvent};
use crate::error::RuntimeError;

pub mod connection;
pub mod conversion;

use connection::{ConnectionShared, TungsteniteConnector, WsConnector};
pub use conversion::{
    build_group_send_request, build_private_send_request, OneBotEvent, OutgoingRequest,
};

/// OneBot 适配器配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OneBotConfig {
    /// WebSocket URL（例如 "ws://127.0.0.1:3001"）。
    pub websocket_url: String,

    /// 访问令牌（若 NapCat 需要）。
    pub access_token: Option<String>,

    /// 重连间隔（秒），作为指数退避的基准。
    pub reconnect_interval_secs: u64,

    /// 最大重连间隔（秒）。
    pub max_reconnect_interval_secs: u64,

    /// 心跳间隔（秒）。
    pub heartbeat_interval_secs: u64,
}

impl Default for OneBotConfig {
    fn default() -> Self {
        Self {
            websocket_url: "ws://127.0.0.1:3001".to_string(),
            access_token: None,
            reconnect_interval_secs: 1,
            max_reconnect_interval_secs: 60,
            heartbeat_interval_secs: 30,
        }
    }
}

/// OneBot 适配器的连接状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OneBotConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}

/// OneBot 适配器 trait。
///
/// 管理与 NapCat 的 WebSocket 连接，并在
/// OneBot 协议与核心事件/动作之间转换。
#[async_trait]
pub trait OneBotAdapter: Send + Sync {
    /// 启动适配器（连接 NapCat）。
    async fn start(&self) -> Result<(), RuntimeError>;

    /// 停止适配器（优雅断开）。
    async fn stop(&self) -> Result<(), RuntimeError>;

    /// 获取当前连接状态。
    async fn state(&self) -> OneBotConnectionState;

    /// 发送一条群消息。
    async fn send_group_message(&self, group_id: &str, content: &str) -> Result<(), RuntimeError>;

    /// 发送一条私聊消息。
    async fn send_private_message(&self, user_id: &str, content: &str) -> Result<(), RuntimeError>;
}

/// OneBot 适配器的具体实现。
///
/// 拥有连接状态、出站通道与入站事件通道，并负责把收到的 OneBot 事件
/// 解析为 CoreEvent 后发布到事件总线。
pub struct OneBotAdapterImpl {
    config: OneBotConfig,
    bus: EventBus,
    conversation_manager: ConversationManager,
    /// 共享的连接状态与出站通道。
    shared: ConnectionShared,
    /// 出站接收端（在 `start` 时被连接循环接管）。
    outbound_rx: Mutex<Option<mpsc::Receiver<String>>>,
    /// 入站发送端（连接循环把原始文本发到这里）。
    event_tx: mpsc::Sender<String>,
    /// 入站接收端（在 `start` 时被处理任务接管）。
    event_rx: Mutex<Option<mpsc::Receiver<String>>>,
    /// 关停信号发送端。
    shutdown_tx: watch::Sender<bool>,
    /// 是否已启动。
    started: AtomicBool,
    /// 连接器（真实或测试用假连接）。
    connector: Arc<dyn WsConnector>,
}

impl OneBotAdapterImpl {
    /// 使用默认的真实 WebSocket 连接器创建一个适配器。
    pub async fn new(
        config: OneBotConfig,
        bus: EventBus,
        conversation_manager: ConversationManager,
    ) -> Self {
        Self::with_connector(
            config,
            bus,
            conversation_manager,
            Arc::new(TungsteniteConnector),
        )
    }

    /// 使用指定连接器创建适配器（便于测试注入假连接）。
    pub fn with_connector(
        config: OneBotConfig,
        bus: EventBus,
        conversation_manager: ConversationManager,
        connector: Arc<dyn WsConnector>,
    ) -> Self {
        let (outbound_tx, outbound_rx) = mpsc::channel(128);
        let (event_tx, event_rx) = mpsc::channel(256);
        let (shutdown_tx, _) = watch::channel(false);

        let shared = ConnectionShared {
            state: Arc::new(Mutex::new(OneBotConnectionState::Disconnected)),
            bus: bus.clone(),
            outbound_tx,
        };

        Self {
            config,
            bus,
            conversation_manager,
            shared,
            outbound_rx: Mutex::new(Some(outbound_rx)),
            event_rx: Mutex::new(Some(event_rx)),
            event_tx,
            shutdown_tx,
            started: AtomicBool::new(false),
            connector,
        }
    }
}

#[async_trait]
impl OneBotAdapter for OneBotAdapterImpl {
    async fn start(&self) -> Result<(), RuntimeError> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(RuntimeError::Adapter("适配器已启动".to_string()));
        }

        // 取出被连接循环 / 处理任务接管的接收端。
        let outbound_rx = self
            .outbound_rx
            .lock()
            .await
            .take()
            .ok_or_else(|| RuntimeError::Adapter("出站通道已被占用".to_string()))?;
        let event_rx = self
            .event_rx
            .lock()
            .await
            .take()
            .ok_or_else(|| RuntimeError::Adapter("入站通道已被占用".to_string()))?;

        let shutdown_rx = self.shutdown_tx.subscribe();
        let connector = Arc::clone(&self.connector);
        let shared = self.shared.clone();
        let event_tx = self.event_tx.clone();
        let config = self.config.clone();

        // 连接循环任务（负责收发、心跳、断线重连）。
        tokio::spawn(connection::run_connection_loop(
            connector,
            shared,
            outbound_rx,
            event_tx,
            config,
            shutdown_rx,
        ));

        // 入站处理任务（解析 → 解析核心 ID → 发布 CoreEvent）。
        let processor = InboundProcessor {
            event_rx,
            conversation_manager: self.conversation_manager.clone(),
            bus: self.bus.clone(),
        };
        tokio::spawn(async move { processor.run().await });

        Ok(())
    }

    async fn stop(&self) -> Result<(), RuntimeError> {
        let _ = self.shutdown_tx.send(true);
        self.shared
            .set_state(OneBotConnectionState::Disconnected)
            .await;
        Ok(())
    }

    async fn state(&self) -> OneBotConnectionState {
        self.shared.state().await
    }

    async fn send_group_message(&self, group_id: &str, content: &str) -> Result<(), RuntimeError> {
        let request = build_group_send_request(group_id, content);
        self.enqueue_outgoing(&request).await
    }

    async fn send_private_message(&self, user_id: &str, content: &str) -> Result<(), RuntimeError> {
        let request = build_private_send_request(user_id, content);
        self.enqueue_outgoing(&request).await
    }
}

impl OneBotAdapterImpl {
    /// 将一个 OneBot 出站请求序列化并通过出站通道发送给连接循环。
    async fn enqueue_outgoing(&self, request: &OutgoingRequest) -> Result<(), RuntimeError> {
        // 仅允许在已连接状态下发送。
        if self.shared.state().await != OneBotConnectionState::Connected {
            return Err(RuntimeError::Adapter(
                "OneBot 未连接，暂时无法发送消息".to_string(),
            ));
        }

        let frame = serde_json::json!({
            "action": request.action,
            "params": request.params,
            "echo": "send",
        });
        let text = serde_json::to_string(&frame)
            .map_err(|e| RuntimeError::Adapter(format!("序列化出站请求失败: {e}")))?;

        self.shared
            .outbound_tx
            .send(text)
            .await
            .map_err(|e| RuntimeError::Adapter(format!("出站通道已关闭: {e}")))?;
        Ok(())
    }
}

/// 入站事件处理器 —— 从通道读取原始文本，转换并发布 CoreEvent。
///
/// 独立于连接循环运行；单独一条消息的处理失败不会导致任务退出。
struct InboundProcessor {
    event_rx: mpsc::Receiver<String>,
    conversation_manager: ConversationManager,
    bus: EventBus,
}

impl InboundProcessor {
    async fn run(mut self) {
        while let Some(raw) = self.event_rx.recv().await {
            // 通道发送方全部关闭 → 结束。
            self.handle_raw(&raw).await;
        }
    }

    async fn handle_raw(&self, raw: &str) {
        let event: OneBotEvent = match serde_json::from_str(raw) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(target: "adapter", error = %e, "无法解析 OneBot JSON");
                return;
            }
        };

        // 仅处理消息事件；元事件 / 通知 / 请求等忽略并记录。
        let inbound = match conversion::onebot_event_to_inbound(&event) {
            Ok(i) => i,
            Err(_) if !is_message_event(&event) => {
                tracing::debug!(target: "adapter", event = %event.post_type, "忽略非消息事件");
                return;
            }
            Err(e) => {
                tracing::debug!(target: "adapter", error = %e, "忽略无法转换的入站事件");
                return;
            }
        };

        // 解析外部 ID → 核心 ID。
        let resolved = match self
            .conversation_manager
            .resolve(
                inbound.conversation_type,
                &inbound.external_conversation_id,
                &inbound.external_sender_id,
                &inbound.display_name,
            )
            .await
        {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(target: "adapter", error = %e, "解析会话/参与者失败");
                return;
            }
        };
        let (conversation_id, sender_id) = resolved;

        // 硬性约束 B：指令消息在发布位截流——不发布 MessageReceived（不落库、
        // 不进角色上下文、不进插件 message 订阅），改为发布 CommandReceived。
        if let Some(command) = crate::application::command::classify(
            &inbound.content,
            inbound.is_mentioned,
            inbound.conversation_type,
        ) {
            let cmd_event = CoreEvent::CommandReceived(CommandReceivedEvent {
                conversation_id,
                sender_id,
                external_sender_id: inbound.external_sender_id.clone(),
                message_id: inbound.platform_message_id,
                content: inbound.content,
                timestamp: chrono::DateTime::from_timestamp(inbound.unix_time, 0)
                    .unwrap_or_else(chrono::Utc::now),
                command,
            });
            let receivers = self.bus.publish(&cmd_event);
            tracing::info!(target: "adapter", conversation_id, receivers, "收到系统指令，已截流发布 CommandReceived（不落库、不进角色上下文）");
            return;
        }

        let event_out = CoreEvent::MessageReceived(MessageReceivedEvent {
            conversation_id,
            sender_id,
            message_id: 0, // 内部消息 ID 由持久化层分配，此处占位
            content: inbound.content,
            timestamp: chrono::DateTime::from_timestamp(inbound.unix_time, 0)
                .unwrap_or_else(chrono::Utc::now),
            is_mentioned: inbound.is_mentioned,
        });

        let receivers = self.bus.publish(&event_out);
        tracing::info!(
            target: "adapter",
            conversation_id,
            sender_id,
            receivers,
            "已将入站消息发布为核心事件"
        );
    }
}

fn is_message_event(event: &OneBotEvent) -> bool {
    event.post_type == "message"
}

/// OneBot API 响应（发送动作后的回执，第一阶段仅用于记录）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OneBotApiResponse {
    pub status: Option<String>,
    pub retcode: Option<i32>,
    pub data: Option<serde_json::Value>,
    pub message: Option<String>,
    pub wording: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::conversation::ConversationManager;
    use crate::application::event_bus::EventSubscription;
    use crate::domain::conversation::{Conversation, Participant};
    use crate::domain::event::Command;
    use crate::domain::repository::{ConversationRepository, ParticipantRepository};
    use crate::error::RepositoryError;
    use async_trait::async_trait;
    use std::sync::Mutex as StdMutex;

    // ---- 内存仓储：会话与参与者（供 ConversationManager 解析外部 ID）----

    struct MemConversationRepo {
        convs: StdMutex<Vec<Conversation>>,
    }
    #[async_trait]
    impl ConversationRepository for MemConversationRepo {
        async fn find_by_id(&self, id: i64) -> Result<Option<Conversation>, RepositoryError> {
            Ok(self
                .convs
                .lock()
                .unwrap()
                .iter()
                .find(|c| c.id == id)
                .cloned())
        }
        async fn find_by_external_id(
            &self,
            _id: &str,
        ) -> Result<Option<Conversation>, RepositoryError> {
            Ok(None)
        }
        async fn find_all(&self) -> Result<Vec<Conversation>, RepositoryError> {
            Ok(self.convs.lock().unwrap().clone())
        }
        async fn insert(&self, _c: &Conversation) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _c: &Conversation) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemParticipantRepo;
    #[async_trait]
    impl ParticipantRepository for MemParticipantRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Participant>, RepositoryError> {
            Ok(None)
        }
        async fn find_by_external_id(
            &self,
            _conversation_id: i64,
            _external_id: &str,
        ) -> Result<Option<Participant>, RepositoryError> {
            Ok(None)
        }
        async fn find_by_conversation_id(
            &self,
            _conversation_id: i64,
        ) -> Result<Vec<Participant>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert(&self, _p: &Participant) -> Result<i64, RepositoryError> {
            Ok(1)
        }
    }

    /// 构造直接面向 `InboundProcessor::handle_raw` 的测试处理器（不启动连接循环）。
    fn processor() -> InboundProcessor {
        let conversation_manager = ConversationManager::new(
            Arc::new(MemConversationRepo {
                convs: StdMutex::new(vec![]),
            }),
            Arc::new(MemParticipantRepo),
        );
        let (_event_tx, event_rx) = mpsc::channel(8);
        InboundProcessor {
            event_rx,
            conversation_manager,
            bus: EventBus::new(),
        }
    }

    /// 把当前排队的全部总线事件取出来（handle_raw 已 await 完成，无竞态）。
    fn drain(sub: &mut EventSubscription) -> Vec<CoreEvent> {
        let mut events = Vec::new();
        while let Some(ev) = sub.try_recv() {
            events.push(ev);
        }
        events
    }

    /// 群聊消息 JSON（可选 at 段；文本内容由 `text` 给出）。
    fn group_message_json(text: &str, with_at: bool) -> String {
        let mut message = vec![serde_json::json!({"type": "text", "data": {"text": text}})];
        if with_at {
            message.insert(
                0,
                serde_json::json!({"type": "at", "data": {"qq": "123456", "name": "小助手"}}),
            );
        }
        serde_json::json!({
            "post_type": "message",
            "time": 1690000000,
            "self_id": 123456,
            "message_type": "group",
            "message_id": 1001,
            "group_id": 500000,
            "user_id": 900001,
            "message": message,
            "sender": {"user_id": 900001, "nickname": "小明", "card": "", "role": "member"}
        })
        .to_string()
    }

    /// 私聊消息 JSON（无 at 段概念）。
    fn private_message_json(text: &str) -> String {
        serde_json::json!({
            "post_type": "message",
            "time": 1690000000,
            "self_id": 123456,
            "message_type": "private",
            "message_id": 1002,
            "user_id": 555,
            "message": text,
            "sender": {"user_id": 555, "nickname": "小红"}
        })
        .to_string()
    }

    // ------------------------------------------------------------------
    // 发布前截流（硬性约束 B）测试
    // ------------------------------------------------------------------

    /// 群聊 @ + "换角色 木然" → 只发布 CommandReceived，绝不发布 MessageReceived。
    #[tokio::test]
    async fn command_message_is_intercepted_not_forwarded_as_message() {
        let p = processor();
        let mut sub = p.bus.subscribe();

        p.handle_raw(&group_message_json("换角色 木然", true)).await;

        let events = drain(&mut sub);
        assert_eq!(events.len(), 1, "应只发布一条事件（指令被截流）");
        match &events[0] {
            CoreEvent::CommandReceived(e) => {
                assert_eq!(e.conversation_id, 1);
                assert_eq!(e.sender_id, 1);
                assert_eq!(e.external_sender_id, "900001");
                assert_eq!(e.message_id, 1001);
                assert_eq!(e.content, "换角色 木然");
                assert_eq!(
                    e.command,
                    Command::SwitchCharacter {
                        character_name: "木然".to_string(),
                    }
                );
            }
            other => panic!("期望 CommandReceived，实际 {other:?}"),
        }
    }

    /// 普通消息（不含指令前缀）→ 仍发布 MessageReceived（回归保障）。
    #[tokio::test]
    async fn normal_message_still_published_as_message_received() {
        let p = processor();
        let mut sub = p.bus.subscribe();

        p.handle_raw(&group_message_json("你好", false)).await;

        let events = drain(&mut sub);
        assert_eq!(events.len(), 1, "普通消息应只发布一条事件");
        assert!(
            matches!(&events[0], CoreEvent::MessageReceived(e) if e.content == "你好"),
            "普通消息应发布 MessageReceived，实际 {:?}",
            events[0]
        );
    }

    /// 私聊无 at 的"换角色 木然" → 同样识别为指令（私聊无需 @）。
    #[tokio::test]
    async fn private_command_without_at_is_intercepted() {
        let p = processor();
        let mut sub = p.bus.subscribe();

        p.handle_raw(&private_message_json("换角色 木然")).await;

        let events = drain(&mut sub);
        assert_eq!(events.len(), 1, "私聊指令应只发布一条事件");
        match &events[0] {
            CoreEvent::CommandReceived(e) => {
                assert_eq!(e.external_sender_id, "555");
                assert_eq!(e.message_id, 1002);
                assert_eq!(
                    e.command,
                    Command::SwitchCharacter {
                        character_name: "木然".to_string(),
                    }
                );
            }
            other => panic!("期望 CommandReceived，实际 {other:?}"),
        }
    }

    /// 群聊未 at 的"换角色 木然" → 不识别，继续发布 MessageReceived。
    #[tokio::test]
    async fn group_command_without_mention_not_intercepted() {
        let p = processor();
        let mut sub = p.bus.subscribe();

        p.handle_raw(&group_message_json("换角色 木然", false))
            .await;

        let events = drain(&mut sub);
        assert_eq!(events.len(), 1, "未 @ 的群聊消息应走普通路径");
        assert!(
            matches!(&events[0], CoreEvent::MessageReceived(e) if e.content == "换角色 木然"),
            "未 @ 的群聊指令应发布 MessageReceived，实际 {:?}",
            events[0]
        );
    }
}
