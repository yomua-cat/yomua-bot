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
use crate::domain::event::{CoreEvent, MessageReceivedEvent};
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
