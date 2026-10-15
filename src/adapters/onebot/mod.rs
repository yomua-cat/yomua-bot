//! OneBot 11 适配器 — 通过 WebSocket 连接 NapCat。
//!
//! 将 OneBot JSON 事件转换为核心事件，并将核心动作转换为 OneBot API 调用。
//! 使用指数退避策略处理重连。适配器断开时核心不会退出。
//!
//! 模块划分：
//! - `mod` —— 配置、连接状态、适配器 trait 与 `OneBotAdapterImpl` 实现
//! - [`conversion`] —— OneBot JSON ↔ 平台无关消息的纯函数转换
//! - [`connection`] —— WebSocket 传输、断线重连与指数退避

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::timeout as tokio_timeout;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch, Mutex};

use crate::application::binding::BindingManager;
use crate::application::conversation::ConversationManager;
use crate::application::event_bus::EventBus;
use crate::application::message_acceptance::MessageAcceptanceService;
use crate::domain::blacklist::BlacklistType;
use crate::domain::event::{CommandReceivedEvent, CoreEvent};
use crate::domain::repository::BlacklistRepository;
use crate::error::RuntimeError;

pub mod connection;
pub mod conversion;

pub use connection::ActionResponse;
use connection::{ConnectionShared, TungsteniteConnector, WsConnector};
pub use conversion::{
    build_get_message_request, build_group_send_request, build_private_send_request, OneBotEvent,
    OutgoingRequest,
};

/// OneBot 适配器配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OneBotConfig {
    /// WebSocket URL（例如 "ws://127.0.0.1:3001"）。
    pub websocket_url: String,

    /// 访问令牌（若 NapCat 需要）。
    pub access_token: Option<String>,

    /// Bot 自身 ID（用于识别自己发的消息）。
    pub bot_self_id: Option<i64>,

    /// 重连间隔（秒），作为指数退避的基准。
    pub reconnect_interval_secs: u64,

    /// 最大重连间隔（秒）。
    pub max_reconnect_interval_secs: u64,

    /// 心跳间隔（秒）。
    pub heartbeat_interval_secs: u64,

    /// Action 请求超时秒数。超时后 pending 请求被清理，调用方收到超时错误。
    pub action_timeout_secs: u64,
}

impl Default for OneBotConfig {
    fn default() -> Self {
        Self {
            websocket_url: "ws://127.0.0.1:3001".to_string(),
            access_token: None,
            bot_self_id: None,
            reconnect_interval_secs: 1,
            max_reconnect_interval_secs: 60,
            heartbeat_interval_secs: 30,
            action_timeout_secs: 10,
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

    /// 发送一条群消息，等待 NapCat Action Response。
    ///
    /// 返回值语义：
    /// - `Ok(response)` = NapCat 返回了响应（需检查 `response.is_ok()` 确认 retcode == 0）
    /// - `Err(...)` = 连接层错误（未连接 / 超时 / 连接断开）
    async fn send_group_message(
        &self,
        group_id: &str,
        content: &str,
    ) -> Result<ActionResponse, RuntimeError>;

    /// 发送一条私聊消息，等待 NapCat Action Response。
    async fn send_private_message(
        &self,
        user_id: &str,
        content: &str,
    ) -> Result<ActionResponse, RuntimeError>;
}

/// OneBot 适配器的具体实现。
///
/// 拥有连接状态、出站通道与入站事件通道，并负责把收到的 OneBot 事件
/// 解析为 CoreEvent 后发布到事件总线。
pub struct OneBotAdapterImpl {
    config: OneBotConfig,
    bus: EventBus,
    conversation_manager: ConversationManager,
    /// C2-4: MessageAcceptanceService for durable message acceptance
    acceptance_service: Arc<MessageAcceptanceService>,
    /// C2-4: BindingManager for active character determination
    binding_manager: Arc<BindingManager>,
    /// 入站硬拦截：用户或群组在有效黑名单中时不进入命令、消息和插件链路。
    blacklist_repo: Option<Arc<dyn BlacklistRepository>>,
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
    /// 适配器常驻任务句柄；关停时必须等待或取消，避免任务继续访问已关闭资源。
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// 连接器（真实或测试用假连接）。
    connector: Arc<dyn WsConnector>,
}

impl OneBotAdapterImpl {
    /// 使用默认的真实 WebSocket 连接器创建一个适配器。
    pub async fn new(
        config: OneBotConfig,
        bus: EventBus,
        conversation_manager: ConversationManager,
        acceptance_service: Arc<MessageAcceptanceService>,
        binding_manager: Arc<BindingManager>,
    ) -> Self {
        Self::with_connector(
            config,
            bus,
            conversation_manager,
            acceptance_service,
            binding_manager,
            Arc::new(TungsteniteConnector),
        )
    }

    /// 使用指定连接器创建适配器（便于测试注入假连接）。
    pub fn with_connector(
        config: OneBotConfig,
        bus: EventBus,
        conversation_manager: ConversationManager,
        acceptance_service: Arc<MessageAcceptanceService>,
        binding_manager: Arc<BindingManager>,
        connector: Arc<dyn WsConnector>,
    ) -> Self {
        let (outbound_tx, outbound_rx) = mpsc::channel(128);
        let (event_tx, event_rx) = mpsc::channel(256);
        let (response_tx, _) = mpsc::channel(64);
        let (shutdown_tx, _) = watch::channel(false);

        let shared = ConnectionShared {
            state: Arc::new(Mutex::new(OneBotConnectionState::Disconnected)),
            bus: bus.clone(),
            outbound_tx,
            response_tx,
            pending: Arc::new(StdMutex::new(HashMap::new())),
            request_id_counter: Arc::new(AtomicU64::new(0)),
            request_timeout_secs: config.action_timeout_secs.max(1),
        };

        Self {
            config,
            bus,
            conversation_manager,
            acceptance_service,
            binding_manager,
            blacklist_repo: None,
            shared,
            outbound_rx: Mutex::new(Some(outbound_rx)),
            event_rx: Mutex::new(Some(event_rx)),
            event_tx,
            shutdown_tx,
            started: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            connector,
        }
    }

    /// 装配入站黑名单仓储。测试和不需要持久黑名单的调用方可保持默认关闭。
    pub fn with_blacklist_repo(mut self, repo: Arc<dyn BlacklistRepository>) -> Self {
        self.blacklist_repo = Some(repo);
        self
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
        let connection_task = tokio::spawn(connection::run_connection_loop(
            connector,
            shared,
            outbound_rx,
            event_tx,
            config,
            shutdown_rx,
        ));

        // 入站处理任务（解析 → 解析核心 ID → Acceptance → 发布 CoreEvent）。
        let processor = InboundProcessor {
            event_rx,
            conversation_manager: self.conversation_manager.clone(),
            bus: self.bus.clone(),
            acceptance_service: self.acceptance_service.clone(),
            binding_manager: self.binding_manager.clone(),
            blacklist_repo: self.blacklist_repo.clone(),
        };
        let processor_task = tokio::spawn(async move { processor.run().await });
        self.tasks
            .lock()
            .await
            .extend([connection_task, processor_task]);

        Ok(())
    }

    async fn stop(&self) -> Result<(), RuntimeError> {
        let _ = self.shutdown_tx.send(true);
        self.shared
            .set_state(OneBotConnectionState::Disconnected)
            .await;
        let tasks = std::mem::take(&mut *self.tasks.lock().await);
        let wait = Duration::from_secs(self.config.action_timeout_secs.max(1));
        for mut task in tasks {
            if tokio::time::timeout(wait, &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
        Ok(())
    }

    async fn state(&self) -> OneBotConnectionState {
        self.shared.state().await
    }

    async fn send_group_message(
        &self,
        group_id: &str,
        content: &str,
    ) -> Result<ActionResponse, RuntimeError> {
        let request = build_group_send_request(group_id, content);
        self.send_action(request).await
    }

    async fn send_private_message(
        &self,
        user_id: &str,
        content: &str,
    ) -> Result<ActionResponse, RuntimeError> {
        let request = build_private_send_request(user_id, content);
        self.send_action(request).await
    }
}

impl OneBotAdapterImpl {
    /// 查询平台上的单条消息，用于核查发送结果未知的任务。
    pub async fn get_message(&self, message_id: i64) -> Result<ActionResponse, RuntimeError> {
        self.send_action(build_get_message_request(message_id))
            .await
    }

    /// 发送一个 Action 请求，等待 NapCat 响应。
    ///
    /// 流程：注册 pending 请求 → 发送 WebSocket 帧 → 等待响应或超时 → 清理 pending。
    async fn send_action(&self, request: OutgoingRequest) -> Result<ActionResponse, RuntimeError> {
        // 仅在已连接状态下才能发送。
        if self.shared.state().await != OneBotConnectionState::Connected {
            return Err(RuntimeError::SendRejected(
                "OneBot 未连接，暂时无法发送消息".to_string(),
            ));
        }

        // 注册 pending 请求，获取唯一 request_id。
        let (id, response_rx) = self.shared.insert_pending(request.action.clone());

        // 构建帧，使用 request_id 作为 echo。
        let frame = serde_json::json!({
            "action": request.action,
            "params": request.params,
            "echo": id.to_string(),
        });
        let text = serde_json::to_string(&frame)
            .map_err(|_e| RuntimeError::SendRejected("消息序列化失败".to_string()))?;

        // 发送到出站 channel。
        if self.shared.outbound_tx.send(text).await.is_err() {
            self.shared.pending.lock().unwrap().remove(&id);
            return Err(RuntimeError::SendRejected(
                "出站通道已关闭，消息未发送".to_string(),
            ));
        }

        // 等待 NapCat 响应或超时。
        let timeout = Duration::from_secs(self.shared.request_timeout_secs);
        match tokio_timeout(timeout, response_rx).await {
            Ok(Ok(response)) => {
                tracing::debug!(
                    target: "adapter",
                    id,
                    status = %response.status,
                    "action response received"
                );
                Ok(response)
            }
            Ok(Err(_)) => {
                // oneshot 被 drop（请求被清理，如连接断开时 cleanup_timed_out_requests）
                Err(RuntimeError::SendUnknown(
                    "请求被清理（连接已断开）".to_string(),
                ))
            }
            Err(_) => {
                // 超时：从 pending 中移除（complete_request 已被超时前的 insert_pending 覆盖？不，
                // 超时后响应仍可能到达，但调用方已不再等待，所以安全清理。
                self.shared.pending.lock().unwrap().remove(&id);
                tracing::warn!(target: "adapter", id, "action request timeout");
                Err(RuntimeError::SendUnknown(format!(
                    "等待 NapCat 响应超时（{} 秒）",
                    timeout.as_secs()
                )))
            }
        }
    }
}

/// 入站事件处理器 —— 从通道读取原始文本，转换并发布 CoreEvent。
///
/// 独立于连接循环运行；单独一条消息的处理失败不会导致任务退出。
struct InboundProcessor {
    event_rx: mpsc::Receiver<String>,
    conversation_manager: ConversationManager,
    bus: EventBus,
    acceptance_service: Arc<MessageAcceptanceService>,
    binding_manager: Arc<BindingManager>,
    blacklist_repo: Option<Arc<dyn BlacklistRepository>>,
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

        // self_id 来自平台事件本身，比本地可选配置更可靠。自身回声不能进入
        // 命令、关系、记忆或回复链路；发送后的 assistant 历史由出站路径落库。
        if inbound.is_self_echo {
            tracing::debug!(
                target: "adapter",
                platform_message_id = inbound.platform_message_id,
                "忽略 OneBot 自身回声事件"
            );
            return;
        }

        // 黑名单必须在 resolve 之前检查。resolve 会按需创建会话和参与者，
        // 否则首次来自被拉黑目标的消息仍会留下身份记录。
        if let Some(blacklist_repo) = &self.blacklist_repo {
            let user_blocked = match blacklist_repo
                .is_blacklisted(&inbound.external_sender_id, BlacklistType::User)
                .await
            {
                Ok(blocked) => blocked,
                Err(error) => {
                    tracing::warn!(target: "adapter", %error, "查询用户黑名单失败，拒绝本条入站消息");
                    return;
                }
            };
            if user_blocked {
                tracing::info!(target: "adapter", sender_id = %inbound.external_sender_id, "用户处于黑名单，忽略入站消息");
                return;
            }
            if inbound.conversation_type == crate::domain::conversation::ConversationType::Group {
                let group_blocked = match blacklist_repo
                    .is_blacklisted(&inbound.external_conversation_id, BlacklistType::Group)
                    .await
                {
                    Ok(blocked) => blocked,
                    Err(error) => {
                        tracing::warn!(target: "adapter", %error, "查询群组黑名单失败，拒绝本条入站消息");
                        return;
                    }
                };
                if group_blocked {
                    tracing::info!(target: "adapter", group_id = %inbound.external_conversation_id, "群组处于黑名单，忽略入站消息");
                    return;
                }
            }
        }

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

        // C2-4: 通过 BindingManager 确定 active character
        // 与 ReplyProcessor 行为一致：无绑定时会话不处理（不落库、不进角色上下文）
        let bindings = match self.binding_manager.by_conversation(conversation_id).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    target: "adapter",
                    error = %e,
                    conversation_id,
                    "查询绑定失败，忽略消息"
                );
                return;
            }
        };

        if bindings.is_empty() {
            tracing::debug!(
                target: "adapter",
                conversation_id,
                "该会话未绑定角色，忽略消息（与 ReplyProcessor 行为一致）"
            );
            return;
        }

        // 取 switched_at 最新的绑定（与 switch_character 逻辑一致）
        let mut sorted = bindings;
        sorted.sort_by_key(|b| std::cmp::Reverse(b.switched_at));
        let character_id = sorted[0].character_id;

        // C2-4: 调用 MessageAcceptanceService 进行 durable acceptance
        let timestamp =
            chrono::DateTime::from_timestamp(inbound.unix_time, 0).unwrap_or_else(chrono::Utc::now);
        let acceptance_result = self
            .acceptance_service
            .accept_inbound(
                conversation_id,
                sender_id,
                inbound.content.clone(),
                timestamp,
                Some(inbound.platform_message_id),
                character_id,
                inbound.is_mentioned,
                serde_json::json!({"author_kind":"user","external_sender_id":inbound.external_sender_id,"display_name":inbound.display_name,"mentions":inbound.mention_targets,"reply_to_platform_message_id":inbound.reply_to,"content_metadata":inbound.content_metadata,"binding_id":sorted[0].id,"binding_version":sorted[0].switched_at}),
            )
            .await;

        let (message_id, conversation_sequence) = match acceptance_result {
            Ok(result) => result,
            Err(e) => {
                // Acceptance 失败是严重的——消息未被持久化但入站已接收
                // 记录错误并继续（不能 silence drop）
                tracing::error!(
                    target: "adapter",
                    error = %e,
                    conversation_id,
                    sender_id,
                    character_id,
                    "MessageAcceptanceService.accept() 失败，消息未持久化"
                );
                return;
            }
        };

        tracing::debug!(
            target: "adapter",
            conversation_id,
            sender_id,
            message_id,
            conversation_sequence,
            character_id,
            "C2-4: MessageAcceptanceService.accept() 成功"
        );

        tracing::info!(
            target: "adapter",
            conversation_id,
            sender_id,
            message_id,
            conversation_sequence,
            "C2-4: 入站消息已通过 Acceptance；canonical MessageAccepted 事件由事务入口发布"
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
    use crate::application::binding::BindingManager;
    use crate::application::conversation::ConversationManager;
    use crate::application::event_bus::EventSubscription;
    use crate::application::message_acceptance::MessageAcceptanceService;
    use crate::domain::conversation::{Conversation, Participant};
    use crate::domain::event::Command;
    use crate::domain::repository::{
        CharacterBindingRepository, CharacterRepository, ConversationRepository, MessageRepository,
        ParticipantRepository,
    };
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
        async fn allocate_sequence(&self, _conversation_id: i64) -> Result<i64, RepositoryError> {
            Ok(1)
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
        let bus = EventBus::new();

        // C2-4: 使用内存仓储创建真实的 MessageAcceptanceService 和 BindingManager
        let mem_msg_repo: Arc<MemMessageRepo> = Arc::new(MemMessageRepo::new());
        let mem_char_repo: Arc<MemCharacterRepo> = Arc::new(MemCharacterRepo::new());
        let mem_binding_repo: Arc<MemBindingRepo> = Arc::new(MemBindingRepo::new());

        let acceptance_service = MessageAcceptanceService::new(
            mem_msg_repo.clone() as Arc<dyn MessageRepository>,
            bus.clone(),
        );
        let binding_manager = BindingManager::new(
            mem_binding_repo as Arc<dyn CharacterBindingRepository>,
            mem_char_repo as Arc<dyn CharacterRepository>,
            Arc::new(MemConversationRepo {
                convs: StdMutex::new(vec![]),
            }) as Arc<dyn ConversationRepository>,
        );

        InboundProcessor {
            event_rx,
            conversation_manager,
            bus,
            acceptance_service: Arc::new(acceptance_service),
            binding_manager: Arc::new(binding_manager),
            blacklist_repo: None,
        }
    }

    // ---- C2-4 测试用内存仓储 ----

    /// 内存消息仓储（测试用）
    struct MemMessageRepo {
        messages: StdMutex<Vec<crate::domain::message::Message>>,
        next_id: StdMutex<i64>,
    }
    impl MemMessageRepo {
        fn new() -> Self {
            Self {
                messages: StdMutex::new(vec![]),
                next_id: StdMutex::new(1),
            }
        }
    }
    #[async_trait]
    impl MessageRepository for MemMessageRepo {
        async fn find_by_id(
            &self,
            id: i64,
        ) -> Result<Option<crate::domain::message::Message>, RepositoryError> {
            let msgs = self.messages.lock().unwrap();
            Ok(msgs.iter().find(|m| m.id == id).cloned())
        }
        async fn find_recent(
            &self,
            conversation_id: i64,
            limit: i64,
        ) -> Result<Vec<crate::domain::message::Message>, RepositoryError> {
            let msgs = self.messages.lock().unwrap();
            let mut result: Vec<_> = msgs
                .iter()
                .filter(|m| m.conversation_id == conversation_id)
                .take(limit as usize)
                .cloned()
                .collect();
            result.reverse();
            Ok(result)
        }
        async fn find_recent_attributed(
            &self,
            conversation_id: i64,
            character_id: i64,
            limit: i64,
        ) -> Result<Vec<crate::domain::message::Message>, RepositoryError> {
            let msgs = self.messages.lock().unwrap();
            let mut result: Vec<_> = msgs
                .iter()
                .filter(|m| {
                    m.conversation_id == conversation_id
                        && m.active_character_id == Some(character_id)
                })
                .take(limit as usize)
                .cloned()
                .collect();
            result.reverse();
            Ok(result)
        }
        async fn insert(
            &self,
            msg: &crate::domain::message::Message,
        ) -> Result<i64, RepositoryError> {
            let mut msgs = self.messages.lock().unwrap();
            let mut next_id = self.next_id.lock().unwrap();
            let mut m = msg.clone();
            m.id = *next_id;
            *next_id += 1;
            msgs.push(m);
            Ok(msg.id)
        }
        async fn latest_message_time(
            &self,
            conversation_id: i64,
        ) -> Result<Option<chrono::DateTime<chrono::Utc>>, RepositoryError> {
            let msgs = self.messages.lock().unwrap();
            Ok(msgs
                .iter()
                .filter(|m| m.conversation_id == conversation_id)
                .map(|m| m.timestamp)
                .max())
        }
        async fn update_active_character_id(
            &self,
            message_id: i64,
            active_character_id: i64,
        ) -> Result<(), RepositoryError> {
            let mut msgs = self.messages.lock().unwrap();
            if let Some(m) = msgs.iter_mut().find(|m| m.id == message_id) {
                m.active_character_id = Some(active_character_id);
            }
            Ok(())
        }
        async fn find_by_conversation_sender_time_content(
            &self,
            conversation_id: i64,
            sender_id: i64,
            timestamp: chrono::DateTime<chrono::Utc>,
            content: &str,
        ) -> Result<Option<crate::domain::message::Message>, RepositoryError> {
            let msgs = self.messages.lock().unwrap();
            Ok(msgs
                .iter()
                .find(|m| {
                    m.conversation_id == conversation_id
                        && m.sender_id == sender_id
                        && m.timestamp == timestamp
                        && serde_json::to_string(&m.content)
                            .map(|c| c == content)
                            .unwrap_or(false)
                })
                .cloned())
        }
        async fn find_before_sequence(
            &self,
            conversation_id: i64,
            _character_id: i64,
            before_sequence: i64,
            limit: i64,
        ) -> Result<Vec<crate::domain::message::Message>, RepositoryError> {
            let msgs = self.messages.lock().unwrap();
            let mut result: Vec<_> = msgs
                .iter()
                .filter(|m| {
                    m.conversation_id == conversation_id
                        && m.conversation_sequence < Some(before_sequence)
                })
                .take(limit as usize)
                .cloned()
                .collect();
            result.reverse();
            Ok(result)
        }
        async fn find_recent_by_sequence(
            &self,
            conversation_id: i64,
            limit: i64,
        ) -> Result<Vec<crate::domain::message::Message>, RepositoryError> {
            let msgs = self.messages.lock().unwrap();
            let mut result: Vec<_> = msgs
                .iter()
                .filter(|m| m.conversation_id == conversation_id)
                .take(limit as usize)
                .cloned()
                .collect();
            result.reverse();
            Ok(result)
        }
        async fn accept_message(
            &self,
            input: &crate::domain::repository::AcceptedMessageInput,
        ) -> Result<(i64, i64, chrono::DateTime<chrono::Utc>), RepositoryError> {
            use chrono::DateTime;
            let timestamp_dt = DateTime::parse_from_rfc3339(&input.timestamp)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or_else(|_| chrono::Utc::now());
            let content: crate::domain::message::MessageContent =
                serde_json::from_str(&input.content_json).unwrap_or(
                    crate::domain::message::MessageContent::Text(input.content_json.clone()),
                );
            let ingested_at = chrono::Utc::now();
            let conversation_sequence = 1;
            let mut msg = crate::domain::message::Message {
                id: 0,
                conversation_id: input.conversation_id,
                sender_id: input.sender_id,
                content,
                timestamp: timestamp_dt,
                reply_to: None,
                mentions: vec![],
                attachments: vec![],
                metadata: serde_json::json!({}),
                active_character_id: Some(input.character_id),
                platform_message_id: input.platform_message_id,
                conversation_sequence: Some(conversation_sequence),
                occurred_at: Some(timestamp_dt),
                ingested_at: Some(ingested_at),
            };
            let id = {
                let mut msgs = self.messages.lock().unwrap();
                let mut next_id = self.next_id.lock().unwrap();
                msg.id = *next_id;
                *next_id += 1;
                msgs.push(msg.clone());
                msg.id
            };
            Ok((id, conversation_sequence, ingested_at))
        }
    }

    /// 内存角色仓储（测试用）
    struct MemCharacterRepo;
    impl MemCharacterRepo {
        fn new() -> Self {
            Self
        }
    }
    #[async_trait]
    impl CharacterRepository for MemCharacterRepo {
        async fn find_by_id(
            &self,
            _id: i64,
        ) -> Result<Option<crate::domain::character::Character>, RepositoryError> {
            Ok(None)
        }
        async fn find_all(
            &self,
        ) -> Result<Vec<crate::domain::character::Character>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert(
            &self,
            _c: &crate::domain::character::Character,
        ) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(
            &self,
            _c: &crate::domain::character::Character,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    /// 内存绑定仓储（测试用）
    struct MemBindingRepo {
        bindings: StdMutex<Vec<crate::domain::character::CharacterBinding>>,
    }
    impl MemBindingRepo {
        fn new() -> Self {
            // 提供默认绑定 so that acceptance proceeds (character_id=1)
            let default_binding = crate::domain::character::CharacterBinding {
                id: 1,
                character_id: 1,
                conversation_id: 1,
                reply_mode: crate::domain::character::ReplyMode::default(),
                proactive_enabled: false,
                mute_schedule: None,
                behavior_overrides: serde_json::json!({}),
                context_policy: serde_json::json!({}),
                switched_at: Some(chrono::Utc::now()),
                cross_reply_enabled: false,
                created_at: chrono::Utc::now(),
            };
            Self {
                bindings: StdMutex::new(vec![default_binding]),
            }
        }
    }

    struct MemBlacklistRepo {
        blocked: bool,
    }

    #[async_trait]
    impl crate::domain::repository::BlacklistRepository for MemBlacklistRepo {
        async fn find_all(
            &self,
        ) -> Result<Vec<crate::domain::blacklist::BlacklistEntry>, RepositoryError> {
            Ok(vec![])
        }

        async fn find_by_target(
            &self,
            _target: &str,
            _target_type: crate::domain::blacklist::BlacklistType,
        ) -> Result<Option<crate::domain::blacklist::BlacklistEntry>, RepositoryError> {
            Ok(None)
        }

        async fn is_blacklisted(
            &self,
            _target: &str,
            _target_type: crate::domain::blacklist::BlacklistType,
        ) -> Result<bool, RepositoryError> {
            Ok(self.blocked)
        }

        async fn insert(
            &self,
            _entry: &crate::domain::blacklist::NewBlacklistEntry,
        ) -> Result<i64, RepositoryError> {
            Ok(1)
        }

        async fn update_reason(
            &self,
            _id: i64,
            _reason: Option<String>,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        async fn set_expiration(
            &self,
            _id: i64,
            _expires_at: Option<chrono::DateTime<chrono::Utc>>,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }

        async fn delete_by_target(
            &self,
            _target: &str,
            _target_type: crate::domain::blacklist::BlacklistType,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }
    #[async_trait]
    impl CharacterBindingRepository for MemBindingRepo {
        async fn find_by_character_id(
            &self,
            _character_id: i64,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(self.bindings.lock().unwrap().clone())
        }
        async fn find_by_conversation_id(
            &self,
            _conversation_id: i64,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(self.bindings.lock().unwrap().clone())
        }
        async fn find_all(
            &self,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_all_enabled(
            &self,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert(
            &self,
            _b: &crate::domain::character::CharacterBinding,
        ) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(
            &self,
            _b: &crate::domain::character::CharacterBinding,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
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

    /// 普通消息（不含指令前缀）→ 发布持久化后的 MessageAccepted。
    #[tokio::test]
    async fn normal_message_publishes_message_accepted_once() {
        let p = processor();
        let mut sub = p.bus.subscribe();

        p.handle_raw(&group_message_json("你好", false)).await;

        let events = drain(&mut sub);
        assert_eq!(events.len(), 1, "普通消息应只发布一条事件");
        assert!(
            matches!(&events[0], CoreEvent::MessageAccepted(e) if e.content == "你好"),
            "普通消息应发布 MessageAccepted，实际 {:?}",
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

    #[tokio::test]
    async fn blacklisted_inbound_message_is_dropped_before_command_or_persistence() {
        let mut p = processor();
        p.blacklist_repo = Some(Arc::new(MemBlacklistRepo { blocked: true }));
        let mut sub = p.bus.subscribe();

        p.handle_raw(&group_message_json("你好", false)).await;

        assert!(drain(&mut sub).is_empty(), "黑名单消息不得发布任何核心事件");
    }

    /// 群聊未 at 的"换角色 木然" → 不识别，继续走普通持久化消息路径。
    #[tokio::test]
    async fn group_command_without_mention_not_intercepted() {
        let p = processor();
        let mut sub = p.bus.subscribe();

        p.handle_raw(&group_message_json("换角色 木然", false))
            .await;

        let events = drain(&mut sub);
        assert_eq!(events.len(), 1, "未 @ 的群聊消息应走普通路径");
        assert!(
            matches!(&events[0], CoreEvent::MessageAccepted(e) if e.content == "换角色 木然"),
            "未 @ 的群聊指令应发布 MessageAccepted，实际 {:?}",
            events[0]
        );
    }
}
