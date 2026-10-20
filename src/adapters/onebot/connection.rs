//! OneBot WebSocket 连接管理与断线重连。
//!
//! 关键设计：通过 `WsTransport` trait 抽象真实 WebSocket，使重连/退避逻辑
//! 可以在不依赖真实网络的情况下单元测试（注入假连接）。
//!
//! 连接循环保证：即使 NapCat 崩溃或断线，该循环也会按指数退避不断重连，
//! 永远不会让 Core 进程退出。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, watch, Mutex};

use serde_json;

use crate::error::RuntimeError;

use super::{OneBotConfig, OneBotConnectionState};
use crate::application::event_bus::EventBus;
use crate::domain::event::{AdapterConnectedEvent, AdapterDisconnectedEvent, CoreEvent};

const MAX_PENDING_INBOUND_EVENTS: usize = 1024;

/// Action 请求的完整信息，包含唯一 ID 和响应接收端。
#[derive(Debug)]
pub struct PendingRequest {
    /// 请求 ID（用作 OneBot echo 字段）。
    pub id: u64,
    /// 请求的动作名称（用于日志）。
    pub action: String,
    /// 响应接收端（由调用方持有）。
    pub response_tx: oneshot::Sender<ActionResponse>,
}

/// 待处理的 API 响应（来自 NapCat）。
#[derive(Debug, Clone)]
pub struct ActionResponse {
    /// 请求 ID（echo）。
    pub id: u64,
    /// 响应的动作名称。
    pub action: String,
    /// 状态（ok / failed）。
    pub status: String,
    /// OneBot 错误码（0 = 成功）。
    pub retcode: Option<i32>,
    /// 成功时返回的数据。
    pub data: Option<serde_json::Value>,
    /// 错误信息。
    pub error: Option<String>,
    /// OneBot 标准失败回执中的简短消息。
    pub message: Option<String>,
    /// NapCat 常用的面向用户错误说明。
    pub wording: Option<String>,
}

impl ActionResponse {
    /// 是否表示成功。
    pub fn is_ok(&self) -> bool {
        self.status.eq_ignore_ascii_case("ok") && self.retcode == Some(0)
    }

    /// 将错误信息格式化为字符串。
    pub fn error_message(&self) -> String {
        let mut messages = Vec::new();
        for value in [&self.error, &self.message, &self.wording]
            .into_iter()
            .flatten()
            .map(String::as_str)
        {
            if !value.trim().is_empty() && !messages.iter().any(|item| item == &value) {
                messages.push(value);
            }
        }
        if !messages.is_empty() {
            return messages.join(" | ");
        }
        if let Some(code) = self.retcode {
            if code != 0 {
                return format!("OneBot API 错误码: {code}");
            }
        }
        String::new()
    }
}

/// 入站消息分类：事件或 Action 响应。
#[derive(Debug)]
pub enum IncomingMessage {
    /// OneBot 事件（如 message、notice、request）。
    Event(String),
    /// Action 响应（带 echo/retcode）。
    Response(ActionResponse),
}

/// 一次 WebSocket 文本帧读取结果。
///
/// `Ok(None)` 表示连接已关闭或到达数据流末尾。
#[async_trait]
pub trait WsTransport: Send {
    /// 读取下一条文本消息；`Ok(None)` 表示连接关闭。
    async fn read_text(&mut self) -> Result<Option<String>, RuntimeError>;

    /// 发送一条文本消息。
    async fn send_text(&mut self, text: &str) -> Result<(), RuntimeError>;

    /// 发送一次协议层 Ping（保持连接活跃）。
    async fn ping(&mut self) -> Result<(), RuntimeError>;
}

/// 打开真实 WebSocket 连接的连接器。
#[async_trait]
pub trait WsConnector: Send + Sync {
    /// 建立连接并返回传输句柄。失败时返回错误（上层负责退避重试）。
    async fn connect(&self, config: &OneBotConfig) -> Result<Box<dyn WsTransport>, RuntimeError>;
}

/// 基于 `tokio-tungstenite` 的真实连接器。
pub struct TungsteniteConnector;

#[async_trait]
impl WsConnector for TungsteniteConnector {
    async fn connect(&self, config: &OneBotConfig) -> Result<Box<dyn WsTransport>, RuntimeError> {
        let transport = TungsteniteTransport::connect(config).await?;
        Ok(Box::new(transport))
    }
}

/// 共享的连接状态，供适配器查询与连接循环更新。
#[derive(Clone)]
pub struct ConnectionShared {
    /// 当前连接状态。
    pub state: Arc<Mutex<OneBotConnectionState>>,
    /// 事件总线（用于发布连接/断开事件）。
    pub bus: EventBus,
    /// 出站通道（适配器写入动作 JSON，连接循环读取后发送）。
    pub outbound_tx: mpsc::Sender<String>,
    /// Action 响应通道（连接循环读取响应并路由，ResponseRouter 接收）。
    pub response_tx: mpsc::Sender<ActionResponse>,
    /// 待处理请求注册表（request_id → PendingRequest）。
    pub pending: Arc<StdMutex<HashMap<u64, PendingRequest>>>,
    /// 唯一请求 ID 计数器。
    pub request_id_counter: Arc<AtomicU64>,
    /// 请求超时（秒），超时后清理 pending 请求。
    pub request_timeout_secs: u64,
}

impl ConnectionShared {
    /// 设置状态并记录日志。
    pub async fn set_state(&self, state: OneBotConnectionState) {
        *self.state.lock().await = state;
    }

    /// 读取当前状态。
    pub async fn state(&self) -> OneBotConnectionState {
        *self.state.lock().await
    }

    /// 注册一个待处理请求。
    ///
    /// 生成唯一 request_id，写入注册表，返回 request_id 和对应的响应接收端。
    pub fn insert_pending(&self, action: String) -> (u64, oneshot::Receiver<ActionResponse>) {
        let id = self.request_id_counter.fetch_add(1, Ordering::SeqCst);
        let (response_tx, response_rx) = oneshot::channel();
        let request = PendingRequest {
            id,
            action: action.clone(),
            response_tx,
        };
        self.pending.lock().unwrap().insert(id, request);
        tracing::debug!(target: "adapter", id, action, "pending request registered");
        (id, response_rx)
    }

    /// 根据 request_id 完成一个 pending 请求。
    ///
    /// 如果请求已超时被清理，返回 false。
    pub fn complete_request(&self, id: u64, response: ActionResponse) -> bool {
        let mut pending = self.pending.lock().unwrap();
        let Some(req) = pending.get(&id) else {
            tracing::warn!(target: "adapter", id, "response received for unknown or timed-out request");
            return false;
        };
        // OneBot 11 的部分实现省略 action 字段，因此空 action 仍按 echo
        // 关联；如果平台明确返回了不同动作，必须保留 pending，等待正确回执
        // 或请求超时，不能把别的请求结果交给当前发送者。
        if !response.action.is_empty() && response.action != req.action {
            tracing::warn!(
                target: "adapter",
                id,
                expected_action = %req.action,
                response_action = %response.action,
                "响应动作与请求不匹配，忽略响应"
            );
            return false;
        }
        let req = pending.remove(&id).expect("pending 请求刚刚确认存在");
        drop(pending);
        {
            // 通知已等待的调用方。
            // 如果接收端已被 drop（调用方超时），send 会失败但这是正常现象。
            let _ = req.response_tx.send(response);
            tracing::debug!(target: "adapter", id, "pending request completed");
            true
        }
    }

    /// 清理所有超时的 pending 请求。
    ///
    /// 返回被清理的数量。
    pub fn cleanup_timed_out_requests(&self) -> usize {
        // 当前实现中，超时由调用方的 oneshot 超时处理。
        // 此方法用于在连接断开时清空所有 pending 请求。
        let mut pending = self.pending.lock().unwrap();
        let count = pending.len();
        // Draining the HashMap to take ownership of the values.
        let requests: Vec<_> = pending.drain().map(|(_, v)| v).collect();
        drop(pending);
        // 连接断开时没有平台回执，不能伪造一个 `failed` 响应。
        // 直接丢弃发送端，让 `send_action` 收到 oneshot 关闭并转换为
        // `SendUnknown`；只有平台明确返回失败时才允许安全重试。
        drop(requests);
        count
    }
}

/// 指数退避调度器。
///
/// 采用"失败次数上限 + 指数增长 + 截断到最大值"的策略：
/// `delay = min(base * 2^attempt, max)`。
pub struct Backoff {
    /// 基准间隔。
    base: Duration,
    /// 最大间隔。
    max: Duration,
    /// 连续失败次数。
    attempt: u32,
}

impl Backoff {
    /// 创建一个退避调度器。
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            attempt: 0,
        }
    }

    /// 计算下一次重连前的等待时长，并推进重试计数。
    pub fn next_delay(&mut self) -> Duration {
        // 指数增长到 max 后截断。
        let factor = self.factor();
        let delay = self.base.saturating_mul(factor).min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        delay
    }

    /// 连接成功后重置退避计数。
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// 当前连续失败次数。
    pub fn attempts(&self) -> u32 {
        self.attempt
    }

    /// 查看下一次重连的等待时长（不推进重试计数）。
    pub fn peek_latest(&self) -> Duration {
        let factor = self.factor();
        self.base.saturating_mul(factor).min(self.max)
    }

    /// 计算当前尝试次数的指数因子 `2^attempt`（超过 u32 上限时截断到最大值）。
    ///
    /// 这里只做秒级退避，`2^31` 秒已远超任何实际重连需求，故用 u32 足够。
    fn factor(&self) -> u32 {
        let attempt = self.attempt.min(31);
        1u32 << attempt
    }
}

/// 运行连接循环（直到收到关停信号）。
///
/// 参数：
/// - `connector`：用于建立真实连接的连接器
/// - `shared`：共享的连接状态与出站通道
/// - `outbound_rx`：出站消息接收端（连接循环读取并发送到 WebSocket）
/// - `event_tx`：入站原始文本的接收端（连接循环把收到的帧转发到处理器）
/// - `config`：连接配置
/// - `shutdown`：关停信号接收端
pub async fn run_connection_loop(
    connector: Arc<dyn WsConnector>,
    shared: ConnectionShared,
    mut outbound_rx: mpsc::Receiver<String>,
    event_tx: mpsc::Sender<String>,
    config: OneBotConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    // 把当前的关停信号值标记为"已见"，避免 `changed()` 在首个新接收器上立刻返回，
    // 否则会在首次成功连接后立即误判为关停（见测试 inbound_message_forwarded...）。
    let _ = shutdown.borrow_and_update();

    let mut backoff = Backoff::new(
        Duration::from_secs(config.reconnect_interval_secs.max(1)),
        Duration::from_secs(config.max_reconnect_interval_secs.max(1)),
    );

    loop {
        // 收到关停信号则退出。
        if *shutdown.borrow() {
            break;
        }

        shared.set_state(OneBotConnectionState::Connecting).await;
        tracing::info!(
            target: "adapter",
            url = %config.websocket_url,
            "正在连接 OneBot..."
        );

        let transport = match connector.connect(&config).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    target: "adapter",
                    error = %e,
                    retry_in_secs = backoff.next_delay().as_secs(),
                    "OneBot 连接失败，准备重连"
                );
                shared.set_state(OneBotConnectionState::Reconnecting).await;
                publish_disconnected(&shared, Some(e.to_string()));
                sleep_until_shutdown(backoff.peek_latest(), &mut shutdown).await;
                continue;
            }
        };

        // 连接成功，重置退避。
        backoff.reset();
        shared.set_state(OneBotConnectionState::Connected).await;
        tracing::info!(target: "adapter", "OneBot 已连接");
        publish_connected(&shared);

        // 在连接存续期间处理读写。返回时表示连接已断开。
        handle_connected_io(
            transport,
            Arc::new(shared.clone()),
            &mut outbound_rx,
            &event_tx,
            config.heartbeat_interval_secs,
            &mut shutdown,
        )
        .await;

        // 连接断开时，清理所有 pending 请求。
        let cleared = shared.cleanup_timed_out_requests();
        if cleared > 0 {
            tracing::warn!(target: "adapter", count = cleared, "连接断开，清理 pending 请求");
        }

        // 连接 IO 因关停信号返回时，不要进入重连分支。否则 stop() 已经
        // 设置为 Disconnected，下面的状态更新会把它错误地改回 Reconnecting。
        if *shutdown.borrow() {
            break;
        }

        shared.set_state(OneBotConnectionState::Reconnecting).await;
        tracing::warn!(
            target: "adapter",
            retry_in_secs = backoff.next_delay().as_secs(),
            "OneBot 连接已断开，准备重连"
        );
        publish_disconnected(&shared, None);
        sleep_until_shutdown(backoff.peek_latest(), &mut shutdown).await;
    }

    // 所有退出路径都以 Disconnected 作为最终状态，供 ctl/status 和监控读取。
    shared.set_state(OneBotConnectionState::Disconnected).await;
}

/// 根据 JSON 文本的 `echo` 字段对入站消息进行分类。
///
/// - 有 `echo` 字段 → Action 响应（`IncomingMessage::Response`）
/// - 无 `echo` 字段 → OneBot 事件（`IncomingMessage::Event`）
fn classify_incoming_message(text: &str) -> IncomingMessage {
    // 尝试解析 JSON 并查找 `echo` 字段。
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
        if json.get("echo").is_some() {
            // 有 echo 字段：解析为 Action 响应。
            let id = json
                .get("echo")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let action = json
                .get("action")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let status = json
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("failed")
                .to_string();
            let retcode = json
                .get("retcode")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);
            let data = json.get("data").cloned();
            let error = json
                .get("error")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let message = json
                .get("message")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
            let wording = json
                .get("wording")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
            return IncomingMessage::Response(ActionResponse {
                id,
                action,
                status,
                retcode,
                data,
                error,
                message,
                wording,
            });
        }
    }
    // 无 echo 或解析失败：视为普通事件。
    IncomingMessage::Event(text.to_string())
}

/// 处理已建立连接期间的读写循环，直到连接关闭或收到关停信号。
///
/// 使用 `tokio::select!` 同时等待：出站发送，入站读取，心跳，关停信号。
async fn handle_connected_io(
    mut transport: Box<dyn WsTransport>,
    shared: Arc<ConnectionShared>,
    outbound_rx: &mut mpsc::Receiver<String>,
    event_tx: &mpsc::Sender<String>,
    heartbeat_interval_secs: u64,
    shutdown: &mut watch::Receiver<bool>,
) {
    let mut heartbeat = tokio::time::interval(Duration::from_secs(heartbeat_interval_secs.max(1)));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut pending_events = std::collections::VecDeque::new();
    let mut peer_closed = false;

    loop {
        if peer_closed && pending_events.is_empty() {
            break;
        }
        tokio::select! {
            // 关停信号。
            _ = shutdown.changed() => {
                break;
            }

            // 出站：从通道取动作 JSON 并发送。
            out = outbound_rx.recv() => {
                match out {
                    Some(text) => {
                        if let Err(e) = transport.send_text(&text).await {
                            tracing::warn!(target: "adapter", error = %e, "出站发送失败");
                            break;
                        }
                    }
                    None => break, // 所有发送方都关闭 → 结束
                }
            }

            // 心跳：周期性发送 Ping 保持连接。
            _ = heartbeat.tick() => {
                if let Err(e) = transport.ping().await {
                    tracing::warn!(target: "adapter", error = %e, "心跳失败");
                    break;
                }
            }

            // 下游处理器繁忙时保留入站顺序，但继续读取 WebSocket，确保后续
            // action 回执仍能完成 pending 请求。缓冲有上限，避免无限占用内存。
            result = event_tx.send(pending_events.front().cloned().unwrap_or_default()),
                if !pending_events.is_empty() => {
                match result {
                    Ok(()) => { pending_events.pop_front(); }
                    Err(_) => {
                        tracing::warn!(target: "adapter", "事件处理通道已关闭");
                        break;
                    }
                }
            }

            // 入站：读取 WebSocket 帧并路由到对应处理器。
            read = transport.read_text(), if !peer_closed && pending_events.len() < MAX_PENDING_INBOUND_EVENTS => {
                match read {
                    Ok(Some(text)) => {
                        match classify_incoming_message(&text) {
                            IncomingMessage::Event(raw) => {
                                pending_events.push_back(raw);
                            }
                            IncomingMessage::Response(resp) => {
                                // 找到 pending 请求并完成它。
                                shared.complete_request(resp.id, resp.clone());
                                if shared.response_tx.send(resp).await.is_err() {
                                    tracing::warn!(target: "adapter", "响应处理通道已关闭");
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::warn!(target: "adapter", "对端关闭了连接");
                        peer_closed = true;
                    }
                    Err(e) => {
                        tracing::warn!(target: "adapter", error = %e, "读取错误，连接断开");
                        peer_closed = true;
                    }
                }
            }
        }
    }
}

/// 暂停一段时间，但在关停信号到来时提前返回。
async fn sleep_until_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) {
    tokio::select! {
        _ = tokio::time::sleep(duration) => {}
        _ = shutdown.changed() => {}
    }
}

fn publish_connected(shared: &ConnectionShared) {
    let _ = shared
        .bus
        .publish(&CoreEvent::AdapterConnected(AdapterConnectedEvent {
            adapter_name: "onebot".to_string(),
            timestamp: chrono::Utc::now(),
        }));
}

fn publish_disconnected(shared: &ConnectionShared, reason: Option<String>) {
    let _ = shared
        .bus
        .publish(&CoreEvent::AdapterDisconnected(AdapterDisconnectedEvent {
            adapter_name: "onebot".to_string(),
            reason,
            timestamp: chrono::Utc::now(),
        }));
}

/// 真实 WebSocket 传输实现。
mod tungstenite {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpStream;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

    /// 基于 `tokio-tungstenite` 的传输。
    pub struct TungsteniteTransport {
        stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    }

    impl TungsteniteTransport {
        /// 建立到 OneBot 的 WebSocket 连接。
        pub async fn connect(config: &OneBotConfig) -> Result<Self, RuntimeError> {
            let mut request = config
                .websocket_url
                .as_str()
                .into_client_request()
                .map_err(|e| RuntimeError::Adapter(format!("无效的 WebSocket 地址: {e}")))?;

            if let Some(token) = &config.access_token {
                // OneBot 常用 `Authorization: Bearer <token>` 头做鉴权。
                let value = format!("Bearer {token}");
                let header = http::header::AUTHORIZATION;
                request.headers_mut().insert(
                    header,
                    value
                        .parse()
                        .map_err(|e| RuntimeError::Adapter(format!("无效的 access_token: {e}")))?,
                );
            }

            let (stream, _) = connect_async(request)
                .await
                .map_err(|e| RuntimeError::Adapter(format!("WebSocket 连接失败: {e}")))?;

            Ok(Self { stream })
        }
    }

    #[async_trait]
    impl WsTransport for TungsteniteTransport {
        async fn read_text(&mut self) -> Result<Option<String>, RuntimeError> {
            loop {
                match self.stream.next().await {
                    Some(Ok(Message::Text(text))) => return Ok(Some(text.to_string())),
                    Some(Ok(Message::Binary(_))) => {
                        // 二进制帧：忽略。
                        continue;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        // 自动回复 Pong，保持连接。
                        if self.stream.send(Message::Pong(data)).await.is_err() {
                            return Ok(None);
                        }
                        continue;
                    }
                    Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => continue,
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                        return Ok(None);
                    }
                }
            }
        }

        async fn send_text(&mut self, text: &str) -> Result<(), RuntimeError> {
            self.stream
                .send(Message::Text(text.to_string()))
                .await
                .map_err(|e| RuntimeError::Adapter(format!("发送失败: {e}")))
        }

        async fn ping(&mut self) -> Result<(), RuntimeError> {
            self.stream
                .send(Message::Ping(Vec::new()))
                .await
                .map_err(|e| RuntimeError::Adapter(format!("心跳失败: {e}")))
        }
    }
}

pub use tungstenite::TungsteniteTransport;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn onebot_connector_ignores_runtime_model_proxy_and_connects_directly() {
        use futures_util::{SinkExt, StreamExt};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        use tokio_tungstenite::tungstenite::Message;

        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_address = proxy_listener.local_addr().unwrap();
        let runtime_config = crate::application::config::RuntimeConfig {
            proxy: Some(format!("http://{proxy_address}")),
            ..crate::application::config::RuntimeConfig::default()
        };

        let proxy_server = tokio::spawn(async move {
            let (mut stream, _) = proxy_listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0_u8; 1024];
            while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&chunk[..count]);
            }
            let request = String::from_utf8_lossy(&bytes)
                .lines()
                .next()
                .unwrap_or_default()
                .to_string();
            stream
                .write_all(
                    b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            let second_connection =
                tokio::time::timeout(Duration::from_millis(200), proxy_listener.accept())
                    .await
                    .is_ok();
            (request, second_connection)
        });

        let client = crate::infrastructure::llm::build_http_client(
            Duration::from_secs(1),
            runtime_config.proxy.as_deref(),
        )
        .unwrap();
        let proxy_response = client
            .get("http://model-target.invalid/test")
            .send()
            .await
            .unwrap();
        assert_eq!(proxy_response.status(), reqwest::StatusCode::BAD_GATEWAY);

        let websocket_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let websocket_address = websocket_listener.local_addr().unwrap();
        let websocket_server = tokio::spawn(async move {
            let (stream, _) = websocket_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            websocket
                .send(Message::Text("connected".into()))
                .await
                .unwrap();
            websocket.next().await
        });
        let connector = TungsteniteConnector;
        let mut transport = connector
            .connect(&OneBotConfig {
                websocket_url: format!("ws://{websocket_address}"),
                ..OneBotConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(
            transport.read_text().await.unwrap().as_deref(),
            Some("connected")
        );
        drop(transport);
        let _ = websocket_server.await.unwrap();
        let (proxy_request, proxy_received_onebot_connection) = proxy_server.await.unwrap();
        assert!(proxy_request.starts_with("GET http://model-target.invalid/test "));
        assert!(!proxy_received_onebot_connection);
    }

    #[test]
    fn action_response_requires_ok_status_and_zero_retcode() {
        let mut response = ActionResponse {
            id: 1,
            action: "send_group_msg".into(),
            status: "ok".into(),
            retcode: Some(0),
            data: None,
            error: None,
            message: None,
            wording: None,
        };
        assert!(response.is_ok());
        response.status = "failed".into();
        assert!(!response.is_ok());
        response.status = "ok".into();
        response.retcode = Some(1);
        assert!(!response.is_ok());
    }

    #[test]
    fn classify_response_preserves_message_and_wording() {
        let incoming = classify_incoming_message(
            r#"{"status":"failed","retcode":100,"message":"消息不存在","wording":"消息未找到","echo":"7"}"#,
        );
        let IncomingMessage::Response(response) = incoming else {
            panic!("应解析为 Action 响应");
        };
        assert_eq!(response.message.as_deref(), Some("消息不存在"));
        assert_eq!(response.wording.as_deref(), Some("消息未找到"));
        assert!(response.error_message().contains("消息未找到"));
    }

    #[test]
    fn mismatched_action_does_not_consume_pending_request() {
        let bus = crate::application::event_bus::EventBus::new();
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let (response_tx, _response_rx) = mpsc::channel(1);
        let shared = ConnectionShared {
            state: Arc::new(Mutex::new(OneBotConnectionState::Connected)),
            bus,
            outbound_tx,
            response_tx,
            pending: Arc::new(StdMutex::new(HashMap::new())),
            request_id_counter: Arc::new(AtomicU64::new(1)),
            request_timeout_secs: 10,
        };
        let (id, mut response_rx) = shared.insert_pending("send_group_msg".into());

        assert!(!shared.complete_request(
            id,
            ActionResponse {
                id,
                action: "get_msg".into(),
                status: "ok".into(),
                retcode: Some(0),
                data: None,
                error: None,
                message: None,
                wording: None,
            }
        ));
        assert_eq!(shared.pending.lock().unwrap().len(), 1);

        assert!(shared.complete_request(
            id,
            ActionResponse {
                id,
                action: "send_group_msg".into(),
                status: "ok".into(),
                retcode: Some(0),
                data: None,
                error: None,
                message: None,
                wording: None,
            }
        ));
        assert!(response_rx.try_recv().is_ok());
        assert!(shared.pending.lock().unwrap().is_empty());
    }

    // ------------------------------------------------------------------
    // 测试用：脚本化连接器（可控的假连接）
    // ------------------------------------------------------------------

    /// 一次连接的脚本：Fail 表示连接失败；Messages 表示连接成功并依次吐出消息后关闭；
    /// Hold 表示连接成功并保持打开（读取时无限阻塞），用于让后台循环安静下来。
    #[derive(Debug, Clone)]
    enum Script {
        Fail,
        Messages(Vec<String>),
        Hold,
    }

    /// 可观察连接次数的脚本化连接器。
    struct ScriptedConnector {
        /// 每次连接尝试弹出的脚本。
        script: StdMutex<VecDeque<Script>>,
        /// 连接尝试次数（含失败）。
        attempts: Arc<StdMutex<usize>>,
    }

    impl ScriptedConnector {
        fn new(script: Vec<Script>) -> (Self, Arc<StdMutex<usize>>) {
            let attempts = Arc::new(StdMutex::new(0));
            let connector = Self {
                script: StdMutex::new(script.into()),
                attempts: attempts.clone(),
            };
            (connector, attempts)
        }
    }

    #[async_trait]
    impl WsConnector for ScriptedConnector {
        async fn connect(
            &self,
            _config: &OneBotConfig,
        ) -> Result<Box<dyn WsTransport>, RuntimeError> {
            *self.attempts.lock().unwrap() += 1;
            let next = self.script.lock().unwrap().pop_front();
            match next {
                Some(Script::Fail) => Err(RuntimeError::Adapter("连接失败（脚本）".to_string())),
                Some(Script::Messages(msgs)) => Ok(Box::new(FakeTransport {
                    messages: msgs.into_iter().collect(),
                    hold_open: false,
                })),
                Some(Script::Hold) => Ok(Box::new(FakeTransport {
                    messages: VecDeque::new(),
                    hold_open: true,
                })),
                None => Err(RuntimeError::Adapter("脚本已耗尽".to_string())),
            }
        }
    }

    /// 测试用假传输：依次返回预设消息，之后返回连接关闭；`hold_open` 表示连接保持打开。
    struct FakeTransport {
        messages: VecDeque<String>,
        hold_open: bool,
    }

    #[async_trait]
    impl WsTransport for FakeTransport {
        async fn read_text(&mut self) -> Result<Option<String>, RuntimeError> {
            if let Some(m) = self.messages.pop_front() {
                return Ok(Some(m));
            }
            if self.hold_open {
                // 保持连接：无限阻塞等待（永远无人写入）。
                std::future::pending::<()>().await;
                unreachable!()
            }
            Ok(None) // 连接关闭
        }

        async fn send_text(&mut self, _text: &str) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn ping(&mut self) -> Result<(), RuntimeError> {
            Ok(())
        }
    }

    fn test_config() -> OneBotConfig {
        OneBotConfig {
            websocket_url: "ws://127.0.0.1:1".to_string(),
            access_token: None,
            bot_self_id: None,
            reconnect_interval_secs: 1,
            max_reconnect_interval_secs: 4,
            heartbeat_interval_secs: 30,
            action_timeout_secs: 10,
        }
    }

    // ------------------------------------------------------------------
    // Backoff 纯逻辑测试
    // ------------------------------------------------------------------

    #[test]
    fn backoff_grows_exponentially_and_caps() {
        let mut b = Backoff::new(Duration::from_secs(1), Duration::from_secs(4));
        // 1s, 2s, 4s（到达上限后保持 4s）
        assert_eq!(b.next_delay(), Duration::from_secs(1));
        assert_eq!(b.next_delay(), Duration::from_secs(2));
        assert_eq!(b.next_delay(), Duration::from_secs(4));
        assert_eq!(b.next_delay(), Duration::from_secs(4));
        assert_eq!(b.attempts(), 4);
    }

    #[test]
    fn backoff_resets_after_success() {
        let mut b = Backoff::new(Duration::from_secs(1), Duration::from_secs(8));
        b.next_delay();
        b.next_delay();
        assert_eq!(b.attempts(), 2);
        b.reset();
        assert_eq!(b.attempts(), 0);
        // 重置后重新从基准开始。
        assert_eq!(b.next_delay(), Duration::from_secs(1));
    }

    #[test]
    fn backoff_peek_does_not_advance() {
        let b = Backoff::new(Duration::from_secs(1), Duration::from_secs(8));
        assert_eq!(b.peek_latest(), Duration::from_secs(1));
        assert_eq!(b.peek_latest(), Duration::from_secs(1)); // 不推进
        assert_eq!(b.attempts(), 0);
    }

    // ------------------------------------------------------------------
    // 断线重连循环测试（使用可控假连接）
    // ------------------------------------------------------------------

    /// 断线重连循环测试：首次连接失败 → 指数退避后重连成功 →
    /// 断开后再重连成功（依据事件总线上的 AdapterConnected 次数判断）。
    #[tokio::test]
    async fn reconnects_after_initial_failure_then_disconnect() {
        let bus = crate::application::event_bus::EventBus::new();
        let mut observer = bus.subscribe();

        // 脚本：第 1 次连接失败，第 2 次成功并关闭，第 3 次保持打开。
        let script = vec![
            Script::Fail,
            Script::Messages(vec!["首次".to_string()]),
            Script::Hold,
        ];
        let (connector, attempts) = ScriptedConnector::new(script);

        let (outbound_tx_unused, outbound_rx) = mpsc::channel(1);
        let (event_tx, _event_rx) = mpsc::channel(8);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (response_tx, _response_rx) = mpsc::channel(8);

        let shared = ConnectionShared {
            state: Arc::new(Mutex::new(OneBotConnectionState::Disconnected)),
            bus: bus.clone(),
            outbound_tx: outbound_tx_unused,
            response_tx,
            pending: Arc::new(StdMutex::new(HashMap::new())),
            request_id_counter: Arc::new(AtomicU64::new(0)),
            request_timeout_secs: 30,
        };

        tokio::spawn(run_connection_loop(
            Arc::new(connector),
            shared.clone(),
            outbound_rx,
            event_tx,
            test_config(),
            shutdown_rx,
        ));

        // 实时轮询事件总线，统计 AdapterConnected 次数；设 30 秒兜底超时。
        let mut connected_count = 0u32;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while connected_count < 2 && std::time::Instant::now() < deadline {
            while let Some(ev) = observer.try_recv() {
                if matches!(ev, CoreEvent::AdapterConnected(_)) {
                    connected_count += 1;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(
            connected_count >= 2,
            "应至少完成两次重连, 实际: {connected_count}"
        );
        assert!(
            *attempts.lock().unwrap() >= 3,
            "连接尝试次数应不少于 3, 实际: {}",
            *attempts.lock().unwrap()
        );

        shutdown_tx.send(true).unwrap();
    }

    /// 连接成功后收到一条消息，再断开并重连（同时验证入站消息被转发到事件通道）。
    #[tokio::test]
    async fn inbound_message_forwarded_and_reconnect_occurs() {
        let bus = crate::application::event_bus::EventBus::new();
        let _observer = bus.subscribe();

        // 脚本：第 1 次连接成功并吐出一条消息后关闭；第 2 次保持打开。
        let script = vec![
            Script::Messages(vec!["hello-onebot".to_string()]),
            Script::Hold,
        ];
        let (connector, attempts) = ScriptedConnector::new(script);

        // 保持出站发送端存活：若此处丢弃发送端，`outbound_rx.recv()` 会立即返回 None，
        // 导致 `handle_connected_io` 在读取入站消息之前就退出连接循环（并发竞态的根源）。
        let (outbound_tx, outbound_rx) = mpsc::channel(1);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (response_tx, _response_rx) = mpsc::channel(8);

        let shared = ConnectionShared {
            state: Arc::new(Mutex::new(OneBotConnectionState::Disconnected)),
            bus: bus.clone(),
            outbound_tx,
            response_tx,
            pending: Arc::new(StdMutex::new(HashMap::new())),
            request_id_counter: Arc::new(AtomicU64::new(0)),
            request_timeout_secs: 30,
        };

        tokio::spawn(run_connection_loop(
            Arc::new(connector),
            shared.clone(),
            outbound_rx,
            event_tx,
            test_config(),
            shutdown_rx,
        ));

        // 等待入站消息被转发（轮询 + 兜底 deadline，避免并发调度下的偶发超时）。
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut forwarded: Option<String> = None;
        while std::time::Instant::now() < deadline {
            if let Ok(msg) = event_rx.try_recv() {
                forwarded = Some(msg);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let msg = forwarded.expect("应收到转发消息");
        assert_eq!(msg, "hello-onebot");

        // 第二条连接成功（重连）应发生。
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while *attempts.lock().unwrap() < 2 && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            *attempts.lock().unwrap() >= 2,
            "应发生至少一次重连, 实际: {}",
            *attempts.lock().unwrap()
        );

        shutdown_tx.send(true).unwrap();
    }

    #[tokio::test]
    async fn action_response_is_completed_while_inbound_queue_is_full() {
        let bus = crate::application::event_bus::EventBus::new();
        let _observer = bus.subscribe();
        let script = vec![
            Script::Messages(vec![
                r#"{"post_type":"message","message":"first"}"#.into(),
                r#"{"post_type":"message","message":"second"}"#.into(),
                r#"{"echo":"0","action":"send_private_msg","status":"ok","retcode":0}"#.into(),
            ]),
            Script::Hold,
        ];
        let (connector, _) = ScriptedConnector::new(script);
        let (outbound_tx, outbound_rx) = mpsc::channel(1);
        let (event_tx, mut event_rx) = mpsc::channel(1);
        event_tx.send("already-queued".into()).await.unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (response_tx, _response_rx) = mpsc::channel(8);
        let shared = ConnectionShared {
            state: Arc::new(Mutex::new(OneBotConnectionState::Disconnected)),
            bus: bus.clone(),
            outbound_tx,
            response_tx,
            pending: Arc::new(StdMutex::new(HashMap::new())),
            request_id_counter: Arc::new(AtomicU64::new(0)),
            request_timeout_secs: 10,
        };
        let (request_id, mut response_rx) = shared.insert_pending("send_private_msg".into());
        assert_eq!(request_id, 0);

        let connection = tokio::spawn(run_connection_loop(
            Arc::new(connector),
            shared.clone(),
            outbound_rx,
            event_tx,
            test_config(),
            shutdown_rx,
        ));

        let response = tokio::time::timeout(Duration::from_secs(1), &mut response_rx)
            .await
            .expect("入站队列积压不应阻塞 action 回执")
            .expect("pending 请求应收到回执");
        assert!(response.is_ok());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .expect("队列中原有事件应可读取")
                .as_deref(),
            Some("already-queued")
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .expect("积压的第一条消息应继续转发")
                .as_deref(),
            Some(r#"{"post_type":"message","message":"first"}"#)
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .expect("积压的第二条消息应继续转发")
                .as_deref(),
            Some(r#"{"post_type":"message","message":"second"}"#)
        );

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .expect("连接循环应响应关停")
            .unwrap();
    }
}
