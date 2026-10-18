//! 插件传输层 —— Unix 域套接字（UDS）服务端与连接管理。
//!
//! Core 是 socket 监听方，插件是连接方；每个插件拥有一个独立 socket
//! （`<sockets_dir>/<name>.sock`）。连接身份由 socket 文件名固化 ——
//! `UdsServer` 在 bind 时拿到 `expected_name`，握手时 `Hello.name` 必须等于
//! 该名字，否则拒绝（防伪冒）。
//!
//! 帧协议见 `protocol.rs`（长度前缀 + MessagePack，`MAX_FRAME_LEN` 上限）。
//! 所有协议错误就地 `tracing::warn!` 并关闭该连接，绝不向 Core 上层传播或 panic。

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};

use crate::application::plugin_api::PluginApi;
use crate::error::RuntimeError;
use crate::infrastructure::plugin::protocol::{
    decode_full_read, encode_frame, EventType, WireMessage, MAX_FRAME_LEN,
};
use crate::infrastructure::plugin::registry::PluginRegistry;
use crate::infrastructure::plugin::PluginTransport;

/// 默认握手超时（等待插件首帧 Hello）。
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// 写通道容量（事件通知排队在此，满则丢弃并发 warn）。
const WRITE_CHANNEL_CAPACITY: usize = 64;
/// 每次读 socket 的临时缓冲大小。单帧可在多次读取中累计（扛半包）。
const READ_CHUNK_SIZE: usize = 4096;
/// macOS `sun_path` 通常约 104 字节（含 NUL），这里对绝对路径字节长度做防护。
const MAX_SOCKET_PATH_BYTES: usize = 103;

/// UDS 服务端 —— 为单个插件（单个 socket）提供 accept/read/write 循环。
pub struct UdsServer {
    listener: UnixListener,
    expected_name: String,
    registry: Arc<PluginRegistry>,
    api: Arc<PluginApi>,
    handshake_timeout: Duration,
    /// 连接关闭时通知的发送端（supervisor 监控用）；`None` 表示不通知。
    /// 用 `Arc` + `Mutex` 包裹以便每个连接 handler 共享同一接收端；
    /// 发送端被 `take` 走后即触发一次（oneshot 语义，防重复通知）。
    conn_closed: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
    /// 握手成功（attach 完成）时通知的发送端；supervisor 用它取消启动超时。
    /// 语义与 `conn_closed` 相同：仅触发一次。
    attached: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
}

impl UdsServer {
    /// 在 `socket_path` 上创建监听服务端。
    ///
    /// bind 前先尝试清除陈旧 socket 文件（失败忽略）；路径字节过长给出明确中文错误。
    pub async fn bind(
        socket_path: &Path,
        expected_name: String,
        registry: Arc<PluginRegistry>,
        api: Arc<PluginApi>,
    ) -> Result<Self, RuntimeError> {
        // 路径过长防护（macOS sun_path 约 104 字节）。
        let path_bytes = socket_path.as_os_str().as_encoded_bytes().len();
        if path_bytes > MAX_SOCKET_PATH_BYTES {
            return Err(RuntimeError::Plugin(format!(
                "socket 路径过长（{} 字节，需 <= {}）：{}",
                path_bytes,
                MAX_SOCKET_PATH_BYTES,
                socket_path.display()
            )));
        }
        use std::os::unix::fs::{FileTypeExt, PermissionsExt};
        if let Ok(metadata) = std::fs::symlink_metadata(socket_path) {
            if !metadata.file_type().is_socket() {
                return Err(RuntimeError::Plugin(
                    "插件 socket 路径已有非 socket 文件".to_string(),
                ));
            }
            match UnixStream::connect(socket_path).await {
                Ok(_) => {
                    return Err(RuntimeError::Plugin(
                        "插件 socket 已被运行中的服务占用".to_string(),
                    ))
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                    ) =>
                {
                    std::fs::remove_file(socket_path).map_err(|error| {
                        RuntimeError::Plugin(format!("清理插件 socket 失败：{error}"))
                    })?;
                }
                Err(error) => {
                    return Err(RuntimeError::Plugin(format!(
                        "检查插件 socket 失败：{error}"
                    )))
                }
            }
        }
        let listener = UnixListener::bind(socket_path).map_err(|e| {
            RuntimeError::Plugin(format!("无法在 {} 绑定 socket：{e}", socket_path.display()))
        })?;
        std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))
            .map_err(|error| RuntimeError::Plugin(format!("设置插件 socket 权限失败：{error}")))?;
        Ok(Self {
            listener,
            expected_name,
            registry,
            api,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            conn_closed: Arc::new(std::sync::Mutex::new(None)),
            attached: Arc::new(std::sync::Mutex::new(None)),
        })
    }

    /// 设置握手超时（builder 风格）。
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// 注册"连接关闭"通知（builder 风格，supervisor 用）。
    pub fn on_close(mut self, tx: oneshot::Sender<()>) -> Self {
        self.conn_closed = Arc::new(std::sync::Mutex::new(Some(tx)));
        self
    }

    /// 注册"握手成功"通知（builder 风格，supervisor 用它取消启动超时）。
    pub fn on_attached(mut self, tx: oneshot::Sender<()>) -> Self {
        self.attached = Arc::new(std::sync::Mutex::new(Some(tx)));
        self
    }

    /// 进入 accept 循环。每来一个连接就 spawn 一个 handler 任务。
    ///
    /// 该循环随 server 挂起；由 supervisor 以 task 运行。返回时所有连接已处理完毕。
    pub async fn run(self) {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (stream, _addr) = match tokio::select! {
                result = self.listener.accept() => result,
                _ = connections.join_next(), if !connections.is_empty() => continue,
            } {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!("accept 失败（插件 {}）：{e}", self.expected_name);
                    continue;
                }
            };
            let registry = self.registry.clone();
            let api = self.api.clone();
            let expected_name = self.expected_name.clone();
            let handshake_timeout = self.handshake_timeout;
            let conn_closed = self.conn_closed.clone();
            let attached = self.attached.clone();
            connections.spawn(async move {
                handle_conn(
                    stream,
                    expected_name,
                    registry,
                    api,
                    handshake_timeout,
                    conn_closed,
                    attached,
                )
                .await;
            });
        }
    }
}

/// 握手成功后持有连接所有权；失败的握手没有清理其他连接的权限。
struct ConnCleanupGuard {
    registry: Arc<PluginRegistry>,
    name: String,
    conn_closed: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
    owner: mpsc::Sender<WireMessage>,
}

impl Drop for ConnCleanupGuard {
    fn drop(&mut self) {
        if self.registry.clear_conn_owned(&self.name, &self.owner) {
            if let Ok(mut guard) = self.conn_closed.lock() {
                if let Some(sender) = guard.take() {
                    let _ = sender.send(());
                }
            }
        }
    }
}

/// 写任务随连接作用域退出取消，包括 panic 和服务端被停止。
struct WriterGuard(tokio::task::JoinHandle<()>);
impl Drop for WriterGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn send_handshake(
    write_half: &mut WriteHalf<UnixStream>,
    reason: Option<String>,
) -> Result<(), RuntimeError> {
    let frame = encode_frame(&WireMessage::HelloAck {
        ok: reason.is_none(),
        reason,
    })
    .map_err(|error| RuntimeError::Plugin(error.to_string()))?;
    tokio::time::timeout(Duration::from_secs(5), write_half.write_all(&frame))
        .await
        .map_err(|_| RuntimeError::Plugin("握手应答超时".to_string()))?
        .map_err(|e| RuntimeError::Plugin(format!("握手应答失败：{e}")))
}

/// 处理单条连接：先验证握手，再挂接连接与启动写任务。
async fn handle_conn(
    stream: UnixStream,
    expected_name: String,
    registry: Arc<PluginRegistry>,
    api: Arc<PluginApi>,
    handshake_timeout: Duration,
    conn_closed: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
    attached: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
) {
    let (mut read_half, mut write_half) = split(stream);
    let mut buffer = Vec::new();
    let hello = match tokio::time::timeout(
        handshake_timeout,
        read_frame(&mut read_half, &mut buffer),
    )
    .await
    {
        Ok(Ok(Some(WireMessage::Hello(hello)))) => hello,
        other => {
            tracing::warn!(plugin = %expected_name, result = ?other, "插件握手失败");
            let reason = match other {
                Ok(Ok(Some(_))) => "首帧必须是 hello".to_string(),
                Ok(Ok(None)) => "握手前连接关闭".to_string(),
                Ok(Err(error)) if error.contains("protocol_version") => error,
                Ok(Err(_)) => "首帧不是合法协议消息".to_string(),
                Err(_) => "握手超时".to_string(),
            };
            let _ = send_handshake(&mut write_half, Some(reason)).await;
            return;
        }
    };
    if hello.name != expected_name {
        let _ = send_handshake(&mut write_half, Some("插件名不匹配".to_string())).await;
        return;
    }
    let granted = registry.permissions_for(&expected_name).unwrap_or_default();
    let mut subscriptions = Vec::new();
    for name in &hello.subscribe {
        let event = match EventType::parse(name) {
            Ok(event) => event,
            Err(_) => {
                let _ = send_handshake(&mut write_half, Some(format!("订阅了未知事件名：{name}")))
                    .await;
                return;
            }
        };
        if crate::infrastructure::plugin::permissions::permission_for_event(event)
            .is_some_and(|required| !granted.contains(&required))
        {
            let _ =
                send_handshake(&mut write_half, Some(format!("权限不足：不能订阅 {name}"))).await;
            return;
        }
        if !subscriptions.contains(&event) {
            subscriptions.push(event);
        }
    }
    let (tx, rx) = mpsc::channel(WRITE_CHANNEL_CAPACITY);
    if let Err(error) = registry.attach_conn(&expected_name, tx.clone()) {
        let _ = send_handshake(&mut write_half, Some(error.to_string())).await;
        return;
    }
    let _cleanup_guard = ConnCleanupGuard {
        registry: registry.clone(),
        name: expected_name.clone(),
        conn_closed,
        owner: tx.clone(),
    };
    registry.set_subscriptions(&expected_name, subscriptions);
    if send_handshake(&mut write_half, None).await.is_err() {
        return;
    }
    if let Ok(mut guard) = attached.lock() {
        if let Some(sender) = guard.take() {
            let _ = sender.send(());
        }
    }
    let mut writer = WriterGuard(tokio::spawn(write_loop(write_half, rx)));
    loop {
        let frame = tokio::select! {
            frame = read_frame(&mut read_half, &mut buffer) => frame,
            _ = &mut writer.0 => return,
        };
        match frame {
            Ok(Some(WireMessage::Request { id, method, params })) => {
                let result = tokio::time::timeout(
                    Duration::from_secs(120),
                    api.dispatch_typed(&expected_name, &method, params),
                )
                .await;
                let response = match result {
                    Ok(Ok(value)) => WireMessage::Response {
                        id,
                        ok: true,
                        result: Some(value),
                        error: None,
                    },
                    Ok(Err(error)) => WireMessage::Response {
                        id,
                        ok: false,
                        result: None,
                        error: Some(error.wire_message()),
                    },
                    Err(_) => WireMessage::Response {
                        id,
                        ok: false,
                        result: None,
                        error: Some(crate::infrastructure::plugin::protocol::mark_error(
                            "INTERNAL_ERROR",
                            "插件请求处理超时",
                        )),
                    },
                };
                if !matches!(
                    tokio::time::timeout(Duration::from_secs(5), tx.send(response)).await,
                    Ok(Ok(()))
                ) {
                    return;
                }
            }
            Ok(None) => return,
            other => {
                tracing::warn!(plugin = %expected_name, result = ?other, "插件连接协议错误，关闭连接");
                return;
            }
        }
    }
}

/// 负责写 socket 的任务：消费通道消息并编码写回。
async fn write_loop(mut write_half: WriteHalf<UnixStream>, mut rx: mpsc::Receiver<WireMessage>) {
    while let Some(msg) = rx.recv().await {
        let frame = match encode_frame(&msg) {
            Ok(frame) => frame,
            Err(e) => {
                tracing::warn!("帧编码失败（{e}），关闭写通道");
                break;
            }
        };
        if write_half.write_all(&frame).await.is_err() {
            tracing::warn!("写 socket 失败，关闭写通道");
            break;
        }
    }
    let _ = write_half.shutdown().await;
}

/// 等待并取出一完整帧。
///
/// 内部维护累计缓冲，处理半包/粘包；帧体超长或无法完成时返回错误，
/// 交由调用方按协议错误关闭连接。
async fn read_frame(
    read_half: &mut ReadHalf<UnixStream>,
    buffer: &mut Vec<u8>,
) -> Result<Option<WireMessage>, String> {
    loop {
        // 若缓冲里已能取出一整帧，则直接返回（粘包：剩余保留在缓冲）。
        if let Some((consumed, msg)) = decode_full_read(buffer) {
            let remaining = buffer.split_off(consumed);
            *buffer = remaining;
            return Ok(Some(msg));
        }

        // 长度前缀越界或完整帧解码失败立即拒绝，不能等待攻击者填满缓冲。
        if buffer.len() >= 4 {
            let length =
                u32::from_be_bytes(buffer[..4].try_into().map_err(|_| "帧长度前缀错误")?) as usize;
            if length > MAX_FRAME_LEN as usize {
                return Err("帧长度前缀超过上限".to_string());
            }
            if buffer.len() >= length + 4 {
                return Err("完整帧无法解码".to_string());
            }
        }
        // 缓冲里仍取不出完整帧：若已超上限且无法继续，判协议错误。
        if buffer.len() > MAX_FRAME_LEN as usize {
            return Err("帧体超过长度上限且无法完成".to_string());
        }

        let mut chunk = [0u8; READ_CHUNK_SIZE];
        let n = match read_half.read(&mut chunk).await {
            Ok(0) if buffer.is_empty() => return Ok(None),
            Ok(0) => return Err("连接在完整帧到达前关闭".to_string()),
            Ok(n) => n,
            Err(e) => return Err(format!("读取 socket 失败：{e}")),
        };
        buffer.extend_from_slice(&chunk[..n]);
    }
}

/// 连接对象 —— 封装写通道，实现 [`PluginTransport`]。
///
/// UDS 连接已在握手时绑定到具体插件，因此 trait 方法的 `plugin_name`
/// 参数被忽略（传任意值均可）。
pub struct UdsConnection {
    tx: mpsc::Sender<WireMessage>,
}

impl UdsConnection {
    /// 从注册表取得的连接句柄构造一个传输对象（供桥接层或测试复用）。
    pub fn from_sender(tx: mpsc::Sender<WireMessage>) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl PluginTransport for UdsConnection {
    async fn request(
        &self,
        _plugin_name: &str,
        _method: &str,
        _params: serde_json::Value,
    ) -> Result<serde_json::Value, RuntimeError> {
        // MVP 不提供 Core → Plugin 的 RPC。
        Err(RuntimeError::Plugin(
            "核心暂不支持向插件发起请求".to_string(),
        ))
    }

    async fn notify(
        &self,
        _plugin_name: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<(), RuntimeError> {
        self.tx
            .try_send(WireMessage::Notify {
                event: method.to_string(),
                data: params,
            })
            .map_err(|_| RuntimeError::Plugin("插件通知通道已满".to_string()))
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    //! 测试共享桩：PluginApi 构造所需的最小仓库/适配器。
    //! 供 transport 与 supervisor 测试复用。

    use super::*;
    use crate::adapters::onebot::{OneBotAdapter, OneBotConnectionState};
    use crate::application::context::ContextBuilder;
    use crate::application::context_assembler::{
        ContextAssembler, ModelCapabilitiesProviderAdapter, TokenEstimatorAdapter,
    };
    use crate::application::plugin_api::PluginApi;
    use crate::domain::character::{Character, CharacterState};
    use crate::domain::conversation::Conversation;
    use crate::domain::memory::Memory;
    use crate::domain::relationship::Relationship;
    use crate::domain::repository::{
        CharacterBindingRepository, CharacterRepository, CharacterStateRepository,
        ConversationRepository, MemoryRepository, MessageRepository, MoodRepository,
        PluginDataRepository, RelationshipRepository,
    };
    use crate::error::RepositoryError;
    use crate::infrastructure::llm::{ApproximateTokenEstimator, FixedModelCapabilities};
    use crate::infrastructure::plugin::registry::PluginRegistry;
    use crate::infrastructure::plugin::{PluginManifest, PluginPermission};
    use crate::infrastructure::storage::connection::SqliteStorage;

    macro_rules! empty_repo {
        ($name:ident) => {
            #[derive(Default)]
            pub(crate) struct $name;
        };
    }
    empty_repo!(EmptyCharacterRepo);
    empty_repo!(EmptyStateRepo);
    empty_repo!(EmptyBindingRepo);
    empty_repo!(EmptyConvRepo);
    empty_repo!(EmptyMessageRepo);
    empty_repo!(EmptyMemoryRepo);
    empty_repo!(EmptyRelationshipRepo);
    empty_repo!(EmptyMoodRepo);

    #[async_trait]
    impl CharacterRepository for EmptyCharacterRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Character>, RepositoryError> {
            Ok(None)
        }
        async fn find_all(&self) -> Result<Vec<Character>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert(&self, _c: &Character) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _c: &Character) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    #[async_trait]
    impl CharacterStateRepository for EmptyStateRepo {
        async fn find_by_character_id(
            &self,
            _character_id: i64,
        ) -> Result<Option<CharacterState>, RepositoryError> {
            Ok(None)
        }
        async fn upsert(&self, _id: i64, _s: &CharacterState) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    #[async_trait]
    impl CharacterBindingRepository for EmptyBindingRepo {
        async fn find_by_character_id(
            &self,
            _id: i64,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_by_conversation_id(
            &self,
            _id: i64,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(vec![])
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

    #[async_trait]
    impl ConversationRepository for EmptyConvRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Conversation>, RepositoryError> {
            Ok(None)
        }
        async fn find_by_external_id(
            &self,
            _id: &str,
        ) -> Result<Option<Conversation>, RepositoryError> {
            Ok(None)
        }
        async fn find_all(&self) -> Result<Vec<Conversation>, RepositoryError> {
            Ok(vec![])
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
            Ok(1) // Mock 实现
        }
    }

    #[async_trait]
    impl MessageRepository for EmptyMessageRepo {
        async fn find_by_id(
            &self,
            _id: i64,
        ) -> Result<Option<crate::domain::message::Message>, RepositoryError> {
            Ok(None)
        }
        async fn find_recent(
            &self,
            _conversation_id: i64,
            _limit: i64,
        ) -> Result<Vec<crate::domain::message::Message>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_recent_attributed(
            &self,
            _conversation_id: i64,
            _character_id: i64,
            _limit: i64,
        ) -> Result<Vec<crate::domain::message::Message>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert(
            &self,
            _m: &crate::domain::message::Message,
        ) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn latest_message_time(
            &self,
            _conversation_id: i64,
        ) -> Result<Option<chrono::DateTime<chrono::Utc>>, RepositoryError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl MemoryRepository for EmptyMemoryRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Memory>, RepositoryError> {
            Ok(None)
        }
        async fn find_by_character_id(
            &self,
            _id: i64,
            _t: Option<crate::domain::memory::MemoryType>,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert(&self, _m: &Memory) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _m: &Memory) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn find_all(&self, _limit: i64) -> Result<Vec<Memory>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_by_conversation_id(
            &self,
            _conversation_id: i64,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_filtered(
            &self,
            _character_id: Option<i64>,
            _conversation_id: Option<i64>,
            _memory_type: Option<crate::domain::memory::MemoryType>,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl RelationshipRepository for EmptyRelationshipRepo {
        async fn find(
            &self,
            _cid: i64,
            _pid: i64,
        ) -> Result<Option<Relationship>, RepositoryError> {
            Ok(None)
        }
        async fn find_by_character_id(
            &self,
            _cid: i64,
        ) -> Result<Vec<Relationship>, RepositoryError> {
            Ok(vec![])
        }
        async fn upsert(&self, _r: &Relationship) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    #[async_trait]
    impl MoodRepository for EmptyMoodRepo {
        async fn find_by_character_and_conversation(
            &self,
            _character_id: i64,
            _conversation_id: i64,
        ) -> Result<Option<crate::domain::emotion::Mood>, RepositoryError> {
            Ok(None)
        }
        async fn upsert(
            &self,
            _character_id: i64,
            _conversation_id: i64,
            _s: &crate::domain::emotion::Mood,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct EmptyAdapter;
    #[async_trait]
    impl OneBotAdapter for EmptyAdapter {
        async fn start(&self) -> Result<(), RuntimeError> {
            Ok(())
        }
        async fn stop(&self) -> Result<(), RuntimeError> {
            Ok(())
        }
        async fn state(&self) -> OneBotConnectionState {
            OneBotConnectionState::Connected
        }
        async fn send_group_message(
            &self,
            _group_id: &str,
            _content: &str,
        ) -> Result<crate::adapters::onebot::ActionResponse, RuntimeError> {
            Ok(crate::adapters::onebot::ActionResponse {
                id: 0,
                action: "send_group_msg".to_string(),
                status: "ok".to_string(),
                retcode: Some(0),
                data: None,
                error: None,
                message: None,
                wording: None,
            })
        }
        async fn send_private_message(
            &self,
            _user_id: &str,
            _content: &str,
        ) -> Result<crate::adapters::onebot::ActionResponse, RuntimeError> {
            Ok(crate::adapters::onebot::ActionResponse {
                id: 0,
                action: "send_private_msg".to_string(),
                status: "ok".to_string(),
                retcode: Some(0),
                data: None,
                error: None,
                message: None,
                wording: None,
            })
        }
    }

    pub(crate) fn manifest(name: &str, permissions: Vec<PluginPermission>) -> PluginManifest {
        PluginManifest {
            name: name.to_string(),
            version: "0.1.0".to_string(),
            description: "示例插件".to_string(),
            permissions,
            executable: "plugin-bin".to_string(),
            config: serde_json::json!({}),
        }
    }

    /// 构造一个具备运行依赖的最小 PluginApi（plugin_data 用 SQLite in-memory）。
    pub(crate) async fn build_api(registry: Arc<PluginRegistry>) -> PluginApi {
        let storage = SqliteStorage::open_in_memory().await.unwrap();
        storage.migrate().await.unwrap();
        let plugin_data: Arc<dyn PluginDataRepository> = Arc::new(
            crate::infrastructure::storage::repository::SqlitePluginDataRepository::new(
                storage.pool().clone(),
            ),
        );

        let context_builder = Arc::new(ContextBuilder::new(
            Arc::new(EmptyMessageRepo) as Arc<dyn MessageRepository>,
            Arc::new(
                crate::application::character_history::RepositoryCharacterHistoryProvider::new(
                    Arc::new(EmptyMessageRepo) as Arc<dyn MessageRepository>,
                ),
            ),
            Arc::new(EmptyConvRepo) as Arc<dyn ConversationRepository>,
            Arc::new(EmptyMemoryRepo) as Arc<dyn MemoryRepository>,
            Arc::new(EmptyRelationshipRepo) as Arc<dyn RelationshipRepository>,
            Arc::new(EmptyMoodRepo) as Arc<dyn MoodRepository>,
            Arc::new(EmptyBindingRepo) as Arc<dyn CharacterBindingRepository>,
        ));

        // Phase 3: ContextAssembler for Token Budget + Layered Degradation
        let token_estimator = Arc::new(TokenEstimatorAdapter(Arc::new(ApproximateTokenEstimator)));
        let model_capabilities = Arc::new(ModelCapabilitiesProviderAdapter(Arc::new(
            FixedModelCapabilities::new(None, None),
        )));
        let context_assembler =
            Arc::new(ContextAssembler::new(token_estimator, model_capabilities));

        let cognition = Arc::new(crate::application::cognition::CognitionLayer::new(
            None,
            context_builder,
            context_assembler,
        ));
        let dispatcher = Arc::new(crate::application::action::ActionDispatcher::new(
            Arc::new(EmptyConvRepo) as Arc<dyn ConversationRepository>,
            Arc::new(EmptyAdapter) as Arc<dyn OneBotAdapter>,
        ));

        PluginApi::new(
            Arc::new(EmptyCharacterRepo) as Arc<dyn CharacterRepository>,
            Arc::new(EmptyStateRepo) as Arc<dyn CharacterStateRepository>,
            Arc::new(EmptyBindingRepo) as Arc<dyn CharacterBindingRepository>,
            Arc::new(EmptyMessageRepo) as Arc<dyn MessageRepository>,
            Arc::new(EmptyMemoryRepo) as Arc<dyn MemoryRepository>,
            Arc::new(EmptyRelationshipRepo) as Arc<dyn RelationshipRepository>,
            plugin_data,
            dispatcher,
            cognition,
            registry,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::plugin::protocol::Hello;
    use crate::infrastructure::plugin::{PluginPermission, PluginState};
    use test_support::{build_api, manifest};

    // -----------------------------------------------------------------------
    // client helper —— 连接 + 编解码往返
    // -----------------------------------------------------------------------

    /// 读取客户端的一个完整应答帧。
    async fn read_one_client_frame(
        stream: &mut UnixStream,
        buffer: &mut Vec<u8>,
    ) -> Result<Option<WireMessage>, String> {
        loop {
            if let Some((consumed, msg)) = decode_full_read(buffer) {
                let remaining = buffer.split_off(consumed);
                *buffer = remaining;
                return Ok(Some(msg));
            }
            if buffer.len() > MAX_FRAME_LEN as usize {
                return Err("应答帧超长".to_string());
            }
            let mut chunk = [0u8; 4096];
            let n = match stream.read(&mut chunk).await {
                Ok(0) => return Ok(None),
                Ok(n) => n,
                Err(e) => return Err(format!("读取 socket 失败：{e}")),
            };
            buffer.extend_from_slice(&chunk[..n]);
        }
    }

    /// 启动一个 server（独立的临时目录与 socket 路径）。
    struct ServerHarness {
        _dir: tempfile::TempDir,
        socket_path: std::path::PathBuf,
        _task: tokio::task::JoinHandle<()>,
    }

    async fn start_server(
        name: &str,
        registry: Arc<PluginRegistry>,
        api: Arc<PluginApi>,
        handshake_timeout: Duration,
    ) -> ServerHarness {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join(format!("{name}.sock"));
        let server = UdsServer::bind(&socket_path, name.to_string(), registry, api)
            .await
            .unwrap()
            .with_handshake_timeout(handshake_timeout);
        let task = tokio::spawn(server.run());
        ServerHarness {
            _dir: dir,
            socket_path,
            _task: task,
        }
    }

    /// 在给定 socket 上完成一次握手并返回流。
    async fn connect_and_hello(socket: &Path, hello: &Hello) -> (UnixStream, WireMessage) {
        let mut stream = UnixStream::connect(socket).await.unwrap();
        stream
            .write_all(&encode_frame(&WireMessage::Hello(hello.clone())).unwrap())
            .await
            .unwrap();
        let mut buffer = Vec::new();
        let ack = read_one_client_frame(&mut stream, &mut buffer)
            .await
            .unwrap()
            .expect("应收到 HelloAck");
        (stream, ack)
    }

    /// 预置一个已注册且处于 Starting 状态的插件。
    fn plugin_registry(plugins: &[(&str, Vec<PluginPermission>)]) -> Arc<PluginRegistry> {
        let registry = Arc::new(PluginRegistry::new());
        for (name, perms) in plugins {
            // 传输层测试使用空权限时，只验证握手和帧传输，不授予业务能力。
            // message.read，避免与现在的订阅权限门禁混淆；专门的权限
            // 测试传入非空权限集合，仍会严格验证拒绝路径。
            let permissions = if perms.is_empty() {
                vec![PluginPermission::MessageRead]
            } else {
                perms.to_vec()
            };
            registry.register(manifest(name, permissions)).unwrap();
            registry.set_state(name, PluginState::Starting);
        }
        registry
    }

    /// 预置一个已注册但保持在 Discovered 状态（未置 Starting）的插件。
    fn plugin_registry_discovered(plugins: &[&str]) -> Arc<PluginRegistry> {
        let registry = Arc::new(PluginRegistry::new());
        for name in plugins {
            registry.register(manifest(name, vec![])).unwrap();
        }
        registry
    }

    fn hello(name: &str, subscribe: &[&str]) -> Hello {
        Hello {
            name: name.to_string(),
            version: "0.1.0".to_string(),
            subscribe: subscribe.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn assert_ack_ok(ack: WireMessage) {
        match ack {
            WireMessage::HelloAck { ok, reason } => {
                assert!(ok, "HelloAck 应成功，reason={:?}", reason)
            }
            other => panic!("期望 HelloAck，实际 {other:?}"),
        }
    }

    fn assert_ack_fail(ack: WireMessage, reason_substr: &str) {
        match ack {
            WireMessage::HelloAck { ok, reason } => {
                assert!(!ok, "HelloAck 应失败");
                let reason = reason.expect("失败时应带 reason");
                assert!(
                    reason.contains(reason_substr),
                    "reason 应含 {reason_substr:?}，实际 {reason}"
                );
            }
            other => panic!("期望 HelloAck，实际 {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // 1. Hello → HelloAck{ok:true} roundtrip
    // -----------------------------------------------------------------------

    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn hello_ack_ok_roundtrip() {
        let registry = plugin_registry(&[("echo", vec![PluginPermission::MessageRead])]);
        let api = build_api(registry.clone()).await;
        let h = start_server(
            "echo",
            registry.clone(),
            Arc::new(api),
            Duration::from_secs(5),
        )
        .await;

        let (_stream, ack) =
            connect_and_hello(&h.socket_path, &hello("echo", &["message.received"])).await;
        assert_ack_ok(ack);
        assert!(
            registry.connected_plugin("echo").is_some(),
            "握手成功后应已挂接连接"
        );
        assert_eq!(
            registry.get("echo").unwrap().state,
            PluginState::Running,
            "握手上应置 Running"
        );
    }

    // 2. Hello.name 与 socket 名不匹配 → Ack{ok:false}
    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn hello_name_mismatch_rejected() {
        let registry = plugin_registry(&[("echo", vec![])]);
        let api = build_api(registry.clone()).await;
        let h = start_server(
            "echo",
            registry.clone(),
            Arc::new(api),
            Duration::from_secs(5),
        )
        .await;

        let (_stream, ack) = connect_and_hello(&h.socket_path, &hello("intruder", &[])).await;
        assert_ack_fail(ack, "插件名不匹配");
        assert!(
            registry.connected_plugin("echo").is_none(),
            "名字不匹配不应挂接连接"
        );
    }

    // 3. subscribe 含非法事件名 → Ack{ok:false}
    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn invalid_subscribe_rejected() {
        let registry = plugin_registry(&[("echo", vec![])]);
        let api = build_api(registry.clone()).await;
        let h = start_server(
            "echo",
            registry.clone(),
            Arc::new(api),
            Duration::from_secs(5),
        )
        .await;

        let (_stream, ack) =
            connect_and_hello(&h.socket_path, &hello("echo", &["nope.nope"])).await;
        assert_ack_fail(ack, "未知事件名");
        assert!(
            registry.connected_plugin("echo").is_none(),
            "非法订阅不应挂接连接"
        );
    }

    // 4. attach_conn 失败路径 → Ack{ok:false}
    //   a) 插件未注册（expected_name 不在 registry）
    //   b) 已注册但状态非 Starting
    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn unregistered_plugin_rejected() {
        // 空 registry：expected_name 未注册 → attach_conn 返回"插件未注册"。
        let registry = Arc::new(PluginRegistry::new());
        let api = build_api(registry.clone()).await;
        let h = start_server(
            "echo",
            registry.clone(),
            Arc::new(api),
            Duration::from_secs(5),
        )
        .await;

        let (_stream, ack) = connect_and_hello(&h.socket_path, &hello("echo", &[])).await;
        assert_ack_fail(ack, "插件未注册");
    }

    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn not_starting_state_rejected() {
        let registry = plugin_registry_discovered(&["echo"]);
        let api = build_api(registry.clone()).await;
        let h = start_server(
            "echo",
            registry.clone(),
            Arc::new(api),
            Duration::from_secs(5),
        )
        .await;

        // 名字匹配但状态非 Starting → attach_conn 拒绝。
        let (_stream, ack) = connect_and_hello(&h.socket_path, &hello("echo", &[])).await;
        assert_ack_fail(ack, "不在启动状态");
        assert!(registry.connected_plugin("echo").is_none());
    }

    // 5. 握手后发 Request：plugin_data.list → ok:true; 无权限 message.send → 权限不足; 未知方法 → ok:false
    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn request_dispatch_roundtrip() {
        let registry = plugin_registry(&[("noauth", vec![])]);
        let api = build_api(registry.clone()).await;
        let h = start_server(
            "noauth",
            registry.clone(),
            Arc::new(api),
            Duration::from_secs(5),
        )
        .await;

        let (mut stream, ack) = connect_and_hello(&h.socket_path, &hello("noauth", &[])).await;
        assert_ack_ok(ack);

        // plugin_data.list → 免权限，应成功且返回空数组。
        stream
            .write_all(
                &encode_frame(&WireMessage::Request {
                    id: 1,
                    method: "plugin_data.list".to_string(),
                    params: serde_json::json!({}),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let mut buffer = Vec::new();
        let resp = read_one_client_frame(&mut stream, &mut buffer)
            .await
            .unwrap()
            .expect("应收到 Response");
        match resp {
            WireMessage::Response {
                id,
                ok,
                result,
                error,
            } => {
                assert_eq!(id, 1);
                assert!(ok, "plugin_data.list 应成功，error={error:?}");
                assert_eq!(result, Some(serde_json::json!([])));
            }
            other => panic!("期望 Response，实际 {other:?}"),
        }

        // 无权限插件 message.send → 权限不足。
        stream
            .write_all(
                &encode_frame(&WireMessage::Request {
                    id: 2,
                    method: "message.send".to_string(),
                    params: serde_json::json!({}),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let resp = read_one_client_frame(&mut stream, &mut buffer)
            .await
            .unwrap()
            .expect("应收到 Response");
        match resp {
            WireMessage::Response { id, ok, error, .. } => {
                assert_eq!(id, 2);
                assert!(!ok, "无权限 message.send 应失败");
                assert!(error.unwrap().contains("权限不足"), "错误应含权限不足");
            }
            other => panic!("期望 Response，实际 {other:?}"),
        }

        // 未知方法 → ok:false。
        stream
            .write_all(
                &encode_frame(&WireMessage::Request {
                    id: 3,
                    method: "no.such".to_string(),
                    params: serde_json::json!({}),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let resp = read_one_client_frame(&mut stream, &mut buffer)
            .await
            .unwrap()
            .expect("应收到 Response");
        match resp {
            WireMessage::Response { id, ok, .. } => {
                assert_eq!(id, 3);
                assert!(!ok, "未知方法应失败");
            }
            other => panic!("期望 Response，实际 {other:?}"),
        }
    }

    // 6. 未握手直接发 Request → 协议错误被关连接（读端 EOF）
    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn request_without_handshake_closes_connection() {
        let registry = plugin_registry(&[("echo", vec![])]);
        let api = build_api(registry.clone()).await;
        let h = start_server(
            "echo",
            registry.clone(),
            Arc::new(api),
            Duration::from_secs(5),
        )
        .await;

        let mut stream = UnixStream::connect(&h.socket_path).await.unwrap();
        stream
            .write_all(
                &encode_frame(&WireMessage::Request {
                    id: 9,
                    method: "plugin_data.list".to_string(),
                    params: serde_json::json!({}),
                })
                .unwrap(),
            )
            .await
            .unwrap();

        // 首帧不是 Hello → 服务端关闭连接 → 读端最终 EOF。
        // （可能先读到一次 HelloAck{ok:false}，随后 EOF。）
        let mut chunk = [0u8; 16];
        let mut saw_eof = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), stream.read(&mut chunk)).await {
                Ok(Ok(0)) => {
                    saw_eof = true;
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => {
                    saw_eof = true;
                    break;
                }
                Err(_) => continue,
            }
        }
        assert!(saw_eof, "未握手连接应在超时前被服务端关闭（EOF/错误）");
        assert!(
            registry.connected_plugin("echo").is_none(),
            "未握手不应挂接连接"
        );
    }

    // 7. Notify 送达：hello 后 via connected_plugin try_send
    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn notify_delivery() {
        let registry = plugin_registry(&[("echo", vec![])]);
        let api = build_api(registry.clone()).await;
        let h = start_server(
            "echo",
            registry.clone(),
            Arc::new(api),
            Duration::from_secs(5),
        )
        .await;

        let (mut stream, ack) =
            connect_and_hello(&h.socket_path, &hello("echo", &["message.received"])).await;
        assert_ack_ok(ack);

        // 通过注册表取得连接发送端，模拟事件桥接推送 Notify。
        let tx = registry.connected_plugin("echo").expect("应已连接");
        let payload = serde_json::json!({ "content": "事件载荷" });
        tx.try_send(WireMessage::Notify {
            event: "message.received".to_string(),
            data: payload.clone(),
        })
        .unwrap();

        let mut buffer = Vec::new();
        let msg = read_one_client_frame(&mut stream, &mut buffer)
            .await
            .unwrap()
            .expect("应收到 Notify");
        match msg {
            WireMessage::Notify { event, data } => {
                assert_eq!(event, "message.received");
                assert_eq!(data, payload);
            }
            other => panic!("期望 Notify，实际 {other:?}"),
        }
    }

    // 8. 半包：同一帧分两次写好，server 仍能处理后续 Request
    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn half_packet_request() {
        let registry = plugin_registry(&[("echo", vec![])]);
        let api = build_api(registry.clone()).await;
        let h = start_server(
            "echo",
            registry.clone(),
            Arc::new(api),
            Duration::from_secs(5),
        )
        .await;

        let (mut stream, ack) = connect_and_hello(&h.socket_path, &hello("echo", &[])).await;
        assert_ack_ok(ack);

        // 把 Request 帧切成两半，分两次写。
        let frame = encode_frame(&WireMessage::Request {
            id: 7,
            method: "plugin_data.list".to_string(),
            params: serde_json::json!({}),
        })
        .unwrap();
        let mid = frame.len() / 2;
        stream.write_all(&frame[..mid]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        stream.write_all(&frame[mid..]).await.unwrap();

        let mut buffer = Vec::new();
        let resp = read_one_client_frame(&mut stream, &mut buffer)
            .await
            .unwrap()
            .expect("半包后应能去帧并回应");
        match resp {
            WireMessage::Response { id, ok, .. } => {
                assert_eq!(id, 7);
                assert!(ok);
            }
            other => panic!("期望 Response，实际 {other:?}"),
        }
    }

    // 9. 握手超时：client 连上不发 Hello，超时后应被关
    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn handshake_timeout_closes_connection() {
        let registry = plugin_registry(&[("echo", vec![])]);
        let api = build_api(registry.clone()).await;
        let h = start_server(
            "echo",
            registry.clone(),
            Arc::new(api),
            Duration::from_millis(300),
        )
        .await;

        let mut stream = UnixStream::connect(&h.socket_path).await.unwrap();
        // 不发送任何帧，等待 server 超时关连接。
        let mut chunk = [0u8; 16];
        // 由于超时后 server 关连接，读端最终得到 EOF 或错误。
        let start = std::time::Instant::now();
        let mut eof = false;
        while start.elapsed() < Duration::from_secs(2) {
            match tokio::time::timeout(Duration::from_millis(100), stream.read(&mut chunk)).await {
                Ok(Ok(0)) => {
                    eof = true;
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => {
                    eof = true;
                    break;
                }
                Err(_) => continue,
            }
        }
        assert!(eof, "超时后连接应被关闭（EOF/错误）");
        assert!(
            registry.connected_plugin("echo").is_none(),
            "超时不应挂接连接"
        );
    }

    // 10. 重连：同一 socket 可二次连接成功
    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn reconnect_after_close() {
        let registry = plugin_registry(&[("echo", vec![])]);
        let api = build_api(registry.clone()).await;
        let h = start_server(
            "echo",
            registry.clone(),
            Arc::new(api),
            Duration::from_secs(5),
        )
        .await;

        // 第一次连接握手成功后关闭。
        let (stream1, ack1) = connect_and_hello(&h.socket_path, &hello("echo", &[])).await;
        assert_ack_ok(ack1);
        drop(stream1);
        // 等 server 感知 EOF 并发起清理。
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            registry.connected_plugin("echo").is_none(),
            "断开后应清除连接"
        );

        // 第二次连接（fresh accept）应再次成功握手。
        registry.set_state("echo", PluginState::Starting);
        let (stream2, ack2) = connect_and_hello(&h.socket_path, &hello("echo", &[])).await;
        assert_ack_ok(ack2);
        assert!(
            registry.connected_plugin("echo").is_some(),
            "重连应再次挂接"
        );
        drop(stream2);
    }

    // -----------------------------------------------------------------------
    // 11. panic 清理守卫：handle_conn 循环内 panic（tokio 任务终止）也必须
    //     摘除连接并触发 conn_closed，保证 supervisor 不会永久失明。
    // -----------------------------------------------------------------------
    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn panic_in_handler_still_cleans_up_connection() {
        let registry = plugin_registry(&[("echo", vec![])]);
        let (closed_tx, closed_rx) = oneshot::channel::<()>();
        let conn_closed = Arc::new(std::sync::Mutex::new(Some(closed_tx)));

        // 预置一条已挂接的连接，模拟握手成功后的运行期状态。
        let (tx, _rx) = mpsc::channel::<WireMessage>(16);
        registry.set_state("echo", PluginState::Starting);
        registry.attach_conn("echo", tx.clone()).unwrap();
        assert!(registry.connected_plugin("echo").is_some());

        // 场景等价：handle_conn 在 spawned 任务中处理请求时 dispatch panic，
        // 循环内的显式 close_conn 不会执行，必须由守卫的 Drop 完成清理。
        // 用 catch_unwind 承接 panic（panic 展开路径与真实任务终止一致：
        // 栈上守卫 Drop 必然运行），并避免任务 panic 污染测试日志。
        let guard_registry = registry.clone();
        let guard_closed = conn_closed.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = ConnCleanupGuard {
                registry: guard_registry,
                name: "echo".to_string(),
                conn_closed: guard_closed,
                owner: tx,
            };
            panic!("模拟插件请求处理 panic");
        }));
        assert!(result.is_err(), "预期内 panic 应被承接");

        assert!(
            registry.connected_plugin("echo").is_none(),
            "panic 后连接必须被摘除（registry.conn == None）"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), closed_rx)
                .await
                .is_ok(),
            "panic 后必须触发 conn_closed 通知（supervisor 才能感知并接管）"
        );
    }

    // 12. 守卫在 spawned 任务被 panic 终止（未承接）时同样完成清理：
    //     这是 handle_conn 消息循环的真实终止形态（panic 不被循环捕获）。
    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn panic_escaping_task_still_cleans_up_connection() {
        let registry = plugin_registry(&[("echo", vec![])]);
        let (closed_tx, closed_rx) = oneshot::channel::<()>();
        let conn_closed = Arc::new(std::sync::Mutex::new(Some(closed_tx)));

        let (tx, _rx) = mpsc::channel::<WireMessage>(16);
        registry.set_state("echo", PluginState::Starting);
        registry.attach_conn("echo", tx.clone()).unwrap();

        let guard_registry = registry.clone();
        let guard_closed = conn_closed.clone();
        let task = tokio::spawn(async move {
            let _guard = ConnCleanupGuard {
                registry: guard_registry,
                name: "echo".to_string(),
                conn_closed: guard_closed,
                owner: tx,
            };
            panic!("模拟 tokio 任务内 dispatch panic（不被承接）");
        });
        // JoinError::is_panic 应为真：panic 逃出任务由 runtime 捕获。
        assert!(
            task.await.unwrap_err().is_panic(),
            "任务应以 panic 形式终止"
        );

        assert!(
            registry.connected_plugin("echo").is_none(),
            "任务 panic 终止后连接必须被摘除"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), closed_rx)
                .await
                .is_ok(),
            "任务 panic 终止后必须触发 conn_closed 通知"
        );
    }
    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn failed_duplicate_handshake_preserves_live_connection_and_subscriptions() {
        let registry = plugin_registry(&[("echo", vec![PluginPermission::MessageRead])]);
        let api = build_api(registry.clone()).await;
        let harness = start_server(
            "echo",
            registry.clone(),
            Arc::new(api),
            Duration::from_secs(5),
        )
        .await;
        let (original, acknowledgement) =
            connect_and_hello(&harness.socket_path, &hello("echo", &["message.received"])).await;
        assert_ack_ok(acknowledgement);
        let owner = registry.connected_plugin("echo").unwrap();
        let (_duplicate, acknowledgement) =
            connect_and_hello(&harness.socket_path, &hello("echo", &[])).await;
        assert_ack_fail(acknowledgement, "连接已存在");
        assert!(registry
            .connected_plugin("echo")
            .unwrap()
            .same_channel(&owner));
        assert_eq!(
            registry.get("echo").unwrap().subscriptions,
            vec![EventType::MessageReceived]
        );
        drop(original);
    }

    #[ignore = "需要允许 Unix socket 的环境"]
    #[tokio::test]
    async fn subscriptions_require_matching_read_permission() {
        let registry = plugin_registry(&[("echo", vec![PluginPermission::MessageSend])]);
        let api = build_api(registry.clone()).await;
        let harness = start_server(
            "echo",
            registry.clone(),
            Arc::new(api),
            Duration::from_secs(5),
        )
        .await;
        let (_connection, acknowledgement) =
            connect_and_hello(&harness.socket_path, &hello("echo", &["message.received"])).await;
        assert_ack_fail(acknowledgement, "权限不足");
        assert!(registry.connected_plugin("echo").is_none());
        assert!(registry.get("echo").unwrap().subscriptions.is_empty());
    }
}
