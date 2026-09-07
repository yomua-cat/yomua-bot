//! 控制服务 —— 通过 UDS socket 接收 CLI 命令。
//!
//! 控制 socket 路径：`<data_dir>/control.sock`
//!
//! 支持的命令（JSON over UDS）：
//! ```json
//! {"id": 123, "cmd": "reload-config"}
//! {"id": 456, "cmd": "status"}
//! {"id": 789, "cmd": "shutdown"}
//! {"id": 111, "cmd": "help"}
//! {"id": 112, "cmd": "state-set", "params": {"character_id": 1, "conversation_id": 10, "mood": 80, "energy": 90, "stress": 20}}
//! {"id": 113, "cmd": "state-get", "params": {"character_id": 1, "conversation_id": 10}}
//! ```
//!
//! `state-set` / `state-get` 通过领域仓储（State System）读写角色状态：
//! - 不带 `conversation_id`：读写全局 Character State（energy / stress / activity）；
//! - 带 `conversation_id`：读写 Character × Conversation 状态（energy / stress → ConversationState，mood → Mood）。
//!
//! `mood` 属于 Character × Conversation 范围，必须提供 `conversation_id`；
//! `activity` 仅存在于全局 Character State；会话与角色都必须已存在
//! （否则返回 `CONVERSATION_NOT_FOUND` / `CHARACTER_NOT_FOUND`，防止孤儿状态行）。
//!
//! 响应格式：
//! ```json
//! {"id": 123, "ok": true, "data": {...}}
//! {"id": 456, "ok": false, "error": {"code": "...", "message": "..."}}
//! ```
//!
//! 扩展方式：实现 `CommandHandler` 并调用 `CommandRegistry::register`。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;

use crate::adapters::onebot::{OneBotAdapter, OneBotAdapterImpl};
use crate::application::config::{validate_runtime, RuntimeConfig};
use crate::domain::character::ConversationState;
use crate::domain::emotion::Mood;

use crate::domain::repository::{
    CharacterRepository, CharacterStateRepository, ConversationRepository,
    ConversationStateRepository, MoodRepository,
};
use crate::error::RuntimeError;
use crate::infrastructure::plugin::supervisor::PluginSupervisor;
use crate::infrastructure::storage::SqliteStorage;

// ---------------------------------------------------------------------------
// 命令处理器类型与注册表
// ---------------------------------------------------------------------------

/// 命令描述（用于 help 命令）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct CommandDesc {
    pub name: &'static str,
    pub description: &'static str,
}

/// 命令处理函数签名（异步）。接受 RuntimeHandle、请求 ID 与请求参数。
type CommandHandler = fn(&RuntimeHandle, id: u64, params: serde_json::Value) -> CommandFuture;

/// 命令处理结果 Future 的类型别名。
type CommandFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<ControlResponse, RuntimeError>> + Send>,
>;

/// 命令注册表。
#[derive(Default, Clone)]
pub struct CommandRegistry {
    handlers: HashMap<String, (CommandHandler, CommandDesc)>,
}

impl CommandRegistry {
    /// 注册一个新命令。
    pub fn register(
        &mut self,
        name: &'static str,
        description: &'static str,
        handler: CommandHandler,
    ) {
        self.handlers.insert(
            name.to_string(),
            (handler, CommandDesc { name, description }),
        );
    }

    /// 分派命令。返回 None 表示命令不存在。
    pub fn dispatch(
        &self,
        cmd: &str,
        handle: &RuntimeHandle,
        id: u64,
        params: serde_json::Value,
    ) -> Option<CommandFuture> {
        self.handlers
            .get(cmd)
            .map(|(handler, _)| handler(handle, id, params))
    }

    /// 返回所有已注册命令的描述。
    pub fn descriptions(&self) -> Vec<CommandDesc> {
        self.handlers
            .values()
            .map(|(_, desc)| desc.clone())
            .collect()
    }

    /// 构建包含所有内置命令的注册表。
    pub fn builtin() -> Self {
        let mut registry = Self::default();
        registry.register("status", "查询运行时状态", handle_status);
        registry.register(
            "reload-config",
            "重新加载并校验配置文件",
            handle_reload_config,
        );
        registry.register("shutdown", "发送优雅关停信号", handle_shutdown);
        registry.register("help", "显示所有可用命令", handle_help);
        registry.register(
            "state-set",
            "修改角色状态（全局 energy/stress/activity，或 Character×Conversation 的 energy/stress/mood）",
            handle_state_set,
        );
        registry.register(
            "state-get",
            "查询角色状态（全局与 Character×Conversation 范围）",
            handle_state_get,
        );
        registry
    }
}

// ---------------------------------------------------------------------------
// 命令处理函数
// ---------------------------------------------------------------------------

fn handle_status(handle: &RuntimeHandle, id: u64, _params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move { Ok(ControlResponse::ok(id, handle.status().await)) })
}

fn handle_reload_config(
    handle: &RuntimeHandle,
    id: u64,
    _params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        use crate::application::config::load_runtime;

        let runtime_path = handle.config_dir.join("runtime.toml");
        let cfg = load_runtime(&runtime_path.display().to_string())
            .map_err(|e| RuntimeError::Config(e.to_string()))?;

        // 校验。
        let errors = validate_runtime(&cfg);
        if !errors.is_empty() {
            let msg = errors.join("; ");
            tracing::error!(target: "control", "{}", msg);
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new("VALIDATION_ERROR", msg),
            ));
        }

        tracing::info!(target: "control", "配置文件重载成功");
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({
                "log_level": cfg.log_level,
                "data_dir": cfg.data_dir,
                "admin_users": cfg.admin_users,
            }),
        ))
    })
}

fn handle_shutdown(handle: &RuntimeHandle, id: u64, _params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        handle.shutdown();
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({"message": "关停信号已发送"}),
        ))
    })
}

fn handle_help(handle: &RuntimeHandle, id: u64, _params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let commands = handle.commands.descriptions();
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({
                "commands": commands,
            }),
        ))
    })
}

fn handle_state_set(handle: &RuntimeHandle, id: u64, params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ))
            }
        };

        // character_id 必填。
        let character_id = match obj.get("character_id").and_then(|v| v.as_i64()) {
            Some(cid) if cid > 0 => cid,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 character_id"),
                ))
            }
        };

        // 至少提供一个目标字段。
        let has_field = ["mood", "energy", "stress", "activity"]
            .iter()
            .any(|k| obj.contains_key(*k));
        if !has_field {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "INVALID_PARAMS",
                    "至少提供一个字段：mood / energy / stress / activity",
                ),
            ));
        }

        // 角色必须存在（与 plugin_api.character.state.write 一致，防止孤儿状态行）。
        let exists = handle
            .character_repo
            .find_by_id(character_id)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?
            .is_some();
        if !exists {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "CHARACTER_NOT_FOUND",
                    format!("角色不存在：{character_id}"),
                ),
            ));
        }

        // conversation_id 可选（缺省表示全局 Character State）。
        let conversation_id = match obj.get("conversation_id").and_then(|v| v.as_i64()) {
            None => None,
            Some(0) => None,
            Some(cid) if cid > 0 => Some(cid),
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "非法参数 conversation_id"),
                ))
            }
        };

        // mood 属于 Character × Conversation 范围：必须提供 conversation_id。
        if obj.contains_key("mood") && conversation_id.is_none() {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "MOOD_REQUIRES_CONVERSATION",
                    "mood 属于 Character × Conversation 范围，必须提供 conversation_id",
                ),
            ));
        }

        // 会话级 activity 在当前状态模型中未定义（仅全局 Character State 有 current_activity）。
        if obj.contains_key("activity") && conversation_id.is_some() {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "SCOPED_ACTIVITY_UNSUPPORTED",
                    "activity 当前仅存在于全局 Character State；会话级 activity 模型未定义",
                ),
            ));
        }

        // 会话必须存在（与角色存在校验一致，防止孤儿状态行）。
        if let Some(cid) = conversation_id {
            let exists = handle
                .conversation_repo
                .find_by_id(cid)
                .await
                .map_err(|e| RuntimeError::Internal(e.to_string()))?
                .is_some();
            if !exists {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("CONVERSATION_NOT_FOUND", format!("会话不存在：{cid}")),
                ));
            }
        }

        let mut result = serde_json::json!({ "character_id": character_id });

        match conversation_id {
            // 全局 Character State：energy / stress / activity。
            None => {
                let mut state = handle
                    .state_repo
                    .find_by_character_id(character_id)
                    .await
                    .map_err(|e| RuntimeError::Internal(e.to_string()))?
                    .unwrap_or_default();

                if let Some(v) = obj.get("energy") {
                    state.energy = v
                        .as_f64()
                        .ok_or_else(|| RuntimeError::Internal("energy 必须是数字".to_string()))?;
                }
                if let Some(v) = obj.get("stress") {
                    state.stress = v
                        .as_f64()
                        .ok_or_else(|| RuntimeError::Internal("stress 必须是数字".to_string()))?;
                }
                if let Some(v) = obj.get("activity") {
                    state.current_activity = match v {
                        serde_json::Value::Null => None,
                        v => Some(
                            v.as_str()
                                .ok_or_else(|| {
                                    RuntimeError::Internal("activity 必须是字符串".to_string())
                                })?
                                .to_string(),
                        ),
                    };
                }

                let state = state.clamped();
                handle
                    .state_repo
                    .upsert(character_id, &state)
                    .await
                    .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                result["character_state"] = serde_json::to_value(&state)
                    .map_err(|e| RuntimeError::Internal(e.to_string()))?;
            }

            // Character × Conversation：energy / stress → ConversationState，mood → Mood。
            Some(cid) => {
                result["conversation_id"] = serde_json::json!(cid);

                let patch_conv = obj.contains_key("energy") || obj.contains_key("stress");
                if patch_conv {
                    let mut state: ConversationState = handle
                        .conversation_state_repo
                        .find_by_character_and_conversation(character_id, cid)
                        .await
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?
                        .unwrap_or_default();

                    if let Some(v) = obj.get("energy") {
                        state.energy = v.as_f64().ok_or_else(|| {
                            RuntimeError::Internal("energy 必须是数字".to_string())
                        })?;
                    }
                    if let Some(v) = obj.get("stress") {
                        state.stress = v.as_f64().ok_or_else(|| {
                            RuntimeError::Internal("stress 必须是数字".to_string())
                        })?;
                    }

                    let state = state.clamped();
                    handle
                        .conversation_state_repo
                        .upsert(character_id, cid, &state)
                        .await
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                    result["conversation_state"] = serde_json::to_value(&state)
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                }

                if obj.contains_key("mood") {
                    let mood_value = obj
                        .get("mood")
                        .and_then(|v| v.as_f64())
                        .ok_or_else(|| RuntimeError::Internal("mood 必须是数字".to_string()))?;
                    let mut mood: Mood = handle
                        .mood_repo
                        .find_by_character_and_conversation(character_id, cid)
                        .await
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?
                        .unwrap_or_default();
                    mood.value = mood_value;
                    let mood = mood.clamped();
                    handle
                        .mood_repo
                        .upsert(character_id, cid, &mood)
                        .await
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                    result["mood"] = serde_json::to_value(&mood)
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                }
            }
        }

        Ok(ControlResponse::ok(id, result))
    })
}

fn handle_state_get(handle: &RuntimeHandle, id: u64, params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ))
            }
        };

        let character_id = match obj.get("character_id").and_then(|v| v.as_i64()) {
            Some(cid) if cid > 0 => cid,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 character_id"),
                ))
            }
        };
        let conversation_id = match obj.get("conversation_id").and_then(|v| v.as_i64()) {
            None => None,
            Some(0) => None,
            Some(cid) if cid > 0 => Some(cid),
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "非法参数 conversation_id"),
                ))
            }
        };

        let mut result = serde_json::json!({ "character_id": character_id });

        // 全局 Character State（无记录返回 null，不做副作用写入）。
        match handle
            .state_repo
            .find_by_character_id(character_id)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?
        {
            Some(state) => {
                result["character_state"] = serde_json::to_value(&state)
                    .map_err(|e| RuntimeError::Internal(e.to_string()))?;
            }
            None => {
                result["character_state"] = serde_json::Value::Null;
            }
        }

        // Character × Conversation State。
        if let Some(cid) = conversation_id {
            result["conversation_id"] = serde_json::json!(cid);
            match handle
                .conversation_state_repo
                .find_by_character_and_conversation(character_id, cid)
                .await
                .map_err(|e| RuntimeError::Internal(e.to_string()))?
            {
                Some(state) => {
                    result["conversation_state"] = serde_json::to_value(&state)
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                }
                None => {
                    result["conversation_state"] = serde_json::Value::Null;
                }
            }
            match handle
                .mood_repo
                .find_by_character_and_conversation(character_id, cid)
                .await
                .map_err(|e| RuntimeError::Internal(e.to_string()))?
            {
                Some(mood) => {
                    result["mood"] = serde_json::to_value(&mood)
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                }
                None => {
                    result["mood"] = serde_json::Value::Null;
                }
            }
        }

        Ok(ControlResponse::ok(id, result))
    })
}

// ---------------------------------------------------------------------------
// 响应类型
// ---------------------------------------------------------------------------

/// 控制协议错误码。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ControlErrorDetail {
    pub code: &'static str,
    pub message: String,
}

impl ControlErrorDetail {
    pub fn new(code: &'static str, message: impl std::fmt::Display) -> Self {
        Self {
            code,
            message: message.to_string(),
        }
    }

    /// 未知命令。
    pub fn unknown_command(cmd: &str) -> Self {
        Self::new(
            "UNKNOWN_COMMAND",
            format!("未知命令: {cmd}，使用 help 查看可用命令"),
        )
    }

    /// 缺少 cmd 字段。
    pub fn missing_cmd() -> Self {
        Self::new("MISSING_CMD", "请求缺少 cmd 字段")
    }

    /// 无效的 JSON。
    pub fn invalid_json(e: &str) -> Self {
        Self::new("INVALID_JSON", format!("无效的 JSON: {e}"))
    }

    /// 内部错误。
    pub fn internal(msg: &str) -> Self {
        Self::new("INTERNAL_ERROR", msg)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ControlResponse {
    /// 请求 ID，用于关联请求与响应。
    pub id: u64,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ControlErrorDetail>,
}

impl ControlResponse {
    fn ok(id: u64, data: impl serde::Serialize) -> Self {
        Self {
            id,
            ok: true,
            data: Some(serde_json::to_value(data).unwrap_or_default()),
            error: None,
        }
    }

    fn err(id: u64, detail: ControlErrorDetail) -> Self {
        Self {
            id,
            ok: false,
            data: None,
            error: Some(detail),
        }
    }
}

// ---------------------------------------------------------------------------
// RuntimeHandle
// ---------------------------------------------------------------------------

/// 运行时句柄 —— 持有所有关键组件，供命令处理器使用。
#[derive(Clone)]
pub struct RuntimeHandle {
    /// 配置目录。
    pub config_dir: PathBuf,
    /// 当前运行时配置。
    pub runtime_cfg: RuntimeConfig,
    /// 数据目录。
    pub data_dir: PathBuf,
    /// 插件监督器。
    pub supervisor: Arc<PluginSupervisor>,
    /// 适配器。
    pub adapter: Arc<OneBotAdapterImpl>,
    /// 存储。
    pub storage: Arc<SqliteStorage>,
    /// 关停信号发送端。
    pub shutdown_tx: watch::Sender<bool>,
    /// 命令注册表（可外部扩展）。
    pub commands: CommandRegistry,
    /// 角色仓储（State System：校验角色存在）。
    pub character_repo: Arc<dyn CharacterRepository>,
    /// 全局角色状态仓储（State System：Character State）。
    pub state_repo: Arc<dyn CharacterStateRepository>,
    /// 会话级状态仓储（State System：Character × Conversation State）。
    pub conversation_state_repo: Arc<dyn ConversationStateRepository>,
    /// Mood 仓储（State System：Character × Conversation Mood）。
    pub mood_repo: Arc<dyn MoodRepository>,
    /// 会话仓储（State System：会话作用域校验，防止孤儿状态行）。
    pub conversation_repo: Arc<dyn ConversationRepository>,
}

impl RuntimeHandle {
    /// 返回当前运行时状态。
    pub async fn status(&self) -> RuntimeStatus {
        RuntimeStatus {
            adapter_state: format!("{:?}", self.adapter.state().await),
            plugin_count: self.supervisor.plugin_count(),
            data_dir: self.data_dir.to_string_lossy().to_string(),
            log_level: self.runtime_cfg.log_level.clone(),
        }
    }

    /// 发送关停信号。
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        tracing::info!(target: "control", "关停信号已发送");
    }
}

/// 运行时状态快照。
#[derive(Debug, serde::Serialize)]
pub struct RuntimeStatus {
    pub adapter_state: String,
    pub plugin_count: usize,
    pub data_dir: String,
    pub log_level: String,
}

// ---------------------------------------------------------------------------
// 控制服务入口
// ---------------------------------------------------------------------------

/// 启动控制服务（后台任务）。
pub fn start_control_service(
    handle: RuntimeHandle,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let socket_path = handle.data_dir.join("control.sock");
    tokio::spawn(async move {
        if let Err(e) = run_control_server(handle, socket_path, shutdown_rx).await {
            tracing::error!(target: "control", error = %e, "控制服务异常退出");
        }
    })
}

async fn run_control_server(
    handle: RuntimeHandle,
    socket_path: PathBuf,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    // 确保 socket 目录存在。
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            RuntimeError::Internal(format!("无法创建 socket 目录 {}: {e}", parent.display()))
        })?;
    }

    // 删除旧 socket 文件（如果存在）。
    let _ = std::fs::remove_file(&socket_path);

    let listener = UnixListener::bind(&socket_path).map_err(|e| {
        RuntimeError::Internal(format!(
            "无法绑定控制 socket {}: {e}",
            socket_path.display()
        ))
    })?;

    tracing::info!(target: "control", path = %socket_path.display(), "控制服务已启动");

    loop {
        tokio::select! {
            // 接受新连接。
            accept = listener.accept() => {
                let (mut stream, _) = accept.map_err(|e| {
                    RuntimeError::Internal(format!("accept 控制 socket 失败: {e}"))
                })?;
                let handle = handle.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(&handle, &mut stream).await {
                        tracing::warn!(target: "control", error = %e, "处理命令出错");
                    }
                });
            }
            // 收到关停信号，退出。
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    tracing::info!(target: "control", "控制服务收到关停信号，正在退出");
                    break;
                }
            }
        }
    }

    // 清理 socket 文件。
    let _ = std::fs::remove_file(&socket_path);
    Ok(())
}

async fn handle_connection(
    handle: &RuntimeHandle,
    stream: &mut UnixStream,
) -> Result<(), RuntimeError> {
    let mut buf = Vec::with_capacity(4096);
    let n = stream
        .read_buf(&mut buf)
        .await
        .map_err(|e| RuntimeError::Internal(format!("读取控制命令失败: {e}")))?;

    if n == 0 {
        return Ok(());
    }

    buf.truncate(n);

    // 解析请求。
    let val: serde_json::Value = match serde_json::from_slice(&buf) {
        Ok(v) => v,
        Err(e) => {
            let resp = ControlResponse::err(0, ControlErrorDetail::invalid_json(&e.to_string()));
            write_response(stream, resp).await?;
            return Ok(());
        }
    };

    // 提取请求 ID（可选，默认为 0）。
    let id = val.get("id").and_then(|v| v.as_u64()).unwrap_or(0);

    // 解析命令名称。
    let cmd = match val.get("cmd").and_then(|v| v.as_str().map(String::from)) {
        Some(cmd) => cmd,
        None => {
            let resp = ControlResponse::err(id, ControlErrorDetail::missing_cmd());
            write_response(stream, resp).await?;
            return Ok(());
        }
    };

    // 提取参数（可选；无 params 视为 Null）。
    let params = val
        .get("params")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    // 分派命令。
    let resp = match handle.commands.dispatch(&cmd, handle, id, params) {
        Some(future) => future.await,
        None => {
            let resp = ControlResponse::err(id, ControlErrorDetail::unknown_command(&cmd));
            write_response(stream, resp).await?;
            return Ok(());
        }
    };

    write_response(stream, resp?).await?;
    Ok(())
}

async fn write_response(
    stream: &mut UnixStream,
    resp: ControlResponse,
) -> Result<(), RuntimeError> {
    let json = serde_json::to_string(&resp)
        .map_err(|e| RuntimeError::Internal(format!("序列化响应失败: {e}")))?;
    stream
        .write_all(json.as_bytes())
        .await
        .map_err(|e| RuntimeError::Internal(format!("写入响应失败: {e}")))?;
    Ok(())
}
