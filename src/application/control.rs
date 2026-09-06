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
//! ```
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

/// 命令处理函数签名（异步）。接受 RuntimeHandle 和请求 ID。
type CommandHandler =
    fn(&RuntimeHandle, id: u64) -> CommandFuture;

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
    pub fn dispatch(&self, cmd: &str, handle: &RuntimeHandle, id: u64) -> Option<CommandFuture> {
        self.handlers.get(cmd).map(|(handler, _)| handler(handle, id))
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
        registry
    }
}

// ---------------------------------------------------------------------------
// 命令处理函数
// ---------------------------------------------------------------------------

fn handle_status(handle: &RuntimeHandle, id: u64) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move { Ok(ControlResponse::ok(id, handle.status().await)) })
}

fn handle_reload_config(handle: &RuntimeHandle, id: u64) -> CommandFuture {
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
            return Ok(ControlResponse::err(id, ControlErrorDetail::new("VALIDATION_ERROR", msg)));
        }

        tracing::info!(target: "control", "配置文件重载成功");
        Ok(ControlResponse::ok(id, serde_json::json!({
            "log_level": cfg.log_level,
            "data_dir": cfg.data_dir,
            "admin_users": cfg.admin_users,
        })))
    })
}

fn handle_shutdown(handle: &RuntimeHandle, id: u64) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        handle.shutdown();
        Ok(ControlResponse::ok(id, serde_json::json!({"message": "关停信号已发送"})))
    })
}

fn handle_help(handle: &RuntimeHandle, id: u64) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let commands = handle.commands.descriptions();
        Ok(ControlResponse::ok(id, serde_json::json!({
            "commands": commands,
        })))
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
        Self::new("UNKNOWN_COMMAND", format!("未知命令: {cmd}，使用 help 查看可用命令"))
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
    let id = val
        .get("id")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    // 解析命令名称。
    let cmd = match val.get("cmd").and_then(|v| v.as_str().map(String::from)) {
        Some(cmd) => cmd,
        None => {
            let resp = ControlResponse::err(id, ControlErrorDetail::missing_cmd());
            write_response(stream, resp).await?;
            return Ok(());
        }
    };

    // 分派命令。
    let resp = match handle.commands.dispatch(&cmd, handle, id) {
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
