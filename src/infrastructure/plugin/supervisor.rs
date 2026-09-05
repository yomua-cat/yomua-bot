//! 插件生命周期监督 —— 进程监督、健康监控与崩溃重启。
//!
//! 状态流（配合 `registry.try_transition` 声明式迁移）：
//! - 首次 spawn：`Discovered → Starting`；
//! - 握手成功：`Starting → Running`（`registry.attach_conn` 内部完成）；
//! - 崩溃后重启：先归口 `Running → Starting`（`decide_restart` 内完成，
//!   附已死 pid 清理）再重新 spawn —— 运行期崩溃的插件必须能回到 Starting，
//!   新实例的 connect + Hello 才有机会 attach 成功；
//! - 主动停止：`Running/Starting → Stopped`（`stop` 置位持久停止标志并立即
//!   置 Stopped，生命周期任务在任意退出路径上感知并清除该标志）；
//! - 崩溃预算耗尽：`Starting/Running → Crashed`，并清理连接与 pid。
//!
//! 崩溃归口只触发一次：每轮实例创建一组 oneshot（`attached` / `conn_closed`），
//! monitor 用 `select!` 择其一进入判定后即结束该轮监控，另一路信号自然失效，
//! 防止双重启。重启采用**循环结构**（每插件一个 `run_plugin_loop` 任务），
//! 避免 async 递归导致的 future 尺寸增长。

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::process::{Child, Command};
use tokio::sync::{oneshot, Notify};

use crate::application::plugin_api::PluginApi;
use crate::error::RuntimeError;
use crate::infrastructure::plugin::manifest;
use crate::infrastructure::plugin::protocol::WireMessage;
use crate::infrastructure::plugin::registry::PluginRegistry;
use crate::infrastructure::plugin::transport::UdsServer;
use crate::infrastructure::plugin::{PluginHost, PluginInfo, PluginManifest, PluginState};

/// 监督器配置。
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// 插件目录（含各插件子目录与 plugin.toml）。
    pub plugins_dir: PathBuf,
    /// socket 存放目录（`<name>.sock`）。
    pub sockets_dir: PathBuf,
    /// 握手超时：spawn 后等待插件连接并完成 Hello 的时限。
    pub handshake_timeout: Duration,
    /// 关停超时：发出 shutdown 通知后等待子进程自主退出的时限，超时 SIGKILL。
    pub shutdown_timeout: Duration,
    /// 最大重启次数（预算耗尽后置 Crashed）。
    pub max_restarts: u32,
    /// 重启退避基数。
    pub restart_base_backoff: Duration,
    /// 重启退避上限。
    pub restart_max_backoff: Duration,
    /// 稳定运行窗口：插件保持 `Running` 且持续存活超过该时长后，重启预算
    /// 重新计数（`restart_count` 清零）。预算 = 连续崩溃重启上限；attach
    /// 成功本身不再清零，避免“连上即崩”的插件被无限重启。
    pub stable_window: Duration,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            plugins_dir: PathBuf::from("plugins"),
            sockets_dir: PathBuf::from("data/plugin-sockets"),
            handshake_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(10),
            max_restarts: 3,
            restart_base_backoff: Duration::from_millis(500),
            restart_max_backoff: Duration::from_secs(8),
            stable_window: Duration::from_secs(60),
        }
    }
}

/// 崩溃重启决策：`restart_count < max_restarts` 时返回本次应等待的退避时长，
/// 否则（预算耗尽）返回 `None`。
///
/// 退避 = `min(restart_max_backoff, restart_base_backoff * 2^restart_count)`。
/// 纯函数，便于单测钉死边界。
pub fn restart_decision(
    restart_count: u32,
    max_restarts: u32,
    restart_base_backoff: Duration,
    restart_max_backoff: Duration,
) -> Option<Duration> {
    if restart_count >= max_restarts {
        return None;
    }
    let exp = restart_base_backoff.saturating_mul(1u32 << restart_count.min(30));
    Some(if exp > restart_max_backoff {
        restart_max_backoff
    } else {
        exp
    })
}

/// 子进程句柄：monitor 独占 `Child`；`stop()` 通过 `Notify` 请求终止并等待完成。
#[derive(Debug, Clone)]
struct ChildHandle {
    /// `stop()` 请求终止（notify_waiters 可并发触发）。
    stop_request: Arc<Notify>,
    /// monitor 完成终止/清理后通知 `stop()` 继续。
    done: Arc<Notify>,
}

impl ChildHandle {
    fn new() -> Self {
        Self {
            stop_request: Arc::new(Notify::new()),
            done: Arc::new(Notify::new()),
        }
    }
}

/// 内部运行上下文：字段均为可克隆/线程安全的共享句柄，供后台任务捕获。
#[derive(Clone)]
struct Core {
    cfg: SupervisorConfig,
    registry: Arc<PluginRegistry>,
    api: Arc<PluginApi>,
    stopping: Arc<Mutex<HashSet<String>>>,
    handles: Arc<Mutex<HashMap<String, ChildHandle>>>,
}

/// 插件监督器 —— 具体的 [`PluginHost`] 实现。
pub struct PluginSupervisor {
    core: Core,
}

impl PluginSupervisor {
    /// 创建一个插件监督器。
    pub fn new(cfg: SupervisorConfig, registry: Arc<PluginRegistry>, api: Arc<PluginApi>) -> Self {
        Self {
            core: Core {
                cfg,
                registry,
                api,
                stopping: Arc::new(Mutex::new(HashSet::new())),
                handles: Arc::new(Mutex::new(HashMap::new())),
            },
        }
    }

    /// 启动目录下的全部插件：发现 → 校验 → 逐个 register + spawn。
    ///
    /// 单个插件失败仅 warn 跳过，不中断整体；插件目录不存在返回中文错误。
    pub async fn start_all(&self) -> Result<(), RuntimeError> {
        if !self.core.cfg.plugins_dir.is_dir() {
            return Err(RuntimeError::Plugin(format!(
                "插件目录不存在：{}",
                self.core.cfg.plugins_dir.display()
            )));
        }
        for plugin in manifest::discover_plugins(&self.core.cfg.plugins_dir) {
            if self.core.registry.get(&plugin.name).is_none() {
                if let Err(e) = self.core.registry.register(plugin.clone()) {
                    tracing::warn!("注册插件 {} 失败：{e}，跳过", plugin.name);
                    continue;
                }
            }
            spawn_one(&self.core, &plugin);
        }
        Ok(())
    }

    /// 主动停止一个插件。
    ///
    /// 停止意图是**持久的**：先置位 stopping 标志并立即把状态置为
    /// `Stopped`，再由监控循环入口 / spawn 子进程前随时检查，不依赖一次性
    /// notify 的时序 —— 即使在实例启动窗口内调用也不会丢失停止意图。
    /// 有连接则先发 `shutdown` 通知（优雅退出），等待 `shutdown_timeout`，
    /// 超时则 SIGKILL；无连接直接杀。结束时清理 socket 文件与连接。
    /// stopping 标志由对应生命周期任务（`run_plugin_loop` 的守卫）退出时清除。
    pub async fn stop(&self, name: &str) -> Result<(), RuntimeError> {
        let record = self
            .core
            .registry
            .get(name)
            .ok_or_else(|| RuntimeError::Plugin("插件未注册".to_string()))?;
        self.core.stopping.lock().unwrap().insert(name.to_string());
        // 立即置 Stopped：各失败归口（重启决策 / 崩溃判定 / spawn 检查）
        // 都据此识别停止意图，不会再把插件置为 Crashed 或重新拉起。
        self.core.registry.set_state(name, PluginState::Stopped);

        // 优雅退出请求：若存在连接则发 shutdown 通知（插件收到后应自行退出）。
        if let Some(tx) = record.conn.clone() {
            let _ = tx.try_send(WireMessage::Notify {
                event: "shutdown".to_string(),
                data: serde_json::Value::Null,
            });
        }

        // 通知 monitor 终止子进程，并等待其完成清理（限时兜底）。
        // done 的等待器先注册再 notify，避免 monitor 快速完成时丢失唤醒。
        let handle = self.core.handles.lock().unwrap().get(name).cloned();
        if let Some(handle) = handle {
            let done_fut = handle.done.notified();
            handle.stop_request.notify_waiters();
            let deadline = self.core.cfg.shutdown_timeout + Duration::from_millis(500);
            tokio::time::timeout(deadline, done_fut)
                .await
                .map(|_| ())
                .unwrap_or_else(|_| {
                    tracing::warn!("插件 {name} 停止超时（monitor 未按时完成）");
                });
        }

        self.core.registry.clear_conn(name);
        let socket = self.sockets_dir().join(format!("{name}.sock"));
        let _ = std::fs::remove_file(&socket);
        self.core.registry.set_state(name, PluginState::Stopped);
        // 注意：这里**不**移除 stopping 标志 —— 由 run_plugin_loop 任务的守卫
        // 在退出时清除，保证 spawn 挂起期间停止意图仍可被随时检查。
        Ok(())
    }

    /// 停止全部插件并清理 socket 目录下的 .sock 文件。
    pub async fn shutdown_all(&self) -> Result<(), RuntimeError> {
        let names: Vec<String> = self
            .core
            .registry
            .all()
            .into_iter()
            .map(|r| r.manifest.name)
            .collect();
        for name in names {
            let _ = self.stop(&name).await;
        }
        // 兜底清理所有 .sock 文件。
        if let Ok(entries) = std::fs::read_dir(self.sockets_dir()) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("sock") {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        Ok(())
    }

    /// 返回 socket 目录。
    pub fn sockets_dir(&self) -> &Path {
        &self.core.cfg.sockets_dir
    }
}

// ---------------------------------------------------------------------------
// spawn 与监控（循环结构，无 async 递归）
// ---------------------------------------------------------------------------

/// 拉起一个插件的生命周期循环（后台任务）。
///
/// 首次启动与崩溃重启共用：`run_plugin_loop` 内部还会在预算内自行重启。
fn spawn_one(core: &Core, plugin: &PluginManifest) {
    // 显式启动 = 新的生命周期：清除上一次生命周期可能残留的停止标志
    // （stop() 不再自行移除标志，交由任务退出时清理）。
    core.stopping.lock().unwrap().remove(&plugin.name);
    tokio::spawn(run_plugin_loop(core.clone(), plugin.clone()));
}

/// 停止标志守卫：`stop()` 置位 stopping 后，由生命周期任务在**任意退出路径**
/// 上统一清除。这使“停止意图”在 spawn 挂起期间仍可被随时检查，不依赖
/// notify 的一次性时序；也意味着 `stop()` 本身不再负责移除标志。
struct StoppingGuard {
    core: Core,
    name: String,
}

impl Drop for StoppingGuard {
    fn drop(&mut self) {
        self.core.stopping.lock().unwrap().remove(&self.name);
    }
}

impl Core {
    fn is_stopping_now(&self, name: &str) -> bool {
        self.stopping.lock().unwrap().contains(name)
    }
}

/// 插件的完整生命周期：spawn 实例 → 监控 → 崩溃判定/重启 → 循环。
///
/// 每插件一个后台任务；`Core` 为共享句柄集合，可整体移入任务。
async fn run_plugin_loop(core: Core, plugin: PluginManifest) {
    let name = plugin.name.clone();

    // 停止标志守卫：任务结束（含提前退出）时清除本任务的停止置位。
    let _stopping_guard = StoppingGuard {
        core: core.clone(),
        name: name.clone(),
    };

    // 防重复：已在 Running 或正被主动停止的不再启动。
    let state = core.registry.get(&name).map(|r| r.state);
    if state == Some(PluginState::Running) || core.is_stopping_now(&name) {
        return;
    }

    loop {
        if core.is_stopping_now(&name) {
            return;
        }

        let instance = match spawn_instance(&core, &plugin).await {
            Ok(instance) => instance,
            Err(reason) => {
                // 停止意图（spawn 被中止）：静默结束，不记错、不重启、不置 Crashed。
                if core.is_stopping_now(&name)
                    || core.registry.get(&name).map(|r| r.state) == Some(PluginState::Stopped)
                {
                    return;
                }
                tracing::warn!("插件 {name} 实例启动失败：{reason}");
                core.registry.set_last_error(&name, Some(reason.clone()));
                if !decide_restart(&core, &name, &reason).await {
                    set_crashed(&core, &name);
                    return;
                }
                continue;
            }
        };

        let trigger = monitor_instance(&core, &plugin, instance).await;

        match trigger {
            Trigger::Stopped => {
                // stop() 已把状态置 Stopped。
                return;
            }
            Trigger::Failed(reason) => {
                tracing::warn!("插件 {name} 运行异常：{reason}");
                core.registry.set_last_error(&name, Some(reason.clone()));
                if core.is_stopping_now(&name) {
                    return;
                }
                if !decide_restart(&core, &name, &reason).await {
                    set_crashed(&core, &name);
                    return;
                }
            }
        }
    }
}

/// 一次实例：子进程 + 服务端任务 + 通知通道 + 句柄。
struct PluginInstance {
    child: Child,
    server_task: tokio::task::JoinHandle<()>,
    conn_rx: oneshot::Receiver<()>,
    attached_rx: oneshot::Receiver<()>,
    handle: ChildHandle,
}

/// 创建并拉起飞一个插件实例（bind → spawn 子进程 → 挂接通知）。
async fn spawn_instance(core: &Core, plugin: &PluginManifest) -> Result<PluginInstance, String> {
    let name = plugin.name.clone();

    // 停止意图可能在任意时刻置位：绑定前先检查一次。
    if core.is_stopping_now(&name) {
        return Err("插件已停止，不再启动".to_string());
    }

    // 确保 sockets 目录存在。
    if let Err(e) = std::fs::create_dir_all(&core.cfg.sockets_dir) {
        return Err(format!("创建 sockets 目录失败：{e}"));
    }

    let socket_path = core.cfg.sockets_dir.join(format!("{name}.sock"));

    // 绑定 UDS 服务端（bind 内部先清理陈旧 socket 文件并校验路径长度）。
    let server = UdsServer::bind(
        &socket_path,
        name.clone(),
        core.registry.clone(),
        core.api.clone(),
    )
    .await
    .map_err(|e| format!("socket 绑定失败：{e}"))?;

    // 挂起期间可能收到 stop()：不再拉起子进程（清理刚创建的 socket 文件）。
    if core.is_stopping_now(&name) {
        let _ = std::fs::remove_file(&socket_path);
        return Err("插件已停止，不再启动".to_string());
    }

    // 状态迁移：Discovered/Stopped/Crashed → Starting。
    // （运行期崩溃后的 Running → Starting 归口在 decide_restart 中完成。）
    let state = core
        .registry
        .get(&name)
        .map(|r| r.state)
        .unwrap_or(PluginState::Discovered);
    if state != PluginState::Starting && state != PluginState::Running {
        core.registry
            .try_transition(&name, PluginState::Discovered, PluginState::Starting)
            .or_else(|_| {
                core.registry
                    .try_transition(&name, PluginState::Stopped, PluginState::Starting)
            })
            .or_else(|_| {
                core.registry
                    .try_transition(&name, PluginState::Crashed, PluginState::Starting)
            })
            .unwrap_or_else(|e| {
                tracing::warn!("插件 {name} 状态迁移失败：{e}");
            });
    }

    // 先注册子进程句柄再拉起子进程：保证 stop() 在 monitor 启动前即可定位句柄
    // 并等待完成信号。
    let handle = ChildHandle::new();
    core.handles
        .lock()
        .unwrap()
        .insert(name.clone(), handle.clone());

    // 拉起子进程：executable 为插件目录内的相对路径，无 CLI 参数；
    // env 注入 socket 绝对路径与插件名。插件日志进 Core 控制台。
    let plugin_dir = core.cfg.plugins_dir.join(&name);
    let executable = plugin_dir.join(&plugin.executable);
    let mut command = Command::new(&executable);
    command
        .current_dir(&plugin_dir)
        .env("YOMUA_PLUGIN_SOCKET", &socket_path)
        .env("YOMUA_PLUGIN_NAME", &name)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        // 兜底：若 Child 因异常路径被丢弃（如测试中途终止），强制杀进程，
        // 防止子进程残留、占用继承的输出管道。
        .kill_on_drop(true);
    let child = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            // 拉起失败：撤掉句柄，避免 stop() 挂到不存在的实例上。
            drop_handle(core, &name);
            return Err(format!("子进程启动失败：{e}"));
        }
    };
    core.registry.set_pid(&name, child.id());

    // attached / conn_closed 通知通道（每实例全新，崩溃归口仅触发一次）。
    let (attached_tx, attached_rx) = oneshot::channel();
    let (conn_tx, conn_rx) = oneshot::channel();
    let server = server
        .on_attached(attached_tx)
        .on_close(conn_tx)
        .with_handshake_timeout(core.cfg.handshake_timeout);
    let server_task = tokio::spawn(server.run());

    Ok(PluginInstance {
        child,
        server_task,
        conn_rx,
        attached_rx,
        handle,
    })
}

/// 监控结果：停止或失败（附原因）。
enum Trigger {
    Stopped,
    Failed(String),
}

/// 主动停止的统一收尾：杀子进程、摘连接、清 pid、撤句柄、停 server、通知完成。
/// 各处识别到停止意图（stopping 标志或状态 Stopped）后共用，保证无残留。
async fn finalize_stopped(core: &Core, inst: &mut PluginInstance, name: &str) {
    kill_and_reap(&mut inst.child).await;
    core.registry.clear_conn(name);
    core.registry.set_pid(name, None);
    drop_handle(core, name);
    inst.server_task.abort();
    inst.handle.done.notify_waiters();
}

/// 监控单个实例：归口（子进程退出 / 连接关闭）+ 启动超时 + 停止请求。
///
/// 任意一路触发即结束本轮监控，绝不回环；需要重启由 `run_plugin_loop` 负责。
/// 子进程退出用 `try_wait` 轮询检测（间隔 50ms），避免 `Child::wait` 的
/// `&mut` 借用与其他分支的 kill 操作冲突。
async fn monitor_instance(
    core: &Core,
    plugin: &PluginManifest,
    mut inst: PluginInstance,
) -> Trigger {
    let name = plugin.name.clone();
    let mut attached_done = false;
    // 握手成功时刻：插件保持 Running 且持续存活超过 `stable_window` 后
    // 重置崩溃预算（预算 = 连续崩溃重启上限）。
    let mut attached_at: Option<std::time::Instant> = None;

    // stop() 可能在 monitor 启动前就已到达（spawn 完成即被停止）：立即终止，
    // 不进入监控，避免“停止意图丢失 → 插件复活”。
    if core.is_stopping_now(&name) {
        tracing::info!("插件 {name} 在监控开始前已收到停止请求，立即终止");
        finalize_stopped(core, &mut inst, &name).await;
        return Trigger::Stopped;
    }

    // 启动超时自 monitor 开始计时（只创建一次；握手成功后由守卫停用）。
    let startup_deadline = tokio::time::sleep(core.cfg.handshake_timeout);
    tokio::pin!(startup_deadline);

    loop {
        // 子进程轮询间隔（每轮重建，保持 50ms 节奏）。
        let check_interval = tokio::time::sleep(Duration::from_millis(50));
        tokio::pin!(check_interval);
        tokio::select! {
            _ = inst.handle.stop_request.notified() => {
                // 主动停止：stop() 已置 Stopped；有连接则等其自主退出，否则强杀。
                if core.registry.connected_plugin(&name).is_some() {
                    let _ = tokio::time::timeout(core.cfg.shutdown_timeout, inst.child.wait()).await;
                } else {
                    kill_and_reap(&mut inst.child).await;
                }
                // 兜底：等待窗口内未退出则 SIGKILL，并统一收尾。
                finalize_stopped(core, &mut inst, &name).await;
                return Trigger::Stopped;
            }
            _ = &mut check_interval => {
                // 稳定运行窗口：Running 持续超过 stable_window 后重置崩溃预算。
                if let Some(at) = attached_at {
                    if at.elapsed() >= core.cfg.stable_window {
                        if core.registry.get(&name).map(|r| r.restart_count) != Some(0) {
                            tracing::info!(
                                "插件 {name} 已稳定运行超过 {}ms，重置崩溃预算",
                                core.cfg.stable_window.as_millis()
                            );
                            core.registry.clear_restarts(&name);
                        }
                        attached_at = None;
                    }
                }
                // 轮询子进程是否退出。
                match inst.child.try_wait() {
                    Ok(Some(status)) => {
                        drop_handle(core, &name);
                        if core.is_stopping_now(&name) {
                            finalize_stopped(core, &mut inst, &name).await;
                            return Trigger::Stopped;
                        }
                        inst.server_task.abort();
                        // 已死子进程的 pid 立即清掉（Crashed/重试前不残留）。
                        core.registry.set_pid(&name, None);
                        return Trigger::Failed(format!("进程退出（码 {}）", exit_code(&status)));
                    }
                    Ok(None) => {
                        // 仍在运行，继续监控。
                    }
                    Err(e) => {
                        drop_handle(core, &name);
                        if core.is_stopping_now(&name) {
                            finalize_stopped(core, &mut inst, &name).await;
                            return Trigger::Stopped;
                        }
                        inst.server_task.abort();
                        core.registry.set_pid(&name, None);
                        return Trigger::Failed(format!("轮询子进程失败：{e}"));
                    }
                }
            }
            _ = &mut inst.conn_rx => {
                // 连接关闭：握手失败（未 attached）或运行期断开（已 attached）。
                drop_handle(core, &name);
                if core.is_stopping_now(&name) {
                    finalize_stopped(core, &mut inst, &name).await;
                    return Trigger::Stopped;
                }
                let reason = if attached_done {
                    "连接中断（连接断开）".to_string()
                } else {
                    "握手失败（连接在 Hello 完成前断开）".to_string()
                };
                kill_and_reap(&mut inst.child).await;
                core.registry.set_pid(&name, None);
                inst.server_task.abort();
                return Trigger::Failed(reason);
            }
            _ = &mut inst.attached_rx, if !attached_done => {
                // 握手成功：进入正常运行期，取消启动超时。
                attached_done = true;
                attached_at = Some(std::time::Instant::now());
            }
            () = &mut startup_deadline, if !attached_done => {
                // 启动超时：spawn 后未在时限内完成握手。
                drop_handle(core, &name);
                if core.is_stopping_now(&name) {
                    finalize_stopped(core, &mut inst, &name).await;
                    return Trigger::Stopped;
                }
                kill_and_reap(&mut inst.child).await;
                core.registry.set_pid(&name, None);
                inst.server_task.abort();
                return Trigger::Failed(format!(
                    "启动超时：{}ms 内未完成握手",
                    core.cfg.handshake_timeout.as_millis()
                ));
            }
        }
    }
}

/// 崩溃重启前的状态归口：把运行期崩溃遗留的 `Running` 迁移回 `Starting`，
/// 保证新实例 connect + Hello 能 attach 成功。
///
/// - 状态本就是 `Starting`（握手前失败）：无需迁移，放行；
/// - 状态为 `Running`（运行期崩溃）：`Running → Starting`，并清掉已死
///   子进程的 pid；迁移失败说明状态机异常，记中文错误并返回 `false`；
/// - 状态为 `Stopped` / 其余异常状态：返回 `false`（调用方走 `set_crashed`，
///   其对 `Stopped` 有保护，不会覆盖）。
fn prepare_restart(core: &Core, name: &str) -> bool {
    let now = core.registry.get(name).map(|r| r.state);
    match now {
        Some(PluginState::Starting) => true,
        Some(PluginState::Running) => {
            if let Err(e) =
                core.registry
                    .try_transition(name, PluginState::Running, PluginState::Starting)
            {
                tracing::error!("插件 {name} 崩溃后无法回到启动状态：{e}");
                core.registry.set_last_error(
                    name,
                    Some("崩溃后状态无法回到启动状态，置为 Crashed".to_string()),
                );
                return false;
            }
            // 旧实例已死：清掉遗留 pid，等待新实例 spawn 后重新设置。
            core.registry.set_pid(name, None);
            true
        }
        Some(PluginState::Stopped) | None => false,
        Some(other) => {
            tracing::warn!("插件 {name} 重启时状态异常（{other:?}），不再重启");
            core.registry
                .set_last_error(name, Some(format!("重启时状态异常（{other:?}），不再重启")));
            false
        }
    }
}

/// 崩溃判定：预算内退回退避时长并返回 `true`（调用方重启），
/// 预算耗尽返回 `false`（调用方置 Crashed）。
async fn decide_restart(core: &Core, name: &str, reason: &str) -> bool {
    if core.is_stopping_now(name)
        || core.registry.get(name).map(|r| r.state) == Some(PluginState::Stopped)
    {
        return false;
    }
    let restart_count = core
        .registry
        .get(name)
        .map(|r| r.restart_count)
        .unwrap_or(0);
    match restart_decision(
        restart_count,
        core.cfg.max_restarts,
        core.cfg.restart_base_backoff,
        core.cfg.restart_max_backoff,
    ) {
        Some(backoff) => {
            tracing::warn!(
                "插件 {name} 异常：{reason}；{backoff:?} 后重启（第 {} 次）",
                restart_count + 1
            );
            core.registry.set_last_error(name, Some(reason.to_string()));
            tokio::time::sleep(backoff).await;
            // 退避期间可能收到主动停止请求。
            if core.is_stopping_now(name)
                || core.registry.get(name).map(|r| r.state) == Some(PluginState::Stopped)
            {
                return false;
            }
            // 状态归口（运行期崩溃的关键修复）：Running → Starting，
            // 失败则不重启。
            if !prepare_restart(core, name) {
                return false;
            }
            core.registry.record_restart(name);
            core.registry.clear_conn(name);
            // 移除陈旧 socket 文件，让下一实例重新 bind。
            let socket = core.cfg.sockets_dir.join(format!("{name}.sock"));
            let _ = std::fs::remove_file(&socket);
            true
        }
        None => false,
    }
}

/// 预算耗尽：状态置 Crashed，last_error 记中文上限提示。
fn set_crashed(core: &Core, name: &str) {
    let state = core.registry.get(name).map(|r| r.state);
    // 若已被并发 stop() 置 Stopped，不再覆盖。
    if state == Some(PluginState::Stopped) {
        return;
    }
    tracing::error!("插件 {name} 重启次数已达上限，置为 Crashed");
    let transition = core
        .registry
        .try_transition(name, PluginState::Starting, PluginState::Crashed)
        .or_else(|_| -> Result<(), RuntimeError> {
            core.registry.set_state(name, PluginState::Crashed);
            Ok(())
        });
    let _ = transition;
    core.registry
        .set_last_error(name, Some("重启次数已达上限".to_string()));
    // 清理陈旧 socket 文件、连接与遗留 pid（Crashed 插件不残留已死 pid）。
    core.registry.clear_conn(name);
    core.registry.set_pid(name, None);
    let socket = core.cfg.sockets_dir.join(format!("{name}.sock"));
    let _ = std::fs::remove_file(&socket);
}

/// SIGKILL 子进程并回收（reap）。
async fn kill_and_reap(child: &mut Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

/// 子进程退出码统一取出（macOS/Linux 均取 exit code，0 = 正常退出）。
fn exit_code(status: &std::process::ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status
            .code()
            .unwrap_or_else(|| status.signal().unwrap_or(-1))
    }
    #[cfg(not(unix))]
    {
        status.code().unwrap_or(-1)
    }
}

/// 实例结束/失败时移除句柄（防止 stop() 与已结束的 monitor 挂钩）。
fn drop_handle(core: &Core, name: &str) {
    core.handles.lock().unwrap().remove(name);
}

// ---------------------------------------------------------------------------
// PluginHost trait
// ---------------------------------------------------------------------------

#[async_trait]
impl PluginHost for PluginSupervisor {
    async fn discover(&self, path: &str) -> Result<Vec<PluginManifest>, RuntimeError> {
        Ok(manifest::discover_plugins(Path::new(path)))
    }

    async fn start(&self, manifest: &PluginManifest) -> Result<(), RuntimeError> {
        if self.core.registry.get(&manifest.name).is_none() {
            self.core.registry.register(manifest.clone())?;
        }
        spawn_one(&self.core, manifest);
        Ok(())
    }

    async fn stop(&self, name: &str) -> Result<(), RuntimeError> {
        self.stop(name).await
    }

    async fn list(&self) -> Result<Vec<PluginInfo>, RuntimeError> {
        Ok(self
            .core
            .registry
            .all()
            .into_iter()
            .map(|r| PluginInfo {
                name: r.manifest.name,
                state: r.state,
                pid: r.pid,
            })
            .collect())
    }

    async fn health_check(&self, name: &str) -> Result<bool, RuntimeError> {
        let record = self
            .core
            .registry
            .get(name)
            .ok_or_else(|| RuntimeError::Plugin("插件未注册".to_string()))?;
        Ok(record.state == PluginState::Running
            && self.core.registry.connected_plugin(name).is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::plugin::protocol::{decode_full_read, encode_frame, Hello};
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    // -----------------------------------------------------------------------
    // 纯逻辑测试
    // -----------------------------------------------------------------------

    fn base() -> Duration {
        Duration::from_millis(500)
    }
    fn cap() -> Duration {
        Duration::from_secs(8)
    }

    #[test]
    fn restart_decision_zero_restarts_returns_base() {
        assert_eq!(
            restart_decision(0, 3, base(), cap()),
            Some(Duration::from_millis(500))
        );
    }

    #[test]
    fn restart_decision_backoff_doubles_and_caps() {
        // 1 次 → 2 * base = 1000ms。
        assert_eq!(
            restart_decision(1, 5, base(), cap()),
            Some(Duration::from_millis(1000))
        );
        // 2 次 → 4 * base = 2000ms。
        assert_eq!(
            restart_decision(2, 5, base(), cap()),
            Some(Duration::from_millis(2000))
        );
        // 指数增长封顶：13 次 → 8192ms > 8s，取上限。
        assert_eq!(restart_decision(13, 20, base(), cap()), Some(cap()));
        // 大指数不会溢出（saturating 乘法）。
        assert!(restart_decision(100, 101, base(), cap()).unwrap() <= cap());
    }

    #[test]
    fn restart_decision_exhausted_returns_none() {
        // 达到预算上限 → 不再重启。
        assert_eq!(restart_decision(3, 3, base(), cap()), None);
        assert_eq!(restart_decision(4, 3, base(), cap()), None);
        // 预算为 0 → 首次失败即耗尽。
        assert_eq!(restart_decision(0, 0, base(), cap()), None);
    }

    #[test]
    fn restart_decision_last_allowed_restart_still_some() {
        // count=2、max=3 时仍允许最后一次重启。
        assert_eq!(
            restart_decision(2, 3, base(), cap()),
            Some(Duration::from_millis(2000))
        );
    }

    // -----------------------------------------------------------------------
    // 冒烟测试：真实子进程 + UDS，本地运行验证
    // -----------------------------------------------------------------------

    /// 在临时目录写一个可执行的 shell 脚本插件，返回其清单。
    fn write_plugin(root: &Path, name: &str, script: &str) -> PluginManifest {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("plugin.toml"),
            format!(
                "name = \"{name}\"\nversion = \"0.1.0\"\ndescription = \"冒烟插件\"\nexecutable = \"run.sh\"\n"
            ),
        )
        .unwrap();
        let script_path = dir.join("run.sh");
        std::fs::write(&script_path, script).unwrap();
        let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script_path, perms).unwrap();
        PluginManifest {
            name: name.to_string(),
            version: "0.1.0".to_string(),
            description: "冒烟插件".to_string(),
            permissions: vec![],
            executable: "run.sh".to_string(),
            config: serde_json::json!({}),
        }
    }

    /// 构造监督器（registry 预注册 + 测试桩 api）。
    async fn make_supervisor(
        plugins_dir: PathBuf,
        sockets_dir: PathBuf,
        cfg_patch: impl Fn(&mut SupervisorConfig),
    ) -> (PluginSupervisor, Arc<PluginRegistry>) {
        let registry = Arc::new(PluginRegistry::new());
        let api =
            crate::infrastructure::plugin::transport::test_support::build_api(registry.clone())
                .await;
        let mut cfg = SupervisorConfig {
            plugins_dir,
            sockets_dir,
            ..SupervisorConfig::default()
        };
        cfg_patch(&mut cfg);
        (
            PluginSupervisor::new(cfg, registry.clone(), Arc::new(api)),
            registry,
        )
    }

    /// 等待插件状态变为某状态（限时轮询）。
    async fn wait_state(registry: &PluginRegistry, name: &str, state: PluginState) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if registry.get(name).map(|r| r.state) == Some(state) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "等待插件 {name} 状态 {:?} 超时，当前 {:?}",
                state,
                registry.get(name).map(|r| r.state)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// smoke 1：`sleep 100` 型插件 + 短握手超时 → 启动超时失败 → 按预算重启 →
    /// 预算耗尽后 Crashed。max_restarts=1 控制时长。
    #[tokio::test]
    async fn smoke_handshake_timeout_restarts_then_crashes() {
        let dir = tempdir().unwrap();
        let _plugin = write_plugin(dir.path(), "sleepy", "#!/bin/sh\nexec sleep 100\n");
        let sockets = dir.path().join("sockets");
        let (supervisor, registry) =
            make_supervisor(dir.path().to_path_buf(), sockets.clone(), |cfg| {
                cfg.handshake_timeout = Duration::from_millis(300);
                cfg.max_restarts = 1;
                cfg.restart_base_backoff = Duration::from_millis(50);
                cfg.restart_max_backoff = Duration::from_millis(100);
                cfg.shutdown_timeout = Duration::from_millis(300);
            })
            .await;

        supervisor.start_all().await.expect("start_all 应成功");

        wait_state(&registry, "sleepy", PluginState::Crashed).await;

        let record = registry.get("sleepy").unwrap();
        assert_eq!(record.state, PluginState::Crashed);
        assert_eq!(record.restart_count, 1, "预算 1 次重启：耗尽后计数为 1");
        assert!(
            record
                .last_error
                .as_deref()
                .unwrap_or("")
                .contains("重启次数已达上限"),
            "last_error 应为中文上限提示，实际 {:?}",
            record.last_error
        );

        // 清理：确保没有遗留 sleep 进程。
        let _ = supervisor.stop("sleepy").await;
    }

    /// smoke 2：`exit 7` 短命脚本 + max_restarts=0 → 首次退出即 Crashed。
    #[tokio::test]
    async fn smoke_short_lived_exit_crashes_immediately() {
        let dir = tempdir().unwrap();
        let _plugin = write_plugin(dir.path(), "shorty", "#!/bin/sh\nexit 7\n");
        let sockets = dir.path().join("sockets");
        let (supervisor, registry) =
            make_supervisor(dir.path().to_path_buf(), sockets.clone(), |cfg| {
                cfg.max_restarts = 0;
                cfg.restart_base_backoff = Duration::from_millis(50);
                cfg.restart_max_backoff = Duration::from_millis(100);
                cfg.shutdown_timeout = Duration::from_millis(300);
            })
            .await;

        supervisor.start_all().await.expect("start_all 应成功");
        wait_state(&registry, "shorty", PluginState::Crashed).await;

        let record = registry.get("shorty").unwrap();
        assert_eq!(record.state, PluginState::Crashed);
        assert_eq!(record.restart_count, 0, "max_restarts=0 不应有任何重启");
        assert!(
            record
                .last_error
                .as_deref()
                .unwrap_or("")
                .contains("重启次数已达上限"),
            "last_error 应为中文上限提示，实际 {:?}",
            record.last_error
        );

        let _ = supervisor.stop("shorty").await;
    }

    /// smoke 3：stop() 无连接路径 —— sleep 型插件直接 kill，状态置 Stopped，秒级完成。
    #[tokio::test]
    async fn smoke_stop_without_conn_kills() {
        let dir = tempdir().unwrap();
        write_plugin(dir.path(), "stopper", "#!/bin/sh\nexec sleep 100\n");
        let sockets = dir.path().join("sockets");
        let (supervisor, registry) =
            make_supervisor(dir.path().to_path_buf(), sockets.clone(), |cfg| {
                cfg.handshake_timeout = Duration::from_secs(30);
                cfg.shutdown_timeout = Duration::from_millis(200);
                cfg.max_restarts = 0;
            })
            .await;

        supervisor.start_all().await.expect("start_all 应成功");
        // 等待子进程出现（Starting 且 pid 已设置）。
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            if registry.get("stopper").and_then(|r| r.pid).is_some() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "子进程未启动");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // 无连接：stop 应直接 kill，不等待 30s 握手超时。
        let stopped_pid = registry.get("stopper").and_then(|r| r.pid);
        let started = std::time::Instant::now();
        supervisor.stop("stopper").await.expect("stop 应成功");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "无连接 stop 应在秒级内完成（直接 kill）"
        );

        let record = registry.get("stopper").unwrap();
        assert_eq!(record.state, PluginState::Stopped);
        assert_eq!(record.pid, None, "停止后应清空 pid");

        // 残留验证：被 stop 的子进程必须已消失（#2/#5 无残留保障）。
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Some(pid) = stopped_pid {
            assert!(!process_alive(pid), "被停止的子进程 {pid} 不得残留存活");
        }

        // stop 未注册插件 → 中文错误。
        let err = supervisor.stop("stranger").await.unwrap_err();
        assert!(err.to_string().contains("插件未注册"), "错误信息：{err}");
    }

    /// smoke 4：start_all 的插件目录不存在 → 中文错误。
    #[tokio::test]
    async fn smoke_missing_plugins_dir_errors() {
        let dir = tempdir().unwrap();
        let sockets = dir.path().join("sockets");
        let (supervisor, _registry) =
            make_supervisor(dir.path().join("no-such-dir"), sockets.clone(), |_cfg| {}).await;
        let err = supervisor.start_all().await.unwrap_err();
        assert!(
            err.to_string().contains("插件目录不存在"),
            "错误信息：{err}"
        );
    }

    // -----------------------------------------------------------------------
    // 生命周期缺陷回归（#1 崩溃重启 / #2 停止不复活 / #4 稳定窗口预算）
    // -----------------------------------------------------------------------

    /// 读取客户端的一个完整应答帧（粘包感知）。
    async fn read_client_frame(
        stream: &mut UnixStream,
        buffer: &mut Vec<u8>,
    ) -> Option<WireMessage> {
        loop {
            if let Some((consumed, msg)) = decode_full_read(buffer) {
                let rest = buffer.split_off(consumed);
                *buffer = rest;
                return Some(msg);
            }
            let mut chunk = [0u8; 4096];
            match stream.read(&mut chunk).await {
                Ok(0) => return None,
                Ok(n) => buffer.extend_from_slice(&chunk[..n]),
                Err(_) => return None,
            }
        }
    }

    /// 试一次连接 + Hello + 读 Ack 确定成功；成功返回持有连接的流。
    async fn try_handshake(socket: &Path, name: &str) -> Option<UnixStream> {
        let mut stream = UnixStream::connect(socket).await.ok()?;
        let hello = WireMessage::Hello(Hello {
            name: name.to_string(),
            version: "0.1.0".to_string(),
            subscribe: vec![],
        });
        let frame = encode_frame(&hello).ok()?;
        stream.write_all(&frame).await.ok()?;
        let mut buf = Vec::new();
        match tokio::time::timeout(
            Duration::from_millis(500),
            read_client_frame(&mut stream, &mut buf),
        )
        .await
        {
            Ok(Some(WireMessage::HelloAck { ok: true, .. })) => Some(stream),
            _ => None,
        }
    }

    /// 等待成功 attach（读到 HelloAck ok）次数达到 target。
    async fn wait_attach_count(count: &Arc<std::sync::Mutex<u32>>, target: u32) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let now = *count.lock().unwrap();
            if now >= target {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "等待第 {target} 次 attach 超时，当前 {now}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// 等待 restart_count 变为 target。
    async fn wait_restart_count(registry: &PluginRegistry, name: &str, target: u32) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let now = registry.get(name).map(|r| r.restart_count).unwrap_or(0);
            if now == target {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "等待 restart_count=={target} 超时，当前 {now}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// 用 `ps` 判断进程是否存活。
    fn process_alive(pid: u32) -> bool {
        match std::process::Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "pid="])
            .output()
        {
            Ok(out) => !out.stdout.is_empty(),
            Err(_) => true, // 无法查询时保守视为存活
        }
    }

    /// 假插件客户端：跟随状态机反复连接。Starting 时握手（计入 attach 次数），
    /// Running 时保持连接，重启（Starting 且已有旧连接）时丢弃旧连接等待下一轮，
    /// Crashed / Stopped 退出。
    async fn run_fake_plugin_client(
        socket: PathBuf,
        name: String,
        registry: Arc<PluginRegistry>,
        attach_count: Arc<std::sync::Mutex<u32>>,
    ) {
        let mut held: Option<UnixStream> = None;
        loop {
            let state = registry.get(&name).map(|r| r.state);
            match state {
                Some(PluginState::Running) => {
                    if held.is_none() {
                        // 理论上 attach 成功才有 Running，此时 held 已设；异常丢失则补连。
                        if let Some(stream) = try_handshake(&socket, &name).await {
                            *attach_count.lock().unwrap() += 1;
                            held = Some(stream);
                        } else {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
                Some(PluginState::Starting) => {
                    if held.is_none() {
                        // 实例就绪：握手完成本轮 attach（首轮与崩溃重启都由此进入）。
                        if let Some(stream) = try_handshake(&socket, &name).await {
                            *attach_count.lock().unwrap() += 1;
                            held = Some(stream);
                        } else {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    } else {
                        // 崩溃重启中：丢弃旧连接，等待下一轮握手（重新 attach）。
                        // 此刻状态只能是从 Running 归口回来的 Starting，
                        // 旧 server 的 handler 才被允许清理——不会擦到新连接。
                        held = None;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
                Some(PluginState::Crashed) | Some(PluginState::Stopped) => break,
                _ => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }

    /// 端到端：#1（运行期崩溃 → 自动重启 → 再次 Running）+ #4（attach 不清零
    /// 预算，短命插件最终 Crashed）。
    ///
    /// 插件脚本 `sleep 0.5; exit 9`：attach 后存活 0.5s 即崩溃；客户端进程内
    /// 扮演插件反复握手/保持连接。
    #[tokio::test]
    async fn lifecycle_crash_restart_reexecutes_then_crashed() {
        let dir = tempdir().unwrap();
        write_plugin(dir.path(), "flappy", "#!/bin/sh\nsleep 0.5\nexit 9\n");
        let sockets = dir.path().join("sockets");
        let (supervisor, registry) =
            make_supervisor(dir.path().to_path_buf(), sockets.clone(), |cfg| {
                cfg.handshake_timeout = Duration::from_secs(2);
                cfg.max_restarts = 1;
                cfg.restart_base_backoff = Duration::from_millis(50);
                cfg.restart_max_backoff = Duration::from_millis(100);
                cfg.shutdown_timeout = Duration::from_millis(300);
                // stable_window 保持默认 60s：短命插件不触发预算清零。
            })
            .await;

        let socket = sockets.join("flappy.sock");
        let attach_count = Arc::new(std::sync::Mutex::new(0u32));
        let client = tokio::spawn(run_fake_plugin_client(
            socket.clone(),
            "flappy".to_string(),
            registry.clone(),
            attach_count.clone(),
        ));

        supervisor.start_all().await.expect("start_all 应成功");

        // 第 1 轮 attach：state → Running。
        wait_attach_count(&attach_count, 1).await;
        wait_state(&registry, "flappy", PluginState::Running).await;
        assert_eq!(
            registry.get("flappy").unwrap().restart_count,
            0,
            "首次 attach 前尚无崩溃"
        );

        // 0.5s 后子进程崩溃 → 监控判定 Running → Starting（#1 状态归口）→
        // 重新 spawn → 客户端重连 attach 成功 → 再次 Running。
        wait_attach_count(&attach_count, 2).await;
        wait_state(&registry, "flappy", PluginState::Running).await;
        assert_eq!(
            registry.get("flappy").unwrap().restart_count,
            1,
            "第二次 Running 时 attach 不应清零预算（restart_count 应保持 1，即 #4 语义）"
        );

        // 第 2 次崩溃：预算（max_restarts=1）耗尽 → Crashed，且不再拉起。
        wait_state(&registry, "flappy", PluginState::Crashed).await;
        let record = registry.get("flappy").unwrap();
        assert_eq!(record.state, PluginState::Crashed);
        assert_eq!(record.restart_count, 1, "max_restarts=1：耗尽后计数为 1");
        assert!(
            record
                .last_error
                .as_deref()
                .unwrap_or("")
                .contains("重启次数已达上限"),
            "last_error 应为中文上限提示，实际 {:?}",
            record.last_error
        );
        assert_eq!(record.pid, None, "Crashed 后不得残留已死 pid（#5）");

        // 再观察 500ms：绝不再拉起。
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            registry.get("flappy").unwrap().state,
            PluginState::Crashed,
            "预算耗尽后不得再拉起"
        );

        client.abort();
        let _ = supervisor.shutdown_all().await;
    }

    /// #2：stop() 在启动窗口内（spawn 任务尚未调度）调用，停止意图不得丢失，
    /// 插件不得复活、不得残留子进程。
    #[tokio::test]
    async fn stop_during_startup_window_never_revives() {
        let dir = tempdir().unwrap();
        write_plugin(dir.path(), "stubborn", "#!/bin/sh\nexec sleep 100\n");
        let sockets = dir.path().join("sockets");
        let (supervisor, registry) =
            make_supervisor(dir.path().to_path_buf(), sockets.clone(), |cfg| {
                cfg.handshake_timeout = Duration::from_secs(2);
                cfg.shutdown_timeout = Duration::from_millis(200);
                cfg.max_restarts = 0;
            })
            .await;

        supervisor.start_all().await.expect("start_all 应成功");
        // 启动后立即（不等待 spawn 调度到位）stop()：停止意图必须保留。
        supervisor.stop("stubborn").await.expect("stop 应成功");

        assert_eq!(
            registry.get("stubborn").unwrap().state,
            PluginState::Stopped,
            "stop 应立即置 Stopped"
        );

        // 等待足够让 spawn/监控任务真正调度的时间：插件不得复活、无 pid 残留。
        tokio::time::sleep(Duration::from_millis(700)).await;
        let record = registry.get("stubborn").unwrap();
        assert_eq!(record.state, PluginState::Stopped, "stop 后插件不得复活");
        assert_eq!(record.pid, None, "停止后不得残留子进程 pid");

        // 再过 500ms 依然 Stopped。
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            registry.get("stubborn").unwrap().state,
            PluginState::Stopped,
            "插件不得在 stop 之后复活"
        );

        let _ = supervisor.shutdown_all().await;
    }

    /// #4：稳定运行超过 stable_window 后崩溃预算重新计数；attach 本身不清零。
    /// 用 SIGKILL 制造两次崩溃，第三次 attach 成功证明预算已被稳定窗口重置。
    #[tokio::test]
    async fn stable_window_resets_crash_budget() {
        let dir = tempdir().unwrap();
        write_plugin(dir.path(), "stable", "#!/bin/sh\nexec sleep 100\n");
        let sockets = dir.path().join("sockets");
        let (supervisor, registry) =
            make_supervisor(dir.path().to_path_buf(), sockets.clone(), |cfg| {
                cfg.handshake_timeout = Duration::from_secs(2);
                cfg.max_restarts = 1;
                cfg.restart_base_backoff = Duration::from_millis(50);
                cfg.restart_max_backoff = Duration::from_millis(100);
                cfg.shutdown_timeout = Duration::from_millis(300);
                cfg.stable_window = Duration::from_millis(150);
            })
            .await;

        let socket = sockets.join("stable.sock");
        let attach_count = Arc::new(std::sync::Mutex::new(0u32));
        let client = tokio::spawn(run_fake_plugin_client(
            socket.clone(),
            "stable".to_string(),
            registry.clone(),
            attach_count.clone(),
        ));

        supervisor.start_all().await.expect("start_all 应成功");

        // 杀掉第 1 个子进程 → 崩溃 1 次（restart_count → 1）→ 重新 attach。
        wait_attach_count(&attach_count, 1).await;
        let pid1 = registry.get("stable").unwrap().pid.expect("应已有 pid");
        std::process::Command::new("kill")
            .args(["-9", &pid1.to_string()])
            .status()
            .expect("kill 应成功");
        wait_attach_count(&attach_count, 2).await;
        wait_restart_count(&registry, "stable", 1).await;

        // 稳定存活超过 stable_window（150ms）→ 预算清零（restart_count → 0）。
        wait_restart_count(&registry, "stable", 0).await;

        // 再次杀掉子进程：预算已重置，应允许重启（attach #3）而非直接 Crashed。
        let pid2 = registry.get("stable").unwrap().pid.expect("应已有 pid");
        std::process::Command::new("kill")
            .args(["-9", &pid2.to_string()])
            .status()
            .expect("kill 应成功");
        wait_attach_count(&attach_count, 3).await;
        assert_eq!(
            registry.get("stable").unwrap().state,
            PluginState::Running,
            "稳定窗口清零预算后，崩溃应可再次重启（而非直接 Crashed）"
        );
        assert_eq!(
            registry.get("stable").unwrap().restart_count,
            1,
            "第三次 attach 前应已记录一次重启（attach 不清零）"
        );

        client.abort();
        let _ = supervisor.shutdown_all().await;
    }
}
