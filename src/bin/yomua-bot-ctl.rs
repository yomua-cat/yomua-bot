//! yomua-bot-ctl —— yomua-bot 控制客户端（正式版本）。
//!
//! 通过 Unix 域套接字向运行中的 yomua-bot 发送控制命令。
//!
//! 支持两种模式：
//! - **One-shot 模式**：`yomua-bot-ctl <命令>` 执行单条命令后退出
//! - **交互模式**：`yomua-bot-ctl`（无命令）进入 REPL，可连续执行多条命令

use std::path::PathBuf;
use std::time::Duration;

use rustyline::config::Configurer;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::timeout;

// ============================================================================
// Section 1: Command Specification & Registry
// ============================================================================

/// 命令路径（如 `["character", "show"]`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandPath(Vec<&'static str>);

impl CommandPath {
    fn matches(&self, input: &[&str]) -> bool {
        self.0.len() == input.len() && self.0.iter().zip(input.iter()).all(|(a, b)| *a == *b)
    }
}

/// 命令选项的枚举形式，支持所有命令的选项联合。
#[derive(Debug, Clone, Default)]
pub struct CommandOptions {
    // Common
    pub raw: bool,
    // character list
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    // character show <id>
    pub id: Option<i64>,
    // state / character operations
    pub character_id: Option<i64>,
    pub conversation_id: Option<i64>,
    // state set
    pub mood: Option<f64>,
    pub energy: Option<f64>,
    pub stress: Option<f64>,
    pub activity: Option<String>,
    // blacklist
    pub qq_id: Option<String>,
    pub duration: Option<String>,
    pub reason: Option<String>,
    // memory
    pub memory_id: Option<i64>,
    pub memory_type: Option<String>,
}

impl CommandOptions {
    #[allow(dead_code)]
    fn with_id(id: i64) -> Self {
        Self {
            id: Some(id),
            ..Default::default()
        }
    }
}

/// 命令描述与元数据。
#[derive(Debug, Clone)]
pub struct CommandSpec {
    pub usage: &'static str,
    pub description: &'static str,
    pub group: &'static str,
}

/// 命令能力状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandCapability {
    /// 正式支持：server 端已注册 handler。
    Supported,
    /// 暂不可用：CLI 可以识别，但 server 端尚未实现。
    Unavailable,
}

/// 内置命令定义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinCommand {
    Status,
    ReloadConfig,
    Shutdown,
    Help,
    CharacterList,
    CharacterShow,
    CharacterCardList,
    CharacterCardEnable,
    CharacterCardDisable,
    CharacterCardReassign,
    ConversationList,
    SessionDeleteAll,
    SessionDeleteCharacter,
    CharacterStateGet,
    CharacterStateSet,
    CharacterStateRefresh,
    StateGet,
    StateSet,
    StateRefresh,
    BlacklistList,
    BlacklistAdd,
    BlacklistRemove,
    BlacklistBan,
    MemoryList,
    MemoryDelete,
}

impl BuiltinCommand {
    fn path(&self) -> CommandPath {
        match self {
            Self::Status => CommandPath(vec!["status"]),
            Self::ReloadConfig => CommandPath(vec!["reload-config"]),
            Self::Shutdown => CommandPath(vec!["shutdown"]),
            Self::Help => CommandPath(vec!["help"]),
            Self::CharacterList => CommandPath(vec!["character", "list"]),
            Self::CharacterShow => CommandPath(vec!["character", "show"]),
            Self::CharacterCardList => CommandPath(vec!["character", "card", "list"]),
            Self::CharacterCardEnable => CommandPath(vec!["character", "card", "enable"]),
            Self::CharacterCardDisable => CommandPath(vec!["character", "card", "disable"]),
            Self::CharacterCardReassign => CommandPath(vec!["character", "card", "reassign"]),
            Self::ConversationList => CommandPath(vec!["conversation", "list"]),
            Self::SessionDeleteAll => CommandPath(vec!["session", "delete-all"]),
            Self::SessionDeleteCharacter => CommandPath(vec!["session", "delete-character"]),
            Self::CharacterStateGet => CommandPath(vec!["character", "state", "get"]),
            Self::CharacterStateSet => CommandPath(vec!["character", "state", "set"]),
            Self::CharacterStateRefresh => CommandPath(vec!["character", "state", "refresh"]),
            // 兼容旧语法
            Self::StateGet => CommandPath(vec!["state", "get"]),
            Self::StateSet => CommandPath(vec!["state", "set"]),
            Self::StateRefresh => CommandPath(vec!["state", "refresh"]),
            Self::BlacklistList => CommandPath(vec!["blacklist", "list"]),
            Self::BlacklistAdd => CommandPath(vec!["blacklist", "add"]),
            Self::BlacklistRemove => CommandPath(vec!["blacklist", "remove"]),
            Self::BlacklistBan => CommandPath(vec!["blacklist", "ban"]),
            Self::MemoryList => CommandPath(vec!["memory", "list"]),
            Self::MemoryDelete => CommandPath(vec!["memory", "delete"]),
        }
    }

    /// 返回命令能力状态。
    fn capability(&self) -> CommandCapability {
        match self {
            // 已在 server 端 CommandRegistry 注册的命令
            Self::Status => CommandCapability::Supported,
            Self::ReloadConfig => CommandCapability::Supported,
            Self::Shutdown => CommandCapability::Supported,
            Self::Help => CommandCapability::Supported,
            Self::CharacterList => CommandCapability::Supported,
            Self::CharacterShow => CommandCapability::Supported,
            Self::CharacterCardList => CommandCapability::Supported,
            Self::CharacterCardEnable => CommandCapability::Supported,
            Self::CharacterCardDisable => CommandCapability::Supported,
            Self::CharacterCardReassign => CommandCapability::Supported,
            Self::ConversationList => CommandCapability::Supported,
            Self::SessionDeleteAll => CommandCapability::Supported,
            Self::SessionDeleteCharacter => CommandCapability::Supported,
            Self::CharacterStateGet => CommandCapability::Supported,
            Self::CharacterStateSet => CommandCapability::Supported,
            Self::CharacterStateRefresh => CommandCapability::Supported,
            // 兼容旧语法
            Self::StateGet => CommandCapability::Supported,
            Self::StateSet => CommandCapability::Supported,
            Self::StateRefresh => CommandCapability::Supported,
            Self::BlacklistList => CommandCapability::Supported,
            Self::BlacklistAdd => CommandCapability::Supported,
            Self::BlacklistRemove => CommandCapability::Supported,
            Self::BlacklistBan => CommandCapability::Supported,
            Self::MemoryList => CommandCapability::Supported,
            Self::MemoryDelete => CommandCapability::Supported,
        }
    }

    fn spec(&self) -> CommandSpec {
        match self {
            Self::Status => CommandSpec {
                usage: "status [--raw]",
                description: "查看运行状态",
                group: "系统",
            },
            Self::ReloadConfig => CommandSpec {
                usage: "reload-config",
                description: "重新加载配置文件",
                group: "系统",
            },
            Self::Shutdown => CommandSpec {
                usage: "shutdown",
                description: "关闭 Runtime",
                group: "系统",
            },
            Self::Help => CommandSpec {
                usage: "help",
                description: "显示帮助",
                group: "系统",
            },
            Self::CharacterList => CommandSpec {
                usage: "character list [--limit N] [--offset N]",
                description: "列出所有角色卡",
                group: "角色",
            },
            Self::CharacterShow => CommandSpec {
                usage: "character show <id> [--raw]",
                description: "显示角色详情",
                group: "角色",
            },
            Self::CharacterCardList => CommandSpec {
                usage: "character card list [--raw]",
                description: "列出所有角色卡（含启用状态）",
                group: "角色",
            },
            Self::CharacterCardEnable => CommandSpec {
                usage: "character card enable <id>",
                description: "启用角色卡",
                group: "角色",
            },
            Self::CharacterCardDisable => CommandSpec {
                usage: "character card disable <id>",
                description: "停用角色卡",
                group: "角色",
            },
            Self::CharacterCardReassign => CommandSpec {
                usage: "character card reassign <old-id> <new-id>",
                description: "修改角色卡 ID",
                group: "角色",
            },
            Self::ConversationList => CommandSpec {
                usage: "session list",
                description: "列出所有会话",
                group: "会话",
            },
            Self::SessionDeleteAll => CommandSpec {
                usage: "session delete-all",
                description: "删除所有会话",
                group: "会话",
            },
            Self::SessionDeleteCharacter => CommandSpec {
                usage: "session delete-character <character-id>",
                description: "删除指定角色的所有会话",
                group: "会话",
            },
            Self::CharacterStateGet => CommandSpec {
                usage: "character state get <character-id> [--conversation <id>] [--raw]",
                description: "查询角色状态（全局/会话级）",
                group: "状态",
            },
            Self::CharacterStateSet => CommandSpec {
                usage: "character state set --character <id> [--conversation <id>] [--energy N] [--stress N] [--mood N] [--activity <text>]",
                description: "修改角色状态",
                group: "状态",
            },
            Self::CharacterStateRefresh => CommandSpec {
                usage: "character state refresh [--raw]",
                description: "刷新所有角色的全局状态时间戳",
                group: "状态",
            },
            // 兼容旧语法
            Self::StateGet => CommandSpec {
                usage: "state get <character-id> [--conversation <id>]",
                description: "查询角色状态",
                group: "状态",
            },
            Self::StateSet => CommandSpec {
                usage: "state set --character <id> [--conversation <id>] [--energy N] [--stress N] [--mood N] [--activity <text>]",
                description: "修改角色状态",
                group: "状态",
            },
            Self::StateRefresh => CommandSpec {
                usage: "state refresh",
                description: "刷新全局状态时间戳",
                group: "状态",
            },
            Self::BlacklistList => CommandSpec {
                usage: "blacklist list",
                description: "查看黑名单",
                group: "黑名单",
            },
            Self::BlacklistAdd => CommandSpec {
                usage: "blacklist add <qq-id>",
                description: "添加永久黑名单",
                group: "黑名单",
            },
            Self::BlacklistRemove => CommandSpec {
                usage: "blacklist remove <qq-id>",
                description: "移除黑名单",
                group: "黑名单",
            },
            Self::BlacklistBan => CommandSpec {
                usage: "blacklist ban <qq-id> [--duration <dur>] [--reason <text>]",
                description: "临时拉黑（30m/1h/6h/12h/1d/7d）",
                group: "黑名单",
            },
            Self::MemoryList => CommandSpec {
                usage: "memory list [--character <id>] [--conversation <id>] [--type <type>] [--limit N]",
                description: "查询记忆",
                group: "记忆",
            },
            Self::MemoryDelete => CommandSpec {
                usage: "memory delete <memory-id>",
                description: "删除记忆",
                group: "记忆",
            },
        }
    }

    fn from_path(path: &[String]) -> Option<Self> {
        let path_refs: Vec<&str> = path.iter().map(String::as_str).collect();
        Self::all().find(|c| c.path().matches(path_refs.as_slice()))
    }

    fn all() -> impl Iterator<Item = Self> {
        [
            Self::Status,
            Self::ReloadConfig,
            Self::Shutdown,
            Self::Help,
            Self::CharacterList,
            Self::CharacterShow,
            Self::CharacterCardList,
            Self::CharacterCardEnable,
            Self::CharacterCardDisable,
            Self::CharacterCardReassign,
            Self::ConversationList,
            Self::SessionDeleteAll,
            Self::SessionDeleteCharacter,
            Self::CharacterStateGet,
            Self::CharacterStateSet,
            Self::CharacterStateRefresh,
            // 兼容旧语法
            Self::StateGet,
            Self::StateSet,
            Self::StateRefresh,
            Self::BlacklistList,
            Self::BlacklistAdd,
            Self::BlacklistRemove,
            Self::BlacklistBan,
            Self::MemoryList,
            Self::MemoryDelete,
        ]
        .into_iter()
    }
}

// ============================================================================
// Section 2: Parsed Command
// ============================================================================

/// 解析后的命令。
#[derive(Debug, Clone)]
pub struct ParsedCommand {
    pub path: Vec<String>,
    pub args: Vec<String>,
    pub opts: CommandOptions,
}

impl ParsedCommand {
    fn parse(tokens: &[&str]) -> Result<Self, String> {
        let mut path = Vec::new();
        let mut args = Vec::new();
        let mut opts = CommandOptions::default();

        let mut i = 0;
        while i < tokens.len() {
            let tok = tokens[i];
            if let Some(key) = tok.strip_prefix("--") {
                match key {
                    "raw" => opts.raw = true,
                    "limit" => {
                        i += 1;
                        opts.limit = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--limit 需要一个数值".to_string())?,
                        );
                    }
                    "offset" => {
                        i += 1;
                        opts.offset = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--offset 需要一个数值".to_string())?,
                        );
                    }
                    "id" => {
                        i += 1;
                        opts.id = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--id 需要一个正整数".to_string())?,
                        );
                    }
                    "character" | "character-id" => {
                        i += 1;
                        opts.character_id = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--character 需要一个正整数".to_string())?,
                        );
                    }
                    "conversation" | "conversation-id" => {
                        i += 1;
                        opts.conversation_id = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--conversation 需要一个正整数".to_string())?,
                        );
                    }
                    "mood" => {
                        i += 1;
                        opts.mood = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--mood 需要一个数值".to_string())?,
                        );
                    }
                    "energy" => {
                        i += 1;
                        opts.energy = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--energy 需要一个数值".to_string())?,
                        );
                    }
                    "stress" => {
                        i += 1;
                        opts.stress = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--stress 需要一个数值".to_string())?,
                        );
                    }
                    "activity" => {
                        i += 1;
                        opts.activity = tokens.get(i).map(|v| v.to_string());
                    }
                    "qq-id" => {
                        i += 1;
                        opts.qq_id = tokens.get(i).map(|v| v.to_string());
                    }
                    "duration" => {
                        i += 1;
                        opts.duration = tokens.get(i).map(|v| v.to_string());
                    }
                    "reason" => {
                        i += 1;
                        opts.reason = tokens.get(i).map(|v| v.to_string());
                    }
                    "memory-id" => {
                        i += 1;
                        opts.memory_id = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--memory-id 需要一个正整数".to_string())?,
                        );
                    }
                    "type" | "memory-type" => {
                        i += 1;
                        opts.memory_type = tokens.get(i).map(|v| v.to_string());
                    }
                    other => {
                        return Err(format!("未知选项: --{other}"));
                    }
                }
            } else if path.len() < 2 {
                path.push(tok.to_string());
            } else {
                args.push(tok.to_string());
            }
            i += 1;
        }

        Ok(ParsedCommand { path, args, opts })
    }
}

// ============================================================================
// Section 3: Control Protocol Types
// ============================================================================

#[derive(Debug, serde::Deserialize)]
struct ControlErrorDetail {
    code: String,
    message: String,
}

#[derive(Debug, serde::Deserialize)]
struct ControlResponse {
    id: u64,
    ok: bool,
    #[serde(default)]
    data: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<ControlErrorDetail>,
}

impl ControlResponse {
    fn error_message(&self) -> String {
        self.error
            .as_ref()
            .map(|e| e.message.as_str())
            .unwrap_or("未知错误")
            .to_string()
    }
}

// ============================================================================
// Section 4: Output Formatter
// ============================================================================

pub struct OutputFormatter {
    raw: bool,
    /// CLI 启动时刻（单调时钟），用于计算 ctl 运行时长。
    ctl_started_at: Option<std::time::Instant>,
}

impl OutputFormatter {
    fn new(raw: bool, ctl_started_at: Option<std::time::Instant>) -> Self {
        Self {
            raw,
            ctl_started_at,
        }
    }

    fn format_and_print(&self, resp: &ControlResponse) {
        if self.raw {
            self.print_raw(resp);
        } else {
            self.print_human(resp);
        }
    }

    fn print_raw(&self, resp: &ControlResponse) {
        if resp.ok {
            if let Some(d) = &resp.data {
                let s = serde_json::to_string_pretty(d).unwrap_or_else(|_| d.to_string());
                println!("{s}");
            } else {
                println!("{{\"ok\": true}}");
            }
        } else {
            let code = resp
                .error
                .as_ref()
                .map(|e| e.code.as_str())
                .unwrap_or("UNKNOWN");
            eprintln!("错误[{code}]: {}", resp.error_message());
        }
    }

    fn print_human(&self, resp: &ControlResponse) {
        if !resp.ok {
            let code = resp
                .error
                .as_ref()
                .map(|e| e.code.as_str())
                .unwrap_or("UNKNOWN");
            eprintln!("错误[{code}]: {}", resp.error_message());
            return;
        }

        let data = match &resp.data {
            Some(d) => d,
            None => {
                println!("操作成功");
                return;
            }
        };

        if let Some(obj) = data.as_object() {
            self.print_object(obj);
        } else {
            if let Ok(s) = serde_json::to_string_pretty(data) {
                println!("{s}");
            } else {
                println!("{data}");
            }
        }
    }

    /// 格式化时长（秒）为人类可读字符串。
    fn format_uptime(seconds: u64) -> String {
        let hours = seconds / 3600;
        let minutes = (seconds % 3600) / 60;
        let secs = seconds % 60;
        if hours > 0 {
            format!("{} 小时 {} 分 {} 秒", hours, minutes, secs)
        } else if minutes > 0 {
            format!("{} 分 {} 秒", minutes, secs)
        } else {
            format!("{} 秒", secs)
        }
    }

    /// 格式化字节数为人类可读字符串。
    fn format_memory(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;
        if bytes >= GB {
            format!("{:.1} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.1} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.1} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }

    fn print_object(&self, obj: &serde_json::Map<String, serde_json::Value>) {
        // status
        if obj.get("system_time").is_some() && obj.get("main_uptime_secs").is_some() {
            // 完整 status（新版）
            let system_time = obj
                .get("system_time")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            let main_uptime = obj
                .get("main_uptime_secs")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let active = obj
                .get("active_sessions")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let total = obj
                .get("total_sessions")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let memory = obj.get("memory_bytes");
            let onebot = obj
                .get("onebot_status")
                .and_then(|v| v.as_str())
                .unwrap_or("未知");
            let data_dir = obj.get("data_dir").and_then(|v| v.as_str()).unwrap_or("?");
            let log_level = obj.get("log_level").and_then(|v| v.as_str()).unwrap_or("?");
            let plugin_count = obj
                .get("plugin_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            // 格式化系统时间（RFC3339 -> YYYY-MM-DD HH:MM:SS）
            let system_time_short = system_time
                .split('T')
                .next()
                .map(|s| {
                    if let Some(time_part) = s.split('+').next() {
                        time_part.replace("T", " ")
                    } else {
                        s.replace("T", " ")
                    }
                })
                .unwrap_or_else(|| system_time.to_string());

            // 格式化主程序运行时长
            let uptime_str = Self::format_uptime(main_uptime);

            // 格式化内存
            let memory_str = memory
                .and_then(|v| v.as_u64())
                .map(Self::format_memory)
                .unwrap_or_else(|| "无法获取".to_string());

            // ctl 运行时长（本地计算）
            let ctl_uptime_str = self
                .ctl_started_at
                .map(|started| {
                    let elapsed = started.elapsed().as_secs_f64();
                    format!("{:.1} 秒", elapsed)
                })
                .unwrap_or_else(|| "无法获取".to_string());

            println!("运行状态");
            println!("  系统时间：{}", system_time_short);
            println!("  主程序运行时长：{}", uptime_str);
            println!("  Ctl 运行时长：{}", ctl_uptime_str);
            println!("  活跃会话：{}", active);
            println!("  全部会话：{}", total);
            println!("  内存占用：{}", memory_str);
            println!("  OneBot：{}", onebot);
            println!("  日志级别：{}", log_level);
            println!("  插件数：{}", plugin_count);
            println!("  数据目录：{}", data_dir);
            return;
        }

        // 旧版 status（兼容）
        if let (Some(state), Some(data_dir), Some(log_level)) = (
            obj.get("adapter_state"),
            obj.get("data_dir"),
            obj.get("log_level"),
        ) {
            println!("运行状态");
            println!("  OneBot: {}", state);
            println!("  数据目录: {}", data_dir);
            println!("  日志级别: {}", log_level);
            if let Some(pc) = obj.get("plugin_count") {
                println!("  插件数: {}", pc);
            }
            return;
        }

        // character list (including character_card_list)
        if let Some(arr) = obj.get("characters").and_then(|v| v.as_array()) {
            println!("角色列表（共 {} 个）", arr.len());
            for c in arr {
                if let (Some(name), Some(id), enabled) = (
                    c.get("name").and_then(|v| v.as_str()),
                    c.get("id").and_then(|v| v.as_i64()),
                    c.get("enabled").and_then(|v| v.as_bool()),
                ) {
                    let status = if enabled.unwrap_or(true) {
                        "启用"
                    } else {
                        "停用"
                    };
                    println!("  [{}] {} ({})", id, name, status);
                } else if let (Some(name), Some(id)) = (
                    c.get("name").and_then(|v| v.as_str()),
                    c.get("id").and_then(|v| v.as_i64()),
                ) {
                    println!("  [{}] {}", id, name);
                } else {
                    println!("  {}", c);
                }
            }
            return;
        }

        // session list (both "conversations" and "sessions")
        if let Some(arr) = obj
            .get("conversations")
            .or(obj.get("sessions"))
            .and_then(|v| v.as_array())
        {
            println!("会话列表（共 {} 个）", arr.len());
            for c in arr {
                if let (Some(id), Some(ext)) = (
                    c.get("id").and_then(|v| v.as_i64()),
                    c.get("external_id").and_then(|v| v.as_str()),
                ) {
                    let typ = c
                        .get("conversation_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    println!("  [{}] {} {}", id, typ, ext);
                } else {
                    println!("  {}", c);
                }
            }
            return;
        }

        // state
        if let (Some(cid), Some(state)) = (
            obj.get("character_id").and_then(|v| v.as_i64()),
            obj.get("character_state"),
        ) {
            println!("角色状态（角色ID: {}）", cid);
            if state.is_null() {
                println!("  （无状态数据）");
            } else if let Some(s) = state.as_object() {
                if let Some(e) = s.get("energy") {
                    println!("  Energy: {}", e);
                }
                if let Some(ss) = s.get("stress") {
                    println!("  Stress: {}", ss);
                }
                if let Some(act) = s.get("current_activity") {
                    println!(
                        "  Activity: {}",
                        if act.is_null() {
                            "(无)"
                        } else {
                            act.as_str().unwrap_or("")
                        }
                    );
                }
                if let Some(u) = s.get("last_updated") {
                    println!("  更新时间: {}", u);
                }
            }
            if let Some(cs) = obj.get("conversation_state") {
                if !cs.is_null() {
                    if let Some(cso) = cs.as_object() {
                        if let Some(e) = cso.get("energy") {
                            println!("  会话 Energy: {}", e);
                        }
                        if let Some(ss) = cso.get("stress") {
                            println!("  会话 Stress: {}", ss);
                        }
                    }
                }
            }
            if let Some(mood) = obj.get("mood") {
                if !mood.is_null() {
                    if let Some(mv) = mood.get("value") {
                        println!("  Mood: {}", mv);
                    }
                }
            }
            return;
        }

        // blacklist
        if let Some(arr) = obj.get("blacklist").and_then(|v| v.as_array()) {
            println!("黑名单（共 {} 项）", arr.len());
            println!("  ID    类型    状态    过期时间");
            println!("  {}", "-".repeat(40));
            for item in arr {
                if let Some(o) = item.as_object() {
                    let id = o.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
                    let target = o.get("target").and_then(|v| v.as_str()).unwrap_or("?");
                    let target_type = o
                        .get("target_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("user");
                    let status = o.get("status").and_then(|v| v.as_str()).unwrap_or("?");
                    let expires_at = o.get("expires_at");
                    let reason = o.get("reason").and_then(|v| v.as_str());
                    print!("  {}  {}  {}", id, target_type, status);
                    if let Some(exp) = expires_at {
                        print!("  {}", exp);
                    } else {
                        print!("  -");
                    }
                    println!();
                    println!("       目标: {}", target);
                    if let Some(r) = reason {
                        println!("       原因: {}", r);
                    }
                } else {
                    println!("  {}", item);
                }
            }
            return;
        }

        // session delete result
        if let Some(deleted) = obj.get("deleted").and_then(|v| v.as_u64()) {
            println!("已删除会话：{}", deleted);
            return;
        }

        // memory delete result
        if let Some(mid) = obj.get("memory_id").and_then(|v| v.as_i64()) {
            println!("已删除记忆：{}", mid);
            return;
        }

        // memory list
        if let Some(arr) = obj.get("memories").and_then(|v| v.as_array()) {
            println!("记忆列表（共 {} 条）", arr.len());
            for m in arr {
                if let Some(o) = m.as_object() {
                    let id = o.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
                    let content = o.get("content").and_then(|v| v.as_str()).unwrap_or("");
                    let mtype = o.get("memory_type").and_then(|v| v.as_str()).unwrap_or("?");
                    let preview: String = content.chars().take(40).collect();
                    println!("  [{}] [{}] {}", id, mtype, preview);
                } else {
                    println!("  {}", m);
                }
            }
            return;
        }

        // commands list (help)
        if let Some(arr) = obj.get("commands").and_then(|v| v.as_array()) {
            println!("可用命令：");
            let mut cmd_items: Vec<(&str, &str)> = Vec::new();
            for cmd in arr {
                if let Some(o) = cmd.as_object() {
                    let name = o.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                    let desc = o.get("description").and_then(|v| v.as_str()).unwrap_or("");
                    cmd_items.push((name, desc));
                }
            }
            for (name, desc) in cmd_items {
                println!("  {:30} — {}", name, desc);
            }
            return;
        }

        // generic fallback: print the whole object
        let raw = serde_json::to_string_pretty(obj).unwrap_or_else(|_| format!("{obj:?}"));
        println!("{raw}");
    }
}

// ============================================================================
// Section 5: Duration Parser
// ============================================================================

pub fn parse_duration(s: &str) -> Result<Option<chrono::DateTime<chrono::Utc>>, String> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(None);
    }

    let unit = s.chars().last().unwrap();
    let value: i64 = s[..s.len() - 1]
        .parse()
        .map_err(|_| format!("无效时长格式: {s}"))?;

    let secs = match unit {
        'm' => value * 60,
        'h' => value * 3600,
        'd' => value * 86400,
        _ => return Err(format!("未知时间单位: {unit}，支持 m/h/d")),
    };

    let expires = chrono::Utc::now() + chrono::Duration::seconds(secs);
    Ok(Some(expires))
}

/// 将 duration 字符串解析为秒数（用于传递给 server）。
fn parse_duration_to_secs(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("时长不能为空".to_string());
    }

    let unit = s.chars().last().unwrap();
    let value: i64 = s[..s.len() - 1]
        .parse()
        .map_err(|_| format!("无效时长格式: {s}"))?;

    if value <= 0 {
        return Err("时长必须为正数".to_string());
    }

    let secs = match unit {
        'm' => value * 60,
        'h' => value * 3600,
        'd' => value * 86400,
        _ => return Err(format!("未知时间单位: {unit}，支持 m/h/d")),
    };

    Ok(secs)
}

// ============================================================================
// Section 6: Control Client
// ============================================================================

fn generate_id() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ (rand_u32() as u64)
}

async fn execute_command(
    socket: &PathBuf,
    builtin: BuiltinCommand,
    opts: &CommandOptions,
    args: &[String],
) -> Result<ControlResponse, String> {
    // 检查命令能力状态
    if builtin.capability() == CommandCapability::Unavailable {
        let spec = builtin.spec();
        return Err(format!(
            "命令 '{}' 当前暂不可用（server 端尚未实现）",
            spec.usage
        ));
    }

    let id = generate_id();
    let (_cmd, params) = build_request(builtin, opts, args, id)?;

    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(|e| format!("无法连接到 control socket {}: {}", socket.display(), e))?;

    let json = serde_json::to_string(&params).map_err(|e| format!("请求序列化失败: {e}"))?;
    stream
        .write_all(json.as_bytes())
        .await
        .map_err(|e| format!("发送请求失败: {e}"))?;

    let mut buf = Vec::with_capacity(8192);
    let read_result = timeout(Duration::from_secs(5), stream.read_buf(&mut buf)).await;

    let n = match read_result {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => return Err(format!("读取响应失败: {e}")),
        Err(_) => return Err("服务器响应超时（5 秒）".to_string()),
    };

    if n == 0 {
        return Err("服务器关闭了连接".to_string());
    }

    buf.truncate(n);
    let resp: ControlResponse =
        serde_json::from_slice(&buf).map_err(|e| format!("无法解析服务器响应: {e}"))?;

    if resp.id != id {
        return Err(format!("响应 ID 不匹配（请求: {id}，响应: {}）", resp.id));
    }

    Ok(resp)
}

fn build_request(
    builtin: BuiltinCommand,
    opts: &CommandOptions,
    args: &[String],
    id: u64,
) -> Result<(&'static str, serde_json::Value), String> {
    use serde_json::json;

    // Extract positional args into opts for commands that need them
    let mut opts = opts.clone();
    match builtin {
        BuiltinCommand::CharacterShow => {
            if let Some(arg) = args.first() {
                if let Ok(v) = arg.parse() {
                    opts.id = Some(v);
                }
            }
        }
        BuiltinCommand::StateGet => {
            if let Some(arg) = args.first() {
                if let Ok(v) = arg.parse() {
                    opts.character_id = Some(v);
                }
            }
        }
        BuiltinCommand::BlacklistAdd
        | BuiltinCommand::BlacklistRemove
        | BuiltinCommand::BlacklistBan => {
            if let Some(arg) = args.first() {
                opts.qq_id = Some(arg.clone());
            }
        }
        BuiltinCommand::MemoryDelete => {
            if let Some(arg) = args.first() {
                if let Ok(v) = arg.parse() {
                    opts.memory_id = Some(v);
                }
            }
        }
        BuiltinCommand::SessionDeleteCharacter => {
            if let Some(arg) = args.first() {
                if let Ok(v) = arg.parse() {
                    opts.character_id = Some(v);
                }
            }
        }
        _ => {}
    }

    match builtin {
        BuiltinCommand::Status => Ok(("status", json!({ "id": id, "cmd": "status" }))),
        BuiltinCommand::ReloadConfig => {
            Ok(("reload-config", json!({ "id": id, "cmd": "reload-config" })))
        }
        BuiltinCommand::Shutdown => Ok(("shutdown", json!({ "id": id, "cmd": "shutdown" }))),
        BuiltinCommand::Help => Ok(("help", json!({ "id": id, "cmd": "help" }))),
        BuiltinCommand::CharacterList => {
            let mut p = serde_json::Map::new();
            if let Some(l) = opts.limit {
                p.insert("limit".to_string(), json!(l));
            }
            if let Some(o) = opts.offset {
                p.insert("offset".to_string(), json!(o));
            }
            Ok((
                "character_list",
                json!({ "id": id, "cmd": "character_list", "params": p }),
            ))
        }
        BuiltinCommand::CharacterShow => {
            let char_id = opts
                .id
                .or(opts.character_id)
                .ok_or_else(|| "需要角色 ID".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("id".to_string(), json!(char_id));
            Ok((
                "character_show",
                json!({ "id": id, "cmd": "character_show", "params": p }),
            ))
        }
        BuiltinCommand::CharacterCardList => Ok((
            "character_card_list",
            json!({ "id": id, "cmd": "character_card_list", "params": {} }),
        )),
        BuiltinCommand::CharacterCardEnable => {
            let char_id = opts
                .id
                .or(opts.character_id)
                .ok_or_else(|| "需要角色 ID".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("id".to_string(), json!(char_id));
            Ok((
                "character_card_enable",
                json!({ "id": id, "cmd": "character_card_enable", "params": p }),
            ))
        }
        BuiltinCommand::CharacterCardDisable => {
            let char_id = opts
                .id
                .or(opts.character_id)
                .ok_or_else(|| "需要角色 ID".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("id".to_string(), json!(char_id));
            Ok((
                "character_card_disable",
                json!({ "id": id, "cmd": "character_card_disable", "params": p }),
            ))
        }
        BuiltinCommand::CharacterCardReassign => {
            if args.len() != 2 {
                return Err("需要 <old-id> <new-id> 两个参数".to_string());
            }
            let old_id: i64 = args[0].parse().map_err(|_| "old-id 必须是整数")?;
            let new_id: i64 = args[1].parse().map_err(|_| "new-id 必须是整数")?;
            let mut p = serde_json::Map::new();
            p.insert("old_id".to_string(), json!(old_id));
            p.insert("new_id".to_string(), json!(new_id));
            Ok((
                "character_card_reassign",
                json!({ "id": id, "cmd": "character_card_reassign", "params": p }),
            ))
        }
        BuiltinCommand::ConversationList => Ok((
            "conversation_list",
            json!({ "id": id, "cmd": "conversation_list" }),
        )),
        BuiltinCommand::SessionDeleteAll => Ok((
            "session_delete_all",
            json!({ "id": id, "cmd": "session_delete_all" }),
        )),
        BuiltinCommand::SessionDeleteCharacter => {
            let mut p = serde_json::Map::new();
            if let Some(cid) = opts.character_id {
                p.insert("character_id".to_string(), json!(cid));
            }
            Ok((
                "session_delete_character",
                json!({ "id": id, "cmd": "session_delete_character", "params": p }),
            ))
        }
        BuiltinCommand::StateGet => {
            let char_id = opts
                .id
                .or(opts.character_id)
                .ok_or_else(|| "需要 --character <角色ID>".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("character_id".to_string(), json!(char_id));
            if let Some(cid) = opts.conversation_id {
                p.insert("conversation_id".to_string(), json!(cid));
            }
            Ok((
                "state-get",
                json!({ "id": id, "cmd": "state-get", "params": p }),
            ))
        }
        BuiltinCommand::StateSet => {
            let char_id = opts
                .character_id
                .ok_or_else(|| "需要 --character <角色ID>".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("character_id".to_string(), json!(char_id));
            if let Some(cid) = opts.conversation_id {
                p.insert("conversation_id".to_string(), json!(cid));
            }
            if let Some(v) = opts.energy {
                p.insert("energy".to_string(), json!(v));
            }
            if let Some(v) = opts.stress {
                p.insert("stress".to_string(), json!(v));
            }
            if let Some(v) = opts.mood {
                p.insert("mood".to_string(), json!(v));
            }
            if let Some(ref v) = opts.activity {
                p.insert("activity".to_string(), json!(v));
            }
            Ok((
                "state-set",
                json!({ "id": id, "cmd": "state-set", "params": p }),
            ))
        }
        BuiltinCommand::StateRefresh => {
            Ok(("state-refresh", json!({ "id": id, "cmd": "state-refresh" })))
        }
        BuiltinCommand::CharacterStateGet => {
            let char_id = opts
                .id
                .or(opts.character_id)
                .ok_or_else(|| "需要 --character <角色ID>".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("character_id".to_string(), json!(char_id));
            if let Some(cid) = opts.conversation_id {
                p.insert("conversation_id".to_string(), json!(cid));
            }
            Ok((
                "state-get",
                json!({ "id": id, "cmd": "state-get", "params": p }),
            ))
        }
        BuiltinCommand::CharacterStateSet => {
            let char_id = opts
                .character_id
                .ok_or_else(|| "需要 --character <角色ID>".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("character_id".to_string(), json!(char_id));
            if let Some(cid) = opts.conversation_id {
                p.insert("conversation_id".to_string(), json!(cid));
            }
            if let Some(v) = opts.energy {
                p.insert("energy".to_string(), json!(v));
            }
            if let Some(v) = opts.stress {
                p.insert("stress".to_string(), json!(v));
            }
            if let Some(v) = opts.mood {
                p.insert("mood".to_string(), json!(v));
            }
            if let Some(ref v) = opts.activity {
                p.insert("activity".to_string(), json!(v));
            }
            Ok((
                "state-set",
                json!({ "id": id, "cmd": "state-set", "params": p }),
            ))
        }
        BuiltinCommand::CharacterStateRefresh => Ok((
            "character_state_refresh",
            json!({ "id": id, "cmd": "character_state_refresh" }),
        )),
        BuiltinCommand::BlacklistList => Ok((
            "blacklist_list",
            json!({ "id": id, "cmd": "blacklist_list" }),
        )),
        BuiltinCommand::BlacklistAdd => {
            let target = opts
                .qq_id
                .clone()
                .ok_or_else(|| "需要 <qq-id>".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("target".to_string(), json!(target));
            p.insert("target_type".to_string(), json!("user"));
            if let Some(ref reason) = opts.reason {
                p.insert("reason".to_string(), json!(reason));
            }
            Ok((
                "blacklist_add",
                json!({ "id": id, "cmd": "blacklist_add", "params": p }),
            ))
        }
        BuiltinCommand::BlacklistRemove => {
            let target = opts
                .qq_id
                .clone()
                .ok_or_else(|| "需要 <qq-id>".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("target".to_string(), json!(target));
            p.insert("target_type".to_string(), json!("user"));
            Ok((
                "blacklist_remove",
                json!({ "id": id, "cmd": "blacklist_remove", "params": p }),
            ))
        }
        BuiltinCommand::BlacklistBan => {
            let target = opts
                .qq_id
                .clone()
                .ok_or_else(|| "需要 <qq-id>".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("target".to_string(), json!(target));
            p.insert("target_type".to_string(), json!("user"));
            if let Some(ref dur) = opts.duration {
                let secs = parse_duration_to_secs(dur)?;
                p.insert("duration_secs".to_string(), json!(secs));
            } else {
                return Err("需要 --duration 参数".to_string());
            }
            if let Some(ref reason) = opts.reason {
                p.insert("reason".to_string(), json!(reason));
            }
            Ok((
                "blacklist_ban",
                json!({ "id": id, "cmd": "blacklist_ban", "params": p }),
            ))
        }
        BuiltinCommand::MemoryList => {
            let mut p = serde_json::Map::new();
            if let Some(cid) = opts.character_id {
                p.insert("character_id".to_string(), json!(cid));
            }
            if let Some(cid) = opts.conversation_id {
                p.insert("conversation_id".to_string(), json!(cid));
            }
            if let Some(ref t) = opts.memory_type {
                p.insert("memory_type".to_string(), json!(t));
            }
            if let Some(lim) = opts.limit {
                p.insert("limit".to_string(), json!(lim));
            }
            Ok((
                "memory_list",
                json!({ "id": id, "cmd": "memory_list", "params": p }),
            ))
        }
        BuiltinCommand::MemoryDelete => {
            let mem_id = opts
                .memory_id
                .ok_or_else(|| "需要 <memory-id>".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("memory_id".to_string(), json!(mem_id));
            Ok((
                "memory_delete",
                json!({ "id": id, "cmd": "memory_delete", "params": p }),
            ))
        }
    }
}

// ============================================================================
// Section 7: Line Editor
// ============================================================================

struct LineEditor {
    editor: Editor<(), rustyline::history::FileHistory>,
}

impl LineEditor {
    fn new() -> Result<Self, ReadlineError> {
        let mut editor = Editor::new()?;
        editor.set_auto_add_history(true);
        Ok(Self { editor })
    }

    fn readline(&mut self, prompt: &str) -> Result<String, ReadlineError> {
        self.editor.readline(prompt)
    }
}

// ============================================================================
// Section 8: REPL
// ============================================================================

const PROMPT: &str = "yomua> ";

async fn run_repl(socket: PathBuf) -> Result<(), String> {
    let mut rl = LineEditor::new().map_err(|e| format!("无法初始化行编辑器: {e}"))?;
    let ctl_started_at = std::time::Instant::now();

    loop {
        let line = match rl.readline(PROMPT) {
            Ok(l) => l,
            Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => break,
            Err(e) => {
                eprintln!("读取行失败: {e}");
                continue;
            }
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if line == "exit" || line == "quit" {
            break;
        }

        let tokens: Vec<&str> = line.split_whitespace().collect();
        let parsed = match ParsedCommand::parse(&tokens) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("错误: {e}");
                continue;
            }
        };

        let builtin = match BuiltinCommand::from_path(&parsed.path) {
            Some(b) => b,
            None => {
                let path_str = parsed.path.join(" ");
                eprintln!("错误: 未知命令: {path_str}");
                eprintln!("运行 help 查看可用命令");
                continue;
            }
        };

        let formatter = OutputFormatter::new(parsed.opts.raw, Some(ctl_started_at));
        match execute_command(&socket, builtin, &parsed.opts, &parsed.args).await {
            Ok(resp) => formatter.format_and_print(&resp),
            Err(e) => eprintln!("错误: {e}"),
        }
    }

    Ok(())
}

// ============================================================================
// Section 9: Help
// ============================================================================

fn print_help() {
    println!("yomua-bot-ctl —— 帮助");
    println!();
    println!("用法: yomua-bot-ctl [选项] <命令> [参数]");
    println!();
    println!("选项:");
    println!("  --raw              输出原始 JSON");
    println!("  --socket <路径>    控制 socket 路径");
    println!("  --data-dir <路径>  数据目录");
    println!();

    let specs: Vec<(&str, &str, &str, CommandCapability)> = BuiltinCommand::all()
        .map(|b| {
            let spec = b.spec();
            (spec.usage, spec.description, spec.group, b.capability())
        })
        .collect();

    let mut groups: std::collections::HashMap<&str, Vec<(&str, &str, CommandCapability)>> =
        std::collections::HashMap::new();
    for (usage, desc, group, cap) in specs {
        groups.entry(group).or_default().push((usage, desc, cap));
    }

    let group_order = ["系统", "角色", "会话", "状态", "黑名单", "记忆"];
    for group in group_order.iter() {
        if let Some(cmds) = groups.get(group) {
            println!("{group}:");
            for (usage, desc, cap) in cmds {
                let unavailable_mark = if *cap == CommandCapability::Unavailable {
                    " [暂不可用]"
                } else {
                    ""
                };
                println!("  {:45} — {}{}", usage, desc, unavailable_mark);
            }
            println!();
        }
    }
}

// ============================================================================
// Section 10: Global Help
// ============================================================================

fn print_global_help() {
    println!(
        r#"yomua-bot-ctl —— yomua-bot 控制客户端

用法:
  yomua-bot-ctl [选项] <命令> [参数]
  yomua-bot-ctl         进入交互模式（无命令）

选项:
  --socket <路径>   控制 socket 路径（默认：<data-dir>/control.sock）
  --data-dir <路径> 数据目录（默认：data）
  --raw             输出原始 JSON（默认人类可读格式）

示例:
  yomua-bot-ctl status
  yomua-bot-ctl character list
  yomua-bot-ctl character show 1
  yomua-bot-ctl state get 1 --conversation 1
  yomua-bot-ctl session list
  yomua-bot-ctl blacklist list
  yomua-bot-ctl memory list --character 1

运行 'help' 命令查看所有可用命令。"#
    );
}

// ============================================================================
// Section 11: Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctl_started_at = std::time::Instant::now();
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_global_help();
        std::process::exit(0);
    }

    if args.is_empty() {
        let socket = PathBuf::from("data/control.sock");
        if let Err(e) = run_repl(socket).await {
            eprintln!("REPL 错误: {e}");
            std::process::exit(1);
        }
        return Ok(());
    }

    // Parse global options
    let mut socket: Option<PathBuf> = None;
    let mut data_dir: PathBuf = PathBuf::from("data");
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--socket" => {
                i += 1;
                if i < args.len() {
                    socket = Some(PathBuf::from(&args[i]));
                    i += 1;
                } else {
                    eprintln!("错误: --socket 需要路径参数");
                    std::process::exit(1);
                }
            }
            "--data-dir" => {
                i += 1;
                if i < args.len() {
                    data_dir = PathBuf::from(&args[i]);
                    i += 1;
                } else {
                    eprintln!("错误: --data-dir 需要路径参数");
                    std::process::exit(1);
                }
            }
            _ => break,
        }
    }

    let remaining = &args[i..];
    if remaining.is_empty() {
        let socket = socket.unwrap_or_else(|| data_dir.join("control.sock"));
        if let Err(e) = run_repl(socket).await {
            eprintln!("REPL 错误: {e}");
            std::process::exit(1);
        }
        return Ok(());
    }

    // help is a special command that doesn't need Runtime
    let tokens: Vec<&str> = remaining.iter().map(String::as_str).collect();
    if tokens == ["help"] {
        print_help();
        return Ok(());
    }

    let parsed = match ParsedCommand::parse(&tokens) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("错误: {e}");
            std::process::exit(1);
        }
    };

    let builtin = match BuiltinCommand::from_path(&parsed.path) {
        Some(b) => b,
        None => {
            let path_str = parsed.path.join(" ");
            eprintln!("错误: 未知命令: {path_str}");
            eprintln!("运行 yomua-bot-ctl help 查看可用命令");
            std::process::exit(1);
        }
    };

    let socket = socket.unwrap_or_else(|| data_dir.join("control.sock"));
    let formatter = OutputFormatter::new(parsed.opts.raw, Some(ctl_started_at));

    match execute_command(&socket, builtin, &parsed.opts, &parsed.args).await {
        Ok(resp) => formatter.format_and_print(&resp),
        Err(e) => {
            eprintln!("错误: {e}");
            std::process::exit(1);
        }
    }

    Ok(())
}

fn rand_u32() -> u32 {
    use std::time::Instant;
    let instant = Instant::now();
    (instant.elapsed().as_nanos() as u32).wrapping_add(instant.elapsed().subsec_nanos())
}

// ============================================================================
// Section 12: Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_status() {
        let tokens = vec!["status"];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["status".to_string()][..]);
        assert!(!p.opts.raw);
    }

    #[test]
    fn test_parse_status_raw() {
        let tokens = vec!["status", "--raw"];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["status".to_string()][..]);
        assert!(p.opts.raw);
    }

    #[test]
    fn test_parse_character_show() {
        let tokens = vec!["character", "show", "1"];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["character".to_string(), "show".to_string()][..]);
        assert_eq!(p.args, &["1".to_string()][..]);
    }

    #[test]
    fn test_parse_character_show_with_raw() {
        let tokens = vec!["character", "show", "1", "--raw"];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["character".to_string(), "show".to_string()][..]);
        assert_eq!(p.args, &["1".to_string()][..]);
        assert!(p.opts.raw);
    }

    #[test]
    fn test_parse_character_list_with_limit() {
        let tokens = vec!["character", "list", "--limit", "10"];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["character".to_string(), "list".to_string()][..]);
        assert_eq!(p.opts.limit, Some(10));
    }

    #[test]
    fn test_parse_character_list_limit_offset() {
        let tokens = vec!["character", "list", "--limit", "20", "--offset", "5"];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["character".to_string(), "list".to_string()][..]);
        assert_eq!(p.opts.limit, Some(20));
        assert_eq!(p.opts.offset, Some(5));
    }

    #[test]
    fn test_parse_state_get() {
        let tokens = vec!["state", "get", "1", "--conversation", "3"];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["state".to_string(), "get".to_string()][..]);
        assert_eq!(p.args, &["1".to_string()][..]);
        assert_eq!(p.opts.conversation_id, Some(3));
    }

    #[test]
    fn test_parse_state_set() {
        let tokens = vec![
            "state",
            "set",
            "--character",
            "1",
            "--energy",
            "50",
            "--stress",
            "20",
        ];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["state".to_string(), "set".to_string()][..]);
        assert_eq!(p.opts.character_id, Some(1));
        assert_eq!(p.opts.energy, Some(50.0));
        assert_eq!(p.opts.stress, Some(20.0));
    }

    #[test]
    fn test_parse_blacklist_add() {
        let tokens = vec!["blacklist", "add", "123456"];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["blacklist".to_string(), "add".to_string()][..]);
        assert_eq!(p.args, &["123456".to_string()][..]);
    }

    #[test]
    fn test_parse_blacklist_ban_with_duration() {
        let tokens = vec![
            "blacklist",
            "ban",
            "123456",
            "--duration",
            "1h",
            "--reason",
            "spam",
        ];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["blacklist".to_string(), "ban".to_string()][..]);
        assert_eq!(p.args, &["123456".to_string()][..]);
        assert_eq!(p.opts.duration.as_deref(), Some("1h"));
        assert_eq!(p.opts.reason.as_deref(), Some("spam"));
    }

    #[test]
    fn test_parse_memory_list_with_character() {
        let tokens = vec!["memory", "list", "--character", "1", "--limit", "20"];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["memory".to_string(), "list".to_string()][..]);
        assert_eq!(p.opts.character_id, Some(1));
        assert_eq!(p.opts.limit, Some(20));
    }

    #[test]
    fn test_parse_memory_delete() {
        let tokens = vec!["memory", "delete", "42"];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["memory".to_string(), "delete".to_string()][..]);
        assert_eq!(p.args, &["42".to_string()][..]);
    }

    #[test]
    fn test_parse_unknown_option() {
        let tokens = vec!["status", "--unknown"];
        let r = ParsedCommand::parse(&tokens);
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("未知选项"));
    }

    #[test]
    fn test_parse_limit_without_value() {
        let tokens = vec!["character", "list", "--limit"];
        let r = ParsedCommand::parse(&tokens);
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("需要"));
    }

    #[test]
    fn test_duration_parser() {
        assert!(parse_duration("1h").is_ok());
        assert!(parse_duration("30m").is_ok());
        assert!(parse_duration("7d").is_ok());
        assert!(parse_duration("").is_ok());
        let r = parse_duration("1x");
        assert!(r.is_err());
    }

    #[test]
    fn test_supported_commands_have_supported_capability() {
        let supported = [
            BuiltinCommand::Status,
            BuiltinCommand::ReloadConfig,
            BuiltinCommand::Shutdown,
            BuiltinCommand::Help,
            BuiltinCommand::CharacterList,
            BuiltinCommand::CharacterShow,
            BuiltinCommand::ConversationList,
            BuiltinCommand::StateGet,
            BuiltinCommand::StateSet,
        ];
        for cmd in supported {
            assert_eq!(
                cmd.capability(),
                CommandCapability::Supported,
                "命令 {:?} 应该是 Supported",
                cmd
            );
        }
    }

    #[test]
    fn test_unavailable_commands_have_unavailable_capability() {
        // MemoryList 和 MemoryDelete 现在是 Supported，不再是 Unavailable
        let unavailable: [BuiltinCommand; 0] = [];
        for cmd in unavailable {
            assert_eq!(
                cmd.capability(),
                CommandCapability::Unavailable,
                "命令 {:?} 应该是 Unavailable",
                cmd
            );
        }
    }

    #[test]
    fn test_builtin_from_path() {
        assert_eq!(
            BuiltinCommand::from_path(&["status".to_string()]),
            Some(BuiltinCommand::Status)
        );
        assert_eq!(
            BuiltinCommand::from_path(&["character".to_string(), "show".to_string()]),
            Some(BuiltinCommand::CharacterShow)
        );
        assert_eq!(
            BuiltinCommand::from_path(&["blacklist".to_string(), "ban".to_string()]),
            Some(BuiltinCommand::BlacklistBan)
        );
        assert_eq!(
            BuiltinCommand::from_path(&["memory".to_string(), "list".to_string()]),
            Some(BuiltinCommand::MemoryList)
        );
        assert_eq!(BuiltinCommand::from_path(&["foobar".to_string()]), None);
    }
}
