//! yomua-bot-ctl —— yomua-bot 控制客户端（正式版本）。
//!
//! 通过 Unix 域套接字向运行中的 yomua-bot 发送控制命令。
//!
//! 支持两种模式：
//! - **One-shot 模式**：`yomua-bot-ctl <命令>` 执行单条命令后退出
//! - **交互模式**：`yomua-bot-ctl`（无命令）进入 REPL，可连续执行多条命令

use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;

use crossterm::cursor;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::style::{Attribute, Color, Print, ResetColor, SetAttribute, SetForegroundColor};
use crossterm::terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen};
use rustyline::completion::{Completer, Pair};
use rustyline::config::Configurer;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Editor, Helper};
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
    pub confirm: bool,
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
    pub memory_instruction: Option<String>,
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
    MemoryUpdate,
    ReplyUnknownList,
    ReplyUnknownCheck,
    ReplyUnknownRetry,
}

impl BuiltinCommand {
    fn operation(self) -> yomua_bot::application::control_schema::ControlOperation {
        use yomua_bot::application::control_schema::ControlOperation as Op;
        match self {
            Self::Status => Op::Status,
            Self::ReloadConfig => Op::ReloadConfig,
            Self::Shutdown => Op::Shutdown,
            Self::Help => Op::Help,
            Self::CharacterList => Op::CharacterList,
            Self::CharacterShow => Op::CharacterShow,
            Self::CharacterCardList => Op::CharacterCardList,
            Self::CharacterCardEnable => Op::CharacterCardEnable,
            Self::CharacterCardDisable => Op::CharacterCardDisable,
            Self::CharacterCardReassign => Op::CharacterCardReassign,
            Self::ConversationList => Op::ConversationList,
            Self::SessionDeleteAll => Op::SessionDeleteAll,
            Self::SessionDeleteCharacter => Op::SessionDeleteCharacter,
            Self::CharacterStateGet | Self::StateGet => Op::CharacterStateGet,
            Self::CharacterStateSet | Self::StateSet => Op::CharacterStateSet,
            Self::CharacterStateRefresh | Self::StateRefresh => Op::CharacterStateRefresh,
            Self::BlacklistList => Op::BlacklistList,
            Self::BlacklistAdd => Op::BlacklistAdd,
            Self::BlacklistRemove => Op::BlacklistRemove,
            Self::BlacklistBan => Op::BlacklistBan,
            Self::MemoryList => Op::MemoryList,
            Self::MemoryDelete => Op::MemoryDelete,
            Self::MemoryUpdate => Op::MemoryUpdate,
            Self::ReplyUnknownList => Op::ReplyUnknownList,
            Self::ReplyUnknownCheck => Op::ReplyUnknownCheck,
            Self::ReplyUnknownRetry => Op::ReplyUnknownRetry,
        }
    }

    fn canonical_name(&self) -> &'static str {
        use yomua_bot::application::control_schema as schema;
        match self {
            Self::Status => schema::STATUS,
            Self::ReloadConfig => schema::RELOAD_CONFIG,
            Self::Shutdown => schema::SHUTDOWN,
            Self::Help => schema::HELP,
            Self::CharacterList => schema::CHARACTER_LIST,
            Self::CharacterShow => schema::CHARACTER_SHOW,
            Self::CharacterCardList => schema::CHARACTER_CARD_LIST,
            Self::CharacterCardEnable => schema::CHARACTER_CARD_ENABLE,
            Self::CharacterCardDisable => schema::CHARACTER_CARD_DISABLE,
            Self::CharacterCardReassign => schema::CHARACTER_CARD_REASSIGN,
            Self::ConversationList => schema::CONVERSATION_LIST,
            Self::SessionDeleteAll => schema::SESSION_DELETE_ALL,
            Self::SessionDeleteCharacter => schema::SESSION_DELETE_CHARACTER,
            Self::CharacterStateGet | Self::StateGet => schema::CHARACTER_STATE_GET,
            Self::CharacterStateSet | Self::StateSet => schema::CHARACTER_STATE_SET,
            Self::CharacterStateRefresh | Self::StateRefresh => schema::CHARACTER_STATE_REFRESH,
            Self::BlacklistList => schema::BLACKLIST_LIST,
            Self::BlacklistAdd => schema::BLACKLIST_ADD,
            Self::BlacklistRemove => schema::BLACKLIST_REMOVE,
            Self::BlacklistBan => schema::BLACKLIST_BAN,
            Self::MemoryList => schema::MEMORY_LIST,
            Self::MemoryDelete => schema::MEMORY_DELETE,
            Self::MemoryUpdate => schema::MEMORY_UPDATE,
            Self::ReplyUnknownList => schema::REPLY_UNKNOWN_LIST,
            Self::ReplyUnknownCheck => schema::REPLY_UNKNOWN_CHECK,
            Self::ReplyUnknownRetry => schema::REPLY_UNKNOWN_RETRY,
        }
    }

    fn path(&self) -> CommandPath {
        CommandPath(self.canonical_name().split(' ').collect())
    }

    fn from_path(path: &[String]) -> Option<Self> {
        let path_refs: Vec<&str> = path.iter().map(String::as_str).collect();
        let explicit = Self::all()
            .find(|c| c.path().matches(path_refs.as_slice()))
            .or(match path_refs.as_slice() {
                ["session", "list"] => Some(Self::ConversationList),
                ["state", "get"] => Some(Self::StateGet),
                ["state", "set"] => Some(Self::StateSet),
                ["state", "refresh"] => Some(Self::StateRefresh),
                _ => None,
            });
        if explicit.is_some() {
            return explicit;
        }

        let input = path.join(" ");
        let canonical = yomua_bot::application::control_schema::canonicalize(&input)?;
        Self::all().find(|command| command.canonical_name() == canonical)
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
            Self::BlacklistList,
            Self::BlacklistAdd,
            Self::BlacklistRemove,
            Self::BlacklistBan,
            Self::MemoryList,
            Self::MemoryDelete,
            Self::MemoryUpdate,
            Self::ReplyUnknownList,
            Self::ReplyUnknownCheck,
            Self::ReplyUnknownRetry,
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
        if tokens.is_empty() {
            return Err("缺少命令".to_string());
        }

        // 命令路径按已注册命令做最长匹配。旧实现把路径硬编码为最多两段，
        // 导致 character card list / character state get 这类三段命令永远不可达。
        let option_start = tokens
            .iter()
            .position(|token| token.starts_with("--"))
            .unwrap_or(tokens.len());
        let max_path_len = option_start.min(3);
        let path_len = (1..=max_path_len)
            .rev()
            .find(|length| {
                let candidate = tokens[..*length]
                    .iter()
                    .map(|token| (*token).to_string())
                    .collect::<Vec<_>>();
                BuiltinCommand::from_path(&candidate).is_some()
            })
            .ok_or_else(|| format!("未知命令: {}", tokens[..option_start].join(" ")))?;

        let path = tokens[..path_len]
            .iter()
            .map(|token| (*token).to_string())
            .collect::<Vec<_>>();
        let mut args = Vec::new();
        let mut opts = CommandOptions::default();

        let mut i = path_len;
        while i < tokens.len() {
            let tok = tokens[i];
            if let Some(key) = tok.strip_prefix("--") {
                match key {
                    "raw" => opts.raw = true,
                    "confirm" => opts.confirm = true,
                    "limit" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--limit 需要一个数值".to_string());
                        }
                        opts.limit = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--limit 需要一个数值".to_string())?,
                        );
                    }
                    "offset" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--offset 需要一个数值".to_string());
                        }
                        opts.offset = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--offset 需要一个数值".to_string())?,
                        );
                    }
                    "id" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--id 需要一个正整数".to_string());
                        }
                        opts.id = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--id 需要一个正整数".to_string())?,
                        );
                    }
                    "character" | "character-id" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--character 需要一个正整数".to_string());
                        }
                        opts.character_id = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--character 需要一个正整数".to_string())?,
                        );
                    }
                    "conversation" | "conversation-id" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--conversation 需要一个正整数".to_string());
                        }
                        opts.conversation_id = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--conversation 需要一个正整数".to_string())?,
                        );
                    }
                    "mood" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--mood 需要一个数值".to_string());
                        }
                        opts.mood = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--mood 需要一个数值".to_string())?,
                        );
                    }
                    "energy" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--energy 需要一个数值".to_string());
                        }
                        opts.energy = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--energy 需要一个数值".to_string())?,
                        );
                    }
                    "stress" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--stress 需要一个数值".to_string());
                        }
                        opts.stress = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--stress 需要一个数值".to_string())?,
                        );
                    }
                    "activity" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--activity 需要文本".to_string());
                        }
                        opts.activity = tokens.get(i).map(|v| v.to_string());
                    }
                    "qq-id" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--qq-id 需要用户 ID".to_string());
                        }
                        opts.qq_id = tokens.get(i).map(|v| v.to_string());
                    }
                    "duration" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--duration 需要时长".to_string());
                        }
                        opts.duration = tokens.get(i).map(|v| v.to_string());
                    }
                    "reason" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--reason 需要文本".to_string());
                        }
                        opts.reason = tokens.get(i).map(|v| v.to_string());
                    }
                    "memory-id" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--memory-id 需要一个正整数".to_string());
                        }
                        opts.memory_id = Some(
                            tokens
                                .get(i)
                                .and_then(|v| v.parse().ok())
                                .ok_or_else(|| "--memory-id 需要一个正整数".to_string())?,
                        );
                    }
                    "type" | "memory-type" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--type 需要记忆类型".to_string());
                        }
                        opts.memory_type = tokens.get(i).map(|v| v.to_string());
                    }
                    "instruction" => {
                        i += 1;
                        if tokens.get(i).is_none_or(|value| value.starts_with("--")) {
                            return Err("--instruction 需要自然语言说明".to_string());
                        }
                        opts.memory_instruction = tokens.get(i).map(|v| v.to_string());
                    }
                    other => {
                        return Err(format!("未知选项: --{other}"));
                    }
                }
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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ControlErrorDetail {
    code: String,
    message: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
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
        // raw 模式是稳定的机器接口：成功和失败都输出完整响应，统一写 stdout。
        let s = serde_json::to_string(resp).unwrap_or_else(|_| {
            "{\"id\":0,\"ok\":false,\"error\":{\"code\":\"SERIALIZE_ERROR\",\"message\":\"无法序列化响应\"}}".to_string()
        });
        println!("{s}");
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
                    let content = o.get("content").and_then(|v| v.as_str());
                    let mtype = o.get("type").and_then(|v| v.as_str()).unwrap_or("?");
                    let detail = content
                        .map(|value| value.to_string())
                        .or_else(|| {
                            o.get("content_fingerprint")
                                .and_then(|v| v.as_str())
                                .map(|v| format!("指纹 {}", v))
                        })
                        .unwrap_or_else(|| "原文未读取".to_string());
                    println!("  [{}] [{}] {}", id, mtype, detail);
                } else {
                    println!("  {}", m);
                }
            }
            return;
        }

        // commands list (help)
        if let Some(arr) = obj.get("commands").and_then(|v| v.as_array()) {
            println!("可用命令：");
            let mut cmd_items: Vec<(&str, &str, &str, bool)> = Vec::new();
            for cmd in arr {
                if let Some(o) = cmd.as_object() {
                    let name = o.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                    let usage = o.get("usage").and_then(|v| v.as_str()).unwrap_or(name);
                    let desc = o.get("description").and_then(|v| v.as_str()).unwrap_or("");
                    let confirm = o
                        .get("requires_confirmation")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    cmd_items.push((usage, name, desc, confirm));
                }
            }
            for (usage, _name, desc, confirm) in cmd_items {
                let suffix = if confirm { " [需要 --confirm]" } else { "" };
                println!("  {:76} — {}{}", usage, desc, suffix);
            }
            return;
        }

        // generic fallback: print the whole object
        let raw = serde_json::to_string_pretty(obj).unwrap_or_else(|_| format!("{obj:?}"));
        println!("{raw}");
    }
}

fn print_client_error(raw: bool, message: &str) {
    if raw {
        println!(
            "{}",
            serde_json::json!({
                "id": 0,
                "ok": false,
                "error": {"code": "CLIENT_ERROR", "message": message}
            })
        );
    } else {
        eprintln!("错误: {message}");
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
        'm' => value
            .checked_mul(60)
            .ok_or_else(|| "时长超出允许范围".to_string())?,
        'h' => value
            .checked_mul(3600)
            .ok_or_else(|| "时长超出允许范围".to_string())?,
        'd' => value
            .checked_mul(86400)
            .ok_or_else(|| "时长超出允许范围".to_string())?,
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
        'm' => value
            .checked_mul(60)
            .ok_or_else(|| "时长超出允许范围".to_string())?,
        'h' => value
            .checked_mul(3600)
            .ok_or_else(|| "时长超出允许范围".to_string())?,
        'd' => value
            .checked_mul(86400)
            .ok_or_else(|| "时长超出允许范围".to_string())?,
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
    let id = generate_id();
    let envelope = build_envelope(builtin, opts, args, id)?;
    let command_name = envelope.request.operation.canonical_name();
    if yomua_bot::application::control_schema::requires_confirmation(command_name) && !opts.confirm
    {
        return Err("此命令需要明确确认，请追加 --confirm".to_string());
    }

    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(|e| format!("无法连接到 control socket {}: {}", socket.display(), e))?;

    use yomua_bot::application::control::{
        read_control_json, write_control_json, CONTROL_REQUEST_LIMIT, CONTROL_RESPONSE_LIMIT,
    };
    write_control_json(&mut stream, &envelope, CONTROL_REQUEST_LIMIT)
        .await
        .map_err(|e| e.to_string())?;
    let resp: ControlResponse = timeout(
        Duration::from_secs(45),
        read_control_json(&mut stream, CONTROL_RESPONSE_LIMIT),
    )
    .await
    .map_err(|_| "服务器响应超时".to_string())?
    .map_err(|e| e.to_string())?;

    if resp.id != id {
        return Err(format!("响应 ID 不匹配（请求: {id}，响应: {}）", resp.id));
    }

    Ok(resp)
}

fn build_envelope(
    builtin: BuiltinCommand,
    opts: &CommandOptions,
    args: &[String],
    id: u64,
) -> Result<yomua_bot::application::control_schema::ControlEnvelope, String> {
    let (_command_name, parameter_request) = build_request(builtin, opts, args, id)?;
    let mut request_params = parameter_request
        .as_object()
        .cloned()
        .ok_or_else(|| "命令参数必须是 JSON 对象".to_string())?;
    if opts.confirm {
        request_params.insert("confirm".to_string(), serde_json::Value::Bool(true));
    }
    let operation = builtin.operation();
    Ok(
        yomua_bot::application::control_schema::ControlEnvelope::new(
            id,
            yomua_bot::application::control_schema::ControlRequest::new(
                operation,
                serde_json::Value::Object(request_params),
            ),
        ),
    )
}

fn build_request(
    builtin: BuiltinCommand,
    opts: &CommandOptions,
    args: &[String],
    _id: u64,
) -> Result<(&'static str, serde_json::Value), String> {
    // 这里只整理参数；bot 只接收 build_envelope 生成的结构化请求。
    use serde_json::json;

    validate_positional_args(builtin, args)?;

    // Extract positional args into opts for commands that need them
    let mut opts = opts.clone();
    match builtin {
        BuiltinCommand::CharacterShow
        | BuiltinCommand::CharacterCardEnable
        | BuiltinCommand::CharacterCardDisable
        | BuiltinCommand::CharacterStateGet
        | BuiltinCommand::CharacterStateSet => {
            if let Some(arg) = args.first() {
                if let Ok(v) = arg.parse() {
                    if matches!(builtin, BuiltinCommand::CharacterStateSet) {
                        opts.character_id = Some(v);
                    } else {
                        opts.id = Some(v);
                    }
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
        BuiltinCommand::MemoryUpdate => {
            if let Some(arg) = args.first() {
                if let Ok(v) = arg.parse() {
                    opts.memory_id = Some(v);
                }
            }
        }
        BuiltinCommand::ReplyUnknownCheck | BuiltinCommand::ReplyUnknownRetry => {
            if let Some(arg) = args.first() {
                if let Ok(v) = arg.parse() {
                    opts.id = Some(v);
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
        BuiltinCommand::Status => Ok(("status", json!({}))),
        BuiltinCommand::ReloadConfig => Ok(("reload-config", json!({}))),
        BuiltinCommand::Shutdown => Ok(("shutdown", json!({}))),
        BuiltinCommand::Help => Ok(("help", json!({}))),
        BuiltinCommand::CharacterList => {
            let mut p = serde_json::Map::new();
            if let Some(l) = opts.limit {
                p.insert("limit".to_string(), json!(l));
            }
            if let Some(o) = opts.offset {
                p.insert("offset".to_string(), json!(o));
            }
            Ok(("character list", serde_json::Value::Object(p)))
        }
        BuiltinCommand::CharacterShow => {
            let char_id = opts
                .id
                .or(opts.character_id)
                .ok_or_else(|| "需要角色 ID".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("id".to_string(), json!(char_id));
            Ok(("character show", serde_json::Value::Object(p)))
        }
        BuiltinCommand::CharacterCardList => Ok(("character card list", json!({}))),
        BuiltinCommand::CharacterCardEnable => {
            let char_id = opts
                .id
                .or(opts.character_id)
                .ok_or_else(|| "需要角色 ID".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("id".to_string(), json!(char_id));
            Ok(("character card enable", serde_json::Value::Object(p)))
        }
        BuiltinCommand::CharacterCardDisable => {
            let char_id = opts
                .id
                .or(opts.character_id)
                .ok_or_else(|| "需要角色 ID".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("id".to_string(), json!(char_id));
            Ok(("character card disable", serde_json::Value::Object(p)))
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
            Ok(("character card reassign", serde_json::Value::Object(p)))
        }
        BuiltinCommand::ConversationList => Ok(("conversation list", json!({}))),
        BuiltinCommand::SessionDeleteAll => Ok(("session delete-all", json!({}))),
        BuiltinCommand::SessionDeleteCharacter => {
            let character_id = opts
                .character_id
                .ok_or_else(|| "需要 <character-id>".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("character_id".to_string(), json!(character_id));
            Ok(("session delete-character", serde_json::Value::Object(p)))
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
            Ok(("character state get", serde_json::Value::Object(p)))
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
            Ok(("character state set", serde_json::Value::Object(p)))
        }
        BuiltinCommand::StateRefresh => Ok(("character state refresh", json!({}))),
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
            Ok(("character state get", serde_json::Value::Object(p)))
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
            Ok(("character state set", serde_json::Value::Object(p)))
        }
        BuiltinCommand::CharacterStateRefresh => Ok(("character state refresh", json!({}))),
        BuiltinCommand::BlacklistList => Ok(("blacklist list", json!({}))),
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
            Ok(("blacklist add", serde_json::Value::Object(p)))
        }
        BuiltinCommand::BlacklistRemove => {
            let target = opts
                .qq_id
                .clone()
                .ok_or_else(|| "需要 <qq-id>".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("target".to_string(), json!(target));
            p.insert("target_type".to_string(), json!("user"));
            Ok(("blacklist remove", serde_json::Value::Object(p)))
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
            Ok(("blacklist ban", serde_json::Value::Object(p)))
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
            if opts.raw {
                p.insert("raw".to_string(), json!(true));
            }
            Ok(("memory list", serde_json::Value::Object(p)))
        }
        BuiltinCommand::MemoryDelete => {
            let mem_id = opts
                .memory_id
                .ok_or_else(|| "需要 <memory-id>".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("memory_id".to_string(), json!(mem_id));
            Ok(("memory delete", serde_json::Value::Object(p)))
        }
        BuiltinCommand::MemoryUpdate => {
            let mem_id = opts
                .memory_id
                .ok_or_else(|| "需要 <memory-id>".to_string())?;
            let mut p = serde_json::Map::new();
            p.insert("memory_id".to_string(), json!(mem_id));
            let instruction = opts
                .memory_instruction
                .as_ref()
                .ok_or_else(|| "需要 --instruction <中文修改说明>".to_string())?;
            p.insert("instruction".to_string(), json!(instruction));
            Ok(("memory update", serde_json::Value::Object(p)))
        }
        BuiltinCommand::ReplyUnknownList => {
            let mut p = serde_json::Map::new();
            if let Some(limit) = opts.limit {
                p.insert("limit".to_string(), json!(limit));
            }
            if opts.raw {
                p.insert("raw".to_string(), json!(true));
            }
            Ok(("reply unknown list", serde_json::Value::Object(p)))
        }
        BuiltinCommand::ReplyUnknownCheck => {
            let message_id = opts.id.ok_or_else(|| "需要 <message-id>".to_string())?;
            Ok(("reply unknown check", json!({ "message_id": message_id })))
        }
        BuiltinCommand::ReplyUnknownRetry => {
            let message_id = opts.id.ok_or_else(|| "需要 <message-id>".to_string())?;
            Ok(("reply unknown retry", json!({ "message_id": message_id })))
        }
    }
}

/// 拒绝没有被命令消费的位置参数。
///
/// 以前只有少数命令检查参数数量，导致 `shutdown typo`、
/// `session delete-all accidental` 仍然会执行危险操作。
fn validate_positional_args(builtin: BuiltinCommand, args: &[String]) -> Result<(), String> {
    let (min, max, usage) = match builtin {
        BuiltinCommand::CharacterShow
        | BuiltinCommand::CharacterCardEnable
        | BuiltinCommand::CharacterCardDisable
        | BuiltinCommand::BlacklistAdd
        | BuiltinCommand::BlacklistRemove
        | BuiltinCommand::BlacklistBan
        | BuiltinCommand::MemoryDelete
        | BuiltinCommand::MemoryUpdate
        | BuiltinCommand::ReplyUnknownCheck
        | BuiltinCommand::ReplyUnknownRetry
        | BuiltinCommand::SessionDeleteCharacter => (1, 1, "需要一个位置参数"),
        BuiltinCommand::CharacterCardReassign => (2, 2, "需要两个位置参数"),
        BuiltinCommand::CharacterStateGet | BuiltinCommand::StateGet => {
            (0, 1, "最多接受一个角色 ID")
        }
        BuiltinCommand::Status
        | BuiltinCommand::ReloadConfig
        | BuiltinCommand::Shutdown
        | BuiltinCommand::Help
        | BuiltinCommand::CharacterList
        | BuiltinCommand::CharacterCardList
        | BuiltinCommand::ConversationList
        | BuiltinCommand::SessionDeleteAll
        | BuiltinCommand::CharacterStateSet
        | BuiltinCommand::StateSet
        | BuiltinCommand::CharacterStateRefresh
        | BuiltinCommand::StateRefresh
        | BuiltinCommand::BlacklistList
        | BuiltinCommand::MemoryList
        | BuiltinCommand::ReplyUnknownList => (0, 0, "不接受位置参数"),
    };

    if args.len() < min || args.len() > max {
        return Err(format!("{}：命令 {}", usage, builtin.canonical_name()));
    }
    Ok(())
}

// ============================================================================
// Section 7: Line Editor
// ============================================================================

struct ReplHelper;

impl Helper for ReplHelper {}
impl Hinter for ReplHelper {
    type Hint = String;
}
impl Highlighter for ReplHelper {}
impl Validator for ReplHelper {}

impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let before = &line[..pos];
        let start = before
            .char_indices()
            .rev()
            .find(|(_, ch)| ch.is_whitespace())
            .map(|(index, ch)| index + ch.len_utf8())
            .unwrap_or(0);
        let current = &before[start..];
        let prefix = before[..start].split_whitespace().collect::<Vec<_>>();

        let mut candidates = Vec::new();
        if current.starts_with('-') {
            for option in [
                "--raw",
                "--confirm",
                "--limit",
                "--offset",
                "--conversation",
                "--character",
                "--energy",
                "--stress",
                "--mood",
                "--activity",
                "--qq-id",
                "--duration",
                "--reason",
                "--type",
                "--instruction",
            ] {
                if option.starts_with(current) {
                    candidates.push(Pair {
                        display: option.to_string(),
                        replacement: option.to_string(),
                    });
                }
            }
        } else {
            for spec in yomua_bot::application::control_schema::specs() {
                let mut paths = vec![spec.name];
                paths.extend(
                    yomua_bot::application::control_schema::aliases(spec.name)
                        .iter()
                        .copied(),
                );
                for path in paths {
                    let parts = path.split_whitespace().collect::<Vec<_>>();
                    if parts.len() <= prefix.len() || parts[..prefix.len()] != prefix[..] {
                        continue;
                    }
                    let next = parts[prefix.len()];
                    if next.starts_with(current)
                        && !candidates
                            .iter()
                            .any(|candidate: &Pair| candidate.replacement == next)
                    {
                        candidates.push(Pair {
                            display: next.to_string(),
                            replacement: next.to_string(),
                        });
                    }
                }
            }
        }
        Ok((start, candidates))
    }
}

struct LineEditor {
    editor: Editor<ReplHelper, rustyline::history::FileHistory>,
    history_path: PathBuf,
}

impl LineEditor {
    fn new(history_path: PathBuf) -> Result<Self, ReadlineError> {
        let config = rustyline::Config::builder()
            .history_ignore_dups(true)?
            .history_ignore_space(true)
            .build();
        let mut editor = Editor::with_config(config)?;
        editor.set_helper(Some(ReplHelper));
        if history_path.exists() {
            let _ = editor.load_history(&history_path);
        }
        editor.set_auto_add_history(true);
        Ok(Self {
            editor,
            history_path,
        })
    }

    fn readline(&mut self, prompt: &str) -> Result<String, ReadlineError> {
        self.editor.readline(prompt)
    }

    fn readline_with_initial(
        &mut self,
        prompt: &str,
        initial: &str,
    ) -> Result<String, ReadlineError> {
        self.editor.readline_with_initial(prompt, (initial, ""))
    }

    fn save_history(&mut self) {
        if let Some(parent) = self.history_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = self.editor.save_history(&self.history_path);
    }
}

// ============================================================================
// Section 8: REPL
// ============================================================================

const PROMPT: &str = "yomua> ";

/// 将 REPL 输入拆成参数，支持单引号、双引号和反斜杠转义。
///
/// one-shot 模式由 shell 负责拆词；REPL 没有 shell，因此不能直接
/// split_whitespace()，否则中文说明、原因和活动名称会被截断。
fn tokenize_repl_line(line: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;

    for ch in line.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            started = true;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            started = true;
            continue;
        }
        if let Some(active_quote) = quote {
            if ch == active_quote {
                quote = None;
            } else {
                current.push(ch);
            }
            started = true;
            continue;
        }
        match ch {
            '\'' | '"' => {
                quote = Some(ch);
                started = true;
            }
            c if c.is_whitespace() => {
                if started {
                    tokens.push(std::mem::take(&mut current));
                    started = false;
                }
            }
            _ => {
                current.push(ch);
                started = true;
            }
        }
    }

    if escaped {
        return Err("命令末尾存在未完成的转义".to_string());
    }
    if quote.is_some() {
        return Err("命令中存在未闭合的引号".to_string());
    }
    if started {
        tokens.push(current);
    }
    Ok(tokens)
}

async fn run_repl(socket: PathBuf) -> Result<(), String> {
    let history_path = socket
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(".yomua-bot-ctl.history");
    let mut rl = LineEditor::new(history_path).map_err(|e| format!("无法初始化行编辑器: {e}"))?;
    let ctl_started_at = std::time::Instant::now();
    println!("已进入 yomua-bot-ctl 交互模式。按 Tab 补全，按上下键查看历史，输入 help 查看命令。输入 q 退出。\n");

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

        if line == "exit" || line == "quit" || line == "q" {
            break;
        }

        if line == "tui" || line == "panel" {
            if let Err(error) = run_tui(socket.clone()).await {
                eprintln!("全屏面板错误: {error}");
            }
            continue;
        }

        if line == "help" || line == "h" || line == "?" {
            print_help();
            continue;
        }
        if let Some(prefix) = line.strip_prefix("help ") {
            print_help_for(prefix.trim());
            continue;
        }

        let token_strings = match tokenize_repl_line(line) {
            Ok(tokens) => tokens,
            Err(e) => {
                eprintln!("错误: {e}");
                continue;
            }
        };
        let tokens: Vec<&str> = token_strings.iter().map(String::as_str).collect();
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
            Ok(resp) => {
                let failed = !resp.ok;
                formatter.format_and_print(&resp);
                if failed {
                    eprintln!("命令执行失败");
                }
            }
            Err(e) => print_client_error(parsed.opts.raw, &e),
        }
    }

    rl.save_history();
    Ok(())
}

// ============================================================================
// Section 9: Full-screen command panel
// ============================================================================

/// 全屏面板只负责挑选命令；需要 ID、文本或确认的命令仍回到行编辑器输入。
/// 这样方向键适合浏览常用操作，复杂参数仍保留 shell/REPL 的可编辑性。
async fn run_tui(socket: PathBuf) -> Result<(), String> {
    let mut stdout = io::stdout();
    terminal::enable_raw_mode().map_err(|e| format!("无法启用终端原始模式: {e}"))?;
    if let Err(error) = execute!(stdout, EnterAlternateScreen, cursor::Hide) {
        let _ = terminal::disable_raw_mode();
        return Err(format!("无法进入全屏面板: {error}"));
    }

    let result = run_tui_loop(&mut stdout, &socket).await;
    let _ = execute!(stdout, LeaveAlternateScreen, cursor::Show, ResetColor);
    let _ = terminal::disable_raw_mode();
    result
}

async fn run_tui_loop(stdout: &mut io::Stdout, socket: &PathBuf) -> Result<(), String> {
    let specs = yomua_bot::application::control_schema::specs();
    let mut selected = 0usize;
    let mut message = "使用 ↑/↓ 选择，Enter 执行，Esc 退出；带参数命令会打开输入行".to_string();

    loop {
        draw_tui(stdout, specs, selected, &message).map_err(|e| format!("绘制面板失败: {e}"))?;
        if !event::poll(Duration::from_millis(100)).map_err(|e| format!("读取按键失败: {e}"))?
        {
            continue;
        }
        let event = event::read().map_err(|e| format!("读取按键失败: {e}"))?;
        let Event::Key(KeyEvent {
            code,
            modifiers,
            kind: _,
            state: _,
        }) = event
        else {
            continue;
        };

        match (code, modifiers) {
            (KeyCode::Esc, _) | (KeyCode::Char('q'), KeyModifiers::NONE) => break,
            (KeyCode::Up, _) | (KeyCode::Char('k'), KeyModifiers::NONE) => {
                selected = selected.saturating_sub(1);
            }
            (KeyCode::Down, _) | (KeyCode::Char('j'), KeyModifiers::NONE) => {
                selected = (selected + 1).min(specs.len().saturating_sub(1));
            }
            (KeyCode::PageUp, _) => selected = selected.saturating_sub(8),
            (KeyCode::PageDown, _) => selected = (selected + 8).min(specs.len().saturating_sub(1)),
            (KeyCode::Char('c'), KeyModifiers::CONTROL) => break,
            (KeyCode::Enter, _) => {
                let spec = specs[selected];
                if spec.usage.contains('<') || spec.requires_confirmation {
                    leave_tui(stdout)?;
                    let result = prompt_tui_command(socket, &spec).await;
                    reenter_tui(stdout)?;
                    message = match result {
                        Ok(Some(text)) => text,
                        Ok(None) => "已取消输入，仍在命令面板".to_string(),
                        Err(error) => format!("错误: {error}"),
                    };
                } else {
                    let path = spec
                        .name
                        .split_whitespace()
                        .map(str::to_string)
                        .collect::<Vec<_>>();
                    let builtin = BuiltinCommand::from_path(&path)
                        .ok_or_else(|| format!("命令定义无法解析: {}", spec.name))?;
                    let response =
                        execute_command(socket, builtin, &CommandOptions::default(), &[]).await;
                    message = match response {
                        Ok(resp) if resp.ok => summarize_tui_response(&resp),
                        Ok(resp) => {
                            format!("失败 [{}]: {}", response_code(&resp), resp.error_message())
                        }
                        Err(error) => format!("错误: {error}"),
                    };
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn draw_tui(
    stdout: &mut io::Stdout,
    specs: &[yomua_bot::application::control_schema::ControlCommandSpec],
    selected: usize,
    message: &str,
) -> io::Result<()> {
    let (_, height) = terminal::size().unwrap_or((100, 24));
    let visible = height.saturating_sub(6) as usize;
    let start = selected.saturating_sub(visible.saturating_sub(1));
    execute!(
        stdout,
        Clear(ClearType::All),
        cursor::MoveTo(0, 0),
        SetForegroundColor(Color::Cyan),
        SetAttribute(Attribute::Bold),
        Print("yomua-bot-ctl 命令面板"),
        ResetColor,
        SetAttribute(Attribute::Reset),
        cursor::MoveTo(0, 1),
        Print("方向键/j/k 选择  Enter 执行  Esc/q 退出  Ctrl-C 退出"),
        cursor::MoveTo(0, 2),
        Print("────────────────────────────────────────────────────────────────────────"),
    )?;
    for (offset, spec) in specs.iter().skip(start).take(visible).enumerate() {
        let index = start + offset;
        let marker = if index == selected { ">" } else { " " };
        let destructive = if spec.requires_confirmation {
            " [需确认]"
        } else {
            ""
        };
        let line = format!(
            "{marker} {:<54} {}{}",
            spec.usage, spec.description, destructive
        );
        execute!(stdout, cursor::MoveTo(0, (3 + offset) as u16))?;
        if index == selected {
            execute!(
                stdout,
                SetForegroundColor(Color::Yellow),
                SetAttribute(Attribute::Bold)
            )?;
        }
        execute!(
            stdout,
            Print(line),
            ResetColor,
            SetAttribute(Attribute::Reset)
        )?;
    }
    execute!(
        stdout,
        cursor::MoveTo(0, height.saturating_sub(2)),
        SetForegroundColor(Color::DarkGrey),
        Print(message),
        ResetColor,
        cursor::MoveTo(0, height.saturating_sub(1)),
        Print(format!("{}/{}", selected + 1, specs.len())),
    )?;
    stdout.flush()?;
    Ok(())
}

fn leave_tui(stdout: &mut io::Stdout) -> Result<(), String> {
    execute!(stdout, LeaveAlternateScreen, cursor::Show, ResetColor)
        .map_err(|e| format!("离开全屏面板失败: {e}"))?;
    terminal::disable_raw_mode().map_err(|e| format!("无法恢复终端输入: {e}"))
}

fn reenter_tui(stdout: &mut io::Stdout) -> Result<(), String> {
    terminal::enable_raw_mode().map_err(|e| format!("无法重新启用终端输入: {e}"))?;
    execute!(stdout, EnterAlternateScreen, cursor::Hide)
        .map_err(|e| format!("无法重新进入全屏面板: {e}"))
}

async fn prompt_tui_command(
    socket: &PathBuf,
    spec: &yomua_bot::application::control_schema::ControlCommandSpec,
) -> Result<Option<String>, String> {
    let history_path = socket
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(".yomua-bot-ctl.history");
    let mut editor = LineEditor::new(history_path).map_err(|e| format!("无法初始化输入行: {e}"))?;
    println!("\n{}\n{}", spec.description, spec.usage);
    println!("输入完整命令；直接回车取消。危险命令必须显式追加 --confirm。\n");
    let line = match editor.readline_with_initial("命令> ", "") {
        Ok(line) => line,
        Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => {
            editor.save_history();
            return Ok(None);
        }
        Err(error) => return Err(format!("读取命令失败: {error}")),
    };
    editor.save_history();
    let line = line.trim();
    if line.is_empty() {
        return Ok(None);
    }
    let token_strings = tokenize_repl_line(line)?;
    let tokens = token_strings.iter().map(String::as_str).collect::<Vec<_>>();
    let parsed = ParsedCommand::parse(&tokens)?;
    let builtin = BuiltinCommand::from_path(&parsed.path).ok_or_else(|| "未知命令".to_string())?;
    let response = execute_command(socket, builtin, &parsed.opts, &parsed.args).await?;
    let formatter = OutputFormatter::new(parsed.opts.raw, None);
    formatter.format_and_print(&response);
    if response.ok {
        Ok(Some("命令已完成，已返回面板".to_string()))
    } else {
        Ok(Some(format!(
            "命令失败 [{}]，已返回面板",
            response_code(&response)
        )))
    }
}

fn response_code(response: &ControlResponse) -> &str {
    response
        .error
        .as_ref()
        .map(|error| error.code.as_str())
        .unwrap_or("UNKNOWN")
}

fn summarize_tui_response(response: &ControlResponse) -> String {
    let Some(data) = response.data.as_ref() else {
        return "命令已完成".to_string();
    };
    let mut text = serde_json::to_string(data).unwrap_or_else(|_| "命令已完成".to_string());
    if text.chars().count() > 96 {
        text = text.chars().take(93).collect::<String>() + "...";
    }
    format!("命令已完成: {text}")
}

// ============================================================================
// Section 10: Help
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

    println!("命令:");
    for spec in yomua_bot::application::control_schema::specs() {
        let confirmation = if spec.requires_confirmation {
            " [需要 --confirm]"
        } else {
            ""
        };
        let aliases = yomua_bot::application::control_schema::aliases(spec.name);
        let alias_text = if aliases.is_empty() {
            String::new()
        } else {
            format!("；别名：{}", aliases.join(", "))
        };
        println!(
            "  {:76} — {}{}{}",
            spec.usage, spec.description, confirmation, alias_text
        );
    }
    println!();
    println!("REPL 控制:");
    println!("  tui / panel         打开全屏命令面板");
    println!("  help <前缀>          只显示匹配的命令");
    println!("  exit / quit / q      退出客户端（不会关闭 bot）");
}

fn print_help_for(prefix: &str) {
    let prefix = prefix.trim();
    if prefix.is_empty() {
        print_help();
        return;
    }
    let specs: Vec<_> = yomua_bot::application::control_schema::specs()
        .iter()
        .filter(|spec| {
            spec.name.starts_with(prefix)
                || yomua_bot::application::control_schema::aliases(spec.name)
                    .iter()
                    .any(|alias| alias.starts_with(prefix))
        })
        .collect();
    if specs.is_empty() {
        println!("没有匹配命令：{prefix}。输入 help 查看全部命令。\n");
        return;
    }
    println!("匹配命令：");
    for spec in specs {
        let confirm = if spec.requires_confirmation {
            " [需要 --confirm]"
        } else {
            ""
        };
        let aliases = yomua_bot::application::control_schema::aliases(spec.name);
        let alias_text = if aliases.is_empty() {
            String::new()
        } else {
            format!("；别名：{}", aliases.join(", "))
        };
        println!(
            "  {:76} — {}{}{}",
            spec.usage, spec.description, confirm, alias_text
        );
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
  yomua-bot-ctl --tui   打开全屏命令面板

选项:
  --socket <路径>   控制 socket 路径（默认：<data-dir>/control.sock）
  --data-dir <路径> 数据目录（默认：data）
  --raw             输出完整控制响应 JSON；可放在命令前或命令后
  --confirm         确认执行危险操作；可放在命令前或命令后
  --tui             打开方向键可操作的全屏命令面板

示例:
  yomua-bot-ctl status
  yomua-bot-ctl character list
  yomua-bot-ctl character show 1
  yomua-bot-ctl character state get 1 --conversation 1
  yomua-bot-ctl conversation list
  yomua-bot-ctl blacklist list
  yomua-bot-ctl memory list --character 1

运行 'help' 命令查看所有可用命令；在 REPL 中按 Tab 可补全命令，输入 tui 可进入全屏面板。"#
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
    let mut tui = false;
    let mut command_prefix_options: Vec<String> = Vec::new();
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
            "--raw" | "--confirm" => {
                // 允许全局选项放在命令前，也保留命令后的写法。
                command_prefix_options.push(args[i].clone());
                i += 1;
            }
            "--tui" => {
                tui = true;
                i += 1;
            }
            _ => break,
        }
    }

    let mut remaining = args[i..].to_vec();
    if !command_prefix_options.is_empty() {
        remaining.extend(command_prefix_options);
    }
    if remaining.is_empty() {
        let socket = socket.unwrap_or_else(|| data_dir.join("control.sock"));
        let result = if tui {
            run_tui(socket).await
        } else {
            run_repl(socket).await
        };
        if let Err(e) = result {
            eprintln!("REPL 错误: {e}");
            std::process::exit(1);
        }
        return Ok(());
    }

    if tui {
        eprintln!("错误: --tui 只能单独使用，复杂参数请在面板中选择后输入");
        std::process::exit(1);
    }

    // help is a special command that doesn't need Runtime
    let tokens: Vec<&str> = remaining.iter().map(String::as_str).collect();
    if tokens == ["help"] {
        print_help();
        return Ok(());
    }
    if tokens.len() > 1 && tokens[0] == "help" {
        print_help_for(&tokens[1..].join(" "));
        return Ok(());
    }

    let parsed = match ParsedCommand::parse(&tokens) {
        Ok(p) => p,
        Err(e) => {
            print_client_error(tokens.contains(&"--raw"), &e);
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
        Ok(resp) => {
            let failed = !resp.ok;
            formatter.format_and_print(&resp);
            if failed {
                std::process::exit(1);
            }
        }
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
    fn test_parse_three_segment_character_commands() {
        let card = ParsedCommand::parse(&["character", "card", "list"]).unwrap();
        assert_eq!(card.path, ["character", "card", "list"]);

        let state =
            ParsedCommand::parse(&["character", "state", "set", "1", "--energy", "50"]).unwrap();
        assert_eq!(state.path, ["character", "state", "set"]);
        assert_eq!(state.args, ["1"]);
        assert_eq!(state.opts.energy, Some(50.0));
    }

    #[test]
    fn test_session_list_alias_resolves_to_conversation_list() {
        assert_eq!(
            BuiltinCommand::from_path(&["session".to_string(), "list".to_string()]),
            Some(BuiltinCommand::ConversationList)
        );
    }

    #[test]
    fn test_human_short_aliases_resolve_to_canonical_commands() {
        assert_eq!(
            BuiltinCommand::from_path(&["s".to_string()]),
            Some(BuiltinCommand::Status)
        );
        assert_eq!(
            BuiltinCommand::from_path(&["c".to_string(), "list".to_string()]),
            Some(BuiltinCommand::CharacterList)
        );
        assert_eq!(
            BuiltinCommand::from_path(&["m".to_string(), "list".to_string()]),
            Some(BuiltinCommand::MemoryList)
        );
        assert_eq!(
            BuiltinCommand::from_path(&["cfg".to_string(), "reload".to_string()]),
            Some(BuiltinCommand::ReloadConfig)
        );
    }

    #[test]
    fn test_short_aliases_keep_longest_path_parsing() {
        let parsed = ParsedCommand::parse(&["c", "card", "list"]).unwrap();
        assert_eq!(
            BuiltinCommand::from_path(&parsed.path),
            Some(BuiltinCommand::CharacterCardList)
        );
        let parsed = ParsedCommand::parse(&["ss", "delete-all", "--confirm"]).unwrap();
        assert_eq!(parsed.path, ["ss", "delete-all"]);
        assert!(parsed.opts.confirm);
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
    fn test_parse_memory_update_options() {
        let tokens = vec![
            "memory",
            "update",
            "42",
            "--instruction",
            "请把这条记忆改成用户喜欢猫",
        ];
        let p = ParsedCommand::parse(&tokens).unwrap();
        assert_eq!(p.path, &["memory".to_string(), "update".to_string()][..]);
        assert_eq!(p.args, &["42".to_string()][..]);
        assert_eq!(
            p.opts.memory_instruction.as_deref(),
            Some("请把这条记忆改成用户喜欢猫")
        );
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
    fn test_repl_tokenizer_preserves_quoted_text() {
        let tokens = tokenize_repl_line(
            r#"memory update 42 --instruction "请把这条记忆改成用户喜欢猫，并保留来源" --confirm"#,
        )
        .unwrap();
        assert_eq!(
            tokens,
            vec![
                "memory",
                "update",
                "42",
                "--instruction",
                "请把这条记忆改成用户喜欢猫，并保留来源",
                "--confirm"
            ]
        );
    }

    #[test]
    fn test_repl_tokenizer_rejects_unclosed_quote() {
        let error = tokenize_repl_line("memory update 42 --instruction '未完成").unwrap_err();
        assert!(error.contains("未闭合"));
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

    #[test]
    fn test_build_request_uses_canonical_command_names() {
        let (cmd, request) = build_request(
            BuiltinCommand::CharacterStateGet,
            &CommandOptions::default(),
            &["7".to_string()],
            1,
        )
        .unwrap();
        assert_eq!(cmd, "character state get");
        assert_eq!(request["character_id"], 7);

        let (cmd, request) = build_request(
            BuiltinCommand::MemoryList,
            &CommandOptions::default(),
            &[],
            2,
        )
        .unwrap();
        assert_eq!(cmd, "memory list");
        assert!(request.is_object());
    }

    #[test]
    fn test_build_envelope_uses_structured_operation() {
        let envelope = build_envelope(
            BuiltinCommand::CharacterStateGet,
            &CommandOptions::default(),
            &["7".to_string()],
            9,
        )
        .expect("结构化请求应构造成功");
        let encoded = serde_json::to_value(envelope).expect("请求应可序列化");
        assert_eq!(encoded["version"], 1);
        assert_eq!(encoded["id"], 9);
        assert_eq!(encoded["request"]["operation"], "character_state_get");
        assert_eq!(encoded["request"]["params"]["character_id"], 7);
        assert!(encoded.get("cmd").is_none());
    }

    #[test]
    fn test_unknown_reply_list_uses_shared_command_schema() {
        let command = BuiltinCommand::from_path(&[
            "reply".to_string(),
            "unknown".to_string(),
            "list".to_string(),
        ]);
        assert_eq!(command, Some(BuiltinCommand::ReplyUnknownList));
        let (name, request) = build_request(
            BuiltinCommand::ReplyUnknownList,
            &CommandOptions {
                limit: Some(12),
                ..Default::default()
            },
            &[],
            17,
        )
        .expect("unknown reply list should build");
        assert_eq!(
            name,
            yomua_bot::application::control_schema::REPLY_UNKNOWN_LIST
        );
        assert_eq!(request["limit"], serde_json::json!(12));
    }

    #[test]
    fn test_unknown_reply_check_and_retry_use_message_id_and_confirmation_policy() {
        let check = BuiltinCommand::from_path(&[
            "reply".to_string(),
            "unknown".to_string(),
            "check".to_string(),
        ])
        .expect("check command should parse");
        let (name, request) =
            build_request(check, &CommandOptions::default(), &["42".to_string()], 18).unwrap();
        assert_eq!(name, "reply unknown check");
        assert_eq!(request["message_id"], serde_json::json!(42));

        let retry = BuiltinCommand::from_path(&[
            "reply".to_string(),
            "unknown".to_string(),
            "retry".to_string(),
        ])
        .expect("retry command should parse");
        assert!(
            yomua_bot::application::control_schema::requires_confirmation(retry.canonical_name())
        );
    }

    #[test]
    fn dangerous_commands_reject_unexpected_positionals() {
        let error = build_request(
            BuiltinCommand::Shutdown,
            &CommandOptions::default(),
            &["typo".into()],
            1,
        )
        .expect_err("shutdown must not ignore extra arguments");
        assert!(error.contains("不接受位置参数"));

        let error = build_request(
            BuiltinCommand::SessionDeleteAll,
            &CommandOptions::default(),
            &["accidental".into()],
            2,
        )
        .expect_err("delete-all must not ignore extra arguments");
        assert!(error.contains("不接受位置参数"));
    }

    #[test]
    fn duration_parser_rejects_integer_overflow() {
        let error = parse_duration_to_secs("9223372036854775807d")
            .expect_err("overflowing duration must be rejected");
        assert!(error.contains("超出允许范围"));
        assert!(parse_duration_to_secs("1h").is_ok());
    }

    #[test]
    fn raw_response_keeps_envelope_for_success_and_failure() {
        let response = ControlResponse {
            id: 7,
            ok: false,
            data: None,
            error: Some(ControlErrorDetail {
                code: "BAD".into(),
                message: "失败".into(),
            }),
        };
        let encoded = serde_json::to_value(&response).expect("response should serialize");
        assert_eq!(encoded["id"], serde_json::json!(7));
        assert_eq!(encoded["ok"], serde_json::json!(false));
        assert_eq!(encoded["error"]["code"], serde_json::json!("BAD"));
    }

    #[test]
    fn test_human_state_alias_is_accepted_but_not_sent_to_bot() {
        assert_eq!(
            BuiltinCommand::from_path(&["state".into(), "get".into()]),
            Some(BuiltinCommand::StateGet)
        );
        assert!(!BuiltinCommand::all().any(|command| command == BuiltinCommand::StateGet));
    }
}
