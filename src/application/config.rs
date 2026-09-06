//! 配置系统 —— 从 TOML 文件读取运行配置。
//!
//! 提供三个配置文件：
//! - `runtime.toml` — 基础运行配置（数据目录、日志级别、关停超时、管理员等）
//! - `onebot.toml`  — OneBot WebSocket 连接配置（见 `crate::adapters::onebot::OneBotConfig`）
//! - `llm.toml`     — LLM Provider 配置（见 `LlmConfig`）
//!
//! 支持首次启动自动生成模板文件、配置校验与热重载。

use serde::{Deserialize, Serialize};

use crate::adapters::onebot::OneBotConfig;
use crate::error::RuntimeError;

/// 基础运行配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// 数据目录（存放 SQLite 数据库等）。
    pub data_dir: String,

    /// 日志级别（例如 "info"、"debug"）。
    pub log_level: String,

    /// 优雅关停时的最大等待秒数。
    pub shutdown_timeout_secs: u64,

    /// 插件目录（为 `None` 时默认禁用插件系统）。
    pub plugins_dir: Option<String>,

    /// 管理员用户外部 ID 列表（如 QQ 号）。为 None 时无人可执行系统指令。
    pub admin_users: Option<Vec<String>>,

    /// 事件总线广播通道容量（默认 256）。
    /// 当订阅者消费速度慢于生产者发送速度时，超出容量的事件会被丢弃。
    /// 高频消息场景下可适当调大。
    pub broadcast_capacity: Option<usize>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            data_dir: "data".to_string(),
            log_level: "info".to_string(),
            shutdown_timeout_secs: 10,
            plugins_dir: None,
            admin_users: None,
            broadcast_capacity: None,
        }
    }
}

/// LLM Provider 配置（第一阶段仅作为占位，默认未启用）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    /// 是否启用 LLM。
    pub enabled: bool,

    /// 优先使用的 Provider 名称（例如 "ollama"、"openai"）。
    pub provider: Option<String>,

    /// 提供给 Provider 的任意附加配置。
    pub options: serde_json::Value,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: None,
            options: serde_json::json!({}),
        }
    }
}

/// 配置文件模板常量。
/// runtime.toml 模板（首次启动自动生成）。
pub const RUNTIME_TEMPLATE: &str = r#"# yomua-bot 运行时配置
# 首次启动自动生成，请根据实际情况修改各字段。

# 数据目录（存放 SQLite 数据库、插件 socket 等）。
# 建议使用绝对路径。
data_dir = "data"

# 日志级别：trace / debug / info / warn / error。
log_level = "info"

# 优雅关停最大等待秒数。
shutdown_timeout_secs = 10

# 插件目录（为 null 则禁用插件系统）。
# plugins_dir = "plugins"

# 管理员用户外部 ID 列表（如 QQ 号）。
# 【重要】未配置或为空将导致所有系统指令（换角色等）无法执行！
admin_users = []

# 事件总线广播通道容量（默认 256）。
# high-throughput 场景可调大。
# broadcast_capacity = 256
"#;

/// onebot.toml 模板。
pub const ONEBOT_TEMPLATE: &str = r#"# yomua-bot OneBot 连接配置
# 首次启动自动生成，请根据实际情况修改。

# NapCat WebSocket 地址。
websocket_url = "ws://127.0.0.1:3001"

# 访问令牌（与 NapCat 配置的 access_token 一致，留空则不认证）。
# access_token = ""

# 重连相关。
reconnect_interval_secs = 1
max_reconnect_interval_secs = 30
heartbeat_interval_secs = 30
"#;

/// llm.toml 模板。
pub const LLM_TEMPLATE: &str = r#"# yomua-bot LLM 配置
# 首次启动自动生成。默认 LLM 未启用（enabled = false）。

enabled = false

# Provider：ollama / openai / openai-compatible。
# provider = "ollama"

# 附加选项（provider 不同配置项不同）。
[options]
# model = "qwen2.5"
# base_url = "http://localhost:11434/v1"
# api_key = "ollama"
"#;

/// 将模板内容写入 path（如果文件不存在）。
/// 成功写入返回 true，文件已存在返回 false。
pub fn write_template_if_missing(path: &str, template: &str) -> std::io::Result<bool> {
    let path = std::path::Path::new(path);
    if path.exists() {
        return Ok(false);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, template)?;
    Ok(true)
}

/// 校验运行时配置，返回所有错误列表（空表示校验通过）。
pub fn validate_runtime(cfg: &RuntimeConfig) -> Vec<String> {
    let mut errors = Vec::new();

    // admin_users 必须非空（否则所有系统指令无法执行）。
    if cfg
        .admin_users
        .as_ref()
        .map_or(true, |list| list.is_empty())
    {
        errors.push("admin_users 未配置或为空！系统指令将无法执行。请在 runtime.toml 中添加 admin_users 字段。".to_string());
    }

    // log_level 必须是合法值。
    let valid_levels = ["trace", "debug", "info", "warn", "error"];
    if !valid_levels.contains(&cfg.log_level.as_str()) {
        errors.push(format!(
            "无效的 log_level：{}，有效值：{:?}",
            cfg.log_level, valid_levels
        ));
    }

    // shutdown_timeout_secs 必须为正数。
    if cfg.shutdown_timeout_secs == 0 {
        errors.push("shutdown_timeout_secs 必须大于 0".to_string());
    }

    // broadcast_capacity 如果设置了必须为正数。
    if let Some(cap) = cfg.broadcast_capacity {
        if cap == 0 {
            errors.push("broadcast_capacity 必须大于 0".to_string());
        }
    }

    errors
}

/// 从路径读取一个 TOML 文件，并在文件缺失或解析失败时提供清晰的错误。
fn load_toml<T: for<'de> Deserialize<'de>>(path: &str) -> Result<Option<T>, RuntimeError> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(RuntimeError::Config(format!(
                "无法读取配置文件 {path}: {e}"
            )));
        }
    };

    let value: T = toml::from_str(&content)
        .map_err(|e| RuntimeError::Config(format!("无法解析配置文件 {path}: {e}")))?;
    Ok(Some(value))
}

/// 加载运行时配置。若 `runtime.toml` 不存在，则使用默认值。
pub fn load_runtime(path: &str) -> Result<RuntimeConfig, RuntimeError> {
    Ok(load_toml::<RuntimeConfig>(path)?.unwrap_or_default())
}

/// 加载 OneBot 配置。若 `onebot.toml` 不存在，则使用默认值。
pub fn load_onebot(path: &str) -> Result<OneBotConfig, RuntimeError> {
    Ok(load_toml::<OneBotConfig>(path)?.unwrap_or_default())
}

/// 加载 LLM 配置。若 `llm.toml` 不存在，则使用默认值（未启用）。
pub fn load_llm(path: &str) -> Result<LlmConfig, RuntimeError> {
    Ok(load_toml::<LlmConfig>(path)?.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn runtime_config_default_when_missing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.toml");
        let cfg = load_runtime(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.data_dir, "data");
        assert_eq!(cfg.log_level, "info");
    }

    #[test]
    fn runtime_config_parses_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.toml");
        fs::write(
            &path,
            "data_dir = \"/tmp/runtime-data\"\nlog_level = \"debug\"\nshutdown_timeout_secs = 5\n",
        )
        .unwrap();
        let cfg = load_runtime(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.data_dir, "/tmp/runtime-data");
        assert_eq!(cfg.log_level, "debug");
        assert_eq!(cfg.shutdown_timeout_secs, 5);
    }

    #[test]
    fn runtime_config_plugins_dir_default_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.toml");
        let cfg = load_runtime(path.to_str().unwrap()).unwrap();
        // 默认不启用插件系统
        assert_eq!(cfg.plugins_dir, None);
    }

    #[test]
    fn runtime_config_plugins_dir_parses_some() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.toml");
        fs::write(
            &path,
            "data_dir = \"/tmp/runtime-data\"\nlog_level = \"info\"\nshutdown_timeout_secs = 10\nplugins_dir = \"plugins\"\n",
        )
        .unwrap();
        let cfg = load_runtime(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.plugins_dir.as_deref(), Some("plugins"));
    }

    #[test]
    fn runtime_config_admin_users_default_none() {
        // 未配置 admin_users → None（无人可执行系统指令）。
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.toml");
        let cfg = load_runtime(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.admin_users, None);
    }

    #[test]
    fn runtime_config_admin_users_parses() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.toml");
        fs::write(
            &path,
            "data_dir = \"/tmp/runtime-data\"\nlog_level = \"info\"\nshutdown_timeout_secs = 10\nadmin_users = [\"10001\", \"10002\"]\n",
        )
        .unwrap();
        let cfg = load_runtime(path.to_str().unwrap()).unwrap();
        assert_eq!(
            cfg.admin_users,
            Some(vec!["10001".to_string(), "10002".to_string()])
        );
    }

    #[test]
    fn llm_config_disable_by_default() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("llm.toml");
        let cfg = load_llm(path.to_str().unwrap()).unwrap();
        assert!(!cfg.enabled);
    }

    #[test]
    fn llm_config_parses_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("llm.toml");
        fs::write(
            &path,
            "enabled = true\nprovider = \"ollama\"\n[options]\nmodel = \"qwen2.5\"\n",
        )
        .unwrap();
        let cfg = load_llm(path.to_str().unwrap()).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.provider.as_deref(), Some("ollama"));
        assert_eq!(cfg.options["model"], "qwen2.5");
    }

    #[test]
    fn onebot_config_parses_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("onebot.toml");
        fs::write(
            &path,
            "websocket_url = \"ws://127.0.0.1:3001\"\naccess_token = \"token123\"\nreconnect_interval_secs = 2\nmax_reconnect_interval_secs = 30\nheartbeat_interval_secs = 20\naction_timeout_secs = 10\n",
        )
        .unwrap();
        let cfg = load_onebot(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.websocket_url, "ws://127.0.0.1:3001");
        assert_eq!(cfg.access_token.as_deref(), Some("token123"));
        assert_eq!(cfg.reconnect_interval_secs, 2);
        assert_eq!(cfg.max_reconnect_interval_secs, 30);
        assert_eq!(cfg.heartbeat_interval_secs, 20);
        assert_eq!(cfg.action_timeout_secs, 10);
    }

    #[test]
    fn malformed_config_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.toml");
        fs::write(&path, "data_dir = \nlog_level =").unwrap();
        assert!(load_runtime(path.to_str().unwrap()).is_err());
    }
}
