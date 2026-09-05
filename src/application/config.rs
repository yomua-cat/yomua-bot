//! 配置系统 —— 从 TOML 文件读取运行配置。
//!
//! 第一阶段提供三个配置文件：
//! - `runtime.toml` — 基础运行配置（数据目录、日志级别、关停超时）
//! - `onebot.toml`  — OneBot WebSocket 连接配置（见 `crate::adapters::onebot::OneBotConfig`）
//! - `llm.toml`     — LLM Provider 占位配置（本阶段未启用）
//!
//! 保持简单：不做 WebUI，不做配置热重载。

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
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            data_dir: "data".to_string(),
            log_level: "info".to_string(),
            shutdown_timeout_secs: 10,
            plugins_dir: None,
            admin_users: None,
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
            "websocket_url = \"ws://127.0.0.1:3001\"\naccess_token = \"token123\"\nreconnect_interval_secs = 2\nmax_reconnect_interval_secs = 30\nheartbeat_interval_secs = 20\n",
        )
        .unwrap();
        let cfg = load_onebot(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.websocket_url, "ws://127.0.0.1:3001");
        assert_eq!(cfg.access_token.as_deref(), Some("token123"));
        assert_eq!(cfg.reconnect_interval_secs, 2);
        assert_eq!(cfg.max_reconnect_interval_secs, 30);
        assert_eq!(cfg.heartbeat_interval_secs, 20);
    }

    #[test]
    fn malformed_config_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.toml");
        fs::write(&path, "data_dir = \nlog_level =").unwrap();
        assert!(load_runtime(path.to_str().unwrap()).is_err());
    }
}
