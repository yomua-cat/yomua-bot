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
#[serde(default)]
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

    /// 默认角色 ID（当新会话没有既有 Binding 时，自动绑定到该角色）。
    /// 为 None 时，新会话不会自动绑定，需要管理员显式配置。
    pub default_character_id: Option<i64>,
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
            default_character_id: None,
        }
    }
}

/// 单个 LLM Provider 的配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmProviderConfig {
    /// Provider 类型（当前仅支持 "openai-compatible"）。
    pub provider_type: String,

    /// 服务根地址。
    pub base_url: String,

    /// 可选的 API Key。
    pub api_key: Option<String>,

    /// 超时时间（秒）。
    pub timeout_secs: Option<u64>,
}

/// 单个 LLM Model Instance 的配置。
///
/// Model Instance 是模型选择的原子单位：
/// - 引用某个已注册的 Provider（按名称）
/// - 指定在该 Provider 上使用的模型名称
/// - 携带该模型的 capability（context_window、max_output_tokens）
///
/// 一个 Model Instance 可以被多个 Role 或多个 Conversation 共同引用，
/// 实现真正的"同一模型，多个调用方"语义。
///
/// ```toml
/// [llm.model_instances.charglm-4]
/// provider = "zhipu"
/// model = "charglm-4"
/// context_window = 4096
/// max_output_tokens = 512
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmModelInstanceConfig {
    /// 使用哪个 Provider（名称须在 `providers` 中存在）。
    pub provider: String,

    /// 在该 Provider 上使用的模型名称（如 "charglm-4"、"MiniMax-M2-her"）。
    pub model: String,

    /// 模型上下文窗口（token 上限）。
    pub context_window: Option<usize>,

    /// 最大输出 token 数。
    pub max_output_tokens: Option<usize>,
}

/// 单个 LLM Role 的配置（引用某个已注册的 Model Instance）。
///
/// Role 是调用方视角的业务语义，自身不持有 Provider/Model/Capability。
///
/// ```toml
/// [llm.roles.decision]
/// model_instance = "charglm-4"
///
/// [llm.roles.conversation]
/// model_instance = "minimax-m2-her"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmRoleConfig {
    /// 引用的 Model Instance 名称。
    pub model_instance: String,

    /// 按顺序尝试的备用 Model Instance。
    #[serde(default)]
    pub fallback_model_instances: Vec<String>,

    /// 此任务单次请求最多尝试的模型数。省略时使用主模型加全部备用模型。
    #[serde(default)]
    pub max_attempts: Option<usize>,

    /// 此任务单次请求允许生成的输出 token 上限；备用模型共享该上限。
    #[serde(default)]
    pub budget_tokens: Option<u32>,
}

/// 决策任务使用的模型引用。Provider 连接参数统一来自 llm.toml。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecisionModelConfig {
    pub provider: String,
    pub model: String,
    pub context_window: Option<usize>,
    pub max_output_tokens: Option<usize>,
}

/// 独立的 decision.toml 配置。
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct DecisionConfig {
    pub enabled: bool,
    pub primary: Option<DecisionModelConfig>,
    pub fallbacks: Vec<DecisionModelConfig>,
    pub max_attempts: Option<usize>,
    pub budget_tokens: Option<u32>,
}

/// LLM 配置。
///
/// 支持两种格式：
/// 1. 旧格式（仅 `[options]`）：当前单 Provider 配置，单一 Provider 用于 Conversation
/// 2. 新格式（`providers` + `roles`）：多 Provider + 多 Role 路由
///
/// 当 `providers` 存在时，`options` 仅作当前单 Provider 配置用（无 providers 时 fallback）；
/// 当 `roles` 存在时，`roles.conversation` 定义 Conversation Role 的默认配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    /// 是否启用 LLM。
    pub enabled: bool,

    /// 【单 Provider 格式】优先使用的 Provider 名称（例如 "ollama"、"openai"）。
    /// 仅在未配置多 Provider 时使用。
    pub provider: Option<String>,

    /// 【单 Provider 格式】提供给 Provider 的任意附加配置（base_url / api_key / model / timeout_secs）。
    /// 仅在未配置多 Provider 时使用。
    pub options: serde_json::Value,

    /// 【新格式】Provider 注册表。key 为 Provider 名称。
    /// 示例: { "zhipu": { "provider_type": "openai-compatible", "base_url": "...", ... } }
    #[serde(default)]
    pub providers: std::collections::HashMap<String, LlmProviderConfig>,

    /// 【新格式】Model Instance 注册表。key 为 Model Instance 名称。
    /// 示例: { "charglm-4": { provider = "zhipu", model = "charglm-4", context_window = 4096, ... } }
    /// 一个 Model Instance 可被多个 Role 引用，实现真正的"同一模型，多个调用方"。
    #[serde(default)]
    pub model_instances: std::collections::HashMap<String, LlmModelInstanceConfig>,

    /// 【新格式】Role → Model Instance 配置映射。
    /// key 为 Role 名称（如 "conversation"、"cognition"）。决策模型单独配置在 decision.toml。
    /// Role 配置引用 `model_instances` 中已注册的 Model Instance。
    #[serde(default)]
    pub roles: std::collections::HashMap<String, LlmRoleConfig>,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: None,
            options: serde_json::json!({}),
            providers: std::collections::HashMap::new(),
            model_instances: std::collections::HashMap::new(),
            roles: std::collections::HashMap::new(),
        }
    }
}

/// 已校验并展开的模型路由，启动装配只使用这一份结果。
#[derive(Debug, Clone)]
pub struct ResolvedLlmConfig {
    pub providers: std::collections::HashMap<String, LlmProviderConfig>,
    pub roles: std::collections::HashMap<String, LlmModelInstanceConfig>,
    pub routes: std::collections::HashMap<String, ResolvedLlmRoute>,
    pub default_provider: String,
}

/// 一个任务的主模型和备用模型链。
#[derive(Debug, Clone)]
pub struct ResolvedLlmRoute {
    pub models: Vec<LlmModelInstanceConfig>,
    pub max_attempts: usize,
    pub budget_tokens: Option<u32>,
}

impl ResolvedLlmConfig {
    pub fn conversation_model(&self) -> &LlmModelInstanceConfig {
        &self.roles["conversation"]
    }
}

impl LlmConfig {
    /// 校验配置并确定全部任务的 Provider、模型与能力信息。
    pub fn resolve(&self) -> Result<Option<ResolvedLlmConfig>, RuntimeError> {
        use std::collections::HashMap;
        if !self.enabled {
            return Ok(None);
        }
        let mut providers = self.providers.clone();
        let mut roles = HashMap::new();
        let mut routes = HashMap::new();
        if providers.is_empty() {
            if !self.roles.is_empty() || !self.model_instances.is_empty() {
                return Err(RuntimeError::Config("模型实例配置缺少 providers".into()));
            }
            let kind = self.provider.as_deref().unwrap_or("openai-compatible");
            if !["openai-compatible", "openai", "ollama"].contains(&kind) {
                return Err(RuntimeError::Config(format!(
                    "不支持的模型 Provider：{kind}"
                )));
            }
            let model = self
                .options
                .get("model")
                .and_then(|v| v.as_str())
                .filter(|v| !v.trim().is_empty())
                .ok_or_else(|| RuntimeError::Config("启用模型时必须设置 options.model".into()))?;
            let base_url = self
                .options
                .get("base_url")
                .and_then(|v| v.as_str())
                .unwrap_or(if kind == "ollama" {
                    "http://127.0.0.1:11434/v1"
                } else {
                    "https://api.openai.com/v1"
                });
            providers.insert(
                "default".into(),
                LlmProviderConfig {
                    provider_type: "openai-compatible".into(),
                    base_url: base_url.into(),
                    api_key: self
                        .options
                        .get("api_key")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                    timeout_secs: self.options.get("timeout_secs").and_then(|v| v.as_u64()),
                },
            );
            let capability = |name: &str| -> Result<Option<usize>, RuntimeError> {
                self.options
                    .get(name)
                    .map(|v| {
                        v.as_u64()
                            .and_then(|v| usize::try_from(v).ok())
                            .ok_or_else(|| {
                                RuntimeError::Config(format!("options.{name} 必须为正整数"))
                            })
                    })
                    .transpose()
            };
            roles.insert(
                "conversation".into(),
                LlmModelInstanceConfig {
                    provider: "default".into(),
                    model: model.into(),
                    context_window: capability("context_window")?,
                    max_output_tokens: capability("max_output_tokens")?,
                },
            );
            routes.insert(
                "conversation".into(),
                ResolvedLlmRoute {
                    models: vec![roles["conversation"].clone()],
                    max_attempts: 1,
                    budget_tokens: self
                        .options
                        .get("budget_tokens")
                        .and_then(|v| v.as_u64())
                        .and_then(|v| u32::try_from(v).ok()),
                },
            );
        } else {
            for (role, config) in &self.roles {
                if !["conversation", "cognition"].contains(&role.as_str()) {
                    return Err(RuntimeError::Config(format!("未知的模型任务角色：{role}")));
                }
                let model = self
                    .model_instances
                    .get(&config.model_instance)
                    .ok_or_else(|| {
                        RuntimeError::Config(format!(
                            "任务 {role} 引用的模型实例不存在：{}",
                            config.model_instance
                        ))
                    })?;
                roles.insert(role.clone(), model.clone());

                let mut models = vec![model.clone()];
                let mut seen = std::collections::HashSet::from([config.model_instance.clone()]);
                for fallback_name in &config.fallback_model_instances {
                    if !seen.insert(fallback_name.clone()) {
                        return Err(RuntimeError::Config(format!(
                            "任务 {role} 的备用模型重复引用：{fallback_name}"
                        )));
                    }
                    let fallback = self.model_instances.get(fallback_name).ok_or_else(|| {
                        RuntimeError::Config(format!(
                            "任务 {role} 引用的备用模型实例不存在：{fallback_name}"
                        ))
                    })?;
                    models.push(fallback.clone());
                }
                let max_attempts = config.max_attempts.unwrap_or(models.len());
                if max_attempts == 0 || max_attempts > models.len() {
                    return Err(RuntimeError::Config(format!(
                        "任务 {role} 的 max_attempts 必须在 1 到 {} 之间",
                        models.len()
                    )));
                }
                if config.budget_tokens == Some(0) {
                    return Err(RuntimeError::Config(format!(
                        "任务 {role} 的 budget_tokens 必须大于 0"
                    )));
                }
                routes.insert(
                    role.clone(),
                    ResolvedLlmRoute {
                        models,
                        max_attempts,
                        budget_tokens: config.budget_tokens,
                    },
                );
            }
        }
        let conversation = roles
            .get("conversation")
            .cloned()
            .ok_or_else(|| RuntimeError::Config("启用模型时必须配置 conversation 任务".into()))?;
        roles
            .entry("cognition".into())
            .or_insert_with(|| conversation.clone());
        routes
            .entry("cognition".into())
            .or_insert_with(|| ResolvedLlmRoute {
                models: vec![conversation.clone()],
                max_attempts: 1,
                budget_tokens: None,
            });
        for (name, provider) in &providers {
            if provider.provider_type != "openai-compatible" {
                return Err(RuntimeError::Config(format!(
                    "Provider {name} 类型不支持：{}",
                    provider.provider_type
                )));
            }
            let url = reqwest::Url::parse(&provider.base_url)
                .map_err(|_| RuntimeError::Config(format!("Provider {name} 地址无效")))?;
            if !["http", "https"].contains(&url.scheme())
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err(RuntimeError::Config(format!(
                    "Provider {name} 地址必须为无内嵌凭据的 HTTP(S) URL"
                )));
            }
            if provider.timeout_secs == Some(0) {
                return Err(RuntimeError::Config(format!(
                    "Provider {name} 超时必须大于 0"
                )));
            }
        }
        for (role, model) in &roles {
            if !providers.contains_key(&model.provider) || model.model.trim().is_empty() {
                return Err(RuntimeError::Config(format!(
                    "任务 {role} 的模型或 Provider 无效"
                )));
            }
            if model.context_window == Some(0)
                || model.max_output_tokens == Some(0)
                || model
                    .max_output_tokens
                    .is_some_and(|v| v > u32::MAX as usize)
                || matches!((model.context_window, model.max_output_tokens), (Some(c), Some(o)) if o >= c)
            {
                return Err(RuntimeError::Config(format!(
                    "任务 {role} 的上下文或输出预算无效"
                )));
            }
        }
        Ok(Some(ResolvedLlmConfig {
            providers,
            roles,
            routes,
            default_provider: conversation.provider,
        }))
    }

    /// 返回是否启用了多 Provider 模式（`providers` 非空）。
    pub fn has_multi_provider(&self) -> bool {
        !self.providers.is_empty()
    }

    /// 返回是否配置了 Model Instance 注册表（`model_instances` 非空）。
    pub fn has_model_instances(&self) -> bool {
        !self.model_instances.is_empty()
    }

    /// 获取指定名称的 Model Instance 配置。
    pub fn model_instance(&self, name: &str) -> Option<&LlmModelInstanceConfig> {
        self.model_instances.get(name)
    }

    /// 获取 Conversation Role 引用的 Model Instance 配置。
    ///
    /// 优先使用 `roles["conversation"]`；若不存在，
    /// fallback 到旧格式的 `options`（backward compatible）。
    pub fn conversation_role_model_instance(&self) -> Option<&LlmModelInstanceConfig> {
        self.roles
            .get("conversation")
            .and_then(|r| self.model_instances.get(r.model_instance.as_str()))
    }

    /// 获取 Decision Role 引用的 Model Instance 配置。
    pub fn decision_role_model_instance(&self) -> Option<&LlmModelInstanceConfig> {
        self.roles
            .get("decision")
            .and_then(|r| self.model_instances.get(r.model_instance.as_str()))
    }

    /// 获取指定名称的 Provider 配置。
    pub fn provider_config(&self, name: &str) -> Option<&LlmProviderConfig> {
        self.providers.get(name)
    }

    /// 将独立 decision.toml 转换为调度器路由。
    pub fn apply_decision_config(
        resolved: &mut ResolvedLlmConfig,
        decision: &DecisionConfig,
    ) -> Result<(), RuntimeError> {
        if !decision.enabled {
            return Ok(());
        }
        let primary = decision
            .primary
            .clone()
            .ok_or_else(|| RuntimeError::Config("decision.toml 启用后必须配置 primary".into()))?;
        let mut models = vec![primary];
        models.extend(decision.fallbacks.clone());
        let max_attempts = decision.max_attempts.unwrap_or(models.len());
        if max_attempts == 0 || max_attempts > models.len() {
            return Err(RuntimeError::Config(format!(
                "decision.toml 的 max_attempts 必须在 1 到 {} 之间",
                models.len()
            )));
        }
        if decision.budget_tokens == Some(0) {
            return Err(RuntimeError::Config(
                "decision.toml 的 budget_tokens 必须大于 0".into(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for model in &models {
            if model.provider.trim().is_empty() || model.model.trim().is_empty() {
                return Err(RuntimeError::Config(
                    "decision.toml 的 Provider 和 model 不能为空".into(),
                ));
            }
            if !seen.insert((model.provider.clone(), model.model.clone())) {
                return Err(RuntimeError::Config(format!(
                    "decision.toml 的模型重复：{} / {}",
                    model.provider, model.model
                )));
            }
            if !resolved.providers.contains_key(&model.provider) {
                return Err(RuntimeError::Config(format!(
                    "decision.toml 引用的 Provider 未注册：{}",
                    model.provider
                )));
            }
            if model.context_window == Some(0) || model.max_output_tokens == Some(0) {
                return Err(RuntimeError::Config(
                    "decision.toml 的上下文和输出预算必须大于 0".into(),
                ));
            }
        }
        resolved.routes.insert(
            "decision".into(),
            ResolvedLlmRoute {
                models: models
                    .into_iter()
                    .map(|model| LlmModelInstanceConfig {
                        provider: model.provider,
                        model: model.model,
                        context_window: model.context_window,
                        max_output_tokens: model.max_output_tokens,
                    })
                    .collect(),
                max_attempts,
                budget_tokens: decision.budget_tokens,
            },
        );
        Ok(())
    }
}

/// 配置文件模板常量。
/// runtime.toml 模板（首次启动自动生成）。
pub const RUNTIME_TEMPLATE: &str = r#"# yomua-bot 运行时配置
# 标准位置：项目/服务根目录下的 config/runtime.toml
# 首次启动自动生成。请先填写管理员 QQ，再启动常驻服务。
# 文件可能包含本地路径和权限边界，不要提交到版本库。

# SQLite、控制 socket 和运行时状态目录。
# 相对路径按启动目录解析；生产部署建议填写绝对路径。
data_dir = "data"

# 日志级别：trace / debug / info / warn / error
log_level = "info"

# 优雅关停的整体超时（秒）。
shutdown_timeout_secs = 10

# 插件目录；删除注释并填写路径后启用插件。
# plugins_dir = "plugins"

# 能执行管理操作的 QQ 号。至少填写一个真实管理员 ID。
# 终端 ctl 默认允许本机操作，QQ 指令仍会校验此列表。
admin_users = []

# 事件广播缓存容量；通常保持默认即可。
# broadcast_capacity = 256

# 默认角色 ID。当前推荐通过 import-card 或 ctl 显式绑定，避免误绑定。
# default_character_id = 1
"#;

/// onebot.toml 模板。
pub const ONEBOT_TEMPLATE: &str = r#"# yomua-bot OneBot 连接配置
# 标准位置：项目/服务根目录下的 config/onebot.toml
# 本文件只连接本机或受信任的 OneBot 11 服务，不包含 QQ 登录凭据。

# NapCat 正向 WebSocket 地址。
websocket_url = "ws://127.0.0.1:3001"

# 与 NapCat websocketServers 配置一致；未启用鉴权时保留空字符串。
# 令牌只保存在本地配置，不要提交到版本库。
# access_token = ""

# Bot 自身 QQ 号，用于识别平台回声和自身消息。
# bot_self_id = 3524189564

# 断线重连和心跳。
reconnect_interval_secs = 1
max_reconnect_interval_secs = 60
heartbeat_interval_secs = 30

# 单次 Action（如发消息）等待回执的超时时间（秒）。
action_timeout_secs = 10
"#;

/// llm.toml 模板。
pub const LLM_TEMPLATE: &str = r#"# yomua-bot LLM 配置
# 标准位置：项目/服务根目录下的 config/llm.toml
# 默认关闭模型。只有确认 Provider、模型名和上下文窗口后才启用。
# API Key 只写在本地文件，不要提交或粘贴到问题反馈中。

enabled = false

# 首次发布支持单 Provider 模式，适合先跑通一条对话链路。
# provider = "openai-compatible"

# 启用后取消以下注释并替换全部占位值。
[options]
# model = "REPLACE_WITH_MODEL_NAME"
# base_url = "https://api.example.invalid/v1"
# api_key = "REPLACE_WITH_LLM_API_KEY"
# timeout_secs = 60
# context_window = 8192
# max_output_tokens = 1024
# output_reservation = 1024

# 多 Provider 配置示例：取消注释并填写后，可让对话和认知使用不同模型。
# 备用模型只在网络错误、超时、429 或 5xx 时尝试；400/401/响应格式错误不会盲目切换。
# [providers.primary]
# provider_type = "openai-compatible"
# base_url = "https://api.example.invalid/v1"
# api_key = "REPLACE_WITH_PRIMARY_KEY"
# timeout_secs = 60
#
# [providers.backup]
# provider_type = "openai-compatible"
# base_url = "https://api.backup.example.invalid/v1"
# api_key = "REPLACE_WITH_BACKUP_KEY"
# timeout_secs = 60
#
# [model_instances.conversation]
# provider = "primary"
# model = "REPLACE_WITH_CONVERSATION_MODEL"
# context_window = 8192
# max_output_tokens = 1024
#
# [model_instances.conversation_backup]
# provider = "backup"
# model = "REPLACE_WITH_CONVERSATION_BACKUP_MODEL"
# context_window = 8192
# max_output_tokens = 1024
#
# [roles.conversation]
# model_instance = "conversation"
# fallback_model_instances = ["conversation_backup"]
# max_attempts = 2
# budget_tokens = 4096
#
# [roles.cognition]
# model_instance = "conversation"
# max_attempts = 1
# budget_tokens = 4096
"#;

/// decision.toml 模板。
pub const DECISION_TEMPLATE: &str = r#"# yomua-bot 决策模型配置
# 标准位置：项目/服务根目录下的 config/decision.toml
# Provider 的 URL、密钥和名称在 llm.toml 的 [providers.*] 中统一维护。
# 本文件只选择决策任务使用的模型，以及可选备用模型。

enabled = false

# 启用后填写主模型。
# [primary]
# provider = "primary"
# model = "REPLACE_WITH_DECISION_MODEL"
# context_window = 8192
# max_output_tokens = 256

# 只有网络错误、超时、429 或 5xx 才会按顺序切换备用模型。
# [[fallbacks]]
# provider = "backup"
# model = "REPLACE_WITH_DECISION_BACKUP_MODEL"
# context_window = 8192
# max_output_tokens = 256

# 省略时尝试主模型加全部备用模型。
# max_attempts = 2
# 主模型和备用模型共享本次请求的 token 上限。
# budget_tokens = 1024
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
    if cfg.data_dir.trim().is_empty() {
        errors.push("data_dir 不能为空".to_string());
    }

    // admin_users 必须非空（否则所有系统指令无法执行）。
    if cfg.admin_users.as_ref().is_none_or(|list| list.is_empty()) {
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

/// 四份配置作为一个校验单元读取，重载失败时不产生部分更新。
#[derive(Clone)]
pub struct ConfigurationBundle {
    pub runtime: RuntimeConfig,
    pub onebot: OneBotConfig,
    pub llm: LlmConfig,
    pub decision: DecisionConfig,
}

impl ConfigurationBundle {
    pub fn load(directory: &std::path::Path) -> Result<Self, RuntimeError> {
        let bundle = Self {
            runtime: load_runtime(&directory.join("runtime.toml").display().to_string())?,
            onebot: load_onebot(&directory.join("onebot.toml").display().to_string())?,
            llm: load_llm(&directory.join("llm.toml").display().to_string())?,
            decision: load_decision(&directory.join("decision.toml").display().to_string())?,
        };
        bundle.validate()?;
        Ok(bundle)
    }

    pub fn validate(&self) -> Result<(), RuntimeError> {
        let mut errors = validate_runtime(&self.runtime);
        errors.extend(validate_onebot(&self.onebot));
        if let Err(error) = self.llm.resolve() {
            errors.push(error.to_string());
        }
        if self.decision.enabled {
            match self.llm.resolve() {
                Ok(Some(mut resolved)) => {
                    if let Err(error) =
                        LlmConfig::apply_decision_config(&mut resolved, &self.decision)
                    {
                        errors.push(error.to_string());
                    }
                }
                Ok(None) => errors.push("decision.toml 启用时必须同时启用 llm.toml".into()),
                Err(_) => {}
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(RuntimeError::Config(errors.join("；")))
        }
    }

    /// 固定装配项不能假装已热更新，任何变化都保持当前整套配置。
    pub fn restart_required(&self, next: &Self) -> Vec<&'static str> {
        let mut fields = Vec::new();
        if self.runtime.data_dir != next.runtime.data_dir {
            fields.push("runtime.data_dir");
        }
        if self.runtime.plugins_dir != next.runtime.plugins_dir {
            fields.push("runtime.plugins_dir");
        }
        if self.runtime.broadcast_capacity != next.runtime.broadcast_capacity {
            fields.push("runtime.broadcast_capacity");
        }
        if self.runtime.default_character_id != next.runtime.default_character_id {
            fields.push("runtime.default_character_id");
        }
        if serde_json::to_value(&self.onebot).ok() != serde_json::to_value(&next.onebot).ok() {
            fields.push("onebot");
        }
        if serde_json::to_value(&self.llm).ok() != serde_json::to_value(&next.llm).ok() {
            fields.push("llm");
        }
        if serde_json::to_value(&self.decision).ok() != serde_json::to_value(&next.decision).ok() {
            fields.push("decision");
        }
        fields
    }
}

pub fn validate_onebot(config: &OneBotConfig) -> Vec<String> {
    let mut errors = Vec::new();
    if !(config.websocket_url.starts_with("ws://") || config.websocket_url.starts_with("wss://")) {
        errors.push("onebot.websocket_url 必须使用 ws:// 或 wss://".to_string());
    }
    if config.bot_self_id.is_some_and(|id| id <= 0) {
        errors.push("onebot.bot_self_id 必须为正整数".to_string());
    }
    if config.reconnect_interval_secs == 0
        || config.max_reconnect_interval_secs < config.reconnect_interval_secs
    {
        errors.push("OneBot 重连间隔必须大于零，最大间隔不能小于初始间隔".to_string());
    }
    if config.heartbeat_interval_secs == 0 || config.action_timeout_secs == 0 {
        errors.push("OneBot 心跳和请求超时必须大于零".to_string());
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

/// 加载独立决策模型配置。缺失时默认关闭决策模型。
pub fn load_decision(path: &str) -> Result<DecisionConfig, RuntimeError> {
    Ok(load_toml::<DecisionConfig>(path)?.unwrap_or_default())
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
    fn decision_config_parses_primary_and_fallbacks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("decision.toml");
        fs::write(
            &path,
            r#"enabled = true
max_attempts = 2
budget_tokens = 1024

[primary]
provider = "xai"
model = "grok-test"
context_window = 8192
max_output_tokens = 256

[[fallbacks]]
provider = "backup"
model = "backup-test"
context_window = 4096
max_output_tokens = 256
"#,
        )
        .unwrap();
        let cfg = load_decision(path.to_str().unwrap()).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.primary.as_ref().unwrap().provider, "xai");
        assert_eq!(cfg.fallbacks.len(), 1);
        assert_eq!(cfg.max_attempts, Some(2));
    }

    #[test]
    fn decision_template_is_valid_when_disabled() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("decision.toml");
        fs::write(&path, DECISION_TEMPLATE).unwrap();
        let cfg = load_decision(path.to_str().unwrap()).unwrap();
        assert!(!cfg.enabled);
        assert!(cfg.primary.is_none());
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
    fn onebot_template_parses_with_optional_placeholders_commented() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("onebot.toml");
        fs::write(&path, ONEBOT_TEMPLATE).unwrap();
        let cfg = load_onebot(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.websocket_url, "ws://127.0.0.1:3001");
        assert_eq!(cfg.access_token, None);
        assert_eq!(cfg.bot_self_id, None);
        assert!(validate_onebot(&cfg).is_empty());
    }

    #[test]
    fn malformed_config_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.toml");
        fs::write(&path, "data_dir = \nlog_level =").unwrap();
        assert!(load_runtime(path.to_str().unwrap()).is_err());
    }
}
