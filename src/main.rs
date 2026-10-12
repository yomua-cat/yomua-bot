//! yomua-bot — Character Runtime
//!
//! 一个角色运行时 / 角色智能体框架。
//! QQ 是第一个消息适配器，但核心与平台无关。
//!
//! 启动流程：加载配置 → 初始化日志 → 打开存储 → 建立仓库 → 装配应用层
//! （Runtime / 行为 / 认知 / 情绪 / 关系）→ 启动订阅者 → 启动 OneBot 适配器 →
//! 等待关停信号。Core 始终存活，适配器断线会自动重连。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tracing_subscriber::EnvFilter;

use yomua_bot::adapters::onebot::{OneBotAdapter, OneBotAdapterImpl};
use yomua_bot::application::action::ActionDispatcher;
use yomua_bot::application::behavior_engine::RuleBehaviorEngine;
use yomua_bot::application::binding::BindingManager;
use yomua_bot::application::character_import::{
    BindOptions, CharacterImportService, ImportOptions,
};
use yomua_bot::application::cognition::CognitionLayer;
use yomua_bot::application::cognition_driver::CognitionDriver;
use yomua_bot::application::command::CommandHandler;
use yomua_bot::application::config::{
    load_llm, load_onebot, load_runtime, validate_runtime, write_template_if_missing, LlmConfig,
    LlmModelInstanceConfig, LLM_TEMPLATE, ONEBOT_TEMPLATE, RUNTIME_TEMPLATE,
};
use yomua_bot::application::context::ContextBuilder;
use yomua_bot::application::context_assembler::{
    ContextAssembler, ModelCapabilitiesProviderAdapter, TokenEstimatorAdapter,
};
use yomua_bot::application::control::{start_control_service, CommandRegistry, RuntimeHandle};
use yomua_bot::application::conversation::ConversationManager;
use yomua_bot::application::decision_orchestrator::{
    DecisionEvaluator, DecisionOrchestrator, LlmAssistanceTrigger, NoOpTrigger,
};
use yomua_bot::application::event_bus::EventBus;
use yomua_bot::application::event_processor::EventProcessor;
use yomua_bot::application::llm_scheduler::{
    DefaultLlmScheduler, EmbeddingScheduler, LlmScheduler,
};
use yomua_bot::application::memory_service::MemoryService;
use yomua_bot::application::message_acceptance::MessageAcceptanceService;
use yomua_bot::application::mood_service::MoodService;
use yomua_bot::application::outbox_dispatcher::{OutboxDispatcher, OutboxDispatcherConfig};
use yomua_bot::application::outbox_processor::OutboxProcessor;
use yomua_bot::application::plugin_api::PluginApi;
use yomua_bot::application::proactive::ProactiveDriver;
use yomua_bot::application::relationship_service::RelationshipService;
use yomua_bot::application::reply_processor::{DelayExecutor, ReplyProcessor, TokioDelayExecutor};
use yomua_bot::application::runtime::CharacterRuntime;
use yomua_bot::application::session_service::SessionService;
use yomua_bot::domain::behavior::BehaviorEngine;
use yomua_bot::domain::character::ReplyMode;
use yomua_bot::domain::conversation::ConversationType;
use yomua_bot::domain::repository::{
    BehaviorStateRepository, BlacklistRepository, CharacterBindingRepository, CharacterRepository,
    CharacterStateRepository, ConversationRepository, ConversationStateRepository,
    MemoryRepository, MessageRepository, MoodRepository, ParticipantRepository,
    PluginDataRepository, RelationshipRepository,
};
use yomua_bot::error::RuntimeError;
use yomua_bot::infrastructure::llm::openai_compatible::{
    OpenAiCompatibleConfig, OpenAiCompatibleProvider,
};
use yomua_bot::infrastructure::llm::{
    ApproximateTokenEstimator, FixedModelCapabilities, LlmProvider,
};
use yomua_bot::infrastructure::plugin::event_bridge::EventBridge;
use yomua_bot::infrastructure::plugin::registry::PluginRegistry;
use yomua_bot::infrastructure::plugin::supervisor::{PluginSupervisor, SupervisorConfig};
use yomua_bot::infrastructure::storage::repository::{
    SqliteBehaviorStateRepository, SqliteBlacklistRepository, SqliteCharacterBindingRepository,
    SqliteCharacterRepository, SqliteCharacterStateRepository, SqliteConversationRepository,
    SqliteConversationStateRepository, SqliteMemoryRepository, SqliteMessageRepository,
    SqliteMoodRepository, SqliteOutboxRepository, SqliteParticipantRepository,
    SqlitePluginDataRepository, SqliteRelationshipRepository,
};
use yomua_bot::infrastructure::storage::SqliteStorage;

/// 默认的配置文件名。
const RUNTIME_CONFIG: &str = "runtime.toml";
const ONEBOT_CONFIG: &str = "onebot.toml";
const LLM_CONFIG: &str = "llm.toml";

/// 从 `LlmConfig` 读取模型能力配置。
///
/// 从 `options` 中读取：
/// - `context_window`: 模型的上下文窗口大小（必须配置）
/// - `max_output_tokens`: 模型的最大输出 token 数（可选）
/// - `output_reservation`: 为输出预留的 input context 空间（可选，默认 512）
fn read_llm_capabilities(cfg: &LlmConfig) -> (Option<usize>, Option<usize>, usize) {
    let options = &cfg.options;

    let context_window = options
        .get("context_window")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);

    let max_output_tokens = options
        .get("max_output_tokens")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);

    // output_reservation 必须为正数
    let output_reservation = options
        .get("output_reservation")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .unwrap_or(512); // 默认值

    (context_window, max_output_tokens, output_reservation)
}

/// 使用 `llm_cfg.model_instances` 和 `llm_cfg.roles` 构建多 Provider 调度器。
///
/// 架构：
/// ```text
/// llm_cfg.roles (role -> model_instance name)
///     ↓
/// llm_cfg.model_instances (model_instance name -> provider + model + capabilities)
///     ↓
/// llm_cfg.providers (provider name -> HTTP transport)
/// ```
fn build_multi_provider_scheduler(
    llm_cfg: LlmConfig,
) -> Result<Arc<DefaultLlmScheduler>, RuntimeError> {
    use std::collections::HashMap;

    // Step 1: 构建 providers registry（name -> LlmProvider 实例）
    let mut providers: HashMap<String, Arc<dyn LlmProvider>> = HashMap::new();
    for provider_name in llm_cfg.providers.keys() {
        let p: Arc<dyn LlmProvider> = Arc::new(
            build_openai_provider(llm_cfg.clone(), Some(provider_name)).map_err(|e| {
                RuntimeError::Config(format!("LLM Provider '{}' 配置错误: {}", provider_name, e))
            })?,
        );
        tracing::info!(target = "llm", name = provider_name, impl = p.name(), "Provider 已加载");
        providers.insert(provider_name.clone(), p);
    }

    // Step 2: 构建 model_instances registry（name -> LlmModelInstanceConfig）
    let mut model_instances: HashMap<String, LlmModelInstanceConfig> = HashMap::new();
    for (mi_name, mi_config) in &llm_cfg.model_instances {
        if !providers.contains_key(&mi_config.provider) {
            return Err(RuntimeError::Config(format!(
                "ModelInstance '{}' 引用的 Provider '{}' 未注册",
                mi_name, mi_config.provider,
            )));
        }
        tracing::info!(
            target = "llm",
            instance = mi_name,
            provider = %mi_config.provider,
            model = %mi_config.model,
            "ModelInstance 已加载",
        );
        model_instances.insert(mi_name.clone(), mi_config.clone());
    }

    // Step 3: 加载 Role -> ModelInstance 路由配置
    let mut role_mis: HashMap<String, LlmModelInstanceConfig> = HashMap::new();
    for (role_name, role_config) in &llm_cfg.roles {
        let mi = model_instances
            .get(&role_config.model_instance)
            .ok_or_else(|| {
                RuntimeError::Config(format!(
                    "Role '{}' 引用的 ModelInstance '{}' 未注册",
                    role_name, role_config.model_instance,
                ))
            })?;
        tracing::info!(
            target = "llm",
            role = role_name,
            model_instance = %role_config.model_instance,
            provider = %mi.provider,
            model = %mi.model,
            "Role 路由已注册",
        );
        role_mis.insert(role_name.clone(), mi.clone());
    }

    if role_mis.is_empty() {
        return Err(RuntimeError::Config("roles 配置为空".to_string()));
    }

    // Step 4: 取第一个 Role 对应的 Provider 作为默认 inner
    let first_role = role_mis.keys().next().unwrap().clone();
    let first_mi = &role_mis[&first_role];
    let inner = providers.get(&first_mi.provider).unwrap();

    // Step 5: 创建调度器
    let scheduler = Arc::new(DefaultLlmScheduler::with_providers(
        Arc::clone(inner),
        4, // pending_limit
        4, // embedding_limit
        providers,
        role_mis,
    ));

    tracing::info!(target = "llm", primary_role = %first_role, "LLM 多 Provider 调度器已初始化");
    Ok(scheduler)
}

/// 从 `LlmConfig` 构造 OpenAI-compatible Provider。
///
/// 优先使用新格式 `providers[name]`；若不存在，则 fallback 到旧格式 `options`。
/// 这确保向后兼容：已有 `llm.toml` 不需要修改即可工作。
fn build_openai_provider(
    cfg: LlmConfig,
    name: Option<&str>,
) -> Result<OpenAiCompatibleProvider, RuntimeError> {
    // 新格式优先：从 providers registry 中查找
    if let Some(key) = name.or(cfg.provider.as_deref()) {
        if let Some(pc) = cfg.providers.get(key) {
            return build_provider_from_config(pc);
        }
    }

    // 旧格式 fallback：从 options 构建
    build_from_options(&cfg.options, cfg.provider.as_deref())
}

/// 从 `LlmProviderConfig` 构造单个 Provider。
///
/// 注意：此函数只负责建立 HTTP 传输层（base_url + api_key + timeout）。
/// 实际的 model 名称由 `LlmModelInstanceConfig.model` 指定，在路由时由 Scheduler 使用。
fn build_provider_from_config(
    pc: &yomua_bot::application::config::LlmProviderConfig,
) -> Result<OpenAiCompatibleProvider, RuntimeError> {
    let provider_type = pc.provider_type.as_str();
    if provider_type != "openai-compatible" {
        return Err(RuntimeError::Config(format!(
            "不支持的 Provider 类型: {provider_type}（仅支持 openai-compatible）"
        )));
    }

    Ok(OpenAiCompatibleProvider::new(OpenAiCompatibleConfig {
        base_url: pc.base_url.clone(),
        api_key: pc.api_key.clone(),
        model: String::new(), // 模型名由 LlmScheduler 根据 role 路由时动态指定
        embedding_model: None,
        timeout: Duration::from_secs(pc.timeout_secs.unwrap_or(60)),
    }))
}

/// 从旧的 `options` JSON 构建 Provider（向后兼容）。
fn build_from_options(
    options: &serde_json::Value,
    provider_name: Option<&str>,
) -> Result<OpenAiCompatibleProvider, RuntimeError> {
    let base_url = options
        .get("base_url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://127.0.0.1:11434/v1")
        .to_string();

    let api_key = options
        .get("api_key")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // 若配置了 provider 名（如 "ollama"）且未显式给出 model，则用 provider 名兜底。
    let model = options
        .get("model")
        .and_then(|v| v.as_str())
        .or(provider_name)
        .unwrap_or("default-model")
        .to_string();

    let timeout_secs = options
        .get("timeout_secs")
        .and_then(|v| v.as_u64())
        .unwrap_or(60);

    Ok(OpenAiCompatibleProvider::new(OpenAiCompatibleConfig {
        base_url,
        api_key,
        model,
        embedding_model: None,
        timeout: Duration::from_secs(timeout_secs),
    }))
}

/// 运行时 CLI 帮助文本。
const RUNTIME_HELP: &str = r#"yomua-bot —— Character Runtime

用法：
  yomua-bot [选项]

选项：
  --help, -h          显示本帮助信息
  --config-dir <目录>  指定配置目录（默认：当前目录）

无参数时，使用当前目录作为配置目录。
配置文件：<config-dir>/runtime.toml、<config-dir>/llm.toml、<config-dir>/onebot.toml。
运行数据目录在 runtime.toml 的 data_dir 字段中指定（默认 data/）。
"#;

/// 解析运行时参数（可供测试访问）。
///
/// 规则：
/// - `--help` / `-h`：打印帮助并以 0 退出
/// - `--config-dir <path>`：设置配置目录
/// - 第一个非 `-` 开头的 positional 参数：配置目录（向后兼容）
/// - 任何剩余的 `-` 开头的未知参数：报错
/// - 多余的 positional 参数：报错
pub(crate) fn parse_runtime_args(args: &[String]) -> Result<PathBuf, RuntimeError> {
    let mut config_dir: Option<PathBuf> = None;
    let mut i = 0;

    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--help" | "-h" => {
                print!("{}", RUNTIME_HELP);
                std::process::exit(0);
            }
            "--config-dir" => {
                i += 1;
                if i >= args.len() {
                    return Err(RuntimeError::Config(
                        "--config-dir 需要一个目录参数".to_string(),
                    ));
                }
                config_dir = Some(PathBuf::from(&args[i]));
                i += 1;
            }
            _ if !arg.starts_with('-') => {
                // 第一个 positional 参数作为配置目录（向后兼容）。
                if config_dir.is_none() {
                    config_dir = Some(PathBuf::from(arg));
                    i += 1;
                } else {
                    return Err(RuntimeError::Config(format!(
                        "不支持的位置参数：{arg}，运行 yomua-bot --help 查看用法"
                    )));
                }
            }
            _ => {
                return Err(RuntimeError::Config(format!(
                    "未知参数：{arg}，运行 yomua-bot --help 查看用法"
                )));
            }
        }
    }

    Ok(config_dir.unwrap_or_else(|| PathBuf::from(".")))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    // 子命令分发：import-card / list-characters / list-bindings / switch-character
    // 走各自命令流程；其余参数视为配置目录（兼容旧用法，直接启动运行时）。
    match args.first().map(String::as_str) {
        Some("import-card") => return run_import(&args[1..]).await,
        Some("list-characters") => return run_list_characters(&args[1..]).await,
        Some("list-bindings") => return run_list_bindings(&args[1..]).await,
        Some("switch-character") => return run_switch_character(&args[1..]).await,
        _ => {}
    }

    let config_dir = parse_runtime_args(&args)?;
    let _handle = run_runtime(&config_dir).await?;
    Ok(())
}

/// 启动常驻运行时。
///
/// 加载配置 → 初始化日志 → 打开存储 → 建立仓库 → 装配应用层
/// （Runtime / 行为 / 认知 / 情绪 / 关系）→ 启动订阅者 → 启动插件系统
/// （可选）→ 启动 OneBot 适配器 → 等待关停信号。
async fn run_runtime(config_dir: &Path) -> Result<RuntimeHandle, RuntimeError> {
    // 1. 配置初始化：文件不存在时生成模板并退出提示用户修改。
    let runtime_path = config_dir.join(RUNTIME_CONFIG);
    let onebot_path = config_dir.join(ONEBOT_CONFIG);
    let llm_path = config_dir.join(LLM_CONFIG);

    if !runtime_path.exists() {
        write_template_if_missing(&runtime_path.display().to_string(), RUNTIME_TEMPLATE).map_err(
            |e| RuntimeError::Config(format!("无法创建配置文件 {}: {e}", runtime_path.display())),
        )?;
        write_template_if_missing(&onebot_path.display().to_string(), ONEBOT_TEMPLATE).ok();
        write_template_if_missing(&llm_path.display().to_string(), LLM_TEMPLATE).ok();
        eprintln!(
            "错误: 配置文件不存在，已在当前目录生成模板文件。\n\
             请修改 runtime.toml 和 onebot.toml 后重新启动。\n\
             文件路径：\n\
             - ./runtime.toml\n\
             - ./onebot.toml\n\
             - ./llm.toml（可选）"
        );
        std::process::exit(1);
    }

    // 加载配置。
    let runtime_cfg = load_runtime(&runtime_path.display().to_string())?;
    let onebot_cfg = load_onebot(&onebot_path.display().to_string())?;
    let llm_cfg = load_llm(&llm_path.display().to_string())?;

    // 1.2 配置校验：所有错误同时报告，不静默忽略。
    // 注意：此时 tracing 尚未初始化，必须直接输出到 stderr，
    // 否则用户只会看到“配置校验失败 N 个错误”而不知道改哪个配置项。
    let validation_errors = validate_runtime(&runtime_cfg);
    if !validation_errors.is_empty() {
        for err in &validation_errors {
            eprintln!("配置错误：{err}");
        }
        return Err(RuntimeError::Config(format!(
            "配置校验失败：{} 个错误（见上文）",
            validation_errors.len()
        )));
    }

    // 2. 初始化日志。
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&runtime_cfg.log_level)),
        )
        .init();

    tracing::info!(target: "runtime", log_level = %runtime_cfg.log_level, "yomua-bot 正在启动...");

    // 3. 打开并迁移存储（SQLite 为唯一的持久化来源）。
    std::fs::create_dir_all(&runtime_cfg.data_dir).map_err(|e| {
        RuntimeError::Config(format!("无法创建数据目录 {}: {e}", runtime_cfg.data_dir))
    })?;
    let db_path = format!("{}/runtime.db", runtime_cfg.data_dir);
    let storage = Arc::new(SqliteStorage::open(&db_path).await?);
    storage.migrate().await?;
    let pool = storage.pool().clone();

    // 4. 建立仓库。
    let repos = build_repos(pool);

    // G1 启动检测：同一会话存在多个角色绑定为脏数据（旧版模型遗留），
    // 仅 warn 不自动删除；行为层取第一个绑定。
    check_binding_dirtiness(&repos.binding_repo)
        .await
        .map_err(|e| RuntimeError::Internal(format!("启动检测失败: {e}")))?;

    // 5. 建立事件总线（容量可配置，默认 256）。
    let bus = if let Some(capacity) = runtime_cfg.broadcast_capacity {
        EventBus::with_capacity(capacity)
    } else {
        EventBus::new()
    };

    // 6. 装配应用层编排依赖。
    let app = build_app_layer(&runtime_cfg, &llm_cfg, &repos, bus.clone())?;

    // 7. 建立会话管理器、动作执行器、OneBot 适配器。
    let conversation_manager = ConversationManager::new(
        repos.conversation_repo.clone(),
        repos.participant_repo.clone(),
    );

    // C2-4: 构造 MessageAcceptanceService 用于 durable message acceptance
    let acceptance_service = Arc::new(MessageAcceptanceService::new(
        repos.message_repo.clone(),
        bus.clone(),
    ));

    let adapter = OneBotAdapterImpl::new(
        onebot_cfg.clone(),
        bus.clone(),
        conversation_manager,
        acceptance_service,
        app.binding_manager.clone(),
    )
    .await;
    let adapter = Arc::new(adapter);

    let action_dispatcher = Arc::new(ActionDispatcher::new(
        repos.conversation_repo.clone(),
        adapter.clone(),
    ));

    // 插件系统装配：注册表 + API 分发器。仅创建不启动；plugins_dir 未配置时
    // 不产生任何插件相关任务（保持既有部署行为不变）。
    let plugin_registry = Arc::new(PluginRegistry::new());
    let plugin_api = Arc::new(PluginApi::new(
        repos.character_repo.clone() as Arc<dyn CharacterRepository>,
        repos.state_repo.clone(),
        repos.binding_repo.clone(),
        repos.message_repo.clone(),
        repos.memory_repo.clone(),
        repos.relationship_repo.clone(),
        repos.plugin_data_repo.clone(),
        action_dispatcher.clone(),
        app.cognition.clone(),
        plugin_registry.clone(),
    ));

    let delay_executor: Arc<dyn DelayExecutor> = Arc::new(TokioDelayExecutor);
    let reply_processor = Arc::new(ReplyProcessor::new(
        app.runtime.clone(),
        app.binding_manager.clone(),
        app.decision_orchestrator.clone(),
        app.cognition.clone(),
        app.relationship_service.clone(),
        app.mood_service.clone(),
        app.memory_service.clone(),
        action_dispatcher.clone(),
        bus.clone(),
        delay_executor,
        repos.participant_repo.clone(),
        onebot_cfg.bot_self_id,
    ));

    // 系统指令处理器（硬性约束 B）：订阅 CommandReceived，执行换角色并中文回复。
    let command_handler = CommandHandler::new(
        app.binding_manager.clone(),
        repos.character_repo.clone() as Arc<dyn CharacterRepository>,
        action_dispatcher.clone(),
        runtime_cfg.admin_users.clone(),
    );

    // 8. 启动订阅者和后台任务（在 shutdown_rx 创建后）。
    // OutboxDispatcher 和 OutboxProcessor 也在这里启动（C2-4 durable event delivery）。

    // 9. 插件系统（条件启用）：plugins_dir 为 None 时不启动任何插件相关任务，
    // 但 WebUI socket 始终创建。
    let supervisor_cfg = SupervisorConfig {
        plugins_dir: runtime_cfg
            .plugins_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("plugins")),
        sockets_dir: PathBuf::from(&runtime_cfg.data_dir).join("plugin-sockets"),
        ..Default::default()
    };
    let supervisor = Arc::new(PluginSupervisor::new(
        supervisor_cfg,
        plugin_registry.clone(),
        plugin_api.clone(),
    ));

    // 插件系统（仅在 plugins_dir 配置时加载插件）。
    if let Some(plugins_dir) = &runtime_cfg.plugins_dir {
        tracing::info!(plugins_dir = %plugins_dir, "插件系统已启用");

        // EventBridge 订阅：把 Core 事件总线上的事件转发给已订阅插件。
        let bridge = EventBridge::new(plugin_registry.clone());
        let subscription = bus.subscribe();
        tokio::spawn(async move { bridge.run(subscription).await });

        // 启动全部插件；单个插件失败不致命（supervisor 内部已隔离），
        // 插件目录不存在只 warn 记录，不中断 Core。
        if let Err(e) = supervisor.start_all().await {
            tracing::warn!(error = %e, "插件启动异常（插件系统保持启用，Core 继续运行）");
        }
    } else {
        tracing::info!("插件系统未启用（runtime.toml 未配置 plugins_dir）");
    }

    // 10. 创建关停通道和控制句柄。
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // 8. 启动后台任务（EventProcessor、CommandHandler、ProactiveDriver、OutboxDispatcher、OutboxProcessor）。
    spawn_background_tasks(
        &repos,
        &bus,
        reply_processor,
        command_handler,
        &app,
        shutdown_rx.clone(),
    );

    // RuntimeHandle 暴露 State System 仓储：控制命令（state-set / state-get）
    // 必须经由这些仓储读写，不得直接操作 SQL。
    let handle = RuntimeHandle {
        config_dir: config_dir.to_path_buf(),
        runtime_cfg: runtime_cfg.clone(),
        data_dir: PathBuf::from(&runtime_cfg.data_dir),
        supervisor: supervisor.clone(),
        adapter: adapter.clone(),
        storage: storage.clone(),
        shutdown_tx,
        commands: CommandRegistry::builtin(),
        character_repo: repos.character_repo.clone() as Arc<dyn CharacterRepository>,
        state_repo: repos.state_repo.clone(),
        conversation_state_repo: repos.conversation_state_repo.clone(),
        mood_repo: repos.mood_repo.clone(),
        conversation_repo: repos.conversation_repo.clone(),
        message_repo: repos.message_repo.clone(),
        started_at: std::time::Instant::now(),
        blacklist_repo: repos.blacklist_repo.clone(),
        session_service: SessionService::new(repos._pool.clone()),
        character_service: yomua_bot::application::character_service::CharacterService::new(
            repos._pool.clone(),
        ),
        memory_repo: repos.memory_repo.clone(),
    };

    // 启动控制服务（后台任务，监听 data/control.sock）。
    let control_handle = start_control_service(handle.clone(), shutdown_rx.clone());
    tracing::info!(target: "runtime", socket_path = %handle.data_dir.join("control.sock").display(), "控制服务已启动（可用 yomua-bot-ctl status/reload-config/shutdown）");

    // 11. 启动适配器并等待消息。
    adapter.start().await?;
    tracing::info!(target: "runtime", "字符运行时已就绪，等待消息...");

    // 12. 等待关停信号（Ctrl+C 或控制 socket）。Core 常驻，适配器断线自动重连。
    tracing::info!(target: "runtime", "正在运行；按 Ctrl+C 或使用 yomua-bot-ctl shutdown 退出。");
    let _ = shutdown_rx.changed().await;
    tracing::info!(target: "runtime", "收到关停信号，正在优雅关闭...");

    // 13. 优雅关停：先停插件（发 shutdown 通知 → 等超时 → 杀残余）→ 停适配器 → 关数据库。
    if let Err(e) = supervisor.shutdown_all().await {
        tracing::warn!(target: "runtime", error = %e, "停止插件失败");
    }
    if let Err(e) = adapter.stop().await {
        tracing::warn!(target: "runtime", error = %e, "停止适配器失败");
    }
    storage.close().await;
    let _ = control_handle.await;

    tracing::info!(target: "runtime", "yomua-bot 已退出。");
    Ok(handle)
}

/// 运行 `import-card` 子命令：把一张角色卡 JSON/PNG 导入 SQLite，
/// 并按需绑定到指定会话。
///
/// 用法：`cargo run -- import-card <卡片路径> [选项]`
///
/// 选项：
/// - `--config-dir <目录>`：配置目录（默认当前目录，读其中的 `runtime.toml`）
/// - `--conversation <外部ID>`：绑定到的会话外部 ID（例如 QQ 群号）
/// - `--group`：目标会话为群聊（默认私聊）
/// - `--participant <外部ID>`：创建/复用会话中的一个人类参与者
/// - `--participant-name <名称>`：参与者的显示名称（首次创建时使用）
/// - `--reply-mode <natural|mention|occasional>`：回复方式（默认 `natural`）
/// - `--proactive`：启用主动消息
async fn run_import(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    // --- 解析命令行参数 ---
    let mut config_dir = PathBuf::from(".");
    let mut path: Option<PathBuf> = None;
    let mut conversation: Option<String> = None;
    let mut conversation_type = ConversationType::Private;
    let mut participant: Option<String> = None;
    let mut participant_name: Option<String> = None;
    let mut reply_mode = ReplyMode::Natural;
    let mut proactive = false;

    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--config-dir" => config_dir = PathBuf::from(arg_value(args, &mut i, arg)?),
            "--conversation" => conversation = Some(arg_value(args, &mut i, arg)?),
            "--participant" => participant = Some(arg_value(args, &mut i, arg)?),
            "--participant-name" => participant_name = Some(arg_value(args, &mut i, arg)?),
            "--reply-mode" => reply_mode = parse_reply_mode(&arg_value(args, &mut i, arg)?)?,
            "--group" => conversation_type = ConversationType::Group,
            "--proactive" => proactive = true,
            _ if arg.starts_with('-') => {
                return Err(RuntimeError::Config(format!("未知参数：{arg}")).into());
            }
            _ => path = Some(PathBuf::from(arg)),
        }
        i += 1;
    }

    let path = path.ok_or_else(|| RuntimeError::Config("请指定角色卡路径".to_string()))?;

    // --- 打开存储并迁移 ---
    let runtime_cfg = load_runtime(&config_dir.join(RUNTIME_CONFIG).display().to_string())?;
    std::fs::create_dir_all(&runtime_cfg.data_dir).map_err(|e| {
        RuntimeError::Config(format!("无法创建数据目录 {}: {e}", runtime_cfg.data_dir))
    })?;
    let db_path = format!("{}/runtime.db", runtime_cfg.data_dir);
    let storage = SqliteStorage::open(&db_path).await?;
    storage.migrate().await?;
    let pool = storage.pool().clone();

    // --- 装配仓储与导入服务 ---
    let character_repo: Arc<dyn CharacterRepository> =
        Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let state_repo: Arc<dyn CharacterStateRepository> =
        Arc::new(SqliteCharacterStateRepository::new(pool.clone()));
    let conversation_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let participant_repo: Arc<dyn ParticipantRepository> =
        Arc::new(SqliteParticipantRepository::new(pool.clone()));
    let binding_repo: Arc<dyn CharacterBindingRepository> =
        Arc::new(SqliteCharacterBindingRepository::new(pool.clone()));

    let conversation_manager = Arc::new(ConversationManager::new(
        conversation_repo.clone(),
        participant_repo.clone(),
    ));
    let binding_manager = Arc::new(BindingManager::new(
        binding_repo,
        character_repo.clone(),
        conversation_repo.clone(),
    ));
    let importer = CharacterImportService::new(
        character_repo,
        state_repo,
        conversation_manager,
        binding_manager,
    );

    // --- 执行导入 ---
    let options = ImportOptions {
        bind: conversation.map(|external_id| BindOptions {
            conversation_type,
            external_conversation_id: external_id,
            participant_external_id: participant,
            participant_display_name: participant_name,
            reply_mode,
            proactive_enabled: proactive,
        }),
    };

    let result = importer.import_file(&path, &options).await?;

    println!("已导入角色：{} (id={})", result.name, result.character_id);
    if let Some(spec) = &result.spec_version {
        println!("卡版本：{spec}");
    }
    if let Some(cid) = result.conversation_id {
        println!("已绑定到会话（id={cid}）");
        if let Some(bid) = result.binding_id {
            println!("绑定 id={bid}");
        }
        if let Some(pid) = result.participant_id {
            println!("参与者 id={pid}");
        }
    }

    storage.close().await;
    Ok(())
}

/// 打开配置目录下的 SQLite 存储并迁移，装配 CLI 只读命令所需的仓储。
///
/// 返回 `(storage, character_repo, conversation_repo, binding_repo)`；
/// 调用方在使用完毕后负责 `storage.close().await`。
#[allow(clippy::type_complexity)]
async fn open_cli_storage(
    config_dir: &Path,
) -> Result<
    (
        SqliteStorage,
        Arc<dyn CharacterRepository>,
        Arc<dyn ConversationRepository>,
        Arc<dyn ParticipantRepository>,
        Arc<dyn CharacterBindingRepository>,
    ),
    Box<dyn std::error::Error>,
> {
    let runtime_cfg = load_runtime(&config_dir.join(RUNTIME_CONFIG).display().to_string())?;
    std::fs::create_dir_all(&runtime_cfg.data_dir).map_err(|e| {
        RuntimeError::Config(format!("无法创建数据目录 {}: {e}", runtime_cfg.data_dir))
    })?;
    let db_path = format!("{}/runtime.db", runtime_cfg.data_dir);
    let storage = SqliteStorage::open(&db_path).await?;
    storage.migrate().await?;
    let pool = storage.pool().clone();

    Ok((
        storage,
        Arc::new(SqliteCharacterRepository::new(pool.clone())) as Arc<dyn CharacterRepository>,
        Arc::new(SqliteConversationRepository::new(pool.clone()))
            as Arc<dyn ConversationRepository>,
        Arc::new(SqliteParticipantRepository::new(pool.clone())) as Arc<dyn ParticipantRepository>,
        Arc::new(SqliteCharacterBindingRepository::new(pool.clone()))
            as Arc<dyn CharacterBindingRepository>,
    ))
}

/// 解析 CLI 公共参数：`--config-dir <目录>`（默认当前目录）。
fn parse_config_dir_arg(args: &[String]) -> Result<PathBuf, RuntimeError> {
    let mut config_dir = PathBuf::from(".");
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--config-dir" => config_dir = PathBuf::from(arg_value(args, &mut i, arg)?),
            _ => return Err(RuntimeError::Config(format!("未知参数：{arg}"))),
        }
        i += 1;
    }
    Ok(config_dir)
}

/// 运行 `list-characters` 子命令：列出全部角色（id + 名称）。
///
/// 用法：`cargo run -- list-characters [--config-dir <目录>]`
async fn run_list_characters(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let config_dir = parse_config_dir_arg(args)?;
    let (storage, character_repo, _, _, _) = open_cli_storage(&config_dir).await?;

    let characters = character_repo.find_all().await?;
    if characters.is_empty() {
        println!("暂无角色。可使用 `import-card` 导入角色卡。");
    } else {
        for c in characters {
            println!("{}\t{}", c.id, c.definition.name);
        }
    }

    storage.close().await;
    Ok(())
}

/// 运行 `list-bindings` 子命令：列出全部会话的角色绑定关系。
///
/// 用法：`cargo run -- list-bindings [--config-dir <目录>]`
async fn run_list_bindings(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let config_dir = parse_config_dir_arg(args)?;
    let (storage, character_repo, conversation_repo, _participant_repo, binding_repo) =
        open_cli_storage(&config_dir).await?;

    let bindings = binding_repo.find_all().await?;
    if bindings.is_empty() {
        println!("暂无绑定关系。可先导入角色并绑定会话。");
        storage.close().await;
        return Ok(());
    }

    // 建会话与角色的 id → 实体映射，用于展示外部 ID 与角色名。
    let conversations = conversation_repo.find_all().await?;
    let characters = character_repo.find_all().await?;
    let conv_by_id: std::collections::HashMap<i64, &yomua_bot::domain::conversation::Conversation> =
        conversations.iter().map(|c| (c.id, c)).collect();
    let char_by_id: std::collections::HashMap<i64, &yomua_bot::domain::character::Character> =
        characters.iter().map(|c| (c.id, c)).collect();

    println!("绑定ID\t会话外部ID\t会话类型\t角色\t切换时间");
    for b in bindings {
        let external_id = conv_by_id
            .get(&b.conversation_id)
            .map(|c| c.external_id.as_str())
            .unwrap_or("-");
        let conv_type = conv_by_id
            .get(&b.conversation_id)
            .map(|c| match c.conversation_type {
                ConversationType::Group => "群聊",
                ConversationType::Private => "私聊",
            })
            .unwrap_or("-");
        let name = char_by_id
            .get(&b.character_id)
            .map(|c| c.definition.name.as_str())
            .unwrap_or("-");
        let switched_at = b
            .switched_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{}\t{external_id}\t{conv_type}\t{name}\t{switched_at}",
            b.id
        );
    }

    storage.close().await;
    Ok(())
}

/// 运行 `switch-character` 子命令：把一个已存在会话的角色切换到指定角色。
///
/// 用法：`cargo run -- switch-character <角色名> --conversation <外部ID> [--group] [--config-dir <目录>]`
///
/// `--group` 表示目标会话为群聊（默认私聊）；本命令只查询已存在会话，
/// 不会像 `import-card` 那样创建会话。
async fn run_switch_character(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mut config_dir = PathBuf::from(".");
    let mut conversation_external_id: Option<String> = None;
    let mut character_name: Option<String> = None;
    let mut conversation_type = ConversationType::Private;

    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--config-dir" => config_dir = PathBuf::from(arg_value(args, &mut i, arg)?),
            "--conversation" => conversation_external_id = Some(arg_value(args, &mut i, arg)?),
            "--group" => conversation_type = ConversationType::Group,
            _ if arg.starts_with('-') => {
                return Err(RuntimeError::Config(format!("未知参数：{arg}")).into());
            }
            _ => character_name = Some(arg.to_string()),
        }
        i += 1;
    }

    let character_name = character_name.ok_or_else(|| {
        RuntimeError::Config(
            "请指定角色名（例：switch-character 木然 --conversation 123456）".to_string(),
        )
    })?;
    let external_id = conversation_external_id.ok_or_else(|| {
        RuntimeError::Config("请用 --conversation 指定目标会话外部 ID（群号或用户号）".to_string())
    })?;

    tracing::debug!(target: "cli", ?conversation_type, "准备切换会话角色");

    // 打开存储并装配局部仓储（只读查询，不创建会话）。
    let (storage, character_repo, conversation_repo, participant_repo, binding_repo) =
        open_cli_storage(&config_dir).await?;

    let target_conversation = conversation_repo
        .find_by_external_id(&external_id)
        .await?
        .ok_or_else(|| RuntimeError::Config(format!("未找到会话 {external_id}")))?;

    let target_character = character_repo
        .find_all()
        .await?
        .into_iter()
        .find(|c| c.definition.name == character_name)
        .ok_or_else(|| RuntimeError::Config(format!("未找到角色 {character_name}")))?;

    let binding_manager = BindingManager::new(
        binding_repo,
        character_repo.clone(),
        conversation_repo.clone(),
    );
    match binding_manager
        .switch_character(target_conversation.id, target_character.id)
        .await
    {
        Ok(_) => {
            println!("已把会话 {external_id} 的角色切换为「{character_name}」");
        }
        Err(e) => {
            eprintln!("换角色失败：{e}");
            return Err(e.into());
        }
    }

    storage.close().await;
    Ok(())
}

/// 读取一个需要取值的命令行选项。
fn arg_value(args: &[String], i: &mut usize, flag: &str) -> Result<String, RuntimeError> {
    *i += 1;
    args.get(*i)
        .cloned()
        .ok_or_else(|| RuntimeError::Config(format!("参数 {flag} 需要一个值")))
}

/// 把命令行中的回复模式字符串解析为 [`ReplyMode`]。
fn parse_reply_mode(s: &str) -> Result<ReplyMode, RuntimeError> {
    match s {
        "natural" => Ok(ReplyMode::Natural),
        "mention" | "mention_only" => Ok(ReplyMode::MentionOnly),
        "occasional" => Ok(ReplyMode::Occasionally),
        other => Err(RuntimeError::Config(format!("未知的回复模式：{other}"))),
    }
}

// ---------------------------------------------------------------------------
// 辅助函数（AUDIT-049：run_runtime 过长，拆分为私有辅助方法）
// ---------------------------------------------------------------------------

use sqlx::SqlitePool;

/// 所有仓库的集合。
struct Repos {
    _pool: SqlitePool,
    character_repo: Arc<SqliteCharacterRepository>,
    state_repo: Arc<dyn CharacterStateRepository>,
    binding_repo: Arc<dyn CharacterBindingRepository>,
    conversation_repo: Arc<dyn ConversationRepository>,
    participant_repo: Arc<dyn ParticipantRepository>,
    message_repo: Arc<dyn MessageRepository>,
    memory_repo: Arc<dyn MemoryRepository>,
    relationship_repo: Arc<dyn RelationshipRepository>,
    mood_repo: Arc<dyn MoodRepository>,
    conversation_state_repo: Arc<dyn ConversationStateRepository>,
    behavior_state_repo: Arc<dyn BehaviorStateRepository>,
    plugin_data_repo: Arc<dyn PluginDataRepository>,
    blacklist_repo: Arc<dyn BlacklistRepository>,
    outbox_repo: Arc<SqliteOutboxRepository>,
}

/// 从 SQLite 连接池构建所有仓库。
fn build_repos(pool: SqlitePool) -> Repos {
    let character_repo = Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let state_repo: Arc<dyn CharacterStateRepository> =
        Arc::new(SqliteCharacterStateRepository::new(pool.clone()));
    let binding_repo: Arc<dyn CharacterBindingRepository> =
        Arc::new(SqliteCharacterBindingRepository::new(pool.clone()));
    let conversation_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let participant_repo: Arc<dyn ParticipantRepository> =
        Arc::new(SqliteParticipantRepository::new(pool.clone()));
    let message_repo: Arc<dyn MessageRepository> =
        Arc::new(SqliteMessageRepository::new(pool.clone()));
    let memory_repo: Arc<dyn MemoryRepository> =
        Arc::new(SqliteMemoryRepository::new(pool.clone()));
    let relationship_repo: Arc<dyn RelationshipRepository> =
        Arc::new(SqliteRelationshipRepository::new(pool.clone()));
    let mood_repo: Arc<dyn MoodRepository> = Arc::new(SqliteMoodRepository::new(pool.clone()));
    let conversation_state_repo: Arc<dyn ConversationStateRepository> =
        Arc::new(SqliteConversationStateRepository::new(pool.clone()));
    let behavior_state_repo: Arc<dyn BehaviorStateRepository> =
        Arc::new(SqliteBehaviorStateRepository::new(pool.clone()));
    let plugin_data_repo: Arc<dyn PluginDataRepository> =
        Arc::new(SqlitePluginDataRepository::new(pool.clone()));
    let blacklist_repo: Arc<dyn BlacklistRepository> =
        Arc::new(SqliteBlacklistRepository::new(pool.clone()));
    let outbox_repo = Arc::new(SqliteOutboxRepository::new(pool.clone()));

    Repos {
        _pool: pool,
        character_repo,
        state_repo,
        binding_repo,
        conversation_repo,
        participant_repo,
        message_repo,
        memory_repo,
        relationship_repo,
        mood_repo,
        conversation_state_repo,
        behavior_state_repo,
        plugin_data_repo,
        blacklist_repo,
        outbox_repo,
    }
}

/// G1 启动检测：同一会话存在多个角色绑定为脏数据（旧版模型遗留），
/// 仅 warn 不自动删除；行为层取第一个绑定。
async fn check_binding_dirtiness(
    binding_repo: &Arc<dyn CharacterBindingRepository>,
) -> Result<(), Box<dyn std::error::Error>> {
    let all_bindings = binding_repo.find_all().await?;
    let mut conv_counts: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    for b in &all_bindings {
        *conv_counts.entry(b.conversation_id).or_insert(0) += 1;
    }
    let mut dirty = 0;
    for (conv, count) in conv_counts {
        if count > 1 {
            dirty += 1;
            tracing::warn!(target: "runtime", conversation_id = conv, count, "会话存在多个角色绑定（脏数据），行为层将取第一个绑定");
        }
    }
    if dirty == 0 {
        tracing::info!(target: "runtime", "会话绑定检查通过：所有会话均为单角色绑定");
    }
    Ok(())
}

/// 应用层编排依赖的集合。
struct AppLayer {
    runtime: Arc<CharacterRuntime>,
    binding_manager: Arc<BindingManager>,
    behavior_engine: Arc<RuleBehaviorEngine>,
    decision_orchestrator: DecisionOrchestrator,
    cognition: Arc<CognitionLayer>,
    relationship_service: Arc<RelationshipService>,
    mood_service: Arc<MoodService>,
    memory_service: Arc<MemoryService>,
    llm_scheduler: Option<Arc<DefaultLlmScheduler>>,
}

/// 装配应用层编排依赖。
fn build_app_layer(
    _runtime_cfg: &yomua_bot::application::config::RuntimeConfig,
    llm_cfg: &LlmConfig,
    repos: &Repos,
    bus: EventBus,
) -> Result<AppLayer, RuntimeError> {
    let runtime = Arc::new(CharacterRuntime::with_event_bus(
        repos.character_repo.clone() as Arc<dyn CharacterRepository>,
        repos.state_repo.clone(),
        repos.conversation_repo.clone(),
        bus.clone(),
    ));

    let binding_manager = Arc::new(BindingManager::new(
        repos.binding_repo.clone(),
        repos.character_repo.clone() as Arc<dyn CharacterRepository>,
        repos.conversation_repo.clone(),
    ));

    let context_builder = Arc::new(ContextBuilder::new(
        repos.message_repo.clone(),
        Arc::new(
            yomua_bot::application::character_history::RepositoryCharacterHistoryProvider::new(
                repos.message_repo.clone(),
            ),
        ),
        repos.conversation_repo.clone(),
        repos.memory_repo.clone(),
        repos.relationship_repo.clone(),
        repos.mood_repo.clone(),
        repos.binding_repo.clone(),
    ));
    let memory_service = Arc::new(MemoryService::new(repos.memory_repo.clone()));

    let mood_service = Arc::new(MoodService::new(repos.mood_repo.clone()));
    let relationship_service = Arc::new(RelationshipService::new(
        repos.relationship_repo.clone(),
        bus.clone(),
    ));

    let behavior_engine = Arc::new(RuleBehaviorEngine::new(
        repos.binding_repo.clone(),
        repos.mood_repo.clone(),
        repos.relationship_repo.clone(),
        repos.state_repo.clone(),
        repos.conversation_state_repo.clone(),
        repos.behavior_state_repo.clone(),
        yomua_bot::application::clock::system_clock(),
    ));

    // LLM 是能力不是生命线：enabled=false 时 scheduler 为 None，走确定性回复。
    let llm_scheduler: Option<Arc<DefaultLlmScheduler>> = if llm_cfg.enabled {
        // 多 Provider 模式：若 llm_cfg.has_multi_provider()，使用 providers registry
        let scheduler = if llm_cfg.has_multi_provider() {
            build_multi_provider_scheduler(llm_cfg.clone())?
        } else {
            // 旧格式 fallback：单 Provider（向后兼容）
            let provider: Arc<dyn LlmProvider> = Arc::new(
                build_openai_provider(llm_cfg.clone(), None)
                    .map_err(|e| RuntimeError::Config(format!("LLM 配置错误: {e}")))?,
            );
            tracing::info!(target = "llm", model = %provider.name(), "LLM 已启用（单 Provider 模式）");
            Arc::new(DefaultLlmScheduler::new(provider))
        };
        Some(scheduler)
    } else {
        tracing::info!(target = "llm", "LLM 未启用，使用确定性回复");
        None
    };
    let scheduler: Option<Arc<dyn LlmScheduler>> =
        llm_scheduler.clone().map(|s| s as Arc<dyn LlmScheduler>);

    // Phase 3: ContextAssembler = Token Budget + Layered Degradation
    // 从 llm.toml [options] 读取 context_window、max_output_tokens 和 output_reservation
    let (context_window, max_output_tokens, output_reservation) = read_llm_capabilities(llm_cfg);
    let token_estimator = Arc::new(TokenEstimatorAdapter(Arc::new(ApproximateTokenEstimator)));
    let model_capabilities = Arc::new(ModelCapabilitiesProviderAdapter(Arc::new(
        FixedModelCapabilities::new(context_window, max_output_tokens),
    )));
    let config = yomua_bot::application::context_assembler::ContextAssemblyConfig {
        degradation_enabled: true,
        output_reservation,
        runtime_max_output_tokens: None,
    };
    let context_assembler = Arc::new(ContextAssembler::with_config(
        token_estimator,
        model_capabilities,
        config,
    ));

    let cognition = Arc::new(CognitionLayer::new(
        scheduler,
        context_builder.clone(),
        context_assembler,
    ));

    // DecisionOrchestrator：使用 RuleBehaviorEngine 作为 evaluator，NoOpTrigger 作为默认 trigger。
    // Pipeline 为 None（MVP 阶段不调用真实 LLM）。
    let evaluator: Arc<dyn DecisionEvaluator> = behavior_engine.clone();
    let trigger: Arc<dyn LlmAssistanceTrigger> = Arc::new(NoOpTrigger);
    let decision_orchestrator = DecisionOrchestrator::new(evaluator, trigger, None);

    Ok(AppLayer {
        runtime,
        binding_manager,
        behavior_engine,
        decision_orchestrator,
        cognition,
        relationship_service,
        mood_service,
        memory_service,
        llm_scheduler,
    })
}

/// 启动后台订阅者和任务。
fn spawn_background_tasks(
    repos: &Repos,
    bus: &EventBus,
    reply_processor: Arc<ReplyProcessor>,
    command_handler: CommandHandler,
    app: &AppLayer,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let processor_bus = bus.clone();
    let reply_processor_for_outbox = reply_processor.clone();
    tokio::spawn(async move {
        EventProcessor::new(reply_processor)
            .run(&processor_bus)
            .await;
    });

    let command_bus = bus.clone();
    tokio::spawn(async move {
        command_handler.run(&command_bus).await;
    });

    // 启动主动行为驱动（后台 tick，无 LLM，仅状态维护）。
    let proactive_driver = ProactiveDriver::new(
        repos.binding_repo.clone(),
        repos.state_repo.clone(),
        repos.behavior_state_repo.clone(),
        app.behavior_engine.clone() as Arc<dyn BehaviorEngine>,
        bus.clone(),
        yomua_bot::application::clock::system_clock(),
    );
    tokio::spawn(async move {
        proactive_driver.run().await;
    });

    // 启动后台认知驱动（LLM 启用时才有意义）。
    if let Some(ref sched) = app.llm_scheduler {
        let cognition_driver = Arc::new(CognitionDriver::new(
            sched.clone() as Arc<dyn EmbeddingScheduler>,
            sched.clone() as Arc<dyn LlmScheduler>,
            repos.memory_repo.clone(),
            repos.binding_repo.clone(),
            repos.message_repo.clone(),
            repos.character_repo.clone() as Arc<dyn CharacterRepository>,
            Arc::new(
                yomua_bot::application::character_history::RepositoryCharacterHistoryProvider::new(
                    repos.message_repo.clone(),
                ),
            ),
            yomua_bot::application::clock::system_clock(),
        ));
        tokio::spawn(async move {
            cognition_driver.run().await;
        });
    }

    // C2-4: 启动 OutboxDispatcher 和 OutboxProcessor（durable event delivery）
    let outbox_dispatcher =
        OutboxDispatcher::new(repos.outbox_repo.clone(), OutboxDispatcherConfig::default());
    let (event_tx, event_rx) = mpsc::channel(32); // bounded MPSC channel
    let outbox_processor = OutboxProcessor::new(reply_processor_for_outbox);
    let dispatcher_shutdown_rx = shutdown_rx.clone();
    let processor_shutdown_rx = shutdown_rx.clone();

    tokio::spawn(async move {
        outbox_dispatcher
            .run(event_tx, dispatcher_shutdown_rx)
            .await;
    });

    tokio::spawn(async move {
        outbox_processor.run(event_rx, processor_shutdown_rx).await;
    });

    tracing::info!(target: "runtime", "OutboxDispatcher 和 OutboxProcessor 已启动");
}

#[cfg(test)]
mod tests {
    use super::*;

    // 注意：parse_runtime_args 在 --help/-h 时直接 exit(0)，无法在单元测试中正常返回。
    // 这些测试覆盖非 exit 路径。

    #[test]
    fn test_parse_runtime_args_empty() {
        let result = parse_runtime_args(&[] as &[String]);
        let path = result.expect("空参数应返回 cwd");
        assert_eq!(path, PathBuf::from("."));
    }

    #[test]
    fn test_parse_runtime_args_positional_config_dir() {
        let result = parse_runtime_args(&["/tmp/my-config".to_string()]);
        let path = result.expect("positional 应被接受");
        assert_eq!(path, PathBuf::from("/tmp/my-config"));
    }

    #[test]
    fn test_parse_runtime_args_config_dir_flag() {
        let result = parse_runtime_args(&["--config-dir".to_string(), "/tmp/other".to_string()]);
        let path = result.expect("--config-dir 应设置目录");
        assert_eq!(path, PathBuf::from("/tmp/other"));
    }

    #[test]
    fn test_parse_runtime_args_config_dir_flag_no_value() {
        // --config-dir 缺少值：应返回错误而非创建目录
        let result = parse_runtime_args(&["--config-dir".to_string()]);
        assert!(result.is_err(), "--config-dir 无值应返回错误");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("--config-dir") || err_msg.contains("需要"),
            "错误信息应提及 --config-dir 或参数缺失：{err_msg}"
        );
    }

    #[test]
    fn test_parse_runtime_args_unknown_flag() {
        let result = parse_runtime_args(&["--unknown".to_string()]);
        assert!(result.is_err(), "未知参数应返回错误");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("--unknown") || err_msg.contains("未知"),
            "错误信息应提及未知参数：{err_msg}"
        );
    }

    #[test]
    fn test_parse_runtime_args_extra_positional() {
        // 第二个 positional 参数应报错
        let result = parse_runtime_args(&["/tmp/first".to_string(), "second".to_string()]);
        assert!(result.is_err(), "多余位置参数应返回错误");
    }

    #[test]
    fn test_parse_runtime_args_config_dir_then_positional() {
        // --config-dir foo bar: bar 是多余位置参数
        let result = parse_runtime_args(&[
            "--config-dir".to_string(),
            "/tmp/cfg".to_string(),
            "extra".to_string(),
        ]);
        assert!(result.is_err(), "多余位置参数应返回错误");
    }

    #[test]
    fn test_parse_runtime_args_dot_as_config_dir() {
        let result = parse_runtime_args(&[".".to_string()]);
        let path = result.expect(".");
        assert_eq!(path, PathBuf::from("."));
    }
}
