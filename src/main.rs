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

use tokio::sync::{mpsc, watch, Notify, RwLock};
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
    load_runtime, write_template_if_missing, ConfigurationBundle, DecisionConfig, LlmConfig,
    DECISION_TEMPLATE, LLM_TEMPLATE, ONEBOT_TEMPLATE, RUNTIME_TEMPLATE,
};
use yomua_bot::application::context::ContextBuilder;
use yomua_bot::application::context_assembler::{
    ContextAssembler, ModelCapabilitiesProviderAdapter, TokenEstimatorAdapter,
};
use yomua_bot::application::control::{
    start_control_service, CommandRegistry, RuntimeHandle, ShutdownResult,
};
use yomua_bot::application::conversation::ConversationManager;
use yomua_bot::application::decision_orchestrator::{
    DecisionEvaluator, DecisionLlmPipeline, DecisionOrchestrator, LlmAssistanceTrigger,
};
use yomua_bot::application::event_bus::EventBus;
use yomua_bot::application::llm_scheduler::{
    DefaultLlmScheduler, EmbeddingScheduler, LlmRouteConfig, LlmScheduler,
};
use yomua_bot::application::memory_service::MemoryService;
use yomua_bot::application::message_acceptance::MessageAcceptanceService;
use yomua_bot::application::mood_service::MoodService;
use yomua_bot::application::outbox_dispatcher::{OutboxDispatcher, OutboxDispatcherConfig};
use yomua_bot::application::outbox_processor::OutboxProcessor;
use yomua_bot::application::plugin_api::PluginApi;
use yomua_bot::application::proactive::ProactiveDriver;
use yomua_bot::application::proactive::ProactiveReplyScheduler;
use yomua_bot::application::relationship_service::RelationshipService;
use yomua_bot::application::reply_processor::{DelayExecutor, ReplyProcessor, TokioDelayExecutor};
use yomua_bot::application::runtime::CharacterRuntime;
use yomua_bot::application::session_service::SessionService;
use yomua_bot::domain::behavior::BehaviorEngine;
use yomua_bot::domain::character::ReplyMode;
use yomua_bot::domain::conversation::ConversationType;
use yomua_bot::domain::decision::{
    DecisionLlmServiceImpl, DecisionResultValidator, DefaultOverridePolicy, HeuristicTrigger,
    NoOpTrigger,
};
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
const DECISION_CONFIG: &str = "decision.toml";
/// 默认配置目录。相对于启动目录解析，部署时启动目录应为项目/服务根目录。
const DEFAULT_CONFIG_DIR: &str = "config";

/// 只使用已展开、已校验的模型路由；默认 Provider 来自 conversation。
fn build_model_scheduler(
    config: &LlmConfig,
    decision_config: &DecisionConfig,
) -> Result<Option<Arc<DefaultLlmScheduler>>, RuntimeError> {
    let Some(mut resolved) = config.resolve()? else {
        return Ok(None);
    };
    LlmConfig::apply_decision_config(&mut resolved, decision_config)?;
    let mut providers = std::collections::HashMap::new();
    for (name, config) in &resolved.providers {
        let provider: Arc<dyn LlmProvider> = Arc::new(build_provider_from_config(config)?);
        providers.insert(name.clone(), provider);
    }
    let default = providers
        .get(&resolved.default_provider)
        .cloned()
        .ok_or_else(|| RuntimeError::Config("默认模型 Provider 不存在".to_string()))?;
    let routes = resolved
        .routes
        .into_iter()
        .map(|(role, route)| {
            (
                role,
                LlmRouteConfig {
                    models: route.models,
                    max_attempts: route.max_attempts,
                    budget_tokens: route.budget_tokens,
                },
            )
        })
        .collect();
    Ok(Some(Arc::new(DefaultLlmScheduler::with_provider_routes(
        default, 4, 4, providers, routes,
    ))))
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

/// 从单 Provider `options` JSON 构建 Provider（当前单 Provider 配置）。
/// 运行时 CLI 帮助文本。
const RUNTIME_HELP: &str = r#"yomua-bot —— Character Runtime

用法：
  yomua-bot [选项]

选项：
  --help, -h          显示本帮助信息
  --config-dir <目录>  指定配置目录（默认：./config）

无参数时，使用启动目录下的 `config/` 作为配置目录；首次启动会在其中生成模板。
配置文件：<config-dir>/runtime.toml、<config-dir>/llm.toml、<config-dir>/decision.toml、<config-dir>/onebot.toml。
运行数据目录在 runtime.toml 的 data_dir 字段中指定（默认 data/）。
"#;

/// 解析运行时参数（可供测试访问）。
///
/// 规则：
/// - `--help` / `-h`：打印帮助并以 0 退出
/// - `--config-dir <path>`：设置配置目录
/// - 任何非选项参数：报错
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
                return Err(RuntimeError::Config(format!(
                    "不支持的位置参数：{arg}，运行 yomua-bot --help 查看用法"
                )));
            }
            _ => {
                return Err(RuntimeError::Config(format!(
                    "未知参数：{arg}，运行 yomua-bot --help 查看用法"
                )));
            }
        }
    }

    Ok(config_dir.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_DIR)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    // 子命令分发：import-card / list-characters / list-bindings / switch-character。
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
    // 1. 配置初始化：任一配置文件缺失时补齐模板并退出提示用户修改。
    let runtime_path = config_dir.join(RUNTIME_CONFIG);
    let onebot_path = config_dir.join(ONEBOT_CONFIG);
    let llm_path = config_dir.join(LLM_CONFIG);
    let decision_path = config_dir.join(DECISION_CONFIG);

    let mut created_templates = Vec::new();
    for (path, template) in [
        (&runtime_path, RUNTIME_TEMPLATE),
        (&onebot_path, ONEBOT_TEMPLATE),
        (&llm_path, LLM_TEMPLATE),
        (&decision_path, DECISION_TEMPLATE),
    ] {
        if write_template_if_missing(&path.display().to_string(), template).map_err(|e| {
            RuntimeError::Config(format!("无法创建配置文件 {}: {e}", path.display()))
        })? {
            created_templates.push(path.clone());
        }
    }

    if !created_templates.is_empty() {
        eprintln!(
            "错误：配置文件不完整，已在 {} 生成占位模板。\n\
             请先填写 runtime.toml 的 admin_users、onebot.toml 的连接参数，\n\
             需要模型时填写 llm.toml；需要决策模型时再填写 decision.toml，并重新启动。\n\
             已生成：\n{}",
            config_dir.display(),
            created_templates
                .iter()
                .map(|path| format!("- {}", path.display()))
                .collect::<Vec<_>>()
                .join("\n")
        );
        std::process::exit(1);
    }

    let launch_config = Arc::new(ConfigurationBundle::load(config_dir)?);
    let runtime_cfg = launch_config.runtime.clone();
    let onebot_cfg = launch_config.onebot.clone();
    let llm_cfg = launch_config.llm.clone();
    let decision_cfg = launch_config.decision.clone();
    let (settings_tx, settings_rx) = watch::channel(runtime_cfg.clone());
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // 日志过滤器与运行设置按完整校验后的配置一起更新。
    use tracing_subscriber::prelude::*;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&runtime_cfg.log_level));
    let (filter, filter_handle) = tracing_subscriber::reload::Layer::new(filter);
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
    let log_reload: yomua_bot::application::control::LogReload = Arc::new(move |level| {
        let filter = EnvFilter::try_new(level)
            .map_err(|e| RuntimeError::Config(format!("日志过滤器无效：{e}")))?;
        filter_handle
            .reload(filter)
            .map_err(|e| RuntimeError::Internal(format!("更新日志过滤器失败：{e}")))
    });

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

    // G1 启动检测：同一会话存在多个角色绑定为脏数据（历史数据或人工修改遗留），
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
    let app = build_app_layer(&runtime_cfg, &llm_cfg, &decision_cfg, &repos, bus.clone())?;

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
    .await
    .with_blacklist_repo(repos.blacklist_repo.clone());
    let adapter = Arc::new(adapter);

    let action_dispatcher = Arc::new(ActionDispatcher::new(
        repos.conversation_repo.clone(),
        adapter.clone(),
    ));

    // 插件系统装配：注册表 + API 分发器。仅创建不启动；plugins_dir 未配置时
    // 不产生任何插件相关任务（保持既有部署行为不变）。
    let plugin_registry = Arc::new(PluginRegistry::new());
    let plugin_api = Arc::new(
        PluginApi::new(
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
        )
        .with_runtime(app.runtime.clone()),
    );

    let delay_executor: Arc<dyn DelayExecutor> = Arc::new(TokioDelayExecutor);
    let reply_processor = Arc::new(
        ReplyProcessor::new(
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
        )
        .with_message_repository(repos.message_repo.clone()),
    );

    // 系统指令处理器（硬性约束 B）：订阅 CommandReceived，执行换角色并中文回复。
    let command_handler = CommandHandler::new(
        app.binding_manager.clone(),
        repos.character_repo.clone() as Arc<dyn CharacterRepository>,
        action_dispatcher.clone(),
        runtime_cfg.admin_users.clone(),
    )
    .with_live_settings(settings_rx);

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
        shutdown_timeout: Duration::from_secs(runtime_cfg.shutdown_timeout_secs),
        ..Default::default()
    };
    let supervisor = Arc::new(PluginSupervisor::new(
        supervisor_cfg,
        plugin_registry.clone(),
        plugin_api.clone(),
    ));

    let mut auxiliary_tasks = BackgroundTasks::default();
    // 插件系统（仅在 plugins_dir 配置时加载插件）。
    if let Some(plugins_dir) = &runtime_cfg.plugins_dir {
        tracing::info!(plugins_dir = %plugins_dir, "插件系统已启用");

        // EventBridge 订阅：把 Core 事件总线上的事件转发给已订阅插件。
        let bridge = EventBridge::new(plugin_registry.clone());
        let subscription = bus.subscribe();
        auxiliary_tasks
            .handles
            .push(spawn_until_stopped(shutdown_rx.clone(), async move {
                bridge.run(subscription).await
            }));

        // 启动全部插件；单个插件失败不致命（supervisor 内部已隔离），
        // 插件目录不存在只 warn 记录，不中断 Core。
        if let Err(e) = supervisor.start_all().await {
            tracing::warn!(error = %e, "插件启动异常（插件系统保持启用，Core 继续运行）");
        }
    } else {
        tracing::info!("插件系统未启用（runtime.toml 未配置 plugins_dir）");
    }

    // 8. 启动后台任务（CommandHandler、ProactiveDriver、OutboxDispatcher、OutboxProcessor）。
    let mut background_tasks = spawn_background_tasks(
        &repos,
        &bus,
        reply_processor,
        command_handler,
        &app,
        shutdown_rx.clone(),
    );

    let shutdown_complete = Arc::new(Notify::new());
    let shutdown_result = Arc::new(RwLock::new(None));

    // RuntimeHandle 暴露 State System 仓储：控制请求（character state set / get）
    // 必须经由这些仓储读写，不得直接操作 SQL。
    let handle = RuntimeHandle {
        config_dir: config_dir.to_path_buf(),
        runtime_cfg: runtime_cfg.clone(),
        live_settings: Some(settings_tx),
        launch_config: Some(Arc::new(tokio::sync::RwLock::new((*launch_config).clone()))),
        log_reload: Some(log_reload),
        runtime: Some(app.runtime.clone()),
        data_dir: PathBuf::from(&runtime_cfg.data_dir),
        supervisor: supervisor.clone(),
        adapter: adapter.clone(),
        storage: storage.clone(),
        shutdown_tx,
        shutdown_complete: shutdown_complete.clone(),
        shutdown_result: shutdown_result.clone(),
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
    // 控制服务单独持有句柄。关停命令需要等待主流程完成，不能把控制服务本身
    // 放进 auxiliary_tasks，否则会形成“控制请求等待主流程、主流程等待控制请求”的环。
    let control_task = start_control_service(handle.clone(), shutdown_rx.clone());
    tracing::info!(target: "runtime", socket_path = %handle.data_dir.join("control.sock").display(), "控制服务已启动（可用 yomua-bot-ctl status/reload-config/shutdown）");

    // 启动失败也进入同一清理流程，避免插件或 socket 遗留。
    let start_result = adapter.start().await;
    if start_result.is_ok() {
        tracing::info!(target: "runtime", "运行时已就绪，等待消息。Ctrl+C、SIGTERM 或控制命令可关停。");
        tokio::select! {
            _ = wait_for_stop(&mut shutdown_rx) => {},
            result = shutdown_signal() => { if let Err(error) = result { tracing::warn!(error = %error, "监听系统关停信号失败"); } },
        }
    }
    handle.shutdown();
    tracing::info!(target: "runtime", "停止接收工作，正在保存并关闭后台任务...");
    let deadline = tokio::time::Instant::now()
        + Duration::from_secs(handle.effective_config().shutdown_timeout_secs);
    if let Some(driver) = &background_tasks.cognition {
        driver.stop();
    }
    let cleanup = async {
        if let Err(error) = adapter.stop().await {
            tracing::warn!(error = %error, "停止适配器失败");
        }
        let (_, plugin_result) = tokio::join!(background_tasks.join(), supervisor.shutdown_all());
        if let Err(error) = plugin_result {
            tracing::warn!(error = %error, "停止插件失败");
        }
        auxiliary_tasks.join().await;
        storage.close().await;
    };
    let cleanup_timed_out = tokio::time::timeout_at(deadline, cleanup).await.is_err();
    let unfinished_tasks = if cleanup_timed_out {
        tracing::warn!("关停超过配置时限，取消剩余任务，未完成回复保留供重启核查");
        let count = background_tasks
            .handles
            .iter()
            .filter(|task| !task.is_finished())
            .count()
            + auxiliary_tasks
                .handles
                .iter()
                .filter(|task| !task.is_finished())
                .count();
        background_tasks.abort();
        auxiliary_tasks.abort();
        supervisor.force_shutdown().await;
        Some(count)
    } else {
        Some(0)
    };

    *shutdown_result.write().await = Some(ShutdownResult {
        completed: !cleanup_timed_out,
        timed_out: cleanup_timed_out,
        unfinished_tasks,
        message: if cleanup_timed_out {
            "关停超时，未完成任务已取消，持久化任务可在下次启动时恢复".to_string()
        } else {
            "关停已完成".to_string()
        },
    });
    shutdown_complete.notify_waiters();

    // 主流程已经完成后再结束控制服务，给 shutdown 请求留下写完响应的机会。
    let _ = tokio::time::timeout(Duration::from_secs(2), control_task).await;
    start_result?;

    tracing::info!(target: "runtime", "yomua-bot 已退出。");
    Ok(handle)
}

/// 运行 `import-card` 子命令：把一张角色卡 JSON/PNG 导入 SQLite，
/// 并按需绑定到指定会话。
///
/// 用法：`cargo run -- import-card <卡片路径> [选项]`
///
/// 选项：
/// - `--config-dir <目录>`：配置目录（默认 `./config`，读其中的 `runtime.toml`）
/// - `--conversation <外部ID>`：绑定到的会话外部 ID（例如 QQ 群号）
/// - `--group`：目标会话为群聊（默认私聊）
/// - `--participant <外部ID>`：创建/复用会话中的一个人类参与者
/// - `--participant-name <名称>`：参与者的显示名称（首次创建时使用）
/// - `--reply-mode <natural|mention|occasional>`：回复方式（默认 `natural`）
/// - `--proactive`：启用主动消息
async fn run_import(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    // --- 解析命令行参数 ---
    let mut config_dir = PathBuf::from(DEFAULT_CONFIG_DIR);
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

/// 解析 CLI 公共参数：`--config-dir <目录>`（默认 `./config`）。
fn parse_config_dir_arg(args: &[String]) -> Result<PathBuf, RuntimeError> {
    let mut config_dir = PathBuf::from(DEFAULT_CONFIG_DIR);
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
    let mut config_dir = PathBuf::from(DEFAULT_CONFIG_DIR);
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
    let (storage, character_repo, conversation_repo, _participant_repo, binding_repo) =
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
// 运行时装配辅助函数。
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

/// G1 启动检测：同一会话存在多个角色绑定为脏数据（历史数据或人工修改遗留），
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
    decision_cfg: &DecisionConfig,
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

    let llm_scheduler = build_model_scheduler(llm_cfg, decision_cfg)?;
    let scheduler: Option<Arc<dyn LlmScheduler>> = llm_scheduler
        .clone()
        .map(|scheduler| scheduler as Arc<dyn LlmScheduler>);
    let resolved = llm_cfg.resolve()?;
    let conversation_model = resolved.as_ref().map(|config| config.conversation_model());
    let context_window = conversation_model.and_then(|model| model.context_window);
    let max_output_tokens = conversation_model.and_then(|model| model.max_output_tokens);
    let output_reservation = max_output_tokens.unwrap_or(512);
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

    // DecisionOrchestrator：规则引擎永远是基线；只有显式配置 decision role
    // 时才启用 advisory 决策模型，模型不能绕过验证器和硬约束。
    let evaluator: Arc<dyn DecisionEvaluator> = behavior_engine.clone();
    let (trigger, pipeline): (
        Arc<dyn LlmAssistanceTrigger>,
        Option<Arc<DecisionLlmPipeline>>,
    ) = if let (Some(scheduler), Some(_resolved)) = (llm_scheduler.clone(), resolved.as_ref()) {
        if decision_cfg.enabled {
            let service = Arc::new(DecisionLlmServiceImpl::new(scheduler));
            let validator = DecisionResultValidator::new(5_000, 30_000);
            let policy = Arc::new(DefaultOverridePolicy);
            (
                Arc::new(HeuristicTrigger),
                Some(Arc::new(DecisionLlmPipeline::new(
                    service, None, validator, policy,
                ))),
            )
        } else {
            (Arc::new(NoOpTrigger), None)
        }
    } else {
        (Arc::new(NoOpTrigger), None)
    };
    let decision_orchestrator = DecisionOrchestrator::new(evaluator, trigger, pipeline);

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
) -> BackgroundTasks {
    let mut tasks = BackgroundTasks::default();
    let reply_processor_for_outbox = reply_processor.clone();

    let command_bus = bus.clone();
    tasks
        .handles
        .push(spawn_until_stopped(shutdown_rx.clone(), async move {
            command_handler.run(&command_bus).await;
        }));

    // 主动消息复用普通回复任务：从同一会话最近的已持久化消息取得依据。
    struct ProactiveEnqueuer {
        reply_processor: Arc<ReplyProcessor>,
        binding_repo: Arc<dyn yomua_bot::domain::repository::CharacterBindingRepository>,
        message_repo: Arc<dyn yomua_bot::domain::repository::MessageRepository>,
    }
    #[async_trait::async_trait]
    impl ProactiveReplyScheduler for ProactiveEnqueuer {
        async fn enqueue_proactive(
            &self,
            character_id: i64,
            conversation_id: i64,
            reason: &str,
        ) -> Result<(), RuntimeError> {
            let binding = self
                .binding_repo
                .find_by_conversation_id(conversation_id)
                .await?
                .into_iter()
                .find(|item| item.character_id == character_id)
                .ok_or_else(|| RuntimeError::Internal("主动任务找不到当前角色绑定".into()))?;
            let source = self
                .message_repo
                .find_recent(conversation_id, 50)
                .await?
                .into_iter()
                .rev()
                .find(|message| message.active_character_id == Some(character_id))
                .ok_or_else(|| RuntimeError::Internal("主动任务缺少已有话题依据".into()))?;
            self.reply_processor
                .enqueue_proactive(&binding, &source, reason)
                .await
        }
    }
    let proactive_scheduler: Arc<dyn ProactiveReplyScheduler> = Arc::new(ProactiveEnqueuer {
        reply_processor: reply_processor.clone(),
        binding_repo: repos.binding_repo.clone(),
        message_repo: repos.message_repo.clone(),
    });

    // 启动主动行为驱动（具备来源约束、冷却、每日上限和统一关停）。
    let proactive_driver = ProactiveDriver::new(
        repos.binding_repo.clone(),
        repos.state_repo.clone(),
        repos.behavior_state_repo.clone(),
        app.behavior_engine.clone() as Arc<dyn BehaviorEngine>,
        bus.clone(),
        yomua_bot::application::clock::system_clock(),
    )
    .with_reply_scheduler(proactive_scheduler);
    let proactive_shutdown = shutdown_rx.clone();
    tasks
        .handles
        .push(spawn_until_stopped(shutdown_rx.clone(), async move {
            proactive_driver.run_until_stopped(proactive_shutdown).await;
        }));

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
        tasks.cognition = Some(cognition_driver.clone());
        tasks
            .handles
            .push(spawn_until_stopped(shutdown_rx.clone(), async move {
                cognition_driver.run().await;
            }));
    }

    // C2-4: 启动 OutboxDispatcher 和 OutboxProcessor（durable event delivery）
    let outbox_dispatcher =
        OutboxDispatcher::new(repos.outbox_repo.clone(), OutboxDispatcherConfig::default());
    let (event_tx, event_rx) = mpsc::channel(32); // bounded MPSC channel
    let outbox_processor = OutboxProcessor::new(reply_processor_for_outbox);
    let dispatcher_shutdown_rx = shutdown_rx.clone();
    let processor_shutdown_rx = shutdown_rx.clone();

    tasks.handles.push(tokio::spawn(async move {
        outbox_dispatcher
            .run(event_tx, dispatcher_shutdown_rx)
            .await;
    }));

    tasks.handles.push(tokio::spawn(async move {
        outbox_processor.run(event_rx, processor_shutdown_rx).await;
    }));

    tracing::info!(target: "runtime", "消息处理与后台任务已启动");
    tasks
}

#[derive(Default)]
struct BackgroundTasks {
    handles: Vec<tokio::task::JoinHandle<()>>,
    cognition: Option<Arc<CognitionDriver>>,
}

impl BackgroundTasks {
    async fn join(&mut self) {
        // 保留句柄直到完成；整体关停超时时仍能定位并取消当前等待的任务。
        for handle in &mut self.handles {
            let _ = handle.await;
        }
        self.handles.clear();
    }
    fn abort(&self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}
impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        self.abort();
    }
}

async fn wait_for_stop(receiver: &mut watch::Receiver<bool>) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            break;
        }
    }
}

fn spawn_until_stopped<F>(
    mut receiver: watch::Receiver<bool>,
    future: F,
) -> tokio::task::JoinHandle<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        tokio::select! { _ = wait_for_stop(&mut receiver) => {}, _ = future => {} }
    })
}

async fn shutdown_signal() -> Result<(), RuntimeError> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|error| RuntimeError::Internal(format!("注册 SIGTERM 失败：{error}")))?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.map_err(|error| RuntimeError::Internal(format!("注册 SIGINT 失败：{error}")))?,
            _ = terminate.recv() => {},
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(|error| RuntimeError::Internal(format!("注册关停信号失败：{error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 注意：parse_runtime_args 在 --help/-h 时直接 exit(0)，无法在单元测试中正常返回。
    // 这些测试覆盖非 exit 路径。

    #[test]
    fn test_parse_runtime_args_empty() {
        let result = parse_runtime_args(&[] as &[String]);
        let path = result.expect("空参数应返回默认配置目录");
        assert_eq!(path, PathBuf::from(DEFAULT_CONFIG_DIR));
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
    fn test_parse_runtime_args_positional_config_dir_is_rejected() {
        let result = parse_runtime_args(&["/tmp/my-config".to_string()]);
        assert!(result.is_err(), "配置目录必须显式使用 --config-dir");
    }
}
