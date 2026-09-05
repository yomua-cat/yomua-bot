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
use yomua_bot::application::config::{load_llm, load_onebot, load_runtime, LlmConfig};
use yomua_bot::application::context::ContextBuilder;
use yomua_bot::application::conversation::ConversationManager;
use yomua_bot::application::emotion_service::EmotionService;
use yomua_bot::application::event_bus::EventBus;
use yomua_bot::application::event_processor::EventProcessor;
use yomua_bot::application::llm_scheduler::{
    DefaultLlmScheduler, EmbeddingScheduler, LlmScheduler,
};
use yomua_bot::application::memory_service::MemoryService;
use yomua_bot::application::message_persistence::MessagePersistence;
use yomua_bot::application::plugin_api::PluginApi;
use yomua_bot::application::proactive::ProactiveDriver;
use yomua_bot::application::relationship_service::RelationshipService;
use yomua_bot::application::reply_processor::{DelayExecutor, ReplyProcessor, TokioDelayExecutor};
use yomua_bot::application::runtime::CharacterRuntime;
use yomua_bot::domain::behavior::BehaviorEngine;
use yomua_bot::domain::character::ReplyMode;
use yomua_bot::domain::conversation::ConversationType;
use yomua_bot::domain::repository::{
    CharacterBindingRepository, CharacterRepository, CharacterStateRepository,
    ConversationRepository, EmotionStateRepository, MemoryRepository, MessageRepository,
    ParticipantRepository, PluginDataRepository, RelationshipRepository,
};
use yomua_bot::error::RuntimeError;
use yomua_bot::infrastructure::llm::openai_compatible::{
    OpenAiCompatibleConfig, OpenAiCompatibleProvider,
};
use yomua_bot::infrastructure::llm::LlmProvider;
use yomua_bot::infrastructure::plugin::event_bridge::EventBridge;
use yomua_bot::infrastructure::plugin::registry::PluginRegistry;
use yomua_bot::infrastructure::plugin::supervisor::{PluginSupervisor, SupervisorConfig};
use yomua_bot::infrastructure::storage::repository::{
    SqliteCharacterBindingRepository, SqliteCharacterRepository, SqliteCharacterStateRepository,
    SqliteConversationRepository, SqliteEmotionStateRepository, SqliteMemoryRepository,
    SqliteMessageRepository, SqliteParticipantRepository, SqlitePluginDataRepository,
    SqliteRelationshipRepository,
};
use yomua_bot::infrastructure::storage::SqliteStorage;

/// 默认的配置文件名。
const RUNTIME_CONFIG: &str = "runtime.toml";
const ONEBOT_CONFIG: &str = "onebot.toml";
const LLM_CONFIG: &str = "llm.toml";

/// 从 `LlmConfig` 构造 OpenAI-compatible Provider。
///
/// `options` 中读取 `base_url` / `api_key` / `model` / `timeout_secs`。
/// 缺失时使用合理的默认值（Ollama 本地默认端点）。
fn build_openai_provider(cfg: &LlmConfig) -> Result<OpenAiCompatibleProvider, RuntimeError> {
    let options = &cfg.options;

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
        .or(cfg.provider.as_deref())
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

    let config_dir = PathBuf::from(args.first().map(String::as_str).unwrap_or_default());
    run_runtime(&config_dir).await
}

/// 启动常驻运行时。
///
/// 加载配置 → 初始化日志 → 打开存储 → 建立仓库 → 装配应用层
/// （Runtime / 行为 / 认知 / 情绪 / 关系）→ 启动订阅者 → 启动插件系统
/// （可选）→ 启动 OneBot 适配器 → 等待关停信号。
async fn run_runtime(config_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // 1. 加载配置。
    let runtime_cfg = load_runtime(&config_dir.join(RUNTIME_CONFIG).display().to_string())?;
    let onebot_cfg = load_onebot(&config_dir.join(ONEBOT_CONFIG).display().to_string())?;
    let llm_cfg = load_llm(&config_dir.join(LLM_CONFIG).display().to_string())?;

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
    let storage = SqliteStorage::open(&db_path).await?;
    storage.migrate().await?;
    let pool = storage.pool().clone();

    // 4. 建立仓库。角色仓库需要同时用于 Runtime 与 BindingManager，
    //    因此保留具体类型，并在需要 trait 对象处 clone。
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
    let emotion_repo: Arc<dyn EmotionStateRepository> =
        Arc::new(SqliteEmotionStateRepository::new(pool.clone()));

    // 插件数据仓储（plugin_data.* 免权限、按插件名命名空间隔离）。
    let plugin_data_repo: Arc<dyn PluginDataRepository> =
        Arc::new(SqlitePluginDataRepository::new(pool.clone()));

    // G1 启动检测：同一会话存在多个角色绑定为脏数据（旧版模型遗留），
    // 仅 warn 不自动删除；行为层取第一个绑定。
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

    // 5. 建立事件总线。
    let bus = EventBus::new();

    // 6. 装配应用层编排依赖。
    //    repos → CharacterRuntime → BindingManager → ContextBuilder →
    //    EmotionService → RelationshipService → RuleBehaviorEngine →
    //    (enabled) Provider → Scheduler → CognitionLayer → ActionDispatcher →
    //    ReplyProcessor → EventProcessor。
    let runtime = Arc::new(CharacterRuntime::with_event_bus(
        character_repo.clone() as Arc<dyn CharacterRepository>,
        state_repo.clone(),
        conversation_repo.clone(),
        bus.clone(),
    ));

    let binding_manager = Arc::new(BindingManager::new(
        binding_repo.clone(),
        character_repo.clone() as Arc<dyn CharacterRepository>,
        conversation_repo.clone(),
    ));

    let context_builder = Arc::new(ContextBuilder::new(
        message_repo.clone(),
        conversation_repo.clone(),
        memory_repo.clone(),
        relationship_repo.clone(),
        emotion_repo.clone(),
        binding_repo.clone(),
    ));
    let memory_service = Arc::new(MemoryService::new(memory_repo.clone()));

    let emotion_service = Arc::new(EmotionService::new(emotion_repo.clone(), bus.clone()));
    let relationship_service = Arc::new(RelationshipService::new(
        relationship_repo.clone(),
        bus.clone(),
    ));

    let behavior_engine = Arc::new(RuleBehaviorEngine::new(
        binding_repo.clone(),
        emotion_repo.clone(),
        relationship_repo.clone(),
        state_repo.clone(),
        yomua_bot::application::clock::system_clock(),
    ));

    // LLM 是能力不是生命线：enabled=false 时 scheduler 为 None，走确定性回复。
    let llm_scheduler: Option<Arc<DefaultLlmScheduler>> = if llm_cfg.enabled {
        let provider: Arc<dyn LlmProvider> = Arc::new(build_openai_provider(&llm_cfg)?);
        tracing::info!(target: "llm", model = %provider.name(), "LLM 已启用");
        Some(Arc::new(DefaultLlmScheduler::new(provider)))
    } else {
        tracing::info!(target: "llm", "LLM 未启用，使用确定性回复");
        None
    };
    let scheduler: Option<Arc<dyn LlmScheduler>> =
        llm_scheduler.clone().map(|s| s as Arc<dyn LlmScheduler>);
    let cognition = Arc::new(CognitionLayer::new(scheduler, context_builder.clone()));

    // 7. 建立会话管理器、动作执行器、OneBot 适配器。
    let conversation_manager =
        ConversationManager::new(conversation_repo.clone(), participant_repo.clone());
    let adapter = OneBotAdapterImpl::new(onebot_cfg, bus.clone(), conversation_manager).await;
    let adapter = Arc::new(adapter);

    let action_dispatcher = Arc::new(ActionDispatcher::new(
        conversation_repo.clone(),
        adapter.clone(),
    ));

    // 插件系统装配：注册表 + API 分发器。仅创建不启动；plugins_dir 未配置时
    // 不产生任何插件相关任务（保持既有部署行为不变）。
    let plugin_registry = Arc::new(PluginRegistry::new());
    let plugin_api = Arc::new(PluginApi::new(
        character_repo.clone() as Arc<dyn CharacterRepository>,
        state_repo.clone(),
        memory_repo.clone(),
        relationship_repo.clone(),
        plugin_data_repo.clone(),
        action_dispatcher.clone(),
        cognition.clone(),
        plugin_registry.clone(),
    ));

    let delay_executor: Arc<dyn DelayExecutor> = Arc::new(TokioDelayExecutor);
    let reply_processor = Arc::new(ReplyProcessor::new(
        runtime,
        binding_manager.clone(),
        behavior_engine.clone(),
        cognition,
        relationship_service,
        emotion_service,
        memory_service,
        action_dispatcher.clone(),
        bus.clone(),
        delay_executor,
        participant_repo.clone(),
    ));

    // 系统指令处理器（硬性约束 B）：订阅 CommandReceived，执行换角色并中文回复。
    let command_handler = CommandHandler::new(
        binding_manager.clone(),
        character_repo.clone() as Arc<dyn CharacterRepository>,
        action_dispatcher.clone(),
        runtime_cfg.admin_users.clone(),
    );

    // 8. 启动订阅者（消息持久化、事件路由）。
    let persistence_bus = bus.clone();
    let persistence_msg_repo: Arc<dyn MessageRepository> =
        Arc::new(SqliteMessageRepository::new(pool.clone()));
    tokio::spawn(async move {
        MessagePersistence::new(persistence_msg_repo)
            .run(&persistence_bus)
            .await;
    });

    let processor_bus = bus.clone();
    tokio::spawn(async move {
        EventProcessor::new(reply_processor)
            .run(&processor_bus)
            .await;
    });

    let command_bus = bus.clone();
    tokio::spawn(async move {
        command_handler.run(&command_bus).await;
    });

    // 9. 启动主动行为驱动（后台 tick，无 LLM，仅状态维护）。
    let proactive_driver = ProactiveDriver::new(
        binding_repo.clone(),
        state_repo.clone(),
        behavior_engine.clone() as Arc<dyn BehaviorEngine>,
        bus.clone(),
        yomua_bot::application::clock::system_clock(),
    );
    tokio::spawn(async move {
        proactive_driver.run().await;
    });

    // 9.1. 启动后台认知驱动（LLM 启用时才有意义）。
    if let Some(ref sched) = llm_scheduler {
        let cognition_driver = Arc::new(CognitionDriver::new(
            sched.clone() as Arc<dyn EmbeddingScheduler>,
            sched.clone() as Arc<dyn LlmScheduler>,
            memory_repo.clone(),
            binding_repo.clone(),
            message_repo.clone(),
            character_repo.clone() as Arc<dyn CharacterRepository>,
            yomua_bot::application::clock::system_clock(),
        ));
        tokio::spawn(async move {
            cognition_driver.run().await;
        });
    }

    // 10. 插件系统（条件启用）：plugins_dir 为 None 时不启动任何插件相关任务。
    let supervisor: Option<Arc<PluginSupervisor>> =
        if let Some(plugins_dir) = &runtime_cfg.plugins_dir {
            let cfg = SupervisorConfig {
                plugins_dir: PathBuf::from(plugins_dir),
                sockets_dir: PathBuf::from(&runtime_cfg.data_dir).join("plugin-sockets"),
                ..SupervisorConfig::default()
            };
            let sup = Arc::new(PluginSupervisor::new(
                cfg,
                plugin_registry.clone(),
                plugin_api.clone(),
            ));
            tracing::info!(plugins_dir = %plugins_dir, "插件系统已启用");

            // EventBridge 订阅：把 Core 事件总线上的事件转发给已订阅插件。
            let bridge = EventBridge::new(plugin_registry.clone());
            let subscription = bus.subscribe();
            tokio::spawn(async move { bridge.run(subscription).await });

            // 启动全部插件；单个插件失败不致命（supervisor 内部已隔离），
            // 插件目录不存在只 warn 记录，不中断 Core。
            if let Err(e) = sup.start_all().await {
                tracing::warn!(error = %e, "插件启动异常（插件系统保持启用，Core 继续运行）");
            }
            Some(sup)
        } else {
            tracing::info!("插件系统未启用（runtime.toml 未配置 plugins_dir）");
            None
        };

    // 11. 启动适配器并等待消息。
    adapter.start().await?;
    tracing::info!(target: "runtime", "字符运行时已就绪，等待消息...");

    // 12. 等待关停信号（Ctrl+C）。Core 常驻，适配器断线自动重连。
    tracing::info!(target: "runtime", "正在运行；按 Ctrl+C 退出。");
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| RuntimeError::Internal(format!("无法注册关停信号处理: {e}")))?;
    tracing::info!(target: "runtime", "收到关停信号，正在优雅关闭...");

    // 13. 优雅关停：先停插件（发 shutdown 通知 → 等超时 → 杀残余）→ 停适配器 → 关数据库。
    if let Some(sup) = &supervisor {
        if let Err(e) = sup.shutdown_all().await {
            tracing::warn!(target: "runtime", error = %e, "停止插件失败");
        }
    }
    if let Err(e) = adapter.stop().await {
        tracing::warn!(target: "runtime", error = %e, "停止适配器失败");
    }
    storage.close().await;

    tracing::info!(target: "runtime", "yomua-bot 已退出。");
    Ok(())
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
    let (storage, character_repo, _, _) = open_cli_storage(&config_dir).await?;

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
    let (storage, character_repo, conversation_repo, binding_repo) =
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
    let (storage, character_repo, conversation_repo, binding_repo) =
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
