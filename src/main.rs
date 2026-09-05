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
use yomua_bot::application::config::{load_llm, load_onebot, load_runtime, LlmConfig};
use yomua_bot::application::context::ContextBuilder;
use yomua_bot::application::conversation::ConversationManager;
use yomua_bot::application::emotion_service::EmotionService;
use yomua_bot::application::event_bus::EventBus;
use yomua_bot::application::event_processor::EventProcessor;
use yomua_bot::application::llm_scheduler::{DefaultLlmScheduler, LlmScheduler};
use yomua_bot::application::memory_service::MemoryService;
use yomua_bot::application::message_persistence::MessagePersistence;
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
    ParticipantRepository, RelationshipRepository,
};
use yomua_bot::error::RuntimeError;
use yomua_bot::infrastructure::llm::openai_compatible::{
    OpenAiCompatibleConfig, OpenAiCompatibleProvider,
};
use yomua_bot::infrastructure::llm::LlmProvider;
use yomua_bot::infrastructure::storage::repository::{
    SqliteCharacterBindingRepository, SqliteCharacterRepository, SqliteCharacterStateRepository,
    SqliteConversationRepository, SqliteEmotionStateRepository, SqliteMemoryRepository,
    SqliteMessageRepository, SqliteParticipantRepository, SqliteRelationshipRepository,
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
        timeout: Duration::from_secs(timeout_secs),
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    // 子命令分发：`import-card` 走导入流程；其余参数视为配置目录（兼容旧用法）。
    if args.first().map(String::as_str) == Some("import-card") {
        return run_import(&args[1..]).await;
    }

    let config_dir = PathBuf::from(args.first().map(String::as_str).unwrap_or_default());
    run_runtime(&config_dir).await
}

/// 启动常驻运行时。
///
/// 加载配置 → 初始化日志 → 打开存储 → 建立仓库 → 装配应用层
/// （Runtime / 行为 / 认知 / 情绪 / 关系）→ 启动订阅者 → 启动 OneBot 适配器 →
/// 等待关停信号。
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
        message_repo,
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
    let scheduler: Option<Arc<dyn LlmScheduler>> = if llm_cfg.enabled {
        let provider: Arc<dyn LlmProvider> = Arc::new(build_openai_provider(&llm_cfg)?);
        tracing::info!(target: "llm", model = %provider.name(), "LLM 已启用");
        Some(Arc::new(DefaultLlmScheduler::new(provider)))
    } else {
        tracing::info!(target: "llm", "LLM 未启用，使用确定性回复");
        None
    };
    let cognition = Arc::new(CognitionLayer::new(scheduler, context_builder.clone()));

    // 7. 建立会话管理器、动作执行器、OneBot 适配器。
    let conversation_manager =
        ConversationManager::new(conversation_repo.clone(), participant_repo);
    let adapter = OneBotAdapterImpl::new(onebot_cfg, bus.clone(), conversation_manager).await;
    let adapter = Arc::new(adapter);

    let action_dispatcher = Arc::new(ActionDispatcher::new(
        conversation_repo.clone(),
        adapter.clone(),
    ));

    let delay_executor: Arc<dyn DelayExecutor> = Arc::new(TokioDelayExecutor);
    let reply_processor = Arc::new(ReplyProcessor::new(
        runtime,
        binding_manager,
        behavior_engine.clone(),
        cognition,
        relationship_service,
        emotion_service,
        memory_service,
        action_dispatcher,
        bus.clone(),
        delay_executor,
    ));

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

    // 10. 启动适配器并等待消息。
    adapter.start().await?;
    tracing::info!(target: "runtime", "字符运行时已就绪，等待消息...");

    // 11. 等待关停信号（Ctrl+C）。Core 常驻，适配器断线自动重连。
    tracing::info!(target: "runtime", "正在运行；按 Ctrl+C 退出。");
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| RuntimeError::Internal(format!("无法注册关停信号处理: {e}")))?;
    tracing::info!(target: "runtime", "收到关停信号，正在优雅关闭...");

    // 11. 优雅关停。
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
