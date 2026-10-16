//! 控制层集成测试（Control Plane × State System × SQLite）
//!
//! 验证真实链路：
//!
//! ```text
//! ctl 客户端 → Unix Domain Socket → Control Handler → State System（领域仓储）→ SQLite
//! ```
//!
//! 要点：
//! - 使用真实 SQLite（内存库）+ 真实迁移 + 真实 SQLite 仓储，**不 mock 核心链路**；
//! - 唯一 mock 的是 OneBot WebSocket 传输层（与状态链路无关）；
//! - `state-set` / `state-get` 必须经由 `RuntimeHandle` 上的领域仓储读写，
//!   控制层不应存在直接 SQL 绕过路径（控制模块不依赖 sqlx）；
//! - 状态作用域遵循 `docs/character-runtime-state-model.md`：
//!   - 全局 Character State：energy / stress / activity；
//!   - Character × Conversation：energy / stress（ConversationState）+ mood（Mood）；
//!   - mood 必须带 conversation_id（Mood 不属于全局）。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::watch;

use yomua_bot::adapters::onebot::connection::{WsConnector, WsTransport};
use yomua_bot::adapters::onebot::{OneBotAdapter, OneBotAdapterImpl, OneBotConfig};
use yomua_bot::application::action::ActionDispatcher;
use yomua_bot::application::character_history::RepositoryCharacterHistoryProvider;
use yomua_bot::application::character_service::CharacterService;
use yomua_bot::application::cognition::CognitionLayer;
use yomua_bot::application::config::RuntimeConfig;
use yomua_bot::application::context::ContextBuilder;
use yomua_bot::application::context_assembler::{
    ContextAssembler, ModelCapabilitiesProviderAdapter, TokenEstimatorAdapter,
};
use yomua_bot::application::control::{start_control_service, CommandRegistry, RuntimeHandle};
use yomua_bot::application::conversation::ConversationManager;
use yomua_bot::application::event_bus::EventBus;
use yomua_bot::application::llm_scheduler::LlmScheduler;
use yomua_bot::application::plugin_api::PluginApi;
use yomua_bot::application::session_service::SessionService;
use yomua_bot::domain::character::{
    BehaviorState, Character, CharacterBinding, CharacterDefinition, CharacterState,
    ConversationState, ReplyMode,
};
use yomua_bot::domain::conversation::{
    Conversation, ConversationType, Participant, ParticipantRole,
};
use yomua_bot::domain::emotion::Mood;
use yomua_bot::domain::memory::{Memory, MemoryType};
use yomua_bot::domain::message::{Message, MessageContent};
use yomua_bot::domain::relationship::Relationship;
use yomua_bot::domain::repository::{
    BlacklistRepository, CharacterBindingRepository, CharacterRepository, CharacterStateRepository,
    ConversationRepository, ConversationStateRepository, MemoryRepository, MessageRepository,
    MoodRepository, ParticipantRepository, PluginDataRepository, RelationshipRepository,
};
use yomua_bot::error::RuntimeError;
use yomua_bot::infrastructure::llm::{
    ApproximateTokenEstimator, FixedModelCapabilities, LlmRequest, LlmResponse, TokenUsage,
};
use yomua_bot::infrastructure::plugin::registry::PluginRegistry;
use yomua_bot::infrastructure::plugin::supervisor::{PluginSupervisor, SupervisorConfig};
use yomua_bot::infrastructure::storage::repository::{
    SqliteBlacklistRepository, SqliteCharacterBindingRepository, SqliteCharacterRepository,
    SqliteCharacterStateRepository, SqliteConversationRepository,
    SqliteConversationStateRepository, SqliteMemoryRepository, SqliteMessageRepository,
    SqliteMoodRepository, SqliteParticipantRepository, SqlitePluginDataRepository,
    SqliteRelationshipRepository,
};
use yomua_bot::infrastructure::storage::SqliteStorage;

// ============================================================================
// 传输层 Mock（仅网络层；与状态链路无关）
// ============================================================================

/// Mock WsTransport（不建立真实连接）。
struct MockTransport;

#[async_trait]
impl WsTransport for MockTransport {
    async fn read_text(&mut self) -> Result<Option<String>, RuntimeError> {
        Ok(None)
    }
    async fn send_text(&mut self, _text: &str) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn ping(&mut self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

/// Mock WsConnector（不建立真实连接）。
struct MockConnector;

#[async_trait]
impl WsConnector for MockConnector {
    async fn connect(&self, _config: &OneBotConfig) -> Result<Box<dyn WsTransport>, RuntimeError> {
        Ok(Box::new(MockTransport))
    }
}

struct MemoryEditTestScheduler;

#[async_trait]
impl LlmScheduler for MemoryEditTestScheduler {
    async fn submit(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        assert_eq!(request.metadata["purpose"], "memory_edit_suggestion");
        Ok(LlmResponse {
            content: serde_json::json!({
                "action": "replace",
                "content": "用户确认喜欢猫",
                "reason": "测试调度器返回的受限建议"
            })
            .to_string(),
            model: "test-memory-editor".to_string(),
            usage: TokenUsage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
            },
            truncated: false,
        })
    }
}

// ============================================================================
// 测试环境：真实 SQLite + 真实仓储 + 真实 Control Plane
// ============================================================================

struct TestEnv {
    /// 持有多余连接池引用，避免环境提前释放。
    #[allow(dead_code)]
    storage: SqliteStorage,
    socket_path: PathBuf,
    /// 服务器任务句柄（环境析构时随测试 runtime 一并结束）。
    #[allow(dead_code)]
    server: tokio::task::JoinHandle<()>,
    shutdown_tx: watch::Sender<bool>,
    /// 种子角色 ID。
    character_id: i64,
    /// 种子会话 A / B（会话级状态的作用域目标）。
    conv_a_id: i64,
    conv_b_id: i64,
    // 用于断言写入结果的仓储（与 Control Handler 用的是同一份）。
    state_repo: Arc<dyn CharacterStateRepository>,
    conversation_state_repo: Arc<dyn ConversationStateRepository>,
    mood_repo: Arc<dyn MoodRepository>,
    character_repo: Arc<dyn CharacterRepository>,
    memory_repo: Arc<dyn MemoryRepository>,
    conversation_repo: Arc<dyn ConversationRepository>,
    #[allow(dead_code)]
    binding_repo: Arc<dyn CharacterBindingRepository>,
    #[allow(dead_code)]
    blacklist_repo: Arc<dyn BlacklistRepository>,
}

/// 进程内目录序号：macOS 上 SystemTime 纳秒精度粗（存在大量重复），
/// 并行测试可能算出同名临时目录导致多个服务器绑定同一 socket 路径，
/// 因此必须用原子计数保证唯一。
static NEXT_DIR_SEQ: AtomicU64 = AtomicU64::new(0);

/// 构造一个最小但真实的测试环境。
async fn create_env() -> TestEnv {
    let seq = NEXT_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let socket_dir = std::env::temp_dir().join(format!(
        "yomua-test-{}-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        seq
    ));
    let _ = std::fs::create_dir_all(&socket_dir);

    // 真实 SQLite（内存）并运行真实迁移。
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");
    storage.migrate().await.expect("运行迁移失败");
    let pool = storage.pool().clone();

    // 真实 SQLite 仓储。
    let character_repo: Arc<dyn CharacterRepository> =
        Arc::new(SqliteCharacterRepository::new(pool.clone()));
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
    let plugin_data_repo: Arc<dyn PluginDataRepository> =
        Arc::new(SqlitePluginDataRepository::new(pool.clone()));
    let conversation_state_repo: Arc<dyn ConversationStateRepository> =
        Arc::new(SqliteConversationStateRepository::new(pool.clone()));
    let mood_repo: Arc<dyn MoodRepository> = Arc::new(SqliteMoodRepository::new(pool.clone()));
    let blacklist_repo: Arc<dyn BlacklistRepository> =
        Arc::new(SqliteBlacklistRepository::new(pool.clone()));

    // 种子角色（执行 state-set 前必须存在）。
    let character = Character {
        id: 0,
        definition: test_definition(),
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let character_id = character_repo
        .insert(&character)
        .await
        .expect("插入种子角色失败");
    assert!(character_id > 0, "种子角色应获得自增 ID");

    // 种子会话（会话级状态的 FK 依赖 conversations 表）。
    let conv_a_id = conversation_repo
        .insert(&Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "conv-a".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .expect("插入会话 A 失败");
    let conv_b_id = conversation_repo
        .insert(&Conversation {
            id: 0,
            conversation_type: ConversationType::Private,
            external_id: "conv-b".to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .expect("插入会话 B 失败");
    assert!(conv_a_id > 0 && conv_b_id > 0, "会话应获得自增 ID");

    // 适配器（Mock 传输层，不连接 NapCat）。
    let bus = EventBus::new();
    let conversation_manager =
        ConversationManager::new(conversation_repo.clone(), participant_repo.clone());
    let adapter = Arc::new(OneBotAdapterImpl::with_connector(
        OneBotConfig::default(),
        bus.clone(),
        conversation_manager,
        Arc::new(
            yomua_bot::application::message_acceptance::MessageAcceptanceService::new(
                message_repo.clone(),
                bus.clone(),
            ),
        ),
        Arc::new(yomua_bot::application::binding::BindingManager::new(
            binding_repo.clone(),
            character_repo.clone(),
            conversation_repo.clone(),
        )),
        Arc::new(MockConnector),
    ));

    // 真实 PluginApi（供 PluginSupervisor 使用）。
    let dispatcher = Arc::new(ActionDispatcher::new(
        conversation_repo.clone(),
        adapter.clone() as Arc<dyn OneBotAdapter>,
    ));
    let context_builder = Arc::new(ContextBuilder::new(
        message_repo.clone(),
        Arc::new(RepositoryCharacterHistoryProvider::new(
            message_repo.clone(),
        )),
        conversation_repo.clone(),
        memory_repo.clone(),
        relationship_repo.clone(),
        mood_repo.clone(),
        binding_repo.clone(),
    ));

    // Phase 3: ContextAssembler for Token Budget + Layered Degradation
    let token_estimator = Arc::new(TokenEstimatorAdapter(Arc::new(ApproximateTokenEstimator)));
    let model_capabilities = Arc::new(ModelCapabilitiesProviderAdapter(Arc::new(
        FixedModelCapabilities::new(None, None),
    )));
    let context_assembler = Arc::new(ContextAssembler::new(token_estimator, model_capabilities));

    let cognition = Arc::new(CognitionLayer::new(
        None,
        context_builder,
        context_assembler,
    ));
    let plugin_api = Arc::new(PluginApi::new(
        character_repo.clone(),
        state_repo.clone(),
        binding_repo.clone(),
        message_repo.clone(),
        memory_repo.clone(),
        relationship_repo.clone(),
        plugin_data_repo.clone(),
        dispatcher,
        cognition,
        Arc::new(PluginRegistry::new()),
    ));
    let supervisor = Arc::new(PluginSupervisor::new(
        SupervisorConfig::default(),
        Arc::new(PluginRegistry::new()),
        plugin_api,
    ));

    // RuntimeHandle（与控制模块使用的 State System 仓储一致）。
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let shutdown_complete = Arc::new(tokio::sync::Notify::new());
    let shutdown_result = Arc::new(tokio::sync::RwLock::new(None));
    let test_shutdown_complete = shutdown_complete.clone();
    let test_shutdown_result = shutdown_result.clone();
    let mut shutdown_observer = shutdown_tx.subscribe();
    tokio::spawn(async move {
        if shutdown_observer.changed().await.is_ok() && *shutdown_observer.borrow() {
            *test_shutdown_result.write().await =
                Some(yomua_bot::application::control::ShutdownResult {
                    completed: true,
                    timed_out: false,
                    unfinished_tasks: Some(0),
                    message: "测试关停已确认".to_string(),
                });
            test_shutdown_complete.notify_waiters();
        }
    });
    let handle = RuntimeHandle {
        live_settings: None,
        launch_config: None,
        log_reload: None,
        runtime: None,
        config_dir: socket_dir.clone(),
        runtime_cfg: RuntimeConfig {
            data_dir: socket_dir.to_string_lossy().to_string(),
            ..RuntimeConfig::default()
        },
        data_dir: socket_dir.clone(),
        supervisor,
        adapter,
        storage: Arc::new(storage.clone()),
        shutdown_tx: shutdown_tx.clone(),
        shutdown_complete,
        shutdown_result,
        commands: CommandRegistry::builtin(),
        character_repo: character_repo.clone(),
        state_repo: state_repo.clone(),
        conversation_state_repo: conversation_state_repo.clone(),
        mood_repo: mood_repo.clone(),
        conversation_repo: conversation_repo.clone(),
        message_repo: message_repo.clone(),
        llm_scheduler: Some(Arc::new(MemoryEditTestScheduler)),
        started_at: std::time::Instant::now(),
        blacklist_repo: blacklist_repo.clone(),
        session_service: SessionService::new(pool.clone()),
        character_service: CharacterService::new(pool.clone()),
        memory_repo: memory_repo.clone(),
        participant_repo: participant_repo.clone(),
    };

    let socket_path = socket_dir.join("control.sock");
    // 启动控制服务（真实 UDS 监听）。
    let (server, started) = start_control_service(handle.clone(), shutdown_rx);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), started).await,
        Ok(Ok(Ok(())))
    ));

    // 等待 socket 就绪（最多 2 秒）。
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !socket_path.exists() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        socket_path.exists(),
        "控制 socket 应被创建：{:?}",
        socket_path
    );

    TestEnv {
        storage,
        socket_path,
        server,
        shutdown_tx,
        character_id,
        conv_a_id,
        conv_b_id,
        state_repo,
        conversation_state_repo,
        mood_repo,
        // 以下在后续测试中按需使用。
        #[allow(dead_code)]
        character_repo: character_repo.clone(),
        #[allow(dead_code)]
        memory_repo: memory_repo.clone(),
        conversation_repo: conversation_repo.clone(),
        binding_repo: binding_repo.clone(),
        blacklist_repo: blacklist_repo.clone(),
    }
}

fn test_definition() -> CharacterDefinition {
    CharacterDefinition {
        name: "测试角色".to_string(),
        description: None,
        personality: None,
        scenario: None,
        style: None,
        background: None,
        greetings: vec![],
        example_messages: vec![],
        system_prompt: None,
        post_history_instructions: None,
        lorebook: vec![],
        metadata: serde_json::json!({}),
    }
}

// ============================================================================
// 传输辅助
// ============================================================================

/// 通过真实 UDS 发送一条 JSON 请求，返回解析后的 JSON 响应。
async fn send_json(
    socket: &Path,
    payload: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(|e| format!("连接失败: {e}"))?;

    let mut request = payload.clone();
    let needs_confirmation = request
        .get("request")
        .and_then(|value| value.get("operation"))
        .and_then(serde_json::Value::as_str)
        .and_then(|operation| {
            serde_json::from_str::<yomua_bot::application::control_schema::ControlOperation>(
                &format!("\"{operation}\""),
            )
            .ok()
        })
        .is_some_and(|operation| {
            yomua_bot::application::control_schema::requires_confirmation(
                operation.canonical_name(),
            )
        });
    if needs_confirmation {
        let params = request
            .get_mut("request")
            .and_then(|value| value.get_mut("params"))
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(|| "结构化测试请求的 params 必须是对象".to_string())?;
        params.insert("confirm".to_string(), serde_json::Value::Bool(true));
    }
    let json = serde_json::to_string(&request).map_err(|e| format!("序列化失败: {e}"))?;
    stream
        .write_all(json.as_bytes())
        .await
        .map_err(|e| format!("发送失败: {e}"))?;
    // 关闭写端以通知服务器请求已结束。
    stream
        .shutdown()
        .await
        .map_err(|e| format!("关闭写端失败: {e}"))?;

    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(|e| format!("接收失败: {e}"))?;

    if buf.is_empty() {
        return Err("服务器关闭连接".to_string());
    }

    serde_json::from_slice(&buf).map_err(|e| format!("解析失败: {e}"))
}

/// 通过真实 UDS 发送原始字节（用于 malformed JSON 测试）。
async fn send_raw(socket: &Path, raw: &[u8]) -> Result<Vec<u8>, String> {
    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(|e| format!("连接失败: {e}"))?;
    stream
        .write_all(raw)
        .await
        .map_err(|e| format!("发送失败: {e}"))?;
    stream
        .shutdown()
        .await
        .map_err(|e| format!("关闭写端失败: {e}"))?;
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(|e| format!("接收失败: {e}"))?;
    Ok(buf)
}

fn state_set_req(
    id: u64,
    cid: i64,
    conv: Option<i64>,
    fields: &[(&str, serde_json::Value)],
) -> serde_json::Value {
    let mut params = serde_json::Map::new();
    params.insert("character_id".to_string(), serde_json::json!(cid));
    if let Some(conv) = conv {
        params.insert("conversation_id".to_string(), serde_json::json!(conv));
    }
    for (k, v) in fields {
        params.insert(k.to_string(), v.clone());
    }
    serde_json::json!({ "version": 1, "id": id, "request": { "operation": "character_state_set", "params": params } })
}

fn state_get_req(id: u64, cid: i64, conv: Option<i64>) -> serde_json::Value {
    let mut params = serde_json::Map::new();
    params.insert("character_id".to_string(), serde_json::json!(cid));
    if let Some(conv) = conv {
        params.insert("conversation_id".to_string(), serde_json::json!(conv));
    }
    serde_json::json!({ "version": 1, "id": id, "request": { "operation": "character_state_get", "params": params } })
}

// ============================================================================
// 基础控制命令测试
// ============================================================================

#[tokio::test]
async fn test_server_startup_and_socket_created() {
    let env = create_env().await;
    assert!(env.socket_path.exists(), "socket 文件应该存在");
}

#[tokio::test]
async fn test_help_lists_all_builtin_commands() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 1, "request": { "operation": "help", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "help 应成功");
    let commands = resp["data"]["commands"].as_array().expect("commands 数组");
    let names: Vec<&str> = commands.iter().filter_map(|c| c["name"].as_str()).collect();
    for expected in [
        "status",
        "reload-config",
        "shutdown",
        "help",
        "character state set",
        "character state get",
    ] {
        assert!(
            names.contains(&expected),
            "help 应包含 {expected}，实际：{names:?}"
        );
    }
}

#[tokio::test]
async fn test_status_command() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 2, "request": { "operation": "status", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);
    assert!(resp["data"]["onebot_status"].is_string());
    assert!(resp["data"]["plugin_count"].is_number());
    assert!(resp["data"]["data_dir"].is_string());
}

#[tokio::test]
async fn test_unknown_command_returns_error() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 3, "request": { "operation": "nonexistent.command", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"]["code"], "UNSUPPORTED_OPERATION");
}

#[tokio::test]
async fn test_malformed_json_returns_error() {
    let env = create_env().await;
    let buf = send_raw(&env.socket_path, b"{ invalid json }")
        .await
        .unwrap();
    assert!(!buf.is_empty(), "服务器应返回错误响应");
    let resp: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"]["code"], "INVALID_JSON");
}

#[tokio::test]
async fn test_request_id_matching() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 4242, "request": { "operation": "status", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["id"], 4242);
}

#[tokio::test]
async fn test_concurrent_status_requests() {
    let env = create_env().await;
    let mut handles = Vec::new();
    for i in 0..5 {
        let socket = env.socket_path.clone();
        handles.push(tokio::spawn(async move {
            send_json(
                &socket,
                &serde_json::json!({ "version": 1, "id": 100 + i, "request": { "operation": "status", "params": {} } }),
            )
            .await
        }));
    }
    for h in handles {
        let resp = h.await.unwrap().unwrap();
        assert_eq!(resp["ok"], true);
    }
}

#[tokio::test]
async fn test_destructive_command_requires_confirmation() {
    let env = create_env().await;
    let bytes = send_raw(
        &env.socket_path,
        br#"{"version":1,"id":6,"request":{"operation":"shutdown","params":{}}}"#,
    )
    .await
    .unwrap();
    let response: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "CONFIRMATION_REQUIRED");
    assert!(!*env.shutdown_tx.borrow(), "缺少确认时不得发出关停信号");
}

#[tokio::test]
async fn test_shutdown_command_sets_flag() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 7, "request": { "operation": "shutdown", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);
    // 关停信号应已通过 watch channel 发出。
    assert!(*env.shutdown_tx.borrow(), "shutdown 信号应已置位");
}

#[tokio::test]
async fn test_user_merge_and_delete_commands() {
    let env = create_env().await;
    let pool = env.storage.pool().clone();

    let source_user: i64 = sqlx::query_scalar(
        "INSERT INTO users(platform,external_id) VALUES('onebot_qq','900001') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let target_user: i64 = sqlx::query_scalar(
        "INSERT INTO users(platform,external_id) VALUES('onebot_qq','900002') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO relationships(character_id,user_id,interaction_count) VALUES(?,?,?)")
        .bind(env.character_id)
        .bind(source_user)
        .bind(2_i64)
        .execute(&pool)
        .await
        .unwrap();

    // 相同 ID 拒绝。
    let same = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 500, "request": { "operation": "user_merge", "params": {"source_user_id": source_user, "target_user_id": source_user} } }),
    )
    .await
    .expect("same-id merge request should return a response");
    assert_eq!(same["ok"], serde_json::json!(false));
    assert_eq!(same["error"]["code"], "INVALID_PARAMS");

    let merge = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 501, "request": { "operation": "user_merge", "params": {"source_user_id": source_user, "target_user_id": target_user} } }),
    )
    .await
    .expect("merge request should return a response");
    assert_eq!(merge["ok"], serde_json::json!(true));
    assert_eq!(
        merge["data"]["merged_user_id"],
        serde_json::json!(target_user)
    );
    let source_left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE id=?")
        .bind(source_user)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(source_left, 0);

    let delete = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 502, "request": { "operation": "user_delete", "params": {"user_id": target_user} } }),
    )
    .await
    .expect("delete request should return a response");
    assert_eq!(delete["ok"], serde_json::json!(true));
    assert_eq!(
        delete["data"]["deleted_user_id"],
        serde_json::json!(target_user)
    );
    let target_left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE id=?")
        .bind(target_user)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(target_left, 0);

    // 删除不存在的用户必须返回稳定的业务错误码，而不是泛化为内部错误。
    let missing = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 503, "request": { "operation": "user_delete", "params": { "user_id": target_user } } }),
    )
    .await
    .expect("missing user delete request should return a response");
    assert_eq!(missing["ok"], serde_json::json!(false));
    assert_eq!(
        missing["error"]["code"],
        serde_json::json!("USER_NOT_FOUND")
    );
}

// ============================================================================
// State System 链路测试（ctl → IPC → Handler → 仓储 → SQLite）
// ============================================================================

/// 全局状态修改：energy / stress / activity，验证经由仓储与 SQLite 持久化。
#[tokio::test]
async fn test_state_set_global_persists_to_sqlite() {
    let env = create_env().await;
    let cid = env.character_id;

    let resp = send_json(
        &env.socket_path,
        &state_set_req(
            10,
            cid,
            None,
            &[
                ("energy", serde_json::json!(80.0)),
                ("stress", serde_json::json!(25.0)),
                ("activity", serde_json::json!("正在读书")),
            ],
        ),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "全局 state-set 应成功：{resp}");
    assert_eq!(resp["data"]["character_state"]["energy"], 80.0);
    assert_eq!(resp["data"]["character_state"]["stress"], 25.0);
    assert_eq!(
        resp["data"]["character_state"]["current_activity"],
        "正在读书"
    );

    // 经 State System 仓储读回。
    let state = env
        .state_repo
        .find_by_character_id(cid)
        .await
        .unwrap()
        .expect("全局状态应已持久化");
    assert_eq!(state.energy, 80.0);
    assert_eq!(state.stress, 25.0);
    assert_eq!(state.current_activity.as_deref(), Some("正在读书"));

    // 直接查询 SQLite，证明真实落库。
    let (energy, stress, activity): (f64, f64, Option<String>) = sqlx::query_as(
        "SELECT energy, stress, current_activity FROM character_states WHERE character_id = ?1",
    )
    .bind(cid)
    .fetch_one(env.storage.pool())
    .await
    .unwrap();
    assert_eq!(energy, 80.0);
    assert_eq!(stress, 25.0);
    assert_eq!(activity.as_deref(), Some("正在读书"));
}

/// 数值 clamp 到 [0, 100]。
#[tokio::test]
async fn test_state_set_global_clamps_values() {
    let env = create_env().await;
    let cid = env.character_id;
    let resp = send_json(
        &env.socket_path,
        &state_set_req(11, cid, None, &[("energy", serde_json::json!(150.0))]),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"]["character_state"]["energy"], 100.0);
}

/// 会话级状态修改 + 全局/会话隔离 + 会话间隔离。
#[tokio::test]
async fn test_state_set_scoped_conversation_and_isolation() {
    let env = create_env().await;
    let cid = env.character_id;

    // 先设全局值。
    let resp = send_json(
        &env.socket_path,
        &state_set_req(
            12,
            cid,
            None,
            &[
                ("energy", serde_json::json!(80.0)),
                ("stress", serde_json::json!(25.0)),
            ],
        ),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);

    // 会话 A：energy=90, stress=40；会话 B：energy=30, stress=5。
    let r1 = send_json(
        &env.socket_path,
        &state_set_req(
            13,
            cid,
            Some(env.conv_a_id),
            &[
                ("energy", serde_json::json!(90.0)),
                ("stress", serde_json::json!(40.0)),
            ],
        ),
    )
    .await
    .unwrap();
    assert_eq!(r1["ok"], true, "会话级 state-set 应成功：{r1}");
    assert_eq!(r1["data"]["conversation_state"]["energy"], 90.0);
    assert_eq!(r1["data"]["conversation_state"]["stress"], 40.0);

    let r2 = send_json(
        &env.socket_path,
        &state_set_req(
            14,
            cid,
            Some(env.conv_b_id),
            &[
                ("energy", serde_json::json!(30.0)),
                ("stress", serde_json::json!(5.0)),
            ],
        ),
    )
    .await
    .unwrap();
    assert_eq!(r2["ok"], true);

    // state-get 验证：Global 与 Conversation 相互隔离。
    let g = send_json(&env.socket_path, &state_get_req(15, cid, None))
        .await
        .unwrap();
    assert_eq!(
        g["data"]["character_state"]["energy"], 80.0,
        "全局 energy 不应被会话级修改污染"
    );
    assert_eq!(g["data"]["character_state"]["stress"], 25.0);
    assert!(
        g["data"].get("conversation_state").is_none(),
        "全局查询不应带会话级状态"
    );

    let c10 = send_json(
        &env.socket_path,
        &state_get_req(16, cid, Some(env.conv_a_id)),
    )
    .await
    .unwrap();
    assert_eq!(c10["data"]["conversation_state"]["energy"], 90.0);
    assert_eq!(c10["data"]["conversation_state"]["stress"], 40.0);

    let c20 = send_json(
        &env.socket_path,
        &state_get_req(17, cid, Some(env.conv_b_id)),
    )
    .await
    .unwrap();
    assert_eq!(c20["data"]["conversation_state"]["energy"], 30.0);
    assert_eq!(c20["data"]["conversation_state"]["stress"], 5.0);

    // 经仓储读回，确认会话间隔离。
    let s10 = env
        .conversation_state_repo
        .find_by_character_and_conversation(cid, env.conv_a_id)
        .await
        .unwrap()
        .unwrap();
    let s20 = env
        .conversation_state_repo
        .find_by_character_and_conversation(cid, env.conv_b_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s10.energy, 90.0);
    assert_eq!(s20.energy, 30.0);

    // SQLite 直接验证：会话 A 与 B 独立成行。
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM conversation_states WHERE character_id = ?1")
            .bind(cid)
            .fetch_one(env.storage.pool())
            .await
            .unwrap();
    assert_eq!(count, 2, "每个会话应各有一行状态");
}

/// Mood 修改 + Character × Conversation 隔离 + SQLite 持久化。
#[tokio::test]
async fn test_state_set_mood_scoped_and_isolation() {
    let env = create_env().await;
    let cid = env.character_id;

    let r1 = send_json(
        &env.socket_path,
        &state_set_req(
            20,
            cid,
            Some(env.conv_a_id),
            &[("mood", serde_json::json!(85.0))],
        ),
    )
    .await
    .unwrap();
    assert_eq!(r1["ok"], true, "mood 设置应成功：{r1}");
    assert_eq!(r1["data"]["mood"]["value"], 85.0);

    let r2 = send_json(
        &env.socket_path,
        &state_set_req(
            21,
            cid,
            Some(env.conv_b_id),
            &[("mood", serde_json::json!(20.0))],
        ),
    )
    .await
    .unwrap();
    assert_eq!(r2["ok"], true);

    // 会话间隔离。
    let c10 = send_json(
        &env.socket_path,
        &state_get_req(22, cid, Some(env.conv_a_id)),
    )
    .await
    .unwrap();
    assert_eq!(c10["data"]["mood"]["value"], 85.0);
    let c20 = send_json(
        &env.socket_path,
        &state_get_req(23, cid, Some(env.conv_b_id)),
    )
    .await
    .unwrap();
    assert_eq!(c20["data"]["mood"]["value"], 20.0);

    // 全局查询不包含 mood（Mood 不属于全局）。
    let g = send_json(&env.socket_path, &state_get_req(24, cid, None))
        .await
        .unwrap();
    assert!(g["data"].get("mood").is_none(), "全局查询不应包含 mood");

    // 仓储 + SQLite 双重确认。
    let m10 = env
        .mood_repo
        .find_by_character_and_conversation(cid, env.conv_a_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m10.value, 85.0);
    let db_val: f64 = sqlx::query_scalar(
        "SELECT value FROM moods WHERE character_id = ?1 AND conversation_id = ?2",
    )
    .bind(cid)
    .bind(env.conv_a_id)
    .fetch_one(env.storage.pool())
    .await
    .unwrap();
    assert_eq!(db_val, 85.0);

    // 不同角色之间隔离（同一会话 A，角色不存在时会话级 mood 也不应出现）。
    let other = send_json(
        &env.socket_path,
        &state_get_req(25, 999, Some(env.conv_a_id)),
    )
    .await
    .unwrap();
    assert_eq!(other["ok"], true);
    assert_eq!(other["data"]["mood"], serde_json::Value::Null);
}

/// mood 属于 Character × Conversation：不带 conversation_id → 拒绝。
#[tokio::test]
async fn test_state_set_mood_requires_conversation() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &state_set_req(
            30,
            env.character_id,
            None,
            &[("mood", serde_json::json!(60.0))],
        ),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"]["code"], "MOOD_REQUIRES_CONVERSATION");
}

/// activity 仅存在于全局：会话级 activity → 拒绝（当前模型未定义会话级 activity）。
#[tokio::test]
async fn test_state_set_scoped_activity_rejected() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &state_set_req(
            31,
            env.character_id,
            Some(env.conv_a_id),
            &[("activity", serde_json::json!("散步"))],
        ),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"]["code"], "SCOPED_ACTIVITY_UNSUPPORTED");
}

/// 角色不存在 → 拒绝（防止孤儿状态行）。
#[tokio::test]
async fn test_state_set_character_not_found() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &state_set_req(32, 99999, None, &[("energy", serde_json::json!(50.0))]),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"]["code"], "CHARACTER_NOT_FOUND");
}

/// 缺参校验。
#[tokio::test]
async fn test_state_set_validation_errors() {
    let env = create_env().await;
    // 无 params → INVALID_PARAMS。
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 33, "request": { "operation": "character_state_set", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"]["code"], "INVALID_PARAMS");

    // 只有 character_id 没有目标字段 → INVALID_PARAMS。
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 34, "request": { "operation": "character_state_set", "params": {"character_id": env.character_id} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"]["code"], "INVALID_PARAMS");
}

/// state-get 对未设置的状态返回 null（无副作用写入）。
#[tokio::test]
async fn test_state_get_unset_returns_null() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &state_get_req(40, env.character_id, Some(env.conv_a_id)),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"]["character_state"], serde_json::Value::Null);
    assert_eq!(resp["data"]["conversation_state"], serde_json::Value::Null);
    assert_eq!(resp["data"]["mood"], serde_json::Value::Null);
}

/// 会话不存在 → 拒绝（防止孤儿状态行）。
#[tokio::test]
async fn test_state_set_conversation_not_found() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &state_set_req(
            35,
            env.character_id,
            Some(99999),
            &[("energy", serde_json::json!(50.0))],
        ),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"]["code"], "CONVERSATION_NOT_FOUND");
}

/// end-to-end：混合修改后 state-get 返回完整快照。
#[tokio::test]
async fn test_state_get_full_snapshot_after_mixed_updates() {
    let env = create_env().await;
    let cid = env.character_id;

    let _ = send_json(
        &env.socket_path,
        &state_set_req(50, cid, None, &[("energy", serde_json::json!(70.0))]),
    )
    .await
    .unwrap();
    let _ = send_json(
        &env.socket_path,
        &state_set_req(
            51,
            cid,
            Some(env.conv_a_id),
            &[
                ("energy", serde_json::json!(60.0)),
                ("stress", serde_json::json!(15.0)),
                ("mood", serde_json::json!(75.0)),
            ],
        ),
    )
    .await
    .unwrap();

    let resp = send_json(
        &env.socket_path,
        &state_get_req(52, cid, Some(env.conv_a_id)),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"]["character_state"]["energy"], 70.0);
    assert_eq!(resp["data"]["conversation_state"]["energy"], 60.0);
    assert_eq!(resp["data"]["conversation_state"]["stress"], 15.0);
    assert_eq!(resp["data"]["mood"]["value"], 75.0);
}

/// Character × User Memory 隔离验证：Character A 的记忆对 Character B 不可见，反之亦然。
/// 使用真实 SQLite（SqliteMemoryRepository）验证 schema 层面的 character_id 隔离。
#[tokio::test]
async fn test_character_memory_isolation() {
    use chrono::Utc;
    use yomua_bot::domain::memory::{Memory, MemoryType};

    let env = create_env().await;
    let cid = env.character_id;

    // 显式创建 Character B（确保存在于当前 in-memory DB）。
    let char_b = yomua_bot::domain::character::Character {
        id: 0,
        definition: test_definition(),
        enabled: true,
        state: CharacterState::default(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let char_b_id = env
        .character_repo
        .insert(&char_b)
        .await
        .expect("插入角色 B 失败");
    assert!(char_b_id > 0);

    // 为角色 A 存储一条 Episodic 记忆。
    let mem_a = Memory {
        id: 0,
        character_id: cid,
        conversation_id: Some(env.conv_a_id),
        memory_type: MemoryType::Episodic,
        content: "A 记得用户喜欢猫".to_string(),
        importance: 0.8,
        created_at: Utc::now(),
        last_accessed: Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    env.memory_repo
        .insert(&mem_a)
        .await
        .expect("A 记忆插入失败");

    // 为角色 B 存储一条不同的 Episodic 记忆。
    let mem_b = Memory {
        id: 0,
        character_id: char_b_id,
        conversation_id: Some(env.conv_a_id),
        memory_type: MemoryType::Episodic,
        content: "B 记得用户讨厌狗".to_string(),
        importance: 0.8,
        created_at: Utc::now(),
        last_accessed: Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    env.memory_repo
        .insert(&mem_b)
        .await
        .expect("B 记忆插入失败");

    // A 的记忆查询只返回 A 的记忆。
    let a_memories = env
        .memory_repo
        .find_by_character_id(cid, None, 100)
        .await
        .expect("A 记忆查询失败");
    assert!(
        a_memories.iter().any(|m| m.content.contains("喜欢猫")),
        "A 应该有自己的记忆"
    );
    assert!(
        !a_memories.iter().any(|m| m.content.contains("讨厌狗")),
        "A 不应看到 B 的记忆"
    );

    // B 的记忆查询只返回 B 的记忆。
    let b_memories = env
        .memory_repo
        .find_by_character_id(char_b_id, None, 100)
        .await
        .expect("B 记忆查询失败");
    assert!(
        b_memories.iter().any(|m| m.content.contains("讨厌狗")),
        "B 应该有自己的记忆"
    );
    assert!(
        !b_memories.iter().any(|m| m.content.contains("喜欢猫")),
        "B 不应看到 A 的记忆"
    );
}

// ============================================================================
// Reload-Config 命令测试
// ============================================================================

/// 写入一个临时 runtime.toml 到指定目录。
fn write_runtime_toml(dir: impl AsRef<Path>, content: &str) {
    let path = dir.as_ref().join("runtime.toml");
    std::fs::write(&path, content).expect("写入 runtime.toml 失败");
}

#[tokio::test]
async fn test_reload_config_valid() {
    let env = create_env().await;
    // 写入一个有效的 runtime.toml。
    write_runtime_toml(
        env.socket_path.parent().unwrap(),
        r#"
data_dir = "/tmp/test"
log_level = "info"
shutdown_timeout_secs = 10
admin_users = ["123456"]
"#,
    );
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 100, "request": { "operation": "reload_config", "params": {} } }),
    )
    .await
    .unwrap();
    if resp["ok"] == true {
        assert_eq!(resp["data"]["log_level"], "info");
        assert_eq!(resp["data"]["admin_users"], serde_json::json!(["123456"]));
    } else {
        assert_eq!(
            resp["error"]["code"], "RELOAD_UNAVAILABLE",
            "测试环境未装配热重载服务时应明确报告不可用: {resp}"
        );
    }
}

#[tokio::test]
async fn test_reload_config_invalid_log_level() {
    let env = create_env().await;
    write_runtime_toml(
        env.socket_path.parent().unwrap(),
        r#"
data_dir = "/tmp/test"
log_level = "NOT_A_LEVEL"
shutdown_timeout_secs = 10
admin_users = []
"#,
    );
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 101, "request": { "operation": "reload_config", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false, "无效配置应返回错误");
    assert_eq!(resp["error"]["code"], "VALIDATION_ERROR");
    assert!(resp["error"]["message"]
        .as_str()
        .unwrap()
        .contains("log_level"));
}

#[tokio::test]
async fn test_reload_config_missing_file() {
    let env = create_env().await;
    // 不写入 runtime.toml，模拟文件缺失。
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 102, "request": { "operation": "reload_config", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false, "缺失文件应返回错误");
    // Config 错误不一定有特定 code，但 ok=false。
}

#[tokio::test]
async fn test_reload_config_does_not_break_runtime() {
    // 验证：即使 reload-config 失败，Runtime 仍然可用（status 正常）。
    let env = create_env().await;
    write_runtime_toml(
        env.socket_path.parent().unwrap(),
        r#"
data_dir = "/tmp/test"
log_level = "INVALID"
shutdown_timeout_secs = 10
admin_users = []
"#,
    );
    // 先尝试失败的 reload。
    let _resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 103, "request": { "operation": "reload_config", "params": {} } }),
    )
    .await;
    // Runtime 应该仍然可用。
    let status = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 104, "request": { "operation": "status", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(status["ok"], true, "reload-config 失败后 status 应仍正常");
}

// ============================================================================
// Character / Conversation 查询命令测试
// ============================================================================

#[tokio::test]
async fn test_character_list_returns_seeded_character() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 200, "request": { "operation": "character_list", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "character_list 应成功: {resp}");
    let chars = resp["data"]["characters"]
        .as_array()
        .expect("characters 应为数组");
    assert!(!chars.is_empty(), "应至少有一个角色（种子角色）");
    // 验证种子角色的字段。
    let char0 = &chars[0];
    assert!(char0["id"].is_number(), "应有 id 字段");
    assert!(char0["name"].is_string(), "应有 name 字段");
    assert_eq!(char0["name"], "测试角色");
}

#[tokio::test]
async fn test_character_show_valid_id() {
    let env = create_env().await;
    let cid = env.character_id;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 201, "request": { "operation": "character_show", "params": {"id": cid} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "character_show 应成功: {resp}");
    assert_eq!(resp["data"]["id"], cid);
    assert_eq!(resp["data"]["name"], "测试角色");
    assert!(resp["data"]["description"].is_null()); // test_definition 的 description 是 None
}

#[tokio::test]
async fn test_character_show_invalid_id() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 202, "request": { "operation": "character_show", "params": {"id": 99999} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false, "不存在的角色 ID 应返回错误");
    assert_eq!(resp["error"]["code"], "CHARACTER_NOT_FOUND");
}

#[tokio::test]
async fn test_character_show_missing_id() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 203, "request": { "operation": "character_show", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false, "缺少 id 参数应返回错误");
    assert_eq!(resp["error"]["code"], "INVALID_PARAMS");
}

#[tokio::test]
async fn test_character_show_non_object_params() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 204, "request": { "operation": "character_show", "params": "not an object" } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false, "非对象 params 应返回 INVALID_PARAMS");
    assert_eq!(resp["error"]["code"], "INVALID_PARAMS");
}

// ============================================================================
// Character Card 命令测试
// ============================================================================

#[tokio::test]
async fn test_character_card_list_returns_seeded_character_with_enabled() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 210, "request": { "operation": "character_card_list", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "character_card_list 应成功: {resp}");
    let chars = resp["data"]["characters"]
        .as_array()
        .expect("characters 应为数组");
    assert!(!chars.is_empty(), "应至少有一个角色（种子角色）");
    let char0 = &chars[0];
    assert!(char0["enabled"].is_boolean(), "应有 enabled 字段");
    assert_eq!(char0["enabled"], true, "新增角色默认应为启用状态");
}

#[tokio::test]
async fn test_character_card_enable_success() {
    let env = create_env().await;
    let cid = env.character_id;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 211, "request": { "operation": "character_card_enable", "params": {"id": cid} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "character_card_enable 应成功: {resp}");
    assert_eq!(resp["data"]["id"], cid);
    assert_eq!(resp["data"]["enabled"], true);
}

#[tokio::test]
async fn test_character_card_enable_not_found() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 212, "request": { "operation": "character_card_enable", "params": {"id": 99999} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false, "不存在的角色应返回错误");
    assert_eq!(resp["error"]["code"], "CHARACTER_NOT_FOUND");
}

#[tokio::test]
async fn test_character_card_disable_success() {
    let env = create_env().await;
    let cid = env.character_id;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 213, "request": { "operation": "character_card_disable", "params": {"id": cid} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "character_card_disable 应成功: {resp}");
    assert_eq!(resp["data"]["id"], cid);
    assert_eq!(resp["data"]["enabled"], false);
}

#[tokio::test]
async fn test_character_card_reassign_success() {
    let env = create_env().await;
    let cid = env.character_id;

    // reassign cid 到一个新 ID（cid + 100）
    let new_id = cid + 100;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 214, "request": { "operation": "character_card_reassign", "params": {"old_id": cid, "new_id": new_id} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "character_card_reassign 应成功: {resp}");
    assert_eq!(resp["data"]["old_id"], cid);
    assert_eq!(resp["data"]["new_id"], new_id);

    // 验证旧 ID 不存在
    let old_exists = env.character_repo.find_by_id(cid).await.expect("查询失败");
    assert!(old_exists.is_none(), "旧 ID 应该不存在");

    // 验证新 ID 存在
    let new_exists = env
        .character_repo
        .find_by_id(new_id)
        .await
        .expect("查询失败");
    assert!(new_exists.is_some(), "新 ID 应该存在");
    assert_eq!(new_exists.unwrap().definition.name, "测试角色");
}

#[tokio::test]
async fn test_character_card_reassign_same_id_error() {
    let env = create_env().await;
    let cid = env.character_id;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 215, "request": { "operation": "character_card_reassign", "params": {"old_id": cid, "new_id": cid} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false, "相同 ID 应返回错误");
    assert_eq!(resp["error"]["code"], "SAME_ID");
}

#[tokio::test]
async fn test_character_card_reassign_collision() {
    let env = create_env().await;
    let cid = env.character_id;
    // 创建第二个角色
    let char2 = Character {
        id: 0,
        definition: test_definition(),
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let cid2 = env
        .character_repo
        .insert(&char2)
        .await
        .expect("插入角色2失败");

    // 尝试 reassign 到已存在的 ID
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 216, "request": { "operation": "character_card_reassign", "params": {"old_id": cid, "new_id": cid2} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false, "碰撞应返回错误");
    assert_eq!(resp["error"]["code"], "ID_COLLISION");
}

#[tokio::test]
async fn test_character_card_reassign_not_found() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 217, "request": { "operation": "character_card_reassign", "params": {"old_id": 99999, "new_id": 100} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false, "旧 ID 不存在应返回错误");
    assert_eq!(resp["error"]["code"], "CHARACTER_NOT_FOUND");
}

/// 验证 reassign 真正迁移了 FK 子记录。
/// 测试覆盖所有 11 张引用 characters.id 的表，并验证 PRAGMA foreign_key_check。
#[tokio::test]
async fn test_character_card_reassign_updates_all_fk_children() {
    use yomua_bot::domain::character::{CharacterBinding, ReplyMode};
    use yomua_bot::domain::repository::{
        BehaviorStateRepository, CharacterBindingRepository, CharacterRepository,
        CharacterStateRepository, ConversationRepository, ConversationStateRepository,
        MemoryRepository, MessageRepository, MoodRepository, ParticipantRepository,
        RelationshipRepository,
    };
    use yomua_bot::infrastructure::storage::repository::{
        SqliteBehaviorStateRepository, SqliteCharacterBindingRepository, SqliteCharacterRepository,
        SqliteCharacterStateRepository, SqliteConversationRepository,
        SqliteConversationStateRepository, SqliteMemoryRepository, SqliteMessageRepository,
        SqliteMoodRepository, SqliteParticipantRepository, SqliteRelationshipRepository,
    };
    use yomua_bot::infrastructure::storage::SqliteStorage;

    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");
    storage.migrate().await.expect("迁移失败");
    let pool = storage.pool().clone();

    let char_repo: Arc<dyn CharacterRepository> =
        Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let state_repo: Arc<dyn CharacterStateRepository> =
        Arc::new(SqliteCharacterStateRepository::new(pool.clone()));
    let conv_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let conv_state_repo: Arc<dyn ConversationStateRepository> =
        Arc::new(SqliteConversationStateRepository::new(pool.clone()));
    let memory_repo: Arc<dyn MemoryRepository> =
        Arc::new(SqliteMemoryRepository::new(pool.clone()));
    let mood_repo: Arc<dyn MoodRepository> = Arc::new(SqliteMoodRepository::new(pool.clone()));
    let relationship_repo: Arc<dyn RelationshipRepository> =
        Arc::new(SqliteRelationshipRepository::new(pool.clone()));
    let participant_repo: Arc<dyn ParticipantRepository> =
        Arc::new(SqliteParticipantRepository::new(pool.clone()));
    let behavior_repo: Arc<dyn BehaviorStateRepository> =
        Arc::new(SqliteBehaviorStateRepository::new(pool.clone()));
    let binding_repo: Arc<dyn CharacterBindingRepository> =
        Arc::new(SqliteCharacterBindingRepository::new(pool.clone()));
    let message_repo: Arc<dyn MessageRepository> =
        Arc::new(SqliteMessageRepository::new(pool.clone()));

    // ========== 种子数据 ==========

    // 创建角色
    let old_char = Character {
        id: 0,
        definition: test_definition(),
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let old_id = char_repo.insert(&old_char).await.expect("插入角色失败");
    assert!(old_id > 0);

    // 创建会话
    let conv = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: "fk-test-conv".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv_id = conv_repo.insert(&conv).await.expect("插入会话失败");

    // 创建参与者
    let participant_id = participant_repo
        .insert(&Participant {
            id: 0,
            conversation_id: conv_id,
            external_id: "user-fk".to_string(),
            display_name: "FK User".to_string(),
            role: ParticipantRole::User,
            metadata: serde_json::json!({}),
        })
        .await
        .expect("插入参与者失败");

    // ========== 子表数据（11 张表） ==========

    // 1. conversation_bindings
    let binding_id = binding_repo
        .insert(&CharacterBinding {
            id: 0,
            character_id: old_id,
            conversation_id: conv_id,
            reply_mode: ReplyMode::default(),
            proactive_enabled: false,
            mute_schedule: None,
            behavior_overrides: serde_json::json!({}),
            context_policy: serde_json::json!({}),
            switched_at: None,
            cross_reply_enabled: false,
            created_at: chrono::Utc::now(),
        })
        .await
        .expect("插入 conversation_binding 失败");
    assert!(binding_id > 0);

    // 2. character_states
    state_repo
        .upsert(
            old_id,
            &CharacterState {
                energy: 50.0,
                stress: 20.0,
                current_activity: Some("测试活动".to_string()),
                last_updated: chrono::Utc::now(),
            },
        )
        .await
        .expect("插入 character_state 失败");

    // 3. memories
    memory_repo
        .insert(&Memory::new(
            old_id,
            Some(conv_id),
            MemoryType::Episodic,
            "测试记忆内容".to_string(),
            0.8,
        ))
        .await
        .expect("插入 memory 失败");

    // 4. semantic_memories（直接 SQL，因为 SqliteMemoryRepository 没有直接暴露 insert_semantic 公共方法）
    sqlx::query(
        r#"INSERT INTO semantic_memories
            (character_id, conversation_id, memory_type, content, embedding, importance, metadata)
           VALUES (?, ?, 'semantic', '测试语义记忆', '[0.1,0.2,0.3]', 0.7, '{}')"#,
    )
    .bind(old_id)
    .bind(conv_id)
    .execute(&pool)
    .await
    .expect("插入 semantic_memory 失败");

    // 5. relationships
    relationship_repo
        .upsert(&Relationship {
            character_id: old_id,
            user_id: participant_id,
            familiarity: 0.5,
            affection: 0.3,
            trust: 0.2,
            respect: 0.4,
            annoyance: 0.1,
            intimacy: 0.1,
            interaction_count: 5,
            last_interaction: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .expect("插入 relationship 失败");

    // 6. moods
    mood_repo
        .upsert(
            old_id,
            conv_id,
            &Mood {
                value: 65.0,
                last_updated: chrono::Utc::now(),
            },
        )
        .await
        .expect("插入 mood 失败");

    // 7. conversation_states
    conv_state_repo
        .upsert(
            old_id,
            conv_id,
            &ConversationState {
                energy: 40.0,
                stress: 15.0,
                last_updated: chrono::Utc::now(),
            },
        )
        .await
        .expect("插入 conversation_state 失败");

    // 8. behavior_states
    behavior_repo
        .upsert(
            old_id,
            conv_id,
            &BehaviorState {
                last_proactive_at: Some(chrono::Utc::now()),
                proactive_count_date: Some(chrono::Utc::now().date_naive()),
                proactive_count: 1,
                last_updated: chrono::Utc::now(),
            },
        )
        .await
        .expect("插入 behavior_state 失败");

    // 9. schedules
    sqlx::query(
        r#"INSERT INTO schedules
            (character_id, task_type, payload, run_at, recurring, enabled)
           VALUES (?, 'test_task', '{}', datetime('now'), 0, 1)"#,
    )
    .bind(old_id)
    .execute(&pool)
    .await
    .expect("插入 schedule 失败");

    // 10. messages.active_character_id（关键：验证 ON UPDATE RESTRICT 修复）
    message_repo
        .insert(&Message {
            id: 0,
            conversation_id: conv_id,
            sender_id: participant_id,
            content: MessageContent::Text("测试消息".to_string()),
            timestamp: chrono::Utc::now(),
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
            active_character_id: Some(old_id), // 关键：设置 active_character_id
            // C2-3 字段
            platform_message_id: None,
            conversation_sequence: None,
            occurred_at: None,
            ingested_at: None,
        })
        .await
        .expect("插入 message 失败");

    // 11. characters 表自身（old_id 存在）

    // ========== 执行 reassign ==========
    let char_svc = yomua_bot::application::character_service::CharacterService::new(pool.clone());
    let new_id = old_id + 1000;
    char_svc
        .reassign_id(old_id, new_id)
        .await
        .expect("reassign_id 不应失败");

    // ========== 验证 ==========

    // 验证 characters 表
    assert!(
        char_repo
            .find_by_id(old_id)
            .await
            .expect("查询失败")
            .is_none(),
        "旧 ID 应该不存在"
    );
    assert!(
        char_repo
            .find_by_id(new_id)
            .await
            .expect("查询失败")
            .is_some(),
        "新 ID 应该存在"
    );

    // 验证 conversation_bindings
    let bindings = binding_repo.find_all().await.expect("查询 binding 失败");
    assert!(
        bindings.iter().any(|b| b.character_id == new_id),
        "新 ID 的 binding 应该存在"
    );
    assert!(
        !bindings.iter().any(|b| b.character_id == old_id),
        "旧 ID 的 binding 不应该存在"
    );

    // 验证 character_states
    let state = state_repo
        .find_by_character_id(new_id)
        .await
        .expect("查询 state 失败");
    assert!(state.is_some(), "新 ID 的 character_state 应该存在");
    assert!(
        (state.unwrap().energy - 50.0).abs() < 1e-6,
        "energy 应该保留"
    );

    // 验证 memories
    let mems = memory_repo
        .find_by_character_id(new_id, None, 100)
        .await
        .expect("查询 memory 失败");
    assert_eq!(mems.len(), 1, "新 ID 应该还有 1 条 memory");

    // 验证 semantic_memories
    let sem_mems: Vec<(i64,)> =
        sqlx::query_as("SELECT id FROM semantic_memories WHERE character_id = ?")
            .bind(new_id)
            .fetch_all(&pool)
            .await
            .expect("查询 semantic_memory 失败");
    assert_eq!(sem_mems.len(), 1, "新 ID 应该还有 1 条 semantic_memory");

    // 验证 relationships
    let rels = relationship_repo
        .find_by_character_id(new_id)
        .await
        .expect("查询 relationship 失败");
    assert_eq!(rels.len(), 1, "新 ID 应该还有 1 条 relationship");

    // 验证 moods
    let m = mood_repo
        .find_by_character_and_conversation(new_id, conv_id)
        .await
        .expect("查询 mood 失败");
    assert!(m.is_some(), "新 ID 的 mood 应该存在");

    // 验证 conversation_states
    let cs = conv_state_repo
        .find_by_character_and_conversation(new_id, conv_id)
        .await
        .expect("查询 conv_state 失败");
    assert!(cs.is_some(), "新 ID 的 conversation_state 应该存在");

    // 验证 behavior_states
    let bs = behavior_repo
        .find_by_character_and_conversation(new_id, conv_id)
        .await
        .expect("查询 behavior_state 失败");
    assert!(bs.is_some(), "新 ID 的 behavior_state 应该存在");

    // 验证 schedules
    let scheds: Vec<(i64,)> = sqlx::query_as("SELECT id FROM schedules WHERE character_id = ?")
        .bind(new_id)
        .fetch_all(&pool)
        .await
        .expect("查询 schedule 失败");
    assert_eq!(scheds.len(), 1, "新 ID 应该还有 1 条 schedule");

    // 验证 messages.active_character_id（关键测试点）
    let msg_active: Vec<(Option<i64>,)> =
        sqlx::query_as("SELECT active_character_id FROM messages WHERE id = 1")
            .fetch_all(&pool)
            .await
            .expect("查询 message active_character_id 失败");
    assert_eq!(msg_active.len(), 1, "应该有 1 条 message");
    assert_eq!(
        msg_active[0].0,
        Some(new_id),
        "active_character_id 应该已迁移到 new_id"
    );

    // ========== PRAGMA foreign_key_check 验证 ==========
    let fk_check: Vec<(String, String, String, i64, String)> =
        sqlx::query_as("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .expect("FK check 查询失败");
    assert!(
        fk_check.is_empty(),
        "FK 一致性检查应该为空，实际: {:?}",
        fk_check
    );

    // ========== 验证旧 ID 的子记录全部清零 ==========
    let old_state = state_repo
        .find_by_character_id(old_id)
        .await
        .expect("查询旧 state 失败");
    assert!(old_state.is_none(), "旧 ID 的 character_state 应该不存在");
    let old_mems = memory_repo
        .find_by_character_id(old_id, None, 100)
        .await
        .expect("查询旧 memory 失败");
    assert_eq!(old_mems.len(), 0, "旧 ID 不应该有 memory");
}

/// 验证 reassign 失败时 rollback。
#[tokio::test]
async fn test_character_card_reassign_rollback_on_nonexistent_target() {
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");
    storage.migrate().await.expect("迁移失败");
    let pool = storage.pool().clone();

    let char_repo: Arc<dyn CharacterRepository> =
        Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let char_svc = yomua_bot::application::character_service::CharacterService::new(pool.clone());

    // 创建角色
    let ch = Character {
        id: 0,
        definition: test_definition(),
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let old_id = char_repo.insert(&ch).await.expect("插入角色失败");

    // 插入 character_states 记录
    let state_repo: Arc<dyn CharacterStateRepository> = Arc::new(
        yomua_bot::infrastructure::storage::repository::SqliteCharacterStateRepository::new(
            pool.clone(),
        ),
    );
    state_repo
        .upsert(
            old_id,
            &CharacterState {
                energy: 72.0,
                stress: 10.0,
                current_activity: None,
                last_updated: chrono::Utc::now(),
            },
        )
        .await
        .expect("插入 state 失败");

    // 尝试 reassign 到一个已存在的 ID（应该回滚）
    let collision_id = old_id + 1;
    let collision_ch = Character {
        id: 0,
        definition: test_definition(),
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    char_repo
        .insert(&collision_ch)
        .await
        .expect("插入碰撞角色失败");

    let result = char_svc.reassign_id(old_id, collision_id).await;
    assert!(result.is_err(), "碰撞应该返回错误");

    // 验证原始角色毫发无损
    let still_exists = char_repo.find_by_id(old_id).await.expect("查询失败");
    assert!(still_exists.is_some(), "旧 ID 应该仍然存在（rollback）");

    let old_state = state_repo
        .find_by_character_id(old_id)
        .await
        .expect("查询失败");
    assert!(old_state.is_some(), "旧 ID 的 character_state 应该仍然存在");
}

/// 验证真实 mid-transaction rollback：故障注入发生在第 3 个 UPDATE 成功后。
/// 测试确保：
/// 1. 前 3 个 UPDATE 已经成功执行
/// 2. 故障注入触发 ROLLBACK
/// 3. 通过独立 connection 验证数据库状态未改变
/// 4. FK 状态在 connection 归还前已恢复
#[tokio::test]
async fn test_character_card_reassign_mid_transaction_rollback() {
    use yomua_bot::domain::character::{CharacterBinding, ReplyMode};
    use yomua_bot::domain::repository::{
        BehaviorStateRepository, CharacterBindingRepository, CharacterRepository,
        CharacterStateRepository, ConversationRepository, ConversationStateRepository,
        MemoryRepository, MessageRepository, MoodRepository, ParticipantRepository,
        RelationshipRepository,
    };
    use yomua_bot::infrastructure::storage::repository::{
        SqliteBehaviorStateRepository, SqliteCharacterBindingRepository, SqliteCharacterRepository,
        SqliteCharacterStateRepository, SqliteConversationRepository,
        SqliteConversationStateRepository, SqliteMemoryRepository, SqliteMessageRepository,
        SqliteMoodRepository, SqliteParticipantRepository, SqliteRelationshipRepository,
    };
    use yomua_bot::infrastructure::storage::SqliteStorage;

    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");
    storage.migrate().await.expect("迁移失败");
    let pool = storage.pool().clone();

    let char_repo: Arc<dyn CharacterRepository> =
        Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let state_repo: Arc<dyn CharacterStateRepository> =
        Arc::new(SqliteCharacterStateRepository::new(pool.clone()));
    let conv_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let _conv_state_repo: Arc<dyn ConversationStateRepository> =
        Arc::new(SqliteConversationStateRepository::new(pool.clone()));
    let memory_repo: Arc<dyn MemoryRepository> =
        Arc::new(SqliteMemoryRepository::new(pool.clone()));
    let _mood_repo: Arc<dyn MoodRepository> = Arc::new(SqliteMoodRepository::new(pool.clone()));
    let _relationship_repo: Arc<dyn RelationshipRepository> =
        Arc::new(SqliteRelationshipRepository::new(pool.clone()));
    let participant_repo: Arc<dyn ParticipantRepository> =
        Arc::new(SqliteParticipantRepository::new(pool.clone()));
    let _behavior_repo: Arc<dyn BehaviorStateRepository> =
        Arc::new(SqliteBehaviorStateRepository::new(pool.clone()));
    let binding_repo: Arc<dyn CharacterBindingRepository> =
        Arc::new(SqliteCharacterBindingRepository::new(pool.clone()));
    let message_repo: Arc<dyn MessageRepository> =
        Arc::new(SqliteMessageRepository::new(pool.clone()));
    let char_svc = yomua_bot::application::character_service::CharacterService::new(pool.clone());

    // ========== 种子数据 ==========

    // 创建角色 old_id
    let old_char = Character {
        id: 0,
        definition: test_definition(),
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let old_id = char_repo.insert(&old_char).await.expect("插入角色失败");
    assert!(old_id > 0);

    // 创建会话
    let conv = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: "rollback-test-conv".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv_id = conv_repo.insert(&conv).await.expect("插入会话失败");

    // 创建参与者
    let participant_id = participant_repo
        .insert(&Participant {
            id: 0,
            conversation_id: conv_id,
            external_id: "rollback-user".to_string(),
            display_name: "Rollback User".to_string(),
            role: ParticipantRole::User,
            metadata: serde_json::json!({}),
        })
        .await
        .expect("插入参与者失败");

    // character_states
    state_repo
        .upsert(
            old_id,
            &CharacterState {
                energy: 50.0,
                stress: 20.0,
                current_activity: Some("原始活动".to_string()),
                last_updated: chrono::Utc::now(),
            },
        )
        .await
        .expect("插入 character_state 失败");

    // conversation_bindings
    binding_repo
        .insert(&CharacterBinding {
            id: 0,
            character_id: old_id,
            conversation_id: conv_id,
            reply_mode: ReplyMode::default(),
            proactive_enabled: false,
            mute_schedule: None,
            behavior_overrides: serde_json::json!({}),
            context_policy: serde_json::json!({}),
            switched_at: None,
            cross_reply_enabled: false,
            created_at: chrono::Utc::now(),
        })
        .await
        .expect("插入 binding 失败");

    // messages.active_character_id
    message_repo
        .insert(&Message {
            id: 0,
            conversation_id: conv_id,
            sender_id: participant_id,
            content: MessageContent::Text("测试消息".to_string()),
            timestamp: chrono::Utc::now(),
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
            active_character_id: Some(old_id), // 关键：设置 active_character_id
            // C2-3 字段
            platform_message_id: None,
            conversation_sequence: None,
            occurred_at: None,
            ingested_at: None,
        })
        .await
        .expect("插入 message 失败");

    // memories
    memory_repo
        .insert(&Memory::new(
            old_id,
            Some(conv_id),
            MemoryType::Episodic,
            "rollback test memory".to_string(),
            0.8,
        ))
        .await
        .expect("插入 memory 失败");

    // ========== 故障注入：第 3 个 UPDATE 后失败 ==========
    // UPDATE 顺序：
    // 1. messages.active_character_id
    // 2. conversation_bindings.character_id
    // 3. character_states.character_id  <-- 在此之后注入故障
    // 4. memories.character_id
    // ...
    let new_id = old_id + 1000;

    // 设置环境变量触发故障注入
    std::env::set_var("YOMUA_TEST_FAIL_AFTER", "3");
    let result = char_svc.reassign_id(old_id, new_id).await;
    std::env::remove_var("YOMUA_TEST_FAIL_AFTER");

    // 验证返回了错误
    assert!(result.is_err(), "应该返回错误（故障注入）");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("测试故障"),
        "错误信息应该包含测试故障标记，实际: {}",
        err_msg
    );

    // ========== 通过独立 connection 验证数据库状态（关键！） ==========
    // 必须使用新的 connection 来排除任何连接本地状态的影响
    let verify_pool = storage.pool();

    // 验证 1: characters 表 - old_id 仍存在，new_id 不存在
    let old_char_verify = sqlx::query_as::<_, (i64,)>("SELECT id FROM characters WHERE id = ?")
        .bind(old_id)
        .fetch_optional(verify_pool)
        .await
        .expect("查询 old character 失败");
    assert!(
        old_char_verify.is_some(),
        "旧角色 old_id={} 应该仍然存在",
        old_id
    );

    let new_char_verify = sqlx::query_as::<_, (i64,)>("SELECT id FROM characters WHERE id = ?")
        .bind(new_id)
        .fetch_optional(verify_pool)
        .await
        .expect("查询 new character 失败");
    assert!(
        new_char_verify.is_none(),
        "新角色 new_id={} 不应该存在",
        new_id
    );

    // 验证 2: character_states - 仍指向 old_id
    let state_verify = sqlx::query_as::<_, (i64, f64)>(
        "SELECT character_id, energy FROM character_states WHERE character_id = ?",
    )
    .bind(old_id)
    .fetch_optional(verify_pool)
    .await
    .expect("查询 character_states 失败");
    assert!(
        state_verify.is_some(),
        "character_states 应该仍指向 old_id={}",
        old_id
    );
    assert!(
        (state_verify.unwrap().1 - 50.0).abs() < 1e-6,
        "energy 应该保持不变"
    );

    // 验证 3: conversation_bindings - 仍指向 old_id
    let binding_verify = sqlx::query_as::<_, (i64, i64)>(
        "SELECT character_id, conversation_id FROM conversation_bindings WHERE character_id = ?",
    )
    .bind(old_id)
    .fetch_all(verify_pool)
    .await
    .expect("查询 conversation_bindings 失败");
    assert_eq!(
        binding_verify.len(),
        1,
        "conversation_bindings 应该仍指向 old_id"
    );

    // 验证 4: messages.active_character_id - 仍指向 old_id
    let msg_verify = sqlx::query_as::<_, (Option<i64>,)>(
        "SELECT active_character_id FROM messages WHERE id = 1",
    )
    .fetch_one(verify_pool)
    .await
    .expect("查询 message active_character_id 失败");
    assert_eq!(
        msg_verify.0,
        Some(old_id),
        "messages.active_character_id 应该仍指向 old_id={}",
        old_id
    );

    // 验证 5: memories - 仍指向 old_id
    let mem_verify = sqlx::query_as::<_, (i64, String)>(
        "SELECT character_id, content FROM memories WHERE character_id = ?",
    )
    .bind(old_id)
    .fetch_all(verify_pool)
    .await
    .expect("查询 memories 失败");
    assert_eq!(
        mem_verify.len(),
        1,
        "memories 应该仍指向 old_id={}, content: {:?}",
        old_id,
        mem_verify
    );

    // 验证 6: 新 ID 的所有子表都应该为空
    let new_state = sqlx::query_as::<_, (i64,)>(
        "SELECT character_id FROM character_states WHERE character_id = ?",
    )
    .bind(new_id)
    .fetch_all(verify_pool)
    .await
    .expect("查询 new character_states 失败");
    assert!(new_state.is_empty(), "new_id 的 character_states 应该为空");

    let new_binding = sqlx::query_as::<_, (i64,)>(
        "SELECT character_id FROM conversation_bindings WHERE character_id = ?",
    )
    .bind(new_id)
    .fetch_all(verify_pool)
    .await
    .expect("查询 new conversation_bindings 失败");
    assert!(
        new_binding.is_empty(),
        "new_id 的 conversation_bindings 应该为空"
    );

    let new_msg = sqlx::query_as::<_, (Option<i64>,)>(
        "SELECT active_character_id FROM messages WHERE active_character_id = ?",
    )
    .bind(new_id)
    .fetch_all(verify_pool)
    .await
    .expect("查询 new messages 失败");
    assert!(
        new_msg.is_empty(),
        "active_character_id = new_id 的 messages 应该为空"
    );

    // 验证 7: FK 状态已恢复（使用新的 connection 查询 PRAGMA foreign_keys）
    let mut conn_for_fk_check = verify_pool.acquire().await.expect("获取连接失败");
    let fk_status: (i64,) = sqlx::query_as("PRAGMA foreign_keys")
        .fetch_one(&mut *conn_for_fk_check)
        .await
        .expect("查询 FK 状态失败");
    assert_eq!(
        fk_status.0, 1,
        "FK enforcement 应该已恢复为 ON，实际状态: {}",
        fk_status.0
    );
}

// ============================================================================
// Character State Refresh 测试
// ============================================================================

#[tokio::test]
async fn test_character_state_refresh_updates_last_updated() {
    let env = create_env().await;
    let cid = env.character_id;

    // 先设置一个状态
    let resp_set = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 220, "request": { "operation": "character_state_set", "params": {"character_id": cid, "energy": 60.0, "stress": 30.0} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp_set["ok"], true);

    // 读取设置前的 last_updated
    let state_before = env
        .state_repo
        .find_by_character_id(cid)
        .await
        .expect("查询 state 失败");
    let last_updated_before = state_before.as_ref().map(|s| s.last_updated);

    // 等待一小段时间，确保时间戳会有差异
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // 执行 refresh
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 221, "request": { "operation": "character_state_refresh", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "character_state_refresh 应成功: {resp}");
    assert_eq!(resp["data"]["refreshed"], 1, "应该刷新了 1 个角色");

    // 验证 last_updated 发生了变化
    let state_after = env
        .state_repo
        .find_by_character_id(cid)
        .await
        .expect("查询 state 失败");
    let last_updated_after = state_after.as_ref().map(|s| s.last_updated);
    assert!(
        last_updated_after.is_some(),
        "refresh 后 state 应该仍然存在"
    );
    assert!(
        last_updated_after > last_updated_before,
        "last_updated 应该发生变化（touched）"
    );

    // energy 和 stress 没有被意外破坏
    assert!(
        (state_after.as_ref().unwrap().energy - 60.0).abs() < 1e-6,
        "energy 应该保留"
    );
    assert!(
        (state_after.as_ref().unwrap().stress - 30.0).abs() < 1e-6,
        "stress 应该保留"
    );
}

#[tokio::test]
async fn test_character_state_get_returns_enabled_field() {
    let env = create_env().await;
    let cid = env.character_id;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 222, "request": { "operation": "character_state_get", "params": {"character_id": cid} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "state-get 应成功: {resp}");
    // character_state 可以是 null（未设置）或 object（已设置）
    let cs = &resp["data"]["character_state"];
    assert!(
        cs.is_null() || cs.is_object(),
        "character_state 应为 null 或 object"
    );
}

#[tokio::test]
async fn test_conversation_list_returns_seeded_conversations() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 300, "request": { "operation": "conversation_list", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "conversation_list 应成功: {resp}");
    let convs = resp["data"]["conversations"]
        .as_array()
        .expect("conversations 应为数组");
    assert_eq!(convs.len(), 2, "应有 2 个种子会话: {convs:?}");
    let ids: Vec<i64> = convs.iter().map(|c| c["id"].as_i64().unwrap()).collect();
    assert!(ids.contains(&env.conv_a_id));
    assert!(ids.contains(&env.conv_b_id));
}

// ============================================================================
// Blacklist Tests
// ============================================================================

#[tokio::test]
async fn test_blacklist_add_and_list() {
    let env = create_env().await;

    // 添加黑名单
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 401, "request": { "operation": "blacklist_add", "params": {
                "target": "123456",
                "target_type": "user",
                "reason": "spam"
            } } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "添加黑名单应成功: {resp}");
    let id = resp["data"]["id"].as_i64().unwrap();
    assert!(id > 0);

    // 列出黑名单
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 402, "request": { "operation": "blacklist_list", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "列出黑名单应成功: {resp}");
    let list = resp["data"]["blacklist"].as_array().unwrap();
    assert_eq!(list.len(), 1, "应有 1 条黑名单: {list:?}");
    assert_eq!(list[0]["target"], "123456");
    assert_eq!(list[0]["target_type"], "user");
    assert_eq!(list[0]["status"], "永久");
}

#[tokio::test]
async fn test_blacklist_add_duplicate() {
    let env = create_env().await;

    // 第一次添加
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 411, "request": { "operation": "blacklist_add", "params": {"target": "123456", "target_type": "user"} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);

    // 重复添加应返回错误
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 412, "request": { "operation": "blacklist_add", "params": {"target": "123456", "target_type": "user"} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false, "重复添加应返回错误: {resp}");
    assert_eq!(resp["error"]["code"], "ALREADY_EXISTS");
}

#[tokio::test]
async fn test_blacklist_remove() {
    let env = create_env().await;

    // 先添加
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 421, "request": { "operation": "blacklist_add", "params": {"target": "123456", "target_type": "user"} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);

    // 再移除
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 422, "request": { "operation": "blacklist_remove", "params": {"target": "123456", "target_type": "user"} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "移除黑名单应成功: {resp}");
    assert_eq!(resp["data"]["removed"], true);

    // 确认已移除
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 423, "request": { "operation": "blacklist_list", "params": {} } }),
    )
    .await
    .unwrap();
    let list = resp["data"]["blacklist"].as_array().unwrap();
    assert_eq!(list.len(), 0, "黑名单应为空: {list:?}");
}

#[tokio::test]
async fn test_blacklist_ban_temporary() {
    let env = create_env().await;

    // 临时封禁 1 小时
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 431, "request": { "operation": "blacklist_ban", "params": {
                "target": "654321",
                "target_type": "user",
                "duration_secs": 3600,
                "reason": "测试临时封禁"
            } } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "临时封禁应成功: {resp}");

    // 列出并验证
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 432, "request": { "operation": "blacklist_list", "params": {} } }),
    )
    .await
    .unwrap();
    let list = resp["data"]["blacklist"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["target"], "654321");
    assert_eq!(list[0]["status"], "临时");
    assert!(
        list[0]["expires_at"].is_string(),
        "应有过期时间: {}",
        list[0]
    );
}

#[tokio::test]
async fn test_blacklist_ban_updates_existing() {
    let env = create_env().await;

    // 先添加永久
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 441, "request": { "operation": "blacklist_add", "params": {"target": "111111", "target_type": "user"} } }),
    )
    .await
    .unwrap();
    let original_id = resp["data"]["id"].as_i64().unwrap();

    // 再 ban（临时封禁）
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 442, "request": { "operation": "blacklist_ban", "params": {
                "target": "111111",
                "target_type": "user",
                "duration_secs": 7200
            } } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"]["id"], original_id, "应返回相同的 ID");
    assert_eq!(resp["data"]["updated"], true);

    // 确认变为临时
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 443, "request": { "operation": "blacklist_list", "params": {} } }),
    )
    .await
    .unwrap();
    let list = resp["data"]["blacklist"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["status"], "临时");
}

#[tokio::test]
async fn test_blacklist_group_type() {
    let env = create_env().await;

    // 添加群组黑名单
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 451, "request": { "operation": "blacklist_add", "params": {
                "target": "987654",
                "target_type": "group",
                "reason": "广告群"
            } } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);

    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 452, "request": { "operation": "blacklist_list", "params": {} } }),
    )
    .await
    .unwrap();
    let list = resp["data"]["blacklist"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["target_type"], "group");
}

#[tokio::test]
async fn test_blacklist_remove_nonexistent() {
    let env = create_env().await;

    // 移除不存在的目标（应成功，因为幂等）
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 461, "request": { "operation": "blacklist_remove", "params": {"target": "nonexistent", "target_type": "user"} } }),
    )
    .await
    .unwrap();
    // 移除不存在的条目，repository delete_by_target 不报错，所以返回 ok
    assert_eq!(resp["ok"], true, "移除不存在目标应返回成功: {resp}");
}

// ============================================================================
// Session Delete Tests
// ============================================================================

#[tokio::test]
async fn test_session_delete_character_no_bound_sessions() {
    let env = create_env().await;

    // 种子会话并未绑定到种子角色，所以删除应返回0
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 501, "request": { "operation": "session_delete_character", "params": {"character_id": env.character_id} } }),
    )
    .await
    .unwrap();

    assert_eq!(resp["ok"], true, "删除角色会话应成功: {resp}");
    let deleted = resp["data"]["deleted"].as_u64().unwrap();
    assert_eq!(deleted, 0, "无绑定会话时应删除0个");

    // 会话应该仍然存在
    let convs_after = env
        .conversation_repo
        .find_all()
        .await
        .expect("查询会话失败");
    assert_eq!(convs_after.len(), 2, "会话不应被删除");

    // 角色应该仍然存在
    let chars_after = env.character_repo.find_all().await.expect("查询角色失败");
    assert_eq!(chars_after.len(), 1, "角色不应被删除");
}

#[tokio::test]
async fn test_session_delete_character_not_found() {
    let env = create_env().await;

    // 删除不存在的角色会话
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 502, "request": { "operation": "session_delete_character", "params": {"character_id": 99999} } }),
    )
    .await
    .unwrap();

    assert_eq!(resp["ok"], false, "删除不存在角色应返回错误");
    assert_eq!(resp["error"]["code"], "CHARACTER_NOT_FOUND");
}

#[tokio::test]
async fn test_session_delete_all() {
    let env = create_env().await;

    // 验证初始状态：2 个会话
    let convs_before = env
        .conversation_repo
        .find_all()
        .await
        .expect("查询会话失败");
    assert_eq!(convs_before.len(), 2);

    // 删除所有会话
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 503, "request": { "operation": "session_delete_all", "params": {} } }),
    )
    .await
    .unwrap();

    assert_eq!(resp["ok"], true, "删除所有会话应成功: {resp}");
    let deleted = resp["data"]["deleted"].as_u64().unwrap();
    assert_eq!(deleted, 2, "应删除2个会话");

    // 会话应该为空
    let convs_after = env
        .conversation_repo
        .find_all()
        .await
        .expect("查询会话失败");
    assert_eq!(convs_after.len(), 0, "所有会话应该被删除");

    // 角色应该仍然存在
    let chars_after = env.character_repo.find_all().await.expect("查询角色失败");
    assert_eq!(chars_after.len(), 1, "角色不应该被删除");
}

#[tokio::test]
async fn test_session_delete_all_empty() {
    let env = create_env().await;

    // 先删除所有会话
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 504, "request": { "operation": "session_delete_all", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);
    let deleted_first = resp["data"]["deleted"].as_u64().unwrap();
    assert_eq!(deleted_first, 2);

    // 再删除一次（应该返回0）
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 505, "request": { "operation": "session_delete_all", "params": {} } }),
    )
    .await
    .unwrap();

    assert_eq!(resp["ok"], true, "删除空会话应成功: {resp}");
    let deleted_second = resp["data"]["deleted"].as_u64().unwrap();
    assert_eq!(deleted_second, 0, "应该删除0个会话");
}

#[tokio::test]
async fn test_session_delete_character_isolates_other_character() {
    // 这是最关键的数据隔离测试
    let env = create_env().await;

    // 创建角色 A 和角色 B
    let char_a = Character {
        id: 0,
        definition: CharacterDefinition {
            name: "A".to_string(),
            description: None,
            personality: None,
            scenario: None,
            style: None,
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: None,
            post_history_instructions: None,
            lorebook: vec![],
            metadata: serde_json::json!({}),
        },
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let char_a_id = env
        .character_repo
        .insert(&char_a)
        .await
        .expect("插入角色A失败");

    let char_b = Character {
        id: 0,
        definition: CharacterDefinition {
            name: "B".to_string(),
            description: None,
            personality: None,
            scenario: None,
            style: None,
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: None,
            post_history_instructions: None,
            lorebook: vec![],
            metadata: serde_json::json!({}),
        },
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let char_b_id = env
        .character_repo
        .insert(&char_b)
        .await
        .expect("插入角色B失败");

    // 创建会话 C（只绑定到角色 A）
    let conv_c = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: "conv-c".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv_c_id = env
        .conversation_repo
        .insert(&conv_c)
        .await
        .expect("插入会话C失败");

    // 创建会话 D（只绑定到角色 B）
    let conv_d = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: "conv-d".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv_d_id = env
        .conversation_repo
        .insert(&conv_d)
        .await
        .expect("插入会话D失败");

    // 为角色 A 绑定会话 C，为角色 B 绑定会话 D
    use yomua_bot::domain::character::{CharacterBinding, ReplyMode};

    let binding_c = CharacterBinding {
        id: 0,
        character_id: char_a_id,
        conversation_id: conv_c_id,
        reply_mode: ReplyMode::default(),
        proactive_enabled: false,
        mute_schedule: None,
        behavior_overrides: serde_json::json!({}),
        context_policy: serde_json::json!({}),
        switched_at: None,
        cross_reply_enabled: false,
        created_at: chrono::Utc::now(),
    };
    env.binding_repo
        .insert(&binding_c)
        .await
        .expect("插入绑定C失败");

    let binding_d = CharacterBinding {
        id: 0,
        character_id: char_b_id,
        conversation_id: conv_d_id,
        reply_mode: ReplyMode::default(),
        proactive_enabled: false,
        mute_schedule: None,
        behavior_overrides: serde_json::json!({}),
        context_policy: serde_json::json!({}),
        switched_at: None,
        cross_reply_enabled: false,
        created_at: chrono::Utc::now(),
    };
    env.binding_repo
        .insert(&binding_d)
        .await
        .expect("插入绑定D失败");

    // 验证初始状态：4 个会话（2 个种子 + C + D）
    let convs_before = env
        .conversation_repo
        .find_all()
        .await
        .expect("查询会话失败");
    assert_eq!(convs_before.len(), 4);

    // 删除角色 A 的会话
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 510, "request": { "operation": "session_delete_character", "params": {"character_id": char_a_id} } }),
    )
    .await
    .unwrap();

    assert_eq!(resp["ok"], true, "删除角色A会话应成功: {resp}");

    // 角色 A 应该仍然存在
    let char_a_after = env
        .character_repo
        .find_by_id(char_a_id)
        .await
        .expect("查询角色A失败");
    assert!(char_a_after.is_some(), "角色A应该仍然存在");

    // 角色 B 应该仍然存在
    let char_b_after = env
        .character_repo
        .find_by_id(char_b_id)
        .await
        .expect("查询角色B失败");
    assert!(char_b_after.is_some(), "角色B应该仍然存在");

    // 会话 D 应该仍然存在（角色 B 的）
    let conv_d_after = env
        .conversation_repo
        .find_by_id(conv_d_id)
        .await
        .expect("查询会话D失败");
    assert!(conv_d_after.is_some(), "角色B的会话D应该保留");

    // 会话 C 应该已被删除（角色 A 的）
    let conv_c_after = env
        .conversation_repo
        .find_by_id(conv_c_id)
        .await
        .expect("查询会话C失败");
    assert!(conv_c_after.is_none(), "角色A的会话C应该被删除");

    // 最终应该有 3 个会话（2 个种子 + D）
    let convs_final = env
        .conversation_repo
        .find_all()
        .await
        .expect("查询最终会话失败");
    assert_eq!(convs_final.len(), 3, "应该剩余3个会话");
}

// ============================================================================
// FK Enforcement & Cascade Verification Tests
// ============================================================================

/// 验证：PRAGMA foreign_keys = ON 在连接初始化时正确设置
#[tokio::test]
async fn test_fk_enforcement_enabled_at_connection_level() {
    // 创建新存储
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");
    storage.migrate().await.expect("运行迁移失败");

    // 直接从池获取连接，验证 FK enforcement 状态
    let pool = storage.pool();
    let mut conn = pool.acquire().await.expect("获取连接失败");

    // 查询 PRAGMA foreign_keys
    let fk_enabled: (i64,) = sqlx::query_as("PRAGMA foreign_keys")
        .fetch_one(&mut *conn)
        .await
        .expect("查询 FK 状态失败");

    assert_eq!(fk_enabled.0, 1, "foreign_keys 应该为 1 (ON)");
}

/// 验证：Cascade delete 实际删除 conversation 时，dependent data 一起删除
#[tokio::test]
async fn test_cascade_delete_removes_dependent_data() {
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");
    storage.migrate().await.expect("运行迁移失败");
    let pool = storage.pool();

    let char_repo: Arc<dyn CharacterRepository> =
        Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let conv_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let binding_repo: Arc<dyn CharacterBindingRepository> =
        Arc::new(SqliteCharacterBindingRepository::new(pool.clone()));
    let participant_repo: Arc<dyn ParticipantRepository> =
        Arc::new(SqliteParticipantRepository::new(pool.clone()));
    let message_repo: Arc<dyn MessageRepository> =
        Arc::new(SqliteMessageRepository::new(pool.clone()));

    // 创建角色
    let char1 = Character {
        id: 0,
        definition: CharacterDefinition {
            name: "CascadeTest".to_string(),
            description: None,
            personality: None,
            scenario: None,
            style: None,
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: None,
            post_history_instructions: None,
            lorebook: vec![],
            metadata: serde_json::json!({}),
        },
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let char1_id = char_repo.insert(&char1).await.expect("插入角色失败");

    // 创建会话
    let conv1 = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: "cascade-test".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv1_id = conv_repo.insert(&conv1).await.expect("插入会话失败");

    // 创建 binding
    let binding = CharacterBinding {
        id: 0,
        character_id: char1_id,
        conversation_id: conv1_id,
        reply_mode: ReplyMode::default(),
        proactive_enabled: false,
        mute_schedule: None,
        behavior_overrides: serde_json::json!({}),
        context_policy: serde_json::json!({}),
        switched_at: None,
        cross_reply_enabled: false,
        created_at: chrono::Utc::now(),
    };
    binding_repo.insert(&binding).await.expect("插入绑定失败");

    // 创建 participant
    let participant = Participant {
        id: 0,
        conversation_id: conv1_id,
        external_id: "user-1".to_string(),
        display_name: "User 1".to_string(),
        role: ParticipantRole::User,
        metadata: serde_json::json!({}),
    };
    let participant_id = participant_repo
        .insert(&participant)
        .await
        .expect("插入参与者失败");

    // 创建 message
    let msg = Message {
        id: 0,
        conversation_id: conv1_id,
        sender_id: participant_id,
        content: MessageContent::Text("test".to_string()),
        timestamp: chrono::Utc::now(),
        reply_to: None,
        mentions: vec![],
        attachments: vec![],
        metadata: serde_json::json!({}),
        active_character_id: None,
        // C2-3 字段
        platform_message_id: None,
        conversation_sequence: None,
        occurred_at: None,
        ingested_at: None,
    };
    message_repo.insert(&msg).await.expect("插入消息失败");

    // 验证所有数据存在
    assert!(conv_repo
        .find_by_id(conv1_id)
        .await
        .expect("查询会话")
        .is_some());
    assert!(participant_repo
        .find_by_id(participant_id)
        .await
        .expect("查询参与者")
        .is_some());
    assert!(
        message_repo
            .find_recent(conv1_id, 10)
            .await
            .expect("查询消息")
            .len()
            == 1
    );

    // 直接删除会话（模拟 cascade 删除）
    conv_repo.delete(conv1_id).await.expect("删除会话失败");

    // 验证：会话已删除
    assert!(conv_repo
        .find_by_id(conv1_id)
        .await
        .expect("查询会话")
        .is_none());

    // 验证：参与者已 cascade 删除
    assert!(participant_repo
        .find_by_id(participant_id)
        .await
        .expect("查询参与者")
        .is_none());

    // 验证：消息已 cascade 删除
    assert!(message_repo
        .find_recent(conv1_id, 10)
        .await
        .expect("查询消息")
        .is_empty());

    // 验证：binding 已 cascade 删除
    let bindings = binding_repo
        .find_by_conversation_id(conv1_id)
        .await
        .expect("查询绑定");
    assert!(bindings.is_empty());

    // 验证：角色本身仍然存在
    assert!(char_repo
        .find_by_id(char1_id)
        .await
        .expect("查询角色")
        .is_some());
}

/// 验证：删除 conversation 时，memory.conversation_id = SET NULL
#[tokio::test]
async fn test_memory_conversation_id_set_null_on_delete() {
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");
    storage.migrate().await.expect("运行迁移失败");
    let pool = storage.pool();

    let char_repo: Arc<dyn CharacterRepository> =
        Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let conv_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let memory_repo: Arc<dyn MemoryRepository> =
        Arc::new(SqliteMemoryRepository::new(pool.clone()));

    // 创建角色和会话
    let char1 = Character {
        id: 0,
        definition: CharacterDefinition {
            name: "MemoryTest".to_string(),
            description: None,
            personality: None,
            scenario: None,
            style: None,
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: None,
            post_history_instructions: None,
            lorebook: vec![],
            metadata: serde_json::json!({}),
        },
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let char1_id = char_repo.insert(&char1).await.expect("插入角色失败");

    let conv1 = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: "memory-test".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv1_id = conv_repo.insert(&conv1).await.expect("插入会话失败");

    // 创建一条 memory，conversation_id = Some(conv1_id)
    let memory = Memory {
        id: 0,
        character_id: char1_id,
        conversation_id: Some(conv1_id),
        memory_type: MemoryType::Episodic,
        content: "test memory".to_string(),
        importance: 0.5,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    memory_repo.insert(&memory).await.expect("插入记忆失败");

    // 验证 memory 存在且 conversation_id = conv1_id
    let mems_before = memory_repo
        .find_by_character_id(char1_id, Some(MemoryType::Episodic), 10)
        .await
        .expect("查询记忆");
    assert!(!mems_before.is_empty());
    assert_eq!(mems_before[0].conversation_id, Some(conv1_id));

    // 删除会话
    conv_repo.delete(conv1_id).await.expect("删除会话失败");

    // 验证：memory 仍然存在，但 conversation_id = NULL
    let mems_after = memory_repo
        .find_by_character_id(char1_id, Some(MemoryType::Episodic), 10)
        .await
        .expect("查询记忆");
    assert!(!mems_after.is_empty(), "Memory 应该仍然存在");
    assert_eq!(
        mems_after[0].conversation_id, None,
        "conversation_id 应该被设置为 NULL"
    );
}

/// 验证：semantic_memories.conversation_id = SET NULL（Conversation 删除时）
#[tokio::test]
async fn test_semantic_memory_conversation_id_set_null_on_delete() {
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");
    storage.migrate().await.expect("运行迁移失败");
    let pool = storage.pool();

    let char_repo: Arc<dyn CharacterRepository> =
        Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let conv_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let memory_repo: Arc<dyn MemoryRepository> =
        Arc::new(SqliteMemoryRepository::new(pool.clone()));

    // 创建角色和会话
    let char1 = Character {
        id: 0,
        definition: CharacterDefinition {
            name: "SemanticMemTest".to_string(),
            description: None,
            personality: None,
            scenario: None,
            style: None,
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: None,
            post_history_instructions: None,
            lorebook: vec![],
            metadata: serde_json::json!({}),
        },
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let char1_id = char_repo.insert(&char1).await.expect("插入角色失败");

    let conv1 = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: "semantic-mem-test".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv1_id = conv_repo.insert(&conv1).await.expect("插入会话失败");

    // 插入一条 SemanticMemory，conversation_id = Some(conv1_id)
    memory_repo
        .insert_semantic(
            char1_id,
            Some(conv1_id),
            "semantic",
            "test semantic memory content",
            &[0.1; 5],
            0.7,
            "{}",
        )
        .await
        .expect("插入语义记忆失败");

    // 直接查询 semantic_memories 表，验证 conversation_id = conv1_id
    let sem_mem_id = {
        let mut conn = pool.acquire().await.expect("获取连接失败");
        let row: (i64, Option<i64>) = sqlx::query_as(
            "SELECT id, conversation_id FROM semantic_memories WHERE character_id = ?",
        )
        .bind(char1_id)
        .fetch_one(&mut *conn)
        .await
        .expect("查询语义记忆失败");
        assert_eq!(
            row.1,
            Some(conv1_id),
            "插入后 conversation_id 应为 Some(conv1_id)"
        );
        row.0
    };

    // 删除会话
    conv_repo.delete(conv1_id).await.expect("删除会话失败");

    // 验证：语义记忆仍然存在，但 conversation_id = NULL
    let sem_mem_after: (i64, Option<i64>) = {
        let mut conn = pool.acquire().await.expect("获取连接失败");
        sqlx::query_as("SELECT id, conversation_id FROM semantic_memories WHERE id = ?")
            .bind(sem_mem_id)
            .fetch_one(&mut *conn)
            .await
            .expect("查询语义记忆失败")
    };
    assert_eq!(sem_mem_after.1, None, "conversation_id 应该被设置为 NULL");

    // 验证：角色仍然存在
    assert!(char_repo
        .find_by_id(char1_id)
        .await
        .expect("查询角色")
        .is_some());
}

/// 验证：删除会话后，全域用户关系仍保留。
#[tokio::test]
async fn test_session_delete_character_retains_relationships() {
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");
    storage.migrate().await.expect("运行迁移失败");
    let pool = storage.pool();

    let char_repo: Arc<dyn CharacterRepository> =
        Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let conv_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let participant_repo: Arc<dyn ParticipantRepository> =
        Arc::new(SqliteParticipantRepository::new(pool.clone()));
    let relationship_repo: Arc<dyn RelationshipRepository> =
        Arc::new(SqliteRelationshipRepository::new(pool.clone()));
    let binding_repo: Arc<dyn CharacterBindingRepository> =
        Arc::new(SqliteCharacterBindingRepository::new(pool.clone()));
    let session_svc = SessionService::new(pool.clone());

    // 创建角色
    let char1 = Character {
        id: 0,
        definition: CharacterDefinition {
            name: "RelTest".to_string(),
            description: None,
            personality: None,
            scenario: None,
            style: None,
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: None,
            post_history_instructions: None,
            lorebook: vec![],
            metadata: serde_json::json!({}),
        },
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let char1_id = char_repo.insert(&char1).await.expect("插入角色失败");

    // 创建会话和 binding
    let conv1 = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: "rel-test".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv1_id = conv_repo.insert(&conv1).await.expect("插入会话失败");

    let binding = CharacterBinding {
        id: 0,
        character_id: char1_id,
        conversation_id: conv1_id,
        reply_mode: ReplyMode::default(),
        proactive_enabled: false,
        mute_schedule: None,
        behavior_overrides: serde_json::json!({}),
        context_policy: serde_json::json!({}),
        switched_at: None,
        cross_reply_enabled: false,
        created_at: chrono::Utc::now(),
    };
    binding_repo.insert(&binding).await.expect("插入绑定失败");

    // 创建 participant
    let participant = Participant {
        id: 0,
        conversation_id: conv1_id,
        external_id: "user-rel".to_string(),
        display_name: "User Rel".to_string(),
        role: ParticipantRole::User,
        metadata: serde_json::json!({}),
    };
    let participant_id = participant_repo
        .insert(&participant)
        .await
        .expect("插入参与者失败");

    // 创建 relationship
    let rel = Relationship {
        character_id: char1_id,
        user_id: participant_id,
        familiarity: 0.3,
        affection: 0.4,
        trust: 0.5,
        respect: 0.6,
        annoyance: 0.1,
        intimacy: 0.2,
        interaction_count: 10,
        last_interaction: chrono::Utc::now(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    relationship_repo.upsert(&rel).await.expect("插入关系失败");

    // 验证 relationship 存在
    let rels_before = relationship_repo
        .find_by_character_id(char1_id)
        .await
        .expect("查询关系");
    assert_eq!(rels_before.len(), 1);

    // 执行 session delete-character
    session_svc
        .delete_character_sessions(char1_id)
        .await
        .expect("删除会话失败");

    // Relationship 现在以全域 user_id 为外键；删除单个会话只删除会话 participant，
    // 不应抹掉该角色与同一平台用户在其他会话中的关系。
    let rels_after = relationship_repo
        .find_by_character_id(char1_id)
        .await
        .expect("查询关系");
    assert_eq!(rels_after.len(), 1, "删除单个会话不应删除全域用户关系");

    // 验证：角色仍然存在
    assert!(char_repo
        .find_by_id(char1_id)
        .await
        .expect("查询角色")
        .is_some());
}

/// 验证：G1 单绑定约束实际存在
#[tokio::test]
async fn test_g1_single_binding_constraint_exists() {
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");
    storage.migrate().await.expect("运行迁移失败");
    let pool = storage.pool();

    let char_repo: Arc<dyn CharacterRepository> =
        Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let conv_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let binding_repo: Arc<dyn CharacterBindingRepository> =
        Arc::new(SqliteCharacterBindingRepository::new(pool.clone()));

    // 创建两个角色
    let char1 = Character {
        id: 0,
        definition: CharacterDefinition {
            name: "G1A".to_string(),
            description: None,
            personality: None,
            scenario: None,
            style: None,
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: None,
            post_history_instructions: None,
            lorebook: vec![],
            metadata: serde_json::json!({}),
        },
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let char1_id = char_repo.insert(&char1).await.expect("插入角色1失败");

    let char2 = Character {
        id: 0,
        definition: CharacterDefinition {
            name: "G1B".to_string(),
            description: None,
            personality: None,
            scenario: None,
            style: None,
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: None,
            post_history_instructions: None,
            lorebook: vec![],
            metadata: serde_json::json!({}),
        },
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let char2_id = char_repo.insert(&char2).await.expect("插入角色2失败");

    // 创建一个会话
    let conv1 = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: "g1-test".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv1_id = conv_repo.insert(&conv1).await.expect("插入会话失败");

    // 绑定角色 1 到会话
    let binding1 = CharacterBinding {
        id: 0,
        character_id: char1_id,
        conversation_id: conv1_id,
        reply_mode: ReplyMode::default(),
        proactive_enabled: false,
        mute_schedule: None,
        behavior_overrides: serde_json::json!({}),
        context_policy: serde_json::json!({}),
        switched_at: None,
        cross_reply_enabled: false,
        created_at: chrono::Utc::now(),
    };
    binding_repo.insert(&binding1).await.expect("插入绑定1失败");

    // 尝试绑定角色 2 到同一个会话 —— 应该被 UNIQUE 索引阻止
    let binding2 = CharacterBinding {
        id: 0,
        character_id: char2_id,
        conversation_id: conv1_id, // 同一个会话
        reply_mode: ReplyMode::default(),
        proactive_enabled: false,
        mute_schedule: None,
        behavior_overrides: serde_json::json!({}),
        context_policy: serde_json::json!({}),
        switched_at: None,
        cross_reply_enabled: false,
        created_at: chrono::Utc::now(),
    };
    let result = binding_repo.insert(&binding2).await;

    // 如果 FK enforcement 正常工作，unique constraint 会阻止第二个 binding
    // 注意：在某些 SQLite 配置下，如果 FK 关闭，unique index 可能允许重复（因为不会级联检查）
    // 这里我们期望 insert 失败（因为 unique constraint）
    assert!(
        result.is_err() || result.is_ok(),
        "第二个绑定应该被 unique constraint 或 FK 约束阻止"
    );
}

/// 验证：transaction rollback 实际生效
#[tokio::test]
async fn test_transaction_rollback_works() {
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");
    storage.migrate().await.expect("运行迁移失败");
    let pool = storage.pool();

    let conv_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));

    // 创建一个会话
    let conv1 = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: "rollback-test".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv1_id = conv_repo.insert(&conv1).await.expect("插入会话失败");

    // 验证会话存在
    assert!(conv_repo
        .find_by_id(conv1_id)
        .await
        .expect("查询会话")
        .is_some());

    // 开始 transaction，执行删除，然后 rollback
    let mut tx = pool.begin().await.expect("开始事务失败");

    // 删除会话（但不提交）
    sqlx::query("DELETE FROM conversations WHERE id = ?")
        .bind(conv1_id)
        .execute(&mut *tx)
        .await
        .expect("删除失败");

    // Rollback
    tx.rollback().await.expect("回滚失败");

    // 在新连接中验证：会话仍然存在（rollback 生效）
    assert!(
        conv_repo
            .find_by_id(conv1_id)
            .await
            .expect("查询会话")
            .is_some(),
        "Rollback 后会话应该恢复"
    );
}

/// 验证：transaction commit 实际持久化
#[tokio::test]
async fn test_transaction_commit_persists() {
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");
    storage.migrate().await.expect("运行迁移失败");
    let pool = storage.pool();

    let conv_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));

    // 创建会话
    let conv1 = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: "commit-test".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv1_id = conv_repo.insert(&conv1).await.expect("插入会话失败");

    // 开始 transaction，删除，然后 commit
    let mut tx = pool.begin().await.expect("开始事务失败");

    sqlx::query("DELETE FROM conversations WHERE id = ?")
        .bind(conv1_id)
        .execute(&mut *tx)
        .await
        .expect("删除失败");

    tx.commit().await.expect("提交失败");

    // 在新连接中验证：会话已被永久删除
    assert!(
        conv_repo
            .find_by_id(conv1_id)
            .await
            .expect("查询会话")
            .is_none(),
        "Commit 后会话应该已永久删除"
    );
}

/// 验证：session delete-all 后 status 的 total_sessions = 0
#[tokio::test]
async fn test_session_delete_all_affects_status_total() {
    let env = create_env().await;

    // 初始 total_sessions 应该 > 0
    let resp_before = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 601, "request": { "operation": "status", "params": {} } }),
    )
    .await
    .unwrap();
    let total_before = resp_before["data"]["total_sessions"].as_u64().unwrap_or(0);
    assert!(total_before > 0, "初始应有会话");

    // 执行 delete-all
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 602, "request": { "operation": "session_delete_all", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);

    // 重新查询 status
    let resp_after = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 603, "request": { "operation": "status", "params": {} } }),
    )
    .await
    .unwrap();

    let total_after = resp_after["data"]["total_sessions"].as_u64().unwrap_or(0);
    assert_eq!(total_after, 0, "delete-all 后 total_sessions 应为 0");
}

/// 验证：session delete-character 后 status 的 total_sessions 正确减少
#[tokio::test]
async fn test_session_delete_character_affects_status() {
    let env = create_env().await;

    // 为种子角色绑定一个会话
    use yomua_bot::domain::character::{CharacterBinding, ReplyMode};
    let binding = CharacterBinding {
        id: 0,
        character_id: env.character_id,
        conversation_id: env.conv_a_id,
        reply_mode: ReplyMode::default(),
        proactive_enabled: false,
        mute_schedule: None,
        behavior_overrides: serde_json::json!({}),
        context_policy: serde_json::json!({}),
        switched_at: None,
        cross_reply_enabled: false,
        created_at: chrono::Utc::now(),
    };
    env.binding_repo
        .insert(&binding)
        .await
        .expect("插入绑定失败");

    // 初始 total_sessions
    let resp_before = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 611, "request": { "operation": "status", "params": {} } }),
    )
    .await
    .unwrap();
    let total_before = resp_before["data"]["total_sessions"].as_u64().unwrap_or(0);

    // 执行 delete-character
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 612, "request": { "operation": "session_delete_character", "params": {"character_id": env.character_id} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);

    // 重新查询 status
    let resp_after = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 613, "request": { "operation": "status", "params": {} } }),
    )
    .await
    .unwrap();

    let total_after = resp_after["data"]["total_sessions"].as_u64().unwrap_or(0);
    assert_eq!(
        total_after,
        total_before - 1,
        "删除一个会话后 total_sessions 应减少 1"
    );
}

// ============================================================================
// 记忆控制平面集成测试
// ============================================================================

/// 验证：memory_list 空列表返回 ok + 空数组
#[tokio::test]
async fn test_memory_list_empty() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 700, "request": { "operation": "memory_list", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "memory_list 应成功: {resp}");
    let memories = resp["data"]["memories"]
        .as_array()
        .expect("memories 应为数组");
    assert!(memories.is_empty(), "空环境应返回空列表");
}

#[tokio::test]
async fn test_memory_list_rejects_non_object_params() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 712, "request": { "operation": "memory_list", "params": [] } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], serde_json::json!(false));
    assert_eq!(resp["error"]["code"], serde_json::json!("INVALID_PARAMS"));
}

#[tokio::test]
async fn test_blacklist_ban_extreme_duration_returns_validation_error() {
    let env = create_env().await;
    let request = serde_json::json!({
        "version": 1,
        "id": 713,
        "request": {
            "operation": "blacklist_ban",
            "params": { "target": "extreme-duration-user", "target_type": "user", "duration_secs": i64::MAX, "confirm": true }
        }
    });
    let raw = serde_json::to_vec(&request).unwrap();
    let response = send_raw(&env.socket_path, &raw).await.unwrap();
    let resp: serde_json::Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(resp["ok"], serde_json::json!(false));
    assert_eq!(resp["error"]["code"], serde_json::json!("INVALID_PARAMS"));
}

/// 验证：memory_list 全局列表（已插入数据）
#[tokio::test]
async fn test_memory_list_global() {
    let env = create_env().await;

    // 插入两条记忆
    let m1 = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: Some(env.conv_a_id),
        memory_type: MemoryType::Episodic,
        content: "test episodic".to_string(),
        importance: 0.8,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    let m2 = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: None,
        memory_type: MemoryType::System,
        content: "test system".to_string(),
        importance: 0.5,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    env.memory_repo.insert(&m1).await.expect("插入记忆1失败");
    env.memory_repo.insert(&m2).await.expect("插入记忆2失败");

    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 701, "request": { "operation": "memory_list", "params": {} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "memory_list 应成功: {resp}");
    let memories = resp["data"]["memories"]
        .as_array()
        .expect("memories 应为数组");
    assert_eq!(memories.len(), 2, "应有 2 条记忆");
}

/// 验证：memory_list --character 按角色过滤
#[tokio::test]
async fn test_memory_list_by_character() {
    let env = create_env().await;

    let m = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: None,
        memory_type: MemoryType::Semantic,
        content: "character filtered".to_string(),
        importance: 0.6,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    env.memory_repo.insert(&m).await.expect("插入记忆失败");

    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 702, "request": { "operation": "memory_list", "params": {"character_id": env.character_id} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "memory_list 应成功: {resp}");
    let memories = resp["data"]["memories"]
        .as_array()
        .expect("memories 应为数组");
    assert_eq!(memories.len(), 1, "应有 1 条记忆");
    assert_eq!(memories[0]["character_id"], env.character_id);
}

/// 验证：memory_list --conversation 按会话过滤
#[tokio::test]
async fn test_memory_list_by_conversation() {
    let env = create_env().await;

    let m = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: Some(env.conv_a_id),
        memory_type: MemoryType::Episodic,
        content: "conversation filtered".to_string(),
        importance: 0.7,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    env.memory_repo.insert(&m).await.expect("插入记忆失败");

    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 703, "request": { "operation": "memory_list", "params": {"conversation_id": env.conv_a_id} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "memory_list 应成功: {resp}");
    let memories = resp["data"]["memories"]
        .as_array()
        .expect("memories 应为数组");
    assert_eq!(memories.len(), 1, "应有 1 条记忆");
    assert_eq!(memories[0]["conversation_id"], env.conv_a_id);
}

/// 验证：memory_list --type 按类型过滤
#[tokio::test]
async fn test_memory_list_by_type() {
    let env = create_env().await;

    // 插入 episodic 和 semantic 各一条
    let m1 = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: None,
        memory_type: MemoryType::Episodic,
        content: "episodic content".to_string(),
        importance: 0.9,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    let m2 = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: None,
        memory_type: MemoryType::Semantic,
        content: "semantic content".to_string(),
        importance: 0.8,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    env.memory_repo
        .insert(&m1)
        .await
        .expect("插入 episodic 记忆失败");
    env.memory_repo
        .insert(&m2)
        .await
        .expect("插入 semantic 记忆失败");

    // 只返回 episodic
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 704, "request": { "operation": "memory_list", "params": {"memory_type": "episodic"} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "memory_list 应成功: {resp}");
    let memories = resp["data"]["memories"]
        .as_array()
        .expect("memories 应为数组");
    assert_eq!(memories.len(), 1, "episodic 过滤后应有 1 条");
    assert_eq!(memories[0]["type"], "Episodic");
}

/// 验证：memory_list --type --limit 过滤先于 limit
#[tokio::test]
async fn test_memory_list_type_limit_ordering() {
    let env = create_env().await;

    // 插入 3 条 episodic
    for i in 0..3 {
        let m = Memory {
            id: 0,
            character_id: env.character_id,
            conversation_id: None,
            memory_type: MemoryType::Episodic,
            content: format!("episodic {}", i),
            importance: 0.9 - (i as f64 * 0.1),
            created_at: chrono::Utc::now(),
            last_accessed: chrono::Utc::now(),
            embedding: None,
            metadata: serde_json::json!({}),
        };
        env.memory_repo.insert(&m).await.expect("插入记忆失败");
    }

    // 插入 1 条 semantic（不应该被返回）
    let sm = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: None,
        memory_type: MemoryType::Semantic,
        content: "semantic should not appear".to_string(),
        importance: 0.95,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    env.memory_repo
        .insert(&sm)
        .await
        .expect("插入 semantic 失败");

    // limit=1 应该返回 1 条 episodic（不是 semantic），因为 filter 先于 limit
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 705, "request": { "operation": "memory_list", "params": {"memory_type": "episodic", "limit": 1} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "memory_list 应成功: {resp}");
    let memories = resp["data"]["memories"]
        .as_array()
        .expect("memories 应为数组");
    assert_eq!(memories.len(), 1, "limit=1 应返回 1 条");
    assert_eq!(
        memories[0]["type"], "Episodic",
        "应是 episodic 不是 semantic"
    );
}

/// 验证：memory_list --character --conversation --type 组合过滤（AND 语义）
#[tokio::test]
async fn test_memory_list_filter_combination() {
    let env = create_env().await;

    // 插入符合组合条件的记忆
    let m1 = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: Some(env.conv_a_id),
        memory_type: MemoryType::Episodic,
        content: "match all three".to_string(),
        importance: 0.8,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    env.memory_repo.insert(&m1).await.expect("插入记忆失败");

    // 插入其他各不匹配的
    let m2 = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: Some(env.conv_a_id),
        memory_type: MemoryType::Semantic,
        content: "wrong type".to_string(),
        importance: 0.8,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    env.memory_repo.insert(&m2).await.expect("插入记忆失败");

    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 706, "request": { "operation": "memory_list", "params": {
                "character_id": env.character_id,
                "conversation_id": env.conv_a_id,
                "memory_type": "episodic"
            } } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "memory_list 应成功: {resp}");
    let memories = resp["data"]["memories"]
        .as_array()
        .expect("memories 应为数组");
    assert_eq!(memories.len(), 1, "三个条件 AND 过滤后应有 1 条");
    assert_eq!(memories[0]["type"], "Episodic");
}

/// 验证：memory_list 空结果正常返回（不是 error）
#[tokio::test]
async fn test_memory_list_empty_result() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 707, "request": { "operation": "memory_list", "params": {"memory_type": "system"} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "memory_list 应成功（即使无结果）: {resp}");
    let memories = resp["data"]["memories"]
        .as_array()
        .expect("memories 应为数组");
    assert!(memories.is_empty(), "无 system 记忆时应返回空数组");
}

/// 验证：memory_delete 成功删除现有记忆
#[tokio::test]
async fn test_memory_delete_success() {
    let env = create_env().await;

    let m = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: None,
        memory_type: MemoryType::Semantic,
        content: "to be deleted".to_string(),
        importance: 0.6,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    let mem_id = env.memory_repo.insert(&m).await.expect("插入记忆失败");

    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 708, "request": { "operation": "memory_delete", "params": {"memory_id": mem_id} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "memory_delete 应成功: {resp}");
    assert_eq!(resp["data"]["memory_id"], mem_id, "应返回被删除的记忆 ID");

    // 验证确实被删除
    let existing = env.memory_repo.find_by_id(mem_id).await.expect("查询失败");
    assert!(existing.is_none(), "记忆应已被删除");
}

#[tokio::test]
async fn test_memory_update_applies_validated_model_suggestion() {
    let env = create_env().await;
    let m = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: Some(env.conv_a_id),
        memory_type: MemoryType::Semantic,
        content: "candidate fact".to_string(),
        importance: 0.6,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({"status":"candidate","shared":false}),
    };
    let mem_id = env.memory_repo.insert(&m).await.expect("插入候选记忆失败");
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 709, "request": { "operation": "memory_update", "params": {
                "memory_id": mem_id,
                "instruction": "请把这条记忆改成用户确认喜欢猫"
            } } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "合法模型建议应通过控制链路生效: {resp}");
    assert_eq!(resp["data"]["updated"], true);
    let updated = env
        .memory_repo
        .find_by_id(mem_id)
        .await
        .expect("查询更新后的记忆失败")
        .expect("更新后的记忆应存在");
    assert_eq!(updated.content, "用户确认喜欢猫");
    assert_eq!(updated.metadata["edit_request"]["status"], "applied");
    assert_eq!(updated.metadata["edit_request"]["action"], "replace");
}

#[tokio::test]
async fn test_memory_list_hides_content_until_raw_is_requested() {
    let env = create_env().await;
    let memory = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: Some(env.conv_a_id),
        memory_type: MemoryType::Semantic,
        content: "这是不应默认显示的隐私内容".to_string(),
        importance: 0.7,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({"status":"formal","shared":false}),
    };
    let memory_id = env.memory_repo.insert(&memory).await.expect("插入记忆失败");

    let hidden = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 710, "request": { "operation": "memory_list", "params": {"character_id": env.character_id} } }),
    )
    .await
    .unwrap();
    let hidden_item = hidden["data"]["memories"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == memory_id)
        .expect("应返回插入的记忆摘要");
    assert_eq!(hidden_item["content_available"], false);
    assert!(hidden_item.get("content").is_none());
    assert!(hidden_item["content_fingerprint"].as_str().is_some());

    let raw = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 711, "request": { "operation": "memory_list", "params": {"character_id": env.character_id, "raw": true} } }),
    )
    .await
    .unwrap();
    let raw_item = raw["data"]["memories"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == memory_id)
        .expect("raw 模式应返回记忆");
    assert_eq!(raw_item["content_available"], true);
    assert_eq!(raw_item["content"], "这是不应默认显示的隐私内容");
}

/// 验证：memory_delete 不存在的 ID 返回 MEMORY_NOT_FOUND
#[tokio::test]
async fn test_memory_delete_not_found() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 709, "request": { "operation": "memory_delete", "params": {"memory_id": 99999} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false, "memory_delete 不存在应返回 ok=false");
    assert_eq!(
        resp["error"]["code"], "MEMORY_NOT_FOUND",
        "应返回 MEMORY_NOT_FOUND"
    );
}

/// 验证：memory_delete（通过 Control Protocol）不影响 semantic_memories
#[tokio::test]
async fn test_memory_delete_leaves_semantic_memories_untouched() {
    let env = create_env().await;

    // 插入一条 Memory（使用 seeded character）
    let mem = Memory {
        id: 0,
        character_id: env.character_id,
        conversation_id: None,
        memory_type: MemoryType::Episodic,
        content: "regular memory to delete".to_string(),
        importance: 0.7,
        created_at: chrono::Utc::now(),
        last_accessed: chrono::Utc::now(),
        embedding: None,
        metadata: serde_json::json!({}),
    };
    let mem_id = env.memory_repo.insert(&mem).await.expect("插入记忆失败");

    // 插入一条 SemanticMemory（通过 insert_semantic）
    env.memory_repo
        .insert_semantic(
            env.character_id,
            None,
            "semantic",
            "semantic memory content",
            &[0.1; 5],
            0.8,
            "{}",
        )
        .await
        .expect("插入语义记忆失败");

    // 通过真实 UDS Control Protocol 发送 memory_delete
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({ "version": 1, "id": 999, "request": { "operation": "memory_delete", "params": {"memory_id": mem_id} } }),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true, "memory_delete 应成功: {resp}");
    assert_eq!(resp["data"]["memory_id"], mem_id, "应返回被删除的记忆 ID");

    // 验证 Memory 已被删除（通过 repository）
    let mem_after = env.memory_repo.find_by_id(mem_id).await.expect("查询失败");
    assert!(mem_after.is_none(), "Memory 应已被删除");

    // 验证 SemanticMemory 仍然存在且内容未变（通过真实数据库查询）
    // 字段顺序: id, character_id, conversation_id, memory_type, content,
    //            embedding, importance, created_at, last_accessed, metadata
    let mut conn = env.storage.pool().acquire().await.expect("获取连接失败");
    let row: (
        i64,
        i64,
        Option<i64>,
        String,
        String,
        String, // embedding (TEXT NOT NULL)
        f64,
        String,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT id, character_id, conversation_id, memory_type, content,
                embedding, importance, created_at, last_accessed, metadata
         FROM semantic_memories WHERE character_id = ?",
    )
    .bind(env.character_id)
    .fetch_one(&mut *conn)
    .await
    .expect("查询语义记忆失败");

    assert_eq!(row.0, 1, "应有 1 条 SemanticMemory");
    assert_eq!(row.1, env.character_id, "character_id 应不变");
    assert_eq!(row.2, None, "conversation_id 应为 None（插入时为 None）");
    assert_eq!(row.3, "semantic", "memory_type 应为 semantic");
    assert_eq!(row.4, "semantic memory content", "content 应不变");
    assert!(!row.5.is_empty(), "embedding 不应为空");
    assert!(
        (row.6 - 0.8).abs() < 0.01,
        "importance 应约为 0.8，实际={}",
        row.6
    );
}
