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
use yomua_bot::application::cognition::CognitionLayer;
use yomua_bot::application::config::RuntimeConfig;
use yomua_bot::application::context::ContextBuilder;
use yomua_bot::application::control::{start_control_service, CommandRegistry, RuntimeHandle};
use yomua_bot::application::conversation::ConversationManager;
use yomua_bot::application::event_bus::EventBus;
use yomua_bot::application::plugin_api::PluginApi;
use yomua_bot::domain::character::{Character, CharacterDefinition, CharacterState};
use yomua_bot::domain::conversation::{Conversation, ConversationType};
use yomua_bot::domain::repository::{
    CharacterBindingRepository, CharacterRepository, CharacterStateRepository,
    ConversationRepository, ConversationStateRepository, MemoryRepository, MessageRepository,
    MoodRepository, ParticipantRepository, PluginDataRepository, RelationshipRepository,
};
use yomua_bot::error::RuntimeError;
use yomua_bot::infrastructure::plugin::registry::PluginRegistry;
use yomua_bot::infrastructure::plugin::supervisor::{PluginSupervisor, SupervisorConfig};
use yomua_bot::infrastructure::storage::repository::{
    SqliteCharacterBindingRepository, SqliteCharacterRepository, SqliteCharacterStateRepository,
    SqliteConversationRepository, SqliteConversationStateRepository, SqliteMemoryRepository,
    SqliteMessageRepository, SqliteMoodRepository, SqliteParticipantRepository,
    SqlitePluginDataRepository, SqliteRelationshipRepository,
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

    // 种子角色（执行 state-set 前必须存在）。
    let character = Character {
        id: 0,
        definition: test_definition(),
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
        Arc::new(MockConnector),
    ));

    // 真实 PluginApi（供 PluginSupervisor 使用）。
    let dispatcher = Arc::new(ActionDispatcher::new(
        conversation_repo.clone(),
        adapter.clone() as Arc<dyn OneBotAdapter>,
    ));
    let context_builder = Arc::new(ContextBuilder::new(
        message_repo.clone(),
        conversation_repo.clone(),
        memory_repo.clone(),
        relationship_repo.clone(),
        mood_repo.clone(),
        binding_repo.clone(),
    ));
    let cognition = Arc::new(CognitionLayer::new(None, context_builder));
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
    let handle = RuntimeHandle {
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
        commands: CommandRegistry::builtin(),
        character_repo: character_repo.clone(),
        state_repo: state_repo.clone(),
        conversation_state_repo: conversation_state_repo.clone(),
        mood_repo: mood_repo.clone(),
        conversation_repo: conversation_repo.clone(),
    };

    let socket_path = socket_dir.join("control.sock");
    // 启动控制服务（真实 UDS 监听）。
    let server = start_control_service(handle.clone(), shutdown_rx);

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

    let json = serde_json::to_string(payload).map_err(|e| format!("序列化失败: {e}"))?;
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
    serde_json::json!({ "id": id, "cmd": "state-set", "params": params })
}

fn state_get_req(id: u64, cid: i64, conv: Option<i64>) -> serde_json::Value {
    let mut params = serde_json::Map::new();
    params.insert("character_id".to_string(), serde_json::json!(cid));
    if let Some(conv) = conv {
        params.insert("conversation_id".to_string(), serde_json::json!(conv));
    }
    serde_json::json!({ "id": id, "cmd": "state-get", "params": params })
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
        &serde_json::json!({"id": 1, "cmd": "help"}),
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
        "state-set",
        "state-get",
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
        &serde_json::json!({"id": 2, "cmd": "status"}),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);
    assert!(resp["data"]["adapter_state"].is_string());
    assert!(resp["data"]["plugin_count"].is_number());
    assert!(resp["data"]["data_dir"].is_string());
}

#[tokio::test]
async fn test_unknown_command_returns_error() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({"id": 3, "cmd": "nonexistent.command"}),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"]["code"], "UNKNOWN_COMMAND");
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
        &serde_json::json!({"id": 4242, "cmd": "status"}),
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
                &serde_json::json!({"id": 100 + i, "cmd": "status"}),
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
async fn test_shutdown_command_sets_flag() {
    let env = create_env().await;
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({"id": 7, "cmd": "shutdown"}),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], true);
    // 关停信号应已通过 watch channel 发出。
    assert!(*env.shutdown_tx.borrow(), "shutdown 信号应已置位");
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
        &serde_json::json!({"id": 33, "cmd": "state-set"}),
    )
    .await
    .unwrap();
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"]["code"], "INVALID_PARAMS");

    // 只有 character_id 没有目标字段 → INVALID_PARAMS。
    let resp = send_json(
        &env.socket_path,
        &serde_json::json!({"id": 34, "cmd": "state-set", "params": {"character_id": env.character_id}}),
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
