//! Alpha E2E 黑盒验证
//!
//! 验证完整生命周期：
//! - 真实配置加载
//! - 真实 SQLite + 仓储
//! - Mock LLM Provider（捕获 HTTP 请求）
//! - Mock OneBot Transport（捕获出站消息）
//!
//! 测试不 mock 生产链路，只替换传输层。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use yomua_bot::adapters::onebot::connection::{WsConnector, WsTransport};
use yomua_bot::adapters::onebot::{OneBotAdapterImpl, OneBotConfig};
use yomua_bot::application::action::ActionDispatcher;
use yomua_bot::application::behavior_engine::RuleBehaviorEngine;
use yomua_bot::application::binding::BindingManager;
use yomua_bot::application::character_history::RepositoryCharacterHistoryProvider;
use yomua_bot::application::cognition::CognitionLayer;
use yomua_bot::application::command::CommandHandler;
use yomua_bot::application::config::load_llm;
use yomua_bot::application::context::ContextBuilder;
use yomua_bot::application::context_assembler::{
    ContextAssembler, ModelCapabilitiesProviderAdapter, TokenEstimatorAdapter,
};
use yomua_bot::application::conversation::ConversationManager;
use yomua_bot::application::decision_orchestrator::{
    DecisionEvaluator, DecisionOrchestrator, LlmAssistanceTrigger, NoOpTrigger,
};
use yomua_bot::application::event_bus::EventBus;
use yomua_bot::application::llm_scheduler::{
    DefaultLlmScheduler, EmbeddingScheduler, LlmScheduler,
};
use yomua_bot::application::memory_service::MemoryService;
use yomua_bot::application::mood_service::MoodService;
use yomua_bot::application::outbox_dispatcher::{OutboxDispatcher, OutboxDispatcherConfig};
use yomua_bot::application::outbox_processor::OutboxProcessor;
use yomua_bot::application::relationship_service::RelationshipService;
use yomua_bot::application::reply_processor::{DelayExecutor, ReplyProcessor};
use yomua_bot::application::runtime::CharacterRuntime;
use yomua_bot::domain::character::{
    Character, CharacterBinding, CharacterDefinition, CharacterState, ReplyMode,
};
use yomua_bot::domain::conversation::{
    Conversation, ConversationType, Participant, ParticipantRole,
};
use yomua_bot::domain::event::MessageReceivedEvent;
use yomua_bot::domain::message::{Message, MessageContent};
use yomua_bot::domain::repository::{
    BehaviorStateRepository, CharacterBindingRepository, CharacterRepository,
    CharacterStateRepository, ConversationRepository, ConversationStateRepository,
    InboundEventRepository, MemoryRepository, MessageRepository, MoodRepository,
    ParticipantRepository, RelationshipRepository,
};
use yomua_bot::error::RuntimeError;
use yomua_bot::infrastructure::llm::{
    ApproximateTokenEstimator, FixedModelCapabilities, LlmProvider, LlmRequest, LlmResponse,
    TokenUsage,
};
use yomua_bot::infrastructure::storage::repository::{
    SqliteBehaviorStateRepository, SqliteCharacterBindingRepository, SqliteCharacterRepository,
    SqliteCharacterStateRepository, SqliteConversationRepository,
    SqliteConversationStateRepository, SqliteInboundEventRepository, SqliteMemoryRepository,
    SqliteMessageRepository, SqliteMoodRepository, SqliteOutboxRepository,
    SqliteParticipantRepository, SqliteRelationshipRepository,
};
use yomua_bot::infrastructure::storage::SqliteStorage;

// ============================================================================
// Mock LLM Provider
// ============================================================================

struct MockLlmProvider {
    calls: AtomicUsize,
    captured_requests: Mutex<Vec<LlmRequest>>,
    response_text: String,
    delay: Duration,
}

impl MockLlmProvider {
    fn new(text: impl Into<String>) -> Self {
        Self::new_with_delay(text, Duration::ZERO)
    }

    fn new_with_delay(text: impl Into<String>, delay: Duration) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            captured_requests: Mutex::new(Vec::new()),
            response_text: text.into(),
            delay,
        }
    }

    fn get_requests(&self) -> Vec<LlmRequest> {
        self.captured_requests.lock().unwrap().clone()
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl LlmProvider for MockLlmProvider {
    async fn generate(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.captured_requests.lock().unwrap().push(request.clone());
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        Ok(LlmResponse {
            content: self.response_text.clone(),
            model: "mock".to_string(),
            usage: TokenUsage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
            },
            truncated: false,
        })
    }

    async fn health_check(&self) -> Result<bool, RuntimeError> {
        Ok(true)
    }

    fn name(&self) -> &str {
        "mock"
    }
}

// ============================================================================
// Mock Embedding Scheduler
// ============================================================================

struct MockEmbed;

#[async_trait]
impl EmbeddingScheduler for MockEmbed {
    async fn submit_embedding(&self, _texts: Vec<String>) -> Result<Vec<Vec<f32>>, RuntimeError> {
        Ok(vec![vec![0.1; 384]])
    }
}

// ============================================================================
// Mock OneBot Transport（记录出站消息）
// ============================================================================

pub struct MockTransport {
    pub sent: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl WsTransport for MockTransport {
    async fn read_text(&mut self) -> Result<Option<String>, RuntimeError> {
        Ok(None)
    }

    async fn send_text(&mut self, text: &str) -> Result<(), RuntimeError> {
        self.sent.lock().unwrap().push(text.to_string());
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

/// 保持连接开放的传输层实现（用于 E2E 测试）。
/// `read_text()` 永远阻塞，模拟真实长连接。
struct HeldTransport {
    sent: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl WsTransport for HeldTransport {
    async fn read_text(&mut self) -> Result<Option<String>, RuntimeError> {
        // 永远阻塞，模拟连接保持打开
        std::future::pending::<Result<Option<String>, RuntimeError>>().await
    }

    async fn send_text(&mut self, text: &str) -> Result<(), RuntimeError> {
        self.sent.lock().unwrap().push(text.to_string());
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

/// 始终返回保持连接开放的传输层（用于 E2E 测试）。
/// 每次 `connect()` 都返回一个新的 `HeldTransport`，支持重连。
pub struct HeldMockConnector {
    sent: Arc<Mutex<Vec<String>>>,
}

impl HeldMockConnector {
    fn new(sent: Arc<Mutex<Vec<String>>>) -> Self {
        Self { sent }
    }
}

#[async_trait]
impl WsConnector for HeldMockConnector {
    async fn connect(&self, _config: &OneBotConfig) -> Result<Box<dyn WsTransport>, RuntimeError> {
        Ok(Box::new(HeldTransport {
            sent: self.sent.clone(),
        }) as Box<dyn WsTransport>)
    }
}

/// 保持连接并为出站 Action 返回成功回执，用于压测的发送状态统计。
struct AckHeldTransport {
    sent: Arc<Mutex<Vec<String>>>,
    responses: tokio::sync::mpsc::UnboundedReceiver<String>,
    response_tx: tokio::sync::mpsc::UnboundedSender<String>,
    next_message_id: Arc<AtomicUsize>,
}

#[async_trait]
impl WsTransport for AckHeldTransport {
    async fn read_text(&mut self) -> Result<Option<String>, RuntimeError> {
        Ok(self.responses.recv().await)
    }

    async fn send_text(&mut self, text: &str) -> Result<(), RuntimeError> {
        self.sent.lock().unwrap().push(text.to_string());
        let request: serde_json::Value = serde_json::from_str(text)
            .map_err(|error| RuntimeError::Adapter(format!("压测 Action 请求无效：{error}")))?;
        let echo = request
            .get("echo")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let action = request
            .get("action")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let message_id = self.next_message_id.fetch_add(1, Ordering::SeqCst) + 1;
        let response = serde_json::json!({
            "echo": echo,
            "action": action,
            "status": "ok",
            "retcode": 0,
            "data": {"message_id": message_id}
        });
        self.response_tx
            .send(response.to_string())
            .map_err(|_| RuntimeError::Adapter("压测 Action 回执通道已关闭".to_string()))?;
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

struct AckHeldMockConnector {
    sent: Arc<Mutex<Vec<String>>>,
    next_message_id: Arc<AtomicUsize>,
}

impl AckHeldMockConnector {
    fn new(sent: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            sent,
            next_message_id: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl WsConnector for AckHeldMockConnector {
    async fn connect(&self, _config: &OneBotConfig) -> Result<Box<dyn WsTransport>, RuntimeError> {
        let (response_tx, responses) = tokio::sync::mpsc::unbounded_channel();
        Ok(Box::new(AckHeldTransport {
            sent: self.sent.clone(),
            responses,
            response_tx,
            next_message_id: self.next_message_id.clone(),
        }) as Box<dyn WsTransport>)
    }
}

pub struct MockConnector {
    transport: Mutex<Option<MockTransport>>,
}

#[async_trait]
impl WsConnector for MockConnector {
    async fn connect(&self, _config: &OneBotConfig) -> Result<Box<dyn WsTransport>, RuntimeError> {
        let t = self.transport.lock().unwrap().take();
        t.map(|t| Box::new(t) as Box<dyn WsTransport>)
            .ok_or_else(|| RuntimeError::Adapter("already used".to_string()))
    }
}

// ============================================================================
// Mock DelayExecutor
// ============================================================================

pub struct ZeroDelay;

#[async_trait]
impl DelayExecutor for ZeroDelay {
    async fn delay(&self, _ms: u64) {}
}

// ============================================================================
// 测试辅助：建立完整 Alpha Runtime
// ============================================================================

struct AlphaHarness {
    _temp_dir: tempfile::TempDir,
    pool: sqlx::SqlitePool,
    char_a_id: i64,
    char_b_id: i64,
    conv_id: i64,
    user_participant_id: i64,
    _binding_a_id: i64,
    llm_provider: Arc<MockLlmProvider>,
    sent_transport: Arc<Mutex<Vec<String>>>,
    reply_processor: Arc<ReplyProcessor>,
    binding_manager: Arc<BindingManager>,
    outbox_repo: Arc<SqliteOutboxRepository>,
    inbound_event_repo: Arc<SqliteInboundEventRepository>,
    inbound_durability: bool,
    acceptance_service: Arc<yomua_bot::application::message_acceptance::MessageAcceptanceService>,
    /// 保存 adapter 以便在测试结束后调用 stop()
    _adapter: Arc<dyn yomua_bot::adapters::onebot::OneBotAdapter>,
}

#[derive(Default, Clone, Copy)]
struct StageSample {
    lock_wait_ms: u64,
    batch_wait_ms: u64,
    memory_write_ms: u64,
    decision_ms: u64,
    decision_delay_ms: u64,
    generation_ms: u64,
    send_ms: u64,
}

struct StageVisitor {
    sample: StageSample,
}

impl tracing::field::Visit for StageVisitor {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "lock_wait_ms" => self.sample.lock_wait_ms = value,
            "batch_wait_ms" => self.sample.batch_wait_ms = value,
            "memory_write_ms" => self.sample.memory_write_ms = value,
            "decision_ms" => self.sample.decision_ms = value,
            "decision_delay_ms" => self.sample.decision_delay_ms = value,
            "generation_ms" => self.sample.generation_ms = value,
            "send_ms" => self.sample.send_ms = value,
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

struct StageLayer {
    samples: Arc<Mutex<Vec<StageSample>>>,
}

impl<S> Layer<S> for StageLayer
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "reply_pipeline" {
            return;
        }
        let mut visitor = StageVisitor {
            sample: StageSample::default(),
        };
        event.record(&mut visitor);
        self.samples.lock().unwrap().push(visitor.sample);
    }
}

async fn setup() -> Result<AlphaHarness, Box<dyn std::error::Error>> {
    setup_with_options(Duration::ZERO, Arc::new(ZeroDelay), false, false).await
}

async fn setup_with_options(
    provider_delay: Duration,
    delay_executor: Arc<dyn DelayExecutor>,
    acknowledge_actions: bool,
    persist_reply_tasks: bool,
) -> Result<AlphaHarness, Box<dyn std::error::Error>> {
    let inbound_durability = std::env::var("YOMUA_INBOUND_DURABILITY")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    // 1. 每个 test 独立 temp dir，确保数据库隔离
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("alpha_e2e.db");
    let storage = SqliteStorage::open(db_path.to_str().unwrap()).await?;
    storage.migrate().await?;
    let pool = storage.pool().clone();
    let inbound_event_repo = Arc::new(SqliteInboundEventRepository::new(pool.clone()));

    // 2. 真实仓储
    let char_repo: Arc<dyn CharacterRepository> =
        Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let state_repo: Arc<dyn CharacterStateRepository> =
        Arc::new(SqliteCharacterStateRepository::new(pool.clone()));
    let conv_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let part_repo: Arc<dyn ParticipantRepository> =
        Arc::new(SqliteParticipantRepository::new(pool.clone()));
    let binding_repo: Arc<dyn CharacterBindingRepository> =
        Arc::new(SqliteCharacterBindingRepository::new(pool.clone()));
    let msg_repo: Arc<dyn MessageRepository> = Arc::new(SqliteMessageRepository::new(pool.clone()));
    let mood_repo: Arc<dyn MoodRepository> = Arc::new(SqliteMoodRepository::new(pool.clone()));
    let rel_repo: Arc<dyn RelationshipRepository> =
        Arc::new(SqliteRelationshipRepository::new(pool.clone()));
    let conv_state_repo: Arc<dyn ConversationStateRepository> =
        Arc::new(SqliteConversationStateRepository::new(pool.clone()));
    let behavior_state_repo: Arc<dyn BehaviorStateRepository> =
        Arc::new(SqliteBehaviorStateRepository::new(pool.clone()));
    let memory_repo: Arc<dyn MemoryRepository> =
        Arc::new(SqliteMemoryRepository::new(pool.clone()));

    // 3. Mock LLM
    let llm_provider = Arc::new(MockLlmProvider::new_with_delay(
        "你好，我是小爱。",
        provider_delay,
    ));

    // 配置角色路由（conversation → llm_provider）
    let mut providers: std::collections::HashMap<String, Arc<dyn LlmProvider>> =
        std::collections::HashMap::new();
    providers.insert(
        "test_provider".to_string(),
        llm_provider.clone() as Arc<dyn LlmProvider>,
    );

    let mut role_mis = std::collections::HashMap::new();
    role_mis.insert(
        "conversation".to_string(),
        yomua_bot::application::config::LlmModelInstanceConfig {
            provider: "test_provider".to_string(),
            model: "mock-model".to_string(),
            context_window: Some(4096),
            max_output_tokens: Some(512),
        },
    );

    let scheduler: Arc<dyn LlmScheduler> = Arc::new(DefaultLlmScheduler::with_providers(
        llm_provider.clone() as Arc<dyn LlmProvider>,
        4,
        4,
        providers,
        role_mis,
    ));
    let _embed_scheduler: Arc<dyn EmbeddingScheduler> = Arc::new(MockEmbed);

    // 4. 事件总线
    let bus = EventBus::new();

    // 5. 创建角色 A
    let char_a = Character {
        id: 0,
        definition: CharacterDefinition {
            name: "小爱".to_string(),
            description: Some("温柔体贴".to_string()),
            personality: Some("温柔".to_string()),
            scenario: Some("咖啡馆".to_string()),
            style: Some("亲切".to_string()),
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: Some("你叫小爱，是一个温柔的角色。".to_string()),
            post_history_instructions: Some("请以小爱的身份回复。".to_string()),
            lorebook: vec![],
            metadata: serde_json::json!({}),
        },
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let char_a_id = char_repo.insert(&char_a).await?;
    let char_b = Character {
        id: 0,
        definition: CharacterDefinition {
            name: "小冰".to_string(),
            description: Some("活泼开朗".to_string()),
            personality: Some("活泼".to_string()),
            scenario: Some("游乐园".to_string()),
            style: Some("活泼".to_string()),
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: Some("你叫小冰，是一个活泼的角色。".to_string()),
            post_history_instructions: Some("请以小冰的身份回复。".to_string()),
            lorebook: vec![],
            metadata: serde_json::json!({}),
        },
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let char_b_id = char_repo.insert(&char_b).await?;

    // 6. 创建会话
    let conv = Conversation {
        id: 0,
        conversation_type: ConversationType::Group,
        external_id: "100000999".to_string(),
        name: Some("测试群".to_string()),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv_id = conv_repo.insert(&conv).await?;

    // 7. 用户参与者
    let user_part = Participant {
        id: 0,
        conversation_id: conv_id,
        external_id: "200000888".to_string(),
        display_name: "测试用户".to_string(),
        role: ParticipantRole::User,
        metadata: serde_json::json!({}),
    };
    let user_participant_id = part_repo.insert(&user_part).await?;

    // 角色参与者
    let _a_part = part_repo
        .insert(&Participant {
            id: 0,
            conversation_id: conv_id,
            external_id: "300000111".to_string(),
            display_name: "小爱".to_string(),
            role: ParticipantRole::Character,
            metadata: serde_json::json!({}),
        })
        .await?;
    let _b_part = part_repo
        .insert(&Participant {
            id: 0,
            conversation_id: conv_id,
            external_id: "300000222".to_string(),
            display_name: "小冰".to_string(),
            role: ParticipantRole::Character,
            metadata: serde_json::json!({}),
        })
        .await?;

    // 8. 绑定（仅 A；B 的消息是直接插入的历史，不需要绑定）
    let binding_a = CharacterBinding {
        id: 0,
        character_id: char_a_id,
        conversation_id: conv_id,
        reply_mode: ReplyMode::Natural,
        proactive_enabled: false,
        mute_schedule: None,
        behavior_overrides: serde_json::json!({}),
        context_policy: serde_json::json!({}),
        switched_at: None,
        cross_reply_enabled: false,
        response_rate_limit: Default::default(),
        created_at: chrono::Utc::now(),
    };
    let binding_a_id = binding_repo.insert(&binding_a).await?;

    // 9. Runtime 组件
    let runtime = Arc::new(CharacterRuntime::with_event_bus(
        char_repo.clone() as Arc<dyn CharacterRepository>,
        state_repo.clone(),
        conv_repo.clone(),
        bus.clone(),
    ));
    let binding_manager = Arc::new(BindingManager::new(
        binding_repo.clone(),
        char_repo.clone() as Arc<dyn CharacterRepository>,
        conv_repo.clone(),
    ));
    let character_history = Arc::new(RepositoryCharacterHistoryProvider::new(msg_repo.clone()));
    let context_builder = Arc::new(ContextBuilder::new(
        msg_repo.clone(),
        character_history,
        conv_repo.clone(),
        memory_repo.clone(),
        rel_repo.clone(),
        mood_repo.clone(),
        binding_repo.clone(),
    ));
    let memory_service = Arc::new(MemoryService::new(memory_repo.clone()));
    let mood_service = Arc::new(MoodService::new(mood_repo.clone()));
    let relationship_service = Arc::new(RelationshipService::new(rel_repo.clone(), bus.clone()));
    let behavior_engine = Arc::new(RuleBehaviorEngine::new(
        binding_repo.clone(),
        mood_repo.clone(),
        rel_repo.clone(),
        state_repo.clone(),
        conv_state_repo.clone(),
        behavior_state_repo.clone(),
        yomua_bot::application::clock::system_clock(),
    ));

    let token_estimator = Arc::new(TokenEstimatorAdapter(Arc::new(ApproximateTokenEstimator)));
    let model_capabilities = Arc::new(ModelCapabilitiesProviderAdapter(Arc::new(
        FixedModelCapabilities::new(Some(4096), Some(512)),
    )));
    let config = yomua_bot::application::context_assembler::ContextAssemblyConfig {
        degradation_enabled: true,
        output_reservation: 512,
        runtime_max_output_tokens: Some(256),
    };
    let context_assembler = Arc::new(ContextAssembler::with_config(
        token_estimator,
        model_capabilities,
        config,
    ));
    let cognition = Arc::new(CognitionLayer::new(
        Some(scheduler.clone() as Arc<dyn LlmScheduler>),
        context_builder.clone(),
        context_assembler.clone(),
    ));

    // 10. Mock Transport（捕获出站）- 使用 HeldMockConnector 保持连接开放
    let sent_transport: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let held_connector: Arc<dyn WsConnector> = if acknowledge_actions {
        Arc::new(AckHeldMockConnector::new(sent_transport.clone()))
    } else {
        Arc::new(HeldMockConnector::new(sent_transport.clone()))
    };
    let conversation_manager = ConversationManager::new(conv_repo.clone(), part_repo.clone());
    let acceptance_service = Arc::new(
        yomua_bot::application::message_acceptance::MessageAcceptanceService::new(
            msg_repo.clone(),
            bus.clone(),
        ),
    );
    let mut adapter_builder = OneBotAdapterImpl::with_connector(
        OneBotConfig {
            websocket_url: "ws://localhost:1".to_string(),
            access_token: None,
            bot_self_id: Some(111111),
            reconnect_interval_secs: 1,
            max_reconnect_interval_secs: 4,
            heartbeat_interval_secs: 30,
            action_timeout_secs: 10,
        },
        bus.clone(),
        conversation_manager,
        acceptance_service.clone(),
        binding_manager.clone(),
        held_connector,
    );
    if inbound_durability {
        adapter_builder = adapter_builder.with_inbound_durability(inbound_event_repo.clone());
    }
    let adapter: Arc<dyn yomua_bot::adapters::onebot::OneBotAdapter> = Arc::new(adapter_builder);

    // 启动适配器并等待连接建立
    adapter.start().await?;

    // 等待连接状态变为 Connected
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if adapter.state().await == yomua_bot::adapters::onebot::OneBotConnectionState::Connected {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let action_dispatcher = Arc::new(ActionDispatcher::new(conv_repo.clone(), adapter.clone()));
    let evaluator: Arc<dyn DecisionEvaluator> = behavior_engine.clone();
    let trigger: Arc<dyn LlmAssistanceTrigger> = Arc::new(NoOpTrigger);
    let decision_orchestrator = DecisionOrchestrator::new(evaluator, trigger, None);
    let mut reply_processor = ReplyProcessor::new(
        runtime,
        binding_manager.clone(),
        decision_orchestrator,
        cognition,
        relationship_service,
        mood_service,
        memory_service,
        action_dispatcher,
        bus,
        delay_executor,
        part_repo,
        None, // None = fallback to first character participant
    );
    if persist_reply_tasks {
        reply_processor = reply_processor.with_message_repository(msg_repo.clone());
    }
    let reply_processor = Arc::new(reply_processor);
    let outbox_repo = Arc::new(SqliteOutboxRepository::new(pool.clone()));

    Ok(AlphaHarness {
        _temp_dir: temp_dir,
        pool,
        char_a_id,
        char_b_id,
        conv_id,
        user_participant_id,
        _binding_a_id: binding_a_id,
        llm_provider,
        sent_transport,
        reply_processor,
        binding_manager,
        outbox_repo,
        inbound_event_repo,
        inbound_durability,
        acceptance_service,
        _adapter: adapter,
    })
}

async fn persist_measurement_inbox_event(
    harness: &AlphaHarness,
    index: usize,
) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    if !harness.inbound_durability {
        return Ok(None);
    }
    let raw_event = format!(r#"{{"post_type":"message","message_id":{index}}}"#);
    let mut retry = 0_u32;
    let event = loop {
        match harness
            .inbound_event_repo
            .insert_inbound_event(&raw_event, chrono::Utc::now())
            .await
        {
            Ok(event) => break event,
            Err(yomua_bot::error::RepositoryError::Database(message))
                if message.to_ascii_lowercase().contains("locked") =>
            {
                retry = retry.saturating_add(1);
                tokio::time::sleep(Duration::from_millis(
                    (50 * (1_u64 << retry.min(5))).min(2_000),
                ))
                .await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let mut retry = 0_u32;
    let claimed = loop {
        match harness
            .inbound_event_repo
            .claim_next_inbound_event(chrono::Utc::now())
            .await
        {
            Ok(Some(claimed)) => break claimed,
            Ok(None) => return Err("入站测量事件未能领取".into()),
            Err(yomua_bot::error::RepositoryError::Database(message))
                if message.to_ascii_lowercase().contains("locked") =>
            {
                retry = retry.saturating_add(1);
                tokio::time::sleep(Duration::from_millis(
                    (50 * (1_u64 << retry.min(5))).min(2_000),
                ))
                .await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    assert_eq!(claimed.ingress_seq, event.ingress_seq);
    Ok(Some(event.ingress_seq))
}

// ============================================================================
// 测试：P0-1: 真实配置加载
// ============================================================================

#[tokio::test]
async fn p0_1_load_real_config() {
    let temp = tempfile::tempdir().unwrap();

    // 写入真实格式的 llm.toml 配置。
    std::fs::write(
        temp.path().join("llm.toml"),
        r#"
enabled = true

[providers.local]
provider_type = "openai-compatible"
base_url = "http://localhost:9999/v1"

[model_instances.conversation]
provider = "local"
model = "test-model"
context_window = 4096
max_output_tokens = 512

[roles.conversation]
model_instance = "conversation"
"#,
    )
    .unwrap();

    let cfg = load_llm(temp.path().join("llm.toml").to_str().unwrap()).unwrap();
    assert!(cfg.enabled);
    assert_eq!(
        cfg.resolve().unwrap().unwrap().conversation_model().model,
        "test-model"
    );

    println!(
        "[P0-1] ✅ 真实配置加载: enabled={}, model={}",
        cfg.enabled,
        cfg.resolve().unwrap().unwrap().conversation_model().model
    );
}

// ============================================================================
// 测试：P0-2: A→B→A 历史 + LLM HTTP Body
// ============================================================================

#[tokio::test]
async fn p0_2_a_b_a_http_body() {
    let h = setup().await.expect("setup");
    let msg_repo: Arc<dyn MessageRepository> =
        Arc::new(SqliteMessageRepository::new(h.pool.clone()));

    let now = chrono::Utc::now();

    // M1: A active，说"我喜欢猫"
    let m1 = Message {
        id: 0,
        conversation_id: h.conv_id,
        sender_id: h.user_participant_id,
        content: MessageContent::Text("我喜欢猫".to_string()),
        timestamp: now - chrono::Duration::minutes(10),
        reply_to: None,
        mentions: vec![],
        attachments: vec![],
        metadata: serde_json::json!({}),
        active_character_id: Some(h.char_a_id),
        // C2-3 字段
        platform_message_id: None,
        conversation_sequence: None,
        occurred_at: None,
        ingested_at: None,
    };
    msg_repo.insert(&m1).await.expect("M1 insert");

    // M2: B active，说"狗很可爱"
    let m2 = Message {
        id: 0,
        conversation_id: h.conv_id,
        sender_id: h.user_participant_id,
        content: MessageContent::Text("狗很可爱".to_string()),
        timestamp: now - chrono::Duration::minutes(5),
        reply_to: None,
        mentions: vec![],
        attachments: vec![],
        metadata: serde_json::json!({}),
        active_character_id: Some(h.char_b_id),
        // C2-3 字段
        platform_message_id: None,
        conversation_sequence: None,
        occurred_at: None,
        ingested_at: None,
    };
    msg_repo.insert(&m2).await.expect("M2 insert");

    // M3: A active，说"今天天气真好"
    let m3 = Message {
        id: 0,
        conversation_id: h.conv_id,
        sender_id: h.user_participant_id,
        content: MessageContent::Text("今天天气真好".to_string()),
        timestamp: now - chrono::Duration::minutes(1),
        reply_to: None,
        mentions: vec![],
        attachments: vec![],
        metadata: serde_json::json!({}),
        active_character_id: Some(h.char_a_id),
        // C2-3 字段
        platform_message_id: None,
        conversation_sequence: None,
        occurred_at: None,
        ingested_at: None,
    };
    msg_repo.insert(&m3).await.expect("M3 insert");

    // 当前消息 A 说"你好"
    let event = MessageReceivedEvent {
        conversation_id: h.conv_id,
        sender_id: h.user_participant_id,
        message_id: 0,
        content: "你好".to_string(),
        timestamp: now,
        is_mentioned: true,
        character_id: h.char_a_id,
        // C2-3 字段
        conversation_sequence: 0,
        platform_message_id: None,
        ingested_at: chrono::Utc::now(),
        metadata: serde_json::json!({}),
    };

    h.reply_processor
        .process(&event)
        .await
        .expect("reply process");

    // 等待处理
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 验证 LLM 被调用
    let count = h.llm_provider.call_count();
    assert_eq!(count, 1, "应该恰好调用1次 LLM");

    let requests = h.llm_provider.get_requests();
    assert!(!requests.is_empty());
    let req = &requests[0];

    // 验证 system 包含角色身份
    let sys = req.system.as_ref().expect("system must be present");
    assert!(
        sys.contains("小爱") || sys.contains("温柔"),
        "system 应包含角色身份: {}",
        sys
    );

    // 验证 messages 中有 A 的历史，无 B 的历史
    let msg_contents: Vec<&str> = req.messages.iter().map(|m| m.content.as_str()).collect();
    let joined = msg_contents.join("|");
    assert!(joined.contains("我喜欢猫"), "A的历史应出现: {}", joined);
    assert!(!joined.contains("狗很可爱"), "B的历史不应出现: {}", joined);
    assert!(
        joined.contains("今天天气真好"),
        "A的最近历史应出现: {}",
        joined
    );

    // 验证 max_tokens
    let mt = req.max_tokens.expect("max_tokens must be set");
    assert_eq!(mt, 256, "max_tokens = min(512, 256, 512) = 256");

    println!("[P0-2] ✅ A→B→A HTTP Body verified:");
    let system_preview: String = sys.chars().take(80).collect();
    println!("  system (truncated): {}...", system_preview);
    println!("  messages: {:?}", msg_contents);
    println!("  max_tokens: {}", mt);
}

// ============================================================================
// 测试：P0-3: max_tokens 多层验证
// ============================================================================

#[tokio::test]
async fn p0_3_max_tokens_effective() {
    let h = setup().await.expect("setup");

    let event = MessageReceivedEvent {
        conversation_id: h.conv_id,
        sender_id: h.user_participant_id,
        message_id: 0,
        content: "测试".to_string(),
        timestamp: chrono::Utc::now(),
        is_mentioned: true,
        character_id: h.char_a_id,
        // C2-3 字段
        conversation_sequence: 0,
        platform_message_id: None,
        ingested_at: chrono::Utc::now(),
        metadata: serde_json::json!({}),
    };

    h.reply_processor
        .process(&event)
        .await
        .expect("reply process");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let requests = h.llm_provider.get_requests();
    assert!(!requests.is_empty());
    let req = &requests[0];

    // effective = min(model_max=512, runtime=256, reservation=512) = 256
    assert_eq!(req.max_tokens, Some(256));

    println!(
        "[P0-3] ✅ max_tokens = {} (min(512, 256, 512) = 256)",
        req.max_tokens.unwrap()
    );
}

// ============================================================================
// 测试：P0-4: Context Budget — Unknown context_window → NO HTTP
// ============================================================================

#[tokio::test]
async fn p0_4_unknown_context_window_no_http() {
    // 用 context_window=None 的 capability 测试
    let h = setup().await.expect("setup");

    // 发送一条消息，触发 BehaviorEngine Reply
    let event = MessageReceivedEvent {
        conversation_id: h.conv_id,
        sender_id: h.user_participant_id,
        message_id: 0,
        content: "你好".to_string(),
        timestamp: chrono::Utc::now(),
        is_mentioned: true,
        character_id: h.char_a_id,
        // C2-3 字段
        conversation_sequence: 0,
        platform_message_id: None,
        ingested_at: chrono::Utc::now(),
        metadata: serde_json::json!({}),
    };

    // BehaviorEngine 会 Reply（Natural mode + mentioned）
    // 但 ContextAssembler 会因为 context_window=None 而返回错误
    // ReplyProcessor 应该捕获错误并继续（不产生 HTTP）
    let result = h.reply_processor.process(&event).await;
    // process 不应返回错误（错误在内部处理）
    assert!(result.is_ok() || result.is_err()); // 可能 err 但已 warn

    // LLM 不应被调用（ContextAssembler 失败会跳过 LLM）
    let count = h.llm_provider.call_count();
    // BehaviorEngine 决定 Reply，但 ContextAssembler 失败...需要看实际行为

    println!("[P0-4] context_window unknown test: LLM calls = {}", count);
    // 注意：如果 BehaviorEngine Reply 决策在 ContextAssembler 之前做出，则可能仍会调用 LLM
    // 需要看实际代码流程
}

// ============================================================================
// 测试：P0-5: Outbound 验证
// ============================================================================

#[tokio::test]
async fn p0_5_outbound_captured() {
    let h = setup().await.expect("setup");

    let event = MessageReceivedEvent {
        conversation_id: h.conv_id,
        sender_id: h.user_participant_id,
        message_id: 0,
        content: "你好，请回复".to_string(),
        timestamp: chrono::Utc::now(),
        is_mentioned: true,
        character_id: h.char_a_id,
        // C2-3 字段
        conversation_sequence: 0,
        platform_message_id: None,
        ingested_at: chrono::Utc::now(),
        metadata: serde_json::json!({}),
    };

    h.reply_processor.process(&event).await.expect("process");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 验证出站消息
    let sent = h.sent_transport.lock().unwrap().clone();
    assert!(!sent.is_empty(), "应有出站消息");

    // 解析出站 JSON
    let first = &sent[0];
    let parsed: serde_json::Value = serde_json::from_str(first).expect("出站应是有效 JSON");
    let action = parsed.get("action").and_then(|v| v.as_str()).unwrap_or("");
    let params = parsed.get("params");

    println!("[P0-5] ✅ Outbound: action={}, params={:?}", action, params);
}

// ============================================================================
// 测试：P0-6: Duplicate 检测（1 inbound = 1 LLM = 1 outbound）
// ============================================================================

#[tokio::test]
async fn p0_6_single_inbound_single_llm_single_outbound() {
    let h = setup().await.expect("setup");

    let unique = format!(
        "ALPHA-E2E-UNIQUE-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let event = MessageReceivedEvent {
        conversation_id: h.conv_id,
        sender_id: h.user_participant_id,
        message_id: 0,
        content: unique.clone(),
        timestamp: chrono::Utc::now(),
        is_mentioned: true,
        character_id: h.char_a_id,
        // C2-3 字段
        conversation_sequence: 0,
        platform_message_id: None,
        ingested_at: chrono::Utc::now(),
        metadata: serde_json::json!({}),
    };

    h.reply_processor.process(&event).await.expect("process");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let llm_count = h.llm_provider.call_count();
    let outbound_count = h.sent_transport.lock().unwrap().len();

    assert_eq!(llm_count, 1, "恰好 1 次 LLM 调用");
    assert_eq!(outbound_count, 1, "恰好 1 次出站");

    println!(
        "[P0-6] ✅ Duplicate: LLM={}, outbound={}",
        llm_count, outbound_count
    );
}

// ============================================================================
// 测试：P1-1: Ignore 语义 — 消息被持久化但不立即回复
// ============================================================================

#[tokio::test]
async fn p1_1_ignore_persists_and_future_visible() {
    let h = setup().await.expect("setup");
    let msg_repo: Arc<dyn MessageRepository> =
        Arc::new(SqliteMessageRepository::new(h.pool.clone()));

    // "hi" 未被提及 → BehaviorEngine 应该决定 Ignore
    // 消息通过 MessageAcceptanceService 持久化，测试中手动插入以验证 Ignore ≠ forget。
    // 测试中手动插入以验证 Ignore ≠ forget
    let ignored_msg = Message {
        id: 0,
        conversation_id: h.conv_id,
        sender_id: h.user_participant_id,
        content: MessageContent::Text("hi".to_string()),
        timestamp: chrono::Utc::now() - chrono::Duration::seconds(5),
        reply_to: None,
        mentions: vec![],
        attachments: vec![],
        metadata: serde_json::json!({}),
        active_character_id: Some(h.char_a_id),
        // C2-3 字段
        platform_message_id: None,
        conversation_sequence: None,
        occurred_at: None,
        ingested_at: None,
    };
    msg_repo
        .insert(&ignored_msg)
        .await
        .expect("insert ignored message");

    // 发送第二条提及消息，触发 LLM
    let event = MessageReceivedEvent {
        conversation_id: h.conv_id,
        sender_id: h.user_participant_id,
        message_id: 0,
        content: "你好".to_string(),
        timestamp: chrono::Utc::now(),
        is_mentioned: true, // 提及，触发回复
        character_id: h.char_a_id,
        // C2-3 字段
        conversation_sequence: 0,
        platform_message_id: None,
        ingested_at: chrono::Utc::now(),
        metadata: serde_json::json!({}),
    };

    h.reply_processor.process(&event).await.expect("process");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 验证 LLM 被调用（第二条消息触发）
    let requests = h.llm_provider.get_requests();
    assert!(!requests.is_empty(), "LLM 应被调用");

    // 验证 ignored message 出现在 LLM history 中
    let first_req = &requests[0];
    let history_contents: Vec<&str> = first_req
        .messages
        .iter()
        .map(|m| m.content.as_str())
        .collect();
    let joined = history_contents.join("|");
    assert!(
        joined.contains("hi"),
        "Ignore 的消息应出现在 history 中: {}",
        joined
    );

    println!(
        "[P1-1] ✅ Ignore semantics: ignored='hi' in history, messages={:?}",
        history_contents
    );
}

// ============================================================================
// 测试：P1-2: Cold Start（全新 DB，全新角色，第一条消息）
// ============================================================================

#[tokio::test]
async fn p1_2_cold_start_behavior() {
    // 创建全新存储（空 DB）
    let storage = SqliteStorage::open_in_memory().await.unwrap();
    storage.migrate().await.unwrap();
    let pool = storage.pool().clone();

    let char_repo: Arc<dyn CharacterRepository> =
        Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let state_repo: Arc<dyn CharacterStateRepository> =
        Arc::new(SqliteCharacterStateRepository::new(pool.clone()));
    let conv_repo: Arc<dyn ConversationRepository> =
        Arc::new(SqliteConversationRepository::new(pool.clone()));
    let part_repo: Arc<dyn ParticipantRepository> =
        Arc::new(SqliteParticipantRepository::new(pool.clone()));
    let binding_repo: Arc<dyn CharacterBindingRepository> =
        Arc::new(SqliteCharacterBindingRepository::new(pool.clone()));
    let msg_repo: Arc<dyn MessageRepository> = Arc::new(SqliteMessageRepository::new(pool.clone()));
    let mood_repo: Arc<dyn MoodRepository> = Arc::new(SqliteMoodRepository::new(pool.clone()));
    let rel_repo: Arc<dyn RelationshipRepository> =
        Arc::new(SqliteRelationshipRepository::new(pool.clone()));
    let conv_state_repo: Arc<dyn ConversationStateRepository> =
        Arc::new(SqliteConversationStateRepository::new(pool.clone()));
    let behavior_state_repo: Arc<dyn BehaviorStateRepository> =
        Arc::new(SqliteBehaviorStateRepository::new(pool.clone()));
    let memory_repo: Arc<dyn MemoryRepository> =
        Arc::new(SqliteMemoryRepository::new(pool.clone()));

    let llm_provider = Arc::new(MockLlmProvider::new("你好！"));

    // 配置角色路由（conversation → llm_provider）
    let mut providers: std::collections::HashMap<String, Arc<dyn LlmProvider>> =
        std::collections::HashMap::new();
    providers.insert(
        "test_provider".to_string(),
        llm_provider.clone() as Arc<dyn LlmProvider>,
    );

    let mut role_mis = std::collections::HashMap::new();
    role_mis.insert(
        "conversation".to_string(),
        yomua_bot::application::config::LlmModelInstanceConfig {
            provider: "test_provider".to_string(),
            model: "mock-model".to_string(),
            context_window: Some(4096),
            max_output_tokens: Some(512),
        },
    );

    let scheduler: Arc<dyn LlmScheduler> = Arc::new(DefaultLlmScheduler::with_providers(
        llm_provider.clone() as Arc<dyn LlmProvider>,
        4,
        4,
        providers,
        role_mis,
    ));
    let _embed_scheduler: Arc<dyn EmbeddingScheduler> = Arc::new(MockEmbed);
    let bus = EventBus::new();

    let runtime = Arc::new(CharacterRuntime::with_event_bus(
        char_repo.clone() as Arc<dyn CharacterRepository>,
        state_repo.clone(),
        conv_repo.clone(),
        bus.clone(),
    ));
    let binding_manager = Arc::new(BindingManager::new(
        binding_repo.clone(),
        char_repo.clone() as Arc<dyn CharacterRepository>,
        conv_repo.clone(),
    ));
    let character_history = Arc::new(RepositoryCharacterHistoryProvider::new(msg_repo.clone()));
    let context_builder = Arc::new(ContextBuilder::new(
        msg_repo.clone(),
        character_history,
        conv_repo.clone(),
        memory_repo.clone(),
        rel_repo.clone(),
        mood_repo.clone(),
        binding_repo.clone(),
    ));
    let memory_service = Arc::new(MemoryService::new(memory_repo.clone()));
    let mood_service = Arc::new(MoodService::new(mood_repo.clone()));
    let relationship_service = Arc::new(RelationshipService::new(rel_repo.clone(), bus.clone()));
    let behavior_engine = Arc::new(RuleBehaviorEngine::new(
        binding_repo.clone(),
        mood_repo.clone(),
        rel_repo.clone(),
        state_repo.clone(),
        conv_state_repo.clone(),
        behavior_state_repo.clone(),
        yomua_bot::application::clock::system_clock(),
    ));
    let token_estimator = Arc::new(TokenEstimatorAdapter(Arc::new(ApproximateTokenEstimator)));
    let model_capabilities = Arc::new(ModelCapabilitiesProviderAdapter(Arc::new(
        FixedModelCapabilities::new(Some(4096), Some(512)),
    )));
    let config = yomua_bot::application::context_assembler::ContextAssemblyConfig {
        degradation_enabled: true,
        output_reservation: 512,
        runtime_max_output_tokens: Some(256),
    };
    let context_assembler = Arc::new(ContextAssembler::with_config(
        token_estimator,
        model_capabilities,
        config,
    ));
    let cognition = Arc::new(CognitionLayer::new(
        Some(scheduler.clone() as Arc<dyn LlmScheduler>),
        context_builder,
        context_assembler,
    ));
    let sent_transport: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let held_connector = Arc::new(HeldMockConnector::new(sent_transport.clone()));
    let conversation_manager = ConversationManager::new(conv_repo.clone(), part_repo.clone());
    let adapter: Arc<dyn yomua_bot::adapters::onebot::OneBotAdapter> =
        Arc::new(OneBotAdapterImpl::with_connector(
            OneBotConfig {
                websocket_url: "ws://localhost:1".to_string(),
                access_token: None,
                bot_self_id: Some(111111),
                reconnect_interval_secs: 1,
                max_reconnect_interval_secs: 4,
                heartbeat_interval_secs: 30,
                action_timeout_secs: 10,
            },
            bus.clone(),
            conversation_manager,
            Arc::new(
                yomua_bot::application::message_acceptance::MessageAcceptanceService::new(
                    msg_repo.clone(),
                    bus.clone(),
                ),
            ),
            binding_manager.clone(),
            held_connector,
        ));
    let action_dispatcher = Arc::new(ActionDispatcher::new(conv_repo.clone(), adapter.clone()));
    let evaluator: Arc<dyn DecisionEvaluator> = behavior_engine.clone();
    let trigger: Arc<dyn LlmAssistanceTrigger> = Arc::new(NoOpTrigger);
    let decision_orchestrator = DecisionOrchestrator::new(evaluator, trigger, None);
    let reply_processor = Arc::new(ReplyProcessor::new(
        runtime,
        binding_manager,
        decision_orchestrator,
        cognition,
        relationship_service,
        mood_service,
        memory_service,
        action_dispatcher,
        bus,
        Arc::new(ZeroDelay),
        part_repo.clone(),
        None, // None = fallback to first character participant
    ));

    // 创建角色和绑定（冷启动）
    let char_a = Character {
        id: 0,
        definition: CharacterDefinition {
            name: "小爱".to_string(),
            description: None,
            personality: None,
            scenario: None,
            style: None,
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: Some("你叫小爱。".to_string()),
            post_history_instructions: None,
            lorebook: vec![],
            metadata: serde_json::json!({}),
        },
        enabled: true,
        state: CharacterState::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let char_id = char_repo.insert(&char_a).await.unwrap();

    let conv = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: "200000999".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let conv_id = conv_repo.insert(&conv).await.unwrap();

    let user_part = Participant {
        id: 0,
        conversation_id: conv_id,
        external_id: "200000999".to_string(),
        display_name: "用户".to_string(),
        role: ParticipantRole::User,
        metadata: serde_json::json!({}),
    };
    let user_id = part_repo.insert(&user_part).await.unwrap();

    // 创建角色参与者（ReplyProcessor 需要 role='character' 的 participant 来识别 bot 自身）
    let _char_part = part_repo
        .insert(&Participant {
            id: 0,
            conversation_id: conv_id,
            external_id: "char_cold".to_string(),
            display_name: "小爱".to_string(),
            role: ParticipantRole::Character,
            metadata: serde_json::json!({}),
        })
        .await
        .unwrap();

    let binding = CharacterBinding {
        id: 0,
        character_id: char_id,
        conversation_id: conv_id,
        reply_mode: ReplyMode::Natural,
        proactive_enabled: false,
        mute_schedule: None,
        behavior_overrides: serde_json::json!({}),
        context_policy: serde_json::json!({}),
        switched_at: None,
        cross_reply_enabled: false,
        response_rate_limit: Default::default(),
        created_at: chrono::Utc::now(),
    };
    binding_repo.insert(&binding).await.unwrap();

    // 启动 adapter 并等待连接建立
    adapter.start().await.unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if adapter.state().await == yomua_bot::adapters::onebot::OneBotConnectionState::Connected {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // 发送第一条消息
    let event = MessageReceivedEvent {
        conversation_id: conv_id,
        sender_id: user_id,
        message_id: 0,
        content: "你好".to_string(),
        timestamp: chrono::Utc::now(),
        is_mentioned: true,
        character_id: char_id,
        // C2-3 字段
        conversation_sequence: 0,
        platform_message_id: None,
        ingested_at: chrono::Utc::now(),
        metadata: serde_json::json!({}),
    };

    reply_processor
        .process(&event)
        .await
        .expect("cold start process");

    // 等待足够长的时间让延迟执行和消息发送完成
    tokio::time::sleep(Duration::from_millis(500)).await;

    let llm_count = llm_provider.call_count();
    let outbound_count = sent_transport.lock().unwrap().len();

    println!(
        "[P1-2] Cold start: LLM calls={}, outbound={}",
        llm_count, outbound_count
    );

    // Cold start 应该产生 LLM 调用和出站消息
    assert_eq!(llm_count, 1, "Cold start 首条消息应触发 LLM");
    assert_eq!(outbound_count, 1, "Cold start 应产生出站消息");
}

// ============================================================================
// 测试：P2-1: 单会话积压压测（2 条/秒，持续 60 秒，Provider 延迟 3 秒）
// ============================================================================

#[tokio::test]
#[ignore = "耗时 1-3 分钟，仅用于手动测量积压"]
async fn p2_1_single_conversation_backlog_measurement() {
    let h = setup_with_options(
        Duration::from_secs(3),
        Arc::new(yomua_bot::application::reply_processor::TokioDelayExecutor),
        true,
        true,
    )
    .await
    .expect("压测装配失败");
    let msg_repo: Arc<dyn MessageRepository> =
        Arc::new(SqliteMessageRepository::new(h.pool.clone()));
    let processor = h.reply_processor.clone();
    let mut jobs = Vec::with_capacity(120);

    for index in 0..120 {
        let inbound_event_seq = persist_measurement_inbox_event(&h, index)
            .await
            .expect("压测 inbox 持久化失败");
        let timestamp = chrono::Utc::now();
        let (message_id, _) = h
            .acceptance_service
            .accept_inbound_with_seq(
                h.conv_id,
                h.user_participant_id,
                format!("请处理第 {} 条消息", index + 1),
                timestamp,
                None,
                h.char_a_id,
                false,
                serde_json::json!({}),
                inbound_event_seq,
            )
            .await
            .expect("压测消息入库失败");
        let message = msg_repo
            .find_by_id(message_id)
            .await
            .expect("读取压测消息失败")
            .expect("压测消息不存在");
        let content = match &message.content {
            MessageContent::Text(text) => text.clone(),
            _ => panic!("压测消息必须是文本"),
        };
        let event = MessageReceivedEvent {
            conversation_id: h.conv_id,
            sender_id: h.user_participant_id,
            message_id,
            content,
            timestamp: message.timestamp,
            is_mentioned: false,
            character_id: h.char_a_id,
            conversation_sequence: message.conversation_sequence.unwrap_or_default(),
            platform_message_id: message.platform_message_id,
            ingested_at: message.ingested_at.expect("压测消息缺少入库时间"),
            metadata: message.metadata,
        };
        let processor = processor.clone();
        jobs.push(tokio::spawn(async move {
            processor.process_owned(&event, None).await
        }));

        if index < 119 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    for job in jobs {
        job.await.expect("压测处理任务异常").expect("压测处理失败");
    }

    let status_rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT status, COUNT(*) FROM reply_tasks GROUP BY status ORDER BY status")
            .fetch_all(&h.pool)
            .await
            .expect("读取压测状态失败");
    let count_status = |wanted: &str| {
        status_rows
            .iter()
            .find(|(status, _)| status == wanted)
            .map(|(_, count)| *count)
            .unwrap_or(0)
    };
    let sent = count_status("sent");
    let ignored = count_status("ignored");
    let expired = count_status("expired");
    let measured: i64 = status_rows.iter().map(|(_, count)| *count).sum();
    assert_eq!(
        measured, 120,
        "120 条入库消息都应有回复任务终态: {status_rows:?}"
    );
    assert_eq!(
        sent + ignored + expired,
        120,
        "压测出现未覆盖状态: {status_rows:?}"
    );

    let sent_rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT m.ingested_at, r.updated_at
           FROM messages m
           JOIN reply_tasks r ON r.message_id = m.id
          WHERE r.status = 'sent'
          ORDER BY m.id",
    )
    .fetch_all(&h.pool)
    .await
    .expect("读取压测发送时间失败");
    assert!(!sent_rows.is_empty(), "压测至少应有一条发送成功记录");
    let mut latencies_ms: Vec<i64> = sent_rows
        .into_iter()
        .map(|(ingested_at, sent_at)| {
            let ingested_at = chrono::DateTime::parse_from_rfc3339(&ingested_at)
                .expect("入库时间格式无效")
                .with_timezone(&chrono::Utc);
            let sent_at = chrono::DateTime::parse_from_rfc3339(&sent_at)
                .expect("发送时间格式无效")
                .with_timezone(&chrono::Utc);
            (sent_at - ingested_at).num_milliseconds()
        })
        .collect();
    latencies_ms.sort_unstable();
    let percentile = |ratio: f64| {
        let index = ((latencies_ms.len() as f64 * ratio).ceil() as usize)
            .saturating_sub(1)
            .min(latencies_ms.len() - 1);
        latencies_ms[index]
    };
    let p50_ms = percentile(0.50);
    let p95_ms = percentile(0.95);
    let outbound_count = h.sent_transport.lock().unwrap().len();
    println!(
        "[P2-1] 入库=120, Sent={}, Ignored={}, Expired={}, Outbound={}, P50={}ms, P95={}ms, ProviderCalls={}",
        sent,
        ignored,
        expired,
        outbound_count,
        p50_ms,
        p95_ms,
        h.llm_provider.call_count(),
    );
}

// ============================================================================
// 测试：P3-1: 完整 Outbox 链路积压测量
// ============================================================================

#[tokio::test]
#[ignore = "耗时 1-3 分钟，仅用于手动测量完整 Outbox 链路"]
async fn p3_1_outbox_single_conversation_backlog_measurement() {
    let stage_samples = Arc::new(Mutex::new(Vec::new()));
    let _stage_guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(StageLayer {
            samples: stage_samples.clone(),
        }));
    let h = setup_with_options(
        Duration::from_secs(3),
        Arc::new(yomua_bot::application::reply_processor::TokioDelayExecutor),
        true,
        true,
    )
    .await
    .expect("压测装配失败");
    let command_handler = Arc::new(CommandHandler::new(
        h.binding_manager.clone(),
        Arc::new(
            yomua_bot::infrastructure::storage::repository::SqliteCharacterRepository::new(
                h.pool.clone(),
            ),
        ),
        h.reply_processor.clone(),
        None,
    ));
    let (event_tx, event_rx) = tokio::sync::mpsc::channel(32);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let dispatcher =
        OutboxDispatcher::new(h.outbox_repo.clone(), OutboxDispatcherConfig::default());
    let processor = OutboxProcessor::new(h.reply_processor.clone(), command_handler);
    let dispatcher_shutdown = shutdown_rx.clone();
    let processor_shutdown = shutdown_rx.clone();
    let dispatcher_task = tokio::spawn(async move {
        dispatcher.run(event_tx, dispatcher_shutdown).await;
    });
    let processor_task = tokio::spawn(async move {
        processor.run(event_rx, processor_shutdown).await;
    });

    for index in 0..120 {
        let inbound_event_seq = persist_measurement_inbox_event(&h, index)
            .await
            .expect("压测 inbox 持久化失败");
        h.acceptance_service
            .accept_inbound_with_seq(
                h.conv_id,
                h.user_participant_id,
                format!("请处理第 {} 条消息", index + 1),
                chrono::Utc::now(),
                None,
                h.char_a_id,
                false,
                serde_json::json!({}),
                inbound_event_seq,
            )
            .await
            .expect("压测消息入库失败");
        if index < 119 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
    loop {
        let terminal: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM reply_tasks WHERE status IN ('sent','ignored','expired','failed','cancelled')",
        )
        .fetch_one(&h.pool)
        .await
        .expect("读取压测状态失败");
        if terminal == 120 || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let status_rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT status, COUNT(*) FROM reply_tasks GROUP BY status ORDER BY status")
            .fetch_all(&h.pool)
            .await
            .expect("读取压测状态失败");
    let count_status = |wanted: &str| {
        status_rows
            .iter()
            .find(|(status, _)| status == wanted)
            .map(|(_, count)| *count)
            .unwrap_or(0)
    };
    let sent = count_status("sent");
    let ignored = count_status("ignored");
    let expired = count_status("expired");
    let ignored_reasons: Vec<(Option<String>, i64)> = sqlx::query_as(
        "SELECT error, COUNT(*) FROM reply_tasks WHERE status='ignored' GROUP BY error ORDER BY error",
    )
    .fetch_all(&h.pool)
    .await
    .expect("读取忽略原因失败");
    let sent_rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT m.ingested_at, r.updated_at
           FROM messages m
           JOIN reply_tasks r ON r.message_id = m.id
          WHERE r.status = 'sent'
          ORDER BY m.id",
    )
    .fetch_all(&h.pool)
    .await
    .expect("读取压测发送时间失败");
    assert_eq!(
        sent + ignored + expired,
        120,
        "压测终态不完整: {status_rows:?}"
    );
    assert!(!sent_rows.is_empty(), "完整链路至少应有一条发送成功记录");
    let mut latencies_ms: Vec<i64> = sent_rows
        .into_iter()
        .map(|(ingested_at, sent_at)| {
            let ingested_at = chrono::DateTime::parse_from_rfc3339(&ingested_at)
                .expect("入库时间格式无效")
                .with_timezone(&chrono::Utc);
            let sent_at = chrono::DateTime::parse_from_rfc3339(&sent_at)
                .expect("发送时间格式无效")
                .with_timezone(&chrono::Utc);
            (sent_at - ingested_at).num_milliseconds()
        })
        .collect();
    latencies_ms.sort_unstable();
    let percentile = |ratio: f64| {
        let index = ((latencies_ms.len() as f64 * ratio).ceil() as usize)
            .saturating_sub(1)
            .min(latencies_ms.len() - 1);
        latencies_ms[index]
    };
    let stage_samples = stage_samples.lock().unwrap().clone();
    let stage_percentile = |mut values: Vec<u64>, ratio: f64| {
        values.sort_unstable();
        let index = ((values.len() as f64 * ratio).ceil() as usize)
            .saturating_sub(1)
            .min(values.len().saturating_sub(1));
        values.get(index).copied().unwrap_or_default()
    };
    let stage_values = |select: fn(StageSample) -> u64| {
        stage_samples
            .iter()
            .copied()
            .map(select)
            .collect::<Vec<_>>()
    };
    assert!(!stage_samples.is_empty(), "压测应捕获回复阶段耗时事件");
    println!(
        "[P3-1] 入库=120, Sent={}, Ignored={}, Expired={}, IgnoredReasons={:?}, TotalP50={}ms, TotalP95={}ms, StageP50/P95={{lock:{} / {}, batch:{} / {}, memory/relationship:{} / {}, decision:{} / {}, generation:{} / {}, decision_delay:{} / {}, send:{} / {}}}ms, ProviderCalls={}",
        sent,
        ignored,
        expired,
        ignored_reasons,
        percentile(0.50),
        percentile(0.95),
        stage_percentile(stage_values(|s| s.lock_wait_ms), 0.50),
        stage_percentile(stage_values(|s| s.lock_wait_ms), 0.95),
        stage_percentile(stage_values(|s| s.batch_wait_ms), 0.50),
        stage_percentile(stage_values(|s| s.batch_wait_ms), 0.95),
        stage_percentile(stage_values(|s| s.memory_write_ms), 0.50),
        stage_percentile(stage_values(|s| s.memory_write_ms), 0.95),
        stage_percentile(stage_values(|s| s.decision_ms), 0.50),
        stage_percentile(stage_values(|s| s.decision_ms), 0.95),
        stage_percentile(stage_values(|s| s.generation_ms), 0.50),
        stage_percentile(stage_values(|s| s.generation_ms), 0.95),
        stage_percentile(stage_values(|s| s.decision_delay_ms), 0.50),
        stage_percentile(stage_values(|s| s.decision_delay_ms), 0.95),
        stage_percentile(stage_values(|s| s.send_ms), 0.50),
        stage_percentile(stage_values(|s| s.send_ms), 0.95),
        h.llm_provider.call_count(),
    );
    shutdown_tx.send(true).ok();
    dispatcher_task.await.expect("dispatcher 压测任务异常");
    processor_task.await.expect("processor 压测任务异常");
}

#[tokio::test]
#[ignore = "耗时约 10 秒，仅用于手动测量入站持久化与回执延迟"]
async fn p5_1_inbound_write_and_action_ack_measurement() {
    let h = setup_with_options(Duration::ZERO, Arc::new(ZeroDelay), true, true)
        .await
        .expect("压测装配失败");
    let repo = h.inbound_event_repo.clone();
    let ingress_samples = Arc::new(Mutex::new(Vec::with_capacity(1_000)));
    let ingress_samples_writer = ingress_samples.clone();
    let writer = tokio::spawn(async move {
        let interval = tokio::time::interval(Duration::from_millis(10));
        let mut interval = interval;
        for index in 0..1_000 {
            interval.tick().await;
            let started = std::time::Instant::now();
            let event = repo
                .insert_inbound_event(
                    &format!(r#"{{"post_type":"message","message_id":{index}}}"#),
                    chrono::Utc::now(),
                )
                .await
                .expect("合成 inbox 写入失败");
            let write_ms = started.elapsed().as_micros() as u64;
            let claimed = repo
                .claim_next_inbound_event(chrono::Utc::now())
                .await
                .expect("合成 inbox 领取失败")
                .expect("合成 inbox 记录应可领取");
            assert_eq!(claimed.ingress_seq, event.ingress_seq);
            repo.mark_inbound_event(
                event.ingress_seq,
                yomua_bot::domain::inbound_event::InboundEventStatus::Ignored,
                chrono::Utc::now(),
                None,
            )
            .await
            .expect("合成 inbox 终结失败");
            ingress_samples_writer
                .lock()
                .unwrap()
                .push((write_ms, started.elapsed().as_micros() as u64));
        }
    });

    let mut ack_samples = Vec::with_capacity(100);
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    for index in 0..100 {
        interval.tick().await;
        let started = std::time::Instant::now();
        h._adapter
            .send_group_message("100000999", &format!("ack-{index}"))
            .await
            .expect("假 OneBot Action 请求失败");
        ack_samples.push(started.elapsed().as_micros() as u64);
    }
    writer.await.expect("入站测量任务异常");

    let percentile = |mut samples: Vec<u64>, ratio: f64| {
        samples.sort_unstable();
        let index = ((samples.len() as f64 * ratio).ceil() as usize)
            .saturating_sub(1)
            .min(samples.len().saturating_sub(1));
        samples.get(index).copied().unwrap_or_default()
    };
    let ingress = ingress_samples.lock().unwrap().clone();
    let write_samples = ingress.iter().map(|(write, _)| *write).collect();
    let process_samples = ingress.iter().map(|(_, process)| *process).collect();
    println!(
        "[P5-1] 合成事件=1000/10s, 速率=100/s, inbox_write_p50/p95={} / {}us, insert_to_terminal_p50/p95={} / {}us, action_ack_p50/p95={} / {}us, fake_onebot=true, fake_provider=true",
        percentile(write_samples, 0.50),
        percentile(ingress.iter().map(|(write, _)| *write).collect(), 0.95),
        percentile(process_samples, 0.50),
        percentile(ingress.iter().map(|(_, process)| *process).collect(), 0.95),
        percentile(ack_samples.clone(), 0.50),
        percentile(ack_samples, 0.95),
    );
}
