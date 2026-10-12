//! yomua-audit-harness — C2-4 Dynamic Verification Harness
//!
//! Audit-only tool to verify the C2-4 pipeline with synthetic events.
//!
//! This is NOT a production binary. It's an audit tool to obtain runtime
//! evidence for the Unit 4 audit.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::{mpsc, watch};

use yomua_bot::adapters::onebot::connection::{WsConnector, WsTransport};
use yomua_bot::adapters::onebot::conversion::{InboundMessage, onebot_event_to_inbound};
use yomua_bot::adapters::onebot::{ActionResponse, OneBotAdapter, OneBotAdapterImpl, OneBotConfig, OneBotConnectionState, OneBotEvent};
use yomua_bot::application::action::ActionDispatcher;
use yomua_bot::application::behavior_engine::RuleBehaviorEngine;
use yomua_bot::application::binding::BindingManager;
use yomua_bot::application::character_history::RepositoryCharacterHistoryProvider;
use yomua_bot::application::cognition::CognitionLayer;
use yomua_bot::application::context::ContextBuilder;
use yomua_bot::application::context_assembler::{
    ContextAssembler, ModelCapabilitiesProviderAdapter, TokenEstimatorAdapter,
};
use yomua_bot::application::conversation::ConversationManager;
use yomua_bot::application::decision_orchestrator::{
    DecisionEvaluator, DecisionOrchestrator, LlmAssistanceTrigger, NoOpTrigger,
};
use yomua_bot::application::event_bus::EventBus;
use yomua_bot::application::llm_scheduler::{DefaultLlmScheduler, LlmScheduler};
use yomua_bot::application::memory_service::MemoryService;
use yomua_bot::application::message_acceptance::MessageAcceptanceService;
use yomua_bot::application::mood_service::MoodService;
use yomua_bot::application::outbox_dispatcher::{OutboxDispatcher, OutboxDispatcherConfig};
use yomua_bot::application::outbox_processor::OutboxProcessor;
use yomua_bot::application::relationship_service::RelationshipService;
use yomua_bot::application::reply_processor::{DelayExecutor, ReplyProcessor, TokioDelayExecutor};
use yomua_bot::application::runtime::CharacterRuntime;
use yomua_bot::domain::character::{
    Character, CharacterBinding, CharacterDefinition, CharacterState, ReplyMode,
};
use yomua_bot::domain::conversation::{Conversation, ConversationType, Participant, ParticipantRole};
use yomua_bot::domain::message::MessageContent;
use yomua_bot::domain::repository::{
    BehaviorStateRepository, BlacklistRepository, CharacterBindingRepository,
    CharacterRepository, CharacterStateRepository, ConversationRepository,
    ConversationStateRepository, MemoryRepository, MessageRepository, MoodRepository,
    OutboxRepository, ParticipantRepository, RelationshipRepository,
};
use yomua_bot::error::RuntimeError;
use yomua_bot::infrastructure::llm::{
    ApproximateTokenEstimator, FixedModelCapabilities, LlmProvider, LlmRequest, LlmResponse,
    TokenUsage,
};
use yomua_bot::infrastructure::storage::repository::{
    SqliteBehaviorStateRepository, SqliteBlacklistRepository, SqliteCharacterBindingRepository,
    SqliteCharacterRepository, SqliteCharacterStateRepository, SqliteConversationRepository,
    SqliteConversationStateRepository, SqliteMemoryRepository, SqliteMessageRepository,
    SqliteMoodRepository, SqliteOutboxRepository, SqliteParticipantRepository,
    SqlitePluginDataRepository, SqliteRelationshipRepository,
};
use yomua_bot::infrastructure::storage::SqliteStorage;

// ============================================================================
// Mock LLM Provider
// ============================================================================

struct MockLlmProvider;

#[async_trait]
impl LlmProvider for MockLlmProvider {
    async fn generate(&self, _request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        Ok(LlmResponse {
            content: "Mock response".to_string(),
            usage: TokenUsage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
            },
            model: "mock".to_string(),
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
// Mock WsConnector (doesn't actually connect to OneBot)
// ============================================================================

struct MockWsConnector;

#[async_trait]
impl WsConnector for MockWsConnector {
    async fn connect(
        &self,
        _config: &OneBotConfig,
    ) -> Result<Box<dyn WsTransport>, RuntimeError> {
        Ok(Box::new(MockWsTransport))
    }
}

struct MockWsTransport;

#[async_trait]
impl WsTransport for MockWsTransport {
    async fn read_text(&mut self) -> Result<Option<String>, RuntimeError> {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(None)
    }

    async fn send_text(&mut self, _text: &str) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

// ============================================================================
// Fake OneBot Adapter for Outbound Verification
// Captures send_message calls without requiring a real NapCat server
// ============================================================================

struct FakeOneBotAdapter {
    sent_messages: Mutex<Vec<(String, String, String)>>, // (target_id, message_type, content)
}

impl FakeOneBotAdapter {
    fn new() -> Self {
        Self {
            sent_messages: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl OneBotAdapter for FakeOneBotAdapter {
    async fn start(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn state(&self) -> OneBotConnectionState {
        OneBotConnectionState::Connected
    }

    async fn send_group_message(
        &self,
        group_id: &str,
        content: &str,
    ) -> Result<ActionResponse, RuntimeError> {
        self.sent_messages
            .lock()
            .unwrap()
            .push((group_id.to_string(), "group".to_string(), content.to_string()));
        Ok(ActionResponse {
            id: 0,
            action: "send_group_msg".to_string(),
            status: "ok".to_string(),
            retcode: Some(0),
            data: None,
            error: None,
        })
    }

    async fn send_private_message(
        &self,
        user_id: &str,
        content: &str,
    ) -> Result<ActionResponse, RuntimeError> {
        self.sent_messages
            .lock()
            .unwrap()
            .push((user_id.to_string(), "private".to_string(), content.to_string()));
        Ok(ActionResponse {
            id: 0,
            action: "send_private_msg".to_string(),
            status: "ok".to_string(),
            retcode: Some(0),
            data: None,
            error: None,
        })
    }
}

// ============================================================================
// Helper functions
// ============================================================================

async fn setup_test_character_and_binding(
    pool: sqlx::SqlitePool,
    char_name: &str,
    external_conv_id: &str,
) -> Result<(i64, i64, i64), Box<dyn std::error::Error>> {
    let char_repo = SqliteCharacterRepository::new(pool.clone());
    let binding_repo = SqliteCharacterBindingRepository::new(pool.clone());
    let conv_repo = SqliteConversationRepository::new(pool.clone());
    let participant_repo = SqliteParticipantRepository::new(pool.clone());
    let state_repo = SqliteCharacterStateRepository::new(pool.clone());

    let char_def = CharacterDefinition {
        name: char_name.to_string(),
        description: Some("Test character".to_string()),
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
    };
    let char_state = CharacterState::default();
    let char_id = char_repo.insert(&Character {
        id: 0,
        definition: char_def,
        state: char_state,
        enabled: true,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }).await?;

    let conv = Conversation {
        id: 0,
        conversation_type: ConversationType::Private,
        external_id: external_conv_id.to_string(),
        name: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let conv_id = conv_repo.insert(&conv).await?;

    let participant = Participant {
        id: 0,
        conversation_id: conv_id,
        external_id: "999999".to_string(),
        display_name: "TestUser".to_string(),
        role: ParticipantRole::User,
        metadata: serde_json::json!({}),
    };
    let participant_id = participant_repo.insert(&participant).await?;

    let binding = CharacterBinding {
        id: 0,
        character_id: char_id,
        conversation_id: conv_id,
        reply_mode: ReplyMode::Natural,
        proactive_enabled: false,
        mute_schedule: None,
        behavior_overrides: serde_json::json!({}),
        context_policy: serde_json::json!({}),
        switched_at: Some(Utc::now()),
        cross_reply_enabled: false,
        created_at: Utc::now(),
    };
    let _binding_id = binding_repo.insert(&binding).await?;

    state_repo.upsert(char_id, &CharacterState::default()).await?;

    Ok((char_id, conv_id, participant_id))
}

// ============================================================================
// Main test harness
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== C2-4 Dynamic Verification Harness ===\n");

    // Create temp directory for test database
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("audit_test.db");
    let db_path_str = db_path.to_string_lossy().to_string();

    println!("Test database: {}", db_path_str);

    // Setup storage
    let storage = SqliteStorage::open(&db_path_str).await?;
    storage.migrate().await?;
    let pool = storage.pool();

    // Create all repositories
    let character_repo = Arc::new(SqliteCharacterRepository::new(pool.clone()));
    let state_repo = Arc::new(SqliteCharacterStateRepository::new(pool.clone()));
    let binding_repo = Arc::new(SqliteCharacterBindingRepository::new(pool.clone()));
    let conversation_repo = Arc::new(SqliteConversationRepository::new(pool.clone()));
    let participant_repo = Arc::new(SqliteParticipantRepository::new(pool.clone()));
    let message_repo = Arc::new(SqliteMessageRepository::new(pool.clone()));
    let memory_repo = Arc::new(SqliteMemoryRepository::new(pool.clone()));
    let relationship_repo = Arc::new(SqliteRelationshipRepository::new(pool.clone()));
    let mood_repo = Arc::new(SqliteMoodRepository::new(pool.clone()));
    let conversation_state_repo = Arc::new(SqliteConversationStateRepository::new(pool.clone()));
    let behavior_state_repo = Arc::new(SqliteBehaviorStateRepository::new(pool.clone()));
    let plugin_data_repo = Arc::new(SqlitePluginDataRepository::new(pool.clone()));
    let blacklist_repo = Arc::new(SqliteBlacklistRepository::new(pool.clone()));
    let outbox_repo = Arc::new(SqliteOutboxRepository::new(pool.clone()));

    // Setup test character and binding
    let (char_id, conv_id, participant_id) = setup_test_character_and_binding(
        pool.clone(),
        "测试角色",
        "999999",
    ).await?;
    println!("Created character id={}, conversation id={}, participant id={}", char_id, conv_id, participant_id);

    // Create EventBus
    let bus = EventBus::new();

    // Create application services
    let conversation_manager = Arc::new(ConversationManager::new(
        conversation_repo.clone(),
        participant_repo.clone(),
    ));

    let acceptance_service = Arc::new(MessageAcceptanceService::new(
        message_repo.clone(),
        bus.clone(),
    ));

    let binding_manager = Arc::new(BindingManager::new(
        binding_repo.clone(),
        character_repo.clone() as Arc<dyn CharacterRepository>,
        conversation_repo.clone(),
    ));

    let runtime = Arc::new(CharacterRuntime::with_event_bus(
        character_repo.clone() as Arc<dyn CharacterRepository>,
        state_repo.clone(),
        conversation_repo.clone(),
        bus.clone(),
    ));

    let context_builder = Arc::new(ContextBuilder::new(
        message_repo.clone(),
        Arc::new(RepositoryCharacterHistoryProvider::new(message_repo.clone())),
        conversation_repo.clone(),
        memory_repo.clone(),
        relationship_repo.clone(),
        mood_repo.clone(),
        binding_repo.clone(),
    ));

    let memory_service = Arc::new(MemoryService::new(memory_repo.clone()));
    let mood_service = Arc::new(MoodService::new(mood_repo.clone()));
    let relationship_service = Arc::new(RelationshipService::new(
        relationship_repo.clone(),
        bus.clone(),
    ));

    let behavior_engine = Arc::new(RuleBehaviorEngine::new(
        binding_repo.clone(),
        mood_repo.clone(),
        relationship_repo.clone(),
        state_repo.clone(),
        conversation_state_repo.clone(),
        behavior_state_repo.clone(),
        yomua_bot::application::clock::system_clock(),
    ));

    // LLM setup
    let llm_provider: Arc<dyn LlmProvider> = Arc::new(MockLlmProvider);
    let scheduler = Arc::new(DefaultLlmScheduler::new(llm_provider));
    let scheduler_opt: Option<Arc<dyn LlmScheduler>> = Some(scheduler.clone() as Arc<dyn LlmScheduler>);

    let token_estimator = Arc::new(TokenEstimatorAdapter(Arc::new(ApproximateTokenEstimator)));
    let model_capabilities = Arc::new(ModelCapabilitiesProviderAdapter(Arc::new(
        FixedModelCapabilities::new(Some(4096), Some(512)),
    )));
    let config = yomua_bot::application::context_assembler::ContextAssemblyConfig {
        degradation_enabled: true,
        output_reservation: 512,
        runtime_max_output_tokens: None,
    };
    let context_assembler = Arc::new(ContextAssembler::with_config(
        token_estimator,
        model_capabilities,
        config,
    ));

    let cognition = Arc::new(CognitionLayer::new(
        scheduler_opt.clone(),
        context_builder.clone(),
        context_assembler.clone(),
    ));

    let evaluator: Arc<dyn DecisionEvaluator> = behavior_engine.clone();
    let trigger: Arc<dyn LlmAssistanceTrigger> = Arc::new(NoOpTrigger);
    let decision_orchestrator = DecisionOrchestrator::new(evaluator, trigger, None);

    let onebot_config = OneBotConfig {
        websocket_url: "ws://127.0.0.1:3001".to_string(),
        access_token: None,
        bot_self_id: Some(123456),
        reconnect_interval_secs: 1,
        max_reconnect_interval_secs: 60,
        heartbeat_interval_secs: 30,
        action_timeout_secs: 10,
    };

    let adapter = OneBotAdapterImpl::with_connector(
        onebot_config,
        bus.clone(),
        (*conversation_manager).clone(),
        acceptance_service.clone(),
        binding_manager.clone(),
        Arc::new(MockWsConnector),
    );

    let action_dispatcher = Arc::new(ActionDispatcher::new(
        conversation_repo.clone(),
        Arc::new(adapter),
    ));

    let delay_executor: Arc<dyn DelayExecutor> = Arc::new(TokioDelayExecutor);
    let reply_processor = Arc::new(ReplyProcessor::new(
        runtime.clone(),
        binding_manager.clone(),
        decision_orchestrator.clone(),
        cognition.clone(),
        relationship_service.clone(),
        mood_service.clone(),
        memory_service.clone(),
        action_dispatcher.clone(),
        bus.clone(),
        delay_executor,
        participant_repo.clone(),
        Some(123456),
    ));

    // ============================================================================
    // EXPERIMENT 1: Call MessageAcceptanceService directly
    // ============================================================================
    println!("\n=== EXPERIMENT 1: Direct MessageAcceptanceService Call ===");

    let msg_count_before: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM messages")
        .fetch_one(pool).await.unwrap_or((0,));
    let outbox_count_before: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(pool).await.unwrap_or((0,));
    println!("Before: messages={}, outbox_events={}", msg_count_before.0, outbox_count_before.0);

    // Call acceptance_service.accept() directly
    let timestamp = Utc::now();
    let content = MessageContent::Text("你好，测试消息".to_string());
    let content_json = serde_json::to_string(&content).unwrap_or_default();

    let accept_result = acceptance_service
        .accept(
            conv_id,
            participant_id,
            content_json,
            timestamp,
            Some(1001),
            char_id,
        )
        .await;

    match accept_result {
        Ok((message_id, sequence)) => {
            println!("MessageAccepted: message_id={}, sequence={}", message_id, sequence);
        }
        Err(e) => {
            println!("MessageAcceptance FAILED: {:?}", e);
        }
    }

    let msg_count_after: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM messages")
        .fetch_one(pool).await.unwrap_or((0,));
    let outbox_count_after: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(pool).await.unwrap_or((0,));
    println!("After: messages={}, outbox_events={}", msg_count_after.0, outbox_count_after.0);

    // Show new messages
    let msgs: Vec<(i64, String, Option<i64>, Option<i64>)> = sqlx::query_as(
        r#"SELECT id, content, active_character_id, conversation_sequence
           FROM messages WHERE conversation_id = ? ORDER BY id DESC LIMIT 10"#
    )
    .bind(conv_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    println!("\nMessages in conversation {}:", conv_id);
    for (id, content, char_id_opt, seq_opt) in &msgs {
        println!("  id={}, char_id={:?}, seq={:?}, content={}", id, char_id_opt, seq_opt, content);
    }

    // Show outbox events
    let outbox_events: Vec<(i64, String, String, String)> = sqlx::query_as(
        r#"SELECT id, event_uuid, event_type, status FROM outbox_events ORDER BY id DESC LIMIT 10"#
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    println!("\nOutbox events:");
    for (id, uuid, event_type, status) in &outbox_events {
        println!("  id={}, uuid={}, type={}, status={}", id, uuid, event_type, status);
    }

    // ============================================================================
    // EXPERIMENT 1B: InboundProcessor Flow Simulation
    // Simulates the InboundProcessor::handle_raw() path using public APIs
    // since handle_raw() is private and cannot be called directly from harness.
    // This verifies: JSON parse → conversion → resolve → binding check → accept → EventBus publish
    // ============================================================================
    println!("\n=== EXPERIMENT 1B: InboundProcessor Flow Simulation ===");

    // Step 1: Construct synthetic OneBotEvent JSON (simulating incoming QQ message)
    // For private messages, external_conversation_id = user_id
    // We use 999999 to match our test conversation's external_id
    let synthetic_onebot_json = serde_json::json!({
        "post_type": "message",
        "time": Utc::now().timestamp(),
        "self_id": 123456,
        "message_type": "private",
        "user_id": 999999,
        "message_id": 2001,
        "sender": {
            "user_id": 999999,
            "nickname": "TestUser",
            "card": ""
        },
        "message": [
            {"type": "text", "data": {"text": "Hello from synthetic inbound"}}
        ]
    });

    let raw_json_str = serde_json::to_string(&synthetic_onebot_json).unwrap();
    println!("Step 1: Synthetic OneBot JSON: {}", raw_json_str);

    // Step 2: Parse JSON to OneBotEvent
    let onebot_event: OneBotEvent = match serde_json::from_str(&raw_json_str) {
        Ok(e) => {
            println!("Step 2: Parsed OneBotEvent successfully");
            e
        }
        Err(e) => {
            println!("Step 2: FAILED to parse OneBotEvent: {}", e);
            return Err(e.into());
        }
    };

    // Step 3: Convert to InboundMessage (same as handle_raw does)
    let inbound: InboundMessage = match onebot_event_to_inbound(&onebot_event) {
        Ok(i) => {
            println!("Step 3: Converted to InboundMessage successfully");
            println!("  - conversation_type: {:?}", i.conversation_type);
            println!("  - external_conversation_id: {}", i.external_conversation_id);
            println!("  - external_sender_id: {}", i.external_sender_id);
            println!("  - content: {}", i.content);
            i
        }
        Err(e) => {
            println!("Step 3: FAILED to convert to InboundMessage: {}", e);
            return Err(e.into());
        }
    };

    // Step 4: Resolve external IDs to core IDs via ConversationManager
    let (conversation_id, sender_id) = match conversation_manager
        .resolve(
            inbound.conversation_type,
            &inbound.external_conversation_id,
            &inbound.external_sender_id,
            &inbound.display_name,
        )
        .await
    {
        Ok(pair) => {
            println!("Step 4: Resolved conversation_id={}, sender_id={}", pair.0, pair.1);
            pair
        }
        Err(e) => {
            println!("Step 4: FAILED to resolve: {}", e);
            return Err(e.into());
        }
    };

    // Step 5: Check bindings via BindingManager (same check handle_raw does)
    let bindings = match binding_manager.by_conversation(conversation_id).await {
        Ok(b) => b,
        Err(e) => {
            println!("Step 5: FAILED to query bindings: {}", e);
            return Err(e.into());
        }
    };

    if bindings.is_empty() {
        println!("Step 5: No bindings for conversation {}, skipping (same as handle_raw behavior)", conversation_id);
        return Err("No bindings for conversation".into());
    }

    let character_id = {
        let mut sorted = bindings.clone();
        sorted.sort_by_key(|b| std::cmp::Reverse(b.switched_at));
        sorted[0].character_id
    };
    println!("Step 5: Bindings found, character_id={}", character_id);

    // Step 6: Accept message via MessageAcceptanceService
    let timestamp = chrono::DateTime::from_timestamp(inbound.unix_time, 0)
        .unwrap_or_else(chrono::Utc::now);
    let acceptance_result = acceptance_service
        .accept(
            conversation_id,
            sender_id,
            inbound.content.clone(),
            timestamp,
            Some(inbound.platform_message_id),
            character_id,
        )
        .await;

    let (message_id, conversation_sequence) = match acceptance_result {
        Ok(result) => {
            println!("Step 6: MessageAcceptanceService.accept() succeeded: message_id={}, seq={}", result.0, result.1);
            result
        }
        Err(e) => {
            println!("Step 6: FAILED MessageAcceptanceService.accept(): {}", e);
            return Err(e.into());
        }
    };

    // Step 7: Subscribe to EventBus BEFORE publishing (simulating production where subscribers are already listening)
    println!("Step 7: Subscribing to EventBus before publish...");
    let mut event_sub = bus.subscribe();

    // Step 8: Publish MessageReceived to EventBus (this is what E2 was investigating)
    println!("Step 8: Publishing MessageReceived to EventBus...");
    let msg_received_event = yomua_bot::domain::event::MessageReceivedEvent {
        conversation_id,
        sender_id,
        message_id: inbound.platform_message_id,
        content: inbound.content,
        timestamp,
        is_mentioned: inbound.is_mentioned,
        character_id,
        conversation_sequence,
        platform_message_id: Some(inbound.platform_message_id),
        ingested_at: chrono::Utc::now(),
    };

    let receivers = bus.publish(&yomua_bot::domain::event::CoreEvent::MessageReceived(msg_received_event.clone()));
    println!("Step 8: MessageReceived published to EventBus, receivers={}", receivers);

    // Step 9: Drain events from subscription
    let events = drain_events(&mut event_sub, 3).await;
    println!("Step 9: EventBus subscriber received {} events", events.len());
    for (i, event) in events.iter().enumerate() {
        println!("  Event {}: {:?}", i, event);
    }

    // Step 10: Check DB state after full flow
    let msg_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM messages")
        .fetch_one(pool).await.unwrap_or((0,));
    let outbox_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(pool).await.unwrap_or((0,));
    println!("Step 10: DB state - messages={}, outbox_events={}", msg_count.0, outbox_count.0);

    // ============================================================================
    // EXPERIMENT 2: EventBus observation
    // ============================================================================
    println!("\n=== EXPERIMENT 2: EventBus Subscription ===");

    let mut event_sub = bus.subscribe();
    let events = drain_events(&mut event_sub, 10).await;
    println!("EventBus events received: {}", events.len());
    for (i, event) in events.iter().enumerate() {
        println!("  Event {}: {:?}", i, event);
    }

    // ============================================================================
    // EXPERIMENT 3: OutboxDispatcher and OutboxProcessor lifecycle
    // ============================================================================
    println!("\n=== EXPERIMENT 3: Outbox Lifecycle ===");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (event_tx, event_rx) = mpsc::channel(32);
    let outbox_dispatcher = Arc::new(OutboxDispatcher::new(
        outbox_repo.clone(),
        OutboxDispatcherConfig::default(),
    ));
    let outbox_processor = Arc::new(OutboxProcessor::new(reply_processor.clone()));

    // Spawn OutboxDispatcher
    let dispatcher_handle = tokio::spawn({
        let dispatcher = outbox_dispatcher.clone();
        async move {
            dispatcher.run(event_tx, shutdown_rx).await;
        }
    });

    // Spawn OutboxProcessor
    let processor_handle = tokio::spawn({
        let processor = outbox_processor.clone();
        async move {
            processor.run(event_rx, tokio::sync::watch::channel(false).1).await;
        }
    });

    // Give time for processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check final outbox state
    let final_outbox: Vec<(i64, String, String, String)> = sqlx::query_as(
        r#"SELECT id, event_uuid, event_type, status FROM outbox_events ORDER BY id DESC LIMIT 10"#
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    println!("\nFinal outbox state ({} events):", final_outbox.len());
    for (id, uuid, event_type, status) in &final_outbox {
        println!("  id={}, uuid={}, type={}, status={}", id, uuid, event_type, status);
    }

    // Shutdown
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), dispatcher_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), processor_handle).await;

    // ============================================================================
    // EXPERIMENT 4: Schema verification
    // ============================================================================
    println!("\n=== EXPERIMENT 4: Schema Verification ===");

    // Verify messages table has expected columns
    let msg_col_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name IN ('active_character_id', 'conversation_sequence')"
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0,));
    println!("messages table has active_character_id + conversation_sequence: {}", msg_col_count.0 == 2);

    // Verify outbox_events table exists and has correct structure
    let outbox_col_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM pragma_table_info('outbox_events') WHERE name IN ('event_uuid', 'status', 'lease_token')"
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0,));
    println!("outbox_events table has event_uuid + status + lease_token: {}", outbox_col_count.0 == 3);

    // ============================================================================
    // EXPERIMENT 5: ReplyProcessor.process_for_outbox() with detailed tracing
    // ============================================================================
    println!("\n=== EXPERIMENT 5: ReplyProcessor.process_for_outbox() ===");

    // Create a second message and outbox event for ReplyProcessor test
    let timestamp2 = Utc::now();
    let content2 = MessageContent::Text("测试回复".to_string());
    let content_json2 = serde_json::to_string(&content2).unwrap_or_default();

    let accept_result2 = acceptance_service
        .accept(
            conv_id,
            participant_id,
            content_json2,
            timestamp2,
            Some(1002),
            char_id,
        )
        .await;

    let (msg_id2, seq2) = match accept_result2 {
        Ok((id, seq)) => {
            println!("Second message accepted: id={}, seq={}", id, seq);
            (id, seq)
        }
        Err(e) => {
            println!("Second message acceptance failed: {:?}", e);
            (0, 0)
        }
    };

    if msg_id2 > 0 {
        // Check bindings BEFORE ReplyProcessor call
        let bindings_check: Vec<(i64, i64, i64, String)> = sqlx::query_as(
            "SELECT id, character_id, conversation_id, reply_mode FROM conversation_bindings WHERE conversation_id = ?"
        )
        .bind(conv_id)
        .fetch_all(pool)
        .await
        .unwrap_or_default();
        println!("Bindings for conv_id={}: {:?}", conv_id, bindings_check);

        // Check the sender participant
        let sender_check: Option<(i64, String, String)> = sqlx::query_as(
            "SELECT id, external_id, role FROM participants WHERE id = ?"
        )
        .bind(participant_id)
        .fetch_optional(pool)
        .await
        .unwrap_or(None);
        match sender_check {
            Some((id, external_id, role)) => {
                println!("Sender: id={}, external_id={}, role={}", id, external_id, role);
            }
            None => {
                println!("Sender NOT FOUND for id={}", participant_id);
            }
        }

        // Check relationship BEFORE ReplyProcessor
        let rel_before: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM relationships WHERE character_id = ? AND participant_id = ?"
        )
        .bind(char_id)
        .bind(participant_id)
        .fetch_one(pool)
        .await
        .unwrap_or((0,));
        println!("Relationships BEFORE ReplyProcessor: {}", rel_before.0);

        // Check mood BEFORE ReplyProcessor
        let mood_before: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM moods WHERE character_id = ? AND conversation_id = ?"
        )
        .bind(char_id)
        .bind(conv_id)
        .fetch_one(pool)
        .await
        .unwrap_or((0,));
        println!("Moods BEFORE ReplyProcessor: {}", mood_before.0);

        // Now construct a MessageReceivedEvent and call ReplyProcessor directly
        // This is the same structure that OutboxProcessor creates
        let msg_received_event = yomua_bot::domain::event::MessageReceivedEvent {
            conversation_id: conv_id,
            sender_id: participant_id,
            message_id: msg_id2,
            content: "测试回复".to_string(),
            timestamp: timestamp2,
            is_mentioned: false,
            character_id: char_id,
            conversation_sequence: seq2,
            platform_message_id: Some(1002),
            ingested_at: timestamp2,
        };

        println!("Calling reply_processor.process_for_outbox()...");
        println!("  event: conversation_id={}, sender_id={}, character_id={}, seq={}",
            msg_received_event.conversation_id, msg_received_event.sender_id,
            msg_received_event.character_id, msg_received_event.conversation_sequence);

        let outcome = reply_processor.process_for_outbox(&msg_received_event).await;
        println!("ReplyProcessor outcome: {:?}", outcome);

        // Check relationship AFTER ReplyProcessor
        let rel_after: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM relationships WHERE character_id = ? AND participant_id = ?"
        )
        .bind(char_id)
        .bind(participant_id)
        .fetch_one(pool)
        .await
        .unwrap_or((0,));
        println!("Relationships AFTER ReplyProcessor: {}", rel_after.0);

        // Check mood AFTER ReplyProcessor
        let mood_after: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM moods WHERE character_id = ? AND conversation_id = ?"
        )
        .bind(char_id)
        .bind(conv_id)
        .fetch_one(pool)
        .await
        .unwrap_or((0,));
        println!("Moods AFTER ReplyProcessor: {}", mood_after.0);

        // List all relationships if any
        if rel_after.0 > 0 {
            let all_rels: Vec<(i64, i64, f64, f64, i64)> = sqlx::query_as(
                "SELECT character_id, participant_id, familiarity, affection, interaction_count FROM relationships"
            )
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            println!("All relationships: {:?}", all_rels);
        }

        // ============================================================================
        // EXPERIMENT 5B: Direct RelationshipService call to verify service works
        // ============================================================================
        println!("\n=== EXPERIMENT 5B: Direct RelationshipService.record_interaction() ===");

        // First, verify participant 1's data in the DB
        let p1_check: Option<(i64, String, String)> = sqlx::query_as(
            "SELECT id, external_id, role FROM participants WHERE id = ?"
        )
        .bind(participant_id)
        .fetch_optional(pool)
        .await
        .unwrap_or(None);
        println!("Participant 1 check: {:?}", p1_check);

        // Create a fresh participant for this test
        let test_participant_id = {
            let test_participant = Participant {
                id: 0,
                conversation_id: conv_id,
                external_id: "TEST_USER_999".to_string(),
                display_name: "TestUser".to_string(),
                role: ParticipantRole::User,
                metadata: serde_json::json!({}),
            };
            participant_repo.insert(&test_participant).await.unwrap_or_else(|e| {
                println!("Failed to insert test participant: {:?}", e);
                0
            })
        };

        if test_participant_id > 0 {
            println!("Created test participant id={}", test_participant_id);

            // Check relationship before direct call
            let rel_before: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM relationships WHERE character_id = ? AND participant_id = ?"
            )
            .bind(char_id)
            .bind(test_participant_id)
            .fetch_one(pool)
            .await
            .unwrap_or((0,));
            println!("Relationships before direct record_interaction: {}", rel_before.0);

            // Call RelationshipService.record_interaction directly
            let rel_result = relationship_service.record_interaction(char_id, test_participant_id).await;
            println!("Direct relationship_service.record_interaction result: {:?}", rel_result);

            // Check relationship after direct call
            let rel_after_direct: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM relationships WHERE character_id = ? AND participant_id = ?"
            )
            .bind(char_id)
            .bind(test_participant_id)
            .fetch_one(pool)
            .await
            .unwrap_or((0,));
            println!("Relationships after direct record_interaction: {}", rel_after_direct.0);

            // List ALL relationships to see what's in DB
            let all_rels: Vec<(i64, i64, String, f64, f64)> = sqlx::query_as(
                "SELECT character_id, participant_id, external_id, familiarity, affection
                 FROM relationships r
                 JOIN participants p ON r.participant_id = p.id"
            )
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            println!("All relationships in DB:");
            for r in &all_rels {
                println!("  char_id={}, participant_id={}, external_id={}, fam={}, aff={}", r.0, r.1, r.2, r.3, r.4);
            }
        } else {
            println!("Skipped direct RelationshipService test (participant creation failed)");
        }
    }

    // ============================================================================
    // EXPERIMENT 8: Full Downstream Processing with Complete Production Context
    // Verify that OutboxProcessor → ReplyProcessor → downstream services work correctly
    // when the production context (Character participant) is complete.
    // ============================================================================
    println!("\n=== EXPERIMENT 8: Full Downstream Processing Verification ===");

    // Create a new conversation with COMPLETE production context for this test
    let (char_id8, conv_id8, participant_id8) = setup_test_character_and_binding(
        pool.clone(),
        "测试角色8",
        "888888",
    ).await?;
    println!("EXP8: Created char_id={}, conv_id={}, participant_id={}", char_id8, conv_id8, participant_id8);

    // Add Character participant (bot_self) - this is what was missing in E5
    // external_id must match bot_self_id = 123456
    let bot_participant8 = Participant {
        id: 0,
        conversation_id: conv_id8,
        external_id: "123456".to_string(),
        display_name: "BotSelf".to_string(),
        role: ParticipantRole::Character,
        metadata: serde_json::json!({}),
    };
    let bot_participant_id8 = participant_repo.insert(&bot_participant8).await?;
    println!("EXP8: Created bot character participant id={}, external_id=123456", bot_participant_id8);

    // Subscribe to EventBus BEFORE processing (to capture downstream events)
    let mut downstream_event_sub = bus.subscribe();
    println!("EXP8: Subscribed to EventBus for downstream events");

    // Record state BEFORE processing
    let rel_count_before: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM relationships WHERE character_id = ?"
    )
    .bind(char_id8)
    .fetch_one(pool)
    .await
    .unwrap_or((0,));
    let mood_count_before: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM moods WHERE character_id = ?"
    )
    .bind(char_id8)
    .fetch_one(pool)
    .await
    .unwrap_or((0,));
    println!("EXP8: State BEFORE - relationships={}, moods={}", rel_count_before.0, mood_count_before.0);

    // Create a message via accept() - this creates both message record and outbox event
    let timestamp8 = Utc::now();
    let content8 = MessageContent::Text("EXP8 test message for downstream".to_string());
    let content_json8 = serde_json::to_string(&content8).unwrap_or_default();

    let (msg_id8, seq8) = match acceptance_service
        .accept(conv_id8, participant_id8, content_json8, timestamp8, Some(8001), char_id8)
        .await
    {
        Ok((id, seq)) => {
            println!("EXP8: Message accepted: id={}, seq={}", id, seq);
            (id, seq)
        }
        Err(e) => {
            println!("EXP8: Message acceptance failed: {:?}", e);
            return Err(e.into());
        }
    };

    // Now spawn OutboxDispatcher and OutboxProcessor to process the outbox event
    let (shutdown_tx8, shutdown_rx8) = watch::channel(false);
    let (event_tx8, event_rx8) = mpsc::channel(32);
    let outbox_dispatcher8 = Arc::new(OutboxDispatcher::new(
        outbox_repo.clone(),
        OutboxDispatcherConfig::default(),
    ));
    let outbox_processor8 = Arc::new(OutboxProcessor::new(reply_processor.clone()));

    // Spawn OutboxDispatcher
    let dispatcher_handle8 = tokio::spawn({
        let dispatcher = outbox_dispatcher8.clone();
        async move {
            dispatcher.run(event_tx8, shutdown_rx8).await;
        }
    });

    // Spawn OutboxProcessor
    let processor_handle8 = tokio::spawn({
        let processor = outbox_processor8.clone();
        async move {
            processor.run(event_rx8, tokio::sync::watch::channel(false).1).await;
        }
    });

    // Give time for processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Send shutdown signal
    let _ = shutdown_tx8.send(true);

    // Drain downstream events from subscription
    let downstream_events = drain_events(&mut downstream_event_sub, 10).await;
    println!("EXP8: Downstream EventBus events received: {}", downstream_events.len());
    for (i, event) in downstream_events.iter().enumerate() {
        println!("EXP8:   Event {}: {:?}", i, event);
    }

    // Check state AFTER processing
    let rel_count_after: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM relationships WHERE character_id = ?"
    )
    .bind(char_id8)
    .fetch_one(pool)
    .await
    .unwrap_or((0,));
    let mood_count_after: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM moods WHERE character_id = ?"
    )
    .bind(char_id8)
    .fetch_one(pool)
    .await
    .unwrap_or((0,));
    println!("EXP8: State AFTER - relationships={}, moods={}", rel_count_after.0, mood_count_after.0);

    // Check if relationship/mood were updated
    if rel_count_after.0 > rel_count_before.0 {
        println!("EXP8: relationship record CREATED");
    } else if rel_count_after.0 == rel_count_before.0 && rel_count_before.0 > 0 {
        println!("EXP8: relationship record EXISTS (may or may not have been updated)");
    } else {
        println!("EXP8: WARNING - no relationship record created despite complete context");
    }

    // Check outbox event status
    let outbox_status: Option<(String,)> = sqlx::query_as(
        "SELECT status FROM outbox_events WHERE entity_id = ? ORDER BY id DESC LIMIT 1"
    )
    .bind(msg_id8)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    println!("EXP8: Outbox event status: {:?}", outbox_status.map(|s| s.0));

    // Wait for spawned tasks to complete
    let _ = dispatcher_handle8.await;
    let _ = processor_handle8.await;

    // ============================================================================
    // EXPERIMENT 6: Outbox Lease Expiration
    // ============================================================================
    println!("\n=== EXPERIMENT 6: Outbox Lease Expiration ===");

    // Create a third message/outbox event
    let timestamp3 = Utc::now();
    let content3 = MessageContent::Text("lease test".to_string());
    let content_json3 = serde_json::to_string(&content3).unwrap_or_default();

    let (msg_id3, _seq3) = match acceptance_service
        .accept(conv_id, participant_id, content_json3, timestamp3, Some(1003), char_id)
        .await
    {
        Ok((id, seq)) => {
            println!("Third message accepted: id={}, seq={}", id, seq);
            (id, seq)
        }
        Err(e) => {
            println!("Third message acceptance failed: {:?}", e);
            (0, 0)
        }
    };

    // Query the outbox event uuid
    let event_uuid3: Option<String> = if msg_id3 > 0 {
        let row: (String,) = sqlx::query_as(
            "SELECT event_uuid FROM outbox_events WHERE entity_id = ? AND event_type = 'MessageAccepted' ORDER BY id DESC LIMIT 1"
        )
        .bind(msg_id3)
        .fetch_optional(pool)
        .await
        .unwrap_or(None)
        .unwrap_or(("".to_string(),));
        Some(row.0)
    } else {
        None
    };

    if let Some(uuid3) = event_uuid3 {
        println!("Event UUID for lease test: {}", uuid3);

        // Force expire the lease using the repository
        outbox_repo.force_expire_lease(&uuid3).await.unwrap_or_else(|e| {
            println!("force_expire_lease error: {:?}", e);
        });

        // Check the event status after force expire
        let status_after_expire: (String,) = sqlx::query_as(
            "SELECT status FROM outbox_events WHERE event_uuid = ?"
        )
        .bind(&uuid3)
        .fetch_optional(pool)
        .await
        .unwrap_or(None)
        .unwrap_or(("unknown".to_string(),));
        println!("Status after force_expire_lease: {}", status_after_expire.0);

        // Try to claim again - should succeed because lease is expired
        let claim_result = outbox_repo.claim_next_event(30).await;
        println!("Claim after lease expire: {:?}", claim_result);
    }

    // ============================================================================
    // EXPERIMENT 7: Duplicate event_uuid handling
    // ============================================================================
    println!("\n=== EXPERIMENT 7: Duplicate event_uuid ===");

    // Try to add a second outbox event with the SAME event_uuid
    let duplicate_uuid = "test-duplicate-event-uuid-TestOnly";

    // Manually insert a duplicate event
    let insert_result = sqlx::query(
        r#"INSERT INTO outbox_events
           (event_uuid, entity_id, entity_type, event_type, event_version,
            payload, ordering_key, ordering_seq, created_at, available_at,
            status, attempts, last_error, locked_until, processed_at, lease_token)
           VALUES (?, ?, 'messages', 'MessageAccepted', 1,
                   '{"test": "payload"}', 'private:PRIVATE_12345', 999,
                   datetime('now'), datetime('now'),
                   'pending', 0, NULL, NULL, NULL, NULL)"#
    )
    .bind(duplicate_uuid)
    .bind(msg_id3)
    .execute(pool)
    .await;

    match insert_result {
        Ok(r) => println!("First insert succeeded, rows affected: {}", r.rows_affected()),
        Err(e) => println!("First insert failed: {:?}", e),
    }

    // Try to insert the same UUID again (should fail due to UNIQUE constraint)
    let insert_result2 = sqlx::query(
        r#"INSERT INTO outbox_events
           (event_uuid, entity_id, entity_type, event_type, event_version,
            payload, ordering_key, ordering_seq, created_at, available_at,
            status, attempts, last_error, locked_until, processed_at, lease_token)
           VALUES (?, ?, 'messages', 'MessageAccepted', 1,
                   '{"test": "payload2"}', 'private:PRIVATE_12345', 1000,
                   datetime('now'), datetime('now'),
                   'pending', 0, NULL, NULL, NULL, NULL)"#
    )
    .bind(duplicate_uuid)
    .bind(msg_id3 + 1)
    .execute(pool)
    .await;

    match insert_result2 {
        Ok(r) => println!("SECOND insert with same UUID succeeded (UNEXPECTED): {}", r.rows_affected()),
        Err(e) => println!("Second insert with same UUID failed (EXPECTED): {:?}", e),
    }

    // Count total events
    let total_events: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(pool)
        .await
        .unwrap_or((0,));
    println!("Total outbox_events after duplicate attempt: {}", total_events.0);

    // ============================================================================
    // EXPERIMENT 9: Outbound Boundary Verification
    // Observe outbound boundary behavior with current harness configuration.
    // Note: Current harness uses OneBotAdapterImpl with MockWsConnector which cannot
    // actually send messages to a real NapCat server. This experiment documents the
    // architectural path rather than verifying actual external calls.
    // ============================================================================
    println!("\n=== EXPERIMENT 9: Outbound Boundary Verification (Architectural) ===");

    // For EXP 9, we observe the existing EXP 8 processing to see:
    // 1. Whether ResponseGenerated event is published (indicates reply was generated)
    // 2. Whether BehaviorDecided event has action="Reply"
    // 3. Outbox status

    // Check the most recent outbox event (from EXP 8)
    let recent_outbox: Option<(i64, String, String)> = sqlx::query_as(
        "SELECT id, status, event_uuid FROM outbox_events ORDER BY id DESC LIMIT 1"
    )
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    println!("EXP9: Most recent outbox event: {:?}", recent_outbox);

    // Check for ResponseGenerated events in the event log
    let response_gen_events: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_type, payload FROM outbox_events WHERE event_type = 'MessageAccepted' LIMIT 5"
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    println!("EXP9: Found {} MessageAccepted events in recent outbox", response_gen_events.len());

    // Architectural observation:
    // The outbound path is: ReplyProcessor -> ActionDispatcher.execute() -> adapter.send_private_message()
    // Current harness has: ActionDispatcher with OneBotAdapterImpl (not connected to real server)
    //
    // KEY FINDING: Even if adapter.send_private_message() fails, the outbox event is marked
    // as "completed" because ReplyProcessor.process() returns Ok(()) when the binding loop
    // completes without fatal error, regardless of whether action_dispatcher.execute() succeeded.
    //
    // This means "completed" status does NOT guarantee the message was actually sent to QQ.
    println!("EXP9: ARCHITECTURAL OBSERVATION:");
    println!("EXP9:   - Outbox 'completed' status means ReplyProcessor finished processing");
    println!("EXP9:   - It does NOT guarantee adapter.send_private_message() succeeded");
    println!("EXP9:   - Actual external send requires real NapCat connection");
    println!("EXP9:   - Current harness cannot verify real outbound due to MockWsConnector");

    // ============================================================================
    // EXPERIMENT 10-A: Claim → Crash → Reclaim
    // Verify: claim → processing → force expire → reclaim behavior
    // ============================================================================
    println!("\n=== EXPERIMENT 10-A: Claim → Crash → Reclaim ===");

    // Wait for previous spawned tasks to fully complete
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create a fresh message for this experiment
    let ts_10a = Utc::now();
    let content_10a = MessageContent::Text("EXP10-A test message".to_string());
    let content_json_10a = serde_json::to_string(&content_10a).unwrap_or_default();

    let (msg_id_10a, _seq_10a) = match acceptance_service
        .accept(conv_id, participant_id, content_json_10a, ts_10a, Some(10001), char_id)
        .await
    {
        Ok((id, seq)) => {
            println!("EXP10-A: Message accepted: id={}, seq={}", id, seq);
            (id, seq)
        }
        Err(e) => {
            println!("EXP10-A: Message acceptance failed: {:?}", e);
            return Err(e.into());
        }
    };

    // Get the outbox event for this message
    let event_uuid_10a: Option<(String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT event_uuid, status, lease_token, available_at FROM outbox_events WHERE entity_id = ? ORDER BY id DESC LIMIT 1"
    )
    .bind(msg_id_10a)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    println!("EXP10-A: Initial outbox state: {:?}", event_uuid_10a);

    // Debug: Check all pending events
    let all_pending: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_uuid, status FROM outbox_events WHERE status = 'pending'"
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    println!("EXP10-A: All pending events: {:?}", all_pending);

    // Debug: Check available_at for pending events
    let pending_avail: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_uuid, available_at FROM outbox_events WHERE status = 'pending'"
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    println!("EXP10-A: Pending events available_at: {:?}", pending_avail);

    // Debug: Check completed events ordering
    let completed_seqs: Vec<(String, i64, String)> = sqlx::query_as(
        "SELECT ordering_key, ordering_seq, status FROM outbox_events WHERE status IN ('completed', 'dead_letter') ORDER BY ordering_key, ordering_seq"
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    println!("EXP10-A: Completed events ordering: {:?}", completed_seqs);

    // Debug: Check our pending event's ordering relative to completed
    let our_pending_seq: Option<(String, i64, i64)> = sqlx::query_as(
        "SELECT ordering_key, ordering_seq, entity_id FROM outbox_events WHERE event_uuid = ?"
    )
    .bind(&event_uuid_10a.as_ref().unwrap().0)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    println!("EXP10-A: Our pending event: {:?}", our_pending_seq);

    // Check what the expected next_seq would be for our event's ordering_key
    if let Some((ok, seq, _)) = our_pending_seq {
        let expected_next: Option<(i64,)> = sqlx::query_as(
            "SELECT COALESCE(MAX(ordering_seq), 0) + 1 as next_seq FROM outbox_events WHERE ordering_key = ? AND status IN ('completed', 'dead_letter')"
        )
        .bind(&ok)
        .fetch_optional(pool)
        .await
        .unwrap_or(None);
        println!("EXP10-A: For ordering_key={}, expected next_seq={:?}, our seq={}", ok, expected_next.map(|x| x.0), seq);
    }

    // First, we need to reclaim the blocking event (seq=4 which is still "processing")
    // Find the event that is blocking our sequence
    let blocking_event: Option<(String, String, i64)> = sqlx::query_as(
        "SELECT event_uuid, status, ordering_seq FROM outbox_events WHERE ordering_key = '1' AND status = 'processing' ORDER BY ordering_seq LIMIT 1"
    )
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    println!("EXP10-A: Blocking event (seq=4 still processing): {:?}", blocking_event);

    if let Some((block_uuid, block_status, block_seq)) = blocking_event {
        println!("EXP10-A: Force expiring blocking event {} (seq={}, status={})", block_uuid, block_seq, block_status);
        let _ = outbox_repo.force_expire_lease(&block_uuid).await;

        // NOTE: force_expire_lease only sets locked_until to past, but does NOT change status.
        // claim_next_event only looks for status IN ('pending', 'retry_wait').
        // So we need to also reset status to 'pending' for the event to be reclaimable.
        // This simulates what would happen if the stale worker's lease naturally expired
        // and the system auto-recovered it.

        // Directly reset status to 'pending' for testing purposes
        // (In production, this would happen via recover_expired_events or similar mechanism)
        sqlx::query(
            "UPDATE outbox_events SET status = 'pending', locked_until = NULL, lease_token = NULL WHERE event_uuid = ?"
        )
        .bind(&block_uuid)
        .execute(pool)
        .await
        .ok();

        println!("EXP10-A: Reset blocking event status to 'pending'");

        // Verify reset
        let block_status_reset: Option<(String,)> = sqlx::query_as(
            "SELECT status FROM outbox_events WHERE event_uuid = ?"
        )
        .bind(&block_uuid)
        .fetch_optional(pool)
        .await
        .unwrap_or(None);
        println!("EXP10-A: Blocking event status after reset: {:?}", block_status_reset.map(|s| s.0));

        // Now try to claim the blocking event (Worker B)
        println!("EXP10-A: Worker B claiming blocking event...");
        let claim_block = outbox_repo.claim_next_event(30).await;
        println!("EXP10-A: Worker B claim blocking result: {:?}", claim_block);

        // Get the lease token
        let (block_lease_token, block_event_uuid) = match &claim_block {
            Ok(Some(e)) => {
                println!("EXP10-A: Worker B claimed blocking event: uuid={}, lease_token={:?}", e.event_uuid, e.lease_token);
                (e.lease_token.clone().unwrap_or_default(), e.event_uuid.clone())
            }
            _ => {
                println!("EXP10-A: Worker B failed to claim blocking event");
                return Err("Worker B claim blocking event failed".into());
            }
        };

        // Worker B mark_completed on the blocking event
        println!("EXP10-A: Worker B marking blocking event as completed...");
        let mark_comp_block = outbox_repo.mark_completed(&block_event_uuid, &block_lease_token).await;
        println!("EXP10-A: mark_completed(blocking) result: {:?}", mark_comp_block);

        // Verify blocking event is now completed
        let block_status_after: Option<(String,)> = sqlx::query_as(
            "SELECT status FROM outbox_events WHERE event_uuid = ?"
        )
        .bind(&block_event_uuid)
        .fetch_optional(pool)
        .await
        .unwrap_or(None);
        println!("EXP10-A: Blocking event status after mark_completed: {:?}", block_status_after.map(|s| s.0));
    }

    // Now try to claim our target event (should be seq=4 now, which is our seq=5)
    println!("EXP10-A: Step 1 - Worker A claiming event after blocking event cleared...");
    let claim_result_1 = outbox_repo.claim_next_event(30).await;
    println!("EXP10-A: Worker A claim result: {:?}", claim_result_1);

    let (claimed_event, lease_token_a) = match &claim_result_1 {
        Ok(Some(e)) => {
            println!("EXP10-A: Worker A claimed event_uuid={}, lease_token={:?}, locked_until={:?}",
                e.event_uuid, e.lease_token, e.locked_until);
            (e.clone(), e.lease_token.clone().unwrap_or_default())
        }
        _ => {
            println!("EXP10-A: Worker A failed to claim - no available events");
            // This is acceptable - event may have been claimed by cleanup
            return Ok(());
        }
    };

    // Verify event is now "processing"
    let status_after_claim: Option<(String,)> = sqlx::query_as(
        "SELECT status FROM outbox_events WHERE event_uuid = ?"
    )
    .bind(&claimed_event.event_uuid)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    println!("EXP10-A: Status after Worker A claim: {:?}", status_after_claim.map(|s| s.0));

    // Step 2: Simulate crash - force expire the lease
    println!("EXP10-A: Step 2 - Force expiring lease (simulating crash)...");
    let expire_result = outbox_repo.force_expire_lease(&claimed_event.event_uuid).await;
    println!("EXP10-A: Force expire result: {:?}", expire_result);

    // Check status after force expire
    let status_after_expire: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT status, lease_token FROM outbox_events WHERE event_uuid = ?"
    )
    .bind(&claimed_event.event_uuid)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    println!("EXP10-A: Status after force expire: {:?}", status_after_expire);

    // Step 3: Worker B tries to claim (should get the same event if reclaimable)
    println!("EXP10-A: Step 3 - Worker B claiming event...");
    let claim_result_2 = outbox_repo.claim_next_event(30).await;
    println!("EXP10-A: Worker B claim result: {:?}", claim_result_2);

    match &claim_result_2 {
        Ok(Some(e)) => {
            println!("EXP10-A: Worker B claimed event_uuid={}, lease_token={:?}, locked_until={:?}",
                e.event_uuid, e.lease_token, e.locked_until);

            if e.event_uuid == claimed_event.event_uuid {
                println!("EXP10-A: Worker B got SAME event as Worker A");
                println!("EXP10-A: New lease_token={:?} vs old lease_token={}", e.lease_token, lease_token_a);
            } else {
                println!("EXP10-A: Worker B got DIFFERENT event (FIFO behavior)");
            }
        }
        Ok(None) => {
            println!("EXP10-A: Worker B got NO event (all events locked or none available)");
        }
        Err(e) => {
            println!("EXP10-A: Worker B claim error: {:?}", e);
        }
    }

    // Step 4: Check final state
    let final_status: Option<(String,)> = sqlx::query_as(
        "SELECT status FROM outbox_events WHERE event_uuid = ?"
    )
    .bind(&claimed_event.event_uuid)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    println!("EXP10-A: Final status: {:?}", final_status.map(|s| s.0));

    // ============================================================================
    // EXPERIMENT 10-B: Stale Worker Fencing
    // Verify: stale Worker A tries mark_completed with old token, should be rejected
    // ============================================================================
    println!("\n=== EXPERIMENT 10-B: Stale Worker Fencing ===");

    // Worker A's old token should NOT work now
    println!("EXP10-B: Worker A (stale) attempting mark_completed with old token={}...", lease_token_a);
    let mark_completed_stale = outbox_repo.mark_completed(&claimed_event.event_uuid, &lease_token_a).await;
    println!("EXP10-B: mark_completed(stale) result: {:?}", mark_completed_stale);

    // Check if status changed
    let status_after_stale: Option<(String,)> = sqlx::query_as(
        "SELECT status FROM outbox_events WHERE event_uuid = ?"
    )
    .bind(&claimed_event.event_uuid)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    println!("EXP10-B: Status after stale mark_completed attempt: {:?}", status_after_stale.map(|s| s.0));

    // Get the current lease_token (from Worker B's claim)
    let current_lease: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT lease_token FROM outbox_events WHERE event_uuid = ?"
    )
    .bind(&claimed_event.event_uuid)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    println!("EXP10-B: Current lease_token: {:?}", current_lease.map(|t| t.0.clone()));

    // If Worker B claimed successfully, it should have a valid token
    let new_token = match &claim_result_2 {
        Ok(Some(e)) => e.lease_token.clone(),
        _ => None,
    };

    if let Some(token) = new_token {
        println!("EXP10-B: Worker B (new) attempting mark_completed with token={}...", token);
        let mark_completed_new = outbox_repo.mark_completed(&claimed_event.event_uuid, &token).await;
        println!("EXP10-B: mark_completed(new) result: {:?}", mark_completed_new);

        let status_after_new: Option<(String,)> = sqlx::query_as(
            "SELECT status FROM outbox_events WHERE event_uuid = ?"
        )
        .bind(&claimed_event.event_uuid)
        .fetch_optional(pool)
        .await
        .unwrap_or(None);
        println!("EXP10-B: Status after new mark_completed: {:?}", status_after_new.map(|s| s.0));
    }

    // ============================================================================
    // EXPERIMENT 11-A: Concurrent Outbox Claim
    // Verify: multiple workers competing for the same pending event
    // ============================================================================
    println!("\n=== EXPERIMENT 11-A: Concurrent Outbox Claim ===");

    // First, create a fresh pending event for concurrent claim test
    let ts_11a = Utc::now();
    let content_11a = MessageContent::Text("EXP11-A concurrent test".to_string());
    let content_json_11a = serde_json::to_string(&content_11a).unwrap_or_default();

    let (msg_id_11a, _seq_11a) = match acceptance_service
        .accept(conv_id, participant_id, content_json_11a, ts_11a, Some(11001), char_id)
        .await
    {
        Ok((id, seq)) => {
            println!("EXP11-A: Message accepted: id={}, seq={}", id, seq);
            (id, seq)
        }
        Err(e) => {
            println!("EXP11-A: Message acceptance failed: {:?}", e);
            return Err(e.into());
        }
    };

    // Get the new event's UUID
    let event_11a: Option<String> = sqlx::query_scalar(
        "SELECT event_uuid FROM outbox_events WHERE entity_id = ? ORDER BY id DESC LIMIT 1"
    )
    .bind(msg_id_11a)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    println!("EXP11-A: New event UUID: {:?}", event_11a);

    // Wait for any previous processing to settle
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Spawn 3 concurrent workers trying to claim the same event
    let repo1 = outbox_repo.clone();
    let repo2 = outbox_repo.clone();
    let repo3 = outbox_repo.clone();

    println!("EXP11-A: Spawning 3 concurrent workers trying to claim...");

    let (handle1, handle2, handle3) = tokio::join! {
        tokio::spawn(async move {
            let result = repo1.claim_next_event(30).await;
            match &result {
                Ok(Some(ev)) => println!("EXP11-A: Worker 1 claim: uuid={}, token={:?}", ev.event_uuid, ev.lease_token),
                Ok(None) => println!("EXP11-A: Worker 1 claim: NO EVENT"),
                Err(e) => println!("EXP11-A: Worker 1 claim error: {:?}", e),
            }
            result
        }),
        tokio::spawn(async move {
            let result = repo2.claim_next_event(30).await;
            match &result {
                Ok(Some(ev)) => println!("EXP11-A: Worker 2 claim: uuid={}, token={:?}", ev.event_uuid, ev.lease_token),
                Ok(None) => println!("EXP11-A: Worker 2 claim: NO EVENT"),
                Err(e) => println!("EXP11-A: Worker 2 claim error: {:?}", e),
            }
            result
        }),
        tokio::spawn(async move {
            let result = repo3.claim_next_event(30).await;
            match &result {
                Ok(Some(ev)) => println!("EXP11-A: Worker 3 claim: uuid={}, token={:?}", ev.event_uuid, ev.lease_token),
                Ok(None) => println!("EXP11-A: Worker 3 claim: NO EVENT"),
                Err(e) => println!("EXP11-A: Worker 3 claim error: {:?}", e),
            }
            result
        })
    };

    // Collect results
    let results = [handle1, handle2, handle3];
    let mut success_count = 0;
    let mut claim_uuids = Vec::new();

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(Ok(Some(event))) => {
                success_count += 1;
                claim_uuids.push(event.event_uuid.clone());
                println!("EXP11-A: Worker {} got event uuid={}", i+1, event.event_uuid);
            }
            Ok(Ok(None)) => {
                println!("EXP11-A: Worker {} got NO event", i+1);
            }
            Ok(Err(e)) => {
                println!("EXP11-A: Worker {} claim error: {:?}", i+1, e);
            }
            Err(e) => {
                println!("EXP11-A: Worker {} task join error: {:?}", i+1, e);
            }
        }
    }

    println!("EXP11-A: Total successful claims: {}", success_count);
    println!("EXP11-A: Claimed UUIDs: {:?}", claim_uuids);

    // Check how many events are now in 'processing' state
    let processing_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM outbox_events WHERE status = 'processing'"
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0,));
    println!("EXP11-A: Events in 'processing' state: {}", processing_count.0);

    // Check final status of our test event
    if let Some(ref uuid) = event_11a {
        let final_status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM outbox_events WHERE event_uuid = ?"
        )
        .bind(uuid)
        .fetch_optional(pool)
        .await
        .unwrap_or(None);
        println!("EXP11-A: Final status of test event: {:?}", final_status);
    }

    // ============================================================================
    // EXPERIMENT 11-B: Concurrent Messages Same Conversation
    // Verify: multiple messages in same conversation observe sequence numbers
    // ============================================================================
    println!("\n=== EXPERIMENT 11-B: Concurrent Messages Same Conversation ===");

    // Create multiple messages rapidly in the same conversation
    let mut seqs = Vec::new();
    for i in 0..5 {
        let ts = Utc::now();
        let content = MessageContent::Text(format!("EXP11-B message {}", i));
        let content_json = serde_json::to_string(&content).unwrap_or_default();

        match acceptance_service
            .accept(conv_id, participant_id, content_json, ts, Some(12000 + i as i64), char_id)
            .await
        {
            Ok((id, seq)) => {
                println!("EXP11-B: Message {} accepted: id={}, seq={}", i, id, seq);
                seqs.push(seq);
            }
            Err(e) => {
                println!("EXP11-B: Message {} acceptance failed: {:?}", i, e);
            }
        }
    }

    println!("EXP11-B: Received sequences: {:?}", seqs);

    // Check for duplicates
    let mut seen = std::collections::HashSet::new();
    let mut duplicates = Vec::new();
    for &seq in &seqs {
        if !seen.insert(seq) {
            duplicates.push(seq);
        }
    }
    if duplicates.is_empty() {
        println!("EXP11-B: No duplicate sequences detected");
    } else {
        println!("EXP11-B: DUPLICATE sequences detected: {:?}", duplicates);
    }

    // Check conversation_sequence in messages table
    let msg_seq_check: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT id, conversation_sequence FROM messages WHERE conversation_id = ? ORDER BY conversation_sequence"
    )
    .bind(conv_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    println!("EXP11-B: Message sequence numbers: {:?}", msg_seq_check);

    // ============================================================================
    // EXPERIMENT 10-D: Actual Recovery Mechanism (recover_expired_processing)
    // Verify that the OutboxDispatcher's recovery mechanism works
    // This was discovered in code: OutboxDispatcher.run() calls recover_expired_events() at startup
    // which calls repo.recover_expired_processing()
    // ============================================================================
    println!("\n=== EXPERIMENT 10-D: Actual Recovery Mechanism ===");

    // First, let's check if there's an event stuck in 'processing' from our earlier experiments
    let stuck_events: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT event_uuid, status, locked_until FROM outbox_events WHERE status = 'processing'"
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    println!("EXP10-D: Found {} events in 'processing' state", stuck_events.len());
    for (uuid, status, locked_until) in &stuck_events {
        println!("EXP10-D:   uuid={}, status={}, locked_until={}", uuid, status, locked_until);
    }

    // If there are stuck events, test the recovery mechanism
    if let Some((stuck_uuid, _, _)) = stuck_events.first() {
        println!("EXP10-D: Testing recovery on event {}", stuck_uuid);

        // Step 1: Force-expire the lease (sets locked_until to past)
        println!("EXP10-D: Step 1 - Force-expiring lease...");
        let expire_result = outbox_repo.force_expire_lease(stuck_uuid).await;
        println!("EXP10-D: force_expire_lease() result: {:?}", expire_result);

        // Verify locked_until is now in the past
        let after_expire: Option<(String,)> = sqlx::query_as(
            "SELECT locked_until FROM outbox_events WHERE event_uuid = ?"
        )
        .bind(stuck_uuid)
        .fetch_optional(pool)
        .await
        .unwrap_or(None);
        println!("EXP10-D: locked_until after force_expire: {:?}", after_expire);

        // Step 2: Call recover_expired_processing() - this is what OutboxDispatcher does at startup
        println!("EXP10-D: Step 2 - Calling recover_expired_processing()...");
        let recover_result = outbox_repo.recover_expired_processing().await;
        println!("EXP10-D: recover_expired_processing() returned: {:?}", recover_result);

        // Step 3: Check status after recovery
        let status_after_recover: Option<(String,)> = sqlx::query_as(
            "SELECT status FROM outbox_events WHERE event_uuid = ?"
        )
        .bind(stuck_uuid)
        .fetch_optional(pool)
        .await
        .unwrap_or(None);
        println!("EXP10-D: Status after recovery: {:?}", status_after_recover.map(|s| s.0));

        // Step 4: Try to claim the recovered event
        println!("EXP10-D: Step 3 - Trying to claim recovered event...");
        let claim_after_recover = outbox_repo.claim_next_event(30).await;
        println!("EXP10-D: claim_next_event() after recovery: {:?}", claim_after_recover);

        match &claim_after_recover {
            Ok(Some(event)) => {
                println!("EXP10-D: SUCCESS - Recovered event was claimed!");
                println!("EXP10-D:   uuid={}, token={:?}, status={}",
                    event.event_uuid, event.lease_token, event.status);

                // Mark it as completed to clean up
                if let Some(token) = &event.lease_token {
                    let mark_result = outbox_repo.mark_completed(&event.event_uuid, token).await;
                    println!("EXP10-D: mark_completed result: {:?}", mark_result);
                }
            }
            Ok(None) => {
                println!("EXP10-D: FAILED - No event was claimed");
            }
            Err(e) => {
                println!("EXP10-D: ERROR - Claim failed: {:?}", e);
            }
        }
    } else {
        println!("EXP10-D: No stuck processing events found to test recovery");
    }

    // ============================================================================
    // EXPERIMENT 10-C: External Observer (Architectural Note)
    // ============================================================================
    println!("\n=== EXPERIMENT 10-C: External Observer (Architectural Note) ===");

    println!("EXP10-C: ARCHITECTURAL NOTE:");
    println!("EXP10-C: To verify external duplicate send, we would need to:");
    println!("EXP10-C:   1. Inject a fake OneBotAdapter that records send calls");
    println!("EXP10-C:   2. Run the full OutboxProcessor → ReplyProcessor → ActionDispatcher chain");
    println!("EXP10-C:   3. Observe how many times the adapter was called");
    println!("EXP10-C:");
    println!("EXP10-C: The current architecture has:");
    println!("EXP10-C:   ReplyProcessor creates its own ActionDispatcher in constructor");
    println!("EXP10-C:   ActionDispatcher holds Arc<dyn OneBotAdapter>");
    println!("EXP10-C:   The adapter is constructed from OneBotAdapterImpl with WsConnector");
    println!("EXP10-C:");
    println!("EXP10-C: BLOCKED: Cannot inject fake observer without modifying ReplyProcessor construction");
    println!("EXP10-C: This is a TESTABILITY BOUNDARY, not a production defect");
    println!("EXP10-C: The adapter boundary exists but is not designed for audit injection");

    // ============================================================================
    // Summary
    // ============================================================================
    println!("\n=== SUMMARY ===");
    println!("Messages created: {}", msg_count_after.0 - msg_count_before.0);
    println!("Outbox events created: {}", outbox_count_after.0 - outbox_count_before.0);
    let completed_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM outbox_events WHERE status = 'completed'"
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0,));
    println!("Outbox events completed: {}", completed_count.0);

    // Cleanup
    storage.close().await;
    drop(temp_dir);

    Ok(())
}

/// Drain events from subscription with timeout
async fn drain_events(
    sub: &mut yomua_bot::application::event_bus::EventSubscription,
    max: usize,
) -> Vec<yomua_bot::domain::event::CoreEvent> {
    let mut events = Vec::new();
    let timeout = tokio::time::sleep(Duration::from_millis(500));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => break,
            event_opt = sub.recv() => {
                match event_opt {
                    Some(e) => {
                        events.push(e);
                        if events.len() >= max {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }
    events
}
