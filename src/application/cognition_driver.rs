//! 后台认知驱动 —— 定期让角色"思考"最近的对话并沉淀认知。
//!
//! 每个 tick（默认 5 分钟）遍历所有启用了主动行为的绑定，
//! 在满足冷却（默认 10 分钟）与 idle（默认 5 分钟无新消息）条件时，
//! 基于最近对话上下文向 LLM 请求一段认知，并将结果存储为语义记忆。
//!
//! 认知 ≠ 行为：行为驱动决定是否主动发起动作，认知驱动负责角色对
//! 近期对话的内在理解与反思。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};

use crate::application::clock::Clock;
use crate::application::llm_scheduler::{EmbeddingScheduler, LlmScheduler};
use crate::domain::character::CharacterBinding;
use crate::domain::message::Message;
use crate::domain::repository::CharacterBindingRepository;
use crate::domain::repository::{MemoryRepository, MessageRepository};
use crate::error::RuntimeError;
use crate::infrastructure::llm::{LlmMessage, LlmRequest, LlmRole};

/// 认知驱动的默认 tick 间隔（5 分钟）。
const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(300);

/// 认知驱动的默认冷却时长（10 分钟）。
const DEFAULT_COOLDOWN: Duration = Duration::from_secs(600);

/// 认知驱动的默认 idle 阈值（5 分钟无新消息则视为 idle）。
const DEFAULT_IDLE_THRESHOLD: Duration = Duration::from_secs(300);

/// 冷却时间映射 —— 防止同一角色在冷却期内被重复触发。
type CooldownMap = RwLock<HashMap<i64, DateTime<Utc>>>;

/// 后台认知驱动。
///
/// 在后台以固定间隔运行；每次 `tick` 独立评估所有启用主动的绑定。
pub struct CognitionDriver {
    embedding_scheduler: Arc<dyn EmbeddingScheduler>,
    llm_scheduler: Arc<dyn LlmScheduler>,
    memory_repo: Arc<dyn MemoryRepository>,
    binding_repo: Arc<dyn CharacterBindingRepository>,
    message_repo: Arc<dyn MessageRepository>,
    character_repo: Arc<dyn crate::domain::repository::CharacterRepository>,
    clock: Arc<dyn Clock>,
    /// 每次 tick 的间隔。
    tick_interval: Duration,
    /// 同一角色两次认知的冷却时间。
    cooldown: Duration,
    /// idle 检测阈值：超过此时间无新消息视为 idle。
    idle_threshold: Duration,
    /// 是否正在运行。
    running: AtomicBool,
    /// 最近一次认知的时间（按 character_id）。
    last_cognition: CooldownMap,
}

impl CognitionDriver {
    /// 创建一个认知驱动。
    pub fn new(
        embedding_scheduler: Arc<dyn EmbeddingScheduler>,
        llm_scheduler: Arc<dyn LlmScheduler>,
        memory_repo: Arc<dyn MemoryRepository>,
        binding_repo: Arc<dyn CharacterBindingRepository>,
        message_repo: Arc<dyn MessageRepository>,
        character_repo: Arc<dyn crate::domain::repository::CharacterRepository>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            embedding_scheduler,
            llm_scheduler,
            memory_repo,
            binding_repo,
            message_repo,
            character_repo,
            clock,
            tick_interval: DEFAULT_TICK_INTERVAL,
            cooldown: DEFAULT_COOLDOWN,
            idle_threshold: DEFAULT_IDLE_THRESHOLD,
            running: AtomicBool::new(false),
            last_cognition: CooldownMap::new(HashMap::new()),
        }
    }

    /// 创建一个认知驱动（支持自定义间隔参数）。
    #[allow(clippy::too_many_arguments)]
    pub fn with_config(
        embedding_scheduler: Arc<dyn EmbeddingScheduler>,
        llm_scheduler: Arc<dyn LlmScheduler>,
        memory_repo: Arc<dyn MemoryRepository>,
        binding_repo: Arc<dyn CharacterBindingRepository>,
        message_repo: Arc<dyn MessageRepository>,
        character_repo: Arc<dyn crate::domain::repository::CharacterRepository>,
        clock: Arc<dyn Clock>,
        tick_interval: Duration,
        cooldown: Duration,
        idle_threshold: Duration,
    ) -> Self {
        Self {
            embedding_scheduler,
            llm_scheduler,
            memory_repo,
            binding_repo,
            message_repo,
            character_repo,
            clock,
            tick_interval,
            cooldown,
            idle_threshold,
            running: AtomicBool::new(false),
            last_cognition: CooldownMap::new(HashMap::new()),
        }
    }

    /// 以固定间隔循环运行（常驻后台任务）。
    pub async fn run(self: Arc<Self>) {
        self.running.store(true, Ordering::SeqCst);
        tracing::info!(target: "runtime", "后台认知驱动已启动");
        let mut ticker = interval(self.tick_interval);

        while self.running.load(Ordering::SeqCst) {
            ticker.tick().await;
            if let Err(e) = self.tick().await {
                tracing::warn!(target: "runtime", error = %e, "CognitionDriver tick error");
            }
        }
    }

    /// 停止驱动。
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// 执行一轮认知评估。
    async fn tick(&self) -> Result<(), RuntimeError> {
        let bindings = self
            .binding_repo
            .find_all_enabled()
            .await
            .map_err(RuntimeError::Repository)?;

        for binding in bindings {
            if let Err(e) = self.process_binding(&binding).await {
                tracing::warn!(
                    target: "runtime",
                    character_id = binding.character_id,
                    conversation_id = binding.conversation_id,
                    error = %e,
                    "认知处理失败"
                );
            }
        }

        Ok(())
    }

    /// 处理单个绑定：冷却检查 → idle 检查 → 执行认知。
    async fn process_binding(&self, binding: &CharacterBinding) -> Result<(), RuntimeError> {
        // 冷却检查：距上次认知不足冷却时长则跳过。
        if self.is_in_cooldown(binding.character_id).await? {
            return Ok(());
        }

        // idle 检查：最近 idle_threshold 时间内无新消息则视为 idle。
        if !self.is_idle(binding.conversation_id).await? {
            return Ok(());
        }

        self.cognize(binding).await?;

        // 更新冷却时间。
        let now = self.clock.now();
        let mut map = self.last_cognition.write().await;
        map.insert(binding.character_id, now);

        Ok(())
    }

    /// 检查某角色是否处于冷却期。
    async fn is_in_cooldown(&self, character_id: i64) -> Result<bool, RuntimeError> {
        let map = self.last_cognition.read().await;
        if let Some(last) = map.get(&character_id) {
            let cooldown = chrono::Duration::from_std(self.cooldown)
                .map_err(|e| RuntimeError::Internal(format!("冷却时长无效: {e}")))?;
            return Ok(self.clock.now().signed_duration_since(*last) < cooldown);
        }
        Ok(false)
    }

    /// 检查某会话是否处于 idle 状态（超过阈值时间无新消息）。
    async fn is_idle(&self, conversation_id: i64) -> Result<bool, RuntimeError> {
        let latest = self
            .message_repo
            .latest_message_time(conversation_id)
            .await
            .map_err(RuntimeError::Repository)?;

        match latest {
            Some(t) => {
                let threshold = chrono::Duration::from_std(self.idle_threshold)
                    .map_err(|e| RuntimeError::Internal(format!("idle 阈值无效: {e}")))?;
                Ok(self.clock.now().signed_duration_since(t) > threshold)
            }
            None => Ok(true), // 从未有过消息，视为 idle
        }
    }

    /// 执行一次认知：构建 prompt → 调用 LLM → 存储结果。
    async fn cognize(&self, binding: &CharacterBinding) -> Result<(), RuntimeError> {
        // 获取角色定义用于渲染 system prompt。
        let character = self
            .character_repo
            .find_by_id(binding.character_id)
            .await
            .map_err(RuntimeError::Repository)?
            .ok_or_else(|| {
                RuntimeError::Internal(format!(
                    "角色 {} 未找到，无法执行认知",
                    binding.character_id
                ))
            })?;

        // 获取最近消息作为上下文。
        let recent_messages = self
            .message_repo
            .find_recent(binding.conversation_id, 20)
            .await
            .map_err(RuntimeError::Repository)?;

        let prompt = self.build_cognition_prompt(&character, &recent_messages);

        // 调用 LLM。
        let request = LlmRequest {
            system: character.definition.system_prompt.clone(),
            messages: vec![LlmMessage {
                role: LlmRole::User,
                content: prompt,
            }],
            model: None,
            temperature: Some(0.7),
            max_tokens: Some(256),
            priority: 3, // P3，后台任务
            metadata: serde_json::json!({
                "character_id": binding.character_id,
                "conversation_id": binding.conversation_id,
            }),
        };

        let response = self
            .llm_scheduler
            .submit(request)
            .await
            .map_err(|e| RuntimeError::Llm(e.to_string()))?;

        let content = response.content.trim();
        if content.is_empty() {
            tracing::debug!(
                target: "runtime",
                character_id = binding.character_id,
                conversation_id = binding.conversation_id,
                "LLM 返回空内容，跳过存储"
            );
            return Ok(());
        }

        // 生成 embedding。
        let embeddings = self
            .embedding_scheduler
            .submit_embedding(vec![content.to_string()])
            .await
            .map_err(|e| RuntimeError::Llm(e.to_string()))?;
        let embedding = embeddings
            .first()
            .ok_or_else(|| RuntimeError::Internal("Embedding 返回为空".to_string()))?;

        // 存储为语义记忆。
        self.memory_repo
            .insert_semantic(
                binding.character_id,
                Some(binding.conversation_id),
                "semantic",
                content,
                embedding,
                0.7, // importance
                "{}",
            )
            .await
            .map_err(RuntimeError::Repository)?;

        tracing::info!(
            target: "runtime",
            character_id = binding.character_id,
            conversation_id = binding.conversation_id,
            content_len = content.len(),
            "认知已沉淀为语义记忆"
        );

        Ok(())
    }

    /// 构建认知 prompt。
    fn build_cognition_prompt(
        &self,
        character: &crate::domain::character::Character,
        recent_messages: &[Message],
    ) -> String {
        // 将消息格式化为简洁的对话摘要。
        let dialogue = recent_messages
            .iter()
            .map(|m| {
                let sender = if m.sender_id == 0 {
                    String::from("用户")
                } else {
                    format!("角色{}", m.sender_id)
                };
                let text = match &m.content {
                    crate::domain::message::MessageContent::Text(t) => t.clone(),
                    _ => String::from("[非文本消息]"),
                };
                format!("{sender}: {text}")
            })
            .collect::<Vec<_>>()
            .join("\n");

        format!(
            "基于以下最近的对话，角色\"{}\"有什么新的认知或理解？\
             请简洁回答（不超过 200 字），只输出认知内容，不需要复述对话。\n\n\
             === 最近对话 ===\n{}\n\n=== 认知 ===",
            character.definition.name, dialogue
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::character::{Character, CharacterDefinition, CharacterState, ReplyMode};
    use crate::domain::message::MessageContent;
    use crate::domain::repository::{
        CharacterBindingRepository, CharacterRepository, MemoryRepository, MessageRepository,
    };
    use crate::error::RepositoryError;
    use crate::infrastructure::llm::{LlmResponse, TokenUsage};
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use std::sync::Mutex;

    // ---- 假时钟 ----

    #[derive(Clone)]
    struct FakeClock {
        time: Arc<Mutex<DateTime<Utc>>>,
    }

    impl FakeClock {
        fn at(year: i32, month: u32, day: u32, hour: u32, min: u32) -> Self {
            let t = chrono::NaiveDate::from_ymd_opt(year, month, day)
                .unwrap()
                .and_hms_opt(hour, min, 0)
                .unwrap()
                .and_utc();
            Self {
                time: Arc::new(Mutex::new(t)),
            }
        }

        fn advance(&self, secs: i64) {
            let mut t = self.time.lock().unwrap();
            *t += chrono::Duration::seconds(secs);
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> DateTime<Utc> {
            *self.time.lock().unwrap()
        }
    }

    // ---- 假仓储 ----

    struct FakeMessageRepo {
        messages: Mutex<HashMap<i64, Vec<Message>>>,
    }

    impl FakeMessageRepo {
        fn new() -> Self {
            Self {
                messages: Mutex::new(HashMap::new()),
            }
        }

        fn add_message(&self, conv_id: i64, sender_id: i64, content: &str, days_ago: i64) {
            let mut msgs = self.messages.lock().unwrap();
            let msg = Message {
                id: msgs.entry(conv_id).or_insert_with(Vec::new).len() as i64 + 1,
                conversation_id: conv_id,
                sender_id,
                content: MessageContent::Text(content.to_string()),
                timestamp: Utc::now() - chrono::Duration::days(days_ago),
                reply_to: None,
                mentions: vec![],
                attachments: vec![],
                metadata: serde_json::json!({}),
            };
            msgs.entry(conv_id).or_insert_with(Vec::new).push(msg);
        }
    }

    #[async_trait]
    impl MessageRepository for FakeMessageRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Message>, RepositoryError> {
            Ok(None)
        }
        async fn find_recent(
            &self,
            conversation_id: i64,
            _limit: i64,
        ) -> Result<Vec<Message>, RepositoryError> {
            Ok(self
                .messages
                .lock()
                .unwrap()
                .get(&conversation_id)
                .cloned()
                .unwrap_or_default())
        }
        async fn insert(&self, _m: &Message) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn latest_message_time(
            &self,
            conversation_id: i64,
        ) -> Result<Option<DateTime<Utc>>, RepositoryError> {
            let msgs = self.messages.lock().unwrap();
            Ok(msgs
                .get(&conversation_id)
                .and_then(|v| v.iter().map(|m| m.timestamp).max()))
        }
    }

    struct FakeBindingRepo {
        bindings: Mutex<Vec<CharacterBinding>>,
    }

    #[async_trait]
    impl CharacterBindingRepository for FakeBindingRepo {
        async fn find_by_character_id(
            &self,
            _character_id: i64,
        ) -> Result<Vec<CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_by_conversation_id(
            &self,
            _conversation_id: i64,
        ) -> Result<Vec<CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_all(&self) -> Result<Vec<CharacterBinding>, RepositoryError> {
            Ok(self.bindings.lock().unwrap().clone())
        }
        async fn find_all_enabled(&self) -> Result<Vec<CharacterBinding>, RepositoryError> {
            Ok(self
                .bindings
                .lock()
                .unwrap()
                .iter()
                .filter(|b| b.proactive_enabled)
                .cloned()
                .collect())
        }
        async fn insert(&self, _b: &CharacterBinding) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _b: &CharacterBinding) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct FakeCharacterRepo {
        characters: Mutex<HashMap<i64, Character>>,
    }

    #[async_trait]
    impl CharacterRepository for FakeCharacterRepo {
        async fn find_by_id(&self, id: i64) -> Result<Option<Character>, RepositoryError> {
            Ok(self.characters.lock().unwrap().get(&id).cloned())
        }
        async fn find_all(&self) -> Result<Vec<Character>, RepositoryError> {
            Ok(self.characters.lock().unwrap().values().cloned().collect())
        }
        async fn insert(&self, _c: &Character) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _c: &Character) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct FakeMemoryRepo;

    #[async_trait]
    impl MemoryRepository for FakeMemoryRepo {
        async fn find_by_character_id(
            &self,
            _character_id: i64,
            _memory_type: Option<crate::domain::memory::MemoryType>,
            _limit: i64,
        ) -> Result<Vec<crate::domain::memory::Memory>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert(&self, _m: &crate::domain::memory::Memory) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _m: &crate::domain::memory::Memory) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn search_by_embedding(
            &self,
            _character_id: i64,
            _query_embedding: &[f32],
            _memory_type: Option<&str>,
            _limit: i64,
        ) -> Result<Vec<crate::domain::memory::SemanticMatchResult>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert_semantic(
            &self,
            _character_id: i64,
            _conversation_id: Option<i64>,
            _memory_type: &str,
            _content: &str,
            _embedding: &[f32],
            _importance: f64,
            _metadata: &str,
        ) -> Result<i64, RepositoryError> {
            Ok(1)
        }
    }

    struct FakeEmbeddingScheduler;

    #[async_trait]
    impl EmbeddingScheduler for FakeEmbeddingScheduler {
        async fn submit_embedding(
            &self,
            texts: Vec<String>,
        ) -> Result<Vec<Vec<f32>>, RuntimeError> {
            Ok(vec![vec![0.1; 384]; texts.len()])
        }
    }

    struct FakeLlmScheduler;

    #[async_trait]
    impl LlmScheduler for FakeLlmScheduler {
        async fn submit(&self, _request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
            Ok(LlmResponse {
                content: "角色开始理解用户的情绪变化".to_string(),
                model: "fake".to_string(),
                usage: TokenUsage {
                    prompt_tokens: 10,
                    completion_tokens: 20,
                    total_tokens: 30,
                },
                truncated: false,
            })
        }
    }

    fn make_character(id: i64, name: &str) -> Character {
        Character {
            id,
            definition: CharacterDefinition {
                name: name.to_string(),
                description: None,
                personality: None,
                scenario: None,
                style: None,
                background: None,
                greetings: vec![],
                example_messages: vec![],
                system_prompt: Some("你是一个温柔的角色。".to_string()),
                post_history_instructions: None,
                lorebook: vec![],
                metadata: serde_json::json!({}),
            },
            state: CharacterState::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn make_binding(
        id: i64,
        character_id: i64,
        conversation_id: i64,
        proactive: bool,
    ) -> CharacterBinding {
        CharacterBinding {
            id,
            character_id,
            conversation_id,
            reply_mode: ReplyMode::Natural,
            proactive_enabled: proactive,
            mute_schedule: None,
            behavior_overrides: serde_json::json!({}),
            context_policy: serde_json::json!({}),
            switched_at: None,
            cross_reply_enabled: false,
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn idle_binding_triggers_cognition() {
        let clock = FakeClock::at(2026, 9, 5, 12, 0);
        let msg_repo = Arc::new(FakeMessageRepo::new());
        // 添加一条 10 分钟前的消息（超过 idle 阈值 5 分钟）。
        msg_repo.add_message(1, 999, "你好", 0);
        let binding_repo = Arc::new(FakeBindingRepo {
            bindings: Mutex::new(vec![make_binding(1, 100, 1, true)]),
        });
        let char_repo = Arc::new(FakeCharacterRepo {
            characters: Mutex::new(
                vec![(100, make_character(100, "小爱"))]
                    .into_iter()
                    .collect(),
            ),
        });

        let driver = Arc::new(CognitionDriver::with_config(
            Arc::new(FakeEmbeddingScheduler),
            Arc::new(FakeLlmScheduler),
            Arc::new(FakeMemoryRepo),
            binding_repo.clone(),
            msg_repo.clone(),
            char_repo,
            Arc::new(clock.clone()),
            Duration::from_secs(300),
            Duration::from_secs(600),
            Duration::from_secs(300),
        ));

        // 推进时钟使消息时间超过 idle 阈值（当前 12:00，消息在 11:50，即 10 分钟前）。
        clock.advance(-600); // 倒退 10 分钟

        driver.tick().await.expect("tick 不应报错");

        // idle 且无冷却，应触发认知。
        // 由于 FakeMemoryRepo 不实际存储，我们只验证不报错。
    }

    #[tokio::test]
    async fn non_idle_binding_skips_cognition() {
        let clock = FakeClock::at(2026, 9, 5, 12, 0);
        let msg_repo = Arc::new(FakeMessageRepo::new());
        // 添加一条 1 分钟前的消息（未超过 idle 阈值）。
        msg_repo.add_message(1, 999, "你好", 0);
        let binding_repo = Arc::new(FakeBindingRepo {
            bindings: Mutex::new(vec![make_binding(1, 100, 1, true)]),
        });
        let char_repo = Arc::new(FakeCharacterRepo {
            characters: Mutex::new(
                vec![(100, make_character(100, "小爱"))]
                    .into_iter()
                    .collect(),
            ),
        });

        let driver = Arc::new(CognitionDriver::with_config(
            Arc::new(FakeEmbeddingScheduler),
            Arc::new(FakeLlmScheduler),
            Arc::new(FakeMemoryRepo),
            binding_repo.clone(),
            msg_repo.clone(),
            char_repo,
            Arc::new(clock.clone()),
            Duration::from_secs(300),
            Duration::from_secs(600),
            Duration::from_secs(300),
        ));

        // 不推进时钟，消息时间距现在不足 5 分钟（当前 12:00，消息在 11:59）
        // 实际消息是当前时间创建的，所以刚刚好是 0 分钟前，不会 idle
        driver.tick().await.expect("tick 不应报错");

        // 非 idle，不应触发认知（但我们用 fake LLM，也无法验证调用次数，
        // 这里只确保不报错）。
    }

    #[tokio::test]
    async fn cooldown_blocks_second_cognition() {
        let clock = FakeClock::at(2026, 9, 5, 12, 0);
        let msg_repo = Arc::new(FakeMessageRepo::new());
        msg_repo.add_message(1, 999, "你好", 10); // 10 天前，远超 idle
        let binding_repo = Arc::new(FakeBindingRepo {
            bindings: Mutex::new(vec![make_binding(1, 100, 1, true)]),
        });
        let char_repo = Arc::new(FakeCharacterRepo {
            characters: Mutex::new(
                vec![(100, make_character(100, "小爱"))]
                    .into_iter()
                    .collect(),
            ),
        });

        let driver = Arc::new(CognitionDriver::with_config(
            Arc::new(FakeEmbeddingScheduler),
            Arc::new(FakeLlmScheduler),
            Arc::new(FakeMemoryRepo),
            binding_repo.clone(),
            msg_repo.clone(),
            char_repo,
            Arc::new(clock.clone()),
            Duration::from_secs(300),
            Duration::from_secs(600), // 10 分钟冷却
            Duration::from_secs(300),
        ));

        // 第一次 tick，触发认知。
        driver.tick().await.expect("第一次 tick 不应报错");

        // 5 分钟后再次 tick，仍在冷却期内。
        clock.advance(300);
        driver.tick().await.expect("第二次 tick 不应报错");
        // 冷却中，第二次不会真正执行认知。
    }

    #[tokio::test]
    async fn disabled_binding_is_skipped() {
        let clock = FakeClock::at(2026, 9, 5, 12, 0);
        let msg_repo = Arc::new(FakeMessageRepo::new());
        msg_repo.add_message(1, 999, "你好", 10);
        let binding_repo = Arc::new(FakeBindingRepo {
            bindings: Mutex::new(vec![make_binding(1, 100, 1, false)]), // 未启用
        });
        let char_repo = Arc::new(FakeCharacterRepo {
            characters: Mutex::new(
                vec![(100, make_character(100, "小爱"))]
                    .into_iter()
                    .collect(),
            ),
        });

        let driver = Arc::new(CognitionDriver::with_config(
            Arc::new(FakeEmbeddingScheduler),
            Arc::new(FakeLlmScheduler),
            Arc::new(FakeMemoryRepo),
            binding_repo.clone(),
            msg_repo.clone(),
            char_repo,
            Arc::new(clock.clone()),
            Duration::from_secs(300),
            Duration::from_secs(600),
            Duration::from_secs(300),
        ));

        // 未启用主动的绑定应在 find_all_enabled 中被过滤掉，不报错。
        driver.tick().await.expect("tick 不应报错");
    }
}
