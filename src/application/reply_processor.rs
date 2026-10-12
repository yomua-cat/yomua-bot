//! Reply Pipeline —— 收到消息后的回复编排器。
//!
//! 负责把一条 `MessageReceivedEvent` 走完整个回复流程：
//! 确定参与角色 → 更新关系/情绪 → 组装上下文 → 行为决策 →
//! （可选）认知生成 → 分派发送动作。供 [`crate::application::event_processor::EventProcessor`]
//! 调用。

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use rand::seq::SliceRandom;

#[allow(unused_imports)]
use crate::adapters::onebot::ActionResponse;
use crate::application::action::ActionDispatcher;
use crate::application::binding::BindingManager;
use crate::application::cognition::CognitionLayer;
use crate::application::context::ContextLimits;
use crate::application::event_bus::EventBus;
use crate::application::memory_service::MemoryService;
use crate::application::mood_service::MoodService;
use crate::application::relationship_service::RelationshipService;
use crate::application::runtime::CharacterRuntime;
use crate::domain::behavior::{Action, BehaviorAction};
use crate::domain::conversation::ParticipantRole;
use crate::domain::event::{
    BehaviorDecidedEvent, CoreEvent, MessageReceivedEvent, ResponseGeneratedEvent, ResponseSource,
};
use crate::domain::outbox::ProcessingOutcome;
use crate::domain::repository::ParticipantRepository;
use crate::error::RuntimeError;

/// 单次回复最长允许的拟人化延迟（毫秒）。
///
/// 行为引擎给出的延时（用于模拟输入间隔）可能较大；为了不让一条普通消息
/// 长时间卡住回复链路，这里统一截断到该上限。
const MAX_REPLY_DELAY_MS: u64 = 3_000;

/// 把行为引擎给出的延迟截断到安全上限。
fn capped_delay_ms(delay_ms: u64) -> u64 {
    delay_ms.min(MAX_REPLY_DELAY_MS)
}

/// 待发送的回复（延迟发送，打乱顺序）。
struct PendingReply {
    /// 所属绑定 ID（用于日志追踪）。
    binding_id: i64,
    /// 回复内容。
    content: String,
    /// 目标会话 ID。
    conversation_id: i64,
    /// 延迟时长。
    delay: Duration,
}

/// 延迟执行器 —— 把"等待 N 毫秒"抽象为可注入依赖。
///
/// 生产环境用 [`TokioDelayExecutor`] 真实 `sleep`，测试注入记录型/受控假执行器，
/// 避免测试真实等待。
#[async_trait::async_trait]
pub trait DelayExecutor: Send + Sync {
    /// 等待指定的毫秒数。
    async fn delay(&self, ms: u64);
}

/// 生产环境：真实等待指定毫秒数。
pub struct TokioDelayExecutor;

#[async_trait::async_trait]
impl DelayExecutor for TokioDelayExecutor {
    async fn delay(&self, ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}

/// 回复处理器 —— 编排一条消息的完整回复链路。
pub struct ReplyProcessor {
    runtime: Arc<CharacterRuntime>,
    binding_manager: Arc<BindingManager>,
    decision_orchestrator: crate::application::decision_orchestrator::DecisionOrchestrator,
    cognition: Arc<CognitionLayer>,
    relationship_service: Arc<RelationshipService>,
    mood_service: Arc<MoodService>,
    memory_service: Arc<MemoryService>,
    action_dispatcher: Arc<ActionDispatcher>,
    event_bus: EventBus,
    delay_executor: Arc<dyn DelayExecutor>,
    /// 参与者仓储（用于跨回复场景：查询当前 Bot 的 participant_id）。
    participant_repo: Arc<dyn ParticipantRepository>,
    /// 当前 Bot 在 OneBot 中的账号 ID（QQ 号），用于精确识别"当前 Bot 自己"。
    /// 在单 Bot 部署中从 OneBotConfig 传入；在多 Bot 场景下由各 Bot 独立持有。
    bot_self_id: Option<i64>,
}

impl ReplyProcessor {
    /// 创建回复处理器。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: Arc<CharacterRuntime>,
        binding_manager: Arc<BindingManager>,
        decision_orchestrator: crate::application::decision_orchestrator::DecisionOrchestrator,
        cognition: Arc<CognitionLayer>,
        relationship_service: Arc<RelationshipService>,
        mood_service: Arc<MoodService>,
        memory_service: Arc<MemoryService>,
        action_dispatcher: Arc<ActionDispatcher>,
        event_bus: EventBus,
        delay_executor: Arc<dyn DelayExecutor>,
        participant_repo: Arc<dyn ParticipantRepository>,
        bot_self_id: Option<i64>,
    ) -> Self {
        Self {
            runtime,
            binding_manager,
            decision_orchestrator,
            cognition,
            relationship_service,
            mood_service,
            memory_service,
            action_dispatcher,
            event_bus,
            delay_executor,
            participant_repo,
            bot_self_id,
        }
    }

    /// 处理一条收到的消息事件。
    ///
    /// 遍历该会话的所有绑定，每个绑定独立决策是否回复。
    /// 多 Bot 共群时，所有待发送回复延迟随机打乱后顺序发送。
    ///
    /// 注意：Message 持久化由 MessagePersistence 负责，ReplyProcessor 不再 INSERT 消息。
    /// event.character_id 是 immutable processing attribution，已由 InboundProcessor 设置。
    pub async fn process(&self, event: &MessageReceivedEvent) -> Result<(), RuntimeError> {
        // 1. 获取该会话的所有绑定。
        let bindings = self
            .binding_manager
            .by_conversation(event.conversation_id)
            .await?;
        if bindings.is_empty() {
            tracing::debug!(
                target: "runtime",
                conversation_id = event.conversation_id,
                "该会话未绑定角色，忽略消息"
            );
            return Ok(());
        }

        // 2. 收集所有待发送回复（延迟发送，打乱顺序）。
        let mut pending_replies: Vec<PendingReply> = Vec::new();

        for binding in &bindings {
            // 2a. Sender 类型过滤：区分普通用户 / 当前 Bot 自己 / 其他 Bot。
            //
            // semantics:
            // - 普通用户（role=user）：始终正常处理
            // - 当前 Bot 自己（role=character 且 external_id 匹配 bot_self_id）：始终跳过（防止自复读）
            // - 其他 Bot（role=character 但 external_id 不同）：仅在 cross_reply_enabled=true 时处理
            let my_character = match self.get_my_character_participant(binding).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(
                        target: "runtime",
                        binding_id = binding.id,
                        "获取当前 Bot participant 失败: {e}"
                    );
                    continue;
                }
            };

            let sender = match self.participant_repo.find_by_id(event.sender_id).await {
                Ok(Some(p)) => p,
                Ok(None) => {
                    tracing::warn!(
                        target: "runtime",
                        sender_id = event.sender_id,
                        "发送者 participant 不存在"
                    );
                    continue;
                }
                Err(e) => {
                    tracing::warn!(target: "runtime", "查询发送者 participant 失败: {e}");
                    continue;
                }
            };

            // Case A: 普通用户 → 正常处理
            let classification = if sender.role == ParticipantRole::User {
                "User".to_string()
            } else if sender.role == ParticipantRole::Character
                && sender.external_id == my_character.external_id
            {
                "SelfBot".to_string()
            } else {
                "OtherBot".to_string()
            };
            tracing::info!(
                target = "runtime",
                conversation_id = event.conversation_id,
                sender_id = event.sender_id,
                sender_role = ?sender.role,
                sender_external_id = %sender.external_id,
                my_character_external_id = %my_character.external_id,
                cross_reply_enabled = binding.cross_reply_enabled,
                classification = %classification,
                "E2E_TRACE: Sender classification"
            );
            if sender.role == ParticipantRole::User {
                // pass through to reply pipeline
            }
            // Case B: 当前 Bot 自己 → 跳过
            else if sender.role == ParticipantRole::Character
                && sender.external_id == my_character.external_id
            {
                tracing::info!(target = "runtime", "E2E_TRACE: Self-bot，跳过");
                continue;
            }
            // Case C: 其他 Bot
            else {
                if !binding.cross_reply_enabled {
                    tracing::info!(
                        target = "runtime",
                        "E2E_TRACE: OtherBot + cross_reply=false，跳过"
                    );
                    continue;
                }
            }

            let character_id = binding.character_id;

            // 2b. 加载角色（运行时缓存 + 状态持久化）。
            let character = match self.runtime.load_character(character_id).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(
                        target: "runtime",
                        binding_id = binding.id,
                        character_id,
                        "加载角色失败: {e}"
                    );
                    continue;
                }
            };

            // 2c. 记忆提取：用户消息可能包含值得长期记住的信息（确定性启发式）。
            if let Err(e) = self
                .memory_service
                .extract_and_store(
                    character_id,
                    Some(event.conversation_id),
                    event.sender_id,
                    &event.content,
                )
                .await
            {
                tracing::warn!(target: "runtime", binding_id = binding.id, "记忆提取失败: {e}");
            }

            // 2d. 更新关系与情绪并持久化。
            if let Err(e) = self
                .relationship_service
                .record_interaction(character_id, event.sender_id)
                .await
            {
                tracing::warn!(target: "runtime", binding_id = binding.id, "关系更新失败: {e}");
            }
            if let Err(e) = self
                .mood_service
                .apply_message_event(character_id, event.conversation_id, &event.content)
                .await
            {
                tracing::warn!(target: "runtime", binding_id = binding.id, "情绪更新失败: {e}");
            }

            // 2e. 行为决策（经 DecisionOrchestrator，支持 LLM assistance 扩展）。
            let decision = match self
                .decision_orchestrator
                .evaluate_response(
                    character_id,
                    event.conversation_id,
                    &event.content,
                    event.is_mentioned,
                    Some(event.sender_id),
                )
                .await
            {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(
                        target = "runtime",
                        binding_id = binding.id,
                        "行为决策失败: {e}"
                    );
                    continue;
                }
            };

            self.event_bus
                .publish(&CoreEvent::BehaviorDecided(BehaviorDecidedEvent {
                    character_id,
                    conversation_id: event.conversation_id,
                    action: format!("{:?}", decision.action),
                    reason: decision.reason.clone(),
                    timestamp: Utc::now(),
                }));

            // 仅回复动作继续。
            if decision.action != BehaviorAction::Reply {
                tracing::info!(
                    target = "runtime",
                    conversation_id = event.conversation_id,
                    character_id,
                    action = ?decision.action,
                    reason = %decision.reason,
                    "E2E_TRACE: BehaviorEngine 决定不回复"
                );
                continue;
            }

            tracing::info!(
                target = "runtime",
                conversation_id = event.conversation_id,
                character_id,
                "E2E_TRACE: BehaviorEngine 决定 Reply，准备生成回复"
            );

            // 2f. 生成响应内容。
            let content = match self.generate_reply(event, &character, character_id).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(target: "runtime", binding_id = binding.id, "生成回复失败: {e}");
                    continue;
                }
            };

            tracing::info!(
                target = "runtime",
                conversation_id = event.conversation_id,
                content_len = content.len(),
                "E2E_TRACE: 回复内容已生成，准备发送"
            );

            // 2g. 收集待发送回复，延迟使用 BehaviorEngine 返回的延迟（截断到安全上限）。
            pending_replies.push(PendingReply {
                binding_id: binding.id,
                content,
                conversation_id: event.conversation_id,
                delay: Duration::from_millis(capped_delay_ms(decision.delay_ms)),
            });
        }

        // 3. 延迟 + 随机打乱顺序后发送。
        if pending_replies.is_empty() {
            tracing::info!(
                target = "runtime",
                conversation_id = event.conversation_id,
                "E2E_TRACE: 无待发送回复"
            );
            return Ok(());
        }
        pending_replies.shuffle(&mut rand::thread_rng());
        for pending in pending_replies {
            self.delay_executor
                .delay(pending.delay.as_millis() as u64)
                .await;
            if let Err(e) = self
                .action_dispatcher
                .execute(&Action::SendMessage {
                    conversation_id: pending.conversation_id,
                    content: pending.content,
                })
                .await
            {
                // 消息发送失败是严重问题：用户看不到回复，且无重试机制。
                // 使用 error 级别以便运维及时发现。
                tracing::error!(
                    target: "runtime",
                    binding_id = pending.binding_id,
                    error = %e,
                    "发送回复失败"
                );
            }
        }

        Ok(())
    }

    /// 处理消息并返回 ProcessingOutcome（用于 outbox 系统）。
    ///
    /// 此方法是 `process()` 的包装，返回 `ProcessingOutcome` 而非 `Result`。
    /// 所有内部错误均映射为 `RetryableFailure`，因为：
    /// - binding/memory/relationship/mood/decision/cognition/action 失败都是非致命的
    /// - 这些错误不应阻止消息处理流程继续
    ///
    /// 永久性失败（如数据库连接完全断开）会返回 `PermanentFailure`。
    pub async fn process_for_outbox(
        &self,
        event: &MessageReceivedEvent,
    ) -> Result<ProcessingOutcome, RuntimeError> {
        match self.process(event).await {
            Ok(()) => Ok(ProcessingOutcome::Success),
            Err(e) => {
                // 检查是否为永久性错误
                let error_str = e.to_string();
                let is_permanent = matches!(
                    e,
                    RuntimeError::Repository(crate::error::RepositoryError::Database(_))
                        | RuntimeError::Storage(crate::error::StorageError::Connection(_))
                );

                if is_permanent {
                    tracing::error!(
                        target: "outbox",
                        conversation_id = event.conversation_id,
                        error = %e,
                        "永久性错误"
                    );
                    Ok(ProcessingOutcome::PermanentFailure { error: error_str })
                } else {
                    // 其他错误视为可重试
                    tracing::warn!(
                        target: "outbox",
                        conversation_id = event.conversation_id,
                        error = %e,
                        "可重试错误"
                    );
                    Ok(ProcessingOutcome::RetryableFailure { error: error_str })
                }
            }
        }
    }

    /// 生成回复内容：LLM 未启用时走确定性回复，启用时经认知层。
    async fn generate_reply(
        &self,
        event: &MessageReceivedEvent,
        character: &crate::domain::character::Character,
        character_id: i64,
    ) -> Result<String, RuntimeError> {
        if self.cognition.enabled() {
            // C2-3: 当 conversation_sequence > 0 时使用 sequence-based 历史过滤
            let current_sequence = if event.conversation_sequence > 0 {
                Some(event.conversation_sequence)
            } else {
                None
            };
            if let Some(content) = self
                .cognition
                .generate(
                    character,
                    event.conversation_id,
                    event.sender_id,
                    &event.content,
                    event.is_mentioned,
                    ContextLimits::default(),
                    current_sequence,
                )
                .await?
            {
                self.event_bus
                    .publish(&CoreEvent::ResponseGenerated(ResponseGeneratedEvent {
                        character_id,
                        conversation_id: event.conversation_id,
                        content: content.clone(),
                        source: ResponseSource::Llm,
                        timestamp: Utc::now(),
                    }));
                return Ok(content);
            }
        }

        // 确定性回复（LLM 是能力不是生命线）。
        let content = deterministic_reply(&event.content);
        self.event_bus
            .publish(&CoreEvent::ResponseGenerated(ResponseGeneratedEvent {
                character_id,
                conversation_id: event.conversation_id,
                content: content.clone(),
                source: ResponseSource::Rule,
                timestamp: Utc::now(),
            }));
        Ok(content)
    }

    /// 获取当前 Bot 在该 binding 对应会话中的 character participant。
    ///
    /// 如果 `bot_self_id` 已设置，精确匹配 `external_id = bot_self_id.to_string()`；
    /// 否则返回第一个 role='character' 的 participant（单 Bot 场景下行为一致）。
    async fn get_my_character_participant(
        &self,
        binding: &crate::domain::character::CharacterBinding,
    ) -> Result<crate::domain::conversation::Participant, RuntimeError> {
        let participants = self
            .participant_repo
            .find_by_conversation_id(binding.conversation_id)
            .await
            .map_err(RuntimeError::Repository)?;

        let bot_external_id = self.bot_self_id.map(|id| id.to_string());

        let character_participant = if let Some(ref ext_id) = bot_external_id {
            // 精确匹配：role='character' AND external_id = bot_self_id
            participants
                .into_iter()
                .find(|p| p.role == ParticipantRole::Character && p.external_id == *ext_id)
        } else {
            // 回退：第一个 role='character'
            participants
                .into_iter()
                .find(|p| p.role == ParticipantRole::Character)
        };

        character_participant.ok_or_else(|| {
            RuntimeError::Repository(crate::error::RepositoryError::NotFound(format!(
                "未找到会话 {} 中的 character 参与者",
                binding.conversation_id
            )))
        })
    }
}

/// 在未启用 LLM 时给出的确定性礼貌确认。
fn deterministic_reply(user_message: &str) -> String {
    let trimmed = user_message.trim();
    if trimmed.is_empty() {
        "嗯？".to_string()
    } else {
        // 截断过长的回声，避免无意义地重复长文本。
        let preview: String = trimmed.chars().take(40).collect();
        format!("我听到了：{preview}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::onebot::{OneBotAdapter, OneBotConnectionState};
    use crate::application::context::ContextBuilder;
    use crate::application::context_assembler::{
        ContextAssembler, ModelCapabilitiesProviderAdapter, TokenEstimatorAdapter,
    };
    use crate::domain::character::{
        Character, CharacterBinding, CharacterDefinition, CharacterState,
    };
    use crate::domain::conversation::{Conversation, ConversationType};
    use crate::domain::emotion::Mood;
    use crate::domain::memory::Memory;
    use crate::domain::message::Message;
    use crate::domain::relationship::Relationship;
    use crate::domain::repository::{
        CharacterBindingRepository, CharacterRepository, CharacterStateRepository,
        ConversationRepository, MemoryRepository, MessageRepository, MoodRepository,
        ParticipantRepository, RelationshipRepository,
    };
    use crate::error::RepositoryError;
    use crate::infrastructure::llm::{
        ApproximateTokenEstimator, FixedModelCapabilities, LlmProvider, LlmRequest, LlmResponse,
        TokenUsage,
    };
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio::sync::{mpsc, oneshot};

    // ---- 内存仓储 ----

    struct MemCharacterRepo {
        chars: Mutex<Vec<Character>>,
    }
    #[async_trait]
    impl CharacterRepository for MemCharacterRepo {
        async fn find_by_id(&self, id: i64) -> Result<Option<Character>, RepositoryError> {
            Ok(self
                .chars
                .lock()
                .unwrap()
                .iter()
                .find(|c| c.id == id)
                .cloned())
        }
        async fn find_all(&self) -> Result<Vec<Character>, RepositoryError> {
            Ok(self.chars.lock().unwrap().clone())
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

    struct MemConversationStateRepo {
        states: Mutex<HashMap<(i64, i64), crate::domain::character::ConversationState>>,
    }
    #[async_trait]
    impl crate::domain::repository::ConversationStateRepository for MemConversationStateRepo {
        async fn find_by_character_and_conversation(
            &self,
            character_id: i64,
            conversation_id: i64,
        ) -> Result<Option<crate::domain::character::ConversationState>, RepositoryError> {
            Ok(self
                .states
                .lock()
                .unwrap()
                .get(&(character_id, conversation_id))
                .cloned())
        }
        async fn upsert(
            &self,
            character_id: i64,
            conversation_id: i64,
            state: &crate::domain::character::ConversationState,
        ) -> Result<(), RepositoryError> {
            self.states
                .lock()
                .unwrap()
                .insert((character_id, conversation_id), state.clone());
            Ok(())
        }
    }

    struct MemBehaviorStateRepo {
        states: Mutex<HashMap<(i64, i64), crate::domain::character::BehaviorState>>,
    }
    #[async_trait]
    impl crate::domain::repository::BehaviorStateRepository for MemBehaviorStateRepo {
        async fn find_by_character_and_conversation(
            &self,
            character_id: i64,
            conversation_id: i64,
        ) -> Result<Option<crate::domain::character::BehaviorState>, RepositoryError> {
            Ok(self
                .states
                .lock()
                .unwrap()
                .get(&(character_id, conversation_id))
                .cloned())
        }
        async fn upsert(
            &self,
            character_id: i64,
            conversation_id: i64,
            state: &crate::domain::character::BehaviorState,
        ) -> Result<(), RepositoryError> {
            self.states
                .lock()
                .unwrap()
                .insert((character_id, conversation_id), state.clone());
            Ok(())
        }
    }

    struct MemStateRepo {
        states: Mutex<HashMap<i64, CharacterState>>,
    }
    #[async_trait]
    impl CharacterStateRepository for MemStateRepo {
        async fn find_by_character_id(
            &self,
            id: i64,
        ) -> Result<Option<CharacterState>, RepositoryError> {
            Ok(self.states.lock().unwrap().get(&id).cloned())
        }
        async fn upsert(&self, id: i64, state: &CharacterState) -> Result<(), RepositoryError> {
            self.states.lock().unwrap().insert(id, state.clone());
            Ok(())
        }
    }

    struct MemBindingRepo {
        bindings: Mutex<Vec<CharacterBinding>>,
    }
    #[async_trait]
    impl CharacterBindingRepository for MemBindingRepo {
        async fn find_by_character_id(
            &self,
            _id: i64,
        ) -> Result<Vec<CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_by_conversation_id(
            &self,
            conversation_id: i64,
        ) -> Result<Vec<CharacterBinding>, RepositoryError> {
            Ok(self
                .bindings
                .lock()
                .unwrap()
                .iter()
                .filter(|b| b.conversation_id == conversation_id)
                .cloned()
                .collect())
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
        async fn update(&self, binding: &CharacterBinding) -> Result<(), RepositoryError> {
            let mut bindings = self.bindings.lock().unwrap();
            if let Some(existing) = bindings.iter_mut().find(|b| b.id == binding.id) {
                *existing = binding.clone();
            }
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemConvRepo {
        convs: Mutex<Vec<Conversation>>,
    }
    #[async_trait]
    impl ConversationRepository for MemConvRepo {
        async fn find_by_id(&self, id: i64) -> Result<Option<Conversation>, RepositoryError> {
            Ok(self
                .convs
                .lock()
                .unwrap()
                .iter()
                .find(|c| c.id == id)
                .cloned())
        }
        async fn find_by_external_id(
            &self,
            _id: &str,
        ) -> Result<Option<Conversation>, RepositoryError> {
            Ok(None)
        }
        async fn find_all(&self) -> Result<Vec<Conversation>, RepositoryError> {
            Ok(self.convs.lock().unwrap().clone())
        }
        async fn insert(&self, _c: &Conversation) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _c: &Conversation) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn allocate_sequence(&self, _conversation_id: i64) -> Result<i64, RepositoryError> {
            Ok(1) // Mock 实现
        }
    }

    struct MemMessageRepo {
        messages: Mutex<Vec<Message>>,
    }
    #[async_trait]
    impl MessageRepository for MemMessageRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Message>, RepositoryError> {
            Ok(None)
        }
        async fn find_recent(
            &self,
            _conversation_id: i64,
            limit: i64,
        ) -> Result<Vec<Message>, RepositoryError> {
            let mut all = self.messages.lock().unwrap().clone();
            all.sort_by_key(|m| m.timestamp);
            all.truncate(limit as usize);
            Ok(all)
        }
        async fn find_recent_attributed(
            &self,
            _conversation_id: i64,
            character_id: i64,
            limit: i64,
        ) -> Result<Vec<Message>, RepositoryError> {
            let mut filtered: Vec<Message> = self
                .messages
                .lock()
                .unwrap()
                .iter()
                .filter(|m| m.active_character_id == Some(character_id))
                .cloned()
                .collect();
            // 模拟 SQL: ORDER BY timestamp DESC, id DESC
            filtered.sort_by(|a, b| {
                let time_cmp = b.timestamp.cmp(&a.timestamp); // DESC
                if time_cmp == std::cmp::Ordering::Equal {
                    b.id.cmp(&a.id) // DESC
                } else {
                    time_cmp
                }
            });
            filtered.truncate(limit as usize);
            // 反转顺序，使最旧的消息排在前面（与 Repository 契约一致）
            filtered.reverse();
            Ok(filtered)
        }
        async fn insert(&self, _m: &Message) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn latest_message_time(
            &self,
            _conversation_id: i64,
        ) -> Result<Option<chrono::DateTime<chrono::Utc>>, RepositoryError> {
            Ok(None)
        }
    }

    struct MemMemoryRepo;
    #[async_trait]
    impl MemoryRepository for MemMemoryRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Memory>, RepositoryError> {
            Ok(None)
        }
        async fn find_by_character_id(
            &self,
            _id: i64,
            _t: Option<crate::domain::memory::MemoryType>,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(vec![])
        }
        async fn search_by_keywords(
            &self,
            _character_id: i64,
            _keywords: &[String],
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            // 桩实现无持久存储，返回空结果。
            Ok(vec![])
        }
        async fn insert(&self, _m: &Memory) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _m: &Memory) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn find_all(&self, _limit: i64) -> Result<Vec<Memory>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_by_conversation_id(
            &self,
            _conversation_id: i64,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_filtered(
            &self,
            _character_id: Option<i64>,
            _conversation_id: Option<i64>,
            _memory_type: Option<crate::domain::memory::MemoryType>,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(vec![])
        }
    }

    struct MemRelationshipRepo {
        rels: Mutex<Vec<Relationship>>,
    }
    #[async_trait]
    impl RelationshipRepository for MemRelationshipRepo {
        async fn find(
            &self,
            character_id: i64,
            participant_id: i64,
        ) -> Result<Option<Relationship>, RepositoryError> {
            Ok(self
                .rels
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.character_id == character_id && r.participant_id == participant_id)
                .cloned())
        }
        async fn find_by_character_id(
            &self,
            _id: i64,
        ) -> Result<Vec<Relationship>, RepositoryError> {
            Ok(vec![])
        }
        async fn upsert(&self, r: &Relationship) -> Result<(), RepositoryError> {
            let mut all = self.rels.lock().unwrap();
            if let Some(existing) = all
                .iter_mut()
                .find(|x| x.character_id == r.character_id && x.participant_id == r.participant_id)
            {
                *existing = r.clone();
            } else {
                all.push(r.clone());
            }
            Ok(())
        }
    }

    struct MemMoodRepo {
        moods: Mutex<HashMap<(i64, i64), Mood>>,
    }
    #[async_trait]
    impl MoodRepository for MemMoodRepo {
        async fn find_by_character_and_conversation(
            &self,
            character_id: i64,
            conversation_id: i64,
        ) -> Result<Option<Mood>, RepositoryError> {
            Ok(self
                .moods
                .lock()
                .unwrap()
                .get(&(character_id, conversation_id))
                .cloned())
        }
        async fn upsert(
            &self,
            character_id: i64,
            conversation_id: i64,
            state: &Mood,
        ) -> Result<(), RepositoryError> {
            self.moods
                .lock()
                .unwrap()
                .insert((character_id, conversation_id), state.clone());
            Ok(())
        }
    }

    struct MemParticipantRepo {
        /// 预设的参与者列表（每个会话一个 role='character' 的 Bot 参与者）。
        participants: Mutex<Vec<crate::domain::conversation::Participant>>,
    }
    #[async_trait]
    impl ParticipantRepository for MemParticipantRepo {
        async fn find_by_id(
            &self,
            id: i64,
        ) -> Result<Option<crate::domain::conversation::Participant>, RepositoryError> {
            Ok(self
                .participants
                .lock()
                .unwrap()
                .iter()
                .find(|p| p.id == id)
                .cloned())
        }
        async fn find_by_external_id(
            &self,
            _conversation_id: i64,
            _external_id: &str,
        ) -> Result<Option<crate::domain::conversation::Participant>, RepositoryError> {
            Ok(None)
        }
        async fn find_by_conversation_id(
            &self,
            conversation_id: i64,
        ) -> Result<Vec<crate::domain::conversation::Participant>, RepositoryError> {
            Ok(self
                .participants
                .lock()
                .unwrap()
                .iter()
                .filter(|p| p.conversation_id == conversation_id)
                .cloned()
                .collect())
        }
        async fn insert(
            &self,
            _participant: &crate::domain::conversation::Participant,
        ) -> Result<i64, RepositoryError> {
            Ok(1)
        }
    }

    // ---- 假 LLM Provider 与假适配器 ----

    struct FakeProvider {
        calls: Mutex<usize>,
    }
    #[async_trait]
    impl LlmProvider for FakeProvider {
        async fn generate(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
            *self.calls.lock().unwrap() += 1;
            Ok(LlmResponse {
                content: format!("AI 回复：{}", request.messages[0].content),
                model: "fake".to_string(),
                usage: TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                },
                truncated: false,
            })
        }
        async fn health_check(&self) -> Result<bool, RuntimeError> {
            Ok(true)
        }
        fn name(&self) -> &str {
            "fake"
        }
    }

    struct FakeAdapter {
        sent: Mutex<Vec<(String, String)>>,
    }
    #[async_trait]
    impl OneBotAdapter for FakeAdapter {
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
            self.sent
                .lock()
                .unwrap()
                .push((group_id.to_string(), content.to_string()));
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
            self.sent
                .lock()
                .unwrap()
                .push((user_id.to_string(), content.to_string()));
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

    // ---- 延迟执行器假实现 ----

    /// 记录每次请求的延迟后立即返回（不真正等待），用于默认装配。
    struct RecordingDelay {
        delays: Arc<Mutex<Vec<u64>>>,
    }
    #[async_trait]
    impl DelayExecutor for RecordingDelay {
        async fn delay(&self, ms: u64) {
            self.delays.lock().unwrap().push(ms);
        }
    }

    /// 受控延迟：记录延迟并经通道通知测试，随后阻塞直到测试放行。
    ///
    /// 用于验证"发送前确实等待了延迟"的时序。
    struct ControlledDelay {
        delays: Arc<Mutex<Vec<u64>>>,
        entered: mpsc::UnboundedSender<u64>,
        release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    }
    #[async_trait]
    impl DelayExecutor for ControlledDelay {
        async fn delay(&self, ms: u64) {
            self.delays.lock().unwrap().push(ms);
            let _ = self.entered.send(ms);
            let release = self.release.lock().unwrap().take();
            if let Some(rx) = release {
                let _ = rx.await;
            }
        }
    }

    // ---- 装配 ----

    fn sample_character() -> Character {
        Character {
            id: 1,
            definition: CharacterDefinition {
                name: "Alice".to_string(),
                description: Some("温柔的咖啡师".to_string()),
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
        }
    }

    type Wiring = (
        Arc<ReplyProcessor>,
        Arc<FakeAdapter>,
        Arc<FakeProvider>,
        Arc<MemMoodRepo>,
        Arc<MemRelationshipRepo>,
        Arc<MemStateRepo>,
        Arc<MemBindingRepo>,
        Arc<MemParticipantRepo>,
        Arc<MemConversationStateRepo>,
        Arc<MemBehaviorStateRepo>,
    );

    async fn wire(onbot_conv: bool, use_llm: bool) -> Wiring {
        let delay: Arc<dyn DelayExecutor> = Arc::new(RecordingDelay {
            delays: Arc::new(Mutex::new(vec![])),
        });
        wire_with_delay(onbot_conv, use_llm, delay).await
    }

    async fn wire_with_delay(
        onbot_conv: bool,
        use_llm: bool,
        delay_executor: Arc<dyn DelayExecutor>,
    ) -> Wiring {
        let character = sample_character();
        let character_repo = Arc::new(MemCharacterRepo {
            chars: Mutex::new(vec![character.clone()]),
        });
        let character_repo_trait: Arc<dyn CharacterRepository> = character_repo.clone();
        let state_repo = Arc::new(MemStateRepo {
            states: Mutex::new(HashMap::new()),
        });
        let binding_repo = Arc::new(MemBindingRepo {
            bindings: Mutex::new(vec![CharacterBinding {
                id: 1,
                character_id: 1,
                conversation_id: 100,
                reply_mode: crate::domain::character::ReplyMode::MentionOnly,
                proactive_enabled: false,
                mute_schedule: None,
                behavior_overrides: serde_json::json!({}),
                context_policy: serde_json::json!({}),
                switched_at: None,
                cross_reply_enabled: false,
                created_at: chrono::Utc::now(),
            }]),
        });
        let conv_repo = Arc::new(MemConvRepo {
            convs: Mutex::new(vec![Conversation {
                id: 100,
                conversation_type: if onbot_conv {
                    ConversationType::Group
                } else {
                    ConversationType::Private
                },
                external_id: if onbot_conv {
                    "g100".to_string()
                } else {
                    "u100".to_string()
                },
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            }]),
        });
        let message_repo = Arc::new(MemMessageRepo {
            messages: Mutex::new(vec![]),
        });
        let memory_repo = Arc::new(MemMemoryRepo);
        let relationship_repo = Arc::new(MemRelationshipRepo {
            rels: Mutex::new(vec![]),
        });
        let mood_repo = Arc::new(MemMoodRepo {
            moods: Mutex::new(HashMap::new()),
        });
        let conversation_state_repo = Arc::new(MemConversationStateRepo {
            states: Mutex::new(HashMap::new()),
        });
        let behavior_state_repo = Arc::new(MemBehaviorStateRepo {
            states: Mutex::new(HashMap::new()),
        });
        // 预设参与者：
        // - Bot 自身（id=55，role='character'，external_id = "111111" 与 bot_self_id 一致）
        // - 普通用户（id=66，role='user'，external_id="u100"）
        // sender_id=66 在 received_event 中对应用户发送者。
        let participant_repo = Arc::new(MemParticipantRepo {
            participants: Mutex::new(vec![
                crate::domain::conversation::Participant {
                    id: 55,
                    conversation_id: 100,
                    external_id: "111111".to_string(),
                    display_name: "TestBot".to_string(),
                    role: crate::domain::conversation::ParticipantRole::Character,
                    metadata: serde_json::json!({}),
                },
                crate::domain::conversation::Participant {
                    id: 66,
                    conversation_id: 100,
                    external_id: "u100".to_string(),
                    display_name: "测试用户".to_string(),
                    role: crate::domain::conversation::ParticipantRole::User,
                    metadata: serde_json::json!({}),
                },
            ]),
        });

        let bus = EventBus::new();

        let runtime = Arc::new(CharacterRuntime::with_event_bus(
            character_repo_trait,
            state_repo.clone(),
            conv_repo.clone(),
            bus.clone(),
        ));
        let binding_manager = Arc::new(BindingManager::new(
            binding_repo.clone(),
            character_repo.clone(),
            conv_repo.clone(),
        ));
        let context_builder = Arc::new(ContextBuilder::new(
            message_repo.clone(),
            Arc::new(
                crate::application::character_history::RepositoryCharacterHistoryProvider::new(
                    message_repo.clone(),
                ),
            ),
            conv_repo.clone(),
            memory_repo.clone(),
            relationship_repo.clone(),
            mood_repo.clone(),
            binding_repo.clone(),
        ));
        let memory_service = Arc::new(MemoryService::new(memory_repo));
        let mood_service = Arc::new(crate::application::mood_service::MoodService::new(
            mood_repo.clone(),
        ));
        let relationship_service = Arc::new(
            crate::application::relationship_service::RelationshipService::new(
                relationship_repo.clone(),
                bus.clone(),
            ),
        );
        let behavior_engine = Arc::new(
            crate::application::behavior_engine::RuleBehaviorEngine::new(
                binding_repo.clone(),
                mood_repo.clone(),
                relationship_repo.clone(),
                state_repo.clone(),
                conversation_state_repo.clone(),
                behavior_state_repo.clone(),
                crate::application::clock::system_clock(),
            ),
        );

        // DecisionOrchestrator：RuleBehaviorEngine 作为 evaluator，NoOpTrigger，pipeline=None
        let evaluator: Arc<dyn crate::application::decision_orchestrator::DecisionEvaluator> =
            behavior_engine.clone();
        let trigger: Arc<dyn crate::application::decision_orchestrator::LlmAssistanceTrigger> =
            Arc::new(crate::application::decision_orchestrator::NoOpTrigger);
        let decision_orchestrator =
            crate::application::decision_orchestrator::DecisionOrchestrator::new(
                evaluator, trigger, None,
            );

        let adapter = Arc::new(FakeAdapter {
            sent: Mutex::new(vec![]),
        });
        let action_dispatcher = Arc::new(crate::application::action::ActionDispatcher::new(
            conv_repo.clone(),
            adapter.clone(),
        ));

        let provider = Arc::new(FakeProvider {
            calls: Mutex::new(0),
        });
        let scheduler = if use_llm {
            // 使用 with_providers 配置角色路由（conversation → provider）
            let mut providers: std::collections::HashMap<
                String,
                Arc<dyn crate::infrastructure::llm::LlmProvider>,
            > = std::collections::HashMap::new();
            providers.insert(
                "test_provider".to_string(),
                provider.clone() as Arc<dyn crate::infrastructure::llm::LlmProvider>,
            );

            let mut role_mis = std::collections::HashMap::new();
            role_mis.insert(
                "conversation".to_string(),
                crate::application::config::LlmModelInstanceConfig {
                    provider: "test_provider".to_string(),
                    model: "test-model".to_string(),
                    context_window: Some(4096),
                    max_output_tokens: Some(512),
                },
            );

            Some(Arc::new(
                crate::application::llm_scheduler::DefaultLlmScheduler::with_providers(
                    provider.clone() as Arc<dyn crate::infrastructure::llm::LlmProvider>,
                    4,
                    4,
                    providers,
                    role_mis,
                ),
            )
                as Arc<dyn crate::application::llm_scheduler::LlmScheduler>)
        } else {
            None
        };

        // Phase 3: ContextAssembler for Token Budget + Layered Degradation
        let token_estimator = Arc::new(TokenEstimatorAdapter(Arc::new(ApproximateTokenEstimator)));
        let model_capabilities = Arc::new(ModelCapabilitiesProviderAdapter(Arc::new(
            FixedModelCapabilities::new(Some(65536), Some(2048)),
        )));
        let context_assembler =
            Arc::new(ContextAssembler::new(token_estimator, model_capabilities));

        let cognition = Arc::new(CognitionLayer::new(
            scheduler,
            context_builder.clone(),
            context_assembler,
        ));

        let processor = Arc::new(ReplyProcessor::new(
            runtime,
            binding_manager,
            decision_orchestrator,
            cognition,
            relationship_service,
            mood_service,
            memory_service,
            action_dispatcher,
            bus,
            delay_executor,
            participant_repo.clone(),
            Some(111111), // bot_self_id for tests
        ));

        (
            processor,
            adapter,
            provider,
            mood_repo,
            relationship_repo,
            state_repo,
            binding_repo,
            participant_repo,
            conversation_state_repo,
            behavior_state_repo,
        )
    }

    fn received_event(is_mentioned: bool) -> MessageReceivedEvent {
        MessageReceivedEvent {
            conversation_id: 100,
            sender_id: 66, // 用户参与者 id=66（role=user）
            message_id: 0,
            content: "你好".to_string(),
            timestamp: chrono::Utc::now(),
            is_mentioned,
            character_id: 1, // 对应 MemBindingRepo 中的 binding.character_id = 1
            // C2-3 字段
            conversation_sequence: 0,
            platform_message_id: None,
            ingested_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn no_binding_ignores_message() {
        // 覆盖空绑定场景：清空 bindings，process 应正常返回且不发送。
        let (processor, adapter, _, _, _, _, binding_repo, _, _, _) = wire(false, false).await;
        binding_repo.bindings.lock().unwrap().clear();
        processor
            .process(&received_event(true))
            .await
            .expect("无绑定时应忽略而非报错");
        assert!(adapter.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn disabled_llm_uses_deterministic_reply() {
        let (processor, adapter, provider, mood_repo, rel_repo, state_repo, _, _, _, _) =
            wire(false, false).await;
        processor
            .process(&received_event(true))
            .await
            .expect("处理应成功");

        // LLM 未启用：不应调用 provider。
        assert_eq!(*provider.calls.lock().unwrap(), 0);

        // 应发送一条确定性回复。
        let sent = adapter.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].0.contains("u100"));

        // 情绪与关系应更新且持久化。
        // 情绪按 Character × Conversation 存储：character_id=1, conversation_id=100
        assert!(mood_repo.moods.lock().unwrap().contains_key(&(1, 100)));
        assert!(!rel_repo.rels.lock().unwrap().is_empty());
        // 状态应被初始化。
        assert!(state_repo.states.lock().unwrap().contains_key(&1));
    }

    #[tokio::test]
    async fn enabled_llm_uses_provider_reply() {
        let (processor, adapter, provider, _, _, _, _, _, _, _) = wire(false, true).await;
        processor
            .process(&received_event(true))
            .await
            .expect("处理应成功");

        // LLM 已启用：应调用 provider 一次。
        assert_eq!(*provider.calls.lock().unwrap(), 1);

        // 应发送 AI 内容。
        let sent = adapter.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].1.contains("AI 回复"));
    }

    #[tokio::test]
    async fn group_conversation_routes_to_group_adapter() {
        let (processor, adapter, _, _, _, _, _, _, _, _) = wire(true, false).await;
        processor
            .process(&received_event(true))
            .await
            .expect("处理应成功");
        let sent = adapter.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].0.contains("g100"));
    }

    #[test]
    fn deterministic_reply_variants() {
        assert!(deterministic_reply("你好").contains("我听到了"));
        assert!(deterministic_reply("   ").contains("嗯"));
        // 长文本会被截断。
        let long = "x".repeat(200);
        let out = deterministic_reply(&long);
        assert!(out.chars().count() < 100);
    }

    #[test]
    fn capped_delay_ms_clamps_to_max() {
        // 低于上限原样返回。
        assert_eq!(capped_delay_ms(0), 0);
        assert_eq!(capped_delay_ms(1500), 1500);
        assert_eq!(capped_delay_ms(3000), 3000);
        // 超过上限被截断。
        assert_eq!(capped_delay_ms(3001), MAX_REPLY_DELAY_MS);
        assert_eq!(capped_delay_ms(10_000), MAX_REPLY_DELAY_MS);
    }

    #[tokio::test]
    async fn reply_waits_for_decision_delay_before_sending() {
        // 受控延迟：请求延迟时长后通知测试并阻塞，直到测试放行。
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel::<u64>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let recorded = Arc::new(Mutex::new(vec![]));
        let controlled: Arc<dyn DelayExecutor> = Arc::new(ControlledDelay {
            delays: recorded.clone(),
            entered: entered_tx,
            release: Arc::new(Mutex::new(Some(release_rx))),
        });

        let (processor, adapter, _, _, _, _, _, _, _, _) =
            wire_with_delay(false, false, controlled).await;

        // 在任务中处理消息（MentionOnly + mentioned → delay 1600 > 0）。
        let proc = processor.clone();
        let ev = received_event(true);
        let handle = tokio::spawn(async move {
            proc.process(&ev).await.expect("处理应成功");
        });

        // 等待延迟被请求。
        let requested = entered_rx.recv().await.expect("应请求一次延迟");
        assert!(requested > 0, "回复决策应带有非零延迟，实际 {requested}");
        assert_eq!(*recorded.lock().unwrap(), vec![requested]);

        // 此刻尚未发送。
        assert!(
            adapter.sent.lock().unwrap().is_empty(),
            "延迟进行中不应发送消息"
        );

        // 放行后发送。
        release_tx.send(()).ok();
        handle.await.expect("处理任务应完成");

        let sent = adapter.sent.lock().unwrap();
        assert_eq!(sent.len(), 1, "延迟结束后应发送一条消息");
    }

    #[tokio::test]
    async fn reply_records_delay_and_sends_given_capacity() {
        // 记录型延迟执行器（不阻塞）会把实际请求的延迟记录下来；
        // 对 @ 消息（MentionOnly + mentioned），基础延迟为 1600ms。
        let recorded = Arc::new(Mutex::new(vec![]));
        let recording: Arc<dyn DelayExecutor> = Arc::new(RecordingDelay {
            delays: recorded.clone(),
        });
        let (processor, adapter, _, _, _, _, _, _, _, _) =
            wire_with_delay(false, false, recording).await;

        processor
            .process(&received_event(true))
            .await
            .expect("处理应成功");

        // MentionOnly + @ 消息基础延迟 1600；process 中 record_interaction 会新建一段
        // 低亲密度关系，触发"陌生→延迟"调制（+400），因此请求的延迟为 2000。
        assert_eq!(*recorded.lock().unwrap(), vec![2000]);
        assert_eq!(adapter.sent.lock().unwrap().len(), 1);
    }
}
