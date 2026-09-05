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

use crate::application::action::ActionDispatcher;
use crate::application::behavior_engine::RuleBehaviorEngine;
use crate::application::binding::BindingManager;
use crate::application::cognition::CognitionLayer;
use crate::application::context::ContextLimits;
use crate::application::emotion_service::EmotionService;
use crate::application::event_bus::EventBus;
use crate::application::memory_service::MemoryService;
use crate::application::relationship_service::RelationshipService;
use crate::application::runtime::CharacterRuntime;
use crate::domain::behavior::{Action, BehaviorAction};
use crate::domain::conversation::ParticipantRole;
use crate::domain::event::{
    BehaviorDecidedEvent, CoreEvent, MessageReceivedEvent, ResponseGeneratedEvent, ResponseSource,
};
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
    behavior_engine: Arc<dyn crate::domain::behavior::BehaviorEngine>,
    cognition: Arc<CognitionLayer>,
    relationship_service: Arc<RelationshipService>,
    emotion_service: Arc<EmotionService>,
    memory_service: Arc<MemoryService>,
    action_dispatcher: Arc<ActionDispatcher>,
    event_bus: EventBus,
    delay_executor: Arc<dyn DelayExecutor>,
    /// 参与者仓储（用于跨回复场景：查询当前 Bot 的 participant_id）。
    participant_repo: Arc<dyn ParticipantRepository>,
}

impl ReplyProcessor {
    /// 创建回复处理器。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: Arc<CharacterRuntime>,
        binding_manager: Arc<BindingManager>,
        behavior_engine: Arc<RuleBehaviorEngine>,
        cognition: Arc<CognitionLayer>,
        relationship_service: Arc<RelationshipService>,
        emotion_service: Arc<EmotionService>,
        memory_service: Arc<MemoryService>,
        action_dispatcher: Arc<ActionDispatcher>,
        event_bus: EventBus,
        delay_executor: Arc<dyn DelayExecutor>,
        participant_repo: Arc<dyn ParticipantRepository>,
    ) -> Self {
        Self {
            runtime,
            binding_manager,
            behavior_engine,
            cognition,
            relationship_service,
            emotion_service,
            memory_service,
            action_dispatcher,
            event_bus,
            delay_executor,
            participant_repo,
        }
    }

    /// 处理一条收到的消息事件。
    ///
    /// 遍历该会话的所有绑定，每个绑定独立决策是否回复。
    /// 多 Bot 共群时，所有待发送回复延迟随机打乱后顺序发送。
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
            // 2a. cross_reply_enabled 检查：非跨回复模式下只处理自己收到的消息。
            if !binding.cross_reply_enabled {
                let my_participant_id = match self.get_my_participant_id(binding).await {
                    Ok(id) => id,
                    Err(e) => {
                        tracing::warn!(
                            target: "runtime",
                            binding_id = binding.id,
                            "获取当前 Bot participant_id 失败: {e}"
                        );
                        continue;
                    }
                };
                if event.sender_id != my_participant_id {
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
                .emotion_service
                .apply_message_event(character_id, &event.content)
                .await
            {
                tracing::warn!(target: "runtime", binding_id = binding.id, "情绪更新失败: {e}");
            }

            // 2e. 行为决策。
            let decision = match self
                .behavior_engine
                .decide_response(
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
                    tracing::warn!(target: "runtime", binding_id = binding.id, "行为决策失败: {e}");
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
                continue;
            }

            // 2f. 生成响应内容。
            let content = match self.generate_reply(event, &character, character_id).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(target: "runtime", binding_id = binding.id, "生成回复失败: {e}");
                    continue;
                }
            };

            // 2g. 收集待发送回复，延迟使用 BehaviorEngine 返回的延迟（截断到安全上限）。
            pending_replies.push(PendingReply {
                binding_id: binding.id,
                content,
                conversation_id: event.conversation_id,
                delay: Duration::from_millis(capped_delay_ms(decision.delay_ms)),
            });
        }

        // 3. 延迟 + 随机打乱顺序后发送。
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
                tracing::warn!(
                    target: "runtime",
                    binding_id = pending.binding_id,
                    "发送回复失败: {e}"
                );
            }
        }

        Ok(())
    }

    /// 生成回复内容：LLM 未启用时走确定性回复，启用时经认知层。
    async fn generate_reply(
        &self,
        event: &MessageReceivedEvent,
        character: &crate::domain::character::Character,
        character_id: i64,
    ) -> Result<String, RuntimeError> {
        if self.cognition.enabled() {
            if let Some(content) = self
                .cognition
                .generate(
                    character,
                    event.conversation_id,
                    event.sender_id,
                    &event.content,
                    event.is_mentioned,
                    ContextLimits::default(),
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

    /// 获取当前 Bot 在该 binding 对应会话中的 participant_id。
    ///
    /// 通过查询 participants 表：role='character' AND conversation_id=binding.conversation_id。
    /// 返回该会话中 Bot 参与者（role='character'）的 ID。
    async fn get_my_participant_id(
        &self,
        binding: &crate::domain::character::CharacterBinding,
    ) -> Result<i64, RuntimeError> {
        let participants = self
            .participant_repo
            .find_by_conversation_id(binding.conversation_id)
            .await
            .map_err(RuntimeError::Repository)?;

        // 查找 role='character' 的参与者（表示 Bot 自身）。
        let character_participant = participants
            .into_iter()
            .find(|p| p.role == ParticipantRole::Character);

        character_participant.map(|p| p.id).ok_or_else(|| {
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
    use crate::domain::character::{
        Character, CharacterBinding, CharacterDefinition, CharacterState,
    };
    use crate::domain::conversation::{Conversation, ConversationType};
    use crate::domain::emotion::EmotionState;
    use crate::domain::memory::Memory;
    use crate::domain::message::Message;
    use crate::domain::relationship::Relationship;
    use crate::domain::repository::{
        CharacterBindingRepository, CharacterRepository, CharacterStateRepository,
        ConversationRepository, EmotionStateRepository, MemoryRepository, MessageRepository,
        ParticipantRepository, RelationshipRepository,
    };
    use crate::error::RepositoryError;
    use crate::infrastructure::llm::{LlmProvider, LlmRequest, LlmResponse, TokenUsage};
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
        async fn find_by_character_id(
            &self,
            _id: i64,
            _t: Option<crate::domain::memory::MemoryType>,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
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

    struct MemEmotionRepo {
        states: Mutex<HashMap<i64, EmotionState>>,
    }
    #[async_trait]
    impl EmotionStateRepository for MemEmotionRepo {
        async fn find_by_character_id(
            &self,
            id: i64,
        ) -> Result<Option<EmotionState>, RepositoryError> {
            Ok(self.states.lock().unwrap().get(&id).cloned())
        }
        async fn upsert(&self, id: i64, state: &EmotionState) -> Result<(), RepositoryError> {
            self.states.lock().unwrap().insert(id, state.clone());
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
        ) -> Result<(), RuntimeError> {
            self.sent
                .lock()
                .unwrap()
                .push((group_id.to_string(), content.to_string()));
            Ok(())
        }
        async fn send_private_message(
            &self,
            user_id: &str,
            content: &str,
        ) -> Result<(), RuntimeError> {
            self.sent
                .lock()
                .unwrap()
                .push((user_id.to_string(), content.to_string()));
            Ok(())
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
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    type Wiring = (
        Arc<ReplyProcessor>,
        Arc<FakeAdapter>,
        Arc<FakeProvider>,
        Arc<MemEmotionRepo>,
        Arc<MemRelationshipRepo>,
        Arc<MemStateRepo>,
        Arc<MemBindingRepo>,
        Arc<MemParticipantRepo>,
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
        let emotion_repo = Arc::new(MemEmotionRepo {
            states: Mutex::new(HashMap::new()),
        });
        // 预设 Bot 参与者（sender_id=55 对应 id=55，role='character'）。
        let participant_repo = Arc::new(MemParticipantRepo {
            participants: Mutex::new(vec![crate::domain::conversation::Participant {
                id: 55,
                conversation_id: 100,
                external_id: "bot123".to_string(),
                display_name: "TestBot".to_string(),
                role: crate::domain::conversation::ParticipantRole::Character,
                metadata: serde_json::json!({}),
            }]),
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
            message_repo,
            conv_repo.clone(),
            memory_repo.clone(),
            relationship_repo.clone(),
            emotion_repo.clone(),
            binding_repo.clone(),
        ));
        let memory_service = Arc::new(MemoryService::new(memory_repo));
        let emotion_service = Arc::new(crate::application::emotion_service::EmotionService::new(
            emotion_repo.clone(),
            bus.clone(),
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
                emotion_repo.clone(),
                relationship_repo.clone(),
                state_repo.clone(),
                crate::application::clock::system_clock(),
            ),
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
            Some(
                Arc::new(crate::application::llm_scheduler::DefaultLlmScheduler::new(
                    provider.clone(),
                )) as Arc<dyn crate::application::llm_scheduler::LlmScheduler>,
            )
        } else {
            None
        };
        let cognition = Arc::new(CognitionLayer::new(scheduler, context_builder.clone()));

        let processor = Arc::new(ReplyProcessor::new(
            runtime,
            binding_manager,
            behavior_engine,
            cognition,
            relationship_service,
            emotion_service,
            memory_service,
            action_dispatcher,
            bus,
            delay_executor,
            participant_repo.clone(),
        ));

        (
            processor,
            adapter,
            provider,
            emotion_repo,
            relationship_repo,
            state_repo,
            binding_repo,
            participant_repo,
        )
    }

    fn received_event(is_mentioned: bool) -> MessageReceivedEvent {
        MessageReceivedEvent {
            conversation_id: 100,
            sender_id: 55,
            message_id: 0,
            content: "你好".to_string(),
            timestamp: chrono::Utc::now(),
            is_mentioned,
        }
    }

    #[tokio::test]
    async fn no_binding_ignores_message() {
        // 覆盖空绑定场景：清空 bindings，process 应正常返回且不发送。
        let (processor, adapter, _, _, _, _, binding_repo, _) = wire(false, false).await;
        binding_repo.bindings.lock().unwrap().clear();
        processor
            .process(&received_event(true))
            .await
            .expect("无绑定时应忽略而非报错");
        assert!(adapter.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn disabled_llm_uses_deterministic_reply() {
        let (processor, adapter, provider, emotion_repo, rel_repo, state_repo, _, _) =
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
        assert!(emotion_repo.states.lock().unwrap().contains_key(&1));
        assert!(!rel_repo.rels.lock().unwrap().is_empty());
        // 状态应被初始化。
        assert!(state_repo.states.lock().unwrap().contains_key(&1));
    }

    #[tokio::test]
    async fn enabled_llm_uses_provider_reply() {
        let (processor, adapter, provider, _, _, _, _, _) = wire(false, true).await;
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
        let (processor, adapter, _, _, _, _, _, _) = wire(true, false).await;
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

        let (processor, adapter, _, _, _, _, _, _) =
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
        let (processor, adapter, _, _, _, _, _, _) = wire_with_delay(false, false, recording).await;

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
