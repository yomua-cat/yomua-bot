//! Reply Pipeline —— 收到消息后的回复编排器。
//!
//! 负责把一条 `MessageReceivedEvent` 走完整个回复流程：
//! 确定参与角色 → 更新关系/情绪 → 组装上下文 → 行为决策 →
//! （可选）认知生成 → 分派发送动作，供 Outbox 处理器复用。

use std::sync::Arc;
use std::time::Duration;

use crate::domain::message::ReplyTaskStatus;
use chrono::Utc;

#[allow(unused_imports)]
use crate::adapters::onebot::ActionResponse;
use crate::application::action::ActionDispatcher;
use crate::application::binding::BindingManager;
use crate::application::cognition::CognitionLayer;
use crate::application::context::ContextLimits;
use crate::application::event_bus::EventBus;
use crate::application::memory_service::{MemoryExtractionInput, MemoryService};
use crate::application::mood_service::MoodService;
use crate::application::relationship_service::RelationshipService;
use crate::application::runtime::CharacterRuntime;
use crate::domain::behavior::BehaviorAction;
use crate::domain::conversation::ParticipantRole;
use crate::domain::event::{
    BehaviorDecidedEvent, CoreEvent, MessageReceivedEvent, ResponseGeneratedEvent, ResponseSource,
};
use crate::domain::outbox::ProcessingOutcome;
use crate::domain::repository::ParticipantRepository;
use crate::error::RuntimeError;

const ASSISTANT_HISTORY_REPAIR_PREFIX: &str = "消息已发送但 assistant 历史落库失败";

/// 单次回复最长允许的拟人化延迟（毫秒）。
///
/// 行为引擎给出的延时（用于模拟输入间隔）可能较大；为了不让一条普通消息
/// 长时间卡住回复链路，这里统一截断到该上限。
const MAX_REPLY_DELAY_MS: u64 = 3_000;
const NORMAL_BATCH_WAIT_MS: u64 = 1_500;
const MENTION_BATCH_WAIT_MS: u64 = 500;
const MAX_BATCH_MESSAGES: usize = 20;

fn combine_batch_contents(contents: impl IntoIterator<Item = String>) -> String {
    contents
        .into_iter()
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// 把行为引擎给出的延迟截断到安全上限。
fn capped_delay_ms(delay_ms: u64) -> u64 {
    delay_ms.min(MAX_REPLY_DELAY_MS)
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
    memory_service: Arc<MemoryService>,
    action_dispatcher: Arc<ActionDispatcher>,
    event_bus: EventBus,
    delay_executor: Arc<dyn DelayExecutor>,
    /// 参与者仓储（用于跨回复场景：查询当前 Bot 的 participant_id）。
    participant_repo: Arc<dyn ParticipantRepository>,
    /// 当前 Bot 在 OneBot 中的账号 ID（QQ 号），用于精确识别"当前 Bot 自己"。
    /// 在单 Bot 部署中从 OneBotConfig 传入；在多 Bot 场景下由各 Bot 独立持有。
    bot_self_id: Option<i64>,
    message_repo: Option<Arc<dyn crate::domain::repository::MessageRepository>>,
}

struct FrozenBatch {
    content: String,
    additional_messages: Vec<crate::domain::message::Message>,
}

impl ReplyProcessor {
    fn event_expired(event: &MessageReceivedEvent) -> bool {
        event.message_id > 0 && (Utc::now() - event.ingested_at).num_seconds() > 120
    }

    /// 在回复任务开始时冻结一轮连续输入。
    ///
    /// 入站消息仍逐条保存；这里只把已经落库、同一会话同一发送者、紧邻当前序号的
    /// 消息合并成一次模型输入。等待结束后才读取数据库，因此生成期间到达的消息
    /// 不会混入本轮，而会由后续 Outbox 任务处理。
    async fn freeze_batch(
        &self,
        event: &MessageReceivedEvent,
    ) -> Result<FrozenBatch, RuntimeError> {
        let Some(repo) = &self.message_repo else {
            return Ok(FrozenBatch {
                content: event.content.clone(),
                additional_messages: Vec::new(),
            });
        };
        let wait_ms = if event.is_mentioned {
            MENTION_BATCH_WAIT_MS
        } else {
            NORMAL_BATCH_WAIT_MS
        };
        self.delay_executor.delay(wait_ms).await;
        let deadline = event.ingested_at + chrono::Duration::milliseconds(wait_ms as i64);
        let ordered = if event.message_id > 0 && event.conversation_sequence > 0 {
            repo.find_after_sequence(
                event.conversation_id,
                event.conversation_sequence,
                MAX_BATCH_MESSAGES as i64,
            )
            .await?
        } else {
            let mut messages = repo
                .find_recent(event.conversation_id, MAX_BATCH_MESSAGES as i64)
                .await?;
            messages.sort_by_key(|message| message.conversation_sequence.unwrap_or(i64::MAX));
            messages
        };
        let mut batch = Vec::new();
        for message in ordered {
            let sequence = message.conversation_sequence.unwrap_or(i64::MAX);
            if event.message_id > 0 && sequence <= event.conversation_sequence {
                continue;
            }
            if message.ingested_at.is_some_and(|at| at > deadline) {
                break;
            }
            let author_kind = message
                .metadata
                .get("author_kind")
                .and_then(|value| value.as_str())
                .unwrap_or("user");
            if author_kind != "user" {
                break;
            }
            if message.sender_id != event.sender_id
                || message.active_character_id != Some(event.character_id)
            {
                break;
            }
            batch.push(message);
            if batch.len() >= MAX_BATCH_MESSAGES {
                break;
            }
        }
        if event.message_id == 0 {
            let mut combined = vec![event.content.clone()];
            let mut additional_messages = Vec::new();
            for message in batch {
                self.mark_task_ignored_if_pending(message.id, event.character_id)
                    .await?;
                if let crate::domain::message::MessageContent::Text(text) = &message.content {
                    combined.push(text.clone());
                }
                additional_messages.push(message);
            }
            return Ok(FrozenBatch {
                content: combine_batch_contents(combined),
                additional_messages,
            });
        }
        if batch.is_empty() {
            return Ok(FrozenBatch {
                content: event.content.clone(),
                additional_messages: Vec::new(),
            });
        }

        let mut combined = vec![event.content.clone()];
        let mut additional_messages = Vec::new();
        for message in batch.iter() {
            if let crate::domain::message::MessageContent::Text(text) = &message.content {
                if !text.trim().is_empty() {
                    combined.push(text.trim().to_string());
                }
            }
            self.mark_task_ignored_if_pending(message.id, event.character_id)
                .await?;
            additional_messages.push(message.clone());
        }
        let combined = combine_batch_contents(combined);
        Ok(FrozenBatch {
            content: if combined.is_empty() {
                event.content.clone()
            } else {
                combined
            },
            additional_messages,
        })
    }

    async fn mark_task_ignored_if_pending(
        &self,
        message_id: i64,
        character_id: i64,
    ) -> Result<(), RuntimeError> {
        if let Some(repo) = &self.message_repo {
            repo.mark_reply_task_ignored_if_pending(message_id, character_id)
                .await?;
        }
        Ok(())
    }

    /// 创建回复处理器。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: Arc<CharacterRuntime>,
        binding_manager: Arc<BindingManager>,
        decision_orchestrator: crate::application::decision_orchestrator::DecisionOrchestrator,
        cognition: Arc<CognitionLayer>,
        relationship_service: Arc<RelationshipService>,
        _mood_service: Arc<MoodService>,
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
            memory_service,
            action_dispatcher,
            event_bus,
            delay_executor,
            participant_repo,
            bot_self_id,
            message_repo: None,
        }
    }

    /// 主动任务沿同一入站事务与发送链路，保存依据与绑定版本。
    pub async fn enqueue_proactive(
        &self,
        binding: &crate::domain::character::CharacterBinding,
        source: &crate::domain::message::Message,
        basis: &str,
    ) -> Result<(), RuntimeError> {
        let repo = self
            .message_repo
            .as_ref()
            .ok_or_else(|| RuntimeError::Internal("主动任务未装配持久仓储".into()))?;
        if basis.trim().is_empty()
            || source.conversation_id != binding.conversation_id
            || source.active_character_id != Some(binding.character_id)
        {
            return Err(RuntimeError::Internal("主动发言缺少有效来源".into()));
        }
        let uuid = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();
        let payload = serde_json::json!({"event_uuid":uuid,"conversation_id":binding.conversation_id,"sender_id":source.sender_id,"message_id":0,"content":basis,"timestamp":now,"is_mentioned":true,"character_id":binding.character_id,"conversation_sequence":0,"platform_message_id":null,"ingested_at":now,"metadata":{"proactive":true,"basis_message_ids":[source.id],"binding_id":binding.id,"binding_version":binding.switched_at,"author_kind":"system"}});
        repo.accept_message(&crate::domain::repository::AcceptedMessageInput {
            conversation_id: binding.conversation_id,
            sender_id: source.sender_id,
            content_json: serde_json::json!({"Text":basis}).to_string(),
            timestamp: now.to_rfc3339(),
            platform_message_id: None,
            character_id: binding.character_id,
            event_uuid: uuid,
            outbox_payload: payload.to_string(),
        })
        .await?;
        Ok(())
    }
    /// 装配可靠回复任务存储。
    pub fn with_message_repository(
        mut self,
        repo: Arc<dyn crate::domain::repository::MessageRepository>,
    ) -> Self {
        self.message_repo = Some(repo);
        self
    }

    /// 发送控制指令的结果。
    ///
    /// 控制指令本身已经作为系统消息写入 `messages`，因此可以复用同一张
    /// `reply_tasks` 表和发送状态机。该路径不运行角色决策、记忆或关系逻辑，
    /// 只负责校验当前绑定、持久化内容、发送和补写 assistant 历史。
    pub async fn send_control_reply(
        &self,
        conversation_id: i64,
        source_message_id: i64,
        content: &str,
    ) -> Result<(), RuntimeError> {
        let repo = self
            .message_repo
            .as_ref()
            .ok_or_else(|| RuntimeError::Internal("控制回复缺少消息仓储".into()))?;
        let source = repo
            .find_by_id(source_message_id)
            .await?
            .ok_or(RuntimeError::Domain(
                crate::error::DomainError::MessageNotFound(source_message_id),
            ))?;
        if source.conversation_id != conversation_id {
            return Err(RuntimeError::Domain(
                crate::error::DomainError::MessageNotFound(source_message_id),
            ));
        }
        let binding = self
            .binding_manager
            .by_conversation(conversation_id)
            .await?
            .into_iter()
            .next()
            .ok_or(RuntimeError::Domain(
                crate::error::DomainError::ConversationNotFound(conversation_id),
            ))?;
        let source_binding_id = source
            .metadata
            .get("binding_id")
            .and_then(|value| value.as_i64());
        let source_binding_version = source.metadata.get("binding_version");
        if source_binding_id != Some(binding.id)
            || source_binding_version != Some(&serde_json::json!(binding.switched_at))
        {
            repo.save_reply_task(
                source_message_id,
                binding.character_id,
                ReplyTaskStatus::Cancelled,
                None,
            )
            .await?;
            return Ok(());
        }
        let character = self.runtime.refresh_character(binding.character_id).await?;
        let event = MessageReceivedEvent {
            conversation_id,
            sender_id: source.sender_id,
            message_id: source_message_id,
            content: String::new(),
            timestamp: source.timestamp,
            is_mentioned: false,
            character_id: binding.character_id,
            conversation_sequence: source.conversation_sequence.unwrap_or_default(),
            platform_message_id: source.platform_message_id,
            ingested_at: source.ingested_at.unwrap_or_else(Utc::now),
            metadata: serde_json::json!({
                "author_kind": "system",
                "binding_id": binding.id,
                "binding_version": binding.switched_at,
            }),
        };

        if !character.enabled {
            repo.save_reply_task(
                source_message_id,
                binding.character_id,
                ReplyTaskStatus::Cancelled,
                None,
            )
            .await?;
            return Ok(());
        }
        if repo
            .reply_task(source_message_id)
            .await?
            .is_some_and(|task| task.status.is_terminal())
        {
            return Ok(());
        }

        let _binding_gate = self
            .binding_manager
            .acquire_conversation_lock(conversation_id)
            .await;
        if repo.find_by_id(source_message_id).await?.is_none() {
            return Ok(());
        }
        let still_current = self
            .binding_manager
            .by_conversation(conversation_id)
            .await?
            .into_iter()
            .any(|candidate| {
                candidate.id == binding.id && candidate.character_id == binding.character_id
            });
        if !still_current {
            repo.save_reply_task(
                source_message_id,
                binding.character_id,
                ReplyTaskStatus::Cancelled,
                None,
            )
            .await?;
            return Ok(());
        }

        repo.save_reply_task(
            source_message_id,
            binding.character_id,
            ReplyTaskStatus::Generated,
            Some(content),
        )
        .await?;
        repo.save_reply_task_delivery(
            source_message_id,
            binding.character_id,
            ReplyTaskStatus::Sending,
            Some(content),
            None,
            None,
        )
        .await?;

        match self
            .action_dispatcher
            .execute_send_message_with_response(conversation_id, content)
            .await
        {
            Ok(response) if response.is_ok() => {
                let platform_message_id = response
                    .data
                    .as_ref()
                    .and_then(|data| data.get("message_id"))
                    .and_then(|value| value.as_i64());
                repo.save_reply_task_delivery(
                    source_message_id,
                    binding.character_id,
                    ReplyTaskStatus::Sending,
                    Some(content),
                    None,
                    platform_message_id,
                )
                .await?;
                if let Err(error) = self
                    .persist_assistant_history(&event, &character, content, platform_message_id)
                    .await
                {
                    repo.save_reply_task_delivery(
                        source_message_id,
                        binding.character_id,
                        ReplyTaskStatus::Sending,
                        Some(content),
                        Some(&format!("{ASSISTANT_HISTORY_REPAIR_PREFIX}: {error}")),
                        platform_message_id,
                    )
                    .await?;
                    return Err(error);
                }
                repo.save_reply_task_delivery(
                    source_message_id,
                    binding.character_id,
                    ReplyTaskStatus::Sent,
                    None,
                    None,
                    platform_message_id,
                )
                .await?;
                Ok(())
            }
            Ok(response) => {
                repo.save_reply_task_delivery(
                    source_message_id,
                    binding.character_id,
                    ReplyTaskStatus::Generated,
                    Some(content),
                    Some(&response.error_message()),
                    None,
                )
                .await?;
                Err(RuntimeError::SendRejected(format!(
                    "OneBot 明确拒绝发送：status={}, retcode={:?}, error={:?}",
                    response.status, response.retcode, response.error
                )))
            }
            Err(error) => {
                repo.save_reply_task_delivery(
                    source_message_id,
                    binding.character_id,
                    ReplyTaskStatus::Unknown,
                    Some(content),
                    Some(&error.to_string()),
                    None,
                )
                .await?;
                Ok(())
            }
        }
    }

    async fn save_task(
        &self,
        event: &MessageReceivedEvent,
        status: ReplyTaskStatus,
        content: Option<&str>,
    ) -> Result<(), RuntimeError> {
        if let Some(repo) = &self.message_repo {
            if event.message_id > 0 {
                repo.save_reply_task(event.message_id, event.character_id, status, content)
                    .await?;
            }
        }
        Ok(())
    }
    async fn save_task_error(
        &self,
        event: &MessageReceivedEvent,
        status: ReplyTaskStatus,
        content: Option<&str>,
        error: &str,
    ) -> Result<(), RuntimeError> {
        if let Some(repo) = &self.message_repo {
            if event.message_id > 0 {
                repo.save_reply_task_error(
                    event.message_id,
                    event.character_id,
                    status,
                    content,
                    Some(error),
                )
                .await?;
            }
        }
        Ok(())
    }

    /// 更新发送阶段状态时绑定当前 Outbox 租约。
    ///
    /// 发送调用可能跨越租约续期边界；所有发送结果都必须通过带租约条件的
    /// 仓储写入，旧 worker 失去所有权后不能覆盖新 worker 的状态。
    async fn save_delivery_owned(
        &self,
        event: &MessageReceivedEvent,
        lease: Option<&str>,
        status: ReplyTaskStatus,
        content: Option<&str>,
        error: Option<&str>,
        platform_message_id: Option<i64>,
    ) -> Result<(), RuntimeError> {
        let Some(repo) = &self.message_repo else {
            return Ok(());
        };
        if event.message_id <= 0 {
            return Ok(());
        }
        let updated = if let Some(token) = lease {
            repo.save_reply_task_delivery_owned(
                event.message_id,
                event.character_id,
                token,
                status,
                content,
                error,
                platform_message_id,
            )
            .await?
        } else {
            repo.save_reply_task_delivery(
                event.message_id,
                event.character_id,
                status,
                content,
                error,
                platform_message_id,
            )
            .await?;
            true
        };
        if !updated {
            return Err(RuntimeError::Repository(
                crate::error::RepositoryError::OwnershipLost,
            ));
        }
        Ok(())
    }

    /// 在会话删除、角色切换或租约恢复后，阻止已失效事件继续产生副作用。
    ///
    /// Outbox 删除只能把事件标记为终态，无法撤回已经被 worker 领取的事件；
    /// 因此每个不可逆副作用前都必须重新检查源消息和租约。检查失败时直接
    /// 返回所有权失效，调度器会确认事件而不会继续写关系、记忆或发送。
    pub(crate) async fn ensure_event_still_owned(
        &self,
        event: &MessageReceivedEvent,
        lease: Option<&str>,
    ) -> Result<(), RuntimeError> {
        let Some(repo) = &self.message_repo else {
            return Ok(());
        };
        if event.message_id <= 0 {
            return Ok(());
        }
        if repo.find_by_id(event.message_id).await?.is_none() {
            return Err(RuntimeError::Repository(
                crate::error::RepositoryError::OwnershipLost,
            ));
        }
        if let Some(token) = lease {
            if !repo.owns_outbox_lease(event.message_id, token).await? {
                return Err(RuntimeError::Repository(
                    crate::error::RepositoryError::OwnershipLost,
                ));
            }
        }
        Ok(())
    }

    /// 幂等保存 assistant 历史。
    ///
    /// 发送 API 成功后，历史写入可能因数据库瞬时故障失败。调用方会把任务
    /// 标记为可识别的 unknown，随后由 outbox 重试进入这里；先按 reply_task
    /// 元数据查重，避免恢复时重复插入 assistant 消息。
    async fn persist_assistant_history(
        &self,
        event: &MessageReceivedEvent,
        character: &crate::domain::character::Character,
        content: &str,
        platform_message_id: Option<i64>,
    ) -> Result<(), RuntimeError> {
        let Some(repo) = &self.message_repo else {
            return Ok(());
        };
        if repo
            .assistant_history_exists(event.conversation_id, event.message_id)
            .await?
        {
            return Ok(());
        }

        // assistant 历史按角色身份归属，而不是按平台账号归属。一个 QQ 账号
        // 可以在不同会话扮演不同角色；未知发送结果的确认路径也使用同一键。
        let assistant_external_id = format!("character:{}", event.character_id);
        let assistant_sender_id = if let Some(participant) = self
            .participant_repo
            .find_by_external_id(event.conversation_id, &assistant_external_id)
            .await?
        {
            participant.id
        } else {
            let participant = crate::domain::conversation::Participant {
                id: 0,
                conversation_id: event.conversation_id,
                external_id: assistant_external_id,
                display_name: character.definition.name.clone(),
                role: crate::domain::conversation::ParticipantRole::Character,
                metadata: serde_json::json!({
                    "author_kind": "assistant",
                    "character_id": event.character_id,
                }),
            };
            self.participant_repo.insert(&participant).await?
        };
        let now = Utc::now();
        let message = crate::domain::message::Message {
            id: 0,
            conversation_id: event.conversation_id,
            sender_id: assistant_sender_id,
            content: crate::domain::message::MessageContent::Text(content.to_string()),
            timestamp: now,
            reply_to: Some(event.message_id),
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({
                "author_kind":"assistant",
                "reply_task":event.message_id
            }),
            active_character_id: Some(event.character_id),
            platform_message_id,
            conversation_sequence: Some(event.conversation_sequence),
            occurred_at: Some(now),
            ingested_at: Some(now),
        };
        repo.insert(&message).await?;
        Ok(())
    }
    async fn current_binding(
        &self,
        event: &MessageReceivedEvent,
    ) -> Result<Option<crate::domain::character::CharacterBinding>, RuntimeError> {
        let binding = self
            .binding_manager
            .by_conversation(event.conversation_id)
            .await?
            .into_iter()
            .find(|b| b.character_id == event.character_id);
        let Some(binding) = binding else {
            return Ok(None);
        };
        if let Some(repo) = &self.message_repo {
            if let Some(msg) = repo.find_by_id(event.message_id).await? {
                if let Some(id) = msg.metadata.get("binding_id").and_then(|v| v.as_i64()) {
                    if id != binding.id
                        || msg.metadata.get("binding_version")
                            != Some(&serde_json::json!(binding.switched_at))
                    {
                        return Ok(None);
                    }
                }
            }
        }
        Ok(Some(binding))
    }
    /// 仅处理接收时的角色；生成内容先落库，未知发送结果交由管理员核查。
    pub async fn process(&self, event: &MessageReceivedEvent) -> Result<(), RuntimeError> {
        self.process_owned(event, None).await
    }
    pub async fn process_owned(
        &self,
        event: &MessageReceivedEvent,
        lease: Option<&str>,
    ) -> Result<(), RuntimeError> {
        // 整个处理期间持有会话 fencing 门闩。会话删除必须等待当前任务结束，
        // 这样删除提交后不会再有关系、记忆或发送副作用落入已删除会话。
        let _conversation_gate = self
            .binding_manager
            .acquire_conversation_lock(event.conversation_id)
            .await;
        // 会话或消息可能已在 Outbox 领取后被删除。此时事件已经失效，
        // 必须在任何关系、记忆或模型调用前直接确认，避免删除后的副作用。
        if let Some(repo) = &self.message_repo {
            if event.message_id > 0 && repo.find_by_id(event.message_id).await?.is_none() {
                tracing::debug!(
                    conversation_id = event.conversation_id,
                    message_id = event.message_id,
                    "源消息已删除，跳过失效 Outbox 事件"
                );
                return Ok(());
            }
        }
        self.ensure_event_still_owned(event, lease).await?;
        // 恢复时先处理上一次可能中断的发送状态，再检查绑定和过期时间。
        // 这样切换角色或任务过期不会把 Sending 任务遗留成可重复发送的状态。
        let stored = if let Some(repo) = &self.message_repo {
            if event.message_id > 0 {
                repo.reply_task(event.message_id).await?
            } else {
                None
            }
        } else {
            None
        };
        // Outbox 是持久化事件流，事件本身可能在任务已经落入终态后再次投递。
        // 尤其是连续消息合并时，首条任务会把同批后续消息标记为 Ignored；
        // 这些消息对应的 Outbox 事件仍然必须被确认，但不能再次触发模型或发送。
        if stored.as_ref().is_some_and(|task| {
            matches!(
                task.status,
                ReplyTaskStatus::Sent
                    | ReplyTaskStatus::Ignored
                    | ReplyTaskStatus::Cancelled
                    | ReplyTaskStatus::Expired
            )
        }) {
            return Ok(());
        }
        if stored
            .as_ref()
            .is_some_and(|task| task.status == ReplyTaskStatus::Sending)
        {
            if let Some(repo) = &self.message_repo {
                if let Some(task) = stored.as_ref() {
                    if let (Some(content), Some(platform_message_id)) =
                        (task.content.as_deref(), task.platform_message_id)
                    {
                        // 已经拿到平台消息 ID，平台发送确定成功；这里只补写本地历史，
                        // 不再重新调用发送接口。
                        let character = self.runtime.refresh_character(event.character_id).await?;
                        self.persist_assistant_history(
                            event,
                            &character,
                            content,
                            Some(platform_message_id),
                        )
                        .await?;
                        self.save_delivery_owned(
                            event,
                            lease,
                            ReplyTaskStatus::Sent,
                            None,
                            None,
                            Some(platform_message_id),
                        )
                        .await?;
                        return Ok(());
                    }
                }
                let changed = if let Some(token) = lease {
                    repo.save_reply_task_delivery_owned(
                        event.message_id,
                        event.character_id,
                        token,
                        ReplyTaskStatus::Unknown,
                        None,
                        Some("进程在发送平台回执前退出，发送结果未知，需要核查"),
                        None,
                    )
                    .await?
                } else {
                    repo.mark_reply_task_unknown_if_sending(
                        event.message_id,
                        event.character_id,
                        "进程在发送平台回执前退出，发送结果未知，需要核查",
                    )
                    .await?
                };
                if changed {
                    tracing::warn!(
                        message_id = event.message_id,
                        "恢复发送中回复任务并标记为 unknown"
                    );
                }
            }
            return Ok(());
        }

        let Some(binding) = self.current_binding(event).await? else {
            self.save_task(event, ReplyTaskStatus::Cancelled, None)
                .await?;
            return Ok(());
        };
        if Self::event_expired(event) {
            self.save_task(event, ReplyTaskStatus::Expired, None)
                .await?;
            return Ok(());
        }
        let sender = self
            .participant_repo
            .find_by_id(event.sender_id)
            .await?
            .ok_or_else(|| RuntimeError::Internal("发送者记录缺失".into()))?;
        if self
            .bot_self_id
            .is_some_and(|id| sender.external_id == id.to_string())
        {
            return Ok(());
        }
        if sender.role != ParticipantRole::User && !binding.cross_reply_enabled {
            return Ok(());
        }
        let character = self.runtime.refresh_character(event.character_id).await?;
        if !character.enabled {
            self.save_task(event, ReplyTaskStatus::Cancelled, None)
                .await?;
            return Ok(());
        }
        if stored.as_ref().is_some_and(|task| {
            task.status == ReplyTaskStatus::Unknown
                && task
                    .error
                    .as_deref()
                    .is_some_and(|error| error.starts_with(ASSISTANT_HISTORY_REPAIR_PREFIX))
        }) {
            let content = stored
                .as_ref()
                .and_then(|task| task.content.as_deref())
                .ok_or_else(|| RuntimeError::Internal("历史修复任务缺少回复内容".into()))?;
            self.persist_assistant_history(event, &character, content, None)
                .await?;
            self.save_delivery_owned(event, lease, ReplyTaskStatus::Sent, None, None, None)
                .await?;
            return Ok(());
        }
        if stored
            .as_ref()
            .is_some_and(|task| task.status.is_terminal())
        {
            return Ok(());
        }
        let stable_user_key = event
            .metadata
            .get("external_sender_id")
            .and_then(|value| value.as_str());
        let non_human_source = event
            .metadata
            .get("author_kind")
            .and_then(|value| value.as_str())
            .is_some_and(|kind| {
                matches!(
                    kind,
                    "system" | "system_command" | "platform_echo" | "other_bot"
                )
            });
        let relationship_participant_id = self
            .relationship_service
            .canonical_participant_id(event.character_id, event.sender_id, stable_user_key)
            .await?;
        let sender_user_id = self
            .participant_repo
            .user_id_for_participant(event.sender_id)
            .await?;
        self.ensure_event_still_owned(event, lease).await?;
        if event.message_id > 0 && !non_human_source {
            self.memory_service
                .extract_and_store(MemoryExtractionInput {
                    character_id: event.character_id,
                    conversation_id: Some(event.conversation_id),
                    // 记忆来源必须保留当前事件的实际参与者；关系服务的
                    // canonical participant 只用于稳定关系键，不能替代来源会话。
                    participant_id: event.sender_id,
                    user_id: sender_user_id,
                    message_text: &event.content,
                    source_message_id: event.message_id,
                    source_occurred_at: event.timestamp,
                })
                .await?;
        }
        self.ensure_event_still_owned(event, lease).await?;
        if event.message_id > 0 && !non_human_source {
            match self
                .relationship_service
                .record_interaction_once(
                    event.character_id,
                    relationship_participant_id,
                    event.message_id,
                )
                .await
            {
                Ok(_) => {}
                Err(crate::error::RepositoryError::NotImplemented(_)) => {
                    tracing::debug!(
                        character_id = event.character_id,
                        participant_id = relationship_participant_id,
                        "关系仓储未提供交互幂等记录，跳过关系计数"
                    );
                }
                Err(error) => return Err(RuntimeError::Repository(error)),
            }
        }
        self.ensure_event_still_owned(event, lease).await?;
        let frozen_batch = self.freeze_batch(event).await?;
        for message in &frozen_batch.additional_messages {
            self.ensure_event_still_owned(event, lease).await?;
            let stable_user_key = message
                .metadata
                .get("external_sender_id")
                .and_then(|value| value.as_str());
            let participant_id = self
                .relationship_service
                .canonical_participant_id(event.character_id, message.sender_id, stable_user_key)
                .await?;
            if let crate::domain::message::MessageContent::Text(text) = &message.content {
                let user_id = self
                    .participant_repo
                    .user_id_for_participant(message.sender_id)
                    .await?;
                self.memory_service
                    .extract_and_store(MemoryExtractionInput {
                        character_id: event.character_id,
                        conversation_id: Some(event.conversation_id),
                        participant_id: message.sender_id,
                        user_id,
                        message_text: text,
                        source_message_id: message.id,
                        source_occurred_at: message.occurred_at.unwrap_or(message.timestamp),
                    })
                    .await?;
            }
            match self
                .relationship_service
                .record_interaction_once(event.character_id, participant_id, message.id)
                .await
            {
                Ok(_) | Err(crate::error::RepositoryError::NotImplemented(_)) => {}
                Err(error) => return Err(RuntimeError::Repository(error)),
            }
        }
        let batch_content = frozen_batch.content;
        let decision = self
            .decision_orchestrator
            .evaluate_response(
                event.character_id,
                event.conversation_id,
                &batch_content,
                event.is_mentioned,
                Some(relationship_participant_id),
            )
            .await?;
        self.ensure_event_still_owned(event, lease).await?;
        self.event_bus
            .publish(&CoreEvent::BehaviorDecided(BehaviorDecidedEvent {
                character_id: event.character_id,
                conversation_id: event.conversation_id,
                action: format!("{:?}", decision.action),
                reason: decision.reason.clone(),
                timestamp: Utc::now(),
            }));
        if decision.action != BehaviorAction::Reply {
            self.save_task(event, ReplyTaskStatus::Ignored, None)
                .await?;
            return Ok(());
        }
        if !self.cognition.enabled()
            && stored
                .as_ref()
                .filter(|task| task.status.can_reuse_content())
                .and_then(|task| task.content.as_ref())
                .is_none()
        {
            tracing::info!(
                conversation_id = event.conversation_id,
                "未配置对话模型，保留原始消息并跳过生成"
            );
            self.save_task(event, ReplyTaskStatus::Ignored, None)
                .await?;
            return Ok(());
        }
        // 即时消息不做猜测式关系、情绪和记忆修改；后台批处理记录来源。
        let content = if let Some(content) = stored
            .as_ref()
            .filter(|task| task.status.can_reuse_content())
            .and_then(|task| task.content.clone())
        {
            content
        } else {
            if let Some(repo) = &self.message_repo {
                if !repo
                    .claim_reply_task_for_generation(event.message_id, event.character_id)
                    .await?
                {
                    return Ok(());
                }
            } else {
                self.save_task(event, ReplyTaskStatus::Generating, None)
                    .await?;
            }
            let content = match self
                .generate_reply(
                    event,
                    &character,
                    event.character_id,
                    event.sender_id,
                    sender_user_id,
                    &batch_content,
                )
                .await
            {
                Ok(content) => content,
                Err(error) => {
                    // 生成失败不能把任务永久留在 generating；恢复为 pending
                    // 后，Outbox 的重试才能再次取得生成权。
                    self.save_task_error(event, ReplyTaskStatus::Pending, None, &error.to_string())
                        .await?;
                    return Err(error);
                }
            };
            self.ensure_event_still_owned(event, lease).await?;
            if Self::event_expired(event) {
                self.save_task(event, ReplyTaskStatus::Expired, Some(&content))
                    .await?;
                return Ok(());
            }
            self.save_task(event, ReplyTaskStatus::Generated, Some(&content))
                .await?;
            content
        };
        self.delay_executor
            .delay(capped_delay_ms(decision.delay_ms))
            .await;
        // 绑定切换与发送前复核已经由整个处理期间持有的会话门闩串行化。
        if Self::event_expired(event) {
            self.save_task(event, ReplyTaskStatus::Expired, Some(&content))
                .await?;
            return Ok(());
        }
        // 发送前再次校验绑定和硬约束，避免切换期间发送旧角色回复。
        if self.current_binding(event).await?.is_none()
            || !self
                .runtime
                .refresh_character(event.character_id)
                .await?
                .enabled
        {
            self.save_task(event, ReplyTaskStatus::Cancelled, None)
                .await?;
            return Ok(());
        }
        // 参与判断只在本轮生成前执行一次。这里重新调用决策模型会把一条普通消息
        // 变成“两次参与判断 + 一次生成”，既增加成本，也可能因为模型结果变化而
        // 在发送前把已经生成的回复临时改掉。发送前只复核不会交给模型的硬约束。
        if let (Some(repo), Some(token)) = (&self.message_repo, lease) {
            if !repo.owns_outbox_lease(event.message_id, token).await? {
                return Err(RuntimeError::Internal(
                    "回复任务租约已失效，停止发送".into(),
                ));
            }
        }
        // 在发送之前原子取得发送栅栏；租约失效的旧 worker 不能继续调用平台。
        if let (Some(repo), Some(token)) = (&self.message_repo, lease) {
            if !repo
                .begin_reply_task_delivery(event.message_id, event.character_id, token)
                .await?
            {
                return Err(RuntimeError::Repository(
                    crate::error::RepositoryError::OwnershipLost,
                ));
            }
        } else {
            // 非 outbox 的直接调用路径没有租约，保留原有本地任务状态记录。
            self.save_task(event, ReplyTaskStatus::Sending, None)
                .await?;
        }
        match self
            .action_dispatcher
            .execute_send_message_with_response(event.conversation_id, &content)
            .await
        {
            Ok(response) if response.is_ok() => {
                let platform_message_id = response
                    .data
                    .as_ref()
                    .and_then(|data| data.get("message_id"))
                    .and_then(|value| value.as_i64());
                if let Some(_repo) = &self.message_repo {
                    // 先持久化正文和平台消息 ID，但保持 sending。若本地历史写入
                    // 失败，重启后可据此补写，而不需要再次发送。
                    self.save_delivery_owned(
                        event,
                        lease,
                        ReplyTaskStatus::Sending,
                        Some(&content),
                        None,
                        platform_message_id,
                    )
                    .await?;
                    if let Err(error) = self
                        .persist_assistant_history(event, &character, &content, platform_message_id)
                        .await
                    {
                        self.save_delivery_owned(
                            event,
                            lease,
                            ReplyTaskStatus::Sending,
                            Some(&content),
                            Some(&format!("{ASSISTANT_HISTORY_REPAIR_PREFIX}: {error}")),
                            platform_message_id,
                        )
                        .await?;
                        return Err(error);
                    }
                    self.save_delivery_owned(
                        event,
                        lease,
                        ReplyTaskStatus::Sent,
                        None,
                        None,
                        platform_message_id,
                    )
                    .await?;
                } else {
                    self.save_task(event, ReplyTaskStatus::Sent, None).await?;
                }
            }
            Ok(response) => {
                self.save_delivery_owned(
                    event,
                    lease,
                    ReplyTaskStatus::Generated,
                    Some(&content),
                    Some(&response.error_message()),
                    None,
                )
                .await?;
                return Err(RuntimeError::SendRejected(format!(
                    "OneBot 明确拒绝发送：status={}, retcode={:?}, error={:?}",
                    response.status, response.retcode, response.error
                )));
            }
            Err(e) => {
                let definitely_not_sent = matches!(e, RuntimeError::SendRejected(_));
                if definitely_not_sent {
                    self.save_delivery_owned(
                        event,
                        lease,
                        ReplyTaskStatus::Generated,
                        Some(&content),
                        Some(&e.to_string()),
                        None,
                    )
                    .await?;
                    return Err(e);
                }
                self.save_delivery_owned(
                    event,
                    lease,
                    ReplyTaskStatus::Unknown,
                    Some(&content),
                    Some(&e.to_string()),
                    None,
                )
                .await?;
                tracing::error!(conversation_id=event.conversation_id,message_id=event.message_id,error=%e,"发送结果未知，停止自动补发，等待核查");
                return Ok(());
            }
        }
        Ok(())
    }

    pub async fn process_for_outbox_owned(
        &self,
        event: &MessageReceivedEvent,
        lease: &str,
    ) -> ProcessingOutcome {
        match self.process_owned(event, Some(lease)).await {
            Ok(()) => ProcessingOutcome::Success,
            Err(e) => Self::classify_outbox_error_for_event(e, event.conversation_id),
        }
    }

    /// 处理消息并返回 ProcessingOutcome（用于 outbox 系统）。
    ///
    /// 此方法是 `process()` 的包装，返回 `ProcessingOutcome` 而非 `Result`。
    /// 可恢复的依赖故障映射为 `RetryableFailure`；明确不可恢复的存储故障
    /// 映射为 `PermanentFailure`。受租约保护的入口使用同一分类函数。
    pub async fn process_for_outbox(
        &self,
        event: &MessageReceivedEvent,
    ) -> Result<ProcessingOutcome, RuntimeError> {
        match self.process(event).await {
            Ok(()) => Ok(ProcessingOutcome::Success),
            Err(e) => Ok(Self::classify_outbox_error_for_event(
                e,
                event.conversation_id,
            )),
        }
    }

    /// 将回复链路错误统一映射为 Outbox 结果。
    ///
    /// 受租约保护的入口和旧的无租约入口必须使用完全相同的分类规则，否则
    /// 同一类故障会在不同启动路径下产生不同的重试行为。
    pub(crate) fn classify_outbox_error_for_event(
        error: RuntimeError,
        conversation_id: i64,
    ) -> ProcessingOutcome {
        if matches!(
            &error,
            RuntimeError::Repository(crate::error::RepositoryError::OwnershipLost)
        ) {
            // 会话删除、角色切换或租约回收后的旧事件已经失效；继续重试只会
            // 让已删除会话永久占用队列。将其确认完成即可安全清理 outbox。
            return ProcessingOutcome::Success;
        }
        let error_text = error.to_string();
        let is_permanent = match &error {
            RuntimeError::Repository(crate::error::RepositoryError::Database(message)) => {
                !Self::is_transient_database_error(message)
            }
            RuntimeError::Storage(crate::error::StorageError::Connection(_)) => false,
            RuntimeError::Storage(crate::error::StorageError::Database(message)) => {
                !Self::is_transient_database_error(message)
            }
            _ => false,
        };

        if is_permanent {
            tracing::error!(
                target: "outbox",
                conversation_id,
                error = %error,
                "永久性错误"
            );
            ProcessingOutcome::PermanentFailure { error: error_text }
        } else {
            tracing::warn!(
                target: "outbox",
                conversation_id,
                error = %error,
                "可重试错误"
            );
            ProcessingOutcome::RetryableFailure { error: error_text }
        }
    }

    /// 识别 SQLite/连接层明确表示“稍后重试”的错误。
    ///
    /// 未列出的数据库错误保持永久失败，避免把 schema 错误、约束错误或
    /// 数据损坏无限重试并掩盖真实问题。
    fn is_transient_database_error(message: &str) -> bool {
        let message = message.to_ascii_lowercase();
        [
            "database is locked",
            "database is busy",
            "sqlite_busy",
            "pool timed out",
            "connection reset",
            "connection closed",
            "broken pipe",
            "temporarily unavailable",
        ]
        .iter()
        .any(|marker| message.contains(marker))
    }

    /// 生成回复内容：只接受认知层生成的真实内容；没有可用模型时返回错误。
    async fn generate_reply(
        &self,
        event: &MessageReceivedEvent,
        character: &crate::domain::character::Character,
        character_id: i64,
        participant_id: i64,
        user_id: Option<i64>,
        content: &str,
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
                .generate(crate::application::cognition::ConversationGeneration {
                    character: character.clone(),
                    conversation_id: event.conversation_id,
                    participant_id,
                    user_id,
                    user_message: content.to_string(),
                    is_mentioned: event.is_mentioned,
                    limits: ContextLimits::default(),
                    current_sequence,
                })
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

        Err(RuntimeError::Llm(
            "未配置可用的对话模型，原始消息已保留".into(),
        ))
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
        reply_tasks: Mutex<HashMap<i64, crate::domain::message::ReplyTask>>,
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
        async fn reply_task(
            &self,
            message_id: i64,
        ) -> Result<Option<crate::domain::message::ReplyTask>, RepositoryError> {
            Ok(self.reply_tasks.lock().unwrap().get(&message_id).cloned())
        }
        async fn save_reply_task(
            &self,
            message_id: i64,
            _character_id: i64,
            status: ReplyTaskStatus,
            content: Option<&str>,
        ) -> Result<(), RepositoryError> {
            let mut tasks = self.reply_tasks.lock().unwrap();
            let task = tasks
                .entry(message_id)
                .or_insert(crate::domain::message::ReplyTask {
                    status,
                    content: None,
                    error: None,
                    platform_message_id: None,
                });
            task.status = status;
            if content.is_some() {
                task.content = content.map(str::to_string);
            }
            Ok(())
        }
        async fn save_reply_task_error(
            &self,
            message_id: i64,
            character_id: i64,
            status: ReplyTaskStatus,
            content: Option<&str>,
            error: Option<&str>,
        ) -> Result<(), RepositoryError> {
            self.save_reply_task(message_id, character_id, status, content)
                .await?;
            if let Some(task) = self.reply_tasks.lock().unwrap().get_mut(&message_id) {
                task.error = error.map(str::to_string);
            }
            Ok(())
        }
        async fn save_reply_task_delivery(
            &self,
            message_id: i64,
            character_id: i64,
            status: ReplyTaskStatus,
            content: Option<&str>,
            error: Option<&str>,
            platform_message_id: Option<i64>,
        ) -> Result<(), RepositoryError> {
            self.save_reply_task_error(message_id, character_id, status, content, error)
                .await?;
            if let Some(task) = self.reply_tasks.lock().unwrap().get_mut(&message_id) {
                if platform_message_id.is_some() {
                    task.platform_message_id = platform_message_id;
                }
            }
            Ok(())
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
                .find(|r| r.character_id == character_id && r.user_id == participant_id)
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
                .find(|x| x.character_id == r.character_id && x.user_id == r.user_id)
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
        async fn user_id_for_participant(
            &self,
            participant_id: i64,
        ) -> Result<Option<i64>, RepositoryError> {
            Ok(self
                .participants
                .lock()
                .unwrap()
                .iter()
                .find(|participant| participant.id == participant_id)
                .and_then(|participant| {
                    if participant.role == crate::domain::conversation::ParticipantRole::User {
                        Some(
                            participant
                                .metadata
                                .get("user_id")
                                .and_then(|value| value.as_i64())
                                .unwrap_or(participant.id),
                        )
                    } else {
                        None
                    }
                }))
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
                message: None,
                wording: None,
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
                message: None,
                wording: None,
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
        Arc<MemMessageRepo>,
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
            reply_tasks: Mutex::new(HashMap::new()),
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
        // - 当前角色（id=55，role='character'，external_id = "character:1"）
        // - 普通用户（id=66，role='user'，external_id="u100"）
        // sender_id=66 在 received_event 中对应用户发送者。
        let participant_repo = Arc::new(MemParticipantRepo {
            participants: Mutex::new(vec![
                crate::domain::conversation::Participant {
                    id: 55,
                    conversation_id: 100,
                    external_id: "character:1".to_string(),
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

        let processor = Arc::new(
            ReplyProcessor::new(
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
            )
            .with_message_repository(message_repo.clone()),
        );

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
            message_repo,
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
            metadata: serde_json::json!({}),
        }
    }

    #[tokio::test]
    async fn no_binding_ignores_message() {
        // 覆盖空绑定场景：清空 bindings，process 应正常返回且不发送。
        let (processor, adapter, _, _, _, _, binding_repo, _, _, _, _) = wire(false, false).await;
        binding_repo.bindings.lock().unwrap().clear();
        processor
            .process(&received_event(true))
            .await
            .expect("无绑定时应忽略而非报错");
        assert!(adapter.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn disabled_llm_keeps_message_without_fake_reply() {
        let (processor, adapter, provider, mood_repo, rel_repo, state_repo, _, _, _, _, _) =
            wire(false, false).await;
        processor
            .process(&received_event(true))
            .await
            .expect("处理应成功");

        // LLM 未启用：不应调用 provider。
        assert_eq!(*provider.calls.lock().unwrap(), 0);

        assert!(adapter.sent.lock().unwrap().is_empty());
        assert!(mood_repo.moods.lock().unwrap().is_empty());
        assert!(rel_repo.rels.lock().unwrap().is_empty());
        assert!(state_repo.states.lock().unwrap().contains_key(&1));
    }

    #[tokio::test]
    async fn terminal_reply_task_is_idempotent_and_skips_generation() {
        let (processor, adapter, provider, _, _, _, _, _, _, _, message_repo) =
            wire(false, true).await;
        message_repo.reply_tasks.lock().unwrap().insert(
            42,
            crate::domain::message::ReplyTask {
                status: ReplyTaskStatus::Ignored,
                content: None,
                error: None,
                platform_message_id: None,
            },
        );
        let mut event = received_event(true);
        event.message_id = 42;

        processor
            .process(&event)
            .await
            .expect("终态回复任务重复投递应直接确认");
        assert_eq!(*provider.calls.lock().unwrap(), 0);
        assert!(adapter.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn enabled_llm_uses_provider_reply() {
        let (processor, adapter, provider, _, _, _, _, _, _, _, _) = wire(false, true).await;
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
        let (processor, adapter, _, _, _, _, _, _, _, _, _) = wire(true, true).await;
        processor
            .process(&received_event(true))
            .await
            .expect("处理应成功");
        let sent = adapter.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].0.contains("g100"));
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

    #[test]
    fn batch_contents_preserve_order_and_drop_empty_lines() {
        assert_eq!(
            combine_batch_contents([
                " 在吗 ".to_string(),
                "".to_string(),
                "我有事".to_string(),
                " 你看看这个".to_string(),
            ]),
            "在吗\n我有事\n你看看这个"
        );
    }

    #[test]
    fn outbox_error_classification_is_shared_by_both_entry_points() {
        let permanent = ReplyProcessor::classify_outbox_error_for_event(
            RuntimeError::Repository(RepositoryError::Database("schema is invalid".into())),
            42,
        );
        let retryable = ReplyProcessor::classify_outbox_error_for_event(
            RuntimeError::Llm("temporary upstream timeout".into()),
            42,
        );

        assert!(permanent.is_permanent());
        assert!(!permanent.is_retryable());
        assert!(retryable.is_retryable());
        assert!(!retryable.is_permanent());
    }

    #[test]
    fn transient_database_errors_are_retryable_but_schema_errors_are_permanent() {
        let locked = ReplyProcessor::classify_outbox_error_for_event(
            RuntimeError::Repository(RepositoryError::Database("database is locked".into())),
            42,
        );
        let schema = ReplyProcessor::classify_outbox_error_for_event(
            RuntimeError::Repository(RepositoryError::Database("schema is invalid".into())),
            42,
        );
        let connection = ReplyProcessor::classify_outbox_error_for_event(
            RuntimeError::Storage(crate::error::StorageError::Connection("连接已断开".into())),
            42,
        );

        assert!(locked.is_retryable());
        assert!(schema.is_permanent());
        assert!(connection.is_retryable());
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

        let (processor, adapter, _, _, _, _, _, _, _, _, _) =
            wire_with_delay(false, true, controlled).await;

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
        let (processor, adapter, _, _, _, _, _, _, _, _, _) =
            wire_with_delay(false, true, recording).await;

        processor
            .process(&received_event(true))
            .await
            .expect("处理应成功");

        // 先等待 @ 消息的短暂合并窗口，再等待行为决策给出的拟人化延迟。
        assert_eq!(*recorded.lock().unwrap(), vec![MENTION_BATCH_WAIT_MS, 1600]);
        assert_eq!(adapter.sent.lock().unwrap().len(), 1);
    }
}
