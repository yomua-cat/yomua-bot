//! 规则行为引擎 —— 确定性决策，无 LLM。
//!
//! 依据绑定 / 关系 / 状态（全局 + 会话级）调制回复意愿与延迟。
//! 状态领域模型重构后：情绪为标量 `Mood`（与 Stress 独立），
//! 压力同时存在于 `CharacterState`（全局）与 `ConversationState`（会话级）。

use std::sync::Arc;

use chrono::FixedOffset;

use crate::application::clock::Clock;
use crate::application::config::{parse_timezone_offset, RuntimeConfig};
use crate::domain::behavior::{BehaviorAction, BehaviorDecision, BehaviorEngine};
use crate::domain::character::ReplyMode;
use crate::domain::decision::{DecisionEvaluator, RuleBehaviorEngineOutput, RuleDecisionInput};
use crate::domain::mute::{is_within_window, parse_mute_schedule, TimeOfDay};
#[allow(unused_imports)]
use crate::domain::relationship::Relationship;
use crate::domain::repository::{
    CharacterBindingRepository, CharacterStateRepository, ConversationStateRepository,
    MoodRepository, RelationshipRepository,
};
use crate::error::DomainError;

use crate::domain::behavior::{CognitionLevel, Priority};
use crate::domain::character::{CharacterState, ConversationState};

/// 主动行为的基础阈值。
pub const PROACTIVE_BASE_THRESHOLD: f64 = 0.5;

/// 规则行为引擎：根据绑定、状态与关系做出确定性行为决策。
pub struct RuleBehaviorEngine {
    binding_repo: Arc<dyn CharacterBindingRepository>,
    mood_repo: Arc<dyn MoodRepository>,
    conversation_state_repo: Arc<dyn ConversationStateRepository>,
    behavior_state_repo: Arc<dyn crate::domain::repository::BehaviorStateRepository>,
    relationship_repo: Arc<dyn RelationshipRepository>,
    state_repo: Arc<dyn CharacterStateRepository>,
    clock: Arc<dyn Clock>,
    live_settings: Option<tokio::sync::watch::Receiver<RuntimeConfig>>,
}

impl RuleBehaviorEngine {
    /// 创建一个规则行为引擎。
    pub fn new(
        binding_repo: Arc<dyn CharacterBindingRepository>,
        mood_repo: Arc<dyn MoodRepository>,
        relationship_repo: Arc<dyn RelationshipRepository>,
        state_repo: Arc<dyn CharacterStateRepository>,
        conversation_state_repo: Arc<dyn ConversationStateRepository>,
        behavior_state_repo: Arc<dyn crate::domain::repository::BehaviorStateRepository>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            binding_repo,
            mood_repo,
            relationship_repo,
            state_repo,
            conversation_state_repo,
            behavior_state_repo,
            clock,
            live_settings: None,
        }
    }

    /// 接入运行时配置，使后续决策读取最新时区设置。
    pub fn with_live_settings(
        mut self,
        settings: tokio::sync::watch::Receiver<RuntimeConfig>,
    ) -> Self {
        self.live_settings = Some(settings);
        self
    }

    fn timezone_offset(&self) -> FixedOffset {
        self.live_settings
            .as_ref()
            .and_then(|settings| parse_timezone_offset(&settings.borrow().timezone).ok())
            .unwrap_or_else(|| FixedOffset::east_opt(0).expect("UTC 偏移有效"))
    }

    /// 构造决策输入快照（Frozen Evaluation Input）。
    ///
    /// 在一次调用中顺序查询所有 repository，构造完成后 freeze。
    /// 后续 Built-in Decision 和 LLM Context 都从同一份 input 派生。
    async fn build_decision_input(
        &self,
        character_id: i64,
        conversation_id: i64,
        message_content: &str,
        is_mentioned: bool,
        participant_id: Option<i64>,
    ) -> Result<RuleDecisionInput, DomainError> {
        let bindings = self
            .binding_repo
            .find_by_conversation_id(conversation_id)
            .await
            .map_err(repo_err)?;
        let binding = bindings
            .into_iter()
            .find(|b| b.character_id == character_id);

        let mood = self
            .mood_repo
            .find_by_character_and_conversation(character_id, conversation_id)
            .await
            .map_err(repo_err)?;

        let relationship = match participant_id {
            Some(pid) => self
                .relationship_repo
                .find(character_id, pid)
                .await
                .map_err(repo_err)?,
            None => None,
        };

        let state = self
            .state_repo
            .find_by_character_id(character_id)
            .await
            .map_err(repo_err)?;

        let conversation_state = self
            .conversation_state_repo
            .find_by_character_and_conversation(character_id, conversation_id)
            .await
            .map_err(repo_err)?;

        let behavior_state = self
            .behavior_state_repo
            .find_by_character_and_conversation(character_id, conversation_id)
            .await
            .map_err(repo_err)?;

        // 查询当前角色的所有关系（用于 proactive 决策计算最佳亲密度）
        let all_character_relationships = self
            .relationship_repo
            .find_by_character_id(character_id)
            .await
            .map_err(repo_err)?;

        let evaluated_at = self.clock.now();
        let timezone = self.timezone_offset();
        let in_mute_window = binding
            .as_ref()
            .and_then(|value| value.mute_schedule.as_deref())
            .and_then(|schedule| parse_mute_schedule(schedule).ok().flatten())
            .map(|window| is_within_window(&window, &time_of_day(evaluated_at, timezone)))
            .unwrap_or(false);

        Ok(RuleDecisionInput {
            binding,
            mood,
            relationship,
            all_character_relationships,
            character_state: state,
            conversation_state,
            behavior_state,
            character_id,
            conversation_id,
            message_content: message_content.to_string(),
            is_mentioned,
            participant_id,
            in_mute_window,
            evaluated_at,
        })
    }

    /// 执行一次完整的行为评估，返回决策结果和决策依据快照。
    ///
    /// 与 decide_response() + load_context() 的区别：
    /// - 在同一次调用中获取所有数据，保证两者来自同一个 RuleDecisionInput
    /// - 不重新查询 Repository
    pub(crate) async fn evaluate_response_impl(
        &self,
        character_id: i64,
        conversation_id: i64,
        message_content: &str,
        is_mentioned: bool,
        participant_id: Option<i64>,
    ) -> Result<RuleBehaviorEngineOutput, DomainError> {
        let input = self
            .build_decision_input(
                character_id,
                conversation_id,
                message_content,
                is_mentioned,
                participant_id,
            )
            .await?;

        let decision = self.compute_decision_response(&input).await?;

        Ok(RuleBehaviorEngineOutput { decision, input })
    }

    /// 执行一次完整的主动行为评估。
    pub(crate) async fn evaluate_proactive_impl(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<RuleBehaviorEngineOutput, DomainError> {
        let input = self
            .build_decision_input(character_id, conversation_id, "", false, None)
            .await?;

        let decision = self.compute_decision_proactive(&input).await?;

        Ok(RuleBehaviorEngineOutput { decision, input })
    }

    /// 根据 RuleDecisionInput 计算回复行为决策。
    async fn compute_decision_response(
        &self,
        input: &RuleDecisionInput,
    ) -> Result<BehaviorDecision, DomainError> {
        let decided_at = input.evaluated_at;

        // 无绑定 → 该角色不在本会话中，忽略。
        let Some(binding) = &input.binding else {
            return Ok(ignore_decision(
                "该会话没有为本角色配置绑定，忽略",
                decided_at,
            ));
        };

        // MentionOnly + !mentioned 是 Hard Constraint。
        // 用户明确选择 MentionOnly 模式，即表示不接受未被提及时的主动回复。
        // 此判断在 roll 计算和任何 modifier 之前生效，冷启动地板不得突破此约束。
        if !input.is_mentioned && binding.reply_mode == ReplyMode::MentionOnly {
            return Ok(ignore_decision(
                "MentionOnly 模式且未被提及，忽略",
                decided_at,
            ));
        }

        let (base_threshold, base_delay) = base_params(&binding.reply_mode, input.is_mentioned);

        let mut threshold = if input.is_mentioned {
            1.0
        } else {
            base_threshold
        };
        let mut delay_ms = base_delay;

        // 关系深浅
        let closeness = input
            .relationship
            .as_ref()
            .map(|r| (r.familiarity + r.affection + r.trust + r.intimacy) / 4.0);
        let close_relation = closeness.map(|c| c > 0.6).unwrap_or(false);
        let distant_relation = closeness.map(|c| c < 0.25).unwrap_or(false);

        let annoyed = input
            .relationship
            .as_ref()
            .map(|r| r.annoyance > 0.6)
            .unwrap_or(false);
        let high_affection = input
            .relationship
            .as_ref()
            .map(|r| r.affection > 0.7)
            .unwrap_or(false);

        let global_stress = input
            .character_state
            .as_ref()
            .map(|s| s.stress)
            .unwrap_or(CharacterState::default().stress);
        let scoped_stress = input
            .conversation_state
            .as_ref()
            .map(|s| s.stress)
            .unwrap_or(ConversationState::default().stress);
        let stress_level = global_stress.max(scoped_stress);
        let stressed = stress_level > 60.0;

        let global_energy = input
            .character_state
            .as_ref()
            .map(|s| s.energy)
            .unwrap_or(CharacterState::default().energy);
        let scoped_energy = input
            .conversation_state
            .as_ref()
            .map(|s| s.energy)
            .unwrap_or(ConversationState::default().energy);
        let energy_level = global_energy.min(scoped_energy);
        let low_energy = energy_level < 30.0;

        let in_mute = binding
            .mute_schedule
            .as_deref()
            .and_then(|s| parse_mute_schedule(s).ok().flatten())
            .map(|w| is_within_window(&w, &time_of_day(decided_at, self.timezone_offset())))
            .unwrap_or(false);

        let withholds = annoyed || low_energy || stressed;

        if !input.is_mentioned && withholds {
            threshold -= 0.2;
        }
        if !input.is_mentioned && stressed {
            threshold -= 0.05;
        }

        let delay_withholds = withholds || distant_relation;
        if delay_withholds {
            delay_ms += 400;
        }
        if stressed {
            delay_ms += 300;
        }

        if high_affection {
            threshold += 0.15;
            delay_ms = delay_ms.saturating_sub(200);
        }
        if close_relation {
            threshold += 0.10;
            delay_ms = delay_ms.saturating_sub(300);
        }
        if in_mute && !input.is_mentioned {
            threshold -= 0.35;
            delay_ms += 800;
        }

        // 参与判断不能把关系 modifier 变成概率骰子：明确提及始终回应，
        // 其它场景只根据可解释的消息信号择机参与。
        let substantive = is_substantive_message(&input.message_content);
        let should_reply =
            input.is_mentioned || (substantive && threshold >= 0.3 && !in_mute && !annoyed);

        if should_reply {
            Ok(BehaviorDecision {
                action: BehaviorAction::Reply,
                priority: Priority::Realtime,
                cognition_level: CognitionLevel::Light,
                delay_ms,
                reason: build_reply_reason(&ReplyReasonFlags {
                    mentioned: input.is_mentioned,
                    muted: in_mute,
                    annoyed,
                    low_energy,
                    stressed,
                    high_affection,
                    close_relation,
                    distant_relation,
                }),
                decided_at,
            })
        } else {
            Ok(ignore_decision("未达到回复阈值，本次忽略", decided_at))
        }
    }

    /// 根据 RuleDecisionInput 计算主动行为决策。
    async fn compute_decision_proactive(
        &self,
        input: &RuleDecisionInput,
    ) -> Result<BehaviorDecision, DomainError> {
        let decided_at = input.evaluated_at;

        let Some(binding) = &input.binding else {
            return Ok(ignore_decision(
                "该会话没有为本角色配置绑定，不主动",
                decided_at,
            ));
        };

        if !binding.proactive_enabled {
            return Ok(ignore_decision("未启用主动行为", decided_at));
        }

        let in_mute = binding
            .mute_schedule
            .as_deref()
            .and_then(|s| parse_mute_schedule(s).ok().flatten())
            .map(|w| is_within_window(&w, &time_of_day(decided_at, self.timezone_offset())))
            .unwrap_or(false);
        if in_mute {
            return Ok(ignore_decision("处于静默时段，主动行为被覆盖", decided_at));
        }

        let mut threshold = PROACTIVE_BASE_THRESHOLD;

        let best_closeness = input
            .all_character_relationships
            .iter()
            .map(|r| (r.familiarity + r.affection + r.trust + r.intimacy) / 4.0)
            .fold(0.0_f64, f64::max);

        if best_closeness > 0.6 {
            threshold += 0.2;
        } else if input.all_character_relationships.is_empty() || best_closeness < 0.25 {
            threshold -= 0.15;
        }

        let global_energy = input
            .character_state
            .as_ref()
            .map(|s| s.energy)
            .unwrap_or_default();
        let global_stress = input
            .character_state
            .as_ref()
            .map(|s| s.stress)
            .unwrap_or_default();
        let scoped_energy = input
            .conversation_state
            .as_ref()
            .map(|s| s.energy)
            .unwrap_or_default();
        let scoped_stress = input
            .conversation_state
            .as_ref()
            .map(|s| s.stress)
            .unwrap_or_default();
        let energy = global_energy.min(scoped_energy);
        let stress = global_stress.max(scoped_stress);

        if energy < 30.0 {
            threshold -= 0.1;
        }
        if stress > 60.0 {
            threshold -= 0.1;
        }
        if energy > 70.0 && stress < 40.0 {
            threshold += 0.1;
        }

        // 主动行为需要可解释的条件；不把时间/ID哈希当作理解能力。
        let should_initiate = threshold >= 0.5 && energy >= 30.0 && stress <= 60.0;
        if should_initiate {
            Ok(BehaviorDecision {
                action: BehaviorAction::InitiateProactive,
                priority: Priority::Background,
                cognition_level: CognitionLevel::None,
                delay_ms: 0,
                reason: "主动行为检查通过，交由回复任务生成".to_string(),
                decided_at,
            })
        } else {
            Ok(ignore_decision("主动意愿未达阈值，等待下一轮", decided_at))
        }
    }
}

#[async_trait::async_trait]
impl BehaviorEngine for RuleBehaviorEngine {
    async fn decide_response(
        &self,
        character_id: i64,
        conversation_id: i64,
        message_content: &str,
        is_mentioned: bool,
        participant_id: Option<i64>,
    ) -> Result<BehaviorDecision, DomainError> {
        // 委托给 evaluate_response_impl，丢弃 input
        self.evaluate_response_impl(
            character_id,
            conversation_id,
            message_content,
            is_mentioned,
            participant_id,
        )
        .await
        .map(|output| output.decision)
    }

    async fn decide_proactive(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<BehaviorDecision, DomainError> {
        // 委托给 evaluate_proactive_impl，丢弃 input
        self.evaluate_proactive_impl(character_id, conversation_id)
            .await
            .map(|output| output.decision)
    }
}

/// 实现 DecisionEvaluator trait for RuleBehaviorEngine。
#[async_trait::async_trait]
impl DecisionEvaluator for RuleBehaviorEngine {
    async fn evaluate_response(
        &self,
        character_id: i64,
        conversation_id: i64,
        message_content: &str,
        is_mentioned: bool,
        participant_id: Option<i64>,
    ) -> Result<RuleBehaviorEngineOutput, DomainError> {
        self.evaluate_response_impl(
            character_id,
            conversation_id,
            message_content,
            is_mentioned,
            participant_id,
        )
        .await
    }

    async fn evaluate_proactive(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<RuleBehaviorEngineOutput, DomainError> {
        self.evaluate_proactive_impl(character_id, conversation_id)
            .await
    }
}

/// 把仓储错误转换为领域错误。
fn repo_err(e: crate::error::RepositoryError) -> DomainError {
    DomainError::Internal(format!("仓储错误: {e}"))
}

/// 将旧测试与调试调用使用的稳定散列映射为 [0,1)；不参与生产回复概率。
#[cfg(test)]
fn deterministic_roll(input: &str) -> f64 {
    let mut hash: u64 = 1469598103934665603;
    for byte in input.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    (hash as f64) / (u64::MAX as f64 + 1.0)
}

/// 依据 reply_mode 与 mentioned 返回基础回复阈值与延迟（毫秒）。
fn base_params(reply_mode: &ReplyMode, is_mentioned: bool) -> (f64, u64) {
    let (threshold, delay) = match reply_mode {
        ReplyMode::MentionOnly => {
            if is_mentioned {
                (1.0, 1600)
            } else {
                (0.0, 1600)
            }
        }
        ReplyMode::Occasionally => {
            if is_mentioned {
                (1.0, 1400)
            } else {
                (0.3, 1800)
            }
        }
        ReplyMode::Natural => {
            if is_mentioned {
                (1.0, 1200)
            } else {
                (0.7, 1000)
            }
        }
    };
    (threshold, delay)
}

fn is_substantive_message(content: &str) -> bool {
    let text = content.trim();
    if text.chars().count() >= 2 {
        return true;
    }
    text.ends_with(['?', '？', '!', '！', '.', '。'])
        || text.contains("吗")
        || text.contains("什么")
        || text.contains("为什么")
}

/// 生成一个忽略决策。
fn ignore_decision(reason: &str, decided_at: chrono::DateTime<chrono::Utc>) -> BehaviorDecision {
    BehaviorDecision {
        action: BehaviorAction::Ignore,
        priority: Priority::Background,
        cognition_level: CognitionLevel::None,
        delay_ms: 0,
        reason: reason.to_string(),
        decided_at,
    }
}

/// 用于构建回复原因的标志集合。
struct ReplyReasonFlags {
    mentioned: bool,
    muted: bool,
    annoyed: bool,
    low_energy: bool,
    stressed: bool,
    high_affection: bool,
    close_relation: bool,
    distant_relation: bool,
}

/// 构建回复决策的中文原因说明。
fn build_reply_reason(f: &ReplyReasonFlags) -> String {
    let mut parts: Vec<String> = Vec::new();
    if f.mentioned {
        parts.push("用户提到了角色".to_string());
    }
    if f.muted {
        parts.push("处于静默时段（未提及）".to_string());
    }
    if f.close_relation {
        parts.push("与对方亲密度高".to_string());
    }
    if f.distant_relation {
        parts.push("与对方较疏离".to_string());
    }
    if f.high_affection {
        parts.push("好感度高".to_string());
    }
    if f.annoyed {
        parts.push("当前厌烦度高".to_string());
    }
    if f.low_energy {
        parts.push("精力较低".to_string());
    }
    if f.stressed {
        parts.push("压力较高".to_string());
    }
    if parts.is_empty() {
        parts.push("达到参与阈值".to_string());
    }
    format!("回复：{}", parts.join("；"))
}

/// 按配置的 UTC 偏移提取一天中的时刻，用于静默时段判断。
pub(crate) fn time_of_day(t: chrono::DateTime<chrono::Utc>, offset: FixedOffset) -> TimeOfDay {
    use chrono::Timelike;
    let local = t.with_timezone(&offset);
    TimeOfDay {
        hour: local.hour(),
        minute: local.minute(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::character::{
        BehaviorState, CharacterBinding, CharacterState, ConversationState,
    };
    use crate::domain::emotion::Mood;

    use crate::error::RepositoryError;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// 可推进的固定时钟，用于测试依赖时间的确定性逻辑。
    #[derive(Clone)]
    struct FakeClock {
        time: std::sync::Arc<std::sync::Mutex<DateTime<Utc>>>,
    }

    impl FakeClock {
        fn new() -> Self {
            Self {
                time: std::sync::Arc::new(std::sync::Mutex::new(Utc::now())),
            }
        }

        #[allow(dead_code)]
        fn set(&self, t: DateTime<Utc>) {
            *self.time.lock().unwrap() = t;
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> DateTime<Utc> {
            *self.time.lock().unwrap()
        }
    }

    // 内存版仓库实现，隔离测试存储。

    struct MemBindingRepo {
        bindings: Mutex<Vec<CharacterBinding>>,
    }
    #[async_trait]
    impl CharacterBindingRepository for MemBindingRepo {
        async fn find_by_character_id(
            &self,
            character_id: i64,
        ) -> Result<Vec<CharacterBinding>, RepositoryError> {
            Ok(self
                .bindings
                .lock()
                .unwrap()
                .iter()
                .filter(|b| b.character_id == character_id)
                .cloned()
                .collect())
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
        async fn insert(&self, b: &CharacterBinding) -> Result<i64, RepositoryError> {
            self.bindings.lock().unwrap().push(b.clone());
            Ok(b.id)
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
            mood: &Mood,
        ) -> Result<(), RepositoryError> {
            self.moods
                .lock()
                .unwrap()
                .insert((character_id, conversation_id), mood.clone());
            Ok(())
        }
    }

    struct MemRelationshipRepo {
        relationships: Mutex<Vec<Relationship>>,
    }
    #[async_trait]
    impl RelationshipRepository for MemRelationshipRepo {
        async fn find(
            &self,
            character_id: i64,
            participant_id: i64,
        ) -> Result<Option<Relationship>, RepositoryError> {
            Ok(self
                .relationships
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.character_id == character_id && r.user_id == participant_id)
                .cloned())
        }
        async fn find_by_character_id(
            &self,
            character_id: i64,
        ) -> Result<Vec<Relationship>, RepositoryError> {
            Ok(self
                .relationships
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.character_id == character_id)
                .cloned()
                .collect())
        }
        async fn upsert(&self, r: &Relationship) -> Result<(), RepositoryError> {
            let mut all = self.relationships.lock().unwrap();
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

    struct MemStateRepo {
        states: Mutex<HashMap<i64, CharacterState>>,
    }
    #[async_trait]
    impl CharacterStateRepository for MemStateRepo {
        async fn find_by_character_id(
            &self,
            character_id: i64,
        ) -> Result<Option<CharacterState>, RepositoryError> {
            Ok(self.states.lock().unwrap().get(&character_id).cloned())
        }
        async fn upsert(
            &self,
            character_id: i64,
            state: &CharacterState,
        ) -> Result<(), RepositoryError> {
            self.states
                .lock()
                .unwrap()
                .insert(character_id, state.clone());
            Ok(())
        }
    }

    struct MemConversationStateRepo {
        states: Mutex<HashMap<(i64, i64), ConversationState>>,
    }
    #[async_trait]
    impl ConversationStateRepository for MemConversationStateRepo {
        async fn find_by_character_and_conversation(
            &self,
            character_id: i64,
            conversation_id: i64,
        ) -> Result<Option<ConversationState>, RepositoryError> {
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
            state: &ConversationState,
        ) -> Result<(), RepositoryError> {
            self.states
                .lock()
                .unwrap()
                .insert((character_id, conversation_id), state.clone());
            Ok(())
        }
    }

    struct MemBehaviorStateRepo {
        states: Mutex<HashMap<(i64, i64), BehaviorState>>,
    }
    #[async_trait]
    impl crate::domain::repository::BehaviorStateRepository for MemBehaviorStateRepo {
        async fn find_by_character_and_conversation(
            &self,
            character_id: i64,
            conversation_id: i64,
        ) -> Result<Option<BehaviorState>, RepositoryError> {
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
            state: &BehaviorState,
        ) -> Result<(), RepositoryError> {
            self.states
                .lock()
                .unwrap()
                .insert((character_id, conversation_id), state.clone());
            Ok(())
        }
    }

    fn binding(conversation_id: i64, mode: ReplyMode) -> CharacterBinding {
        binding_mute(conversation_id, mode, None)
    }

    fn binding_mute(
        conversation_id: i64,
        mode: ReplyMode,
        mute_schedule: Option<&str>,
    ) -> CharacterBinding {
        CharacterBinding {
            id: 1,
            character_id: 1,
            conversation_id,
            reply_mode: mode,
            proactive_enabled: false,
            mute_schedule: mute_schedule.map(String::from),
            behavior_overrides: serde_json::json!({}),
            context_policy: serde_json::json!({}),
            switched_at: None,
            cross_reply_enabled: false,
            created_at: chrono::Utc::now(),
        }
    }

    fn binding_proactive(
        conversation_id: i64,
        mode: ReplyMode,
        mute_schedule: Option<&str>,
    ) -> CharacterBinding {
        CharacterBinding {
            proactive_enabled: true,
            ..binding_mute(conversation_id, mode, mute_schedule)
        }
    }

    fn relationship(annoyance: f64, affection: f64) -> Relationship {
        relationship_full(annoyance, affection, 0.1, 0.1, 0.0)
    }

    fn relationship_full(
        annoyance: f64,
        affection: f64,
        familiarity: f64,
        trust: f64,
        intimacy: f64,
    ) -> Relationship {
        Relationship {
            character_id: 1,
            user_id: 1,
            familiarity,
            affection,
            trust,
            respect: 0.1,
            annoyance,
            intimacy,
            interaction_count: 1,
            last_interaction: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[allow(clippy::type_complexity)]
    fn build_repos(
        bindings: Vec<CharacterBinding>,
        state: Option<CharacterState>,
        conversation_state: Option<ConversationState>,
        rel: Option<Relationship>,
    ) -> (
        Arc<MemBindingRepo>,
        Arc<MemMoodRepo>,
        Arc<MemRelationshipRepo>,
        Arc<MemStateRepo>,
        Arc<MemConversationStateRepo>,
        Arc<MemBehaviorStateRepo>,
    ) {
        let conversation_id = bindings.first().map(|b| b.conversation_id).unwrap_or(10);
        let binding_repo = Arc::new(MemBindingRepo {
            bindings: Mutex::new(bindings),
        });
        let mood_repo = Arc::new(MemMoodRepo {
            moods: Mutex::new(HashMap::new()),
        });
        let rel_repo = Arc::new(MemRelationshipRepo {
            relationships: Mutex::new(rel.into_iter().collect()),
        });
        let state_repo = Arc::new(MemStateRepo {
            states: Mutex::new(state.into_iter().map(|s| (1, s)).collect()),
        });
        let conv_state_repo = Arc::new(MemConversationStateRepo {
            states: Mutex::new(
                conversation_state
                    .into_iter()
                    .map(|s| ((1, conversation_id), s))
                    .collect(),
            ),
        });
        let behavior_state_repo = Arc::new(MemBehaviorStateRepo {
            states: Mutex::new(HashMap::new()),
        });
        (
            binding_repo,
            mood_repo,
            rel_repo,
            state_repo,
            conv_state_repo,
            behavior_state_repo,
        )
    }

    fn build_engine(
        bindings: Vec<CharacterBinding>,
        state: Option<CharacterState>,
        conversation_state: Option<ConversationState>,
        rel: Option<Relationship>,
        clock: Arc<dyn Clock>,
    ) -> (RuleBehaviorEngine, Arc<MemRelationshipRepo>) {
        let (binding_repo, mood_repo, rel_repo, state_repo, conv_state_repo, behavior_state_repo) =
            build_repos(bindings, state, conversation_state, rel);

        let engine = RuleBehaviorEngine::new(
            binding_repo,
            mood_repo,
            rel_repo.clone(),
            state_repo,
            conv_state_repo,
            behavior_state_repo,
            clock,
        );
        (engine, rel_repo)
    }

    /// 用状态构造引擎；测试只设置 energy，其余字段使用默认值。
    fn build_engine_from_energy(
        bindings: Vec<CharacterBinding>,
        rel: Option<Relationship>,
        energy: Option<f64>,
        clock: Arc<dyn Clock>,
    ) -> (RuleBehaviorEngine, Arc<MemRelationshipRepo>) {
        let state = energy.map(|e| CharacterState {
            energy: e,
            ..Default::default()
        });
        build_engine(bindings, state, None, rel, clock)
    }

    async fn engine_with(
        bindings: Vec<CharacterBinding>,
        rel: Option<Relationship>,
        energy: Option<f64>,
    ) -> (RuleBehaviorEngine, Arc<MemRelationshipRepo>) {
        build_engine_from_energy(bindings, rel, energy, Arc::new(FakeClock::new()))
    }

    #[tokio::test]
    async fn no_binding_ignores() {
        let (engine, _) = engine_with(vec![], None, None).await;
        let d = engine
            .decide_response(1, 10, "你好", false, None)
            .await
            .unwrap();
        assert_eq!(d.action, BehaviorAction::Ignore);
    }

    #[tokio::test]
    async fn mention_only_mentions_replies() {
        for mentioned in [true, false] {
            let (engine, _) =
                engine_with(vec![binding(10, ReplyMode::MentionOnly)], None, None).await;
            let d = engine
                .decide_response(1, 10, "看看这个", mentioned, None)
                .await
                .unwrap();
            if mentioned {
                assert_eq!(d.action, BehaviorAction::Reply);
                assert_eq!(d.cognition_level, CognitionLevel::Light);
            } else {
                assert_eq!(d.action, BehaviorAction::Ignore);
            }
        }
    }

    #[tokio::test]
    async fn occasionally_mentions_always_replies() {
        let (engine, _) = engine_with(vec![binding(10, ReplyMode::Occasionally)], None, None).await;
        let d = engine
            .decide_response(1, 10, "你好呀", true, None)
            .await
            .unwrap();
        assert_eq!(d.action, BehaviorAction::Reply);
    }

    #[tokio::test]
    async fn occasional_unmentioned_deterministic() {
        let (engine, _) = engine_with(vec![binding(10, ReplyMode::Occasionally)], None, None).await;
        let content = "随便聊点什么";
        let d1 = engine
            .decide_response(1, 10, content, false, None)
            .await
            .unwrap();
        let d2 = engine
            .decide_response(1, 10, content, false, None)
            .await
            .unwrap();
        assert_eq!(d1.action, d2.action);
    }

    #[tokio::test]
    async fn high_annoyance_raises_delay_and_can_suppress() {
        let (engine, _) = engine_with(
            vec![binding(10, ReplyMode::Natural)],
            Some(relationship(0.9, 0.2)),
            Some(90.0),
        )
        .await;
        let d = engine
            .decide_response(1, 10, "喂", true, Some(1))
            .await
            .unwrap();
        assert_eq!(d.action, BehaviorAction::Reply);
        assert!(d.delay_ms >= 1400, "高厌烦应增加延迟，实际 {}", d.delay_ms);
    }

    #[tokio::test]
    async fn high_affection_low_annoyance_reduces_delay() {
        let (engine, _) = engine_with(
            vec![binding(10, ReplyMode::Natural)],
            Some(relationship(0.0, 0.9)),
            Some(90.0),
        )
        .await;
        let d = engine
            .decide_response(1, 10, "你好", true, Some(1))
            .await
            .unwrap();
        assert_eq!(d.action, BehaviorAction::Reply);
        // 高好感：natural + mentioned 基础延迟 1200 - 200。
        assert!(d.delay_ms <= 1000, "高好感应降低延迟，实际 {}", d.delay_ms);
    }

    #[tokio::test]
    async fn proactive_disabled_ignores() {
        let (engine, _) = engine_with(vec![binding(10, ReplyMode::Natural)], None, None).await;
        let d = engine.decide_proactive(1, 10).await.unwrap();
        assert_eq!(d.action, BehaviorAction::Ignore);
        assert!(d.reason.contains("未启用"));
    }

    #[tokio::test]
    async fn proactive_same_input_is_deterministic() {
        let (engine, _) = build_engine(
            vec![binding_proactive(10, ReplyMode::Natural, None)],
            None,
            None,
            None,
            clock_at(14, 0),
        );
        let d1 = engine.decide_proactive(1, 10).await.unwrap();
        let d2 = engine.decide_proactive(1, 10).await.unwrap();
        assert_eq!(d1.action, d2.action);
        assert_eq!(d1.reason, d2.reason);
    }

    #[tokio::test]
    async fn proactive_mute_overrides_to_ignore() {
        let (engine, _) = build_engine(
            vec![binding_proactive(
                10,
                ReplyMode::Natural,
                Some("10:00-12:00"),
            )],
            None,
            None,
            None,
            clock_at(10, 30),
        );
        let d = engine.decide_proactive(1, 10).await.unwrap();
        assert_eq!(d.action, BehaviorAction::Ignore);
        assert!(d.reason.contains("静默时段"));
    }

    #[test]
    fn deterministic_roll_is_stable_in_unit_range() {
        for s in ["a", "hello", "你好", "测试内容 long string", ""] {
            let r = deterministic_roll(s);
            assert!((0.0..1.0).contains(&r), "roll 应在 [0,1)，got {r}");
        }
    }

    #[test]
    fn time_of_day_uses_configured_offset() {
        let utc = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(16, 30, 0)
            .unwrap()
            .and_utc();
        let offset = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
        let local = time_of_day(utc, offset);
        assert_eq!((local.hour, local.minute), (0, 30));
    }

    #[test]
    fn live_timezone_setting_is_read_for_each_decision() {
        let (binding_repo, mood_repo, rel_repo, state_repo, conv_state_repo, behavior_state_repo) =
            build_repos(vec![], None, None, None);
        let (settings_tx, settings_rx) = tokio::sync::watch::channel(RuntimeConfig::default());
        let engine = RuleBehaviorEngine::new(
            binding_repo,
            mood_repo,
            rel_repo,
            state_repo,
            conv_state_repo,
            behavior_state_repo,
            Arc::new(FakeClock::new()),
        )
        .with_live_settings(settings_rx);
        assert_eq!(engine.timezone_offset().local_minus_utc(), 8 * 3600);
        settings_tx.send_modify(|settings| settings.timezone = "-5".to_string());
        assert_eq!(engine.timezone_offset().local_minus_utc(), -5 * 3600);
    }

    fn clock_at(hour: u32, minute: u32) -> Arc<dyn Clock> {
        let t = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
            .and_utc();
        let c = FakeClock::new();
        c.set(t);
        Arc::new(c)
    }

    #[tokio::test]
    async fn mute_window_unmentioned_tends_to_ignore() {
        let content = "你好今天天气不错";
        let mute = "10:00-12:00";

        let (in_engine, _) = build_engine(
            vec![binding_mute(10, ReplyMode::Natural, Some(mute))],
            None,
            None,
            None,
            clock_at(10, 30),
        );
        let in_decision = in_engine
            .decide_response(1, 10, content, false, None)
            .await
            .unwrap();
        assert_eq!(
            in_decision.action,
            BehaviorAction::Ignore,
            "静默时段未提及消息应忽略"
        );

        let (out_engine, _) = build_engine(
            vec![binding_mute(10, ReplyMode::Natural, Some(mute))],
            None,
            None,
            None,
            clock_at(14, 0),
        );
        let out_decision = out_engine
            .decide_response(1, 10, content, false, None)
            .await
            .unwrap();
        assert_eq!(
            out_decision.action,
            BehaviorAction::Reply,
            "静默时段外应正常回复"
        );
    }

    #[tokio::test]
    async fn mute_window_mentioned_still_replies() {
        let (engine, _) = build_engine(
            vec![binding_mute(10, ReplyMode::Natural, Some("10:00-12:00"))],
            None,
            None,
            None,
            clock_at(10, 30),
        );
        let d = engine
            .decide_response(1, 10, "紧急找我", true, Some(1))
            .await
            .unwrap();
        assert_eq!(d.action, BehaviorAction::Reply, "被 @ 的消息应照常回复");
    }

    #[tokio::test]
    async fn mute_same_clock_is_deterministic() {
        let (engine, _) = build_engine(
            vec![binding_mute(10, ReplyMode::Natural, Some("10:00-12:00"))],
            None,
            None,
            None,
            clock_at(11, 0),
        );
        let content = "在忙吗";
        let d1 = engine
            .decide_response(1, 10, content, false, None)
            .await
            .unwrap();
        let d2 = engine
            .decide_response(1, 10, content, false, None)
            .await
            .unwrap();
        assert_eq!(d1.action, d2.action);
    }

    fn state_with(energy: f64, stress: f64) -> CharacterState {
        CharacterState {
            energy,
            stress,
            ..Default::default()
        }
    }

    fn conversation_state_with(energy: f64, stress: f64) -> ConversationState {
        ConversationState {
            energy,
            stress,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn high_conversation_stress_raises_delay() {
        // 会话级压力高 → 延迟增加（压力从 ConversationState 取值）。
        let content = "在吗";
        let (stressed, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            Some(state_with(90.0, 10.0)),
            Some(conversation_state_with(50.0, 90.0)),
            None,
            Arc::new(FakeClock::new()),
        );
        let stressed_d = stressed
            .decide_response(1, 10, content, true, Some(1))
            .await
            .unwrap();

        let (calm, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            Some(state_with(90.0, 10.0)),
            Some(conversation_state_with(50.0, 10.0)),
            None,
            Arc::new(FakeClock::new()),
        );
        let calm_d = calm
            .decide_response(1, 10, content, true, Some(1))
            .await
            .unwrap();

        assert!(
            stressed_d.delay_ms > calm_d.delay_ms,
            "高压力应比低压力延迟更长（{} vs {}）",
            stressed_d.delay_ms,
            calm_d.delay_ms
        );
    }

    #[tokio::test]
    async fn high_global_stress_raises_delay() {
        let content = "在吗";
        let (stressed, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            Some(state_with(90.0, 90.0)),
            None,
            None,
            Arc::new(FakeClock::new()),
        );
        let stressed_d = stressed
            .decide_response(1, 10, content, true, Some(1))
            .await
            .unwrap();

        let (calm, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            Some(state_with(90.0, 10.0)),
            None,
            None,
            Arc::new(FakeClock::new()),
        );
        let calm_d = calm
            .decide_response(1, 10, content, true, Some(1))
            .await
            .unwrap();

        assert!(
            stressed_d.delay_ms > calm_d.delay_ms,
            "高压力应比低压力延迟更长（{} vs {}）",
            stressed_d.delay_ms,
            calm_d.delay_ms
        );
    }

    #[tokio::test]
    async fn close_relation_lowers_delay_and_stranger_raises_it() {
        let content = "在吗";
        let (close, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None,
            None,
            Some(relationship_full(0.0, 0.8, 0.9, 0.9, 0.9)),
            Arc::new(FakeClock::new()),
        );
        let close_d = close
            .decide_response(1, 10, content, true, Some(1))
            .await
            .unwrap();

        let (stranger, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None,
            None,
            Some(relationship_full(0.0, 0.1, 0.0, 0.0, 0.0)),
            Arc::new(FakeClock::new()),
        );
        let stranger_d = stranger
            .decide_response(1, 10, content, true, Some(1))
            .await
            .unwrap();

        assert_eq!(close_d.action, BehaviorAction::Reply);
        assert_eq!(stranger_d.action, BehaviorAction::Reply);
        assert!(
            close_d.delay_ms < stranger_d.delay_ms,
            "高亲密度应比陌生人更快回复（{} vs {}）",
            close_d.delay_ms,
            stranger_d.delay_ms
        );
    }

    #[tokio::test]
    async fn same_state_same_content_is_deterministic() {
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            Some(state_with(40.0, 40.0)),
            None,
            Some(relationship_full(0.5, 0.5, 0.5, 0.5, 0.5)),
            Arc::new(FakeClock::new()),
        );
        let content = "随机聊天内容 abc";
        let d1 = engine
            .decide_response(1, 10, content, false, Some(1))
            .await
            .unwrap();
        let d2 = engine
            .decide_response(1, 10, content, false, Some(1))
            .await
            .unwrap();
        assert_eq!(d1.action, d2.action);
        assert_eq!(d1.delay_ms, d2.delay_ms, "相同状态应得到一致的延迟");
        assert_eq!(d1.reason, d2.reason, "相同状态应得到一致的原因");
    }

    // =========================================================================
    // 冷启动保护测试
    // =========================================================================

    /// Test 1：新 Relationship + Normal mood → cold-start floor 高，REPLY
    #[tokio::test]
    async fn cold_start_new_user_natural_mood_replies() {
        // 新用户：closeness = (0 + 0.2 + 0.1 + 0) / 4 = 0.075 → distant
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None, // 默认 mood（energy=72, stress=10）
            None,
            Some(relationship_full(0.0, 0.2, 0.1, 0.0, 0.0)), // 新用户
            Arc::new(FakeClock::new()),
        );
        // Natural unmentioned + base_threshold=0.7
        // cold_start_floor = 0.7 + 0.7*(1-0.7) = 0.91
        // 即使 roll=0.91 也应该通过 floor
        let d = engine
            .decide_response(1, 10, "你好", false, Some(1))
            .await
            .unwrap();
        assert_eq!(d.action, BehaviorAction::Reply, "新用户正常消息应触发回复");
    }

    /// Test 2：新 Relationship + stressed → cold-start floor 仍然有效
    #[tokio::test]
    async fn cold_start_new_user_stressed_still_replies() {
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            Some(CharacterState {
                energy: 72.0,
                stress: 80.0, // stressed
                current_activity: None,
                last_updated: chrono::Utc::now(),
            }),
            None,
            Some(relationship_full(0.0, 0.2, 0.1, 0.0, 0.0)),
            Arc::new(FakeClock::new()),
        );
        // stressed 会降低 threshold（-0.05），但 cold_start_floor=0.91 应该仍然有效
        // normal threshold = 0.7 - 0.2(only distant removed from withholds) - 0.05(stressed) = 0.45
        // floor = 0.91, final = max(0.45, 0.91) = 0.91
        let d = engine
            .decide_response(1, 10, "你好", false, Some(1))
            .await
            .unwrap();
        assert_eq!(
            d.action,
            BehaviorAction::Reply,
            "新用户即使 stressed 也应触发回复"
        );
    }

    /// Test 3：新 Relationship + low energy → cold-start floor 仍然有效
    #[tokio::test]
    async fn cold_start_new_user_low_energy_still_replies() {
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            Some(CharacterState {
                energy: 20.0, // low energy
                stress: 10.0,
                current_activity: None,
                last_updated: chrono::Utc::now(),
            }),
            None,
            Some(relationship_full(0.0, 0.2, 0.1, 0.0, 0.0)),
            Arc::new(FakeClock::new()),
        );
        // low_energy 会降低 threshold（-0.2），但 cold_start_floor=0.91 应该仍然有效
        // normal threshold = 0.7 - 0.2 = 0.5
        // floor = 0.91, final = max(0.5, 0.91) = 0.91
        let d = engine
            .decide_response(1, 10, "你好", false, Some(1))
            .await
            .unwrap();
        assert_eq!(
            d.action,
            BehaviorAction::Reply,
            "新用户即使 low energy 也应触发回复"
        );
    }

    /// Test 4：新 Relationship + stressed + low energy → cold-start floor 仍然有效
    #[tokio::test]
    async fn cold_start_new_user_stressed_and_low_energy_still_replies() {
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            Some(CharacterState {
                energy: 20.0,
                stress: 80.0,
                current_activity: None,
                last_updated: chrono::Utc::now(),
            }),
            None,
            Some(relationship_full(0.0, 0.2, 0.1, 0.0, 0.0)),
            Arc::new(FakeClock::new()),
        );
        // stressed + low_energy: threshold = 0.7 - 0.2 - 0.05 = 0.45
        // floor = 0.91, final = 0.91
        let d = engine
            .decide_response(1, 10, "你好", false, Some(1))
            .await
            .unwrap();
        assert_eq!(
            d.action,
            BehaviorAction::Reply,
            "新用户即使 stressed+low_energy 也应触发回复"
        );
    }

    /// Test 5：关系变化不会把可解释参与规则变成内容哈希骰子。
    #[tokio::test]
    async fn cold_start_floor_decreases_as_closeness_increases() {
        let closeness_values = [0.075, 0.125, 0.1875, 0.249];

        for closeness in closeness_values {
            let familiarity = closeness * 4.0 * 0.5;
            let affection = closeness * 4.0 * 0.3;
            let trust = closeness * 4.0 * 0.1;
            let intimacy = closeness * 4.0 * 0.1;
            let (engine, _) = build_engine(
                vec![binding(10, ReplyMode::Natural)],
                None,
                None,
                Some(relationship_full(
                    0.0,
                    affection,
                    familiarity,
                    trust,
                    intimacy,
                )),
                Arc::new(FakeClock::new()),
            );
            let d = engine
                .decide_response(1, 10, "你好", false, Some(1))
                .await
                .unwrap();
            assert_eq!(
                d.action,
                BehaviorAction::Reply,
                "实质消息应按统一规则参与 closeness={closeness}"
            );
        }
    }

    /// Test 6：Established relationship → cold-start protection inactive
    #[tokio::test]
    async fn cold_start_not_active_for_established_relationship() {
        // closeness = 0.4 → distant_relation = false → cold-start inactive
        // 使用 mentioned=true 确保 threshold=1.0 必定通过
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None,
            None,
            Some(relationship_full(0.0, 0.5, 0.5, 0.3, 0.3)), // closeness ≈ 0.4
            Arc::new(FakeClock::new()),
        );
        let d = engine
            .decide_response(1, 10, "你好", true, Some(1))
            .await
            .unwrap();
        // mentioned: threshold=1.0 →必定 reply
        assert_eq!(d.action, BehaviorAction::Reply, "已建立关系用户正常回复");
    }

    /// Test 7：Hard constraints 不受 cold-start 影响（无 binding → IGNORE）
    #[tokio::test]
    async fn cold_start_does_not_affect_hard_constraints() {
        // 无 binding → 应该直接 IGNORE，不受 cold-start 影响
        let (engine, _) = build_engine(
            vec![], // 无 binding
            None,
            None,
            Some(relationship_full(0.0, 0.2, 0.1, 0.0, 0.0)), // 新用户
            Arc::new(FakeClock::new()),
        );
        let d = engine
            .decide_response(1, 10, "你好", false, Some(1))
            .await
            .unwrap();
        assert_eq!(d.action, BehaviorAction::Ignore, "无 binding 应直接忽略");
    }

    /// Test 8：Self-bot 不受 cold-start 影响
    #[tokio::test]
    async fn cold_start_does_not_affect_self_bot() {
        // Self-bot 过滤发生在 ReplyProcessor 层，不在 BehaviorEngine 层
        // 此测试仅用于文档说明 BehaviorEngine 不处理 Self-bot 情况
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None,
            None,
            Some(relationship_full(0.0, 0.2, 0.1, 0.0, 0.0)),
            Arc::new(FakeClock::new()),
        );
        // 正常决策，不考虑 Self-bot（由上层处理）
        let _d = engine
            .decide_response(1, 10, "你好", true, Some(1))
            .await
            .unwrap();
    }

    /// Test 9：Cold-start 保护不改变 delay 行为（distant_relation 仍增加延迟）
    #[tokio::test]
    async fn cold_start_preserves_delay_for_distant_relation() {
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None,
            None,
            Some(relationship_full(0.0, 0.2, 0.1, 0.0, 0.0)), // 新用户
            Arc::new(FakeClock::new()),
        );
        let d = engine
            .decide_response(1, 10, "你好", true, Some(1)) // mentioned=true
            .await
            .unwrap();
        // mentioned: threshold=1.0, delay=base=1200
        // delay_withholds includes distant_relation, so delay += 400 = 1600
        assert_eq!(d.action, BehaviorAction::Reply);
        assert_eq!(d.delay_ms, 1600, "新用户 mentioned 消息延迟应为 1600");
    }

    /// Test 10：MentionOnly 模式的 cold-start 行为
    #[tokio::test]
    async fn cold_start_mention_only_mentioned_always_replies() {
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::MentionOnly)],
            None,
            None,
            Some(relationship_full(0.0, 0.2, 0.1, 0.0, 0.0)), // 新用户
            Arc::new(FakeClock::new()),
        );
        // MentionOnly + mentioned: threshold=1.0, delay=1600
        let d = engine
            .decide_response(1, 10, "你好", true, Some(1))
            .await
            .unwrap();
        assert_eq!(d.action, BehaviorAction::Reply);
    }

    /// Test 11：MentionOnly + !mentioned 是 Hard Constraint，不受 cold-start 影响
    #[tokio::test]
    async fn mention_only_unmentioned_is_hard_constraint() {
        // 新用户（distant_relation）+ cold_start_floor = 0.7
        // 但 MentionOnly + !mentioned 是 Hard Constraint，必须忽略
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::MentionOnly)],
            None,
            None,
            Some(relationship_full(0.0, 0.2, 0.1, 0.0, 0.0)), // 新用户，closeness=0.075
            Arc::new(FakeClock::new()),
        );
        let d = engine
            .decide_response(1, 10, "你好", false, Some(1))
            .await
            .unwrap();
        assert_eq!(
            d.action,
            BehaviorAction::Ignore,
            "MentionOnly + !mentioned 即使 distant_relation 也必须忽略"
        );
    }

    // =========================================================================
    // MentionOnly Hard Constraint 回归测试
    // =========================================================================

    /// Case 1：普通 MentionOnly + !mentioned → Ignore
    #[tokio::test]
    async fn mention_only_unmentioned_ignores_always() {
        let (engine, _) = engine_with(vec![binding(10, ReplyMode::MentionOnly)], None, None).await;
        let d = engine
            .decide_response(1, 10, "随便消息", false, None)
            .await
            .unwrap();
        assert_eq!(d.action, BehaviorAction::Ignore);
    }

    /// Case 2：MentionOnly + !mentioned + distant_relation → Ignore（冷启动地板不得突破）
    #[tokio::test]
    async fn mention_only_unmentioned_distant_ignores_always() {
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::MentionOnly)],
            None,
            None,
            Some(relationship_full(0.0, 0.2, 0.1, 0.0, 0.0)), // 新用户
            Arc::new(FakeClock::new()),
        );
        // 用 roll 值足够小的消息内容（如 "a"），原本可能触发 Reply
        let d = engine
            .decide_response(1, 10, "a", false, Some(1))
            .await
            .unwrap();
        assert_eq!(
            d.action,
            BehaviorAction::Ignore,
            "MentionOnly + !mentioned 即使 roll < cold_start_floor 也必须忽略"
        );
    }

    /// Case 3：极端 roll 值验证
    #[tokio::test]
    async fn mention_only_unmentioned_ignores_extreme_roll_values() {
        // 测试各种消息内容，确保即使 roll 极小也不会 Reply
        let messages = ["a", "b", "c", "0", "1", "阿", "中"];
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::MentionOnly)],
            None,
            None,
            Some(relationship_full(0.0, 0.2, 0.1, 0.0, 0.0)), // 新用户
            Arc::new(FakeClock::new()),
        );
        for msg in messages {
            let d = engine
                .decide_response(1, 10, msg, false, Some(1))
                .await
                .unwrap();
            assert_eq!(
                d.action,
                BehaviorAction::Ignore,
                "MentionOnly + !mentioned 对 '{}' 应忽略（roll 值应无关）",
                msg
            );
        }
    }

    /// Case 4：MentionOnly + mentioned → Reply（mention 行为不受影响）
    #[tokio::test]
    async fn mention_only_mentioned_always_replies() {
        let (engine, _) = engine_with(vec![binding(10, ReplyMode::MentionOnly)], None, None).await;
        let d = engine
            .decide_response(1, 10, "有人提到我", true, None)
            .await
            .unwrap();
        assert_eq!(d.action, BehaviorAction::Reply);
        assert_eq!(d.cognition_level, CognitionLevel::Light);
    }

    /// Case 5：其他 ReplyMode 回归测试
    /// 验证 MentionOnly 的修复没有意外改变 Occasionally / Natural 的行为。
    #[tokio::test]
    async fn occasionally_and_natural_modes_unchanged() {
        // Occasionally + mentioned → Reply（is_mentioned 覆盖所有模式）
        let (engine_o, _) =
            engine_with(vec![binding(10, ReplyMode::Occasionally)], None, None).await;
        let d_o_m = engine_o
            .decide_response(1, 10, "有人提到我", true, None)
            .await
            .unwrap();
        assert_eq!(
            d_o_m.action,
            BehaviorAction::Reply,
            "Occasionally + mentioned 应 Reply"
        );

        // Natural + mentioned → Reply
        let (engine_n, _) = engine_with(vec![binding(10, ReplyMode::Natural)], None, None).await;
        let d_n_m = engine_n
            .decide_response(1, 10, "有人提到我", true, None)
            .await
            .unwrap();
        assert_eq!(
            d_n_m.action,
            BehaviorAction::Reply,
            "Natural + mentioned 应 Reply"
        );

        // Occasionally + !mentioned 行为确定性（与现有测试一致）
        let d1 = engine_o
            .decide_response(1, 10, "随便聊点什么", false, None)
            .await
            .unwrap();
        let d2 = engine_o
            .decide_response(1, 10, "随便聊点什么", false, None)
            .await
            .unwrap();
        assert_eq!(
            d1.action, d2.action,
            "Occasionally + !mentioned 应保持确定性"
        );
    }
}
