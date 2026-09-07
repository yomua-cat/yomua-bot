//! 规则行为引擎 —— 确定性决策，无 LLM。
//!
//! 依据绑定 / 关系 / 状态（全局 + 会话级）调制回复意愿与延迟。
//! 状态领域模型重构后：情绪为标量 `Mood`（与 Stress 独立），
//! 压力同时存在于 `CharacterState`（全局）与 `ConversationState`（会话级）。

use std::sync::Arc;

use crate::application::clock::Clock;
use crate::domain::behavior::{BehaviorAction, BehaviorDecision, BehaviorEngine};
use crate::domain::character::{CharacterBinding, ReplyMode};
use crate::domain::mute::{is_within_window, parse_mute_schedule, TimeOfDay};
use crate::domain::relationship::Relationship;
use crate::domain::repository::{
    CharacterBindingRepository, CharacterStateRepository, ConversationStateRepository,
    MoodRepository, RelationshipRepository,
};
use crate::error::DomainError;

use crate::domain::behavior::{CognitionLevel, Priority};
use crate::domain::character::{BehaviorState, CharacterState, ConversationState};
use crate::domain::emotion::Mood;

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
}

/// 加载到的、用于决策的上下文。
struct DecisionContext {
    binding: Option<CharacterBinding>,
    #[allow(dead_code)]
    mood: Option<Mood>,
    conversation_state: Option<ConversationState>,
    #[allow(dead_code)]
    behavior_state: Option<BehaviorState>,
    relationship: Option<Relationship>,
    state: Option<CharacterState>,
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
        }
    }

    /// 加载决策所需的上下文（绑定 / 情绪 / 关系 / 全局状态 / 会话状态 / 行为状态）。
    async fn load_context(
        &self,
        character_id: i64,
        conversation_id: i64,
        participant_id: Option<i64>,
    ) -> Result<DecisionContext, DomainError> {
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

        Ok(DecisionContext {
            binding,
            mood,
            relationship,
            state,
            conversation_state,
            behavior_state,
        })
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
        let ctx = self
            .load_context(character_id, conversation_id, participant_id)
            .await?;

        let decided_at = self.clock.now();

        // 无绑定 → 该角色不在本会话中，忽略。
        let Some(binding) = ctx.binding else {
            return Ok(ignore_decision(
                "该会话没有为本角色配置绑定，忽略",
                decided_at,
            ));
        };

        // 由内容哈希派生一个确定的 [0, 1) 值，模拟拟人化的随机感。
        let roll = deterministic_roll(message_content);

        // 依据绑定计算一个基础回复阈值；mentioned 时几乎总是回复。
        let (base_threshold, base_delay) = base_params(&binding.reply_mode, is_mentioned);

        // 若 mentioned，直接回复（阈值 1.0 恒成立）。
        let mut threshold = if is_mentioned { 1.0 } else { base_threshold };
        let mut delay_ms = base_delay;

        // ---- 关系 / 状态调制（区别对待 + 状态驱动）----

        // 关系深浅：综合熟悉度 / 好感 / 信任 / 亲密得出一个亲密度。
        let closeness = ctx
            .relationship
            .as_ref()
            .map(|r| (r.familiarity + r.affection + r.trust + r.intimacy) / 4.0);
        let close_relation = closeness.map(|c| c > 0.6).unwrap_or(false);
        let distant_relation = closeness.map(|c| c < 0.25).unwrap_or(false);

        let annoyed = ctx
            .relationship
            .as_ref()
            .map(|r| r.annoyance > 0.6)
            .unwrap_or(false);
        let high_affection = ctx
            .relationship
            .as_ref()
            .map(|r| r.affection > 0.7)
            .unwrap_or(false);

        // 压力来自全局状态与会话级状态二者中的较高者（均 0-100 尺度）。
        // 未持久化的状态行按领域默认值处理（全局 energy 72 / stress 10，
        // 会话级 energy 50 / stress 10），避免「从未落库」被误判为 0 值。
        let global_stress = ctx
            .state
            .as_ref()
            .map(|s| s.stress)
            .unwrap_or(crate::domain::character::CharacterState::default().stress);
        let scoped_stress = ctx
            .conversation_state
            .as_ref()
            .map(|s| s.stress)
            .unwrap_or(crate::domain::character::ConversationState::default().stress);
        let stress_level = global_stress.max(scoped_stress);
        let stressed = stress_level > 60.0;

        // 状态驱动：精力（全局）与会话级精力中的较低者决定参与意愿。
        let global_energy = ctx
            .state
            .as_ref()
            .map(|s| s.energy)
            .unwrap_or(crate::domain::character::CharacterState::default().energy);
        let scoped_energy = ctx
            .conversation_state
            .as_ref()
            .map(|s| s.energy)
            .unwrap_or(crate::domain::character::ConversationState::default().energy);
        let energy_level = global_energy.min(scoped_energy);
        let low_energy = energy_level < 30.0;

        // 静默时段：仅在命中且未被 @ 时压低未提及消息的回复意愿并拉长延迟。
        let in_mute = binding
            .mute_schedule
            .as_deref()
            .and_then(|s| parse_mute_schedule(s).ok().flatten())
            .map(|w| is_within_window(&w, &time_of_day(decided_at)))
            .unwrap_or(false);

        // 负向因素集合：厌烦、精力低、压力高或关系疏离。
        // 这些会降低参与意愿（未提及消息）并拉长延迟；被 @ 的实时消息始终回复，
        // 只受延迟调制（直接呼叫不应被忽略，符合 mute / 拟人语义）。
        let withholds = annoyed || low_energy || stressed || distant_relation;

        // 阈值（回复概率）惩罚仅作用于未提及消息。
        if !is_mentioned && withholds {
            threshold -= 0.2;
        }
        if !is_mentioned && stressed {
            threshold -= 0.05;
        }

        // 延迟调制对被 @ 与未 @ 消息都生效。
        if withholds {
            delay_ms += 400;
        }
        if stressed {
            delay_ms += 300;
        }

        // 正向调制：高好感 / 高亲密度 → 更愿意参与、更及时。
        if high_affection {
            threshold += 0.15;
            delay_ms = delay_ms.saturating_sub(200);
        }
        if close_relation {
            threshold += 0.10;
            delay_ms = delay_ms.saturating_sub(300);
        }
        // 静默时段（未提及）大幅压低。
        if in_mute && !is_mentioned {
            threshold -= 0.35;
            delay_ms += 800;
        }

        let should_reply = roll < threshold.clamp(0.0, 1.0);

        if should_reply {
            // 需要 LLM 生成，属于轻量认知。
            Ok(BehaviorDecision {
                action: BehaviorAction::Reply,
                priority: Priority::Realtime,
                cognition_level: CognitionLevel::Light,
                delay_ms,
                reason: build_reply_reason(&ReplyReasonFlags {
                    mentioned: is_mentioned,
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

    async fn decide_proactive(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<BehaviorDecision, DomainError> {
        let ctx = self
            .load_context(character_id, conversation_id, None)
            .await?;
        let decided_at = self.clock.now();

        // 无绑定 → 该角色不在本会话中，不主动。
        let Some(binding) = ctx.binding else {
            return Ok(ignore_decision(
                "该会话没有为本角色配置绑定，不主动",
                decided_at,
            ));
        };

        // 未启用主动行为。
        if !binding.proactive_enabled {
            return Ok(ignore_decision("未启用主动行为", decided_at));
        }

        // 静默时段覆盖主动行为。
        let in_mute = binding
            .mute_schedule
            .as_deref()
            .and_then(|s| parse_mute_schedule(s).ok().flatten())
            .map(|w| is_within_window(&w, &time_of_day(decided_at)))
            .unwrap_or(false);
        if in_mute {
            return Ok(ignore_decision("处于静默时段，主动行为被覆盖", decided_at));
        }

        // 主动意愿阈值：基准由关系亲密度与角色状态调制。
        let mut threshold = PROACTIVE_BASE_THRESHOLD;

        // 关系：取该角色所有关系中最高亲密度作为"最佳关系"。
        let relationships = self
            .relationship_repo
            .find_by_character_id(character_id)
            .await
            .map_err(repo_err)?;
        let best_closeness = relationships
            .iter()
            .map(|r| (r.familiarity + r.affection + r.trust + r.intimacy) / 4.0)
            .fold(0.0_f64, f64::max);
        if best_closeness > 0.6 {
            threshold += 0.2;
        } else if relationships.is_empty() || best_closeness < 0.25 {
            threshold -= 0.15;
        }

        // 状态调制（全局 + 会话级取较不利者）：
        // 状态差 → 不愿主动；状态好 → 更想主动。
        let global_energy = ctx.state.as_ref().map(|s| s.energy).unwrap_or_default();
        let global_stress = ctx.state.as_ref().map(|s| s.stress).unwrap_or_default();
        let scoped_energy = ctx
            .conversation_state
            .as_ref()
            .map(|s| s.energy)
            .unwrap_or_default();
        let scoped_stress = ctx
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

        // 确定性哈希：角色 + 会话 + 小时桶，让主动意愿随小时自然变化且可复现。
        let hour_bucket = decided_at.format("%Y-%m-%d-%H");
        let roll = deterministic_roll(&format!("{character_id}|{conversation_id}|{hour_bucket}"));

        let should_initiate = roll < threshold.clamp(0.0, 1.0);
        if should_initiate {
            // MVP：主动行为仅更新内部状态，不发消息、不调用 LLM。
            Ok(BehaviorDecision {
                action: BehaviorAction::UpdateState,
                priority: Priority::Background,
                cognition_level: CognitionLevel::None,
                delay_ms: 0,
                reason: "主动行为检查通过（MVP：仅更新内部状态）".to_string(),
                decided_at,
            })
        } else {
            Ok(ignore_decision("主动意愿未达阈值，等待下一轮", decided_at))
        }
    }
}

/// 把仓储错误转换为领域错误。
fn repo_err(e: crate::error::RepositoryError) -> DomainError {
    DomainError::Internal(format!("仓储错误: {e}"))
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

/// 用一个简单的确定性哈希把消息内容映射到 [0, 1)。
fn deterministic_roll(content: &str) -> f64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in content.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash & 0xFFFF_FFFF) as f64 / 4_294_967_296.0
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

/// 从 UTC 时间提取一天中的时刻（本地时区语义在此阶段不引入，
/// 静默判断基于 UTC 小时/分钟）。
pub(crate) fn time_of_day(t: chrono::DateTime<chrono::Utc>) -> TimeOfDay {
    use chrono::Timelike;
    TimeOfDay {
        hour: t.hour(),
        minute: t.minute(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::character::{
        BehaviorState, CharacterBinding, CharacterState, ConversationState,
    };
    use crate::domain::emotion::Mood;
    use crate::domain::relationship::Relationship;
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
                .find(|r| r.character_id == character_id && r.participant_id == participant_id)
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
                .find(|x| x.character_id == r.character_id && x.participant_id == r.participant_id)
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
            participant_id: 1,
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

    /// 用状态构造引擎；energy 兼容旧封装，其余字段使用默认值。
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
}
