//! 确定性行为引擎 —— `BehaviorEngine` trait 的具体实现。
//!
//! 基于规则（reply_mode × is_mentioned × 情绪/关系/状态调制）做出
//! 确定性决策，不依赖 LLM。拟人化的"随机感"由消息内容的确定性哈希
//! 提供，保证同一条消息在相同状态下得到相同结论（可测试、可复现）。

use std::sync::Arc;

use crate::application::clock::Clock;
use crate::domain::behavior::{
    BehaviorAction, BehaviorDecision, BehaviorEngine, CognitionLevel, Priority,
};
use crate::domain::character::{CharacterBinding, CharacterState, ReplyMode};
use crate::domain::emotion::EmotionState;
use crate::domain::mute::{is_within_window, parse_mute_schedule, TimeOfDay};
use crate::domain::relationship::Relationship;
use crate::domain::repository::{
    CharacterBindingRepository, CharacterStateRepository, EmotionStateRepository,
    RelationshipRepository,
};
use crate::error::{DomainError, RepositoryError};

/// 将仓储错误映射为领域错误（内部错误）。
fn repo_err(e: RepositoryError) -> DomainError {
    DomainError::Internal(format!("读取决策上下文失败: {e}"))
}

/// 主动行为的基准意愿阈值。
///
/// 主动触发 = 确定性哈希的 roll < 阈值。阈值由关系亲密度与状态调制，
/// 表示"角色此时有多想主动发起接触"。
const PROACTIVE_BASE_THRESHOLD: f64 = 0.5;

/// `BehaviorEngine` 的规则实现。
pub struct RuleBehaviorEngine {
    binding_repo: Arc<dyn CharacterBindingRepository>,
    emotion_repo: Arc<dyn EmotionStateRepository>,
    relationship_repo: Arc<dyn RelationshipRepository>,
    state_repo: Arc<dyn CharacterStateRepository>,
    clock: Arc<dyn Clock>,
}

/// 加载到的、用于决策的上下文。
struct DecisionContext {
    binding: Option<CharacterBinding>,
    emotion: Option<EmotionState>,
    relationship: Option<Relationship>,
    state: Option<CharacterState>,
}

impl RuleBehaviorEngine {
    /// 创建一个规则行为引擎。
    pub fn new(
        binding_repo: Arc<dyn CharacterBindingRepository>,
        emotion_repo: Arc<dyn EmotionStateRepository>,
        relationship_repo: Arc<dyn RelationshipRepository>,
        state_repo: Arc<dyn CharacterStateRepository>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            binding_repo,
            emotion_repo,
            relationship_repo,
            state_repo,
            clock,
        }
    }

    /// 加载决策所需的上下文（绑定 / 情绪 / 关系 / 状态）。
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

        let emotion = self
            .emotion_repo
            .find_by_character_id(character_id)
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

        Ok(DecisionContext {
            binding,
            emotion,
            relationship,
            state,
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

        // ---- 情绪 / 关系 / 状态调制（区别对待 + 状态驱动）----

        // 关系深浅：综合熟悉度 / 好感 / 信任 / 亲密得出一个亲密度。
        // 高亲密度 → 更愿意参与、回复更及时；陌生 / 低好感 → 更疏离。
        // 亲密度综合熟悉度 / 好感 / 信任 / 亲密；仅在确有这段关系时参与区别对待，
        // 避免"无关参与者"被误判为疏离。
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

        // 情绪层面的压力（0-1 尺度）。
        let stressed_emotion = ctx
            .emotion
            .as_ref()
            .map(|e| e.stress > 0.6)
            .unwrap_or(false);

        // 状态驱动：角色的精力 / 注意力 / 压力（均 0-100 尺度）。
        let state = ctx.state.as_ref();
        let low_energy = state.map(|s| s.energy < 30.0).unwrap_or(false);
        let low_attention = state.map(|s| s.attention < 40.0).unwrap_or(false);
        let stressed_state = state.map(|s| s.stress > 60.0).unwrap_or(false);

        // 静默时段：仅在命中且未被 @ 时压低未提及消息的回复意愿并拉长延迟；
        // 被 @ 的实时消息不受影响。
        let in_mute = binding
            .mute_schedule
            .as_deref()
            .and_then(|s| parse_mute_schedule(s).ok().flatten())
            .map(|w| is_within_window(&w, &time_of_day(decided_at)))
            .unwrap_or(false);

        // 负向因素集合：厌烦、精力低、注意力涣散、压力高或关系疏离。
        // 这些会降低参与意愿（未提及消息）并拉长延迟；被 @ 的实时消息始终回复，
        // 只受延迟调制（直接呼叫不应被忽略，符合 mute / 拟人语义）。
        let withholds = annoyed
            || low_energy
            || low_attention
            || stressed_state
            || stressed_emotion
            || distant_relation;

        // 阈值（回复概率）惩罚仅作用于未提及消息。
        if !is_mentioned && withholds {
            threshold -= 0.2;
        }
        if !is_mentioned && low_attention {
            threshold -= 0.05;
        }
        if !is_mentioned && stressed_state {
            threshold -= 0.05;
        }

        // 延迟调制对被 @ 与未 @ 消息都生效。
        if withholds {
            delay_ms += 400;
        }
        if low_attention {
            delay_ms += 300;
        }
        if stressed_state {
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
                    low_attention,
                    stressed: stressed_state || stressed_emotion,
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

        // 完整状态调制：状态差 → 不愿主动；状态好 → 更想主动。
        if let Some(s) = ctx.state.as_ref() {
            if s.energy < 30.0 {
                threshold -= 0.1;
            }
            if s.attention < 40.0 {
                threshold -= 0.05;
            }
            if s.stress > 60.0 {
                threshold -= 0.1;
            }
            if s.energy > 70.0 && s.attention > 60.0 && s.stress < 40.0 {
                threshold += 0.1;
            }
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
///
/// 采用 FNV-1a 风格哈希，无外部依赖，保证同输入同输出。
fn deterministic_roll(content: &str) -> f64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in content.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    // 取低 32 位映射到 [0, 1)。
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
    low_attention: bool,
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
    if f.low_attention {
        parts.push("注意力涣散".to_string());
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
    use crate::application::clock::Clock;
    use crate::domain::character::{CharacterBinding, CharacterState};
    use crate::domain::emotion::EmotionState;
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

        // 后阶段（mute / proactive 冷却）测试会推进时钟，因此保留该辅助方法。
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
        async fn insert(&self, b: &CharacterBinding) -> Result<i64, RepositoryError> {
            self.bindings.lock().unwrap().push(b.clone());
            Ok(b.id)
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
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
            character_id: i64,
        ) -> Result<Option<EmotionState>, RepositoryError> {
            Ok(self.states.lock().unwrap().get(&character_id).cloned())
        }
        async fn upsert(
            &self,
            character_id: i64,
            state: &EmotionState,
        ) -> Result<(), RepositoryError> {
            self.states
                .lock()
                .unwrap()
                .insert(character_id, state.clone());
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
            created_at: chrono::Utc::now(),
        }
    }

    /// 构造一个启用了主动行为的绑定（可带静默时段）。
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

    fn build_repos(
        bindings: Vec<CharacterBinding>,
        emotion: Option<EmotionState>,
        rel: Option<Relationship>,
        state: Option<CharacterState>,
    ) -> (
        Arc<MemBindingRepo>,
        Arc<MemEmotionRepo>,
        Arc<MemRelationshipRepo>,
        Arc<MemStateRepo>,
    ) {
        let binding_repo = Arc::new(MemBindingRepo {
            bindings: Mutex::new(bindings),
        });
        let emotion_repo = Arc::new(MemEmotionRepo {
            states: Mutex::new(emotion.into_iter().map(|s| (1, s)).collect()),
        });
        let rel_repo = Arc::new(MemRelationshipRepo {
            relationships: Mutex::new(rel.into_iter().collect()),
        });
        let state_repo = Arc::new(MemStateRepo {
            states: Mutex::new(state.into_iter().map(|s| (1, s)).collect()),
        });
        (binding_repo, emotion_repo, rel_repo, state_repo)
    }

    fn build_engine(
        bindings: Vec<CharacterBinding>,
        emotion: Option<EmotionState>,
        rel: Option<Relationship>,
        state: Option<CharacterState>,
        clock: Arc<dyn Clock>,
    ) -> (RuleBehaviorEngine, Arc<MemRelationshipRepo>) {
        let (binding_repo, emotion_repo, rel_repo, state_repo) =
            build_repos(bindings, emotion, rel, state);

        let engine = RuleBehaviorEngine::new(
            binding_repo,
            emotion_repo,
            rel_repo.clone(),
            state_repo,
            clock,
        );
        (engine, rel_repo)
    }

    /// 用状态构造引擎；energy 兼容旧封装，其余字段使用默认值。
    fn build_engine_from_energy(
        bindings: Vec<CharacterBinding>,
        emotion: Option<EmotionState>,
        rel: Option<Relationship>,
        energy: Option<f64>,
        clock: Arc<dyn Clock>,
    ) -> (RuleBehaviorEngine, Arc<MemRelationshipRepo>) {
        let state = energy.map(|e| CharacterState {
            energy: e,
            ..Default::default()
        });
        build_engine(bindings, emotion, rel, state, clock)
    }

    async fn engine_with(
        bindings: Vec<CharacterBinding>,
        emotion: Option<EmotionState>,
        rel: Option<Relationship>,
        energy: Option<f64>,
    ) -> (RuleBehaviorEngine, Arc<MemRelationshipRepo>) {
        build_engine_from_energy(bindings, emotion, rel, energy, Arc::new(FakeClock::new()))
    }

    #[tokio::test]
    async fn no_binding_ignores() {
        let (engine, _) = engine_with(vec![], None, None, None).await;
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
                engine_with(vec![binding(10, ReplyMode::MentionOnly)], None, None, None).await;
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
        let (engine, _) =
            engine_with(vec![binding(10, ReplyMode::Occasionally)], None, None, None).await;
        let d = engine
            .decide_response(1, 10, "你好呀", true, None)
            .await
            .unwrap();
        assert_eq!(d.action, BehaviorAction::Reply);
    }

    #[tokio::test]
    async fn occasional_unmentioned_deterministic() {
        // 未提及 + occasionally：同一条消息两次应得到相同决策（确定性）。
        let (engine, _) =
            engine_with(vec![binding(10, ReplyMode::Occasionally)], None, None, None).await;
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
        // 高度厌烦 → 阈值降低 + 延迟增加。
        let (engine, _) = engine_with(
            vec![binding(10, ReplyMode::Natural)],
            None,
            Some(relationship(0.9, 0.2)),
            Some(90.0),
        )
        .await;
        // 参与者的关系（participant_id=1）用于调制。
        let d = engine
            .decide_response(1, 10, "喂", true, Some(1))
            .await
            .unwrap();
        // mentioned → 恒回复，但高厌烦会加延迟。
        assert_eq!(d.action, BehaviorAction::Reply);
        assert!(d.delay_ms >= 1400, "高厌烦应增加延迟，实际 {}", d.delay_ms);
    }

    #[tokio::test]
    async fn high_affection_low_annoyance_reduces_delay() {
        let (engine, _) = engine_with(
            vec![binding(10, ReplyMode::Natural)],
            None,
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
        let (engine, _) =
            engine_with(vec![binding(10, ReplyMode::Natural)], None, None, None).await;
        let d = engine.decide_proactive(1, 10).await.unwrap();
        assert_eq!(d.action, BehaviorAction::Ignore);
        assert!(d.reason.contains("未启用"));
    }

    #[tokio::test]
    async fn proactive_same_input_is_deterministic() {
        // 相同角色 / 会话 / 小时桶 → 两次决策一致。
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
        // 静默时段覆盖主动行为 → Ignore。
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

    /// 构造一个固定在指定 UTC 时刻的假时钟。
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
        // Natural + 静默 10:00-12:00。内容 "你好今天天气不错" roll≈0.485：
        // 时段外阈值 0.7 → 回复；时段内阈值降到 0.35 → 忽略。
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
        // 静默时段内被 @ 的实时消息不受影响，仍回复。
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
        // 同一静默时刻、同一内容两次决策一致。
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

    /// 构造一个仅覆盖部分数值字段的角色状态。
    fn state_with(energy: f64, attention: f64, stress: f64) -> CharacterState {
        CharacterState {
            energy,
            attention,
            stress,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn low_attention_raises_delay_over_high_attention() {
        // 状态驱动：注意力涣散 → 延迟拉长（同一 @ 消息，注意力高时延迟更短）。
        let content = "在吗";
        let (low, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None,
            None,
            Some(state_with(90.0, 20.0, 10.0)),
            Arc::new(FakeClock::new()),
        );
        let low_d = low
            .decide_response(1, 10, content, true, Some(1))
            .await
            .unwrap();

        let (high, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None,
            None,
            Some(state_with(90.0, 90.0, 10.0)),
            Arc::new(FakeClock::new()),
        );
        let high_d = high
            .decide_response(1, 10, content, true, Some(1))
            .await
            .unwrap();

        assert_eq!(low_d.action, BehaviorAction::Reply);
        assert_eq!(high_d.action, BehaviorAction::Reply);
        assert!(
            low_d.delay_ms > high_d.delay_ms,
            "低注意力应比高注意力延迟更长（{} vs {}）",
            low_d.delay_ms,
            high_d.delay_ms
        );
    }

    #[tokio::test]
    async fn high_state_stress_raises_delay() {
        // 状态驱动：高压力 → 延迟增加。
        let content = "在吗";
        let (stressed, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None,
            None,
            Some(state_with(90.0, 50.0, 90.0)),
            Arc::new(FakeClock::new()),
        );
        let stressed_d = stressed
            .decide_response(1, 10, content, true, Some(1))
            .await
            .unwrap();

        let (calm, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None,
            None,
            Some(state_with(90.0, 50.0, 10.0)),
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
        // 区别对待：高亲密度 → 更及时；陌生 / 低好感 → 更疏离（延迟更长）。
        let content = "在吗";
        // 亲密度高：familiarity/affection/trust/intimacy 都高 → closeness≈0.875。
        let (close, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None,
            Some(relationship_full(0.0, 0.8, 0.9, 0.9, 0.9)),
            None,
            Arc::new(FakeClock::new()),
        );
        let close_d = close
            .decide_response(1, 10, content, true, Some(1))
            .await
            .unwrap();

        // 陌生 / 低好感：closeness≈0.025。
        let (stranger, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None,
            Some(relationship_full(0.0, 0.1, 0.0, 0.0, 0.0)),
            None,
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
        // 相同状态、相同内容两次决策一致（确定性）。
        let (engine, _) = build_engine(
            vec![binding(10, ReplyMode::Natural)],
            None,
            Some(relationship_full(0.5, 0.5, 0.5, 0.5, 0.5)),
            Some(state_with(40.0, 40.0, 40.0)),
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
