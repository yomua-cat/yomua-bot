//! 主动行为驱动 —— 后台循环评估角色是否有主动意愿并落库。
//!
//! 每个 tick 枚举所有绑定上的角色，仅在主动开关、冷却和静默检查通过后询问
//! 决策编排器。规则判定有主动意愿后，决策模型可保留或取消；通过后才预留额度并入队。
//! 通过统一回复任务发送消息；状态只记录冷却和每日额度，内容仍由回复链生成。

use std::sync::Arc;
use std::time::Duration;

use crate::application::behavior_engine::time_of_day;
use crate::application::clock::Clock;
use crate::application::config::{parse_timezone_offset, RuntimeConfig};
use crate::application::decision_orchestrator::DecisionOrchestrator;
use crate::application::event_bus::EventBus;
use crate::domain::behavior::BehaviorAction;
use crate::domain::character::CharacterBinding;
use crate::domain::event::{BehaviorDecidedEvent, CoreEvent};
use crate::domain::mute::{is_within_window, parse_mute_schedule};
use crate::domain::repository::{
    BehaviorStateRepository, CharacterBindingRepository, CharacterStateRepository,
};
use crate::error::DomainError;

#[async_trait::async_trait]
pub trait ProactiveReplyScheduler: Send + Sync {
    async fn enqueue_proactive(
        &self,
        character_id: i64,
        conversation_id: i64,
        reason: &str,
    ) -> Result<(), crate::error::RuntimeError>;
}

/// 主动行为驱动的固定 TICK 间隔（60 秒）。
pub const PROACTIVE_TICK_INTERVAL: Duration = Duration::from_secs(60);

/// 主动行为的默认冷却时长（10 分钟）。
pub const PROACTIVE_COOLDOWN: Duration = Duration::from_secs(10 * 60);

/// 主动行为驱动。
///
/// 在后台以固定间隔运行；每次 `tick` 独立评估所有启用主动的绑定。
/// 冷却状态以 `BehaviorState`（Character × Conversation 范围）记录。
pub struct ProactiveDriver {
    binding_repo: Arc<dyn CharacterBindingRepository>,
    state_repo: Arc<dyn CharacterStateRepository>,
    behavior_state_repo: Arc<dyn BehaviorStateRepository>,
    decision_orchestrator: DecisionOrchestrator,
    event_bus: EventBus,
    clock: Arc<dyn Clock>,
    tick_interval: Duration,
    cooldown: Duration,
    reply_scheduler: Option<Arc<dyn ProactiveReplyScheduler>>,
    daily_limit: u32,
    live_settings: Option<tokio::sync::watch::Receiver<RuntimeConfig>>,
}

impl ProactiveDriver {
    /// 创建一个主动行为驱动。tick 间隔与冷却使用固定常量。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        binding_repo: Arc<dyn CharacterBindingRepository>,
        state_repo: Arc<dyn CharacterStateRepository>,
        behavior_state_repo: Arc<dyn BehaviorStateRepository>,
        decision_orchestrator: DecisionOrchestrator,
        event_bus: EventBus,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            binding_repo,
            state_repo,
            behavior_state_repo,
            decision_orchestrator,
            event_bus,
            clock,
            tick_interval: PROACTIVE_TICK_INTERVAL,
            cooldown: PROACTIVE_COOLDOWN,
            reply_scheduler: None,
            daily_limit: 10,
            live_settings: None,
        }
    }

    /// 注入持久回复任务调度器；未注入时主动评估不会产生消息。
    pub fn with_reply_scheduler(mut self, scheduler: Arc<dyn ProactiveReplyScheduler>) -> Self {
        self.reply_scheduler = Some(scheduler);
        self
    }

    /// 接入运行时配置，使后续主动评估读取最新时区设置。
    pub fn with_live_settings(
        mut self,
        settings: tokio::sync::watch::Receiver<RuntimeConfig>,
    ) -> Self {
        self.live_settings = Some(settings);
        self
    }

    fn timezone_offset(&self) -> chrono::FixedOffset {
        self.live_settings
            .as_ref()
            .and_then(|settings| parse_timezone_offset(&settings.borrow().timezone).ok())
            .unwrap_or_else(|| chrono::FixedOffset::east_opt(0).expect("UTC 偏移有效"))
    }

    /// 以固定间隔循环运行（常驻后台任务）。
    pub async fn run(self) {
        tracing::info!(target: "runtime", "主动行为驱动已启动");
        loop {
            self.tick().await;
            tokio::time::sleep(self.tick_interval).await;
        }
    }

    /// 接受统一关停信号的常驻循环。
    pub async fn run_until_stopped(self, mut stop: tokio::sync::watch::Receiver<bool>) {
        tracing::info!(target: "runtime", "主动行为驱动已启动");
        loop {
            if *stop.borrow() {
                break;
            }
            tokio::select! {
                result = stop.changed() => {
                    if result.is_err() || *stop.borrow() { break; }
                }
                _ = tokio::time::sleep(self.tick_interval) => {
                    self.tick().await;
                }
            }
        }
    }

    /// 执行一轮主动评估。
    pub async fn tick(&self) {
        let bindings = match self.binding_repo.find_all().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(target: "runtime", error = %e, "主动驱动枚举绑定失败");
                return;
            }
        };

        for binding in bindings {
            if let Err(e) = self.evaluate_binding(&binding).await {
                tracing::warn!(
                    target: "runtime",
                    character_id = binding.character_id,
                    conversation_id = binding.conversation_id,
                    error = %e,
                    "主动评估失败"
                );
            }
        }
    }

    /// 评估单个绑定：冷却 / 静默 / 决策 → （可选）落库与事件。
    async fn evaluate_binding(&self, binding: &CharacterBinding) -> Result<(), DomainError> {
        // 未启用主动的绑定直接跳过。
        if !binding.proactive_enabled {
            return Ok(());
        }
        let now = self.clock.now();
        let timezone = self.timezone_offset();

        // 静默时段跳过主动。
        let in_mute = binding
            .mute_schedule
            .as_deref()
            .and_then(|s| parse_mute_schedule(s).ok().flatten())
            .map(|w| is_within_window(&w, &time_of_day(now, timezone)))
            .unwrap_or(false);
        if in_mute {
            return Ok(());
        }

        // 冷却判断：距上次主动不足冷却时长则跳过（按 Character × Conversation 范围）。
        let behavior_state = self
            .behavior_state_repo
            .find_by_character_and_conversation(binding.character_id, binding.conversation_id)
            .await
            .map_err(|e| DomainError::Internal(format!("读取行为状态失败: {e}")))?;
        if let Some(state) = &behavior_state {
            if let Some(last) = state.last_proactive_at {
                let cooldown = chrono::Duration::from_std(self.cooldown)
                    .map_err(|e| DomainError::Internal(format!("冷却时长无效: {e}")))?;
                if now.signed_duration_since(last) < cooldown {
                    return Ok(());
                }
            }
        }

        // 行为引擎决策。
        let decision = self
            .decision_orchestrator
            .evaluate_proactive(binding.character_id, binding.conversation_id)
            .await?;

        if decision.action != BehaviorAction::InitiateProactive {
            return Ok(());
        }
        if let Some(scheduler) = &self.reply_scheduler {
            let cooldown = chrono::Duration::from_std(self.cooldown)
                .map_err(|e| DomainError::Internal(format!("冷却时长无效: {e}")))?;
            let reserved = self
                .behavior_state_repo
                .try_reserve_proactive_at_date(
                    binding.character_id,
                    binding.conversation_id,
                    now,
                    now.with_timezone(&timezone).date_naive(),
                    cooldown,
                    self.daily_limit,
                )
                .await
                .map_err(|e| DomainError::Internal(format!("主动额度预留失败: {e}")))?;
            if !reserved {
                return Ok(());
            }
            scheduler
                .enqueue_proactive(
                    binding.character_id,
                    binding.conversation_id,
                    &decision.reason,
                )
                .await
                .map_err(|e| DomainError::Internal(e.to_string()))?;
        } else {
            return Ok(());
        }

        // 同时维护全局角色状态（刷新最后更新时间）。
        let mut new_state = self
            .state_repo
            .find_by_character_id(binding.character_id)
            .await
            .map_err(|e| DomainError::Internal(format!("读取角色状态失败: {e}")))?
            .unwrap_or_default();
        new_state.last_updated = now;
        self.state_repo
            .upsert(binding.character_id, &new_state)
            .await
            .map_err(|e| DomainError::Internal(format!("角色状态落库失败: {e}")))?;

        self.event_bus
            .publish(&CoreEvent::BehaviorDecided(BehaviorDecidedEvent {
                character_id: binding.character_id,
                conversation_id: binding.conversation_id,
                action: format!("{:?}", decision.action),
                reason: decision.reason,
                timestamp: now,
            }));

        tracing::debug!(
            target: "runtime",
            character_id = binding.character_id,
            conversation_id = binding.conversation_id,
            "主动状态已更新并落库"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::behavior_engine::RuleBehaviorEngine;
    use crate::application::decision_orchestrator::{
        DecisionEvaluator, DecisionLlmPipeline, DecisionLlmService, LlmAssistance,
        LlmAssistanceTrigger, NoOpTrigger,
    };
    use crate::domain::character::{BehaviorState, CharacterBinding, ConversationState, ReplyMode};
    use crate::domain::decision::{
        BuiltInDecision, DecisionAction, DecisionContext, DecisionFailure, DecisionResult,
        DecisionResultValidator,
    };
    use crate::domain::emotion::Mood;
    use crate::domain::relationship::Relationship;
    use crate::domain::repository::{
        BehaviorStateRepository, CharacterBindingRepository, CharacterStateRepository,
        ConversationStateRepository, MoodRepository, RelationshipRepository,
    };
    use crate::error::RepositoryError;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// 可推进的假时钟。
    #[derive(Clone)]
    struct FakeClock {
        time: Arc<Mutex<DateTime<Utc>>>,
    }

    impl FakeClock {
        fn at(hour: u32, minute: u32) -> Self {
            let t = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(hour, minute, 0)
                .unwrap()
                .and_utc();
            Self {
                time: Arc::new(Mutex::new(t)),
            }
        }

        fn advance_minutes(&self, mins: i64) {
            let mut t = self.time.lock().unwrap();
            *t += chrono::Duration::minutes(mins);
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> DateTime<Utc> {
            *self.time.lock().unwrap()
        }
    }

    struct AlwaysRequiredTrigger;
    impl LlmAssistanceTrigger for AlwaysRequiredTrigger {
        fn evaluate(&self, _: &DecisionContext, _: &BuiltInDecision) -> LlmAssistance {
            LlmAssistance::Required {
                reason: "test".to_string(),
            }
        }
    }

    #[derive(Default)]
    struct CountingReplyScheduler(AtomicUsize);

    #[async_trait]
    impl ProactiveReplyScheduler for CountingReplyScheduler {
        async fn enqueue_proactive(
            &self,
            _: i64,
            _: i64,
            _: &str,
        ) -> Result<(), crate::error::RuntimeError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct StubDecisionLlmService;

    #[async_trait]
    impl DecisionLlmService for StubDecisionLlmService {
        async fn call(
            &self,
            _: &DecisionContext,
            _: &BuiltInDecision,
        ) -> Result<DecisionResult, DecisionFailure> {
            Err(DecisionFailure::Transport("test".to_string()))
        }

        fn name(&self) -> &str {
            "stub"
        }
    }

    // ---- 内存仓库 ----

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

    struct MemStateRepo {
        states: Mutex<HashMap<i64, crate::domain::character::CharacterState>>,
    }
    #[async_trait]
    impl CharacterStateRepository for MemStateRepo {
        async fn find_by_character_id(
            &self,
            character_id: i64,
        ) -> Result<Option<crate::domain::character::CharacterState>, RepositoryError> {
            Ok(self.states.lock().unwrap().get(&character_id).cloned())
        }
        async fn upsert(
            &self,
            character_id: i64,
            state: &crate::domain::character::CharacterState,
        ) -> Result<(), RepositoryError> {
            self.states
                .lock()
                .unwrap()
                .insert(character_id, state.clone());
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
    impl BehaviorStateRepository for MemBehaviorStateRepo {
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

    // ---- 装配 ----

    fn binding(
        character_id: i64,
        conversation_id: i64,
        proactive: bool,
        mute: Option<&str>,
    ) -> CharacterBinding {
        CharacterBinding {
            id: character_id,
            character_id,
            conversation_id,
            reply_mode: ReplyMode::Natural,
            proactive_enabled: proactive,
            mute_schedule: mute.map(String::from),
            behavior_overrides: serde_json::json!({}),
            context_policy: serde_json::json!({}),
            switched_at: None,
            cross_reply_enabled: false,
            created_at: Utc::now(),
        }
    }

    fn close_relationship(character_id: i64) -> Relationship {
        Relationship {
            character_id,
            user_id: 1,
            familiarity: 0.9,
            affection: 0.8,
            trust: 0.9,
            respect: 0.8,
            annoyance: 0.0,
            intimacy: 0.9,
            interaction_count: 50,
            last_interaction: Utc::now(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    struct Harness {
        driver: ProactiveDriver,
        behavior_engine: Arc<RuleBehaviorEngine>,
        state_repo: Arc<MemStateRepo>,
        conversation_state_repo: Arc<MemConversationStateRepo>,
        behavior_state_repo: Arc<MemBehaviorStateRepo>,
        binding_repo: Arc<MemBindingRepo>,
        clock: FakeClock,
    }

    /// 装配：角色 2 / 会话 20，启用主动，可带静默时段与关系。
    fn harness(proactive: bool, mute: Option<&str>, with_close_rel: bool) -> Harness {
        let clock = FakeClock::at(10, 0);
        let binding_repo = Arc::new(MemBindingRepo {
            bindings: Mutex::new(vec![binding(2, 20, proactive, mute)]),
        });
        let state_repo = Arc::new(MemStateRepo {
            states: Mutex::new(HashMap::new()),
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
        let rel_repo = Arc::new(MemRelationshipRepo {
            relationships: Mutex::new(if with_close_rel {
                vec![close_relationship(2)]
            } else {
                vec![]
            }),
        });

        let behavior_engine = Arc::new(RuleBehaviorEngine::new(
            binding_repo.clone(),
            mood_repo,
            rel_repo,
            state_repo.clone(),
            conversation_state_repo.clone(),
            behavior_state_repo.clone(),
            Arc::new(clock.clone()),
        ));
        let evaluator: Arc<dyn DecisionEvaluator> = behavior_engine.clone();
        let decision_orchestrator =
            DecisionOrchestrator::new(evaluator, Arc::new(NoOpTrigger), None);

        let driver = ProactiveDriver::new(
            binding_repo.clone(),
            state_repo.clone(),
            behavior_state_repo.clone(),
            decision_orchestrator,
            EventBus::new(),
            Arc::new(clock.clone()),
        );
        Harness {
            driver,
            behavior_engine,
            state_repo,
            conversation_state_repo,
            behavior_state_repo,
            binding_repo,
            clock,
        }
    }

    #[tokio::test]
    async fn find_all_returns_every_binding() {
        let h = harness(true, None, false);
        let all = h.binding_repo.find_all().await.unwrap();
        assert_eq!(all.len(), 1);
        assert!(all[0].proactive_enabled);
    }

    #[tokio::test]
    async fn disabled_proactive_binding_never_touches_state() {
        let h = harness(false, None, false);
        h.driver.tick().await;
        assert!(
            h.state_repo.states.lock().unwrap().is_empty(),
            "未启用主动不应落库任何状态"
        );
        assert!(
            h.behavior_state_repo.states.lock().unwrap().is_empty(),
            "未启用主动不应落库任何行为状态"
        );
    }

    #[tokio::test]
    async fn no_scheduler_does_not_emit_proactive_messages() {
        let h = harness(true, None, true);
        // 没有持久回复调度器时，程序不能凭空发言，也不消耗冷却配额。
        h.driver.tick().await;
        assert!(h.behavior_state_repo.states.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn no_scheduler_preserves_state_across_ticks() {
        let h = harness(true, None, true);
        h.driver.tick().await;
        h.clock.advance_minutes(31);
        h.driver.tick().await;
        assert!(h.behavior_state_repo.states.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn mute_window_skips_proactive() {
        // 10:00 处于 09:00-12:00 静默窗口内 → 跳过，不落库。
        let h = harness(true, Some("09:00-12:00"), true);
        h.driver.tick().await;
        assert!(
            h.state_repo.states.lock().unwrap().is_empty(),
            "静默时段应跳过主动，不落库"
        );
        assert!(
            h.behavior_state_repo.states.lock().unwrap().is_empty(),
            "静默时段应跳过主动，不落库行为状态"
        );
    }

    fn replace_decision_result(harness: &mut Harness, result: DecisionResult) {
        let pipeline = Arc::new(
            DecisionLlmPipeline::new(
                Arc::new(StubDecisionLlmService),
                None,
                DecisionResultValidator::new(2_000, 60_000),
                Arc::new(crate::domain::decision::DefaultOverridePolicy),
            )
            .with_override_result(result),
        );
        let evaluator: Arc<dyn DecisionEvaluator> = harness.behavior_engine.clone();
        harness.driver.decision_orchestrator =
            DecisionOrchestrator::new(evaluator, Arc::new(AlwaysRequiredTrigger), Some(pipeline));
    }

    #[tokio::test]
    async fn model_veto_prevents_proactive_quota_and_enqueue() {
        let mut h = harness(true, None, true);
        h.state_repo
            .states
            .lock()
            .unwrap()
            .insert(2, crate::domain::character::CharacterState::default());
        h.conversation_state_repo.states.lock().unwrap().insert(
            (2, 20),
            crate::domain::character::ConversationState::default(),
        );
        let scheduler = Arc::new(CountingReplyScheduler::default());
        h.driver = h.driver.with_reply_scheduler(scheduler.clone());
        replace_decision_result(
            &mut h,
            DecisionResult {
                action: DecisionAction::SuggestIgnore,
                reasoning: Some("本轮先不主动发言".to_string()),
                confidence: Some(0.9),
            },
        );

        h.driver.tick().await;

        assert_eq!(scheduler.0.load(Ordering::SeqCst), 0);
        assert!(h.behavior_state_repo.states.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn model_keep_reserves_proactive_quota_and_enqueues() {
        let mut h = harness(true, None, true);
        h.state_repo
            .states
            .lock()
            .unwrap()
            .insert(2, crate::domain::character::CharacterState::default());
        h.conversation_state_repo.states.lock().unwrap().insert(
            (2, 20),
            crate::domain::character::ConversationState::default(),
        );
        let scheduler = Arc::new(CountingReplyScheduler::default());
        h.driver = h.driver.with_reply_scheduler(scheduler.clone());
        replace_decision_result(&mut h, DecisionResult::keep());

        h.driver.tick().await;

        assert_eq!(scheduler.0.load(Ordering::SeqCst), 1);
        assert_eq!(h.behavior_state_repo.states.lock().unwrap().len(), 1);
    }
}
