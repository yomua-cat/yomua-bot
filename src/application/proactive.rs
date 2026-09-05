//! 主动行为驱动 —— 后台循环评估角色是否有主动意愿并落库。
//!
//! 每个 tick 枚举所有绑定上的角色，仅在满足以下全部条件时询问行为引擎
//! `decide_proactive`：启用主动、与上次主动间隔超过冷却、不处于静默时段。
//! MVP 阶段主动动作仅更新内部状态（写回 `last_proactive_at`），
//! 不发送消息、不调用 LLM。

use std::sync::Arc;
use std::time::Duration;

use crate::application::behavior_engine::time_of_day;
use crate::application::clock::Clock;
use crate::application::event_bus::EventBus;
use crate::domain::behavior::{BehaviorAction, BehaviorEngine};
use crate::domain::character::CharacterBinding;
use crate::domain::event::{BehaviorDecidedEvent, CoreEvent};
use crate::domain::mute::{is_within_window, parse_mute_schedule};
use crate::domain::repository::{CharacterBindingRepository, CharacterStateRepository};
use crate::error::DomainError;

/// 主动行为驱动的固定 TICK 间隔（60 秒）。
pub const PROACTIVE_TICK_INTERVAL: Duration = Duration::from_secs(60);

/// 主动行为的固定冷却时长（30 分钟）。
pub const PROACTIVE_COOLDOWN: Duration = Duration::from_secs(30 * 60);

/// 主动行为驱动。
///
/// 在后台以固定间隔运行；每次 `tick` 独立评估所有启用主动的绑定。
pub struct ProactiveDriver {
    binding_repo: Arc<dyn CharacterBindingRepository>,
    state_repo: Arc<dyn CharacterStateRepository>,
    behavior_engine: Arc<dyn BehaviorEngine>,
    event_bus: EventBus,
    clock: Arc<dyn Clock>,
    tick_interval: Duration,
    cooldown: Duration,
}

impl ProactiveDriver {
    /// 创建一个主动行为驱动。tick 间隔与冷却使用固定常量。
    pub fn new(
        binding_repo: Arc<dyn CharacterBindingRepository>,
        state_repo: Arc<dyn CharacterStateRepository>,
        behavior_engine: Arc<dyn BehaviorEngine>,
        event_bus: EventBus,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            binding_repo,
            state_repo,
            behavior_engine,
            event_bus,
            clock,
            tick_interval: PROACTIVE_TICK_INTERVAL,
            cooldown: PROACTIVE_COOLDOWN,
        }
    }

    /// 以固定间隔循环运行（常驻后台任务）。
    pub async fn run(self) {
        tracing::info!(target: "runtime", "主动行为驱动已启动");
        loop {
            self.tick().await;
            tokio::time::sleep(self.tick_interval).await;
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

        // 静默时段跳过主动。
        let in_mute = binding
            .mute_schedule
            .as_deref()
            .and_then(|s| parse_mute_schedule(s).ok().flatten())
            .map(|w| is_within_window(&w, &time_of_day(now)))
            .unwrap_or(false);
        if in_mute {
            return Ok(());
        }

        // 冷却判断：距上次主动不足冷却时长则跳过。
        let existing = self
            .state_repo
            .find_by_character_id(binding.character_id)
            .await
            .map_err(|e| DomainError::Internal(format!("读取角色状态失败: {e}")))?;
        if let Some(state) = &existing {
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
            .behavior_engine
            .decide_proactive(binding.character_id, binding.conversation_id)
            .await?;

        // MVP：仅处理状态更新类动作。
        if decision.action != BehaviorAction::UpdateState {
            return Ok(());
        }

        // 写回维护后的状态：更新主动时间戳。
        let mut new_state =
            existing.unwrap_or_else(crate::domain::character::CharacterState::default);
        new_state.last_proactive_at = Some(now);
        new_state.last_updated = now;
        self.state_repo
            .upsert(binding.character_id, &new_state)
            .await
            .map_err(|e| DomainError::Internal(format!("主动状态落库失败: {e}")))?;

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
    use crate::domain::character::{CharacterBinding, CharacterState, ReplyMode};
    use crate::domain::emotion::EmotionState;
    use crate::domain::relationship::Relationship;
    use crate::domain::repository::{
        CharacterBindingRepository, CharacterStateRepository, EmotionStateRepository,
        RelationshipRepository,
    };
    use crate::error::RepositoryError;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;
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
        async fn insert(&self, b: &CharacterBinding) -> Result<i64, RepositoryError> {
            self.bindings.lock().unwrap().push(b.clone());
            Ok(b.id)
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
            created_at: Utc::now(),
        }
    }

    fn close_relationship(character_id: i64) -> Relationship {
        Relationship {
            character_id,
            participant_id: 1,
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
        state_repo: Arc<MemStateRepo>,
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
        let emotion_repo = Arc::new(MemEmotionRepo {
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
            emotion_repo,
            rel_repo,
            state_repo.clone(),
            Arc::new(clock.clone()),
        ));

        let driver = ProactiveDriver::new(
            binding_repo.clone(),
            state_repo.clone(),
            behavior_engine,
            EventBus::new(),
            Arc::new(clock.clone()),
        );
        Harness {
            driver,
            state_repo,
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
    }

    #[tokio::test]
    async fn cooldown_blocks_trigger_within_window() {
        let h = harness(true, None, true);
        // 首个 tick（10:00）触发并写入 last_proactive_at。
        h.driver.tick().await;
        let after_first = h
            .state_repo
            .states
            .lock()
            .unwrap()
            .get(&2)
            .cloned()
            .expect("首轮应落库状态");

        // 5 分钟后仍在冷却内：last_proactive_at 不变。
        h.clock.advance_minutes(5);
        h.driver.tick().await;
        let after_second = h
            .state_repo
            .states
            .lock()
            .unwrap()
            .get(&2)
            .cloned()
            .unwrap();
        assert_eq!(
            after_first.last_proactive_at,
            after_second.last_proactive_at
        );
    }

    #[tokio::test]
    async fn after_cooldown_triggers_and_updates_timestamp() {
        let h = harness(true, None, true);
        // 首轮触发写入初始时间。
        h.driver.tick().await;
        let first = h
            .state_repo
            .states
            .lock()
            .unwrap()
            .get(&2)
            .cloned()
            .unwrap();
        let first_time = first.last_proactive_at.expect("首轮应写入主动时间");

        // 31 分钟后冷却已过 → 再次触发并更新时间。
        h.clock.advance_minutes(31);
        h.driver.tick().await;
        let second = h
            .state_repo
            .states
            .lock()
            .unwrap()
            .get(&2)
            .cloned()
            .unwrap();
        let second_time = second.last_proactive_at.expect("过冷却后应再次写入");
        assert!(
            second_time > first_time,
            "过冷却后主动时间应更新（{second_time} > {first_time}）"
        );
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
    }
}
