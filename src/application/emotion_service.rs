//! 情绪服务 —— 情绪状态的读取、更新与持久化。
//!
//! 情绪采用确定性模型：前一个状态 + 事件 = 新状态，任何永久变化
//! 都会通过 [`crate::domain::repository::EmotionStateRepository`]
//! 落库，并发布 [`crate::domain::event::EmotionChangedEvent`]。

use std::sync::Arc;

use chrono::Utc;

use crate::application::event_bus::EventBus;
use crate::domain::emotion::{EmotionAdjustment, EmotionEvent, EmotionEventType, EmotionState};
use crate::domain::event::{CoreEvent, EmotionChangedEvent};
use crate::domain::repository::EmotionStateRepository;
use crate::error::RuntimeError;

/// 时间衰减速率（每秒），用于 `apply_decay`。
const DECAY_RATE: f64 = 0.05;

/// 情绪服务。
pub struct EmotionService {
    emotion_repo: Arc<dyn EmotionStateRepository>,
    event_bus: EventBus,
}

impl EmotionService {
    /// 创建一个情绪服务。
    pub fn new(emotion_repo: Arc<dyn EmotionStateRepository>, event_bus: EventBus) -> Self {
        Self {
            emotion_repo,
            event_bus,
        }
    }

    /// 加载一个角色的情绪状态（Character × Conversation 范围）；若尚无持久化记录，则写入默认状态。
    pub async fn load(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<EmotionState, RuntimeError> {
        if let Some(state) = self
            .emotion_repo
            .find_by_character_and_conversation(character_id, conversation_id)
            .await?
        {
            return Ok(state);
        }
        let default = EmotionState::default();
        self.emotion_repo
            .upsert_scoped(character_id, conversation_id, &default)
            .await?;
        Ok(default)
    }

    /// 对一条收到消息应用情绪变化（Character × Conversation 范围）：先衰减，再应用事件调整，最后落库并发布事件。
    ///
    /// 收到消息的调整说明：
    /// - `happiness +0.03`：和人交流带来愉悦；
    /// - `affection +0.02`：互动增进好感；
    /// - `energy -0.01`：消耗少量精力。
    pub async fn apply_message_event(
        &self,
        character_id: i64,
        conversation_id: i64,
        _user_message: &str,
    ) -> Result<EmotionState, RuntimeError> {
        let state = self.load(character_id, conversation_id).await?;

        // 先按流逝时间衰减。
        let now = Utc::now();
        let elapsed = (now - state.last_updated).num_seconds().max(0) as f64;
        let mut decayed = state.clone();
        decayed.apply_decay(elapsed, DECAY_RATE);

        // 再应用「收到消息」事件调整。
        let event = EmotionEvent {
            event_type: EmotionEventType::MessageReceived,
            adjustments: vec![
                // 愉悦 + 0.03
                EmotionAdjustment {
                    dimension: "happiness".to_string(),
                    value: 0.03,
                },
                // 好感 + 0.02
                EmotionAdjustment {
                    dimension: "affection".to_string(),
                    value: 0.02,
                },
                // 精力 - 0.01
                EmotionAdjustment {
                    dimension: "energy".to_string(),
                    value: -0.01,
                },
            ],
            description: Some("收到用户消息".to_string()),
        };
        let new_state = decayed.apply_event(&event);

        // 落库（任何永久变化必须持久化）。
        self.emotion_repo
            .upsert_scoped(character_id, conversation_id, &new_state)
            .await?;

        // 发布事件。
        self.event_bus
            .publish(&CoreEvent::EmotionChanged(EmotionChangedEvent {
                character_id,
                conversation_id,
                timestamp: Utc::now(),
            }));

        Ok(new_state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RepositoryError;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// 内存版 EmotionRepo，使用 (character_id, conversation_id) 二元组作为 key。
    struct MemEmotionRepo {
        states: Mutex<HashMap<(i64, i64), EmotionState>>,
    }
    impl MemEmotionRepo {
        fn persisted(&self, character_id: i64, conversation_id: i64) -> Option<EmotionState> {
            self.states
                .lock()
                .unwrap()
                .get(&(character_id, conversation_id))
                .cloned()
        }
    }
    #[async_trait]
    impl EmotionStateRepository for MemEmotionRepo {
        #[allow(deprecated)]
        async fn find_by_character_id(
            &self,
            _character_id: i64,
        ) -> Result<Option<EmotionState>, RepositoryError> {
            let states = self.states.lock().unwrap();
            // 旧方法：取任一 conversation 的记录（取第一个）
            Ok(states.values().next().cloned())
        }
        async fn find_by_character_and_conversation(
            &self,
            character_id: i64,
            conversation_id: i64,
        ) -> Result<Option<EmotionState>, RepositoryError> {
            Ok(self
                .states
                .lock()
                .unwrap()
                .get(&(character_id, conversation_id))
                .cloned())
        }
        #[allow(deprecated)]
        async fn upsert(
            &self,
            character_id: i64,
            state: &EmotionState,
        ) -> Result<(), RepositoryError> {
            // 旧方法：写入 conversation_id = 0
            self.states
                .lock()
                .unwrap()
                .insert((character_id, 0), state.clone());
            Ok(())
        }
        async fn upsert_scoped(
            &self,
            character_id: i64,
            conversation_id: i64,
            state: &EmotionState,
        ) -> Result<(), RepositoryError> {
            self.states
                .lock()
                .unwrap()
                .insert((character_id, conversation_id), state.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn load_creates_and_persists_default() {
        let repo = Arc::new(MemEmotionRepo {
            states: Mutex::new(HashMap::new()),
        });
        let bus = EventBus::new();
        let service = EmotionService::new(repo.clone(), bus);

        let state = service.load(1, 10).await.unwrap();
        assert_eq!(state.happiness, EmotionState::default().happiness);
        // 已落库。
        assert!(repo.persisted(1, 10).is_some());
    }

    #[tokio::test]
    async fn apply_message_event_updates_and_persists() {
        let repo = Arc::new(MemEmotionRepo {
            states: Mutex::new(HashMap::new()),
        });
        let bus = EventBus::new();
        let service = EmotionService::new(repo.clone(), bus.clone());

        let mut sub = bus.subscribe();

        let state = service.apply_message_event(1, 10, "你好").await.unwrap();
        // 收到消息 → happiness 上升、affection 上升、energy 下降。
        assert!(state.happiness > EmotionState::default().happiness);
        assert!(state.affection > EmotionState::default().affection);
        assert!(state.energy < EmotionState::default().energy);

        // 落库。
        let persisted = repo.persisted(1, 10).unwrap();
        assert_eq!(persisted.happiness, state.happiness);

        // 发布了情绪变更事件。
        let event = sub.recv().await.expect("应收到情绪变更事件");
        match event {
            CoreEvent::EmotionChanged(e) => {
                assert_eq!(e.character_id, 1);
                assert_eq!(e.conversation_id, 10);
            }
            other => panic!("期望 EmotionChanged，实际 {other:?}"),
        }
    }

    #[tokio::test]
    async fn emotion_isolation_between_conversations() {
        // Alice 在 Q (10) 和 W (20) 应该有不同的情绪状态
        let repo = Arc::new(MemEmotionRepo {
            states: Mutex::new(HashMap::new()),
        });
        let bus = EventBus::new();
        let service = EmotionService::new(repo.clone(), bus);

        // Alice 在 Q：happiness = 0.8
        let state_q = service.load(1, 10).await.unwrap();
        let mut state_q = state_q;
        state_q.happiness = 0.8;
        repo.upsert_scoped(1, 10, &state_q).await.unwrap();

        // Alice 在 W：happiness = 0.2
        let state_w = service.load(1, 20).await.unwrap();
        let mut state_w = state_w;
        state_w.happiness = 0.2;
        repo.upsert_scoped(1, 20, &state_w).await.unwrap();

        // 验证隔离：Alice × Q 的情绪不会影响 Alice × W
        let check_q = repo.persisted(1, 10).unwrap();
        let check_w = repo.persisted(1, 20).unwrap();
        assert_eq!(check_q.happiness, 0.8);
        assert_eq!(check_w.happiness, 0.2);
    }

    #[tokio::test]
    async fn emotion_isolation_between_characters() {
        // Alice 和 Bob 在同一 Conversation Q 应该有不同情绪
        let repo = Arc::new(MemEmotionRepo {
            states: Mutex::new(HashMap::new()),
        });
        // 直接操作 repo 测试隔离，不通过 service 层
        let _bus = EventBus::new();

        // Alice 在 Q：happiness = 0.9
        let mut state_alice = EmotionState::default();
        state_alice.happiness = 0.9;
        repo.upsert_scoped(1, 10, &state_alice).await.unwrap();

        // Bob 在 Q：happiness = 0.1
        let mut state_bob = EmotionState::default();
        state_bob.happiness = 0.1;
        repo.upsert_scoped(2, 10, &state_bob).await.unwrap();

        // 验证隔离：Bob 的情绪不会继承 Alice
        let check_alice = repo.persisted(1, 10).unwrap();
        let check_bob = repo.persisted(2, 10).unwrap();
        assert_eq!(check_alice.happiness, 0.9);
        assert_eq!(check_bob.happiness, 0.1);
    }
}
