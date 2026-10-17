//! 关系服务 —— 关系状态的读取、更新与持久化。
//!
//! 关系是 Character × User 的结构化状态，任何永久变化都通过
//! [`crate::domain::repository::RelationshipRepository`] 落库，并发布
//! [`crate::domain::event::RelationshipChangedEvent`]。

use std::sync::Arc;

use chrono::Utc;

use crate::application::event_bus::EventBus;
use crate::domain::event::{CoreEvent, RelationshipChangedEvent};
use crate::domain::relationship::Relationship;
use crate::domain::repository::RelationshipRepository;
use crate::error::RuntimeError;

/// 关系服务。
pub struct RelationshipService {
    relationship_repo: Arc<dyn RelationshipRepository>,
    event_bus: EventBus,
}

impl RelationshipService {
    /// 创建一个关系服务。
    pub fn new(relationship_repo: Arc<dyn RelationshipRepository>, event_bus: EventBus) -> Self {
        Self {
            relationship_repo,
            event_bus,
        }
    }

    /// 加载（或创建默认）一段角色与参与者之间的关系。
    pub async fn load_or_new(
        &self,
        character_id: i64,
        participant_id: i64,
    ) -> Result<Relationship, RuntimeError> {
        if let Some(rel) = self
            .relationship_repo
            .find(character_id, participant_id)
            .await?
        {
            return Ok(rel);
        }
        self.relationship_repo
            .create_for_participant(character_id, participant_id)
            .await
            .map_err(RuntimeError::Repository)
    }

    /// 使用平台稳定用户键查找已有关系；找不到时回退到当前会话参与者。
    pub async fn canonical_participant_id(
        &self,
        character_id: i64,
        participant_id: i64,
        stable_user_key: Option<&str>,
    ) -> Result<i64, RuntimeError> {
        let Some(key) = stable_user_key.filter(|value| !value.trim().is_empty()) else {
            return Ok(participant_id);
        };
        self.relationship_repo
            .ensure_stable_relationship_participant(character_id, participant_id, key)
            .await
            .map_err(RuntimeError::Repository)
    }

    /// 记录一次已去重交互，仅累计熟悉度。调用方必须保证事件只应用一次。
    pub async fn record_interaction(
        &self,
        character_id: i64,
        participant_id: i64,
    ) -> Result<Relationship, RuntimeError> {
        let mut rel = self.load_or_new(character_id, participant_id).await?;

        // 调用领域模型记录交互（更新次数 / 时间 / 熟悉度）。
        rel.record_interaction();

        self.relationship_repo.upsert(&rel).await?;

        self.event_bus
            .publish(&CoreEvent::RelationshipChanged(RelationshipChangedEvent {
                character_id,
                user_id: rel.user_id,
                timestamp: Utc::now(),
            }));

        Ok(rel)
    }

    /// 按来源消息幂等记录一次交互；Outbox 重试不会重复增加熟悉度。
    pub async fn record_interaction_once(
        &self,
        character_id: i64,
        participant_id: i64,
        source_message_id: i64,
    ) -> Result<bool, crate::error::RepositoryError> {
        let user_id = self
            .relationship_repo
            .record_interaction_once(character_id, participant_id, source_message_id)
            .await?;
        if let Some(user_id) = user_id {
            self.event_bus
                .publish(&CoreEvent::RelationshipChanged(RelationshipChangedEvent {
                    character_id,
                    user_id,
                    timestamp: Utc::now(),
                }));
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RepositoryError;
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct MemRelationshipRepo {
        relationships: Mutex<Vec<Relationship>>,
    }
    impl MemRelationshipRepo {
        fn find_mem(&self, c: i64, p: i64) -> Option<Relationship> {
            self.relationships
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.character_id == c && r.user_id == p)
                .cloned()
        }
    }
    #[async_trait]
    impl RelationshipRepository for MemRelationshipRepo {
        async fn create_for_participant(
            &self,
            character_id: i64,
            participant_id: i64,
        ) -> Result<Relationship, RepositoryError> {
            let relationship = Relationship::new(character_id, participant_id);
            self.upsert(&relationship).await?;
            Ok(relationship)
        }

        async fn find(
            &self,
            character_id: i64,
            participant_id: i64,
        ) -> Result<Option<Relationship>, RepositoryError> {
            Ok(self.find_mem(character_id, participant_id))
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
        async fn upsert(&self, relationship: &Relationship) -> Result<(), RepositoryError> {
            let mut all = self.relationships.lock().unwrap();
            if let Some(existing) = all.iter_mut().find(|r| {
                r.character_id == relationship.character_id && r.user_id == relationship.user_id
            }) {
                *existing = relationship.clone();
            } else {
                all.push(relationship.clone());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn load_or_new_creates_default_and_persists() {
        let repo = Arc::new(MemRelationshipRepo {
            relationships: Mutex::new(vec![]),
        });
        let bus = EventBus::new();
        let service = RelationshipService::new(repo.clone(), bus);

        let rel = service.load_or_new(1, 2).await.unwrap();
        assert_eq!(rel.character_id, 1);
        assert_eq!(rel.user_id, 2);
        // 已落库。
        assert!(repo.find_mem(1, 2).is_some());
    }

    #[tokio::test]
    async fn record_interaction_increments_and_publishes() {
        let repo = Arc::new(MemRelationshipRepo {
            relationships: Mutex::new(vec![]),
        });
        let bus = EventBus::new();
        let service = RelationshipService::new(repo.clone(), bus.clone());

        let mut sub = bus.subscribe();

        let rel = service.record_interaction(1, 2).await.unwrap();
        assert_eq!(rel.interaction_count, 1);
        assert_eq!(rel.affection, 0.2); // 熟悉并不自动代表好感

        // 落库。
        let persisted = repo.find_mem(1, 2).unwrap();
        assert_eq!(persisted.interaction_count, 1);

        // 发布关系变更事件。
        let event = sub.recv().await.expect("应收到关系变更事件");
        match event {
            CoreEvent::RelationshipChanged(e) => {
                assert_eq!(e.character_id, 1);
                assert_eq!(e.user_id, 2);
            }
            other => panic!("期望 RelationshipChanged，实际 {other:?}"),
        }
    }

    #[tokio::test]
    async fn record_interaction_accumulates() {
        let repo = Arc::new(MemRelationshipRepo {
            relationships: Mutex::new(vec![]),
        });
        let bus = EventBus::new();
        let service = RelationshipService::new(repo.clone(), bus);

        for _ in 0..3 {
            service.record_interaction(7, 9).await.unwrap();
        }
        let rel = repo.find_mem(7, 9).unwrap();
        assert_eq!(rel.interaction_count, 3);
    }
}
