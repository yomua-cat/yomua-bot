//! Mood 服务 —— 标量情绪状态的读取、更新与持久化。
//!
//! `Mood` 按 Character × Conversation 范围管理，与状态领域模型重构后的
//! 其它会话级状态一致。

use std::sync::Arc;

use chrono::Utc;

use crate::domain::emotion::Mood;
use crate::domain::repository::MoodRepository;
use crate::error::RuntimeError;

/// Mood 服务。
pub struct MoodService {
    mood_repo: Arc<dyn MoodRepository>,
}

impl MoodService {
    /// 创建一个 Mood 服务。
    pub fn new(mood_repo: Arc<dyn MoodRepository>) -> Self {
        Self { mood_repo }
    }

    /// 加载一个角色在某会话的 Mood；若尚无持久化记录，则写入默认状态。
    pub async fn load(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<Mood, RuntimeError> {
        if let Some(mood) = self
            .mood_repo
            .find_by_character_and_conversation(character_id, conversation_id)
            .await?
        {
            return Ok(mood);
        }
        let default = Mood::default();
        self.mood_repo
            .upsert(character_id, conversation_id, &default)
            .await?;
        Ok(default)
    }

    /// 对一条收到消息应用情绪变化（Character × Conversation 范围）。
    ///
    /// 收到消息的调整说明：与人交流带来愉悦（+2）、增进好感（按现有模型以 mood 上升体现）。
    pub async fn apply_message_event(
        &self,
        character_id: i64,
        conversation_id: i64,
        _user_message: &str,
    ) -> Result<Mood, RuntimeError> {
        let mut mood = self.load(character_id, conversation_id).await?;
        mood = mood.clamped();
        // 收到消息后情绪略微回升。
        mood.value = (mood.value + 2.0).clamp(0.0, 100.0);
        mood.last_updated = Utc::now();
        self.mood_repo
            .upsert(character_id, conversation_id, &mood)
            .await?;
        Ok(mood)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RepositoryError;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// 内存版 MoodRepo，使用 (character_id, conversation_id) 二元组作为 key。
    struct MemMoodRepo {
        moods: Mutex<HashMap<(i64, i64), Mood>>,
    }
    impl MemMoodRepo {
        fn persisted(&self, character_id: i64, conversation_id: i64) -> Option<Mood> {
            self.moods
                .lock()
                .unwrap()
                .get(&(character_id, conversation_id))
                .cloned()
        }
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

    #[tokio::test]
    async fn load_creates_and_persists_default() {
        let repo = Arc::new(MemMoodRepo {
            moods: Mutex::new(HashMap::new()),
        });
        let service = MoodService::new(repo.clone());

        let mood = service.load(1, 10).await.unwrap();
        assert_eq!(mood.value, Mood::default().value);
        // 已落库。
        assert!(repo.persisted(1, 10).is_some());
    }

    #[tokio::test]
    async fn apply_message_event_updates_and_persists() {
        let repo = Arc::new(MemMoodRepo {
            moods: Mutex::new(HashMap::new()),
        });
        let service = MoodService::new(repo.clone());

        let mood = service.apply_message_event(1, 10, "你好").await.unwrap();
        // 收到消息 → mood 上升。
        assert!(mood.value > Mood::default().value);

        // 落库。
        let persisted = repo.persisted(1, 10).unwrap();
        assert_eq!(persisted.value, mood.value);
    }

    #[tokio::test]
    async fn mood_isolation_between_conversations() {
        let repo = Arc::new(MemMoodRepo {
            moods: Mutex::new(HashMap::new()),
        });
        let service = MoodService::new(repo.clone());

        let mut mood_q = service.load(1, 10).await.unwrap();
        mood_q.value = 80.0;
        repo.upsert(1, 10, &mood_q).await.unwrap();

        let mut mood_w = service.load(1, 20).await.unwrap();
        mood_w.value = 20.0;
        repo.upsert(1, 20, &mood_w).await.unwrap();

        assert_eq!(repo.persisted(1, 10).unwrap().value, 80.0);
        assert_eq!(repo.persisted(1, 20).unwrap().value, 20.0);
    }
}
