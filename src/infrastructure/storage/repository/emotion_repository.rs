//! EmotionStateRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::emotion::EmotionState;
use crate::domain::repository::EmotionStateRepository;
use crate::error::RepositoryError;

pub struct SqliteEmotionStateRepository {
    pool: SqlitePool,
}

impl SqliteEmotionStateRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn parse_emotion_row(
    row: (f64, f64, f64, f64, f64, f64, f64, String),
) -> Result<EmotionState, RepositoryError> {
    let (happiness, anger, sadness, fear, affection, stress, energy, last_updated) = row;
    let last_updated = super::timestamp::parse_timestamp(&last_updated)?;
    Ok(EmotionState {
        happiness,
        anger,
        sadness,
        fear,
        affection,
        stress,
        energy,
        last_updated,
    })
}

#[async_trait]
impl EmotionStateRepository for SqliteEmotionStateRepository {
    #[allow(deprecated)]
    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Option<EmotionState>, RepositoryError> {
        // 旧方法：查找任一 conversation 的记录（取第一个）。
        // 迁移后每个 conversation 都有独立记录，取最新的一个。
        let row: Option<(f64, f64, f64, f64, f64, f64, f64, String)> = sqlx::query_as(
            r#"SELECT happiness, anger, sadness, fear, affection, stress, energy, last_updated
             FROM emotion_states WHERE character_id = ?
             ORDER BY last_updated DESC LIMIT 1"#,
        )
        .bind(character_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => Ok(Some(parse_emotion_row(r)?)),
            None => Ok(None),
        }
    }

    async fn find_by_character_and_conversation(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<Option<EmotionState>, RepositoryError> {
        let row: Option<(f64, f64, f64, f64, f64, f64, f64, String)> = sqlx::query_as(
            r#"SELECT happiness, anger, sadness, fear, affection, stress, energy, last_updated
             FROM emotion_states
             WHERE character_id = ? AND conversation_id = ?"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => Ok(Some(parse_emotion_row(r)?)),
            None => Ok(None),
        }
    }

    #[allow(deprecated)]
    async fn upsert(&self, character_id: i64, state: &EmotionState) -> Result<(), RepositoryError> {
        // 旧方法：写入到 character_id 的任一 conversation（取 conversation_id = 0 作为默认值）
        sqlx::query(
            r#"INSERT INTO emotion_states
                (character_id, conversation_id, happiness, anger, sadness, fear, affection, stress, energy, last_updated)
             VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(character_id, conversation_id) DO UPDATE SET
                happiness = excluded.happiness,
                anger = excluded.anger,
                sadness = excluded.sadness,
                fear = excluded.fear,
                affection = excluded.affection,
                stress = excluded.stress,
                energy = excluded.energy,
                last_updated = excluded.last_updated"#,
        )
        .bind(character_id)
        .bind(state.happiness)
        .bind(state.anger)
        .bind(state.sadness)
        .bind(state.fear)
        .bind(state.affection)
        .bind(state.stress)
        .bind(state.energy)
        .bind(state.last_updated.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }

    async fn upsert_scoped(
        &self,
        character_id: i64,
        conversation_id: i64,
        state: &EmotionState,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"INSERT INTO emotion_states
                (character_id, conversation_id, happiness, anger, sadness, fear, affection, stress, energy, last_updated)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(character_id, conversation_id) DO UPDATE SET
                happiness = excluded.happiness,
                anger = excluded.anger,
                sadness = excluded.sadness,
                fear = excluded.fear,
                affection = excluded.affection,
                stress = excluded.stress,
                energy = excluded.energy,
                last_updated = excluded.last_updated"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(state.happiness)
        .bind(state.anger)
        .bind(state.sadness)
        .bind(state.fear)
        .bind(state.affection)
        .bind(state.stress)
        .bind(state.energy)
        .bind(state.last_updated.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }
}
