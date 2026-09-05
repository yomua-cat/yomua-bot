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

#[async_trait]
impl EmotionStateRepository for SqliteEmotionStateRepository {
    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Option<EmotionState>, RepositoryError> {
        let row: Option<(f64, f64, f64, f64, f64, f64, f64, String)> = sqlx::query_as(
            r#"SELECT happiness, anger, sadness, fear, affection, stress, energy, last_updated
             FROM emotion_states WHERE character_id = ?"#,
        )
        .bind(character_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((happiness, anger, sadness, fear, affection, stress, energy, last_updated)) => {
                let last_updated = super::timestamp::parse_timestamp(&last_updated)?;
                Ok(Some(EmotionState {
                    happiness,
                    anger,
                    sadness,
                    fear,
                    affection,
                    stress,
                    energy,
                    last_updated,
                }))
            }
            None => Ok(None),
        }
    }

    async fn upsert(&self, character_id: i64, state: &EmotionState) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"INSERT INTO emotion_states
                (character_id, happiness, anger, sadness, fear, affection, stress, energy, last_updated)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(character_id) DO UPDATE SET
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
}
