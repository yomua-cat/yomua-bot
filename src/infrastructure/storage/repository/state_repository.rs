//! CharacterStateRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::character::CharacterState;
use crate::domain::repository::CharacterStateRepository;
use crate::error::RepositoryError;

pub struct SqliteCharacterStateRepository {
    pool: SqlitePool,
}

impl SqliteCharacterStateRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CharacterStateRepository for SqliteCharacterStateRepository {
    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Option<CharacterState>, RepositoryError> {
        let row: Option<(
            f64,
            f64,
            Option<String>,
            Option<String>,
            f64,
            Option<String>,
            String,
        )> = sqlx::query_as(
            r#"SELECT energy, attention, current_activity, social_mood, stress,
                    last_proactive_at, last_updated
             FROM character_states WHERE character_id = ?"#,
        )
        .bind(character_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((
                energy,
                attention,
                current_activity,
                social_mood,
                stress,
                last_proactive_at,
                last_updated,
            )) => {
                let last_updated = super::timestamp::parse_timestamp(&last_updated)?;
                let last_proactive_at = match last_proactive_at {
                    Some(s) => Some(super::timestamp::parse_timestamp(&s)?),
                    None => None,
                };

                Ok(Some(CharacterState {
                    energy,
                    attention,
                    current_activity,
                    social_mood,
                    stress,
                    last_proactive_at,
                    last_updated,
                }))
            }
            None => Ok(None),
        }
    }

    async fn upsert(
        &self,
        character_id: i64,
        state: &CharacterState,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"INSERT INTO character_states (character_id, energy, attention, current_activity, social_mood, stress, last_proactive_at, last_updated)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(character_id) DO UPDATE SET
                energy = excluded.energy,
                attention = excluded.attention,
                current_activity = excluded.current_activity,
                social_mood = excluded.social_mood,
                stress = excluded.stress,
                last_proactive_at = excluded.last_proactive_at,
                last_updated = excluded.last_updated"#,
        )
        .bind(character_id)
        .bind(state.energy)
        .bind(state.attention)
        .bind(&state.current_activity)
        .bind(&state.social_mood)
        .bind(state.stress)
        .bind(state.last_proactive_at.map(|t| t.to_rfc3339()))
        .bind(state.last_updated.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }
}
