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
        let row: Option<(f64, f64, Option<String>, String)> = sqlx::query_as(
            r#"SELECT energy, stress, current_activity, last_updated
             FROM character_states WHERE character_id = ?"#,
        )
        .bind(character_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((energy, stress, current_activity, last_updated)) => {
                let last_updated = super::timestamp::parse_timestamp(&last_updated)?;
                Ok(Some(CharacterState {
                    energy,
                    stress,
                    current_activity,
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
            r#"INSERT INTO character_states (character_id, energy, stress, current_activity, last_updated)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(character_id) DO UPDATE SET
                energy = excluded.energy,
                stress = excluded.stress,
                current_activity = excluded.current_activity,
                last_updated = excluded.last_updated"#,
        )
        .bind(character_id)
        .bind(state.energy)
        .bind(state.stress)
        .bind(&state.current_activity)
        .bind(state.last_updated.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }
}
