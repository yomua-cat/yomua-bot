//! ConversationStateRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::character::ConversationState;
use crate::domain::repository::ConversationStateRepository;
use crate::error::RepositoryError;

pub struct SqliteConversationStateRepository {
    pool: SqlitePool,
}

impl SqliteConversationStateRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ConversationStateRepository for SqliteConversationStateRepository {
    async fn find_by_character_and_conversation(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<Option<ConversationState>, RepositoryError> {
        let row: Option<(f64, f64, String)> = sqlx::query_as(
            r#"SELECT energy, stress, last_updated FROM conversation_states
             WHERE character_id = ? AND conversation_id = ?"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((energy, stress, last_updated)) => {
                let last_updated = super::timestamp::parse_timestamp(&last_updated)?;
                Ok(Some(ConversationState {
                    energy,
                    stress,
                    last_updated,
                }))
            }
            None => Ok(None),
        }
    }

    async fn upsert(
        &self,
        character_id: i64,
        conversation_id: i64,
        state: &ConversationState,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"INSERT INTO conversation_states (character_id, conversation_id, energy, stress, last_updated)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(character_id, conversation_id) DO UPDATE SET
                energy = excluded.energy,
                stress = excluded.stress,
                last_updated = excluded.last_updated"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(state.energy)
        .bind(state.stress)
        .bind(state.last_updated.to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
