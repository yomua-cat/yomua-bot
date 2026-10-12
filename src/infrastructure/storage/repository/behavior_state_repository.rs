//! BehaviorStateRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::character::BehaviorState;
use crate::domain::repository::BehaviorStateRepository;
use crate::error::RepositoryError;

pub struct SqliteBehaviorStateRepository {
    pool: SqlitePool,
}

impl SqliteBehaviorStateRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BehaviorStateRepository for SqliteBehaviorStateRepository {
    async fn find_by_character_and_conversation(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<Option<BehaviorState>, RepositoryError> {
        let row: Option<(Option<String>, String)> = sqlx::query_as(
            r#"SELECT last_proactive_at, last_updated FROM behavior_states
             WHERE character_id = ? AND conversation_id = ?"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((last_proactive_at, last_updated)) => {
                let last_proactive_at = match last_proactive_at {
                    Some(s) => Some(super::timestamp::parse_timestamp(&s)?),
                    None => None,
                };
                let last_updated = super::timestamp::parse_timestamp(&last_updated)?;
                Ok(Some(BehaviorState {
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
        conversation_id: i64,
        state: &BehaviorState,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"INSERT INTO behavior_states (character_id, conversation_id, last_proactive_at, last_updated)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(character_id, conversation_id) DO UPDATE SET
                last_proactive_at = excluded.last_proactive_at,
                last_updated = excluded.last_updated"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(state.last_proactive_at.map(|t| t.to_rfc3339()))
        .bind(state.last_updated.to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
