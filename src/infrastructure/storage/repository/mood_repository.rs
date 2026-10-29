//! MoodRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::emotion::Mood;
use crate::domain::repository::MoodRepository;
use crate::error::RepositoryError;

pub struct SqliteMoodRepository {
    pool: SqlitePool,
}

impl SqliteMoodRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl MoodRepository for SqliteMoodRepository {
    async fn find_by_character_and_conversation(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<Option<Mood>, RepositoryError> {
        let row: Option<(f64, String)> = sqlx::query_as(
            r#"SELECT value, last_updated FROM moods
             WHERE character_id = ? AND conversation_id = ?"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((value, last_updated)) => {
                let last_updated = super::timestamp::parse_timestamp(&last_updated)?;
                Ok(Some(Mood {
                    value,
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
        mood: &Mood,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"INSERT INTO moods (character_id, conversation_id, value, last_updated)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(character_id, conversation_id) DO UPDATE SET
                value = excluded.value,
                last_updated = excluded.last_updated"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(mood.value)
        .bind(mood.last_updated.to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
