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
        let row: Option<(Option<String>, Option<String>, i64, String)> = sqlx::query_as(
            r#"SELECT last_proactive_at, proactive_count_date, proactive_count, last_updated FROM behavior_states
             WHERE character_id = ? AND conversation_id = ?"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((last_proactive_at, proactive_count_date, proactive_count, last_updated)) => {
                let last_proactive_at = match last_proactive_at {
                    Some(s) => Some(super::timestamp::parse_timestamp(&s)?),
                    None => None,
                };
                let proactive_count_date = proactive_count_date
                    .map(|s| {
                        chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(|e| {
                            RepositoryError::Database(format!("主动额度日期无效：{e}"))
                        })
                    })
                    .transpose()?;
                let last_updated = super::timestamp::parse_timestamp(&last_updated)?;
                Ok(Some(BehaviorState {
                    last_proactive_at,
                    proactive_count_date,
                    proactive_count: proactive_count.max(0) as u32,
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
            r#"INSERT INTO behavior_states (character_id, conversation_id, last_proactive_at, proactive_count_date, proactive_count, last_updated)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(character_id, conversation_id) DO UPDATE SET
                last_proactive_at = excluded.last_proactive_at,
                proactive_count_date = excluded.proactive_count_date,
                proactive_count = excluded.proactive_count,
                last_updated = excluded.last_updated"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(state.last_proactive_at.map(|t| t.to_rfc3339()))
        .bind(state.proactive_count_date.map(|date| date.to_string()))
        .bind(i64::from(state.proactive_count))
        .bind(state.last_updated.to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn try_reserve_proactive(
        &self,
        character_id: i64,
        conversation_id: i64,
        now: chrono::DateTime<chrono::Utc>,
        cooldown: chrono::Duration,
        daily_limit: u32,
    ) -> Result<bool, RepositoryError> {
        self.try_reserve_proactive_at_date(
            character_id,
            conversation_id,
            now,
            now.date_naive(),
            cooldown,
            daily_limit,
        )
        .await
    }

    async fn try_reserve_proactive_at_date(
        &self,
        character_id: i64,
        conversation_id: i64,
        now: chrono::DateTime<chrono::Utc>,
        local_date: chrono::NaiveDate,
        cooldown: chrono::Duration,
        daily_limit: u32,
    ) -> Result<bool, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let row: Option<(Option<String>, Option<String>, i64)> = sqlx::query_as(
            r#"SELECT last_proactive_at, proactive_count_date, proactive_count
                 FROM behavior_states
                WHERE character_id = ? AND conversation_id = ?"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .fetch_optional(&mut *tx)
        .await?;

        let today = local_date;
        let (last, date, count) = match row {
            Some((last, date, count)) => {
                let last = last
                    .map(|value| super::timestamp::parse_timestamp(&value))
                    .transpose()?;
                let date = date
                    .map(|value| {
                        chrono::NaiveDate::parse_from_str(&value, "%Y-%m-%d").map_err(|e| {
                            RepositoryError::Database(format!("主动额度日期无效：{e}"))
                        })
                    })
                    .transpose()?;
                (last, date, count.max(0) as u32)
            }
            None => (None, None, 0),
        };
        let count = if date == Some(today) { count } else { 0 };
        if count >= daily_limit
            || last.is_some_and(|value| now.signed_duration_since(value) < cooldown)
        {
            tx.rollback().await?;
            return Ok(false);
        }

        sqlx::query(
            r#"INSERT INTO behavior_states
                    (character_id, conversation_id, last_proactive_at,
                     proactive_count_date, proactive_count, last_updated)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(character_id, conversation_id) DO UPDATE SET
                    last_proactive_at = excluded.last_proactive_at,
                    proactive_count_date = excluded.proactive_count_date,
                    proactive_count = excluded.proactive_count,
                    last_updated = excluded.last_updated"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(now.to_rfc3339())
        .bind(today.to_string())
        .bind(i64::from(count + 1))
        .bind(now.to_rfc3339())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }
}
