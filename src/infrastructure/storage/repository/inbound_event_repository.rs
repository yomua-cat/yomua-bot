//! 入站 OneBot 事件仓储的 SQLite 实现。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{FromRow, SqlitePool};

use crate::domain::inbound_event::{InboundEvent, InboundEventStatus};
use crate::domain::repository::InboundEventRepository;
use crate::error::RepositoryError;

#[derive(Debug, FromRow)]
struct InboundEventRow {
    ingress_seq: i64,
    raw_event: String,
    received_at: String,
    status: String,
    attempts: i32,
    last_error: Option<String>,
    processing_at: Option<String>,
    processed_at: Option<String>,
}

pub struct SqliteInboundEventRepository {
    pool: SqlitePool,
}

impl SqliteInboundEventRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn parse_time(value: &str) -> Result<DateTime<Utc>, RepositoryError> {
    super::timestamp::parse_timestamp(value)
}

fn parse_optional_time(value: Option<String>) -> Result<Option<DateTime<Utc>>, RepositoryError> {
    value.as_deref().map(parse_time).transpose()
}

fn parse_row(row: InboundEventRow) -> Result<InboundEvent, RepositoryError> {
    let status = row.status.parse().map_err(|error: String| {
        RepositoryError::Database(format!("无效的入站事件状态 {}: {error}", row.status))
    })?;
    Ok(InboundEvent {
        ingress_seq: row.ingress_seq,
        raw_event: row.raw_event,
        received_at: parse_time(&row.received_at)?,
        status,
        attempts: row.attempts,
        last_error: row.last_error,
        processing_at: parse_optional_time(row.processing_at)?,
        processed_at: parse_optional_time(row.processed_at)?,
    })
}

#[async_trait]
impl InboundEventRepository for SqliteInboundEventRepository {
    async fn next_pending_inbound_seq(&self) -> Result<Option<i64>, RepositoryError> {
        sqlx::query_scalar(
            "SELECT ingress_seq FROM inbound_events WHERE status = 'received' ORDER BY ingress_seq LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::from)
    }

    async fn insert_inbound_event(
        &self,
        raw_event: &str,
        received_at: DateTime<Utc>,
    ) -> Result<InboundEvent, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let pending_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM inbound_events WHERE status IN ('received', 'processing')",
        )
        .fetch_one(&mut *tx)
        .await?;
        if pending_count >= crate::domain::inbound_event::INBOUND_EVENT_MAX_PENDING {
            tx.rollback().await?;
            return Err(RepositoryError::InboundQueueFull);
        }
        let result = sqlx::query(
            r#"INSERT INTO inbound_events(raw_event, received_at, status)
               VALUES (?, ?, 'received')"#,
        )
        .bind(raw_event)
        .bind(received_at.to_rfc3339())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(InboundEvent {
            ingress_seq: result.last_insert_rowid(),
            raw_event: raw_event.to_owned(),
            received_at,
            status: InboundEventStatus::Received,
            attempts: 0,
            last_error: None,
            processing_at: None,
            processed_at: None,
        })
    }

    async fn claim_next_inbound_event(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<InboundEvent>, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let now_str = now.to_rfc3339();
        let row: Option<InboundEventRow> = sqlx::query_as(
            r#"SELECT ingress_seq, raw_event, received_at, status, attempts,
                      last_error, processing_at, processed_at
                 FROM inbound_events
                WHERE status = 'received'
                  AND NOT EXISTS (
                      SELECT 1 FROM inbound_events earlier
                       WHERE earlier.ingress_seq < inbound_events.ingress_seq
                         AND earlier.status NOT IN ('done', 'ignored', 'dead')
                  )
                ORDER BY ingress_seq ASC
                LIMIT 1"#,
        )
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(None);
        };
        let next_attempts = row.attempts.saturating_add(1);
        let updated = sqlx::query(
            r#"UPDATE inbound_events
                  SET status = 'processing', attempts = ?, processing_at = ?, last_error = NULL
                WHERE ingress_seq = ? AND status = 'received'"#,
        )
        .bind(next_attempts)
        .bind(&now_str)
        .bind(row.ingress_seq)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        if updated.rows_affected() != 1 {
            return Ok(None);
        }
        let mut event = parse_row(row)?;
        event.status = InboundEventStatus::Processing;
        event.attempts = next_attempts;
        event.processing_at = Some(now);
        event.last_error = None;
        Ok(Some(event))
    }

    async fn recover_inbound_events(&self, before: DateTime<Utc>) -> Result<u64, RepositoryError> {
        let result = sqlx::query(
            r#"UPDATE inbound_events
                  SET status = 'received', processing_at = NULL
                WHERE status = 'processing' AND processing_at IS NOT NULL AND processing_at <= ?"#,
        )
        .bind(before.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn mark_inbound_event(
        &self,
        ingress_seq: i64,
        status: InboundEventStatus,
        processed_at: DateTime<Utc>,
        last_error: Option<&str>,
    ) -> Result<bool, RepositoryError> {
        if !status.is_terminal() {
            return Err(RepositoryError::Internal(
                "入站事件只能标记为终态".to_string(),
            ));
        }
        let result = sqlx::query(
            r#"UPDATE inbound_events
                  SET status = ?, processed_at = ?, processing_at = NULL, last_error = ?
                WHERE ingress_seq = ? AND status = 'processing'"#,
        )
        .bind(status.as_str())
        .bind(processed_at.to_rfc3339())
        .bind(last_error)
        .bind(ingress_seq)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn retry_inbound_event(
        &self,
        ingress_seq: i64,
        last_error: &str,
    ) -> Result<bool, RepositoryError> {
        let result = sqlx::query(
            r#"UPDATE inbound_events
                  SET status = 'received', processing_at = NULL, last_error = ?
                WHERE ingress_seq = ? AND status = 'processing'"#,
        )
        .bind(last_error)
        .bind(ingress_seq)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn cleanup_inbound_events(
        &self,
        before: DateTime<Utc>,
        limit: i64,
    ) -> Result<u64, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let ids: Vec<i64> = sqlx::query_scalar(
            r#"SELECT ingress_seq FROM inbound_events
                WHERE status IN ('done', 'ignored', 'dead')
                  AND processed_at IS NOT NULL AND processed_at < ?
                ORDER BY processed_at ASC, ingress_seq ASC
                LIMIT ?"#,
        )
        .bind(before.to_rfc3339())
        .bind(limit.max(0))
        .fetch_all(&mut *tx)
        .await?;
        if ids.is_empty() {
            tx.rollback().await?;
            return Ok(0);
        }
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let query = format!("DELETE FROM inbound_events WHERE ingress_seq IN ({placeholders})");
        let mut delete = sqlx::query(&query);
        for id in ids {
            delete = delete.bind(id);
        }
        let deleted = delete.execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::repository::InboundEventRepository;
    use crate::infrastructure::storage::SqliteStorage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn inbound_events_are_inserted_and_claimed_in_sequence_order() {
        let storage = SqliteStorage::open_in_memory().await.unwrap();
        storage.migrate().await.unwrap();
        let repo = SqliteInboundEventRepository::new(storage.pool().clone());
        let received_at = Utc::now();
        let first = repo
            .insert_inbound_event("{\"id\":1}", received_at)
            .await
            .unwrap();
        let second = repo
            .insert_inbound_event("{\"id\":2}", received_at)
            .await
            .unwrap();
        assert!(first.ingress_seq < second.ingress_seq);
        let claimed = repo
            .claim_next_inbound_event(received_at)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.ingress_seq, first.ingress_seq);
        assert_eq!(claimed.attempts, 1);
        assert!(repo
            .claim_next_inbound_event(received_at)
            .await
            .unwrap()
            .is_none());
        repo.mark_inbound_event(
            first.ingress_seq,
            InboundEventStatus::Done,
            received_at,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            repo.claim_next_inbound_event(received_at)
                .await
                .unwrap()
                .unwrap()
                .ingress_seq,
            second.ingress_seq
        );
    }

    #[tokio::test]
    async fn inbound_events_resume_in_order_after_reopening_sqlite_file() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("inbound-events.sqlite");
        let path_string = path.to_string_lossy().into_owned();
        let received_at = Utc::now();
        let first_seq;
        {
            let storage = SqliteStorage::open(&path_string).await.unwrap();
            storage.migrate().await.unwrap();
            let repo = SqliteInboundEventRepository::new(storage.pool().clone());
            first_seq = repo
                .insert_inbound_event("first", received_at)
                .await
                .unwrap()
                .ingress_seq;
            repo.insert_inbound_event("second", received_at)
                .await
                .unwrap();
            let claimed = repo
                .claim_next_inbound_event(received_at)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(claimed.ingress_seq, first_seq);
            storage.close().await;
        }
        let storage = SqliteStorage::open(&path_string).await.unwrap();
        storage.migrate().await.unwrap();
        let repo = SqliteInboundEventRepository::new(storage.pool().clone());
        assert_eq!(repo.recover_inbound_events(Utc::now()).await.unwrap(), 1);
        assert_eq!(
            repo.claim_next_inbound_event(received_at)
                .await
                .unwrap()
                .unwrap()
                .ingress_seq,
            first_seq
        );
        repo.mark_inbound_event(first_seq, InboundEventStatus::Done, Utc::now(), None)
            .await
            .unwrap();
        assert_eq!(
            repo.claim_next_inbound_event(received_at)
                .await
                .unwrap()
                .unwrap()
                .ingress_seq,
            first_seq + 1
        );
    }

    #[tokio::test]
    async fn inbound_event_attempts_recover_and_terminal_cleanup_work() {
        let storage = SqliteStorage::open_in_memory().await.unwrap();
        storage.migrate().await.unwrap();
        let repo = SqliteInboundEventRepository::new(storage.pool().clone());
        let started_at = Utc::now() - chrono::Duration::minutes(2);
        let event = repo
            .insert_inbound_event("poison", started_at)
            .await
            .unwrap();
        let claimed = repo
            .claim_next_inbound_event(started_at)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.attempts, 1);
        assert_eq!(
            repo.recover_inbound_events(started_at + chrono::Duration::seconds(1))
                .await
                .unwrap(),
            1
        );
        let reclaimed = repo
            .claim_next_inbound_event(started_at + chrono::Duration::seconds(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reclaimed.attempts, 2);
        repo.mark_inbound_event(
            event.ingress_seq,
            InboundEventStatus::Dead,
            started_at,
            Some("脱敏错误"),
        )
        .await
        .unwrap();
        assert_eq!(
            repo.cleanup_inbound_events(Utc::now(), 10).await.unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn dead_inbound_event_does_not_block_following_event() {
        let storage = SqliteStorage::open_in_memory().await.unwrap();
        storage.migrate().await.unwrap();
        let repo = SqliteInboundEventRepository::new(storage.pool().clone());
        let now = Utc::now();
        let poison = repo.insert_inbound_event("poison", now).await.unwrap();
        let following = repo.insert_inbound_event("following", now).await.unwrap();
        let claimed = repo.claim_next_inbound_event(now).await.unwrap().unwrap();
        assert_eq!(claimed.ingress_seq, poison.ingress_seq);
        repo.mark_inbound_event(
            poison.ingress_seq,
            InboundEventStatus::Dead,
            now,
            Some("脱敏错误"),
        )
        .await
        .unwrap();
        assert_eq!(
            repo.claim_next_inbound_event(now)
                .await
                .unwrap()
                .unwrap()
                .ingress_seq,
            following.ingress_seq
        );
    }

    #[tokio::test]
    async fn inbound_event_queue_rejects_insert_at_capacity() {
        let storage = SqliteStorage::open_in_memory().await.unwrap();
        storage.migrate().await.unwrap();
        sqlx::query(
            r#"WITH RECURSIVE pending(n) AS (
                   SELECT 1
                   UNION ALL SELECT n + 1 FROM pending
                    WHERE n < ?
               )
               INSERT INTO inbound_events(raw_event, received_at, status)
               SELECT 'event', ?, 'received' FROM pending"#,
        )
        .bind(crate::domain::inbound_event::INBOUND_EVENT_MAX_PENDING)
        .bind(Utc::now().to_rfc3339())
        .execute(storage.pool())
        .await
        .unwrap();
        let repo = SqliteInboundEventRepository::new(storage.pool().clone());
        assert!(matches!(
            repo.insert_inbound_event("overflow", Utc::now()).await,
            Err(RepositoryError::InboundQueueFull)
        ));
    }
}
