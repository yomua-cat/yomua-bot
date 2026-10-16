//! OutboxRepository 的 SQLite 实现。

use async_trait::async_trait;
use chrono::{Duration, Utc};
use sqlx::{FromRow, SqlitePool};

use crate::domain::outbox::{OutboxEvent, OutboxStatus};
use crate::domain::repository::OutboxRepository;
use crate::error::RepositoryError;

/// 进程在持有租约期间崩溃时，最多允许重新领取的次数。
///
/// 该值必须与 OutboxDispatcher 的默认上限一致。恢复流程在同一事务中把
/// 已达到上限的过期事件置为 dead_letter，避免它们再次阻塞同一会话的后续事件。
const DEFAULT_MAX_RETRY_ATTEMPTS: i32 = 3;

/// P0: OutboxRow 使用结构体而不是元组，因为 sqlx 的 FromRow
/// 只支持最多 16 个元素的元组，而 outbox_events 有 17 列。
#[derive(Debug, FromRow)]
struct OutboxRow {
    id: i64,
    event_uuid: String,
    entity_id: i64,
    entity_type: String,
    event_type: String,
    event_version: i32,
    payload: String,
    ordering_key: String,
    ordering_seq: i64,
    created_at: String,
    available_at: String,
    status: String,
    attempts: i32,
    last_error: Option<String>,
    locked_until: Option<String>,
    processed_at: Option<String>,
    lease_token: Option<String>,
}

pub struct SqliteOutboxRepository {
    pool: SqlitePool,
}

impl SqliteOutboxRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn parse_outbox_row(row: OutboxRow) -> Result<OutboxEvent, RepositoryError> {
    let created_at = super::timestamp::parse_timestamp(&row.created_at)?;
    let available_at = super::timestamp::parse_timestamp(&row.available_at)?;
    let locked_until = row
        .locked_until
        .as_deref()
        .map(super::timestamp::parse_timestamp)
        .transpose()?;
    let processed_at = row
        .processed_at
        .as_deref()
        .map(super::timestamp::parse_timestamp)
        .transpose()?;

    let status: OutboxStatus = row.status.parse().map_err(|e| {
        RepositoryError::Database(format!("无效的 outbox 状态 {}: {}", row.status, e))
    })?;

    Ok(OutboxEvent {
        id: row.id,
        event_uuid: row.event_uuid,
        entity_id: row.entity_id,
        entity_type: row.entity_type,
        event_type: row.event_type,
        event_version: row.event_version,
        payload: row.payload,
        ordering_key: row.ordering_key,
        ordering_seq: row.ordering_seq,
        created_at,
        available_at,
        status,
        attempts: row.attempts,
        last_error: row.last_error,
        locked_until,
        processed_at,
        lease_token: row.lease_token,
    })
}

#[async_trait]
impl OutboxRepository for SqliteOutboxRepository {
    async fn renew_lease(
        &self,
        event_uuid: &str,
        token: &str,
        duration_secs: i64,
    ) -> Result<bool, RepositoryError> {
        let now = Utc::now();
        let result=sqlx::query("UPDATE outbox_events SET locked_until=? WHERE event_uuid=? AND status='processing' AND lease_token=? AND locked_until > ?")
          .bind((now+Duration::seconds(duration_secs.max(1))).to_rfc3339()).bind(event_uuid).bind(token).bind(now.to_rfc3339()).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }
    async fn claim_next_event(
        &self,
        lease_duration_secs: i64,
    ) -> Result<Option<OutboxEvent>, RepositoryError> {
        let mut tx = self
            .pool
            .acquire()
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let mut tx = sqlx::Acquire::begin(&mut *tx)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let now = Utc::now();
        let now_str = now.to_rfc3339();

        // 查找第一个可 claim 的事件：
        // 1. status IN ('pending', 'retry_wait')
        // 2. available_at <= now
        // 3. locked_until IS NULL OR locked_until <= now
        // 4. 前一个 ordering_seq 已完成或不存在
        //
        // 使用子查询找到每个 ordering_key 的最大已完成 seq，
        // 然后只选择 seq = max_completed_seq + 1 的事件
        let row: Option<OutboxRow> = sqlx::query_as(
            r#"
            SELECT o.id, o.event_uuid, o.entity_id, o.entity_type, o.event_type,
                   o.event_version, o.payload, o.ordering_key, o.ordering_seq,
                   o.created_at, o.available_at, o.status, o.attempts,
                   o.last_error, o.locked_until, o.processed_at, o.lease_token
            FROM outbox_events o
            WHERE o.status IN ('pending', 'retry_wait')
              AND o.available_at <= ?
              AND (o.locked_until IS NULL OR o.locked_until <= ?)
              AND NOT EXISTS (
                  SELECT 1 FROM outbox_events earlier
                  WHERE earlier.ordering_key=o.ordering_key
                  AND earlier.ordering_seq < o.ordering_seq
                  AND earlier.status NOT IN ('completed','dead_letter')
              )
            ORDER BY o.available_at, o.id
            LIMIT 1
            "#,
        )
        .bind(&now_str)
        .bind(&now_str)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        if row.is_none() {
            // 无可用事件，回滚事务
            tx.rollback()
                .await
                .map_err(|e| RepositoryError::Database(e.to_string()))?;
            return Ok(None);
        }

        let row = row.unwrap();
        let event = parse_outbox_row(row)?;

        // P0: 生成唯一的 lease_token 用于 ownership fencing
        let lease_token = uuid::Uuid::new_v4().to_string();

        // 原子性地 claim：更新 status、locked_until 和 lease_token
        let locked_until = (now + Duration::seconds(lease_duration_secs)).to_rfc3339();
        let attempts = event.attempts + 1;

        let updated = sqlx::query(
            r#"
            UPDATE outbox_events
            SET status = 'processing',
                locked_until = ?,
                attempts = ?,
                lease_token = ?
            WHERE id = ? AND status IN ('pending', 'retry_wait')
            "#,
        )
        .bind(&locked_until)
        .bind(attempts)
        .bind(&lease_token)
        .bind(event.id)
        .execute(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        if updated.rows_affected() == 0 {
            // 竞态：另一个 dispatcher 已经 claim 了
            return Ok(None);
        }

        // 返回 claim 的事件（带更新后的状态和 lease_token）
        let mut updated_event = event.clone();
        updated_event.status = OutboxStatus::Processing;
        updated_event.attempts = attempts;
        updated_event.locked_until = Some(
            chrono::DateTime::parse_from_rfc3339(&locked_until)
                .unwrap()
                .with_timezone(&Utc),
        );
        updated_event.lease_token = Some(lease_token);

        Ok(Some(updated_event))
    }

    async fn mark_completed(
        &self,
        event_uuid: &str,
        lease_token: &str,
    ) -> Result<(), RepositoryError> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();

        // P0: 使用 lease_token 进行真正的 ownership fencing。
        // stale worker（token 不匹配）无法完成另一个 worker 的 lease。
        let result = sqlx::query(
            r#"
            UPDATE outbox_events
            SET status = 'completed',
                locked_until = NULL,
                lease_token = NULL,
                processed_at = ?
            WHERE event_uuid = ?
              AND status = 'processing'
              AND lease_token = ?
            "#,
        )
        .bind(now_str.clone())
        .bind(event_uuid)
        .bind(lease_token)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        // rows_affected == 0 表示：
        // 1. 事件不在 processing 状态（已被完成/重试/标记）
        // 2. lease_token 不匹配（被新 worker claim）
        // 3. 事件已被另一个 worker 完成
        if result.rows_affected() == 0 {
            tracing::debug!(
                target: "outbox",
                event_uuid = %event_uuid,
                "completion 被跳过：stale worker 或事件状态已改变"
            );
        }

        Ok(())
    }

    async fn mark_retry_wait(
        &self,
        event_uuid: &str,
        lease_token: &str,
        error: &str,
        next_available_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), RepositoryError> {
        let next_str = next_available_at.to_rfc3339();

        // P0: 使用 lease_token 进行真正的 ownership fencing
        sqlx::query(
            r#"
            UPDATE outbox_events
            SET status = 'retry_wait',
                locked_until = NULL,
                lease_token = NULL,
                last_error = ?,
                available_at = ?
            WHERE event_uuid = ?
              AND status = 'processing'
              AND lease_token = ?
            "#,
        )
        .bind(error)
        .bind(&next_str)
        .bind(event_uuid)
        .bind(lease_token)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }

    async fn mark_dead_letter(
        &self,
        event_uuid: &str,
        lease_token: &str,
        error: &str,
    ) -> Result<(), RepositoryError> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();

        // P0: 使用 lease_token 进行真正的 ownership fencing
        sqlx::query(
            r#"
            UPDATE outbox_events
            SET status = 'dead_letter',
                locked_until = NULL,
                lease_token = NULL,
                last_error = ?,
                processed_at = ?
            WHERE event_uuid = ?
              AND status = 'processing'
              AND lease_token = ?
            "#,
        )
        .bind(error)
        .bind(&now_str)
        .bind(event_uuid)
        .bind(lease_token)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }

    async fn max_completed_seq(&self, ordering_key: &str) -> Result<Option<i64>, RepositoryError> {
        let row: Option<(i64,)> = sqlx::query_as(
            r#"
            SELECT MAX(ordering_seq)
            FROM outbox_events
            WHERE ordering_key = ? AND status IN ('completed', 'dead_letter')
            "#,
        )
        .bind(ordering_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(row.map(|(v,)| v))
    }

    async fn max_seq(&self, ordering_key: &str) -> Result<Option<i64>, RepositoryError> {
        let row: Option<(i64,)> = sqlx::query_as(
            r#"
            SELECT MAX(ordering_seq)
            FROM outbox_events
            WHERE ordering_key = ?
            "#,
        )
        .bind(ordering_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(row.map(|(v,)| v))
    }

    async fn recover_expired_processing(&self) -> Result<i64, RepositoryError> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        // attempts 统计成功领取 processing 的次数，恢复不能把它清零。
        // 达到上限的任务必须在本事务中进入 dead_letter，否则同一 ordering_key
        // 会被永久卡住，后续事件也无法继续。
        let dead_lettered = sqlx::query(
            r#"
            UPDATE outbox_events
            SET status = 'dead_letter',
                last_error = COALESCE(last_error, '租约过期且已达到最大尝试次数'),
                locked_until = NULL,
                lease_token = NULL,
                processed_at = ?
            WHERE status = 'processing'
              AND locked_until <= ?
              AND attempts >= ?
            "#,
        )
        .bind(&now)
        .bind(&now)
        .bind(DEFAULT_MAX_RETRY_ATTEMPTS)
        .execute(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let pending = sqlx::query(
            r#"
            UPDATE outbox_events
            SET status = 'pending',
                locked_until = NULL,
                lease_token = NULL
            WHERE status = 'processing'
              AND locked_until <= ?
              AND attempts < ?
            "#,
        )
        .bind(&now)
        .bind(DEFAULT_MAX_RETRY_ATTEMPTS)
        .execute(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok((dead_lettered.rows_affected() + pending.rows_affected()) as i64)
    }

    async fn find_by_uuid(&self, event_uuid: &str) -> Result<Option<OutboxEvent>, RepositoryError> {
        let row: Option<OutboxRow> = sqlx::query_as(
            r#"
            SELECT id, event_uuid, entity_id, entity_type, event_type,
                   event_version, payload, ordering_key, ordering_seq,
                   created_at, available_at, status, attempts,
                   last_error, locked_until, processed_at, lease_token
            FROM outbox_events
            WHERE event_uuid = ?
            "#,
        )
        .bind(event_uuid)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        row.map(parse_outbox_row).transpose()
    }

    async fn list_terminal_events(&self, limit: i64) -> Result<Vec<OutboxEvent>, RepositoryError> {
        let rows: Vec<OutboxRow> = sqlx::query_as(
            r#"
            SELECT id, event_uuid, entity_id, entity_type, event_type,
                   event_version, payload, ordering_key, ordering_seq,
                   created_at, available_at, status, attempts,
                   last_error, locked_until, processed_at, lease_token
            FROM outbox_events
            WHERE status IN ('completed', 'dead_letter')
            ORDER BY processed_at DESC
            LIMIT ?
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        rows.into_iter().map(parse_outbox_row).collect()
    }

    async fn processing_count(&self) -> Result<i64, RepositoryError> {
        let row: (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(*) FROM outbox_events WHERE status = 'processing'
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(row.0)
    }

    async fn pending_count(&self) -> Result<i64, RepositoryError> {
        let row: (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(*) FROM outbox_events
            WHERE status IN ('pending', 'retry_wait')
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(row.0)
    }

    /// 添加 outbox 事件（用于测试或初始化）。
    async fn add_outbox_event(&self, event: OutboxEvent) -> Result<(), RepositoryError> {
        let created_at = event.created_at.to_rfc3339();
        let available_at = event.available_at.to_rfc3339();
        let processed_at = event.processed_at.map(|dt| dt.to_rfc3339());

        sqlx::query(
            r#"
            INSERT INTO outbox_events (
                event_uuid, entity_id, entity_type, event_type, event_version,
                payload, ordering_key, ordering_seq, created_at, available_at,
                status, attempts, last_error, locked_until, processed_at, lease_token
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&event.event_uuid)
        .bind(event.entity_id)
        .bind(&event.entity_type)
        .bind(&event.event_type)
        .bind(event.event_version)
        .bind(&event.payload)
        .bind(&event.ordering_key)
        .bind(event.ordering_seq)
        .bind(&created_at)
        .bind(&available_at)
        .bind(event.status.to_string())
        .bind(event.attempts)
        .bind(&event.last_error)
        .bind(event.locked_until.map(|dt| dt.to_rfc3339()))
        .bind(&processed_at)
        .bind(&event.lease_token)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }

    /// 强制使指定事件的 lease 过期（用于测试）。
    /// 直接将 locked_until 设置为过去的时间。
    async fn force_expire_lease(&self, event_uuid: &str) -> Result<(), RepositoryError> {
        let past = (Utc::now() - Duration::seconds(60)).to_rfc3339();
        sqlx::query(
            r#"
            UPDATE outbox_events
            SET locked_until = ?
            WHERE event_uuid = ?
            "#,
        )
        .bind(&past)
        .bind(event_uuid)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(())
    }
}
