//! 主动意图与回复批次的 SQLite 仓储实现。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;

use crate::domain::autonomy::{AutonomyIntent, AutonomyIntentStatus, ReplyBatch, ReplyBatchStatus};
use crate::domain::repository::AutonomyRepository;
use crate::error::RepositoryError;

pub struct SqliteAutonomyRepository {
    pool: SqlitePool,
}

impl SqliteAutonomyRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AutonomyRepository for SqliteAutonomyRepository {
    async fn insert_autonomy_intent(
        &self,
        intent: &AutonomyIntent,
    ) -> Result<i64, RepositoryError> {
        let result = sqlx::query(
            r#"INSERT INTO autonomy_intents
                (character_id, conversation_id, status, not_before, cooldown_key,
                 quota_reservation, source_snapshot, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(intent.character_id)
        .bind(intent.conversation_id)
        .bind(intent.status.as_str())
        .bind(intent.not_before.to_rfc3339())
        .bind(&intent.cooldown_key)
        .bind(i64::from(intent.quota_reservation))
        .bind(
            serde_json::to_string(&intent.source_snapshot)
                .map_err(|e| RepositoryError::Internal(format!("主动意图快照序列化失败：{e}")))?,
        )
        .bind(intent.created_at.to_rfc3339())
        .bind(intent.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(result.last_insert_rowid())
    }

    async fn find_pending_autonomy_intents(
        &self,
        not_before: DateTime<Utc>,
        limit: i64,
    ) -> Result<Vec<AutonomyIntent>, RepositoryError> {
        let rows: Vec<IntentRow> = sqlx::query_as(
            r#"SELECT id, character_id, conversation_id, status, not_before,
                      cooldown_key, quota_reservation, source_snapshot, created_at, updated_at
                 FROM autonomy_intents
                WHERE status = 'pending' AND not_before <= ?
                ORDER BY not_before ASC, id ASC
                LIMIT ?"#,
        )
        .bind(not_before.to_rfc3339())
        .bind(limit.max(0))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(parse_intent_row).collect()
    }

    async fn update_autonomy_intent_status(
        &self,
        id: i64,
        expected: AutonomyIntentStatus,
        status: AutonomyIntentStatus,
        updated_at: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let result = sqlx::query(
            "UPDATE autonomy_intents SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
        )
        .bind(status.as_str())
        .bind(updated_at.to_rfc3339())
        .bind(id)
        .bind(expected.as_str())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn insert_reply_batch(&self, batch: &ReplyBatch) -> Result<i64, RepositoryError> {
        let message_ids = serde_json::to_string(&batch.message_ids)
            .map_err(|e| RepositoryError::Internal(format!("回复批次消息 ID 序列化失败：{e}")))?;
        let result = sqlx::query(
            r#"INSERT INTO reply_batches
                (conversation_id, start_sequence, end_sequence, message_ids,
                 trigger_event_uuid, context_hash, status, lease_token, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(trigger_event_uuid) DO NOTHING"#,
        )
        .bind(batch.conversation_id)
        .bind(batch.start_sequence)
        .bind(batch.end_sequence)
        .bind(message_ids)
        .bind(&batch.trigger_event_uuid)
        .bind(&batch.context_hash)
        .bind(batch.status.as_str())
        .bind(&batch.lease_token)
        .bind(batch.created_at.to_rfc3339())
        .bind(batch.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(result.last_insert_rowid());
        }
        sqlx::query_scalar("SELECT id FROM reply_batches WHERE trigger_event_uuid = ?")
            .bind(&batch.trigger_event_uuid)
            .fetch_one(&self.pool)
            .await
            .map_err(RepositoryError::from)
    }

    async fn find_reply_batch_by_trigger(
        &self,
        trigger_event_uuid: &str,
    ) -> Result<Option<ReplyBatch>, RepositoryError> {
        let row: Option<BatchRow> = sqlx::query_as(
            r#"SELECT id, conversation_id, start_sequence, end_sequence, message_ids,
                      trigger_event_uuid, context_hash, status, lease_token, created_at, updated_at
                 FROM reply_batches WHERE trigger_event_uuid = ?"#,
        )
        .bind(trigger_event_uuid)
        .fetch_optional(&self.pool)
        .await?;
        row.map(parse_batch_row).transpose()
    }

    async fn claim_reply_batch(
        &self,
        id: i64,
        lease_token: &str,
        updated_at: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let result = sqlx::query(
            r#"UPDATE reply_batches
                  SET status = 'processing', lease_token = ?, updated_at = ?
                WHERE id = ? AND status = 'frozen' AND lease_token IS NULL"#,
        )
        .bind(lease_token)
        .bind(updated_at.to_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn update_reply_batch_status_owned(
        &self,
        id: i64,
        lease_token: &str,
        expected: ReplyBatchStatus,
        status: ReplyBatchStatus,
        updated_at: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let result = sqlx::query(
            "UPDATE reply_batches SET status = ?, updated_at = ? WHERE id = ? AND status = ? AND lease_token = ?",
        )
        .bind(status.as_str())
        .bind(updated_at.to_rfc3339())
        .bind(id)
        .bind(expected.as_str())
        .bind(lease_token)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }
}

type IntentRow = (
    i64,
    i64,
    i64,
    String,
    String,
    String,
    i64,
    String,
    String,
    String,
);
type BatchRow = (
    i64,
    i64,
    i64,
    i64,
    String,
    String,
    String,
    String,
    Option<String>,
    String,
    String,
);

fn parse_intent_row(row: IntentRow) -> Result<AutonomyIntent, RepositoryError> {
    let (
        id,
        character_id,
        conversation_id,
        status,
        not_before,
        cooldown_key,
        quota_reservation,
        source_snapshot,
        created_at,
        updated_at,
    ) = row;
    Ok(AutonomyIntent {
        id,
        character_id,
        conversation_id,
        status: status
            .parse()
            .map_err(|e: String| RepositoryError::Internal(format!("主动意图状态无效：{e}")))?,
        not_before: super::timestamp::parse_timestamp(&not_before)?,
        cooldown_key,
        quota_reservation: quota_reservation.max(0) as u32,
        source_snapshot: serde_json::from_str(&source_snapshot)
            .map_err(|e| RepositoryError::Internal(format!("主动意图快照无效：{e}")))?,
        created_at: super::timestamp::parse_timestamp(&created_at)?,
        updated_at: super::timestamp::parse_timestamp(&updated_at)?,
    })
}

fn parse_batch_row(row: BatchRow) -> Result<ReplyBatch, RepositoryError> {
    let (
        id,
        conversation_id,
        start_sequence,
        end_sequence,
        message_ids,
        trigger_event_uuid,
        context_hash,
        status,
        lease_token,
        created_at,
        updated_at,
    ) = row;
    Ok(ReplyBatch {
        id,
        conversation_id,
        start_sequence,
        end_sequence,
        message_ids: serde_json::from_str(&message_ids)
            .map_err(|e| RepositoryError::Internal(format!("回复批次消息 ID 无效：{e}")))?,
        trigger_event_uuid,
        context_hash,
        status: status
            .parse()
            .map_err(|e: String| RepositoryError::Internal(format!("回复批次状态无效：{e}")))?,
        lease_token,
        created_at: super::timestamp::parse_timestamp(&created_at)?,
        updated_at: super::timestamp::parse_timestamp(&updated_at)?,
    })
}
