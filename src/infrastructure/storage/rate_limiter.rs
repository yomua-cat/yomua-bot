//! SQLite 普通回复限额实现。

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sqlx::SqlitePool;

use crate::application::rate_limiter::{effective_limits, ResponseRateLimiter};
use crate::domain::character::ResponseRateLimitPolicy;
use crate::error::RepositoryError;

pub struct SqliteResponseRateLimiter {
    pool: SqlitePool,
}

impl SqliteResponseRateLimiter {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ResponseRateLimiter for SqliteResponseRateLimiter {
    async fn try_reserve(
        &self,
        character_id: i64,
        conversation_id: i64,
        participant_id: Option<i64>,
        policy: &ResponseRateLimitPolicy,
        is_mentioned: bool,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let (user_limit, session_limit) = effective_limits(policy, is_mentioned);
        if user_limit.is_none() && session_limit.is_none() {
            return Ok(true);
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| RepositoryError::Database(format!("开始回复限额事务失败: {e}")))?;
        let now_text = now.to_rfc3339();
        let user_boundary = (now - Duration::minutes(1)).to_rfc3339();
        let session_boundary = (now - Duration::hours(1)).to_rfc3339();

        let session_count: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM response_rate_limit_events
               WHERE character_id = ? AND conversation_id = ? AND scope = 'session'
                 AND occurred_at > ?"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(&session_boundary)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(format!("查询会话回复限额失败: {e}")))?;
        if session_limit.is_some_and(|limit| session_count >= i64::from(limit)) {
            tx.rollback().await.ok();
            return Ok(false);
        }

        if let (Some(limit), Some(participant_id)) = (user_limit, participant_id) {
            let user_count: i64 = sqlx::query_scalar(
                r#"SELECT COUNT(*) FROM response_rate_limit_events
                   WHERE character_id = ? AND conversation_id = ? AND scope = 'user'
                     AND participant_id = ? AND occurred_at > ?"#,
            )
            .bind(character_id)
            .bind(conversation_id)
            .bind(participant_id)
            .bind(&user_boundary)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| RepositoryError::Database(format!("查询用户回复限额失败: {e}")))?;
            if user_count >= i64::from(limit) {
                tx.rollback().await.ok();
                return Ok(false);
            }
        }

        if session_limit.is_some() {
            sqlx::query(
                r#"INSERT INTO response_rate_limit_events
                   (character_id, conversation_id, participant_id, scope, occurred_at)
                   VALUES (?, ?, NULL, 'session', ?)"#,
            )
            .bind(character_id)
            .bind(conversation_id)
            .bind(&now_text)
            .execute(&mut *tx)
            .await
            .map_err(|e| RepositoryError::Database(format!("写入会话回复限额失败: {e}")))?;
        }
        if user_limit.is_some() {
            if let Some(participant_id) = participant_id {
                sqlx::query(
                    r#"INSERT INTO response_rate_limit_events
                       (character_id, conversation_id, participant_id, scope, occurred_at)
                       VALUES (?, ?, ?, 'user', ?)"#,
                )
                .bind(character_id)
                .bind(conversation_id)
                .bind(participant_id)
                .bind(&now_text)
                .execute(&mut *tx)
                .await
                .map_err(|e| RepositoryError::Database(format!("写入用户回复限额失败: {e}")))?;
            }
        }

        // 清理同一绑定已失效的旧事件，保持长期运行时表大小有界。
        sqlx::query(
            "DELETE FROM response_rate_limit_events WHERE character_id = ? AND conversation_id = ? AND occurred_at <= ?",
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(&session_boundary)
        .execute(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(format!("清理回复限额事件失败: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| RepositoryError::Database(format!("提交回复限额事务失败: {e}")))?;
        Ok(true)
    }
}
