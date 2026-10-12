//! BlacklistRepository 的 SQLite 实现。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;

use crate::domain::blacklist::{BlacklistEntry, BlacklistType, NewBlacklistEntry};
use crate::domain::repository::BlacklistRepository;
use crate::error::RepositoryError;

pub struct SqliteBlacklistRepository {
    pool: SqlitePool,
}

impl SqliteBlacklistRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BlacklistRepository for SqliteBlacklistRepository {
    async fn find_all(&self) -> Result<Vec<BlacklistEntry>, RepositoryError> {
        let rows: Vec<(i64, String, String, Option<String>, String, Option<String>)> =
            sqlx::query_as(
                r#"SELECT id, target, target_type, reason, created_at, expires_at
                   FROM blacklist_entries
                   ORDER BY created_at DESC"#,
            )
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(parse_blacklist_row).collect()
    }

    async fn find_by_target(
        &self,
        target: &str,
        target_type: BlacklistType,
    ) -> Result<Option<BlacklistEntry>, RepositoryError> {
        let target_type_str = target_type.as_str();
        let row: Option<(i64, String, String, Option<String>, String, Option<String>)> =
            sqlx::query_as(
                r#"SELECT id, target, target_type, reason, created_at, expires_at
                   FROM blacklist_entries
                   WHERE target = ? AND target_type = ?"#,
            )
            .bind(target)
            .bind(target_type_str)
            .fetch_optional(&self.pool)
            .await?;

        row.map(parse_blacklist_row).transpose()
    }

    async fn is_blacklisted(
        &self,
        target: &str,
        target_type: BlacklistType,
    ) -> Result<bool, RepositoryError> {
        let target_type_str = target_type.as_str();
        let count: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM blacklist_entries
               WHERE target = ? AND target_type = ?
                 AND (expires_at IS NULL OR expires_at > datetime('now'))"#,
        )
        .bind(target)
        .bind(target_type_str)
        .fetch_one(&self.pool)
        .await?;

        Ok(count > 0)
    }

    async fn insert(&self, entry: &NewBlacklistEntry) -> Result<i64, RepositoryError> {
        let target_type_str = entry.target_type.as_str();
        let expires_at_str = entry.expires_at.map(|dt| dt.to_rfc3339());

        let result = sqlx::query(
            r#"INSERT INTO blacklist_entries (target, target_type, reason, created_at, expires_at)
               VALUES (?, ?, ?, datetime('now'), ?)"#,
        )
        .bind(&entry.target)
        .bind(target_type_str)
        .bind(&entry.reason)
        .bind(expires_at_str)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(result.last_insert_rowid())
    }

    async fn update_reason(&self, id: i64, reason: Option<String>) -> Result<(), RepositoryError> {
        sqlx::query(r#"UPDATE blacklist_entries SET reason = ? WHERE id = ?"#)
            .bind(&reason)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }

    async fn set_expiration(
        &self,
        id: i64,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<(), RepositoryError> {
        let expires_at_str = expires_at.map(|dt| dt.to_rfc3339());
        sqlx::query(r#"UPDATE blacklist_entries SET expires_at = ? WHERE id = ?"#)
            .bind(expires_at_str)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }

    async fn delete(&self, id: i64) -> Result<(), RepositoryError> {
        sqlx::query("DELETE FROM blacklist_entries WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }

    async fn delete_by_target(
        &self,
        target: &str,
        target_type: BlacklistType,
    ) -> Result<(), RepositoryError> {
        let target_type_str = target_type.as_str();
        sqlx::query(r#"DELETE FROM blacklist_entries WHERE target = ? AND target_type = ?"#)
            .bind(target)
            .bind(target_type_str)
            .execute(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }
}

type BlacklistRow = (i64, String, String, Option<String>, String, Option<String>);

fn parse_blacklist_row(row: BlacklistRow) -> Result<BlacklistEntry, RepositoryError> {
    let (id, target, target_type_str, reason, created_at_str, expires_at_str) = row;

    let target_type = BlacklistType::parse(&target_type_str).ok_or_else(|| {
        RepositoryError::Database(format!("unknown target_type: {target_type_str}"))
    })?;

    let created_at = super::timestamp::parse_timestamp(&created_at_str)?;
    let expires_at = expires_at_str
        .map(|s| DateTime::parse_from_rfc3339(&s).map(|dt| dt.with_timezone(&Utc)))
        .transpose()
        .map_err(|e| RepositoryError::Database(format!("invalid expires_at: {e}")))?;

    Ok(BlacklistEntry {
        id,
        target,
        target_type,
        reason,
        created_at,
        expires_at,
    })
}
