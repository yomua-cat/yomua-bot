//! PluginDataRepository 的 SQLite 实现。
//!
//! 表结构（MIGRATION_001 已建）：
//! `plugin_data(plugin_name, key, value, created_at, updated_at, PRIMARY KEY(plugin_name, key))`。
//! `value` 列存 JSON 文本，读回时解析为 `serde_json::Value`。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::repository::PluginDataRepository;
use crate::error::RepositoryError;

pub struct SqlitePluginDataRepository {
    pool: SqlitePool,
}

impl SqlitePluginDataRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PluginDataRepository for SqlitePluginDataRepository {
    async fn get(
        &self,
        plugin_name: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>, RepositoryError> {
        let value_json: Option<String> = sqlx::query_scalar(
            r#"SELECT value FROM plugin_data WHERE plugin_name = ? AND key = ?"#,
        )
        .bind(plugin_name)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        match value_json {
            Some(s) => serde_json::from_str(&s).map(Some).map_err(|e| {
                RepositoryError::Database(format!("plugin_data 值 JSON 解析失败：{e}"))
            }),
            None => Ok(None),
        }
    }

    async fn set(
        &self,
        plugin_name: &str,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), RepositoryError> {
        let value_json = serde_json::to_string(value).map_err(|e| {
            RepositoryError::Database(format!("plugin_data 值 JSON 序列化失败：{e}"))
        })?;

        sqlx::query(
            r#"INSERT INTO plugin_data (plugin_name, key, value)
             VALUES (?, ?, ?)
             ON CONFLICT(plugin_name, key) DO UPDATE SET
                value = excluded.value,
                updated_at = datetime('now')"#,
        )
        .bind(plugin_name)
        .bind(key)
        .bind(&value_json)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }

    async fn delete(&self, plugin_name: &str, key: &str) -> Result<(), RepositoryError> {
        sqlx::query("DELETE FROM plugin_data WHERE plugin_name = ? AND key = ?")
            .bind(plugin_name)
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(())
    }

    async fn list_keys(&self, plugin_name: &str) -> Result<Vec<String>, RepositoryError> {
        let keys: Vec<String> =
            sqlx::query_scalar(r#"SELECT key FROM plugin_data WHERE plugin_name = ? ORDER BY key"#)
                .bind(plugin_name)
                .fetch_all(&self.pool)
                .await?;
        Ok(keys)
    }
}
