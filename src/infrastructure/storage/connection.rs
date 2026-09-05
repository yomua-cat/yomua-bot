//! SQLite 数据库连接管理。

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;

use crate::error::StorageError;

/// SQLite 存储句柄。
///
/// 包装一个连接池，并提供迁移支持。
#[derive(Debug, Clone)]
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// 在给定路径打开（或创建）SQLite 数据库，并运行迁移。
    pub async fn open(path: &str) -> Result<Self, StorageError> {
        let options = SqliteConnectOptions::from_str(path)
            .map_err(|e| StorageError::Connection(e.to_string()))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Self { pool })
    }

    /// 打开一个内存数据库（用于测试）。
    pub async fn open_in_memory() -> Result<Self, StorageError> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(|e| StorageError::Connection(e.to_string()))?
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Self { pool })
    }

    /// 运行所有待执行的迁移。
    pub async fn migrate(&self) -> Result<(), StorageError> {
        crate::infrastructure::storage::migrations::run_migrations(&self.pool).await
    }

    /// 获取底层连接池的引用。
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// 关闭连接池。
    pub async fn close(&self) {
        self.pool.close().await;
    }
}
