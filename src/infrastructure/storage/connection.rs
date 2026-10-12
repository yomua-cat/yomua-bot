//! SQLite 数据库连接管理。

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteTransaction};
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
            .busy_timeout(std::time::Duration::from_secs(5))
            // 所有连接启用外键约束 enforcement（SQLite 默认关闭）
            .pragma("foreign_keys", "ON");

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
            .create_if_missing(true)
            // 测试环境也启用外键约束，确保行为一致
            .pragma("foreign_keys", "ON");

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

    /// 在一个事务中执行一组操作。
    ///
    /// 事务开始时会启用 `PRAGMA foreign_keys = ON`，确保 cascade 规则生效。
    /// 闭包返回 `Ok` 时自动 commit，返回 `Err` 时 rollback。
    ///
    /// 闭包接收一个 `&mut SqliteTransaction`，在闭包内部执行所有 async 操作。
    /// 闭包本身是同步的，返回一个 Future。
    pub async fn with_transaction<F, Fut, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&mut SqliteTransaction<'_>) -> Fut,
        Fut: std::future::Future<Output = Result<T, StorageError>>,
    {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Connection(format!("事务开始失败: {}", e)))?;

        // 启用外键约束 enforcement（SQLite 默认关闭）
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Connection(format!("PRAGMA foreign_keys 失败: {}", e)))?;

        let result = f(&mut tx).await;

        match result {
            Ok(value) => {
                tx.commit()
                    .await
                    .map_err(|e| StorageError::Connection(format!("事务提交失败: {}", e)))?;
                Ok(value)
            }
            Err(e) => {
                // Rollback 会自动发生，但显式调用更清晰
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }
}
