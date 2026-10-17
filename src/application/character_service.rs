//! 角色卡服务 —— 角色全局状态与 ID 操作.

use sqlx::{Acquire, SqlitePool};

use crate::error::RuntimeError;

/// 角色卡服务。
#[derive(Clone)]
pub struct CharacterService {
    pool: SqlitePool,
}

impl CharacterService {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// 设置角色为启用状态（幂等）。
    pub async fn enable_character(&self, character_id: i64) -> Result<(), RuntimeError> {
        let result = sqlx::query("UPDATE characters SET enabled = 1 WHERE id = ?")
            .bind(character_id)
            .execute(&self.pool)
            .await;
        result.map_err(|e| RuntimeError::Internal(format!("enable_character: {}", e)))?;
        Ok(())
    }

    /// 设置角色为停用状态（幂等）。
    pub async fn disable_character(&self, character_id: i64) -> Result<(), RuntimeError> {
        let result = sqlx::query("UPDATE characters SET enabled = 0 WHERE id = ?")
            .bind(character_id)
            .execute(&self.pool)
            .await;
        result.map_err(|e| RuntimeError::Internal(format!("disable_character: {}", e)))?;
        Ok(())
    }

    /// 重新分配角色 ID。
    ///
    /// SQLite 没有 ON UPDATE CASCADE，且 FK 约束会阻止子表 UPDATE 指向不存在的父表 ID。
    ///
    /// SQLite `PRAGMA foreign_keys` 是 connection-local 的，必须在同一个连接上执行：
    /// 1. 从池中显式获取一个专属连接
    /// 2. 在该连接上执行 `PRAGMA foreign_keys = OFF`（禁用 FK 检查）
    /// 3. 在该连接上开启事务
    /// 4. 执行所有子表 + 父表的 ID 重写
    /// 5. 提交事务
    /// 6. 在该连接上执行 `PRAGMA foreign_keys = ON`（恢复 FK）
    /// 7. 在该连接上执行 `PRAGMA foreign_key_check`（验证 FK 一致性）
    /// 8. 连接归还到池
    ///
    /// 任意步骤失败 → 整个操作回滚（原子性）。
    pub async fn reassign_id(&self, old_id: i64, new_id: i64) -> Result<(), RuntimeError> {
        if old_id == new_id {
            return Err(RuntimeError::UserInput(
                "SAME_ID".to_string(),
                "old-id 和 new-id 不能相同".to_string(),
            ));
        }

        // 先验证连接级 FK enforcement 已启用（Safety check）
        let fk_result: Result<Vec<(i64,)>, _> = sqlx::query_as("PRAGMA foreign_keys")
            .fetch_all(&self.pool)
            .await;
        let fk_rows = fk_result.map_err(|e| RuntimeError::Internal(format!("FK 检查: {}", e)))?;
        let fk_on = fk_rows.iter().any(|(v,): &(i64,)| *v == 1);
        if !fk_on {
            return Err(RuntimeError::Internal(
                "FK enforcement 未启用，请通过 SqliteStorage 创建连接".to_string(),
            ));
        }

        // 验证 old_id 存在
        let old_row = sqlx::query_as::<_, (i64,)>("SELECT id FROM characters WHERE id = ?")
            .bind(old_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| RuntimeError::Internal(format!("查 old_id: {}", e)))?;
        if old_row.is_none() {
            return Err(RuntimeError::UserInput(
                "CHARACTER_NOT_FOUND".to_string(),
                format!("角色不存在：{}", old_id),
            ));
        }

        // 验证 new_id 不存在
        let new_row = sqlx::query_as::<_, (i64,)>("SELECT id FROM characters WHERE id = ?")
            .bind(new_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| RuntimeError::Internal(format!("查 new_id: {}", e)))?;
        if new_row.is_some() {
            return Err(RuntimeError::UserInput(
                "ID_COLLISION".to_string(),
                format!("ID {} 已存在，无法 reassign", new_id),
            ));
        }

        // ============================================================
        // 关键：从池中显式获取一个专属连接，确保所有 PRAGMA/UPDATE/
        // BEGIN/COMMIT 在同一个 connection 上执行。
        // ============================================================
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| RuntimeError::Internal(format!("获取数据库连接: {}", e)))?;

        // 在专属连接上禁用 FK（事务外）
        sqlx::query("PRAGMA foreign_keys = OFF")
            .execute(&mut *conn)
            .await
            .map_err(|e| RuntimeError::Internal(format!("FK OFF: {}", e)))?;

        // 在同一个连接上开启事务
        let mut tx = conn
            .begin()
            .await
            .map_err(|e| RuntimeError::Internal(format!("事务开始: {}", e)))?;

        // 级联更新所有引用 characters.id 的表（FK 检查已禁用，顺序无关）
        // 涉及表（共 11 张）：
        // - messages.active_character_id (无 ON DELETE/UPDATE，默认 RESTRICT)
        // - conversation_bindings.character_id (CASCADE DELETE)
        // - character_states.character_id (CASCADE DELETE, PK)
        // - memories.character_id (CASCADE DELETE)
        // - semantic_memories.character_id (CASCADE DELETE)
        // - relationships.character_id (CASCADE DELETE, PK)
        // - moods.character_id (CASCADE DELETE, PK)
        // - conversation_states.character_id (CASCADE DELETE, PK)
        // - behavior_states.character_id (CASCADE DELETE, PK)
        // - schedules.character_id (CASCADE DELETE)
        // - characters.id (主表)
        //
        // 注意：messages.active_character_id 没有显式 ON DELETE/UPDATE 声明，
        // SQLite 默认行为是 RESTRICT，因此必须在父表 ID 变更前将其更新。
        let tables: [(&str, &str); 11] = [
            ("messages", "active_character_id"),
            ("conversation_bindings", "character_id"),
            ("character_states", "character_id"),
            ("memories", "character_id"),
            ("semantic_memories", "character_id"),
            ("relationships", "character_id"),
            ("moods", "character_id"),
            ("conversation_states", "character_id"),
            ("behavior_states", "character_id"),
            ("schedules", "character_id"),
            ("characters", "id"),
        ];

        // 执行所有 ID 重写
        for (tbl, col) in tables.iter() {
            let q = format!("UPDATE {} SET {} = ? WHERE {} = ?", tbl, col, col);
            let update_result = sqlx::query(&q)
                .bind(new_id)
                .bind(old_id)
                .execute(&mut *tx)
                .await;

            if let Err(e) = update_result {
                // 出错时回滚并恢复 FK
                let _ = tx.rollback().await;
                let _ = sqlx::query("PRAGMA foreign_keys = ON")
                    .execute(&mut *conn)
                    .await;
                return Err(RuntimeError::Internal(format!("更新 {}: {}", tbl, e)));
            }
        }

        // 提交事务
        let commit_result = tx.commit().await;
        if let Err(e) = commit_result {
            let _ = sqlx::query("PRAGMA foreign_keys = ON")
                .execute(&mut *conn)
                .await;
            return Err(RuntimeError::Internal(format!("提交: {}", e)));
        }

        // 在同一连接上重新启用 FK
        if let Err(e) = sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&mut *conn)
            .await
        {
            // 提交已成功，FK 恢复失败仅记录警告（不影响业务）
            tracing::warn!(target: "character_service", "FK 恢复失败: {}", e);
        }

        // 在同一连接上验证 FK 一致性
        let fk_check: Vec<(String, String, String, i64, String)> =
            sqlx::query_as("PRAGMA foreign_key_check")
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| RuntimeError::Internal(format!("FK 一致性检查: {}", e)))?;
        if !fk_check.is_empty() {
            return Err(RuntimeError::Internal(format!(
                "FK 一致性检查失败: {:?}",
                fk_check
            )));
        }

        // 连接自动归还到池

        Ok(())
    }
}
