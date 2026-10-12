//! 会话删除服务 —— 在事务中安全删除 Session 数据。
//!
//! ## 语义定义
//!
//! `session delete-character <id>`：删除指定角色所关联的所有会话数据。
//! - 删除目标角色的所有 ConversationBinding
//! - 删除这些 Binding 对应的 Conversation（连带 Messages, Participants, States 等）
//! - Character 定义本身保留（角色定义 ≠ 会话数据）
//! - Character-scoped 数据（Relationship, Memory, SemanticMemory）保留
//!
//! `session delete-all`：删除所有会话数据。
//! - 删除所有 Conversation（连带 Messages, Participants, States 等）
//! - 所有 CharacterBinding 连带删除
//! - Character 定义本身保留
//! - Character-scoped 数据（Relationship, Memory, SemanticMemory）保留
//!
//! ## 事务保障
//!
//! 所有删除操作在一个 SQLite 事务中执行：
//! - `PRAGMA foreign_keys = ON` 保证 cascade 规则生效
//! - 任何步骤失败 → rollback
//! - 成功 → commit

use sqlx::SqlitePool;

use crate::error::RuntimeError;

/// 会话删除服务。
#[derive(Clone)]
pub struct SessionService {
    pool: SqlitePool,
}

impl SessionService {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// 删除指定角色关联的所有会话数据。
    ///
    /// 返回删除的会话数量。
    pub async fn delete_character_sessions(
        &self,
        character_id: i64,
    ) -> Result<usize, RuntimeError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| RuntimeError::Internal(format!("事务开始失败: {}", e)))?;

        // 启用外键 enforcement
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&mut *tx)
            .await
            .map_err(|e| RuntimeError::Internal(format!("PRAGMA: {}", e)))?;

        // 找到该角色的所有会话 ID
        let conv_ids: Vec<(i64,)> = sqlx::query_as(
            "SELECT conversation_id FROM conversation_bindings WHERE character_id = ?",
        )
        .bind(character_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| RuntimeError::Internal(format!("查询会话: {}", e)))?;

        if conv_ids.is_empty() {
            tx.rollback()
                .await
                .map_err(|e| RuntimeError::Internal(format!("回滚失败: {}", e)))?;
            return Ok(0);
        }

        let conv_ids: Vec<i64> = conv_ids.into_iter().map(|(id,)| id).collect();

        // 删除该角色的 bindings（cascade 会清理 conversation_states, moods, behavior_states）
        sqlx::query("DELETE FROM conversation_bindings WHERE character_id = ?")
            .bind(character_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| RuntimeError::Internal(format!("删除 bindings: {}", e)))?;

        // 删除这些会话（cascade 删除 messages, participants, 以及 binding 引用的其他 binding）
        let mut deleted = 0;
        for conv_id in &conv_ids {
            let result = sqlx::query("DELETE FROM conversations WHERE id = ?")
                .bind(conv_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| RuntimeError::Internal(format!("删除会话 {}: {}", conv_id, e)))?;
            deleted += result.rows_affected() as usize;
        }

        tx.commit()
            .await
            .map_err(|e| RuntimeError::Internal(format!("提交失败: {}", e)))?;

        Ok(deleted)
    }

    /// 删除所有会话数据。
    ///
    /// 返回删除的会话数量。
    pub async fn delete_all_sessions(&self) -> Result<usize, RuntimeError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| RuntimeError::Internal(format!("事务开始失败: {}", e)))?;

        // 启用外键 enforcement
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&mut *tx)
            .await
            .map_err(|e| RuntimeError::Internal(format!("PRAGMA: {}", e)))?;

        // 统计删除前的会话数量
        let before: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM conversations")
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| RuntimeError::Internal(format!("统计会话: {}", e)))?;

        // 删除所有会话（cascade 删除 messages, participants, conversation_bindings 等）
        sqlx::query("DELETE FROM conversations")
            .execute(&mut *tx)
            .await
            .map_err(|e| RuntimeError::Internal(format!("删除所有会话: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| RuntimeError::Internal(format!("提交失败: {}", e)))?;

        Ok(before.0 as usize)
    }
}
