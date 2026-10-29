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

use std::sync::Arc;

use sqlx::SqlitePool;
use tokio::sync::OwnedMutexGuard;

use crate::application::binding::ConversationGateRegistry;
use crate::domain::character::ConversationState;
use crate::domain::emotion::Mood;
use crate::error::RuntimeError;

/// 会话删除服务。
#[derive(Clone)]
pub struct SessionService {
    pool: SqlitePool,
    conversation_gates: Arc<ConversationGateRegistry>,
}

impl SessionService {
    pub fn new(pool: SqlitePool) -> Self {
        Self::with_gate_registry(pool, Arc::new(ConversationGateRegistry::new()))
    }

    pub fn with_gate_registry(
        pool: SqlitePool,
        conversation_gates: Arc<ConversationGateRegistry>,
    ) -> Self {
        Self {
            pool,
            conversation_gates,
        }
    }

    /// 原子更新会话范围的角色状态。
    ///
    /// `ConversationState` 和 `Mood` 属于同一个控制操作；调用方已经完成
    /// 所有输入校验后，两个 upsert 必须一起提交，避免 ctl 返回失败但只写入
    /// 其中一半。传入 `None` 的部分保持不变。
    pub async fn upsert_conversation_state_and_mood(
        &self,
        character_id: i64,
        conversation_id: i64,
        conversation_state: Option<&ConversationState>,
        mood: Option<&Mood>,
    ) -> Result<(), RuntimeError> {
        if conversation_state.is_none() && mood.is_none() {
            return Ok(());
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| RuntimeError::Internal(format!("状态事务开始失败: {e}")))?;

        if let Some(state) = conversation_state {
            sqlx::query(
                r#"INSERT INTO conversation_states (character_id, conversation_id, energy, stress, last_updated)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(character_id, conversation_id) DO UPDATE SET
                     energy = excluded.energy,
                     stress = excluded.stress,
                     last_updated = excluded.last_updated"#,
            )
            .bind(character_id)
            .bind(conversation_id)
            .bind(state.energy)
            .bind(state.stress)
            .bind(state.last_updated.to_rfc3339())
            .execute(&mut *tx)
            .await
            .map_err(|e| RuntimeError::Internal(format!("写入会话状态失败: {e}")))?;
        }

        if let Some(mood) = mood {
            sqlx::query(
                r#"INSERT INTO moods (character_id, conversation_id, value, last_updated)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(character_id, conversation_id) DO UPDATE SET
                     value = excluded.value,
                     last_updated = excluded.last_updated"#,
            )
            .bind(character_id)
            .bind(conversation_id)
            .bind(mood.value)
            .bind(mood.last_updated.to_rfc3339())
            .execute(&mut *tx)
            .await
            .map_err(|e| RuntimeError::Internal(format!("写入心情失败: {e}")))?;
        }

        tx.commit()
            .await
            .map_err(|e| RuntimeError::Internal(format!("状态事务提交失败: {e}")))?;
        Ok(())
    }

    /// 删除指定角色关联的所有会话数据。
    ///
    /// 返回删除的会话数量。
    pub async fn delete_character_sessions(
        &self,
        character_id: i64,
    ) -> Result<usize, RuntimeError> {
        // 先锁住目标会话，和发送前复核/角色切换使用同一把门闩。
        // 已经领取的任务会在锁释放后再次看到源消息已删除或租约失效。
        let conv_ids: Vec<i64> = sqlx::query_scalar(
            "SELECT conversation_id FROM conversation_bindings WHERE character_id = ?",
        )
        .bind(character_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| RuntimeError::Internal(format!("查询会话: {e}")))?;
        let _guards = self.lock_conversations(&conv_ids).await;

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
        if conv_ids.is_empty() {
            tx.rollback()
                .await
                .map_err(|e| RuntimeError::Internal(format!("回滚失败: {}", e)))?;
            return Ok(0);
        }

        // 会话删除必须同时终止尚未完成的持久事件，防止删除后仍触发模型、
        // 关系和记忆副作用。payload 中保存了 MessageAccepted 的会话 ID。
        let placeholders = std::iter::repeat_n("?", conv_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let cancel_sql = format!(
            "UPDATE outbox_events SET status='dead_letter', last_error=? WHERE status IN ('pending','retry_wait','processing') AND json_extract(payload, '$.conversation_id') IN ({placeholders})"
        );
        let mut cancel_query = sqlx::query(&cancel_sql).bind("会话已删除");
        for conversation_id in &conv_ids {
            cancel_query = cancel_query.bind(conversation_id);
        }
        cancel_query
            .execute(&mut *tx)
            .await
            .map_err(|e| RuntimeError::Internal(format!("取消会话事件: {}", e)))?;

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
        let conv_ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM conversations ORDER BY id")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| RuntimeError::Internal(format!("查询会话: {e}")))?;
        let _guards = self.lock_conversations(&conv_ids).await;
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

        sqlx::query(
            "UPDATE outbox_events SET status='dead_letter', last_error='会话已删除' WHERE status IN ('pending','retry_wait','processing') AND json_extract(payload, '$.conversation_id') IN (SELECT id FROM conversations)",
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| RuntimeError::Internal(format!("取消会话事件: {}", e)))?;

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

    async fn lock_conversations(&self, ids: &[i64]) -> Vec<OwnedMutexGuard<()>> {
        let mut sorted = ids.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let mut guards = Vec::with_capacity(sorted.len());
        for id in sorted {
            guards.push(self.conversation_gates.gate_for(id).lock_owned().await);
        }
        guards
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::storage::SqliteStorage;
    use chrono::Utc;

    #[tokio::test]
    async fn scoped_state_update_rolls_back_when_mood_write_fails() {
        let storage = SqliteStorage::open_in_memory().await.unwrap();
        storage.migrate().await.unwrap();
        sqlx::query(
            "CREATE TRIGGER fail_mood_write BEFORE INSERT ON moods BEGIN SELECT RAISE(ABORT, 'forced mood failure'); END",
        )
        .execute(storage.pool())
        .await
        .unwrap();

        let service = SessionService::new(storage.pool().clone());
        let now = Utc::now();
        let state = ConversationState {
            energy: 70.0,
            stress: 20.0,
            last_updated: now,
        };
        let mood = Mood {
            value: 80.0,
            last_updated: now,
        };

        assert!(service
            .upsert_conversation_state_and_mood(1, 2, Some(&state), Some(&mood))
            .await
            .is_err());
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM conversation_states WHERE character_id = 1 AND conversation_id = 2",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(count.0, 0, "心情写入失败时会话状态也必须回滚");
    }

    #[tokio::test]
    async fn session_delete_waits_for_conversation_fence() {
        let storage = SqliteStorage::open_in_memory().await.unwrap();
        storage.migrate().await.unwrap();
        sqlx::query("INSERT INTO characters(name) VALUES ('测试角色')")
            .execute(storage.pool())
            .await
            .unwrap();
        let conversation_id: i64 = sqlx::query_scalar(
            "INSERT INTO conversations(conversation_type, external_id) VALUES ('group', 'fence-test') RETURNING id",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO conversation_bindings(character_id, conversation_id) VALUES (1, ?)",
        )
        .bind(conversation_id)
        .execute(storage.pool())
        .await
        .unwrap();

        let gates = Arc::new(ConversationGateRegistry::new());
        let held = gates.gate_for(conversation_id).lock_owned().await;
        let service = SessionService::with_gate_registry(storage.pool().clone(), gates);
        let task = tokio::spawn(async move { service.delete_character_sessions(1).await });
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "删除必须等待正在处理该会话的任务释放门闩"
        );
        drop(held);
        assert_eq!(task.await.unwrap().unwrap(), 1);
    }
}
