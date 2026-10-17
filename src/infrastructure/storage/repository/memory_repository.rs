//! MemoryRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::memory::{Memory, MemoryType, SemanticMatchResult};
use crate::domain::repository::MemoryRepository;
use crate::error::RepositoryError;

pub struct SqliteMemoryRepository {
    pool: SqlitePool,
}

impl SqliteMemoryRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl MemoryRepository for SqliteMemoryRepository {
    async fn find_pending_edit_requests(&self, limit: i64) -> Result<Vec<Memory>, RepositoryError> {
        let rows: Vec<MemoryRow> = sqlx::query_as(
            r#"SELECT id, character_id, user_id, conversation_id, memory_type, content,
                      importance, created_at, last_accessed, metadata, embedding
                 FROM memories
                WHERE json_valid(metadata)
                  AND json_extract(metadata, '$.edit_request.status') = 'pending'
                ORDER BY json_extract(metadata, '$.edit_request.requested_at') ASC, id ASC
                LIMIT ?"#,
        )
        .bind(limit.max(0))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(parse_memory_row).collect()
    }

    async fn promote_safe_candidates(&self, limit: i64) -> Result<u64, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let ids: Vec<i64> = sqlx::query_scalar(
            r#"SELECT id
                 FROM memories
                WHERE COALESCE(json_extract(metadata, '$.status'), 'formal') = 'candidate'
                  AND json_extract(metadata, '$.kind') = 'statement'
                  AND json_extract(metadata, '$.source_message_id') IS NOT NULL
                  AND COALESCE(json_extract(metadata, '$.candidate_confidence'), 0) >= 0.8
                  AND importance >= 0.5
                ORDER BY importance DESC, created_at ASC, id ASC
                LIMIT ?"#,
        )
        .bind(limit.max(0))
        .fetch_all(&mut *tx)
        .await?;

        if ids.is_empty() {
            tx.rollback().await?;
            return Ok(0);
        }

        let confirmed_at = chrono::Utc::now().to_rfc3339();
        let mut promoted = 0_u64;
        for id in ids {
            let result = sqlx::query(
                r#"UPDATE memories
                      SET metadata = json_set(
                          CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                          '$.status', 'formal',
                          '$.verification', 'program_confirmed',
                          '$.confirmed_at', ?,
                          '$.confirmation_reason', '有来源的确定性陈述候选，置信度和重要度满足程序门槛'
                      )
                    WHERE id = ?
                      AND COALESCE(json_extract(metadata, '$.status'), 'formal') = 'candidate'"#,
            )
            .bind(&confirmed_at)
            .bind(id)
            .execute(&mut *tx)
            .await?;
            promoted += u64::from(result.rows_affected() == 1);
        }

        tx.commit().await?;
        Ok(promoted)
    }

    async fn candidate_exists_for_source(
        &self,
        character_id: i64,
        conversation_id: Option<i64>,
        source_message_id: i64,
    ) -> Result<bool, RepositoryError> {
        let exists: i64 = sqlx::query_scalar(
            r#"SELECT EXISTS(
                SELECT 1 FROM memories
                WHERE character_id = ?
                  AND ((conversation_id = ?) OR (conversation_id IS NULL AND ? IS NULL))
                  AND json_extract(metadata, '$.source_message_id') = ?
            )"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(conversation_id)
        .bind(source_message_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists != 0)
    }

    async fn insert_candidate_once(&self, memory: &Memory) -> Result<Option<i64>, RepositoryError> {
        let Some(source_message_id) = memory
            .metadata
            .get("source_message_id")
            .and_then(|value| value.as_i64())
        else {
            return Ok(Some(self.insert(memory).await?));
        };
        let mut tx = self.pool.begin().await?;
        let metadata = serde_json::to_string(&memory.metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        // 将去重判断和插入放进同一条写语句。SQLite 会在真正写入前取得写锁，
        // 即使多个 Outbox 重试并发到达，也不会出现“都读到不存在、随后各插一条”。
        let id: Option<i64> = sqlx::query_scalar(
            r#"INSERT INTO memories
                (character_id, user_id, conversation_id, memory_type, content, importance,
                 created_at, last_accessed, metadata, embedding)
             SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
              WHERE NOT EXISTS (
                  SELECT 1 FROM memories
                   WHERE character_id = ?
                     AND ((conversation_id = ?) OR (conversation_id IS NULL AND ? IS NULL))
                     AND json_extract(metadata, '$.source_message_id') = ?
              )
             RETURNING id"#,
        )
        .bind(memory.character_id)
        .bind(memory.user_id)
        .bind(memory.conversation_id)
        .bind(memory_type_name(memory.memory_type))
        .bind(&memory.content)
        .bind(memory.importance)
        .bind(memory.created_at.to_rfc3339())
        .bind(memory.last_accessed.to_rfc3339())
        .bind(&metadata)
        .bind(&memory.embedding)
        .bind(memory.character_id)
        .bind(memory.conversation_id)
        .bind(memory.conversation_id)
        .bind(source_message_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(id) = id else {
            tx.rollback().await?;
            return Ok(None);
        };
        sqlx::query(
            r#"UPDATE memories
                  SET user_id = (SELECT p.user_id FROM participants p
                                   WHERE p.id = json_extract(?, '$.source_participant_id'))
                WHERE id = ? AND user_id IS NULL"#,
        )
        .bind(&metadata)
        .bind(id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(id))
    }

    async fn cognition_progress(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<i64, RepositoryError> {
        Ok(sqlx::query_scalar("SELECT last_message_id FROM cognition_progress WHERE character_id=? AND conversation_id=?").bind(character_id).bind(conversation_id).fetch_optional(&self.pool).await?.unwrap_or(0))
    }
    async fn commit_cognition_batch(
        &self,
        character_id: i64,
        conversation_id: i64,
        expected_progress: i64,
        new_progress: i64,
        memories: &[Memory],
    ) -> Result<bool, RepositoryError> {
        if new_progress <= expected_progress {
            return Err(RepositoryError::Internal("认知进度必须前进".into()));
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT OR IGNORE INTO cognition_progress(character_id,conversation_id,last_message_id) VALUES(?,?,0)").bind(character_id).bind(conversation_id).execute(&mut *tx).await?;
        let result=sqlx::query("UPDATE cognition_progress SET last_message_id=? WHERE character_id=? AND conversation_id=? AND last_message_id=?").bind(new_progress).bind(character_id).bind(conversation_id).bind(expected_progress).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(false);
        }
        for memory in memories {
            if memory.character_id != character_id
                || memory.conversation_id != Some(conversation_id)
            {
                return Err(RepositoryError::Internal("认知记忆来源范围不一致".into()));
            }
            let kind = memory_type_name(memory.memory_type);
            let metadata = memory.metadata.to_string();
            sqlx::query("INSERT INTO memories(character_id,user_id,conversation_id,memory_type,content,importance,created_at,last_accessed,metadata,embedding) VALUES(?,?,?,?,?,?,?,?,?,?)")
            .bind(character_id).bind(memory.user_id).bind(conversation_id).bind(kind).bind(&memory.content).bind(memory.importance).bind(memory.created_at.to_rfc3339()).bind(memory.last_accessed.to_rfc3339()).bind(&metadata).bind(&memory.embedding).execute(&mut *tx).await?;
            sqlx::query(
                r#"UPDATE memories
                      SET user_id = (SELECT p.user_id FROM participants p
                                       WHERE p.id = json_extract(?, '$.source_participant_id'))
                    WHERE id = last_insert_rowid() AND user_id IS NULL"#,
            )
            .bind(&metadata)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(true)
    }
    async fn find_visible(
        &self,
        character_id: i64,
        conversation_id: i64,
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError> {
        let rows:Vec<MemoryRow>=sqlx::query_as("SELECT id,character_id,user_id,conversation_id,memory_type,content,importance,created_at,last_accessed,metadata,embedding FROM memories WHERE character_id=? AND COALESCE(json_extract(metadata,'$.status'),'formal') != 'candidate' AND conversation_id=? ORDER BY importance DESC,created_at DESC,id DESC LIMIT ?")
            .bind(character_id).bind(conversation_id).bind(limit.max(0)).fetch_all(&self.pool).await?;
        rows.into_iter().map(parse_memory_row).collect()
    }

    async fn find_visible_for_participant(
        &self,
        character_id: i64,
        conversation_id: i64,
        participant_id: i64,
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError> {
        let rows: Vec<MemoryRow> = sqlx::query_as(
            r#"SELECT id,character_id,user_id,conversation_id,memory_type,content,importance,
                      created_at,last_accessed,metadata,embedding
                 FROM memories
                WHERE character_id=?
                  AND COALESCE(json_extract(metadata,'$.status'),'formal') != 'candidate'
                  AND (conversation_id=? OR (
                       user_id=(SELECT user_id FROM participants WHERE id=? AND role='user')
                       AND COALESCE(json_extract(metadata,'$.shared'),0)=1
                       AND NOT EXISTS (
                           SELECT 1 FROM memory_sharing_policies policy
                            WHERE policy.character_id = memories.character_id
                              AND policy.user_id = memories.user_id
                              AND policy.status = 'confirmed'
                       )
                  ))
                ORDER BY importance DESC,created_at DESC,id DESC LIMIT ?"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(participant_id)
        .bind(limit.max(0))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(parse_memory_row).collect()
    }

    async fn search_visible_by_keywords(
        &self,
        character_id: i64,
        conversation_id: i64,
        participant_id: i64,
        keywords: &[String],
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError> {
        let keywords: Vec<String> = keywords
            .iter()
            .map(|keyword| keyword.trim().to_lowercase())
            .filter(|keyword| !keyword.is_empty())
            .collect();
        if keywords.is_empty() {
            return Ok(Vec::new());
        }
        let mut builder = sqlx::QueryBuilder::new(
            "SELECT id,character_id,user_id,conversation_id,memory_type,content,importance,created_at,last_accessed,metadata,embedding FROM memories WHERE character_id=",
        );
        builder.push_bind(character_id);
        builder.push(
            " AND COALESCE(json_extract(metadata,'$.status'),'formal') != 'candidate' AND (conversation_id=",
        );
        builder.push_bind(conversation_id);
        builder.push(" OR (user_id=(SELECT user_id FROM participants WHERE id=");
        builder.push_bind(participant_id);
        builder.push(
            " AND role='user') AND COALESCE(json_extract(metadata,'$.shared'),0)=1 AND NOT EXISTS (SELECT 1 FROM memory_sharing_policies policy WHERE policy.character_id=memories.character_id AND policy.user_id=memories.user_id AND policy.status='confirmed')) ) AND (",
        );
        for (index, keyword) in keywords.iter().enumerate() {
            if index > 0 {
                builder.push(" OR ");
            }
            builder.push("lower(content) LIKE ");
            builder.push_bind(format!("%{keyword}%"));
        }
        builder.push(") ORDER BY importance DESC,created_at DESC,id DESC LIMIT ");
        builder.push_bind(limit.max(0));
        let rows = builder
            .build_query_as::<MemoryRow>()
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(parse_memory_row).collect()
    }

    async fn request_sharing_policy(
        &self,
        character_id: i64,
        user_id: i64,
        reason: Option<&str>,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"INSERT INTO memory_sharing_policies
                    (character_id, user_id, status, requested_at, confirmed_at, reason)
               VALUES (?, ?, 'requested', datetime('now'), NULL, ?)
               ON CONFLICT(character_id, user_id) DO UPDATE SET
                    status = 'requested',
                    requested_at = datetime('now'),
                    confirmed_at = NULL,
                    reason = excluded.reason"#,
        )
        .bind(character_id)
        .bind(user_id)
        .bind(reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn confirm_sharing_policy(
        &self,
        character_id: i64,
        user_id: i64,
        reason: Option<&str>,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"INSERT INTO memory_sharing_policies
                    (character_id, user_id, status, requested_at, confirmed_at, reason)
               VALUES (?, ?, 'confirmed', datetime('now'), datetime('now'), ?)
               ON CONFLICT(character_id, user_id) DO UPDATE SET
                    status = 'confirmed',
                    confirmed_at = datetime('now'),
                    reason = COALESCE(excluded.reason, memory_sharing_policies.reason)"#,
        )
        .bind(character_id)
        .bind(user_id)
        .bind(reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn cancel_sharing_policy(
        &self,
        character_id: i64,
        user_id: i64,
        reason: Option<&str>,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"INSERT INTO memory_sharing_policies
                    (character_id, user_id, status, requested_at, confirmed_at, reason)
               VALUES (?, ?, 'cancelled', datetime('now'), NULL, ?)
               ON CONFLICT(character_id, user_id) DO UPDATE SET
                    status = 'cancelled',
                    confirmed_at = NULL,
                    reason = COALESCE(excluded.reason, memory_sharing_policies.reason)"#,
        )
        .bind(character_id)
        .bind(user_id)
        .bind(reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
    async fn find_by_id(&self, id: i64) -> Result<Option<Memory>, RepositoryError> {
        let rows: Vec<MemoryRow> = sqlx::query_as(
            r#"SELECT id, character_id, user_id, conversation_id, memory_type, content,
                      importance, created_at, last_accessed, metadata, embedding
               FROM memories WHERE id = ?"#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await?;

        match rows.len() {
            0 => Ok(None),
            1 => parse_memory_row(rows.into_iter().next().unwrap()).map(Some),
            _ => Err(RepositoryError::Database(
                "unexpected multiple rows".to_string(),
            )),
        }
    }

    async fn find_by_character_id(
        &self,
        character_id: i64,
        memory_type: Option<MemoryType>,
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError> {
        let type_str = memory_type.map(|mt| match mt {
            MemoryType::Episodic => "episodic",
            MemoryType::Semantic => "semantic",
            MemoryType::Relationship => "relationship",
            MemoryType::System => "system",
        });

        let rows: Vec<MemoryRow> = if let Some(t) = type_str {
            sqlx::query_as(
                r#"SELECT id, character_id, user_id, conversation_id, memory_type, content,
                        importance, created_at, last_accessed, metadata, embedding
                     FROM memories WHERE character_id = ? AND memory_type = ?
                       AND COALESCE(json_extract(metadata, '$.status'), 'formal') != 'candidate'
                     ORDER BY importance DESC, created_at DESC LIMIT ?"#,
            )
            .bind(character_id)
            .bind(t)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                r#"SELECT id, character_id, user_id, conversation_id, memory_type, content,
                        importance, created_at, last_accessed, metadata, embedding
                     FROM memories WHERE character_id = ?
                       AND COALESCE(json_extract(metadata, '$.status'), 'formal') != 'candidate'
                     ORDER BY importance DESC, created_at DESC LIMIT ?"#,
            )
            .bind(character_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };

        rows.into_iter().map(parse_memory_row).collect()
    }

    async fn search_by_keywords(
        &self,
        character_id: i64,
        keywords: &[String],
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError> {
        // 清洗关键词：去空白、去空、去重。
        let mut unique: Vec<String> = Vec::new();
        for kw in keywords {
            let kw = kw.trim().to_lowercase();
            if kw.is_empty() || unique.contains(&kw) {
                continue;
            }
            unique.push(kw);
        }
        if unique.is_empty() {
            return Ok(Vec::new());
        }

        // 按角色过滤，对内容做子串匹配（MVP 无需向量检索）。
        // 用 QueryBuilder 构造动态 `OR content LIKE ?` 子句。
        let mut builder = sqlx::QueryBuilder::new(
            "SELECT id, character_id, user_id, conversation_id, memory_type, content, importance, \
             created_at, last_accessed, metadata, embedding \
             FROM memories WHERE character_id = ",
        );
        builder.push_bind(character_id);
        builder.push(
            " AND COALESCE(json_extract(metadata, '$.status'), 'formal') != 'candidate' AND (",
        );
        let mut first = true;
        for kw in &unique {
            if !first {
                builder.push(" OR ");
            }
            first = false;
            builder.push("content LIKE ");
            builder.push_bind(format!("%{kw}%"));
        }
        builder.push(") ORDER BY importance DESC, created_at DESC LIMIT ");
        builder.push_bind(limit);

        let rows = builder
            .build_query_as::<MemoryRow>()
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(parse_memory_row).collect()
    }

    async fn insert(&self, memory: &Memory) -> Result<i64, RepositoryError> {
        let memory_type_str = memory_type_name(memory.memory_type);
        let metadata = serde_json::to_string(&memory.metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let result = sqlx::query(
            r#"INSERT INTO memories
                (character_id, user_id, conversation_id, memory_type, content, importance, metadata, embedding)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(memory.character_id)
        .bind(memory.user_id)
        .bind(memory.conversation_id)
        .bind(memory_type_str)
        .bind(&memory.content)
        .bind(memory.importance)
        .bind(&metadata)
        .bind(&memory.embedding)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let id = result.last_insert_rowid();
        sqlx::query(
            r#"UPDATE memories
                  SET user_id = (SELECT p.user_id FROM participants p
                                   WHERE p.id = json_extract(?, '$.source_participant_id'))
                WHERE id = ? AND user_id IS NULL"#,
        )
        .bind(&metadata)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    async fn update(&self, memory: &Memory) -> Result<(), RepositoryError> {
        let metadata = serde_json::to_string(&memory.metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let result = sqlx::query(
            r#"UPDATE memories SET
                content = ?, importance = ?, last_accessed = ?, metadata = ?
             WHERE id = ?"#,
        )
        .bind(&memory.content)
        .bind(memory.importance)
        .bind(memory.last_accessed.to_rfc3339())
        .bind(&metadata)
        .bind(memory.id)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        if result.rows_affected() != 1 {
            return Err(RepositoryError::NotFound(format!(
                "memory {} not found",
                memory.id
            )));
        }
        Ok(())
    }

    async fn update_if_unchanged(
        &self,
        memory: &Memory,
        expected: &Memory,
    ) -> Result<bool, RepositoryError> {
        let metadata = serde_json::to_string(&memory.metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let expected_metadata = serde_json::to_string(&expected.metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let result = sqlx::query(
            r#"UPDATE memories SET
                content = ?, importance = ?, last_accessed = ?, metadata = ?
             WHERE id = ? AND content = ? AND importance = ? AND metadata = ?"#,
        )
        .bind(&memory.content)
        .bind(memory.importance)
        .bind(memory.last_accessed.to_rfc3339())
        .bind(&metadata)
        .bind(memory.id)
        .bind(&expected.content)
        .bind(expected.importance)
        .bind(&expected_metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        if result.rows_affected() == 1 {
            Ok(true)
        } else {
            let exists: Option<i64> = sqlx::query_scalar("SELECT id FROM memories WHERE id = ?")
                .bind(memory.id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| RepositoryError::Database(e.to_string()))?;
            if exists.is_none() {
                Err(RepositoryError::NotFound(format!(
                    "memory {} not found",
                    memory.id
                )))
            } else {
                Ok(false)
            }
        }
    }

    async fn delete(&self, id: i64) -> Result<(), RepositoryError> {
        let result = sqlx::query("DELETE FROM memories WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        if result.rows_affected() != 1 {
            return Err(RepositoryError::NotFound(format!("memory {id} not found")));
        }
        Ok(())
    }

    async fn find_all(&self, limit: i64) -> Result<Vec<Memory>, RepositoryError> {
        let rows: Vec<MemoryRow> = sqlx::query_as(
            r#"SELECT id, character_id, user_id, conversation_id, memory_type, content,
                      importance, created_at, last_accessed, metadata, embedding
               FROM memories
               WHERE COALESCE(json_extract(metadata, '$.status'), 'formal') != 'candidate'
               ORDER BY importance DESC, created_at DESC, id DESC
               LIMIT ?"#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(parse_memory_row).collect()
    }

    async fn find_by_conversation_id(
        &self,
        conversation_id: i64,
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError> {
        let rows: Vec<MemoryRow> = sqlx::query_as(
            r#"SELECT id, character_id, user_id, conversation_id, memory_type, content,
                      importance, created_at, last_accessed, metadata, embedding
               FROM memories
               WHERE conversation_id = ?
                 AND COALESCE(json_extract(metadata, '$.status'), 'formal') != 'candidate'
               ORDER BY importance DESC, created_at DESC, id DESC
               LIMIT ?"#,
        )
        .bind(conversation_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(parse_memory_row).collect()
    }

    async fn find_filtered(
        &self,
        character_id: Option<i64>,
        conversation_id: Option<i64>,
        memory_type: Option<MemoryType>,
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError> {
        let type_str = memory_type.map(|mt| match mt {
            MemoryType::Episodic => "episodic",
            MemoryType::Semantic => "semantic",
            MemoryType::Relationship => "relationship",
            MemoryType::System => "system",
        });

        #[allow(unused_mut)]
        let mut query = String::from(
            r#"SELECT id, character_id, user_id, conversation_id, memory_type, content,
                      importance, created_at, last_accessed, metadata, embedding
               FROM memories WHERE COALESCE(json_extract(metadata, '$.status'), 'formal') != 'candidate'"#,
        );
        let mut builder = sqlx::QueryBuilder::new(&query);

        if let Some(cid) = character_id {
            builder.push(" AND character_id = ");
            builder.push_bind(cid);
        }
        if let Some(cid) = conversation_id {
            builder.push(" AND conversation_id = ");
            builder.push_bind(cid);
        }
        if let Some(t) = type_str {
            builder.push(" AND memory_type = ");
            builder.push_bind(t);
        }

        builder.push(" ORDER BY importance DESC, created_at DESC, id DESC LIMIT ");
        builder.push_bind(limit);

        let rows: Vec<MemoryRow> = builder
            .build_query_as::<MemoryRow>()
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(parse_memory_row).collect()
    }

    async fn search_visible_by_embedding(
        &self,
        character_id: i64,
        conversation_id: i64,
        user_id: Option<i64>,
        query_embedding: &[f32],
        memory_type: Option<&str>,
        limit: i64,
    ) -> Result<Vec<SemanticMatchResult>, RepositoryError> {
        // 向量记忆仍使用独立表；先在 SQL 中限定会话和共享策略，再计算相似度。
        let query = r#"
            SELECT id,character_id,user_id,conversation_id,memory_type,content,
                   importance,created_at,last_accessed,metadata,embedding
              FROM semantic_memories
             WHERE character_id = ?
               AND (? IS NULL OR memory_type = ?)
               AND (
                    conversation_id = ?
                    OR (
                        ? IS NOT NULL
                        AND user_id = ?
                        AND COALESCE(json_extract(metadata, '$.shared'), 0) = 1
                        AND NOT EXISTS (
                            SELECT 1
                              FROM memory_sharing_policies policy
                             WHERE policy.character_id = semantic_memories.character_id
                               AND policy.user_id = semantic_memories.user_id
                               AND policy.status = 'confirmed'
                        )
                    )
               )
        "#;
        let rows: Vec<MemoryRow> = sqlx::query_as(query)
            .bind(character_id)
            .bind(memory_type)
            .bind(memory_type)
            .bind(conversation_id)
            .bind(user_id)
            .bind(user_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        // 计算每条记录的余弦相似度，过滤无效 embedding，按相似度降序排列。
        let mut scored: Vec<SemanticMatchResult> = Vec::new();
        for row in rows {
            if let Ok(memory) = parse_memory_row(row) {
                if let Some(ref emb_str) = memory.embedding {
                    if let Ok(stored) = serde_json::from_str::<Vec<f32>>(emb_str) {
                        if let Some(sim) = cosine_similarity(query_embedding, &stored) {
                            scored.push(SemanticMatchResult { memory, score: sim });
                        }
                    }
                }
            }
        }

        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(limit.max(0) as usize);
        Ok(scored)
    }

    async fn insert_semantic(
        &self,
        character_id: i64,
        conversation_id: Option<i64>,
        memory_type: &str,
        content: &str,
        embedding: &[f32],
        importance: f64,
        metadata: &str,
    ) -> Result<i64, RepositoryError> {
        let embedding_json = serde_json::to_string(embedding)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let result = sqlx::query(
            r#"INSERT INTO semantic_memories
                (character_id, conversation_id, memory_type, content, embedding, importance, metadata)
             VALUES (?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(character_id)
        .bind(conversation_id)
        .bind(memory_type)
        .bind(content)
        .bind(&embedding_json)
        .bind(importance)
        .bind(metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let id = result.last_insert_rowid();
        sqlx::query(
            r#"UPDATE semantic_memories
                  SET user_id = COALESCE(
                      CAST(json_extract(CASE WHEN json_valid(?) THEN ? ELSE '{}' END, '$.user_id') AS INTEGER),
                      (SELECT p.user_id FROM participants p
                        WHERE p.id = CAST(json_extract(CASE WHEN json_valid(?) THEN ? ELSE '{}' END,
                                                     '$.source_participant_id') AS INTEGER))
                  )
                WHERE id = ? AND user_id IS NULL"#,
        )
        .bind(metadata)
        .bind(metadata)
        .bind(metadata)
        .bind(metadata)
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(id)
    }
}

type MemoryRow = (
    i64,
    i64,
    Option<i64>,
    Option<i64>,
    String,
    String,
    f64,
    String,
    String,
    String,
    Option<String>,
);

fn memory_type_name(memory_type: MemoryType) -> &'static str {
    match memory_type {
        MemoryType::Episodic => "episodic",
        MemoryType::Semantic => "semantic",
        MemoryType::Relationship => "relationship",
        MemoryType::System => "system",
    }
}

fn parse_memory_row(row: MemoryRow) -> Result<Memory, RepositoryError> {
    let (
        id,
        character_id,
        user_id,
        conversation_id,
        memory_type_str,
        content,
        importance,
        created_at,
        last_accessed,
        metadata_json,
        embedding,
    ) = row;

    let memory_type = match memory_type_str.as_str() {
        "episodic" => MemoryType::Episodic,
        "semantic" => MemoryType::Semantic,
        "relationship" => MemoryType::Relationship,
        "system" => MemoryType::System,
        other => {
            return Err(RepositoryError::Database(format!(
                "unknown memory_type: {other}"
            )))
        }
    };

    let metadata: serde_json::Value = serde_json::from_str(&metadata_json)
        .map_err(|e| RepositoryError::Database(format!("invalid metadata JSON: {e}")))?;

    let created_at = super::timestamp::parse_timestamp(&created_at)?;
    let last_accessed = super::timestamp::parse_timestamp(&last_accessed)?;

    Ok(Memory {
        id,
        character_id,
        user_id,
        conversation_id,
        memory_type,
        content,
        importance,
        created_at,
        last_accessed,
        embedding,
        metadata,
    })
}

/// 计算两个向量的余弦相似度。
///
/// 返回 `None` 当任一向量为零向量（无法归一化）。
fn cosine_similarity(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() || a.is_empty() {
        return None;
    }

    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    for (av, bv) in a.iter().zip(b.iter()) {
        dot += av * bv;
        norm_a += av * av;
        norm_b += bv * bv;
    }

    let norm_product = norm_a.sqrt() * norm_b.sqrt();
    if norm_product < 1e-10 {
        return None;
    }

    Some(dot / norm_product)
}
