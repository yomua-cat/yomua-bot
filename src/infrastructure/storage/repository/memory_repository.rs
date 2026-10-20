//! MemoryRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::memory::{
    CandidateReviewDecision, Memory, MemoryCandidateReview, MemoryClaim, MemoryClaimOperation,
    MemoryClaimProposal, MemoryClaimResult, MemoryRevisionOperation, MemoryRevisionRequest,
    MemoryRevisionResult, MemoryType, SemanticMatchResult,
};
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

    async fn find_pending_candidates(&self, limit: i64) -> Result<Vec<Memory>, RepositoryError> {
        let rows: Vec<MemoryRow> = sqlx::query_as(
            r#"SELECT id,character_id,user_id,conversation_id,memory_type,content,importance,
                      created_at,last_accessed,metadata,embedding
                 FROM memories
                WHERE COALESCE(json_extract(metadata, '$.status'), 'formal') = 'candidate'
                  AND json_extract(metadata, '$.source_message_id') IS NOT NULL
                  AND COALESCE(json_extract(metadata, '$.review.decision'), '') NOT IN ('confirm', 'reject')
                  AND (COALESCE(json_extract(metadata, '$.review.decision'), '') != 'defer'
                       OR datetime(json_extract(metadata, '$.review.reviewed_at')) < datetime('now', '-1 day'))
                ORDER BY created_at ASC, id ASC
                LIMIT ?"#,
        )
        .bind(limit.max(0))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(parse_memory_row).collect()
    }

    async fn apply_candidate_review(
        &self,
        review: &MemoryCandidateReview,
    ) -> Result<bool, RepositoryError> {
        let decision = match review.decision {
            CandidateReviewDecision::Confirm => "confirm",
            CandidateReviewDecision::Reject => "reject",
            CandidateReviewDecision::Defer => "defer",
        };
        let status = match review.decision {
            CandidateReviewDecision::Confirm => "formal",
            CandidateReviewDecision::Reject => "rejected",
            CandidateReviewDecision::Defer => "candidate",
        };
        let metadata = serde_json::json!({
            "decision": decision,
            "confidence": review.confidence,
            "reason": review.reason,
            "model": review.model,
            "reviewed_at": review.reviewed_at,
        });
        let result = sqlx::query(
            r#"UPDATE memories
                  SET metadata = json_set(
                      CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                      '$.status', ?,
                      '$.verification', 'decision_model_reviewed',
                      '$.review', json(?),
                      '$.visibility', CASE WHEN memory_type IN ('semantic', 'relationship') THEN 'shared' ELSE 'conversation' END,
                      '$.shared', json(CASE WHEN memory_type IN ('semantic', 'relationship') THEN 'true' ELSE 'false' END),
                      '$.confirmed_at', CASE WHEN ? = 'formal' THEN ? ELSE json_extract(metadata, '$.confirmed_at') END
                  )
                WHERE id = ?
                  AND COALESCE(json_extract(metadata, '$.status'), 'formal') = 'candidate'
                  AND json_extract(metadata, '$.source_message_id') IS NOT NULL
                  AND (? != 'formal' OR (
                       json_extract(metadata, '$.kind') = 'statement'
                       AND memory_type IN ('semantic', 'relationship', 'episodic')
                       AND user_id IS NOT NULL
                       AND conversation_id IS NOT NULL
                       AND importance >= 0.5
                       AND ? >= 0.8
                       AND json_extract(metadata, '$.source_user_id') = user_id
                       AND CAST(json_extract(metadata, '$.source_conversation_id') AS INTEGER) = conversation_id
                       AND EXISTS (
                           SELECT 1 FROM messages source
                            WHERE source.id = CAST(json_extract(memories.metadata, '$.source_message_id') AS INTEGER)
                              AND source.conversation_id = memories.conversation_id
                              AND json_extract(source.metadata, '$.author_kind') = 'user'
                              AND CAST(json_extract(source.metadata, '$.user_id') AS INTEGER) = memories.user_id
                       )
                  ))"#,
        )
        .bind(status)
        .bind(metadata.to_string())
        .bind(status)
        .bind(review.reviewed_at.to_rfc3339())
        .bind(review.candidate_id)
        .bind(status)
        .bind(review.confidence)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
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
        let rows:Vec<MemoryRow>=sqlx::query_as("SELECT id,character_id,user_id,conversation_id,memory_type,content,importance,created_at,last_accessed,metadata,embedding FROM memories WHERE character_id=? AND COALESCE(json_extract(metadata,'$.status'),'formal') NOT IN ('candidate','rejected','superseded') AND conversation_id=? ORDER BY importance DESC,created_at DESC,id DESC LIMIT ?")
            .bind(character_id).bind(conversation_id).bind(limit.max(0)).fetch_all(&self.pool).await?;
        rows.into_iter().map(parse_memory_row).collect()
    }

    async fn find_visible_for_user(
        &self,
        character_id: i64,
        conversation_id: i64,
        user_id: Option<i64>,
        limit: i64,
    ) -> Result<Vec<Memory>, RepositoryError> {
        let base = "SELECT id,character_id,user_id,conversation_id,memory_type,content,importance,created_at,last_accessed,metadata,embedding FROM memories WHERE character_id=? AND COALESCE(json_extract(metadata,'$.status'),'formal') NOT IN ('candidate','rejected','superseded') AND (conversation_id=?";
        let rows: Vec<MemoryRow> = match user_id {
            Some(user_id) => sqlx::query_as(&format!(
                "{base} OR (user_id=? AND COALESCE(json_extract(metadata,'$.shared'),0)=1 AND NOT EXISTS (SELECT 1 FROM memory_sharing_policies policy WHERE policy.character_id=memories.character_id AND policy.user_id=memories.user_id AND policy.status='confirmed'))) ORDER BY importance DESC,created_at DESC,id DESC LIMIT ?"
            ))
            .bind(character_id)
            .bind(conversation_id)
            .bind(user_id)
            .bind(limit.max(0))
            .fetch_all(&self.pool)
            .await?,
            None => sqlx::query_as(&format!(
                "{base}) ORDER BY importance DESC,created_at DESC,id DESC LIMIT ?"
            ))
            .bind(character_id)
            .bind(conversation_id)
            .bind(limit.max(0))
            .fetch_all(&self.pool)
            .await?,
        };
        rows.into_iter().map(parse_memory_row).collect()
    }

    async fn search_visible_by_keywords(
        &self,
        character_id: i64,
        conversation_id: i64,
        user_id: Option<i64>,
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
            " AND COALESCE(json_extract(metadata,'$.status'),'formal') NOT IN ('candidate','rejected','superseded') AND (conversation_id=",
        );
        builder.push_bind(conversation_id);
        match user_id {
            Some(user_id) => {
                builder.push(" OR (user_id=");
                builder.push_bind(user_id);
                builder.push(
                    " AND COALESCE(json_extract(metadata,'$.shared'),0)=1 AND NOT EXISTS (SELECT 1 FROM memory_sharing_policies policy WHERE policy.character_id=memories.character_id AND policy.user_id=memories.user_id AND policy.status='confirmed')) ) AND (",
                );
            }
            None => {
                builder.push(") AND (");
            }
        }
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
                       AND COALESCE(json_extract(metadata, '$.status'), 'formal') NOT IN ('candidate','rejected','superseded')
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
                  AND COALESCE(json_extract(metadata, '$.status'), 'formal') NOT IN ('candidate','rejected','superseded')
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
            " AND COALESCE(json_extract(metadata, '$.status'), 'formal') NOT IN ('candidate','rejected','superseded') AND (",
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

        Ok(result.last_insert_rowid())
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

    async fn apply_revision(
        &self,
        request: &MemoryRevisionRequest,
    ) -> Result<MemoryRevisionResult, RepositoryError> {
        let old = &request.expected_old;
        let old_metadata = serde_json::to_string(&old.metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let mut tx = self.pool.begin().await?;

        let existing_revision: Option<(i64, i64, String)> = sqlx::query_as(
            "SELECT old_memory_id, new_memory_id, operation FROM memory_revisions WHERE source_request_id = ?",
        )
        .bind(&request.source_request_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((old_memory_id, new_memory_id, _operation)) = existing_revision {
            tx.commit().await?;
            return Ok(MemoryRevisionResult {
                old_memory_id,
                new_memory_id,
                applied: false,
            });
        }

        let current_matches: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM memories
                WHERE id = ? AND content = ? AND importance = ?
                  AND datetime(last_accessed) = datetime(?) AND metadata = ?"#,
        )
        .bind(old.id)
        .bind(&old.content)
        .bind(old.importance)
        .bind(old.last_accessed.to_rfc3339())
        .bind(&old_metadata)
        .fetch_one(&mut *tx)
        .await?;
        if current_matches != 1 {
            let exists: Option<i64> = sqlx::query_scalar("SELECT id FROM memories WHERE id = ?")
                .bind(old.id)
                .fetch_optional(&mut *tx)
                .await?;
            tx.rollback().await?;
            return if exists.is_none() {
                Err(RepositoryError::NotFound(format!(
                    "memory {} not found",
                    old.id
                )))
            } else {
                Err(RepositoryError::Internal(
                    "记忆修订冲突：原记忆已被修改".into(),
                ))
            };
        }

        let mut new_metadata = request.metadata.clone();
        if !new_metadata.is_object() {
            new_metadata = serde_json::json!({});
        }
        new_metadata["status"] = serde_json::Value::String("formal".into());
        new_metadata["revision"] = serde_json::json!({
            "old_memory_id": old.id,
            "source_request_id": request.source_request_id.clone(),
            "reason": request.reason.clone(),
        });
        let new_metadata_json = serde_json::to_string(&new_metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let inserted = sqlx::query(
            r#"INSERT INTO memories
                (character_id, user_id, conversation_id, memory_type, content, importance,
                 created_at, last_accessed, metadata, embedding)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(old.character_id)
        .bind(old.user_id)
        .bind(old.conversation_id)
        .bind(memory_type_name(old.memory_type))
        .bind(&request.content)
        .bind(old.importance)
        .bind(request.applied_at.to_rfc3339())
        .bind(request.applied_at.to_rfc3339())
        .bind(&new_metadata_json)
        .bind(&old.embedding)
        .execute(&mut *tx)
        .await?;
        let new_memory_id = inserted.last_insert_rowid();

        let mut old_update_metadata = old.metadata.clone();
        if !old_update_metadata.is_object() {
            old_update_metadata = serde_json::json!({});
        }
        if matches!(request.operation, MemoryRevisionOperation::Replace) {
            old_update_metadata["status"] = serde_json::Value::String("superseded".into());
            old_update_metadata["superseded_by"] = serde_json::Value::from(new_memory_id);
            old_update_metadata["superseded_at"] =
                serde_json::Value::String(request.applied_at.to_rfc3339());
        }
        if let Some(edit_request) = new_metadata.get("edit_request") {
            old_update_metadata["edit_request"] = edit_request.clone();
        }
        old_update_metadata["revision"] = serde_json::json!({
            "new_memory_id": new_memory_id,
            "source_request_id": request.source_request_id.clone(),
            "reason": request.reason.clone(),
        });
        let old_update_json = serde_json::to_string(&old_update_metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let updated = sqlx::query(
            "UPDATE memories SET metadata = ?, last_accessed = ? WHERE id = ? AND content = ? AND importance = ? AND metadata = ?",
        )
        .bind(&old_update_json)
        .bind(request.applied_at.to_rfc3339())
        .bind(old.id)
        .bind(&old.content)
        .bind(old.importance)
        .bind(&old_metadata)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(RepositoryError::Internal(
                "记忆修订冲突：无法更新旧记忆".into(),
            ));
        }

        sqlx::query(
            r#"INSERT INTO memory_revisions
                (old_memory_id, new_memory_id, operation, reason, source_request_id, model_version, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(old.id)
        .bind(new_memory_id)
        .bind(request.operation.as_str())
        .bind(&request.reason)
        .bind(&request.source_request_id)
        .bind(&request.model_version)
        .bind(request.applied_at.to_rfc3339())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(MemoryRevisionResult {
            old_memory_id: old.id,
            new_memory_id,
            applied: true,
        })
    }

    async fn apply_claim_proposal(
        &self,
        proposal: &MemoryClaimProposal,
    ) -> Result<MemoryClaimResult, RepositoryError> {
        validate_claim_proposal(proposal)?;
        let mut tx = self.pool.begin().await?;
        let source: Option<(String, String, Option<i64>)> = sqlx::query_as(
            r#"SELECT memory_type,
                      COALESCE(json_extract(metadata, '$.status'), 'formal'),
                      user_id
                 FROM memories WHERE id = ?"#,
        )
        .bind(proposal.source_memory_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((memory_type, status, user_id)) = source else {
            return Err(RepositoryError::NotFound(format!(
                "memory {} not found",
                proposal.source_memory_id
            )));
        };
        if !matches!(memory_type.as_str(), "semantic" | "relationship")
            || status != "formal"
            || user_id != Some(proposal.subject_user_id)
        {
            return Err(RepositoryError::Internal(
                "claim 来源记忆未通过类型、状态或用户归属校验".into(),
            ));
        }

        if matches!(proposal.operation, MemoryClaimOperation::Reject) {
            tx.commit().await?;
            return Ok(MemoryClaimResult {
                claim_id: None,
                changed: false,
            });
        }

        if matches!(proposal.operation, MemoryClaimOperation::Replace) {
            sqlx::query(
                r#"UPDATE memory_claims
                      SET current = 0, valid_to = ?
                    WHERE claim_key = ? AND subject_user_id = ? AND current = 1"#,
            )
            .bind(proposal.proposed_at.to_rfc3339())
            .bind(&proposal.claim_key)
            .bind(proposal.subject_user_id)
            .execute(&mut *tx)
            .await?;
        }

        let result = sqlx::query(
            r#"INSERT INTO memory_claims
                (claim_key, subject_user_id, predicate, value, source_memory_id, valid_from, current)
             VALUES (?, ?, ?, ?, ?, ?, 1)"#,
        )
        .bind(&proposal.claim_key)
        .bind(proposal.subject_user_id)
        .bind(&proposal.predicate)
        .bind(&proposal.value)
        .bind(proposal.source_memory_id)
        .bind(proposal.proposed_at.to_rfc3339())
        .execute(&mut *tx)
        .await?;
        let claim_id = result.last_insert_rowid();
        tx.commit().await?;
        Ok(MemoryClaimResult {
            claim_id: Some(claim_id),
            changed: true,
        })
    }

    async fn find_current_claims(
        &self,
        claim_key: &str,
        subject_user_id: i64,
    ) -> Result<Vec<MemoryClaim>, RepositoryError> {
        let rows: Vec<ClaimRow> = sqlx::query_as(
            r#"SELECT id, claim_key, subject_user_id, predicate, value,
                      source_memory_id, valid_from, valid_to, current
                 FROM memory_claims
                WHERE claim_key = ? AND subject_user_id = ? AND current = 1
                ORDER BY id ASC"#,
        )
        .bind(claim_key)
        .bind(subject_user_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(parse_claim_row).collect()
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
               WHERE COALESCE(json_extract(metadata, '$.status'), 'formal') NOT IN ('candidate','rejected','superseded')
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
                 AND COALESCE(json_extract(metadata, '$.status'), 'formal') NOT IN ('candidate','rejected','superseded')
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
               FROM memories WHERE COALESCE(json_extract(metadata, '$.status'), 'formal') NOT IN ('candidate','rejected','superseded')"#,
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

type ClaimRow = (
    i64,
    String,
    i64,
    String,
    String,
    i64,
    String,
    Option<String>,
    i64,
);

fn validate_claim_proposal(proposal: &MemoryClaimProposal) -> Result<(), RepositoryError> {
    if proposal.source_memory_id <= 0 || proposal.subject_user_id <= 0 {
        return Err(RepositoryError::Internal("claim 来源或用户 ID 无效".into()));
    }
    if proposal.claim_key.trim().is_empty() || proposal.claim_key.chars().count() > 128 {
        return Err(RepositoryError::Internal("claim_key 长度无效".into()));
    }
    if proposal.predicate.trim().is_empty() || proposal.predicate.chars().count() > 128 {
        return Err(RepositoryError::Internal("claim predicate 长度无效".into()));
    }
    if proposal.value.trim().is_empty() || proposal.value.chars().count() > 2_000 {
        return Err(RepositoryError::Internal("claim value 长度无效".into()));
    }
    if proposal.reason.chars().count() > 500 {
        return Err(RepositoryError::Internal("claim reason 长度无效".into()));
    }
    Ok(())
}

fn parse_claim_row(row: ClaimRow) -> Result<MemoryClaim, RepositoryError> {
    let (
        id,
        claim_key,
        subject_user_id,
        predicate,
        value,
        source_memory_id,
        valid_from,
        valid_to,
        current,
    ) = row;
    Ok(MemoryClaim {
        id,
        claim_key,
        subject_user_id,
        predicate,
        value,
        source_memory_id,
        valid_from: super::timestamp::parse_timestamp(&valid_from)?,
        valid_to: valid_to
            .as_deref()
            .map(super::timestamp::parse_timestamp)
            .transpose()?,
        current: current != 0,
    })
}

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
