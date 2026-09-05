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
                r#"SELECT id, character_id, conversation_id, memory_type, content,
                        importance, created_at, last_accessed, metadata, embedding
                     FROM memories WHERE character_id = ? AND memory_type = ?
                     ORDER BY importance DESC, created_at DESC LIMIT ?"#,
            )
            .bind(character_id)
            .bind(t)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                r#"SELECT id, character_id, conversation_id, memory_type, content,
                        importance, created_at, last_accessed, metadata, embedding
                     FROM memories WHERE character_id = ?
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
            "SELECT id, character_id, conversation_id, memory_type, content, importance, \
             created_at, last_accessed, metadata, embedding \
             FROM memories WHERE character_id = ",
        );
        builder.push_bind(character_id);
        builder.push(" AND (");
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
        let memory_type_str = match memory.memory_type {
            MemoryType::Episodic => "episodic",
            MemoryType::Semantic => "semantic",
            MemoryType::Relationship => "relationship",
            MemoryType::System => "system",
        };
        let metadata = serde_json::to_string(&memory.metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let result = sqlx::query(
            r#"INSERT INTO memories
                (character_id, conversation_id, memory_type, content, importance, metadata, embedding)
             VALUES (?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(memory.character_id)
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

        sqlx::query(
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

        Ok(())
    }

    async fn delete(&self, id: i64) -> Result<(), RepositoryError> {
        sqlx::query("DELETE FROM memories WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(())
    }

    async fn search_by_embedding(
        &self,
        character_id: i64,
        query_embedding: &[f32],
        memory_type: Option<&str>,
        limit: i64,
    ) -> Result<Vec<SemanticMatchResult>, RepositoryError> {
        // 从 semantic_memories 表读取同角色、同类型的全部记录。
        let type_filter = memory_type.map(|t| format!("AND memory_type = '{}'", t));
        let query = if let Some(tf) = &type_filter {
            format!(
                r#"SELECT id, character_id, conversation_id, memory_type, content,
                          importance, created_at, last_accessed, metadata, embedding
                   FROM semantic_memories
                   WHERE character_id = {} {}
                   ORDER BY importance DESC, created_at DESC"#,
                character_id, tf
            )
        } else {
            format!(
                r#"SELECT id, character_id, conversation_id, memory_type, content,
                          importance, created_at, last_accessed, metadata, embedding
                   FROM semantic_memories
                   WHERE character_id = {}
                   ORDER BY importance DESC, created_at DESC"#,
                character_id
            )
        };

        let rows: Vec<MemoryRow> = sqlx::query_as(&query)
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
        scored.truncate(limit as usize);
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

        Ok(result.last_insert_rowid())
    }
}

type MemoryRow = (
    i64,
    i64,
    Option<i64>,
    String,
    String,
    f64,
    String,
    String,
    String,
    Option<String>,
);

fn parse_memory_row(row: MemoryRow) -> Result<Memory, RepositoryError> {
    let (
        id,
        character_id,
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
