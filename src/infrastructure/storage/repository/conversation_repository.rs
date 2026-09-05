//! ConversationRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::conversation::{Conversation, ConversationType};
use crate::domain::repository::ConversationRepository;
use crate::error::RepositoryError;

pub struct SqliteConversationRepository {
    pool: SqlitePool,
}

impl SqliteConversationRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ConversationRepository for SqliteConversationRepository {
    async fn find_by_id(&self, id: i64) -> Result<Option<Conversation>, RepositoryError> {
        let row: Option<(i64, String, String, Option<String>, String, String)> = sqlx::query_as(
            r#"SELECT id, conversation_type, external_id, name, created_at, updated_at
                 FROM conversations WHERE id = ?"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(parse_conversation_row).transpose()
    }

    async fn find_by_external_id(
        &self,
        external_id: &str,
    ) -> Result<Option<Conversation>, RepositoryError> {
        let row: Option<(i64, String, String, Option<String>, String, String)> = sqlx::query_as(
            r#"SELECT id, conversation_type, external_id, name, created_at, updated_at
                 FROM conversations WHERE external_id = ?"#,
        )
        .bind(external_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(parse_conversation_row).transpose()
    }

    async fn find_all(&self) -> Result<Vec<Conversation>, RepositoryError> {
        let rows: Vec<(i64, String, String, Option<String>, String, String)> = sqlx::query_as(
            r#"SELECT id, conversation_type, external_id, name, created_at, updated_at
                 FROM conversations ORDER BY id"#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(parse_conversation_row).collect()
    }

    async fn insert(&self, conversation: &Conversation) -> Result<i64, RepositoryError> {
        let conv_type = match conversation.conversation_type {
            ConversationType::Private => "private",
            ConversationType::Group => "group",
        };

        let result = sqlx::query(
            r#"INSERT INTO conversations (conversation_type, external_id, name)
             VALUES (?, ?, ?)"#,
        )
        .bind(conv_type)
        .bind(&conversation.external_id)
        .bind(&conversation.name)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(result.last_insert_rowid())
    }

    async fn update(&self, conversation: &Conversation) -> Result<(), RepositoryError> {
        let conv_type = match conversation.conversation_type {
            ConversationType::Private => "private",
            ConversationType::Group => "group",
        };

        sqlx::query(
            r#"UPDATE conversations SET
                conversation_type = ?, external_id = ?, name = ?,
                updated_at = datetime('now')
             WHERE id = ?"#,
        )
        .bind(conv_type)
        .bind(&conversation.external_id)
        .bind(&conversation.name)
        .bind(conversation.id)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }

    async fn delete(&self, id: i64) -> Result<(), RepositoryError> {
        sqlx::query("DELETE FROM conversations WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(())
    }
}

type ConversationRow = (i64, String, String, Option<String>, String, String);

fn parse_conversation_row(row: ConversationRow) -> Result<Conversation, RepositoryError> {
    let (id, conv_type_str, external_id, name, created_at, updated_at) = row;

    let conversation_type = match conv_type_str.as_str() {
        "private" => ConversationType::Private,
        "group" => ConversationType::Group,
        other => {
            return Err(RepositoryError::Database(format!(
                "unknown conversation_type: {other}"
            )))
        }
    };

    let created_at = super::timestamp::parse_timestamp(&created_at)?;
    let updated_at = super::timestamp::parse_timestamp(&updated_at)?;

    Ok(Conversation {
        id,
        conversation_type,
        external_id,
        name,
        created_at,
        updated_at,
    })
}
