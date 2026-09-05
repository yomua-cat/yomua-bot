//! MessageRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::message::{Attachment, Message, MessageContent};
use crate::domain::repository::MessageRepository;
use crate::error::RepositoryError;

pub struct SqliteMessageRepository {
    pool: SqlitePool,
}

impl SqliteMessageRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl MessageRepository for SqliteMessageRepository {
    async fn find_by_id(&self, id: i64) -> Result<Option<Message>, RepositoryError> {
        let row: Option<(
            i64,
            i64,
            i64,
            String,
            String,
            Option<i64>,
            String,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT id, conversation_id, sender_id, content, timestamp,
                    reply_to, mentions, attachments, metadata
                 FROM messages WHERE id = ?"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(parse_message_row).transpose()
    }

    async fn find_recent(
        &self,
        conversation_id: i64,
        limit: i64,
    ) -> Result<Vec<Message>, RepositoryError> {
        let rows: Vec<(
            i64,
            i64,
            i64,
            String,
            String,
            Option<i64>,
            String,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT id, conversation_id, sender_id, content, timestamp,
                    reply_to, mentions, attachments, metadata
                 FROM messages WHERE conversation_id = ?
                 ORDER BY timestamp DESC LIMIT ?"#,
        )
        .bind(conversation_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut messages: Vec<Message> = rows
            .into_iter()
            .map(parse_message_row)
            .collect::<Result<Vec<_>, _>>()?;

        // 反转顺序，使最旧的消息排在前面（自然的先后顺序）
        messages.reverse();
        Ok(messages)
    }

    async fn insert(&self, message: &Message) -> Result<i64, RepositoryError> {
        let content_json = serde_json::to_string(&message.content)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let mentions_json = serde_json::to_string(&message.mentions)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let attachments_json = serde_json::to_string(&message.attachments)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let metadata_json = serde_json::to_string(&message.metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let result = sqlx::query(
            r#"INSERT INTO messages
                (conversation_id, sender_id, content, timestamp, reply_to, mentions, attachments, metadata)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(message.conversation_id)
        .bind(message.sender_id)
        .bind(&content_json)
        .bind(message.timestamp.to_rfc3339())
        .bind(message.reply_to)
        .bind(&mentions_json)
        .bind(&attachments_json)
        .bind(&metadata_json)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(result.last_insert_rowid())
    }
}

type MessageRow = (
    i64,
    i64,
    i64,
    String,
    String,
    Option<i64>,
    String,
    String,
    String,
);

fn parse_message_row(row: MessageRow) -> Result<Message, RepositoryError> {
    let (
        id,
        conversation_id,
        sender_id,
        content_json,
        timestamp,
        reply_to,
        mentions_json,
        attachments_json,
        metadata_json,
    ) = row;

    let content: MessageContent = serde_json::from_str(&content_json)
        .map_err(|e| RepositoryError::Database(format!("invalid content JSON: {e}")))?;
    let mentions: Vec<i64> = serde_json::from_str(&mentions_json)
        .map_err(|e| RepositoryError::Database(format!("invalid mentions JSON: {e}")))?;
    let attachments: Vec<Attachment> = serde_json::from_str(&attachments_json)
        .map_err(|e| RepositoryError::Database(format!("invalid attachments JSON: {e}")))?;
    let metadata: serde_json::Value = serde_json::from_str(&metadata_json)
        .map_err(|e| RepositoryError::Database(format!("invalid metadata JSON: {e}")))?;

    let timestamp = super::timestamp::parse_timestamp(&timestamp)?;

    Ok(Message {
        id,
        conversation_id,
        sender_id,
        content,
        timestamp,
        reply_to,
        mentions,
        attachments,
        metadata,
    })
}
