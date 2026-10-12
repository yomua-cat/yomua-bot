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

    /// 获取底层连接池（用于在辅助函数中构造新的同类实例）。
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
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
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
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
        active_character_id,
        platform_message_id,
        conversation_sequence,
        occurred_at,
        ingested_at,
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

    let occurred_at = occurred_at
        .map(|s| super::timestamp::parse_timestamp(&s))
        .transpose()?;
    let ingested_at = ingested_at
        .map(|s| super::timestamp::parse_timestamp(&s))
        .transpose()?;

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
        active_character_id,
        platform_message_id,
        conversation_sequence,
        occurred_at,
        ingested_at,
    })
}

#[async_trait]
impl MessageRepository for SqliteMessageRepository {
    async fn find_by_id(&self, id: i64) -> Result<Option<Message>, RepositoryError> {
        let row: Option<MessageRow> = sqlx::query_as(
            r#"SELECT id, conversation_id, sender_id, content, timestamp,
                    reply_to, mentions, attachments, metadata, active_character_id,
                    platform_message_id, conversation_sequence,
                    occurred_at, ingested_at
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
        let rows: Vec<MessageRow> = sqlx::query_as(
            r#"SELECT id, conversation_id, sender_id, content, timestamp,
                    reply_to, mentions, attachments, metadata, active_character_id,
                    platform_message_id, conversation_sequence,
                    occurred_at, ingested_at
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

    async fn find_recent_attributed(
        &self,
        conversation_id: i64,
        character_id: i64,
        limit: i64,
    ) -> Result<Vec<Message>, RepositoryError> {
        let rows: Vec<MessageRow> = sqlx::query_as(
            r#"SELECT id, conversation_id, sender_id, content, timestamp,
                    reply_to, mentions, attachments, metadata, active_character_id,
                    platform_message_id, conversation_sequence,
                    occurred_at, ingested_at
               FROM messages
               WHERE conversation_id = ? AND active_character_id = ?
               ORDER BY timestamp DESC, id DESC
               LIMIT ?"#,
        )
        .bind(conversation_id)
        .bind(character_id)
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

        let occurred_at = message.occurred_at.map(|dt| dt.to_rfc3339());
        let ingested_at = message.ingested_at.map(|dt| dt.to_rfc3339());

        let result = sqlx::query(
            r#"INSERT INTO messages
                (conversation_id, sender_id, content, timestamp, reply_to, mentions,
                 attachments, metadata, active_character_id,
                 platform_message_id, conversation_sequence, occurred_at, ingested_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(message.conversation_id)
        .bind(message.sender_id)
        .bind(&content_json)
        .bind(message.timestamp.to_rfc3339())
        .bind(message.reply_to)
        .bind(&mentions_json)
        .bind(&attachments_json)
        .bind(&metadata_json)
        .bind(message.active_character_id)
        .bind(message.platform_message_id)
        .bind(message.conversation_sequence)
        .bind(&occurred_at)
        .bind(&ingested_at)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(result.last_insert_rowid())
    }

    async fn update_active_character_id(
        &self,
        message_id: i64,
        active_character_id: i64,
    ) -> Result<(), RepositoryError> {
        sqlx::query(r#"UPDATE messages SET active_character_id = ? WHERE id = ?"#)
            .bind(active_character_id)
            .bind(message_id)
            .execute(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(())
    }

    async fn find_by_conversation_sender_time_content(
        &self,
        conversation_id: i64,
        sender_id: i64,
        timestamp: chrono::DateTime<chrono::Utc>,
        content: &str,
    ) -> Result<Option<Message>, RepositoryError> {
        let row: Option<MessageRow> = sqlx::query_as(
            r#"SELECT id, conversation_id, sender_id, content, timestamp,
                    reply_to, mentions, attachments, metadata, active_character_id,
                    platform_message_id, conversation_sequence,
                    occurred_at, ingested_at
               FROM messages
               WHERE conversation_id = ? AND sender_id = ? AND timestamp = ? AND content = ?"#,
        )
        .bind(conversation_id)
        .bind(sender_id)
        .bind(timestamp.to_rfc3339())
        .bind(content)
        .fetch_optional(&self.pool)
        .await?;

        row.map(parse_message_row).transpose()
    }

    async fn latest_message_time(
        &self,
        conversation_id: i64,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, RepositoryError> {
        let row: Option<(String,)> =
            sqlx::query_as(r#"SELECT MAX(timestamp) FROM messages WHERE conversation_id = ?"#)
                .bind(conversation_id)
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some((timestamp,)) => {
                let dt = super::timestamp::parse_timestamp(&timestamp)?;
                Ok(Some(dt))
            }
            None => Ok(None),
        }
    }

    async fn find_before_sequence(
        &self,
        conversation_id: i64,
        character_id: i64,
        before_sequence: i64,
        limit: i64,
    ) -> Result<Vec<Message>, RepositoryError> {
        let rows: Vec<MessageRow> = sqlx::query_as(
            r#"SELECT id, conversation_id, sender_id, content, timestamp,
                    reply_to, mentions, attachments, metadata, active_character_id,
                    platform_message_id, conversation_sequence,
                    occurred_at, ingested_at
               FROM messages
               WHERE conversation_id = ?
                 AND active_character_id = ?
                 AND conversation_sequence < ?
               ORDER BY conversation_sequence ASC
               LIMIT ?"#,
        )
        .bind(conversation_id)
        .bind(character_id)
        .bind(before_sequence)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let messages: Vec<Message> = rows
            .into_iter()
            .map(parse_message_row)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(messages)
    }

    async fn find_recent_by_sequence(
        &self,
        conversation_id: i64,
        limit: i64,
    ) -> Result<Vec<Message>, RepositoryError> {
        let rows: Vec<MessageRow> = sqlx::query_as(
            r#"SELECT id, conversation_id, sender_id, content, timestamp,
                    reply_to, mentions, attachments, metadata, active_character_id,
                    platform_message_id, conversation_sequence,
                    occurred_at, ingested_at
               FROM messages
               WHERE conversation_id = ?
               ORDER BY conversation_sequence DESC
               LIMIT ?"#,
        )
        .bind(conversation_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut messages: Vec<Message> = rows
            .into_iter()
            .map(parse_message_row)
            .collect::<Result<Vec<_>, _>>()?;

        // 反转顺序，使最旧的消息排在前面
        messages.reverse();
        Ok(messages)
    }

    async fn accept_message(
        &self,
        conversation_id: i64,
        sender_id: i64,
        content_json: &str,
        timestamp: &str,
        platform_message_id: Option<i64>,
        character_id: i64,
        event_uuid: &str,
        outbox_payload: &str,
    ) -> Result<(i64, i64, chrono::DateTime<chrono::Utc>), RepositoryError> {
        // 使用排他事务，保证 sequence allocation 和 insert 原子性
        let mut tx = self
            .pool
            .acquire()
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let mut tx = sqlx::Acquire::begin(&mut *tx)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        // 1. 原子分配 sequence（在同一个事务内）
        let row: (i64,) = sqlx::query_as(
            "SELECT COALESCE(sequence_counter, 0) + 1 FROM conversations WHERE id = ?",
        )
        .bind(conversation_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let conversation_sequence = row.0;

        // 2. 更新 sequence_counter
        sqlx::query("UPDATE conversations SET sequence_counter = ? WHERE id = ?")
            .bind(conversation_sequence)
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        // 3. 生成 ingested_at（事务内的 authoritative timestamp）
        let ingested_at = chrono::Utc::now();

        // 4. 插入消息（不包含 local_message_id，使用 messages.id 作为 canonical identity）
        let result = sqlx::query(
            r#"INSERT INTO messages
                (conversation_id, sender_id, content, timestamp, reply_to, mentions,
                 attachments, metadata, active_character_id,
                 platform_message_id, conversation_sequence, occurred_at, ingested_at)
              VALUES (?, ?, ?, ?, NULL, '[]', '[]', '{}', ?, ?, ?, ?, ?)"#,
        )
        .bind(conversation_id)
        .bind(sender_id)
        .bind(content_json)
        .bind(timestamp)
        .bind(character_id)
        .bind(platform_message_id)
        .bind(conversation_sequence)
        .bind(timestamp) // occurred_at = platform timestamp
        .bind(ingested_at.to_rfc3339())
        .execute(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let message_id = result.last_insert_rowid();

        // 5. C2-4: 插入 durable outbox event（与 message 在同一事务中）
        sqlx::query(
            r#"INSERT INTO outbox_events
                (event_uuid, entity_id, entity_type, event_type, event_version,
                 payload, ordering_key, ordering_seq, status)
              VALUES (?, ?, 'messages', 'MessageAccepted', 1, ?, ?, ?, 'pending')"#,
        )
        .bind(event_uuid)
        .bind(message_id)
        .bind(outbox_payload)
        .bind(conversation_id.to_string())
        .bind(conversation_sequence)
        .execute(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        // 6. 提交事务
        tx.commit()
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok((message_id, conversation_sequence, ingested_at))
    }
}
