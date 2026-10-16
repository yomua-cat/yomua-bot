//! MessageRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::message::{Attachment, Message, MessageContent};
use crate::domain::repository::{AcceptedMessageInput, AcceptedMessageResult, MessageRepository};
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
    async fn find_cognition_batch(
        &self,
        character_id: i64,
        conversation_id: i64,
        after_message_id: i64,
        limit: i64,
    ) -> Result<Vec<Message>, RepositoryError> {
        let rows:Vec<MessageRow>=sqlx::query_as("SELECT id,conversation_id,sender_id,content,timestamp,reply_to,mentions,attachments,metadata,active_character_id,platform_message_id,conversation_sequence,occurred_at,ingested_at FROM messages WHERE active_character_id=? AND conversation_id=? AND id>? AND COALESCE(json_extract(metadata,'$.proactive'),0)!=1 ORDER BY id LIMIT ?")
        .bind(character_id).bind(conversation_id).bind(after_message_id).bind(limit.max(0)).fetch_all(&self.pool).await?;
        rows.into_iter().map(parse_message_row).collect()
    }
    async fn owns_outbox_lease(
        &self,
        message_id: i64,
        token: &str,
    ) -> Result<bool, RepositoryError> {
        let found:i64=sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events WHERE entity_id=? AND status='processing' AND lease_token=? AND locked_until > ?")
            .bind(message_id).bind(token).bind(chrono::Utc::now().to_rfc3339()).fetch_one(&self.pool).await?;
        Ok(found > 0)
    }
    async fn reply_task(
        &self,
        message_id: i64,
    ) -> Result<Option<crate::domain::message::ReplyTask>, RepositoryError> {
        let row: Option<(String, Option<String>, Option<String>, Option<i64>)> = sqlx::query_as(
            "SELECT status,content,error,platform_message_id FROM reply_tasks WHERE message_id=?",
        )
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(status, content, error, platform_message_id)| {
            let status = serde_json::from_value(serde_json::json!(status))
                .map_err(|e| RepositoryError::Internal(format!("回复任务状态无效：{e}")))?;
            Ok(crate::domain::message::ReplyTask {
                status,
                content,
                error,
                platform_message_id,
            })
        })
        .transpose()
    }

    async fn assistant_history_exists(
        &self,
        conversation_id: i64,
        reply_task_message_id: i64,
    ) -> Result<bool, RepositoryError> {
        let exists: i64 = sqlx::query_scalar(
            r#"SELECT EXISTS(
                SELECT 1
                  FROM messages
                 WHERE conversation_id = ?
                   AND json_extract(metadata, '$.reply_task') = ?
                   AND json_extract(metadata, '$.author_kind') = 'assistant'
            )"#,
        )
        .bind(conversation_id)
        .bind(reply_task_message_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists != 0)
    }
    async fn find_unknown_reply_tasks(
        &self,
        limit: i64,
    ) -> Result<Vec<crate::domain::message::UnknownReplyTask>, RepositoryError> {
        let rows: Vec<(i64, i64, Option<String>, Option<String>, Option<i64>, String)> = sqlx::query_as(
            "SELECT message_id,character_id,content,error,platform_message_id,updated_at FROM reply_tasks WHERE status='unknown' ORDER BY updated_at ASC,message_id ASC LIMIT ?",
        )
        .bind(limit.clamp(1, 1000))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(
                |(message_id, character_id, content, error, platform_message_id, updated_at)| {
                    let updated_at = super::timestamp::parse_timestamp(&updated_at)?;
                    Ok(crate::domain::message::UnknownReplyTask {
                        message_id,
                        character_id,
                        platform_message_id,
                        content,
                        error,
                        updated_at,
                    })
                },
            )
            .collect()
    }
    async fn find_unknown_reply_task(
        &self,
        message_id: i64,
    ) -> Result<Option<crate::domain::message::UnknownReplyTask>, RepositoryError> {
        let row: Option<(i64, i64, Option<String>, Option<String>, Option<i64>, String)> =
            sqlx::query_as(
                "SELECT message_id,character_id,content,error,platform_message_id,updated_at FROM reply_tasks WHERE message_id=? AND status='unknown'",
            )
            .bind(message_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(
            |(message_id, character_id, content, error, platform_message_id, updated_at)| {
                let updated_at = super::timestamp::parse_timestamp(&updated_at)?;
                Ok(crate::domain::message::UnknownReplyTask {
                    message_id,
                    character_id,
                    platform_message_id,
                    content,
                    error,
                    updated_at,
                })
            },
        )
        .transpose()
    }
    async fn save_reply_task(
        &self,
        message_id: i64,
        character_id: i64,
        status: crate::domain::message::ReplyTaskStatus,
        content: Option<&str>,
    ) -> Result<(), RepositoryError> {
        sqlx::query("INSERT INTO reply_tasks(message_id,character_id,status,content,error,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(message_id) DO UPDATE SET status=excluded.status,content=COALESCE(excluded.content,reply_tasks.content),error=COALESCE(excluded.error,reply_tasks.error),updated_at=excluded.updated_at")
            .bind(message_id).bind(character_id).bind(status.to_string()).bind(content).bind(None::<&str>).bind(chrono::Utc::now().to_rfc3339()).execute(&self.pool).await?;
        Ok(())
    }
    async fn save_reply_task_error(
        &self,
        message_id: i64,
        character_id: i64,
        status: crate::domain::message::ReplyTaskStatus,
        content: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), RepositoryError> {
        sqlx::query("INSERT INTO reply_tasks(message_id,character_id,status,content,error,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(message_id) DO UPDATE SET status=excluded.status,content=COALESCE(excluded.content,reply_tasks.content),error=excluded.error,updated_at=excluded.updated_at")
            .bind(message_id).bind(character_id).bind(status.to_string()).bind(content).bind(error).bind(chrono::Utc::now().to_rfc3339()).execute(&self.pool).await?;
        Ok(())
    }
    async fn mark_reply_task_unknown_if_sending(
        &self,
        message_id: i64,
        character_id: i64,
        error: &str,
    ) -> Result<bool, RepositoryError> {
        let result = sqlx::query(
            r#"UPDATE reply_tasks
               SET status = 'unknown', error = ?, updated_at = ?
               WHERE message_id = ? AND character_id = ? AND status = 'sending'"#,
        )
        .bind(error)
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(message_id)
        .bind(character_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }
    async fn save_reply_task_delivery(
        &self,
        message_id: i64,
        character_id: i64,
        status: crate::domain::message::ReplyTaskStatus,
        content: Option<&str>,
        error: Option<&str>,
        platform_message_id: Option<i64>,
    ) -> Result<(), RepositoryError> {
        sqlx::query("INSERT INTO reply_tasks(message_id,character_id,status,content,error,platform_message_id,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(message_id) DO UPDATE SET status=excluded.status,content=COALESCE(excluded.content,reply_tasks.content),error=excluded.error,platform_message_id=COALESCE(excluded.platform_message_id,reply_tasks.platform_message_id),updated_at=excluded.updated_at")
            .bind(message_id).bind(character_id).bind(status.to_string()).bind(content).bind(error).bind(platform_message_id).bind(chrono::Utc::now().to_rfc3339()).execute(&self.pool).await?;
        Ok(())
    }

    async fn requeue_unknown_reply_task(&self, message_id: i64) -> Result<bool, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(
            r#"UPDATE reply_tasks
               SET status = 'generated', error = NULL, updated_at = ?
               WHERE message_id = ? AND status = 'unknown'"#,
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(message_id)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(false);
        }

        let outbox = sqlx::query(
            r#"UPDATE outbox_events
               SET status = 'pending', available_at = ?, locked_until = NULL,
                   lease_token = NULL, processed_at = NULL, last_error = NULL,
                   attempts = 0
               WHERE entity_type = 'messages'
                 AND entity_id = ?
                 AND event_type = 'MessageAccepted'
                 AND status IN ('completed', 'dead_letter')"#,
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(message_id)
        .execute(&mut *tx)
        .await?;
        if outbox.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(RepositoryError::Database(
                "未知回复任务缺少可重试的 MessageAccepted 事件".into(),
            ));
        }
        tx.commit().await?;
        Ok(true)
    }

    async fn confirm_unknown_reply_task(
        &self,
        message_id: i64,
        platform_message_id: i64,
    ) -> Result<bool, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let task: Option<(i64, Option<String>, i64, Option<i64>)> = sqlx::query_as(
            r#"SELECT task.character_id, task.content, message.conversation_id,
                       message.conversation_sequence
                  FROM reply_tasks task
                  JOIN messages message ON message.id = task.message_id
                 WHERE task.message_id = ? AND task.status = 'unknown'"#,
        )
        .bind(message_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((character_id, content, conversation_id, conversation_sequence)) = task else {
            tx.rollback().await?;
            return Ok(false);
        };

        let updated = sqlx::query(
            r#"UPDATE reply_tasks
                  SET status = 'sent', platform_message_id = ?, error = NULL,
                      updated_at = ?
                WHERE message_id = ? AND status = 'unknown'"#,
        )
        .bind(platform_message_id)
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(message_id)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(false);
        }

        if let Some(content) = content {
            let external_id = format!("character:{character_id}");
            sqlx::query(
                r#"INSERT OR IGNORE INTO participants
                     (conversation_id, external_id, display_name, role, metadata)
                   SELECT ?, ?, name, 'character', json_object(
                       'author_kind', 'assistant', 'character_id', ?
                   )
                     FROM characters WHERE id = ?"#,
            )
            .bind(conversation_id)
            .bind(&external_id)
            .bind(character_id)
            .bind(character_id)
            .execute(&mut *tx)
            .await?;
            let assistant_id: i64 = sqlx::query_scalar(
                "SELECT id FROM participants WHERE conversation_id = ? AND external_id = ?",
            )
            .bind(conversation_id)
            .bind(&external_id)
            .fetch_one(&mut *tx)
            .await?;
            sqlx::query(
                r#"INSERT INTO messages
                     (conversation_id, sender_id, content, timestamp, reply_to,
                      mentions, attachments, metadata, active_character_id,
                      platform_message_id, conversation_sequence, occurred_at, ingested_at)
                   SELECT ?, ?, ?, ?, ?, '[]', '[]', ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM messages
                         WHERE conversation_id = ?
                           AND json_extract(metadata, '$.reply_task') = ?
                           AND json_extract(metadata, '$.author_kind') = 'assistant'
                    )"#,
            )
            .bind(conversation_id)
            .bind(assistant_id)
            .bind(serde_json::json!({"Text": content}).to_string())
            .bind(chrono::Utc::now().to_rfc3339())
            .bind(message_id)
            .bind(
                serde_json::json!({
                    "author_kind": "assistant",
                    "reply_task": message_id,
                })
                .to_string(),
            )
            .bind(character_id)
            .bind(platform_message_id)
            .bind(conversation_sequence)
            .bind(chrono::Utc::now().to_rfc3339())
            .bind(chrono::Utc::now().to_rfc3339())
            .bind(conversation_id)
            .bind(message_id)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(true)
    }

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
               ORDER BY timestamp DESC, id DESC LIMIT ?"#,
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
               ORDER BY conversation_sequence DESC
               LIMIT ?"#,
        )
        .bind(conversation_id)
        .bind(character_id)
        .bind(before_sequence)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut messages: Vec<Message> = rows
            .into_iter()
            .map(parse_message_row)
            .collect::<Result<Vec<_>, _>>()?;

        messages.reverse();
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
        input: &AcceptedMessageInput,
    ) -> Result<(i64, i64, chrono::DateTime<chrono::Utc>), RepositoryError> {
        let result = self.accept_message_with_status(input).await?;
        Ok((
            result.message_id,
            result.conversation_sequence,
            result.ingested_at,
        ))
    }

    async fn accept_message_with_status(
        &self,
        input: &AcceptedMessageInput,
    ) -> Result<AcceptedMessageResult, RepositoryError> {
        let conversation_id = input.conversation_id;
        let sender_id = input.sender_id;
        let content_json = &input.content_json;
        let timestamp = &input.timestamp;
        let platform_message_id = input.platform_message_id;
        let character_id = input.character_id;
        let event_uuid = &input.event_uuid;
        let outbox_payload = &input.outbox_payload;
        let mut tx = self.pool.begin().await?;
        // 先取得写锁再读取去重记录，避免延迟事务升级时竞争。
        sqlx::query("UPDATE conversations SET sequence_counter = sequence_counter WHERE id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await?;
        if let Some(platform_id) = platform_message_id {
            let existing: Option<(i64,i64,String)> = sqlx::query_as("SELECT id, conversation_sequence, ingested_at FROM messages WHERE conversation_id = ? AND platform_message_id = ? ORDER BY id LIMIT 1")
                .bind(conversation_id).bind(platform_id).fetch_optional(&mut *tx).await?;
            if let Some((id, seq, at)) = existing {
                tx.commit().await?;
                return Ok(AcceptedMessageResult {
                    message_id: id,
                    conversation_sequence: seq,
                    ingested_at: super::timestamp::parse_timestamp(&at)?,
                    created: false,
                });
            }
        }
        let conversation_sequence: i64 = sqlx::query_scalar("UPDATE conversations SET sequence_counter = COALESCE(sequence_counter,0) + 1 WHERE id = ? RETURNING sequence_counter")
            .bind(conversation_id).fetch_one(&mut *tx).await?;
        let mut payload: serde_json::Value = serde_json::from_str(outbox_payload)
            .map_err(|e| RepositoryError::Internal(e.to_string()))?;
        let metadata = payload
            .get("metadata")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({"author_kind":"user"}));
        // OneBot 的引用 ID 属于平台命名空间，不能直接当作本地 messages.id。
        // 只有在同一会话找到对应平台消息时，才写入本地 reply_to 外键；
        // 找不到时仍保留 metadata 中的平台引用，避免伪造关系或因外键错误丢消息。
        let reply_to = if let Some(platform_reply_id) = metadata
            .get("reply_to_platform_message_id")
            .and_then(|value| value.as_i64())
        {
            sqlx::query_scalar::<_, i64>(
                "SELECT id FROM messages WHERE conversation_id = ? AND platform_message_id = ? ORDER BY id LIMIT 1",
            )
            .bind(conversation_id)
            .bind(platform_reply_id)
            .fetch_optional(&mut *tx)
            .await?
        } else {
            None
        };
        // 3. 生成 ingested_at（事务内的 authoritative timestamp）
        let ingested_at = chrono::Utc::now();

        // 4. 插入消息（不包含 local_message_id，使用 messages.id 作为 canonical identity）
        let result = sqlx::query(
            r#"INSERT INTO messages
                (conversation_id, sender_id, content, timestamp, reply_to, mentions,
                 attachments, metadata, active_character_id,
                 platform_message_id, conversation_sequence, occurred_at, ingested_at)
              VALUES (?, ?, ?, ?, ?, ?, '[]', ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(conversation_id)
        .bind(sender_id)
        .bind(content_json)
        .bind(timestamp)
        .bind(reply_to)
        .bind(
            metadata
                .get("mentions")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([]))
                .to_string(),
        )
        .bind(metadata.to_string())
        .bind(character_id)
        .bind(platform_message_id)
        .bind(conversation_sequence)
        .bind(timestamp) // occurred_at = platform timestamp
        .bind(ingested_at.to_rfc3339())
        .execute(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let message_id = result.last_insert_rowid();

        payload["message_id"] = serde_json::json!(message_id);
        payload["conversation_sequence"] = serde_json::json!(conversation_sequence);
        payload["ingested_at"] = serde_json::json!(ingested_at);
        // 完整事件与消息在同一事务中提交。（与 message 在同一事务中）
        sqlx::query(
            r#"INSERT INTO outbox_events
                (event_uuid, entity_id, entity_type, event_type, event_version,
                 payload, ordering_key, ordering_seq, status)
              VALUES (?, ?, 'messages', 'MessageAccepted', 1, ?, ?, ?, 'pending')"#,
        )
        .bind(event_uuid)
        .bind(message_id)
        .bind(payload.to_string())
        .bind(conversation_id.to_string())
        .bind(conversation_sequence)
        .execute(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        // 6. 提交事务
        tx.commit()
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(AcceptedMessageResult {
            message_id,
            conversation_sequence,
            ingested_at,
            created: true,
        })
    }
}
