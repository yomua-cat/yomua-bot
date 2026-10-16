//! ParticipantRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::conversation::{Participant, ParticipantRole};
use crate::domain::repository::ParticipantRepository;
use crate::error::RepositoryError;

pub struct SqliteParticipantRepository {
    pool: SqlitePool,
}

impl SqliteParticipantRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ParticipantRepository for SqliteParticipantRepository {
    async fn find_by_id(&self, id: i64) -> Result<Option<Participant>, RepositoryError> {
        let row: Option<ParticipantRow> = sqlx::query_as(
            r#"SELECT id, conversation_id, external_id, display_name, role, metadata, user_id
             FROM participants WHERE id = ?"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(parse_participant_row).transpose()
    }

    async fn find_by_external_id(
        &self,
        conversation_id: i64,
        external_id: &str,
    ) -> Result<Option<Participant>, RepositoryError> {
        let row: Option<ParticipantRow> = sqlx::query_as(
            r#"SELECT id, conversation_id, external_id, display_name, role, metadata, user_id
             FROM participants WHERE conversation_id = ? AND external_id = ?"#,
        )
        .bind(conversation_id)
        .bind(external_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(parse_participant_row).transpose()
    }

    async fn find_by_conversation_id(
        &self,
        conversation_id: i64,
    ) -> Result<Vec<Participant>, RepositoryError> {
        let rows: Vec<ParticipantRow> = sqlx::query_as(
            r#"SELECT id, conversation_id, external_id, display_name, role, metadata, user_id
             FROM participants WHERE conversation_id = ? ORDER BY id"#,
        )
        .bind(conversation_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(parse_participant_row).collect()
    }

    async fn insert(&self, participant: &Participant) -> Result<i64, RepositoryError> {
        let role_str = match participant.role {
            ParticipantRole::User => "user",
            ParticipantRole::Character => "character",
            ParticipantRole::System => "system",
        };

        let mut metadata_value = participant.metadata.clone();
        let user_id = if participant.role == ParticipantRole::User {
            let platform = metadata_value
                .get("platform")
                .and_then(|value| value.as_str())
                .unwrap_or("onebot_qq");
            let user_id = self
                .ensure_user(platform, &participant.external_id)
                .await?
                .ok_or_else(|| RepositoryError::Database("无法创建全域用户".into()))?;
            metadata_value["platform"] = serde_json::json!(platform);
            metadata_value["user_id"] = serde_json::json!(user_id);
            Some(user_id)
        } else {
            None
        };
        let metadata = serde_json::to_string(&metadata_value)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let result = sqlx::query(
            r#"INSERT INTO participants (conversation_id, external_id, display_name, role, metadata, user_id)
             VALUES (?, ?, ?, ?, ?, ?)"#,
        )
        .bind(participant.conversation_id)
        .bind(&participant.external_id)
        .bind(&participant.display_name)
        .bind(role_str)
        .bind(&metadata)
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(result.last_insert_rowid())
    }

    async fn update(&self, participant: &Participant) -> Result<(), RepositoryError> {
        let metadata = serde_json::to_string(&participant.metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        sqlx::query("UPDATE participants SET display_name = ?, metadata = ? WHERE id = ?")
            .bind(&participant.display_name)
            .bind(metadata)
            .bind(participant.id)
            .execute(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(())
    }

    async fn ensure_user(
        &self,
        platform: &str,
        external_id: &str,
    ) -> Result<Option<i64>, RepositoryError> {
        let platform = platform.trim();
        let external_id = external_id.trim();
        if platform.is_empty() || external_id.is_empty() {
            return Err(RepositoryError::Database("平台或外部用户 ID 为空".into()));
        }
        sqlx::query("INSERT OR IGNORE INTO users(platform, external_id) VALUES (?, ?)")
            .bind(platform)
            .bind(external_id)
            .execute(&self.pool)
            .await?;
        let id: i64 =
            sqlx::query_scalar("SELECT id FROM users WHERE platform = ? AND external_id = ?")
                .bind(platform)
                .bind(external_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(Some(id))
    }

    async fn merge_users(
        &self,
        source_user_id: i64,
        target_user_id: i64,
    ) -> Result<i64, RepositoryError> {
        if source_user_id == target_user_id {
            return Ok(target_user_id);
        }
        let mut tx = self.pool.begin().await?;
        let identities: Vec<(String, String)> = sqlx::query_as(
            "SELECT platform, external_id FROM users WHERE id IN (?, ?) ORDER BY id",
        )
        .bind(source_user_id)
        .bind(target_user_id)
        .fetch_all(&mut *tx)
        .await?;
        if identities.len() != 2 {
            return Err(RepositoryError::NotFound("待合并用户不存在".into()));
        }
        if identities[0].0 != identities[1].0 {
            return Err(RepositoryError::Database(
                "只能合并同一平台的全域用户".into(),
            ));
        }
        let source_external_id = identities[0].1.clone();

        // 先清理历史副本中的源账号标识。消息正文保留，作者仍通过 sender_id
        // 和归并后的 user_id 可追溯，但旧平台 ID、昵称和适配器元数据不再泄露。
        sqlx::query(
            r#"UPDATE messages
                  SET metadata = json_remove(
                      json_set(CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                               '$.author_kind', 'user', '$.display_name', '已合并用户'),
                      '$.external_sender_id', '$.user_id', '$.platform', '$.stable_user_key')
                WHERE sender_id IN (SELECT id FROM participants WHERE user_id = ?)"#,
        )
        .bind(source_user_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"UPDATE messages
                  SET metadata = json_set(
                      CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                      '$.mentions', json(COALESCE((
                          SELECT json_group_array(value)
                            FROM json_each(CASE WHEN json_valid(messages.metadata) THEN messages.metadata ELSE '{}' END, '$.mentions')
                           WHERE CAST(value AS TEXT) <> ?
                      ), '[]')))
                WHERE json_valid(metadata)
                  AND json_type(metadata, '$.mentions') = 'array'
                  AND EXISTS (
                      SELECT 1 FROM json_each(metadata, '$.mentions')
                       WHERE CAST(value AS TEXT) = ?
                  )"#,
        )
        .bind(&source_external_id)
        .bind(&source_external_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"UPDATE outbox_events
                  SET payload = json_remove(
                      json_set(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,
                               '$.metadata.author_kind', 'user',
                               '$.metadata.display_name', '已合并用户'),
                      '$.metadata.external_sender_id', '$.metadata.user_id', '$.metadata.platform', '$.metadata.stable_user_key')
                WHERE entity_type = 'messages'
                  AND entity_id IN (
                      SELECT id FROM messages
                       WHERE sender_id IN (SELECT id FROM participants WHERE user_id = ?)
                  )
                  AND json_valid(payload)"#,
        )
        .bind(source_user_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"UPDATE outbox_events
                  SET payload = json_set(
                      payload, '$.metadata.mentions', json(COALESCE((
                          SELECT json_group_array(value)
                            FROM json_each(payload, '$.metadata.mentions')
                           WHERE CAST(value AS TEXT) <> ?
                      ), '[]')))
                WHERE json_valid(payload)
                  AND json_type(payload, '$.metadata.mentions') = 'array'
                  AND EXISTS (
                      SELECT 1 FROM json_each(payload, '$.metadata.mentions')
                       WHERE CAST(value AS TEXT) = ?
                  )"#,
        )
        .bind(&source_external_id)
        .bind(&source_external_id)
        .execute(&mut *tx)
        .await?;

        // 旧私聊路由不能继续使用已经合并的账号 ID。
        sqlx::query(
            r#"UPDATE conversation_bindings
                  SET proactive_enabled = 0
                WHERE conversation_id IN (
                    SELECT id FROM conversations
                     WHERE conversation_type = 'private' AND external_id = ?
                )"#,
        )
        .bind(&source_external_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"UPDATE conversations
                  SET external_id = 'merged-user-conversation-' || id,
                      name = '已合并用户'
                WHERE conversation_type = 'private' AND external_id = ?"#,
        )
        .bind(&source_external_id)
        .execute(&mut *tx)
        .await?;

        // 在切换 user_id 前匿名化源参与者，否则后续条件已无法再定位源身份。
        sqlx::query(
            r#"UPDATE participants
                  SET external_id = 'merged-user-' || id,
                      display_name = '已合并用户',
                      metadata = json_object('merged_into_user_id', ?)
                WHERE user_id = ?"#,
        )
        .bind(target_user_id)
        .bind(source_user_id)
        .execute(&mut *tx)
        .await?;

        // 关系和记忆按 user_id 归并；关系状态由迁移同样的规则合并。
        sqlx::query("UPDATE participants SET user_id = ? WHERE user_id = ?")
            .bind(target_user_id)
            .bind(source_user_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE memories SET user_id = ? WHERE user_id = ?")
            .bind(target_user_id)
            .bind(source_user_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE semantic_memories SET user_id = ? WHERE user_id = ?")
            .bind(target_user_id)
            .bind(source_user_id)
            .execute(&mut *tx)
            .await?;

        // 合并关系交互幂等键，确保来源消息在合并后重放仍只计一次。
        sqlx::query(
            r#"INSERT OR IGNORE INTO relationship_interactions
                 (character_id, user_id, source_message_id, created_at)
               SELECT character_id, ?, source_message_id, created_at
                 FROM relationship_interactions
                WHERE user_id = ?"#,
        )
        .bind(target_user_id)
        .bind(source_user_id)
        .execute(&mut *tx)
        .await?;

        // 关系合并规则与 016 迁移一致：交互次数求和，状态取最大值，
        // 创建时间取最早，最后交互和更新时间取最新。
        sqlx::query(
            r#"INSERT INTO relationships
                 (character_id,user_id,familiarity,affection,trust,respect,annoyance,intimacy,
                  interaction_count,last_interaction,created_at,updated_at)
               SELECT character_id, ?, MAX(familiarity), MAX(affection), MAX(trust), MAX(respect),
                      MAX(annoyance), MAX(intimacy), SUM(interaction_count), MAX(last_interaction),
                      MIN(created_at), MAX(updated_at)
                 FROM relationships WHERE user_id IN (?, ?)
                GROUP BY character_id
               ON CONFLICT(character_id,user_id) DO UPDATE SET
                 familiarity=excluded.familiarity, affection=excluded.affection,
                 trust=excluded.trust, respect=excluded.respect, annoyance=excluded.annoyance,
                 intimacy=excluded.intimacy, interaction_count=excluded.interaction_count,
                 last_interaction=excluded.last_interaction, created_at=excluded.created_at,
                 updated_at=excluded.updated_at"#,
        )
        .bind(target_user_id)
        .bind(source_user_id)
        .bind(target_user_id)
        .execute(&mut *tx)
        .await?;
        // 关系表使用复合主键，合并后删除来源行；目标行已保留聚合结果。
        sqlx::query("DELETE FROM relationships WHERE user_id = ?")
            .bind(source_user_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM users WHERE id = ?")
            .bind(source_user_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(target_user_id)
    }

    async fn delete_user(&self, user_id: i64) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let (platform, external_id): (String, String) =
            sqlx::query_as("SELECT platform, external_id FROM users WHERE id = ?")
                .bind(user_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| RepositoryError::NotFound(format!("用户 {user_id} 不存在")))?;

        // 先清除所有属于该身份的记忆；认知记忆可能只在来源参与者 ID 数组中留有归属。
        sqlx::query(
            r#"DELETE FROM memories
                WHERE user_id = ?
                   OR json_extract(CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                                   '$.source_participant_id') IN
                      (SELECT id FROM participants WHERE user_id = ?)
                   OR EXISTS (
                      SELECT 1 FROM json_each(
                          CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                          '$.source_participant_ids'
                      ) AS source
                       WHERE CAST(source.value AS INTEGER) IN
                             (SELECT id FROM participants WHERE user_id = ?)
                   )
                   OR EXISTS (
                      SELECT 1 FROM json_each(
                          CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                          '$.participant_ids'
                      ) AS source
                       WHERE CAST(source.value AS INTEGER) IN
                             (SELECT id FROM participants WHERE user_id = ?)
                   )"#,
        )
        .bind(user_id)
        .bind(user_id)
        .bind(user_id)
        .bind(user_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;

        // 向量记忆有独立存储表，不受普通记忆删除影响；按来源参与者一并清理。
        sqlx::query(
            r#"DELETE FROM semantic_memories
                WHERE user_id = ?
                   OR json_extract(CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                                   '$.user_id') = CAST(? AS TEXT)
                   OR json_extract(CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                                   '$.source_participant_id') IN
                      (SELECT id FROM participants WHERE user_id = ?)
                   OR EXISTS (
                      SELECT 1 FROM json_each(
                          CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                          '$.source_participant_ids'
                      ) AS source
                       WHERE CAST(source.value AS INTEGER) IN
                             (SELECT id FROM participants WHERE user_id = ?)
                   )
                   OR EXISTS (
                      SELECT 1 FROM json_each(
                          CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                          '$.participant_ids'
                      ) AS source
                       WHERE CAST(source.value AS INTEGER) IN
                             (SELECT id FROM participants WHERE user_id = ?)
                   )"#,
        )
        .bind(user_id)
        .bind(user_id)
        .bind(user_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;

        // 历史消息正文和消息行保留，作者改成匿名占位；移除消息元数据里的外部身份信息。
        sqlx::query(
            r#"UPDATE messages
                  SET metadata = json_remove(
                      json_set(CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                               '$.author_kind', 'user', '$.display_name', '已删除用户'),
                      '$.external_sender_id', '$.user_id'
                  )
                WHERE sender_id IN (SELECT id FROM participants WHERE user_id = ?)"#,
        )
        .bind(user_id)
        .execute(&mut *tx)
        .await?;

        // QQ ID 也可能出现在其他消息的 @ 目标数组中；messages.mentions 保存的内部参与者
        // 关联仍然保留，因此只从复制的外部 ID 元数据中移除被删除账号。
        sqlx::query(
            r#"UPDATE messages
                  SET metadata = json_set(
                      metadata, '$.mentions', json(COALESCE((
                          SELECT json_group_array(value)
                            FROM json_each(metadata, '$.mentions')
                           WHERE CAST(value AS TEXT) <> ?
                      ), '[]')))
                WHERE json_valid(metadata)
                  AND json_type(CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                                '$.mentions') = 'array'
                  AND EXISTS (
                      SELECT 1 FROM json_each(
                          CASE WHEN json_valid(messages.metadata) THEN messages.metadata ELSE '{}' END,
                          '$.mentions'
                      )
                       WHERE CAST(value AS TEXT) = ?
                  )"#,
        )
        .bind(&external_id)
        .bind(&external_id)
        .execute(&mut *tx)
        .await?;

        // Outbox 可能仍保存待处理或已完成事件的副本；同步匿名化其作者元数据，保留事件正文。
        sqlx::query(
            r#"UPDATE outbox_events
                  SET payload = json_remove(
                      json_set(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,
                               '$.metadata.author_kind', 'user',
                               '$.metadata.display_name', '已删除用户'),
                      '$.metadata.external_sender_id', '$.metadata.user_id'
                  )
                WHERE entity_type = 'messages'
                  AND entity_id IN (
                      SELECT id FROM messages
                       WHERE sender_id IN (SELECT id FROM participants WHERE user_id = ?)
                  )
                  AND json_valid(payload)"#,
        )
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"UPDATE outbox_events
                  SET payload = json_set(
                      payload, '$.metadata.mentions', json(COALESCE((
                          SELECT json_group_array(value)
                            FROM json_each(payload, '$.metadata.mentions')
                           WHERE CAST(value AS TEXT) <> ?
                      ), '[]')))
                WHERE json_valid(payload)
                  AND json_type(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,
                                '$.metadata.mentions') = 'array'
                  AND EXISTS (
                      SELECT 1 FROM json_each(
                          CASE WHEN json_valid(outbox_events.payload) THEN outbox_events.payload ELSE '{}' END,
                          '$.metadata.mentions'
                      )
                       WHERE CAST(value AS TEXT) = ?
                  )"#,
        )
        .bind(&external_id)
        .bind(&external_id)
        .execute(&mut *tx)
        .await?;

        // 参与者行继续作为历史消息的外键目标，但不再保留 QQ 号、昵称或原始适配器元数据。
        sqlx::query(
            r#"UPDATE participants
                  SET external_id = 'deleted-user-' || id,
                      display_name = '已删除用户',
                      role = 'system',
                      metadata = '{}'
                WHERE user_id = ?"#,
        )
        .bind(user_id)
        .execute(&mut *tx)
        .await?;

        if platform == "onebot_qq" {
            // 私聊会话用 QQ 号作为 external_id；匿名化会话路由并关闭旧会话主动发言。
            sqlx::query(
                r#"UPDATE conversation_bindings
                      SET proactive_enabled = 0
                    WHERE conversation_id IN (
                        SELECT id FROM conversations
                         WHERE conversation_type = 'private' AND external_id = ?
                    )"#,
            )
            .bind(&external_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"UPDATE conversations
                      SET external_id = 'deleted-user-conversation-' || id,
                          name = '已删除用户'
                    WHERE conversation_type = 'private' AND external_id = ?"#,
            )
            .bind(&external_id)
            .execute(&mut *tx)
            .await?;
            // 黑名单仍以平台外部 ID 存储；删除账户时移除对应条目。
            sqlx::query("DELETE FROM blacklist_entries WHERE target = ? AND target_type = 'user'")
                .bind(&external_id)
                .execute(&mut *tx)
                .await?;
        }

        // user 外键会级联删除关系及交互去重记录；参与者外键仅解绑，保留消息归属占位。
        sqlx::query("DELETE FROM users WHERE id = ?")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

type ParticipantRow = (i64, i64, String, String, String, String, Option<i64>);

fn parse_participant_row(row: ParticipantRow) -> Result<Participant, RepositoryError> {
    let (id, conversation_id, external_id, display_name, role_str, metadata_json, user_id) = row;

    let role = match role_str.as_str() {
        "user" => ParticipantRole::User,
        "character" => ParticipantRole::Character,
        "system" => ParticipantRole::System,
        other => return Err(RepositoryError::Database(format!("unknown role: {other}"))),
    };

    let mut metadata: serde_json::Value = serde_json::from_str(&metadata_json)
        .map_err(|e| RepositoryError::Database(format!("invalid metadata JSON: {e}")))?;
    if let Some(user_id) = user_id {
        metadata["user_id"] = serde_json::json!(user_id);
    }

    Ok(Participant {
        id,
        conversation_id,
        external_id,
        display_name,
        role,
        metadata,
    })
}
