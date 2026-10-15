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
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE id IN (?, ?)")
            .bind(source_user_id)
            .bind(target_user_id)
            .fetch_one(&mut *tx)
            .await?;
        if exists != 2 {
            return Err(RepositoryError::NotFound("待合并用户不存在".into()));
        }

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
        sqlx::query("DELETE FROM users WHERE id = ?")
            .bind(user_id)
            .execute(&self.pool)
            .await?;
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
