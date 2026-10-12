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
        let row: Option<(i64, i64, String, String, String, String)> = sqlx::query_as(
            r#"SELECT id, conversation_id, external_id, display_name, role, metadata
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
        let row: Option<(i64, i64, String, String, String, String)> = sqlx::query_as(
            r#"SELECT id, conversation_id, external_id, display_name, role, metadata
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
        let rows: Vec<(i64, i64, String, String, String, String)> = sqlx::query_as(
            r#"SELECT id, conversation_id, external_id, display_name, role, metadata
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

        let metadata = serde_json::to_string(&participant.metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let result = sqlx::query(
            r#"INSERT INTO participants (conversation_id, external_id, display_name, role, metadata)
             VALUES (?, ?, ?, ?, ?)"#,
        )
        .bind(participant.conversation_id)
        .bind(&participant.external_id)
        .bind(&participant.display_name)
        .bind(role_str)
        .bind(&metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(result.last_insert_rowid())
    }
}

type ParticipantRow = (i64, i64, String, String, String, String);

fn parse_participant_row(row: ParticipantRow) -> Result<Participant, RepositoryError> {
    let (id, conversation_id, external_id, display_name, role_str, metadata_json) = row;

    let role = match role_str.as_str() {
        "user" => ParticipantRole::User,
        "character" => ParticipantRole::Character,
        "system" => ParticipantRole::System,
        other => return Err(RepositoryError::Database(format!("unknown role: {other}"))),
    };

    let metadata: serde_json::Value = serde_json::from_str(&metadata_json)
        .map_err(|e| RepositoryError::Database(format!("invalid metadata JSON: {e}")))?;

    Ok(Participant {
        id,
        conversation_id,
        external_id,
        display_name,
        role,
        metadata,
    })
}
