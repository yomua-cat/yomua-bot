//! RelationshipRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::relationship::Relationship;
use crate::domain::repository::RelationshipRepository;
use crate::error::RepositoryError;

pub struct SqliteRelationshipRepository {
    pool: SqlitePool,
}

impl SqliteRelationshipRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RelationshipRepository for SqliteRelationshipRepository {
    async fn find(
        &self,
        character_id: i64,
        participant_id: i64,
    ) -> Result<Option<Relationship>, RepositoryError> {
        let row: Option<(
            i64,
            i64,
            f64,
            f64,
            f64,
            f64,
            f64,
            f64,
            i64,
            String,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT character_id, participant_id, familiarity, affection, trust,
                respect, annoyance, intimacy, interaction_count,
                last_interaction, created_at, updated_at
             FROM relationships WHERE character_id = ? AND participant_id = ?"#,
        )
        .bind(character_id)
        .bind(participant_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(parse_relationship_row).transpose()
    }

    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Vec<Relationship>, RepositoryError> {
        let rows: Vec<(
            i64,
            i64,
            f64,
            f64,
            f64,
            f64,
            f64,
            f64,
            i64,
            String,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT character_id, participant_id, familiarity, affection, trust,
                respect, annoyance, intimacy, interaction_count,
                last_interaction, created_at, updated_at
             FROM relationships WHERE character_id = ? ORDER BY participant_id"#,
        )
        .bind(character_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(parse_relationship_row).collect()
    }

    async fn upsert(&self, relationship: &Relationship) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"INSERT INTO relationships
                (character_id, participant_id, familiarity, affection, trust,
                 respect, annoyance, intimacy, interaction_count,
                 last_interaction, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(character_id, participant_id) DO UPDATE SET
                familiarity = excluded.familiarity,
                affection = excluded.affection,
                trust = excluded.trust,
                respect = excluded.respect,
                annoyance = excluded.annoyance,
                intimacy = excluded.intimacy,
                interaction_count = excluded.interaction_count,
                last_interaction = excluded.last_interaction,
                updated_at = excluded.updated_at"#,
        )
        .bind(relationship.character_id)
        .bind(relationship.participant_id)
        .bind(relationship.familiarity)
        .bind(relationship.affection)
        .bind(relationship.trust)
        .bind(relationship.respect)
        .bind(relationship.annoyance)
        .bind(relationship.intimacy)
        .bind(relationship.interaction_count)
        .bind(relationship.last_interaction.to_rfc3339())
        .bind(relationship.created_at.to_rfc3339())
        .bind(relationship.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }
}

type RelationshipRow = (
    i64,
    i64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    i64,
    String,
    String,
    String,
);

fn parse_relationship_row(row: RelationshipRow) -> Result<Relationship, RepositoryError> {
    let (
        character_id,
        participant_id,
        familiarity,
        affection,
        trust,
        respect,
        annoyance,
        intimacy,
        interaction_count,
        last_interaction,
        created_at,
        updated_at,
    ) = row;

    let parse_ts = |s: &str| -> Result<chrono::DateTime<chrono::Utc>, RepositoryError> {
        super::timestamp::parse_timestamp(s)
    };

    Ok(Relationship {
        character_id,
        participant_id,
        familiarity,
        affection,
        trust,
        respect,
        annoyance,
        intimacy,
        interaction_count,
        last_interaction: parse_ts(&last_interaction)?,
        created_at: parse_ts(&created_at)?,
        updated_at: parse_ts(&updated_at)?,
    })
}
