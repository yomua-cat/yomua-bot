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
    async fn record_interaction_once(
        &self,
        character_id: i64,
        participant_id: i64,
        source_message_id: i64,
    ) -> Result<bool, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO relationship_interactions(character_id,participant_id,source_message_id) VALUES(?,?,?)",
        )
        .bind(character_id)
        .bind(participant_id)
        .bind(source_message_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if inserted == 0 {
            tx.rollback().await?;
            return Ok(false);
        }

        let row = sqlx::query_as::<_, RelationshipRow>(
            r#"SELECT character_id, participant_id, familiarity, affection, trust,
                      respect, annoyance, intimacy, interaction_count,
                      last_interaction, created_at, updated_at
               FROM relationships WHERE character_id=? AND participant_id=?"#,
        )
        .bind(character_id)
        .bind(participant_id)
        .fetch_optional(&mut *tx)
        .await?;
        let mut relationship = match row {
            Some(row) => parse_relationship_row(row)?,
            None => Relationship::new(character_id, participant_id),
        };
        relationship.record_interaction();
        sqlx::query(
            r#"INSERT INTO relationships
                (character_id, participant_id, familiarity, affection, trust, respect,
                 annoyance, intimacy, interaction_count, last_interaction, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(character_id,participant_id) DO UPDATE SET
                 familiarity=excluded.familiarity, affection=excluded.affection,
                 trust=excluded.trust, respect=excluded.respect, annoyance=excluded.annoyance,
                 intimacy=excluded.intimacy, interaction_count=excluded.interaction_count,
                 last_interaction=excluded.last_interaction, updated_at=excluded.updated_at"#,
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
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

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

    async fn find_by_stable_user_key(
        &self,
        character_id: i64,
        stable_user_key: &str,
    ) -> Result<Option<Relationship>, RepositoryError> {
        let row = sqlx::query_as::<_, RelationshipRow>(
            r#"SELECT r.character_id, r.participant_id, r.familiarity, r.affection,
                      r.trust, r.respect, r.annoyance, r.intimacy,
                      r.interaction_count, r.last_interaction, r.created_at, r.updated_at
               FROM relationships r
               JOIN participants p ON p.id = r.participant_id
              WHERE r.character_id = ?
                AND json_extract(p.metadata, '$.stable_user_key') = ?
              ORDER BY r.updated_at DESC, r.participant_id
              LIMIT 1"#,
        )
        .bind(character_id)
        .bind(stable_user_key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(parse_relationship_row).transpose()
    }

    async fn ensure_stable_relationship_participant(
        &self,
        character_id: i64,
        participant_id: i64,
        stable_user_key: &str,
    ) -> Result<i64, RepositoryError> {
        let key = stable_user_key.trim();
        if key.is_empty() {
            return Ok(participant_id);
        }

        let mut tx = self.pool.begin().await?;
        let legacy_participant: Option<i64> = sqlx::query_scalar(
            r#"SELECT r.participant_id
                 FROM relationships r
                 JOIN participants p ON p.id = r.participant_id
                WHERE r.character_id = ?
                  AND json_extract(p.metadata, '$.stable_user_key') = ?
                ORDER BY r.participant_id
                LIMIT 1"#,
        )
        .bind(character_id)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let selected_candidate = legacy_participant.unwrap_or(participant_id);
        sqlx::query(
            r#"INSERT OR IGNORE INTO relationship_stable_users
               (character_id, stable_user_key, participant_id)
               VALUES (?, ?, ?)"#,
        )
        .bind(character_id)
        .bind(key)
        .bind(selected_candidate)
        .execute(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let selected: i64 = sqlx::query_scalar(
            r#"SELECT participant_id FROM relationship_stable_users
               WHERE character_id = ? AND stable_user_key = ?"#,
        )
        .bind(character_id)
        .bind(key)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;
        tx.commit().await?;
        Ok(selected)
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
