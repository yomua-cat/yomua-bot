//! RelationshipRepository 的 SQLite 实现。
//!
//! 关系的持久化主键是 Character × User。Participant 只用于把消息作者
//! 映射到全域用户，并作为上下文中可展示的代表参与者。

use async_trait::async_trait;
use sqlx::{Sqlite, SqlitePool, Transaction};

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
    ) -> Result<Option<i64>, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let user_id: i64 =
            sqlx::query_scalar("SELECT user_id FROM participants WHERE id=? AND role='user'")
                .bind(participant_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| {
                    RepositoryError::NotFound(format!("用户参与者 {participant_id} 不存在"))
                })?;
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO relationship_interactions(character_id,user_id,source_message_id) VALUES(?,?,?)",
        )
        .bind(character_id)
        .bind(user_id)
        .bind(source_message_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if inserted == 0 {
            tx.rollback().await?;
            return Ok(None);
        }

        let row = sqlx::query_as::<_, RelationshipRow>(
            r#"SELECT r.character_id, r.user_id,
                      (SELECT MIN(p.id) FROM participants p WHERE p.user_id=r.user_id AND p.role='user'),
                      r.familiarity, r.affection, r.trust, r.respect, r.annoyance, r.intimacy,
                      r.interaction_count, r.last_interaction, r.created_at, r.updated_at
                 FROM relationships r WHERE r.character_id=? AND r.user_id=?"#,
        )
        .bind(character_id)
        .bind(user_id)
        .fetch_optional(&mut *tx)
        .await?;
        let mut relationship = match row {
            Some(row) => parse_relationship_row(row)?,
            None => Relationship::new(character_id, user_id),
        };
        relationship.record_interaction();
        upsert_relationship(&mut tx, &relationship).await?;
        tx.commit().await?;
        Ok(Some(user_id))
    }

    async fn find(
        &self,
        character_id: i64,
        participant_id: i64,
    ) -> Result<Option<Relationship>, RepositoryError> {
        let user_id: Option<i64> =
            sqlx::query_scalar("SELECT user_id FROM participants WHERE id=? AND role='user'")
                .bind(participant_id)
                .fetch_optional(&self.pool)
                .await?;
        let Some(user_id) = user_id else {
            return Ok(None);
        };
        let row = sqlx::query_as::<_, RelationshipRow>(
            r#"SELECT r.character_id, r.user_id,
                      (SELECT MIN(p.id) FROM participants p WHERE p.user_id=r.user_id AND p.role='user'),
                      r.familiarity, r.affection, r.trust, r.respect, r.annoyance, r.intimacy,
                      r.interaction_count, r.last_interaction, r.created_at, r.updated_at
                 FROM relationships r WHERE r.character_id=? AND r.user_id=?"#,
        )
        .bind(character_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(parse_relationship_row).transpose()
    }

    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Vec<Relationship>, RepositoryError> {
        let rows: Vec<RelationshipRow> = sqlx::query_as(
            r#"SELECT r.character_id, r.user_id,
                      (SELECT MIN(p.id) FROM participants p WHERE p.user_id=r.user_id AND p.role='user'),
                      r.familiarity, r.affection, r.trust, r.respect, r.annoyance, r.intimacy,
                      r.interaction_count, r.last_interaction, r.created_at, r.updated_at
                 FROM relationships r WHERE r.character_id=? ORDER BY r.user_id"#,
        )
        .bind(character_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(parse_relationship_row).collect()
    }

    async fn find_by_user_id(
        &self,
        character_id: i64,
        user_id: i64,
    ) -> Result<Option<Relationship>, RepositoryError> {
        let row = sqlx::query_as::<_, RelationshipRow>(
            r#"SELECT r.character_id, r.user_id,
                      (SELECT MIN(p.id) FROM participants p WHERE p.user_id=r.user_id AND p.role='user'),
                      r.familiarity, r.affection, r.trust, r.respect, r.annoyance, r.intimacy,
                      r.interaction_count, r.last_interaction, r.created_at, r.updated_at
                 FROM relationships r
                WHERE r.character_id=? AND r.user_id=?"#,
        )
        .bind(character_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(parse_relationship_row).transpose()
    }

    async fn find_by_stable_user_key(
        &self,
        character_id: i64,
        stable_user_key: &str,
    ) -> Result<Option<Relationship>, RepositoryError> {
        let row = sqlx::query_as::<_, RelationshipRow>(
            r#"SELECT r.character_id, r.user_id,
                      (SELECT MIN(p.id) FROM participants p WHERE p.user_id=r.user_id AND p.role='user'),
                      r.familiarity, r.affection, r.trust, r.respect, r.annoyance, r.intimacy,
                      r.interaction_count, r.last_interaction, r.created_at, r.updated_at
                 FROM relationships r JOIN users u ON u.id=r.user_id
                WHERE r.character_id=? AND u.external_id=? AND u.platform='onebot_qq'
                LIMIT 1"#,
        )
        .bind(character_id)
        .bind(stable_user_key.trim())
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
        let user_id: i64 =
            sqlx::query_scalar("SELECT user_id FROM participants WHERE id=? AND role='user'")
                .bind(participant_id)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| {
                    RepositoryError::NotFound(format!("用户参与者 {participant_id} 不存在"))
                })?;
        if self.find(character_id, participant_id).await?.is_none() {
            self.upsert(&Relationship::new(character_id, user_id))
                .await?;
        }
        let _ = stable_user_key;
        sqlx::query_scalar::<_, i64>(
            "SELECT MIN(id) FROM participants WHERE user_id=? AND role='user'",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(RepositoryError::from)
    }

    async fn create_for_participant(
        &self,
        character_id: i64,
        participant_id: i64,
    ) -> Result<Relationship, RepositoryError> {
        let user_id: i64 =
            sqlx::query_scalar("SELECT user_id FROM participants WHERE id=? AND role='user'")
                .bind(participant_id)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| {
                    RepositoryError::NotFound(format!("用户参与者 {participant_id} 不存在"))
                })?;
        let relationship = Relationship::new(character_id, user_id);
        self.upsert(&relationship).await?;
        Ok(relationship)
    }

    async fn upsert(&self, relationship: &Relationship) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await?;
        upsert_relationship(&mut tx, relationship).await?;
        tx.commit().await?;
        Ok(())
    }
}

async fn upsert_relationship(
    tx: &mut Transaction<'_, Sqlite>,
    relationship: &Relationship,
) -> Result<(), RepositoryError> {
    sqlx::query(
        r#"INSERT INTO relationships
            (character_id,user_id,familiarity,affection,trust,respect,annoyance,intimacy,
             interaction_count,last_interaction,created_at,updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(character_id,user_id) DO UPDATE SET
             familiarity=excluded.familiarity, affection=excluded.affection,
             trust=excluded.trust, respect=excluded.respect, annoyance=excluded.annoyance,
             intimacy=excluded.intimacy, interaction_count=excluded.interaction_count,
             last_interaction=excluded.last_interaction, updated_at=excluded.updated_at"#,
    )
    .bind(relationship.character_id)
    .bind(relationship.user_id)
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
    .execute(&mut **tx)
    .await?;
    Ok(())
}

type RelationshipRow = (
    i64,
    i64,
    Option<i64>,
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
        user_id,
        _participant_id,
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
    // 用户可能暂时没有任何会话 participant（例如删除了最后一个会话），
    // 关系仍然有效；0 表示当前没有可展示的会话代表。
    Ok(Relationship {
        character_id,
        user_id,
        familiarity,
        affection,
        trust,
        respect,
        annoyance,
        intimacy,
        interaction_count,
        last_interaction: super::timestamp::parse_timestamp(&last_interaction)?,
        created_at: super::timestamp::parse_timestamp(&created_at)?,
        updated_at: super::timestamp::parse_timestamp(&updated_at)?,
    })
}
