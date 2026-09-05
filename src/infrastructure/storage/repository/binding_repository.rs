//! CharacterBindingRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::character::{CharacterBinding, ReplyMode};
use crate::domain::repository::CharacterBindingRepository;
use crate::error::RepositoryError;

pub struct SqliteCharacterBindingRepository {
    pool: SqlitePool,
}

impl SqliteCharacterBindingRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CharacterBindingRepository for SqliteCharacterBindingRepository {
    async fn find_by_character_id(
        &self,
        character_id: i64,
    ) -> Result<Vec<CharacterBinding>, RepositoryError> {
        let rows: Vec<(
            i64,
            i64,
            i64,
            String,
            i32,
            Option<String>,
            String,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT id, character_id, conversation_id, reply_mode, proactive_enabled,
                    mute_schedule, behavior_overrides, context_policy, created_at
                 FROM conversation_bindings WHERE character_id = ?"#,
        )
        .bind(character_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(parse_binding_row).collect()
    }

    async fn find_by_conversation_id(
        &self,
        conversation_id: i64,
    ) -> Result<Vec<CharacterBinding>, RepositoryError> {
        let rows: Vec<(
            i64,
            i64,
            i64,
            String,
            i32,
            Option<String>,
            String,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT id, character_id, conversation_id, reply_mode, proactive_enabled,
                    mute_schedule, behavior_overrides, context_policy, created_at
                 FROM conversation_bindings WHERE conversation_id = ?"#,
        )
        .bind(conversation_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(parse_binding_row).collect()
    }

    async fn find_all(&self) -> Result<Vec<CharacterBinding>, RepositoryError> {
        let rows: Vec<BindingRow> = sqlx::query_as(
            r#"SELECT id, character_id, conversation_id, reply_mode, proactive_enabled,
                    mute_schedule, behavior_overrides, context_policy, created_at
                 FROM conversation_bindings"#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(parse_binding_row).collect()
    }

    async fn insert(&self, binding: &CharacterBinding) -> Result<i64, RepositoryError> {
        let reply_mode = match binding.reply_mode {
            ReplyMode::MentionOnly => "mention_only",
            ReplyMode::Occasionally => "occasional",
            ReplyMode::Natural => "natural",
        };
        let behavior_overrides = serde_json::to_string(&binding.behavior_overrides)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let context_policy = serde_json::to_string(&binding.context_policy)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let result = sqlx::query(
            r#"INSERT INTO conversation_bindings
                (character_id, conversation_id, reply_mode, proactive_enabled,
                 mute_schedule, behavior_overrides, context_policy)
             VALUES (?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(binding.character_id)
        .bind(binding.conversation_id)
        .bind(reply_mode)
        .bind(binding.proactive_enabled as i32)
        .bind(&binding.mute_schedule)
        .bind(&behavior_overrides)
        .bind(&context_policy)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(result.last_insert_rowid())
    }

    async fn delete(&self, id: i64) -> Result<(), RepositoryError> {
        sqlx::query("DELETE FROM conversation_bindings WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(())
    }
}

type BindingRow = (
    i64,
    i64,
    i64,
    String,
    i32,
    Option<String>,
    String,
    String,
    String,
);

fn parse_binding_row(row: BindingRow) -> Result<CharacterBinding, RepositoryError> {
    let (
        id,
        character_id,
        conversation_id,
        reply_mode_str,
        proactive_enabled,
        mute_schedule,
        behavior_overrides_json,
        context_policy_json,
        created_at,
    ) = row;

    let reply_mode = match reply_mode_str.as_str() {
        "mention_only" => ReplyMode::MentionOnly,
        "occasional" => ReplyMode::Occasionally,
        "natural" => ReplyMode::Natural,
        other => {
            return Err(RepositoryError::Database(format!(
                "unknown reply_mode: {other}"
            )))
        }
    };

    let behavior_overrides: serde_json::Value = serde_json::from_str(&behavior_overrides_json)
        .map_err(|e| RepositoryError::Database(format!("invalid behavior_overrides JSON: {e}")))?;
    let context_policy: serde_json::Value = serde_json::from_str(&context_policy_json)
        .map_err(|e| RepositoryError::Database(format!("invalid context_policy JSON: {e}")))?;

    let created_at = super::timestamp::parse_timestamp(&created_at)?;

    Ok(CharacterBinding {
        id,
        character_id,
        conversation_id,
        reply_mode,
        proactive_enabled: proactive_enabled != 0,
        mute_schedule,
        behavior_overrides,
        context_policy,
        created_at,
    })
}
