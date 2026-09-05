//! CharacterRepository 的 SQLite 实现。

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::character::{Character, CharacterDefinition, CharacterState, LorebookEntry};
use crate::domain::repository::CharacterRepository;
use crate::error::RepositoryError;

pub struct SqliteCharacterRepository {
    pool: SqlitePool,
}

impl SqliteCharacterRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CharacterRepository for SqliteCharacterRepository {
    async fn find_by_id(&self, id: i64) -> Result<Option<Character>, RepositoryError> {
        let row: Option<(
            i64,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
            String,
            Option<String>,
            Option<String>,
            String,
            String,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT id, name, description, personality, scenario, style, background,
                    greetings, example_messages, system_prompt, post_history_instructions,
                    lorebook, metadata, created_at, updated_at
                 FROM characters WHERE id = ?"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => Ok(Some(parse_character_row(r)?)),
            None => Ok(None),
        }
    }

    async fn find_all(&self) -> Result<Vec<Character>, RepositoryError> {
        let rows: Vec<(
            i64,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
            String,
            Option<String>,
            Option<String>,
            String,
            String,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT id, name, description, personality, scenario, style, background,
                    greetings, example_messages, system_prompt, post_history_instructions,
                    lorebook, metadata, created_at, updated_at
                 FROM characters ORDER BY id"#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(parse_character_row).collect()
    }

    async fn insert(&self, character: &Character) -> Result<i64, RepositoryError> {
        let greetings = serde_json::to_string(&character.definition.greetings)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let example_messages = serde_json::to_string(&character.definition.example_messages)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let lorebook = serde_json::to_string(&character.definition.lorebook)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let metadata = serde_json::to_string(&character.definition.metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        let result = sqlx::query(
            r#"INSERT INTO characters
                (name, description, personality, scenario, style, background,
                 greetings, example_messages, system_prompt, post_history_instructions,
                 lorebook, metadata)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&character.definition.name)
        .bind(&character.definition.description)
        .bind(&character.definition.personality)
        .bind(&character.definition.scenario)
        .bind(&character.definition.style)
        .bind(&character.definition.background)
        .bind(&greetings)
        .bind(&example_messages)
        .bind(&character.definition.system_prompt)
        .bind(&character.definition.post_history_instructions)
        .bind(&lorebook)
        .bind(&metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(result.last_insert_rowid())
    }

    async fn update(&self, character: &Character) -> Result<(), RepositoryError> {
        let greetings = serde_json::to_string(&character.definition.greetings)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let example_messages = serde_json::to_string(&character.definition.example_messages)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let lorebook = serde_json::to_string(&character.definition.lorebook)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        let metadata = serde_json::to_string(&character.definition.metadata)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        sqlx::query(
            r#"UPDATE characters SET
                name = ?, description = ?, personality = ?, scenario = ?,
                style = ?, background = ?, greetings = ?, example_messages = ?,
                system_prompt = ?, post_history_instructions = ?,
                lorebook = ?, metadata = ?, updated_at = datetime('now')
             WHERE id = ?"#,
        )
        .bind(&character.definition.name)
        .bind(&character.definition.description)
        .bind(&character.definition.personality)
        .bind(&character.definition.scenario)
        .bind(&character.definition.style)
        .bind(&character.definition.background)
        .bind(&greetings)
        .bind(&example_messages)
        .bind(&character.definition.system_prompt)
        .bind(&character.definition.post_history_instructions)
        .bind(&lorebook)
        .bind(&metadata)
        .bind(character.id)
        .execute(&self.pool)
        .await
        .map_err(|e| RepositoryError::Database(e.to_string()))?;

        Ok(())
    }

    async fn delete(&self, id: i64) -> Result<(), RepositoryError> {
        sqlx::query("DELETE FROM characters WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(())
    }
}

type CharacterRow = (
    i64,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
    String,
);

fn parse_character_row(row: CharacterRow) -> Result<Character, RepositoryError> {
    let (
        id,
        name,
        description,
        personality,
        scenario,
        style,
        background,
        greetings_json,
        example_messages_json,
        system_prompt,
        post_history_instructions,
        lorebook_json,
        metadata_json,
        created_at,
        updated_at,
    ) = row;

    let greetings: Vec<String> = serde_json::from_str(&greetings_json)
        .map_err(|e| RepositoryError::Database(format!("invalid greetings JSON: {e}")))?;
    let example_messages: Vec<String> = serde_json::from_str(&example_messages_json)
        .map_err(|e| RepositoryError::Database(format!("invalid example_messages JSON: {e}")))?;
    let lorebook: Vec<LorebookEntry> = serde_json::from_str(&lorebook_json)
        .map_err(|e| RepositoryError::Database(format!("invalid lorebook JSON: {e}")))?;
    let metadata: serde_json::Value = serde_json::from_str(&metadata_json)
        .map_err(|e| RepositoryError::Database(format!("invalid metadata JSON: {e}")))?;

    let created_at = super::timestamp::parse_timestamp(&created_at)?;
    let updated_at = super::timestamp::parse_timestamp(&updated_at)?;

    Ok(Character {
        id,
        definition: CharacterDefinition {
            name,
            description,
            personality,
            scenario,
            style,
            background,
            greetings,
            example_messages,
            system_prompt,
            post_history_instructions,
            lorebook,
            metadata,
        },
        state: CharacterState::default(),
        created_at,
        updated_at,
    })
}
