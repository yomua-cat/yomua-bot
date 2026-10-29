//! 从 assets/characters 导入尚未登记的角色卡文件。

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use sqlx::SqlitePool;

use crate::application::character_import::{CharacterImportService, ImportOptions};
use crate::error::RuntimeError;

#[derive(Debug, Default, serde::Serialize)]
pub struct ScanResult {
    pub imported: Vec<(i64, String)>,
    pub unchanged: usize,
    pub errors: Vec<String>,
}

pub async fn scan_character_cards(
    directory: &Path,
    pool: &SqlitePool,
    importer: &CharacterImportService,
) -> Result<ScanResult, RuntimeError> {
    let mut result = ScanResult::default();
    if !directory.exists() {
        return Ok(result);
    }
    let mut paths = Vec::new();
    collect_cards(directory, &mut paths).map_err(|e| RuntimeError::Config(e.to_string()))?;
    paths.sort();

    for path in paths {
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) => {
                result.errors.push(format!("{}: {error}", path.display()));
                continue;
            }
        };
        let canonical = path.canonicalize().unwrap_or(path.clone());
        let path_key = canonical.to_string_lossy().to_string();
        let fingerprint = format!("{:x}", Sha256::digest(&bytes));
        let old = sqlx::query_scalar::<_, String>(
            "SELECT fingerprint FROM character_card_sources WHERE path = ?",
        )
        .bind(&path_key)
        .fetch_optional(pool)
        .await
        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
        if old.as_ref().is_some_and(|hash| hash == &fingerprint) {
            result.unchanged += 1;
            continue;
        }

        if let Some((character_id, name)) = sqlx::query_as::<_, (i64, String)>(
            "SELECT character_id, name FROM character_card_sources WHERE fingerprint = ? LIMIT 1",
        )
        .bind(&fingerprint)
        .fetch_optional(pool)
        .await
        .map_err(|e| RuntimeError::Internal(e.to_string()))?
        {
            sqlx::query(
                "INSERT INTO character_card_sources(path, fingerprint, character_id, name) VALUES (?, ?, ?, ?) ON CONFLICT(path) DO UPDATE SET fingerprint=excluded.fingerprint, character_id=excluded.character_id, name=excluded.name",
            )
            .bind(path_key)
            .bind(fingerprint)
            .bind(character_id)
            .bind(&name)
            .execute(pool)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;
            result.unchanged += 1;
            continue;
        }

        match importer
            .import_card(&bytes, &ImportOptions::default())
            .await
        {
            Ok(imported) => {
                sqlx::query(
                    "INSERT INTO character_card_sources(path, fingerprint, character_id, name) VALUES (?, ?, ?, ?) ON CONFLICT(path) DO UPDATE SET fingerprint=excluded.fingerprint, character_id=excluded.character_id, name=excluded.name",
                )
                .bind(path_key)
                .bind(fingerprint)
                .bind(imported.character_id)
                .bind(&imported.name)
                .execute(pool)
                .await
                .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                result.imported.push((imported.character_id, imported.name));
            }
            Err(error) => result.errors.push(format!("{}: {error}", path.display())),
        }
    }
    Ok(result)
}

fn collect_cards(directory: &Path, paths: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_cards(&path, paths)?;
        } else if matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("json" | "png")
        ) {
            paths.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::application::{
        binding::BindingManager, character_import::CharacterImportService,
        conversation::ConversationManager,
    };
    use crate::domain::repository::{CharacterRepository, CharacterStateRepository};
    use crate::infrastructure::storage::{repository::*, SqliteStorage};

    #[tokio::test]
    async fn imports_new_card_once_and_reimports_changed_content() {
        let storage = SqliteStorage::open_in_memory().await.unwrap();
        storage.migrate().await.unwrap();
        let pool = storage.pool().clone();
        let characters: Arc<dyn CharacterRepository> =
            Arc::new(SqliteCharacterRepository::new(pool.clone()));
        let conversations = Arc::new(SqliteConversationRepository::new(pool.clone()));
        let participants = Arc::new(SqliteParticipantRepository::new(pool.clone()));
        let importer = CharacterImportService::new(
            characters.clone(),
            Arc::new(SqliteCharacterStateRepository::new(pool.clone()))
                as Arc<dyn CharacterStateRepository>,
            Arc::new(ConversationManager::new(
                conversations.clone(),
                participants,
            )),
            Arc::new(BindingManager::new(
                Arc::new(SqliteCharacterBindingRepository::new(pool.clone())),
                characters.clone(),
                conversations,
            )),
        );
        let dir = tempfile::tempdir().unwrap();
        let card = dir.path().join("card.json");
        std::fs::write(&card, r#"{"name":"first"}"#).unwrap();

        let first = scan_character_cards(dir.path(), &pool, &importer)
            .await
            .unwrap();
        assert_eq!(first.imported.len(), 1);
        assert_eq!(first.imported[0].0, 1);
        let repeat = scan_character_cards(dir.path(), &pool, &importer)
            .await
            .unwrap();
        assert_eq!(repeat.unchanged, 1);
        assert!(repeat.imported.is_empty());

        std::fs::write(&card, r#"{"name":"second"}"#).unwrap();
        let changed = scan_character_cards(dir.path(), &pool, &importer)
            .await
            .unwrap();
        assert_eq!(changed.imported[0].0, 2);
        assert_eq!(characters.find_all().await.unwrap().len(), 2);
    }
}
