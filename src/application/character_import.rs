//! 角色卡导入编排 —— 把外部角色卡入库并（可选）绑定到会话。
//!
//! 分层：
//! - 「解析」在 `infrastructure/character_card`（识别 V1/V2/V3 JSON / PNG）；
//! - 本模块负责「读取字节 → 解析 → 校验 → 落库（`characters` +
//!   `character_states` 默认态）→ 可选创建 conversation / participant /
//!   binding」的编排；
//! - Domain 不依赖任何外部卡格式；仓储走既有 trait。
//!
//! 会话/绑定复用 [`ConversationManager`] 与 [`BindingManager`]，
//! 避免重复实现外部 ID 解析与唯一性校验。

use std::path::Path;
use std::sync::Arc;

use chrono::Utc;

use crate::application::binding::BindingManager;
use crate::application::conversation::ConversationManager;
use crate::domain::character::{Character, CharacterState, ReplyMode};
use crate::domain::conversation::ConversationType;
use crate::domain::repository::{CharacterRepository, CharacterStateRepository};
use crate::error::RuntimeError;
use crate::infrastructure::character_card::{self, CardImportError};

/// 把角色卡解析错误包装为运行时错误。
impl From<CardImportError> for RuntimeError {
    fn from(e: CardImportError) -> Self {
        RuntimeError::CardImport(e.to_string())
    }
}

/// PNG 文件签名（8 字节）。用于区分 PNG 卡与 JSON 卡。
const PNG_SIGNATURE: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];

/// 导入时是否创建角色与会话的绑定及相关实体。
#[derive(Debug, Clone, Default)]
pub struct BindOptions {
    /// 目标会话类型（私聊 / 群聊）。
    pub conversation_type: ConversationType,
    /// 目标会话的平台外部 ID（例如 QQ 群号）。
    pub external_conversation_id: String,
    /// 可选：创建该会话中的一个人类参与者（用于预置关系）。
    pub participant_external_id: Option<String>,
    /// 可选：参与者的显示名称（仅在首次创建时使用）。
    pub participant_display_name: Option<String>,
    /// 角色在此会话中的回复方式。
    pub reply_mode: ReplyMode,
    /// 是否启用主动消息。
    pub proactive_enabled: bool,
}

/// 导入选项。
#[derive(Debug, Clone, Default)]
pub struct ImportOptions {
    /// 为 `Some` 时把角色绑定到一个会话（并可选创建参与者）。
    pub bind: Option<BindOptions>,
}

/// 一次导入的结果。
#[derive(Debug, Clone)]
pub struct ImportResult {
    /// 新插入的角色 ID。
    pub character_id: i64,
    /// 角色显示名称。
    pub name: String,
    /// 识别到的卡版本标记（来自元数据的 `spec_version`，若存在）。
    pub spec_version: Option<String>,
    /// 若绑定了会话，为其核心 ID。
    pub conversation_id: Option<i64>,
    /// 若绑定了角色，为其绑定 ID。
    pub binding_id: Option<i64>,
    /// 若创建了参与者，为其 ID。
    pub participant_id: Option<i64>,
}

/// 角色卡导入编排服务。
///
/// 依赖仓储 trait 与应用层管理器，可在测试中用内存实现替换。
pub struct CharacterImportService {
    character_repo: Arc<dyn CharacterRepository>,
    state_repo: Arc<dyn CharacterStateRepository>,
    conversation_manager: Arc<ConversationManager>,
    binding_manager: Arc<BindingManager>,
}

impl CharacterImportService {
    /// 创建一个导入服务。
    pub fn new(
        character_repo: Arc<dyn CharacterRepository>,
        state_repo: Arc<dyn CharacterStateRepository>,
        conversation_manager: Arc<ConversationManager>,
        binding_manager: Arc<BindingManager>,
    ) -> Self {
        Self {
            character_repo,
            state_repo,
            conversation_manager,
            binding_manager,
        }
    }

    /// 从文件路径导入一张角色卡（JSON 或 PNG）。
    pub async fn import_file(
        &self,
        path: &Path,
        options: &ImportOptions,
    ) -> Result<ImportResult, RuntimeError> {
        let bytes = std::fs::read(path).map_err(|e| {
            RuntimeError::Config(format!("无法读取角色卡文件 {}: {e}", path.display()))
        })?;
        self.import_card(&bytes, options).await
    }

    /// 从内存字节导入一张角色卡（JSON 或 PNG）。
    ///
    /// 先识别文件类型：以 PNG 签名开头按 PNG 解析，否则按 JSON 文本解析。
    /// 解析后做 `validate()` 校验，再插入角色与默认状态，
    /// 并按需创建会话 / 参与者 / 绑定。
    pub async fn import_card(
        &self,
        bytes: &[u8],
        options: &ImportOptions,
    ) -> Result<ImportResult, RuntimeError> {
        // 1. 解析：PNG 卡或 JSON 卡。
        let definition = if bytes.starts_with(&PNG_SIGNATURE) {
            character_card::parse_png_character_card(bytes)?
        } else {
            let text = std::str::from_utf8(bytes)
                .map_err(|e| RuntimeError::Config(format!("卡片不是合法 UTF-8 文本：{e}")))?;
            character_card::parse_character_card(text)?
        };

        // 2. 校验：核心字段约束（name 非空）。
        definition.validate()?;

        // 3. 插入角色。
        let now = Utc::now();
        let character = Character {
            id: 0,
            definition,
            state: CharacterState::default(),
            created_at: now,
            updated_at: now,
        };
        let name = character.definition.name.clone();
        let spec_version = character
            .definition
            .metadata
            .get("spec_version")
            .and_then(|v| v.as_str())
            .map(String::from);
        let character_id = self.character_repo.insert(&character).await?;

        // 4. 写入默认状态（character_states 默认态 由 `CharacterState::default()` 提供）。
        self.state_repo
            .upsert(character_id, &CharacterState::default())
            .await?;

        // 5. 可选：创建会话 / 参与者 / 绑定。
        let mut result = ImportResult {
            character_id,
            name,
            spec_version,
            conversation_id: None,
            binding_id: None,
            participant_id: None,
        };

        if let Some(bind) = &options.bind {
            let conversation_id = self
                .conversation_manager
                .resolve_or_create_conversation(
                    bind.conversation_type,
                    &bind.external_conversation_id,
                )
                .await?;

            // 可选创建人类参与者（预置关系用）。
            let participant_id = if let Some(ext) = &bind.participant_external_id {
                Some(
                    self.conversation_manager
                        .resolve_or_create_participant(
                            conversation_id,
                            ext,
                            bind.participant_display_name.as_deref().unwrap_or(ext),
                        )
                        .await?,
                )
            } else {
                None
            };

            // 创建绑定（含唯一性与存在性校验）。
            let binding = self
                .binding_manager
                .bind(
                    character_id,
                    conversation_id,
                    bind.reply_mode,
                    bind.proactive_enabled,
                    None, // mute_schedule
                    serde_json::json!({}),
                    serde_json::json!({}),
                )
                .await?;

            result.conversation_id = Some(conversation_id);
            result.participant_id = participant_id;
            result.binding_id = Some(binding.id);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::repository::{
        CharacterBindingRepository, CharacterRepository, CharacterStateRepository,
        ConversationRepository, ParticipantRepository,
    };
    use crate::infrastructure::storage::repository::{
        SqliteCharacterBindingRepository, SqliteCharacterRepository,
        SqliteCharacterStateRepository, SqliteConversationRepository, SqliteParticipantRepository,
    };
    use crate::infrastructure::storage::SqliteStorage;

    async fn setup() -> CharacterImportService {
        let storage = SqliteStorage::open_in_memory()
            .await
            .expect("打开内存库应成功");
        storage.migrate().await.expect("迁移应成功");
        let pool = storage.pool().clone();

        let character_repo: Arc<dyn CharacterRepository> =
            Arc::new(SqliteCharacterRepository::new(pool.clone()));
        let state_repo: Arc<dyn CharacterStateRepository> =
            Arc::new(SqliteCharacterStateRepository::new(pool.clone()));
        let conversation_repo: Arc<dyn ConversationRepository> =
            Arc::new(SqliteConversationRepository::new(pool.clone()));
        let participant_repo: Arc<dyn ParticipantRepository> =
            Arc::new(SqliteParticipantRepository::new(pool.clone()));
        let binding_repo: Arc<dyn CharacterBindingRepository> =
            Arc::new(SqliteCharacterBindingRepository::new(pool.clone()));

        let conversation_manager = Arc::new(ConversationManager::new(
            conversation_repo.clone(),
            participant_repo.clone(),
        ));
        let binding_manager = Arc::new(BindingManager::new(
            binding_repo,
            character_repo.clone(),
            conversation_repo.clone(),
        ));

        CharacterImportService::new(
            character_repo,
            state_repo,
            conversation_manager,
            binding_manager,
        )
    }

    const V3_CARD: &str = r#"{
        "spec": "chara_card_v3",
        "spec_version": "chara_card_v3",
        "data": {
            "name": "油木然-bot",
            "description": "一个仿生人助手",
            "first_mes": "说吧，长话短说。"
        }
    }"#;

    #[tokio::test]
    async fn import_without_bind_inserts_character_and_state() {
        let service = setup().await;
        let result = service
            .import_card(V3_CARD.as_bytes(), &ImportOptions::default())
            .await
            .expect("导入应成功");

        assert!(result.character_id > 0);
        assert_eq!(result.name, "油木然-bot");
        assert_eq!(result.spec_version.as_deref(), Some("chara_card_v3"));
        assert!(result.conversation_id.is_none());
        assert!(result.binding_id.is_none());
    }

    #[tokio::test]
    async fn import_with_bind_creates_conversation_binding_and_participant() {
        let service = setup().await;
        let options = ImportOptions {
            bind: Some(BindOptions {
                conversation_type: ConversationType::Group,
                external_conversation_id: "123456".to_string(),
                participant_external_id: Some("u99".to_string()),
                participant_display_name: Some("小油".to_string()),
                reply_mode: ReplyMode::Natural,
                proactive_enabled: false,
            }),
        };

        let result = service
            .import_card(V3_CARD.as_bytes(), &options)
            .await
            .expect("导入并绑定应成功");

        assert!(result.character_id > 0);
        assert!(result.conversation_id.is_some());
        assert!(result.binding_id.is_some());
        assert!(result.participant_id.is_some());

        // G1 强制单绑定：一个会话最多一个角色绑定。
        // 同一会话再次导入并绑定 → 应被拒绝（换角色走 switch_character，而非重复绑定）。
        let second = service.import_card(V3_CARD.as_bytes(), &options).await;
        assert!(second.is_err(), "同会话第二次绑定应被拒绝（G1）");
    }

    #[tokio::test]
    async fn rejects_blank_name_card() {
        let service = setup().await;
        let bad = r#"{
            "spec": "chara_card_v3",
            "data": { "name": "   " }
        }"#;
        assert!(service
            .import_card(bad.as_bytes(), &ImportOptions::default())
            .await
            .is_err());
    }
}
