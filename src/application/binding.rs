//! 角色绑定管理 —— Character 与 Conversation 之间绑定关系的高层操作。
//!
//! 依赖底层的 `CharacterBindingRepository` trait（而非具体 SQLite 实现），
//! 因此本模块可在测试中用内存实现替换存储层。

use std::sync::Arc;

use chrono::Utc;

use crate::domain::character::{CharacterBinding, ReplyMode};
use crate::domain::repository::{
    CharacterBindingRepository, CharacterRepository, ConversationRepository,
};
use crate::error::{DomainError, RuntimeError};

/// 绑定管理器 —— 负责验证、创建与查询角色与会话之间的绑定。
pub struct BindingManager {
    binding_repo: Arc<dyn CharacterBindingRepository>,
    character_repo: Arc<dyn CharacterRepository>,
    conversation_repo: Arc<dyn ConversationRepository>,
}

impl BindingManager {
    /// 创建一个绑定管理器。
    pub fn new(
        binding_repo: Arc<dyn CharacterBindingRepository>,
        character_repo: Arc<dyn CharacterRepository>,
        conversation_repo: Arc<dyn ConversationRepository>,
    ) -> Self {
        Self {
            binding_repo,
            character_repo,
            conversation_repo,
        }
    }

    /// 为一个角色与会话创建绑定。
    ///
    /// 校验角色与会话都存在；若该角色已在同一会话中绑定，则返回错误（唯一约束）。
    #[allow(clippy::too_many_arguments)]
    pub async fn bind(
        &self,
        character_id: i64,
        conversation_id: i64,
        reply_mode: ReplyMode,
        proactive_enabled: bool,
        mute_schedule: Option<String>,
        behavior_overrides: serde_json::Value,
        context_policy: serde_json::Value,
    ) -> Result<CharacterBinding, RuntimeError> {
        // 校验角色存在。
        self.character_repo
            .find_by_id(character_id)
            .await?
            .ok_or(RuntimeError::Domain(DomainError::CharacterNotFound(
                character_id,
            )))?;

        // 校验会话存在。
        self.conversation_repo
            .find_by_id(conversation_id)
            .await?
            .ok_or(RuntimeError::Domain(DomainError::ConversationNotFound(
                conversation_id,
            )))?;

        // 检查该角色是否已在此会话中绑定（唯一冲突）。
        let existing = self.binding_repo.find_by_character_id(character_id).await?;
        if existing
            .iter()
            .any(|b| b.conversation_id == conversation_id)
        {
            return Err(RuntimeError::Domain(DomainError::InvalidState(format!(
                "角色 {character_id} 已绑定到会话 {conversation_id}"
            ))));
        }

        // G1：一个会话最多一个绑定（无论角色），保证"每会话单角色"。
        let by_conversation = self
            .binding_repo
            .find_by_conversation_id(conversation_id)
            .await?;
        if !by_conversation.is_empty() {
            return Err(RuntimeError::Domain(DomainError::InvalidState(format!(
                "会话 {conversation_id} 已绑定角色，一个会话只能绑定一个角色"
            ))));
        }

        let binding = CharacterBinding {
            id: 0,
            character_id,
            conversation_id,
            reply_mode,
            proactive_enabled,
            mute_schedule,
            behavior_overrides,
            context_policy,
            switched_at: None,
            cross_reply_enabled: false,
            created_at: Utc::now(),
        };
        self.binding_repo.insert(&binding).await?;

        let mut saved = binding;
        // 从仓储中回读以拿到真实的插入 ID（若仓储未填充 id）。
        if let Some(recorded) = self
            .binding_repo
            .find_by_character_id(character_id)
            .await?
            .into_iter()
            .find(|b| b.conversation_id == conversation_id)
        {
            saved = recorded;
        }
        Ok(saved)
    }

    /// 按 ID 删除一个绑定。
    pub async fn unbind(&self, id: i64) -> Result<(), RuntimeError> {
        self.binding_repo.delete(id).await?;
        Ok(())
    }

    /// 查询一个会话的所有绑定。
    pub async fn by_conversation(
        &self,
        conversation_id: i64,
    ) -> Result<Vec<CharacterBinding>, RuntimeError> {
        Ok(self
            .binding_repo
            .find_by_conversation_id(conversation_id)
            .await?)
    }

    /// 查询一个角色的所有绑定。
    pub async fn by_character(
        &self,
        character_id: i64,
    ) -> Result<Vec<CharacterBinding>, RuntimeError> {
        Ok(self.binding_repo.find_by_character_id(character_id).await?)
    }

    /// 把会话切换到另一个角色（换角色）。
    ///
    /// - 校验目标角色存在；
    /// - 取该会话当前绑定（多绑定脏数据时取第一个）；
    /// - 更新 character_id 并把 switched_at 置为当前时间；
    /// - 会话配置字段（reply_mode / proactive_enabled / mute_schedule /
    ///   behavior_overrides / context_policy）随绑定保留，不随角色迁移；
    /// - 使用仓储的单行原子 UPDATE，最小化与回复链路/主动 tick 的竞态窗口。
    pub async fn switch_character(
        &self,
        conversation_id: i64,
        new_character_id: i64,
    ) -> Result<CharacterBinding, RuntimeError> {
        // 校验新角色存在。
        self.character_repo
            .find_by_id(new_character_id)
            .await?
            .ok_or(RuntimeError::Domain(DomainError::CharacterNotFound(
                new_character_id,
            )))?;

        // 取该会话当前绑定（脏数据时取第一个）。
        let bindings = self
            .binding_repo
            .find_by_conversation_id(conversation_id)
            .await?;
        let Some(current) = bindings.into_iter().next() else {
            return Err(RuntimeError::Domain(DomainError::InvalidState(format!(
                "会话 {conversation_id} 未绑定角色，无法换角色"
            ))));
        };

        // 构造更新后的绑定：保留会话配置字段，仅换角色并记录生效时间。
        let mut updated = current.clone();
        updated.character_id = new_character_id;
        updated.switched_at = Some(Utc::now());
        self.binding_repo.update(&updated).await?;
        Ok(updated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::character::{Character, CharacterDefinition, CharacterState};
    use crate::domain::conversation::{Conversation, ConversationType};
    use crate::domain::repository::CharacterRepository;
    use crate::error::RepositoryError;
    use async_trait::async_trait;

    // 内存版仓储实现，用于隔离测试（不依赖 SQLite）。

    struct MemCharacterRepo {
        characters: std::sync::Mutex<Vec<Character>>,
    }

    #[async_trait]
    impl CharacterRepository for MemCharacterRepo {
        async fn find_by_id(&self, id: i64) -> Result<Option<Character>, RepositoryError> {
            Ok(self
                .characters
                .lock()
                .unwrap()
                .iter()
                .find(|c| c.id == id)
                .cloned())
        }
        async fn find_all(&self) -> Result<Vec<Character>, RepositoryError> {
            Ok(self.characters.lock().unwrap().clone())
        }
        async fn insert(&self, c: &Character) -> Result<i64, RepositoryError> {
            let mut chars = self.characters.lock().unwrap();
            let id = chars.len() as i64 + 1;
            let mut c = c.clone();
            c.id = id;
            chars.push(c);
            Ok(id)
        }
        async fn update(&self, _c: &Character) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemConvRepo {
        conversations: std::sync::Mutex<Vec<Conversation>>,
    }

    #[async_trait]
    impl ConversationRepository for MemConvRepo {
        async fn find_by_id(&self, id: i64) -> Result<Option<Conversation>, RepositoryError> {
            Ok(self
                .conversations
                .lock()
                .unwrap()
                .iter()
                .find(|c| c.id == id)
                .cloned())
        }
        async fn find_by_external_id(
            &self,
            _external_id: &str,
        ) -> Result<Option<Conversation>, RepositoryError> {
            Ok(None)
        }
        async fn find_all(&self) -> Result<Vec<Conversation>, RepositoryError> {
            Ok(self.conversations.lock().unwrap().clone())
        }
        async fn insert(&self, c: &Conversation) -> Result<i64, RepositoryError> {
            let mut convs = self.conversations.lock().unwrap();
            let id = convs.len() as i64 + 1;
            let mut c = c.clone();
            c.id = id;
            convs.push(c);
            Ok(id)
        }
        async fn update(&self, _c: &Conversation) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemBindingRepo {
        bindings: std::sync::Mutex<Vec<CharacterBinding>>,
        next_id: std::sync::Mutex<i64>,
    }

    #[async_trait]
    impl CharacterBindingRepository for MemBindingRepo {
        async fn find_by_character_id(
            &self,
            character_id: i64,
        ) -> Result<Vec<CharacterBinding>, RepositoryError> {
            Ok(self
                .bindings
                .lock()
                .unwrap()
                .iter()
                .filter(|b| b.character_id == character_id)
                .cloned()
                .collect())
        }
        async fn find_by_conversation_id(
            &self,
            conversation_id: i64,
        ) -> Result<Vec<CharacterBinding>, RepositoryError> {
            Ok(self
                .bindings
                .lock()
                .unwrap()
                .iter()
                .filter(|b| b.conversation_id == conversation_id)
                .cloned()
                .collect())
        }
        async fn find_all(&self) -> Result<Vec<CharacterBinding>, RepositoryError> {
            Ok(self.bindings.lock().unwrap().clone())
        }
        async fn find_all_enabled(&self) -> Result<Vec<CharacterBinding>, RepositoryError> {
            Ok(self
                .bindings
                .lock()
                .unwrap()
                .iter()
                .filter(|b| b.proactive_enabled)
                .cloned()
                .collect())
        }
        async fn insert(&self, b: &CharacterBinding) -> Result<i64, RepositoryError> {
            let mut bindings = self.bindings.lock().unwrap();
            let mut next = self.next_id.lock().unwrap();
            *next += 1;
            let mut b = b.clone();
            b.id = *next;
            bindings.push(b);
            Ok(*next)
        }
        async fn update(&self, binding: &CharacterBinding) -> Result<(), RepositoryError> {
            let mut bindings = self.bindings.lock().unwrap();
            if let Some(existing) = bindings.iter_mut().find(|b| b.id == binding.id) {
                *existing = binding.clone();
            }
            Ok(())
        }
        async fn delete(&self, id: i64) -> Result<(), RepositoryError> {
            let mut bindings = self.bindings.lock().unwrap();
            bindings.retain(|b| b.id != id);
            Ok(())
        }
    }

    fn sample_definition(name: &str) -> CharacterDefinition {
        CharacterDefinition {
            name: name.to_string(),
            description: None,
            personality: None,
            scenario: None,
            style: None,
            background: None,
            greetings: vec![],
            example_messages: vec![],
            system_prompt: None,
            post_history_instructions: None,
            lorebook: vec![],
            metadata: serde_json::json!({}),
        }
    }

    async fn setup() -> (BindingManager, Arc<MemCharacterRepo>, Arc<MemConvRepo>) {
        let char_repo = Arc::new(MemCharacterRepo {
            characters: std::sync::Mutex::new(vec![]),
        });
        let conv_repo = Arc::new(MemConvRepo {
            conversations: std::sync::Mutex::new(vec![]),
        });
        let binding_repo = Arc::new(MemBindingRepo {
            bindings: std::sync::Mutex::new(vec![]),
            next_id: std::sync::Mutex::new(0),
        });

        let now = chrono::Utc::now();
        char_repo
            .insert(&Character {
                id: 1,
                definition: sample_definition("Alice"),
                state: CharacterState::default(),
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        conv_repo
            .insert(&Conversation {
                id: 1,
                conversation_type: ConversationType::Private,
                external_id: "user42".to_string(),
                name: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        conv_repo
            .insert(&Conversation {
                id: 2,
                conversation_type: ConversationType::Group,
                external_id: "group9".to_string(),
                name: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();

        let manager = BindingManager::new(binding_repo, char_repo.clone(), conv_repo.clone());
        (manager, char_repo, conv_repo)
    }

    #[tokio::test]
    async fn bind_creates_and_returns_binding() {
        let (manager, _, _) = setup().await;
        let binding = manager
            .bind(
                1,
                1,
                ReplyMode::MentionOnly,
                true,
                None,
                serde_json::json!({}),
                serde_json::json!({"history": 20}),
            )
            .await
            .expect("bind 应成功");
        assert_eq!(binding.character_id, 1);
        assert_eq!(binding.conversation_id, 1);
        assert_eq!(binding.reply_mode, ReplyMode::MentionOnly);
        assert!(binding.proactive_enabled);
        assert!(binding.id > 0);
        // 新绑定的 switched_at 应为 None（从未换过角色）。
        assert!(binding.switched_at.is_none());
    }

    #[tokio::test]
    async fn bind_rejects_duplicate_for_same_conversation() {
        let (manager, _, _) = setup().await;
        manager
            .bind(
                1,
                1,
                ReplyMode::MentionOnly,
                true,
                None,
                serde_json::json!({}),
                serde_json::json!({}),
            )
            .await
            .expect("首次绑定的应成功");

        // 同一角色绑定到同一会话 → 报错
        let dup = manager
            .bind(
                1,
                1,
                ReplyMode::Natural,
                false,
                None,
                serde_json::json!({}),
                serde_json::json!({}),
            )
            .await;
        assert!(dup.is_err());
    }

    #[tokio::test]
    async fn bind_rejects_missing_character_or_conversation() {
        let (manager, _, _) = setup().await;
        // 不存在的角色
        assert!(manager
            .bind(
                999,
                1,
                ReplyMode::Natural,
                true,
                None,
                serde_json::json!({}),
                serde_json::json!({}),
            )
            .await
            .is_err());
        // 不存在的会话
        assert!(manager
            .bind(
                1,
                999,
                ReplyMode::Natural,
                true,
                None,
                serde_json::json!({}),
                serde_json::json!({}),
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn by_conversation_and_by_character() {
        let (manager, _, _) = setup().await;
        manager
            .bind(
                1,
                1,
                ReplyMode::MentionOnly,
                true,
                None,
                serde_json::json!({}),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        manager
            .bind(
                1,
                2,
                ReplyMode::Occasionally,
                false,
                Some("0 0 * * *".to_string()),
                serde_json::json!({}),
                serde_json::json!({}),
            )
            .await
            .unwrap();

        let by_conv = manager.by_conversation(1).await.unwrap();
        assert_eq!(by_conv.len(), 1);

        let by_char = manager.by_character(1).await.unwrap();
        assert_eq!(by_char.len(), 2);
    }

    #[tokio::test]
    async fn unbind_removes_binding() {
        let (manager, _, _) = setup().await;
        let binding = manager
            .bind(
                1,
                1,
                ReplyMode::MentionOnly,
                true,
                None,
                serde_json::json!({}),
                serde_json::json!({}),
            )
            .await
            .unwrap();

        manager.unbind(binding.id).await.expect("unbind 应成功");
        let by_conv = manager.by_conversation(1).await.unwrap();
        assert!(by_conv.is_empty());
    }

    #[tokio::test]
    async fn bind_rejects_second_character_for_same_conversation() {
        let (manager, char_repo, _) = setup().await;
        let now = chrono::Utc::now();
        // 插入角色 2（MemCharacterRepo.insert 自动分配 id=2）。
        char_repo
            .insert(&Character {
                id: 0,
                definition: sample_definition("Bob"),
                state: CharacterState::default(),
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();

        // 角色 1 → 会话 1 成功。
        manager
            .bind(
                1,
                1,
                ReplyMode::MentionOnly,
                true,
                None,
                serde_json::json!({}),
                serde_json::json!({}),
            )
            .await
            .expect("首次绑定应成功");

        // G1：角色 2 → 会话 1 必须被拒绝（一个会话最多一个绑定）。
        let second = manager
            .bind(
                2,
                1,
                ReplyMode::Natural,
                false,
                None,
                serde_json::json!({}),
                serde_json::json!({}),
            )
            .await;
        assert!(second.is_err(), "同一会话的第二角色绑定应被拒绝");
    }

    #[tokio::test]
    async fn switch_character_updates_character_preserves_config() {
        let (manager, char_repo, _) = setup().await;
        let now = chrono::Utc::now();
        // 插入角色 2。
        char_repo
            .insert(&Character {
                id: 0,
                definition: sample_definition("Bob"),
                state: CharacterState::default(),
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();

        let binding = manager
            .bind(
                1,
                1,
                ReplyMode::MentionOnly,
                true,
                Some("22:00-07:00".to_string()),
                serde_json::json!({"tone": "cool"}),
                serde_json::json!({"history": 30}),
            )
            .await
            .expect("首次绑定应成功");
        assert!(binding.switched_at.is_none());

        let switched = manager.switch_character(1, 2).await.expect("换角色应成功");
        // 角色已切换，并记录了生效时间。
        assert_eq!(switched.character_id, 2);
        assert!(switched.switched_at.is_some(), "切换后应记录 switched_at");
        // 会话配置字段随绑定保留，不随角色迁移。
        assert_eq!(switched.reply_mode, ReplyMode::MentionOnly);
        assert!(switched.proactive_enabled);
        assert_eq!(switched.mute_schedule.as_deref(), Some("22:00-07:00"));
        assert_eq!(
            switched.behavior_overrides,
            serde_json::json!({"tone": "cool"})
        );
        assert_eq!(switched.context_policy, serde_json::json!({"history": 30}));
        assert_eq!(switched.id, binding.id, "绑定 id 应保持不变");
    }

    #[tokio::test]
    async fn switch_character_rejects_missing_character() {
        let (manager, _, _) = setup().await;
        manager
            .bind(
                1,
                1,
                ReplyMode::Natural,
                true,
                None,
                serde_json::json!({}),
                serde_json::json!({}),
            )
            .await
            .unwrap();

        // 目标角色不存在 → 报错。
        assert!(manager.switch_character(1, 999).await.is_err());
    }

    #[tokio::test]
    async fn switch_character_rejects_unbound_conversation() {
        let (manager, _, _) = setup().await;
        // 会话 2 存在但未绑定角色（setup 已创建会话 2）。
        let result = manager.switch_character(2, 1).await;
        assert!(result.is_err(), "未绑定角色的会话换角色应报错");
    }
}
