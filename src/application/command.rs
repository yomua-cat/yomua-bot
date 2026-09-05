//! 系统指令模块 —— 识别、权限校验与执行。
//!
//! 纯函数 `classify` / `is_admin` 供适配层与应用层复用；
//! `CommandHandler` 订阅 `CommandReceived` 事件并执行指令（换角色）。
//!
//! 硬性约束 B：指令消息在消息发布位被截流，不发布 `MessageReceived`，
//! 因此不落库、不进角色上下文、不进插件 message 订阅。

use std::sync::Arc;

use crate::application::action::ActionDispatcher;
use crate::application::binding::BindingManager;
use crate::application::event_bus::EventBus;
use crate::domain::conversation::ConversationType;
use crate::domain::event::{Command, CommandReceivedEvent, CoreEvent};
use crate::domain::repository::CharacterRepository;
use crate::error::RuntimeError;

/// 识别一条入站消息是否为系统指令。
///
/// - 群聊：必须明确 @ 机器人（is_mentioned），避免误触；
/// - 私聊：消息本身就是发给机器人的，无需 @；
/// - 匹配形态："换角色 <角色名>"（全字匹配，避免"换角色xx"粘连误触）。
pub fn classify(
    content: &str,
    is_mentioned: bool,
    conversation_type: ConversationType,
) -> Option<Command> {
    if conversation_type == ConversationType::Group && !is_mentioned {
        return None;
    }
    let trimmed = content.trim();
    let rest = trimmed.strip_prefix("换角色")?;
    // 前缀后必须紧跟空白且名字非空（全字匹配，防误触）。
    if rest.is_empty() {
        return None;
    }
    let first = rest.chars().next().expect("rest 非空");
    if !first.is_whitespace() {
        return None;
    }
    let name = rest.trim();
    if name.is_empty() {
        return None;
    }
    Some(Command::SwitchCharacter {
        character_name: name.to_string(),
    })
}

/// 权限判定纯函数：仅配置在 `admin_users` 中的发送者可执行系统指令。
///
/// `admin_users` 为 None（未配置管理员）时任何人无权限。
pub fn is_admin(sender_external_id: &str, admin_users: Option<&[String]>) -> bool {
    admin_users
        .map(|list| list.iter().any(|id| id == sender_external_id))
        .unwrap_or(false)
}

/// 系统指令执行器：订阅 `CommandReceived`，校验权限后执行换角色并中文回复结果。
pub struct CommandHandler {
    binding_manager: Arc<BindingManager>,
    character_repo: Arc<dyn CharacterRepository>,
    action_dispatcher: Arc<ActionDispatcher>,
    admin_users: Option<Vec<String>>,
}

impl CommandHandler {
    pub fn new(
        binding_manager: Arc<BindingManager>,
        character_repo: Arc<dyn CharacterRepository>,
        action_dispatcher: Arc<ActionDispatcher>,
        admin_users: Option<Vec<String>>,
    ) -> Self {
        Self {
            binding_manager,
            character_repo,
            action_dispatcher,
            admin_users,
        }
    }

    /// 从事件总线持续消费并处理 `CommandReceived`。
    pub async fn run(self, bus: &EventBus) {
        let mut subscription = bus.subscribe();
        while let Some(event) = subscription.recv().await {
            let CoreEvent::CommandReceived(e) = &event else {
                continue;
            };
            if let Err(err) = self.handle(e).await {
                tracing::warn!(target: "command", error = %err, "指令处理失败");
            }
        }
    }

    /// 处理一条指令：权限校验 → 执行 → 中文结果回复。
    pub async fn handle(&self, event: &CommandReceivedEvent) -> Result<(), RuntimeError> {
        // 1. 权限校验：非管理员直接中文拒绝（不执行）。
        if !is_admin(&event.external_sender_id, self.admin_users.as_deref()) {
            self.reply(event.conversation_id, "你没有权限执行该指令。")
                .await?;
            return Ok(());
        }

        // 2. 解析并执行指令。
        match &event.command {
            Command::SwitchCharacter { character_name } => {
                // 2.1 按名字查找目标角色（精确匹配；不存在则中文提示）。
                let characters = self.character_repo.find_all().await?;
                let Some(target) = characters
                    .iter()
                    .find(|c| c.definition.name == *character_name)
                else {
                    self.reply(
                        event.conversation_id,
                        &format!("未找到角色「{character_name}」，请先导入该角色。"),
                    )
                    .await?;
                    return Ok(());
                };

                // 2.2 执行换角色（会话配置字段随绑定保留）。
                match self
                    .binding_manager
                    .switch_character(event.conversation_id, target.id)
                    .await
                {
                    Ok(_) => {
                        self.reply(
                            event.conversation_id,
                            &format!("已把本会话角色切换为「{character_name}」。"),
                        )
                        .await?;
                    }
                    Err(err) => {
                        self.reply(event.conversation_id, &format!("换角色失败：{err}"))
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// 通过动作分发器向会话发送一条文本消息（复用既有发送链路）。
    async fn reply(&self, conversation_id: i64, content: &str) -> Result<(), RuntimeError> {
        self.action_dispatcher
            .execute(&crate::domain::behavior::Action::SendMessage {
                conversation_id,
                content: content.to_string(),
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::onebot::{OneBotAdapter, OneBotConnectionState};
    use crate::domain::character::{
        Character, CharacterBinding, CharacterDefinition, CharacterState, ReplyMode,
    };
    use crate::domain::conversation::Conversation;
    use crate::domain::repository::{
        CharacterBindingRepository, CharacterRepository, ConversationRepository,
    };
    use crate::error::RepositoryError;
    use async_trait::async_trait;
    use chrono::Utc;
    use std::sync::Mutex;

    // ------------------------------------------------------------------
    // 内存仓储（隔离测试，不依赖 SQLite）
    // ------------------------------------------------------------------

    struct MemCharacterRepo {
        chars: Mutex<Vec<Character>>,
    }
    #[async_trait]
    impl CharacterRepository for MemCharacterRepo {
        async fn find_by_id(&self, id: i64) -> Result<Option<Character>, RepositoryError> {
            Ok(self
                .chars
                .lock()
                .unwrap()
                .iter()
                .find(|c| c.id == id)
                .cloned())
        }
        async fn find_all(&self) -> Result<Vec<Character>, RepositoryError> {
            Ok(self.chars.lock().unwrap().clone())
        }
        async fn insert(&self, _c: &Character) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _c: &Character) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemConvRepo {
        convs: Mutex<Vec<Conversation>>,
    }
    #[async_trait]
    impl ConversationRepository for MemConvRepo {
        async fn find_by_id(&self, id: i64) -> Result<Option<Conversation>, RepositoryError> {
            Ok(self
                .convs
                .lock()
                .unwrap()
                .iter()
                .find(|c| c.id == id)
                .cloned())
        }
        async fn find_by_external_id(
            &self,
            _id: &str,
        ) -> Result<Option<Conversation>, RepositoryError> {
            Ok(None)
        }
        async fn find_all(&self) -> Result<Vec<Conversation>, RepositoryError> {
            Ok(self.convs.lock().unwrap().clone())
        }
        async fn insert(&self, _c: &Conversation) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _c: &Conversation) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemBindingRepo {
        bindings: Mutex<Vec<CharacterBinding>>,
    }
    #[async_trait]
    impl CharacterBindingRepository for MemBindingRepo {
        async fn find_by_character_id(
            &self,
            _id: i64,
        ) -> Result<Vec<CharacterBinding>, RepositoryError> {
            Ok(vec![])
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
        async fn insert(&self, _b: &CharacterBinding) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, binding: &CharacterBinding) -> Result<(), RepositoryError> {
            let mut bindings = self.bindings.lock().unwrap();
            if let Some(existing) = bindings.iter_mut().find(|b| b.id == binding.id) {
                *existing = binding.clone();
            }
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    // ------------------------------------------------------------------
    // 假适配器：记录发送内容（复用既有 ActionDispatcher + FakeAdapter 测试先例）
    // ------------------------------------------------------------------

    struct FakeAdapter {
        sent: Mutex<Vec<(String, String)>>,
    }
    #[async_trait]
    impl OneBotAdapter for FakeAdapter {
        async fn start(&self) -> Result<(), RuntimeError> {
            Ok(())
        }
        async fn stop(&self) -> Result<(), RuntimeError> {
            Ok(())
        }
        async fn state(&self) -> OneBotConnectionState {
            OneBotConnectionState::Connected
        }
        async fn send_group_message(
            &self,
            group_id: &str,
            content: &str,
        ) -> Result<(), RuntimeError> {
            self.sent
                .lock()
                .unwrap()
                .push((group_id.to_string(), content.to_string()));
            Ok(())
        }
        async fn send_private_message(
            &self,
            user_id: &str,
            content: &str,
        ) -> Result<(), RuntimeError> {
            self.sent
                .lock()
                .unwrap()
                .push((user_id.to_string(), content.to_string()));
            Ok(())
        }
    }

    // ------------------------------------------------------------------
    // 装配
    // ------------------------------------------------------------------

    fn sample_character(id: i64, name: &str) -> Character {
        let now = Utc::now();
        Character {
            id,
            definition: CharacterDefinition {
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
            },
            state: CharacterState::default(),
            created_at: now,
            updated_at: now,
        }
    }

    /// 标准装配：会话 100（私聊，外部 u100）绑定角色 1（木然），另有角色 2（苏苏）。
    #[allow(clippy::type_complexity)]
    fn setup() -> (CommandHandler, Arc<MemBindingRepo>, Arc<FakeAdapter>) {
        let character_repo = Arc::new(MemCharacterRepo {
            chars: Mutex::new(vec![
                sample_character(1, "木然"),
                sample_character(2, "苏苏"),
            ]),
        });
        let character_repo_trait: Arc<dyn CharacterRepository> = character_repo.clone();

        let conv_repo = Arc::new(MemConvRepo {
            convs: Mutex::new(vec![Conversation {
                id: 100,
                conversation_type: crate::domain::conversation::ConversationType::Private,
                external_id: "u100".to_string(),
                name: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            }]),
        });
        let binding_repo = Arc::new(MemBindingRepo {
            bindings: Mutex::new(vec![CharacterBinding {
                id: 1,
                character_id: 1,
                conversation_id: 100,
                reply_mode: ReplyMode::MentionOnly,
                proactive_enabled: false,
                mute_schedule: None,
                behavior_overrides: serde_json::json!({}),
                context_policy: serde_json::json!({}),
                switched_at: None,
                cross_reply_enabled: false,
                created_at: Utc::now(),
            }]),
        });

        let adapter = Arc::new(FakeAdapter {
            sent: Mutex::new(vec![]),
        });
        let binding_manager = Arc::new(BindingManager::new(
            binding_repo.clone(),
            character_repo,
            conv_repo.clone(),
        ));
        let action_dispatcher = Arc::new(ActionDispatcher::new(conv_repo, adapter.clone()));

        let handler = CommandHandler::new(
            binding_manager,
            character_repo_trait,
            action_dispatcher,
            Some(vec!["900001".to_string()]),
        );
        (handler, binding_repo, adapter)
    }

    fn sample_event(character_name: &str) -> CommandReceivedEvent {
        CommandReceivedEvent {
            conversation_id: 100,
            sender_id: 55,
            external_sender_id: "900001".to_string(),
            message_id: 1001,
            content: format!("换角色 {character_name}"),
            timestamp: Utc::now(),
            command: Command::SwitchCharacter {
                character_name: character_name.to_string(),
            },
        }
    }

    // ------------------------------------------------------------------
    // classify 纯函数测试
    // ------------------------------------------------------------------

    #[test]
    fn classify_group_requires_mention() {
        // 群聊未 @ → 不识别；@ 后 → 识别。
        assert_eq!(
            classify(
                "换角色 木然",
                false,
                crate::domain::conversation::ConversationType::Group
            ),
            None
        );
        assert_eq!(
            classify(
                "换角色 木然",
                true,
                crate::domain::conversation::ConversationType::Group
            ),
            Some(Command::SwitchCharacter {
                character_name: "木然".to_string(),
            })
        );
    }

    #[test]
    fn classify_private_no_mention_needed() {
        // 私聊无需 @，直接识别。
        assert_eq!(
            classify(
                "换角色 木然",
                false,
                crate::domain::conversation::ConversationType::Private
            ),
            Some(Command::SwitchCharacter {
                character_name: "木然".to_string(),
            })
        );
    }

    #[test]
    fn classify_matches_switch_character() {
        // 群聊 + @ + "换角色 木然" → 识别；多空格 → 名字收尾 trim。
        assert_eq!(
            classify(
                "换角色 木然",
                true,
                crate::domain::conversation::ConversationType::Group
            ),
            Some(Command::SwitchCharacter {
                character_name: "木然".to_string(),
            })
        );
        assert_eq!(
            classify(
                "换角色   木然",
                true,
                crate::domain::conversation::ConversationType::Group
            ),
            Some(Command::SwitchCharacter {
                character_name: "木然".to_string(),
            })
        );
    }

    #[test]
    fn classify_rejects_glued_text() {
        // "换角色木然"（无空白）→ 不识别（全字匹配防误触）。
        assert_eq!(
            classify(
                "换角色木然",
                true,
                crate::domain::conversation::ConversationType::Group
            ),
            None
        );
    }

    #[test]
    fn classify_rejects_missing_name() {
        // "换角色" 或 "换角色  " → 名字为空，不识别。
        assert_eq!(
            classify(
                "换角色",
                true,
                crate::domain::conversation::ConversationType::Group
            ),
            None
        );
        assert_eq!(
            classify(
                "换角色  ",
                true,
                crate::domain::conversation::ConversationType::Group
            ),
            None
        );
    }

    #[test]
    fn classify_ignores_unrelated_text() {
        // 不以"换角色"开头 → 不识别；粘连的其他文本 → 不识别。
        assert_eq!(
            classify(
                "你好 换角色",
                true,
                crate::domain::conversation::ConversationType::Group
            ),
            None
        );
        assert_eq!(
            classify(
                "换角色牌",
                true,
                crate::domain::conversation::ConversationType::Group
            ),
            None
        );
    }

    // ------------------------------------------------------------------
    // is_admin 权限判定测试
    // ------------------------------------------------------------------

    #[test]
    fn is_admin_checks_membership() {
        // 未配置管理员 → 任何人无权限；配置后按外部 ID 精确匹配。
        assert!(!is_admin("123", None));
        let admins = vec!["123".to_string()];
        assert!(is_admin("123", Some(&admins)));
        assert!(!is_admin("456", Some(&admins)));
    }

    // ------------------------------------------------------------------
    // CommandHandler 执行测试
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handle_switch_character_success() {
        let (handler, binding_repo, adapter) = setup();
        handler
            .handle(&sample_event("苏苏"))
            .await
            .expect("管理员执行换角色应成功");

        // 已发送"已把本会话角色切换为「苏苏」"。
        {
            let sent = adapter.sent.lock().unwrap();
            assert_eq!(sent.len(), 1);
            assert_eq!(sent[0].0, "u100");
            assert_eq!(sent[0].1, "已把本会话角色切换为「苏苏」。");
        }

        // 绑定已更新为角色 2。
        let bindings = binding_repo
            .find_by_conversation_id(100)
            .await
            .expect("查询绑定应成功");
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].character_id, 2);
        assert!(
            bindings[0].switched_at.is_some(),
            "换角色后应记录 switched_at"
        );
    }

    #[tokio::test]
    async fn handle_rejects_non_admin() {
        // 未配置管理员 → 中文拒绝且绑定不变。
        let (mut handler, binding_repo, adapter) = setup();
        handler.admin_users = None;

        handler
            .handle(&sample_event("苏苏"))
            .await
            .expect("非管理员拒绝不应报错");

        {
            let sent = adapter.sent.lock().unwrap();
            assert_eq!(sent.len(), 1);
            assert_eq!(sent[0].1, "你没有权限执行该指令。");
        }

        let bindings = binding_repo
            .find_by_conversation_id(100)
            .await
            .expect("查询绑定应成功");
        assert_eq!(bindings[0].character_id, 1, "非管理员不应改变绑定");
    }

    #[tokio::test]
    async fn handle_character_not_found() {
        let (handler, binding_repo, adapter) = setup();
        handler
            .handle(&sample_event("不存在的角色"))
            .await
            .expect("角色不存在提示不应报错");

        {
            let sent = adapter.sent.lock().unwrap();
            assert_eq!(sent.len(), 1);
            assert_eq!(sent[0].1, "未找到角色「不存在的角色」，请先导入该角色。");
        }

        let bindings = binding_repo
            .find_by_conversation_id(100)
            .await
            .expect("查询绑定应成功");
        assert_eq!(bindings[0].character_id, 1, "未找到角色时不应改变绑定");
    }
}
