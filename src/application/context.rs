//! 会话上下文组装 —— `ContextBuilder` 深化版。
//!
//! 本模块聚焦**纯数据组装**：把角色、会话、最近消息、命中的 lorebook 条目、
//! 记忆、关系、当前情绪、场景与历史后指令聚合为一个
//! [`ConversationContext`]，供上层（认知 / 行为）使用。它不负责 LLM
//! prompt 格式设计（那在认知层完成）。

use std::sync::Arc;

use crate::domain::character::{Character, CharacterBinding, LorebookEntry};
use crate::domain::conversation::Conversation;
use crate::domain::emotion::EmotionState;
use crate::domain::memory::Memory;
use crate::domain::message::{Message, MessageContent};
use crate::domain::relationship::Relationship;
use crate::domain::repository::{
    CharacterBindingRepository, ConversationRepository, EmotionStateRepository, MemoryRepository,
    MessageRepository, RelationshipRepository,
};
use crate::error::{DomainError, RuntimeError};

/// 上下文组装时的各类数量上限。
#[derive(Debug, Clone, Copy)]
pub struct ContextLimits {
    /// 最近消息条数。
    pub context_limit: usize,
    /// 记忆条数上限。
    pub memory_limit: usize,
    /// 命中的 lorebook 条数上限。
    pub lorebook_limit: usize,
}

impl Default for ContextLimits {
    fn default() -> Self {
        Self {
            context_limit: 20,
            memory_limit: 10,
            lorebook_limit: 5,
        }
    }
}

/// 一个会话的已组装上下文。
#[derive(Debug, Clone)]
pub struct ConversationContext {
    /// 相关角色。
    pub character: Character,
    /// 相关会话。
    pub conversation: Conversation,
    /// 最近的会话消息（最旧在前）。
    pub recent_messages: Vec<Message>,
    /// 从角色 lorebook 中命中的条目（按优先级降序，已截断）。
    pub matching_lorebook: Vec<LorebookEntry>,
    /// 该会话中的角色绑定（若有）。
    pub binding: Option<CharacterBinding>,
    /// 角色在该会话相关的记忆（已截断）。
    pub memory: Vec<Memory>,
    /// 与当前参与者（发送者）的关系（若有）。
    pub relationship: Option<Relationship>,
    /// 角色当前情绪状态（若有）。
    pub current_emotion: Option<EmotionState>,
    /// 角色定义的场景设定（若有）。
    pub scenario: Option<String>,
    /// 角色定义的历史后指令（若有）。
    pub post_history_instructions: Option<String>,
}

/// 上下文组装器。
///
/// 依赖仓储 trait（而非具体 SQLite 实现），可在测试中用内存实现替换。
pub struct ContextBuilder {
    message_repo: Arc<dyn MessageRepository>,
    conversation_repo: Arc<dyn ConversationRepository>,
    memory_repo: Arc<dyn MemoryRepository>,
    relationship_repo: Arc<dyn RelationshipRepository>,
    emotion_repo: Arc<dyn EmotionStateRepository>,
    binding_repo: Arc<dyn CharacterBindingRepository>,
}

impl ContextBuilder {
    /// 创建一个上下文组装器。
    pub fn new(
        message_repo: Arc<dyn MessageRepository>,
        conversation_repo: Arc<dyn ConversationRepository>,
        memory_repo: Arc<dyn MemoryRepository>,
        relationship_repo: Arc<dyn RelationshipRepository>,
        emotion_repo: Arc<dyn EmotionStateRepository>,
        binding_repo: Arc<dyn CharacterBindingRepository>,
    ) -> Self {
        Self {
            message_repo,
            conversation_repo,
            memory_repo,
            relationship_repo,
            emotion_repo,
            binding_repo,
        }
    }

    /// 组装一个会话的上下文。
    ///
    /// - 加载会话与最近消息（上限 `limits.context_limit`）；
    /// - 从角色 lorebook 挑选启用且命中最近消息关键词的条目，按优先级降序并截断；
    /// - 加载该会话中该角色的绑定；
    /// - 加载该角色的记忆、与参与者的关系、当前情绪；
    /// - 携带角色定义的场景与历史后指令。
    pub async fn build(
        &self,
        character: &Character,
        conversation_id: i64,
        participant_id: i64,
        limits: ContextLimits,
    ) -> Result<ConversationContext, RuntimeError> {
        let conversation = self
            .conversation_repo
            .find_by_id(conversation_id)
            .await?
            .ok_or(RuntimeError::Domain(DomainError::ConversationNotFound(
                conversation_id,
            )))?;

        let recent_messages = self
            .message_repo
            .find_recent(conversation_id, limits.context_limit as i64)
            .await?;

        let matching_lorebook = {
            let mut matched = self.match_lorebook(character, &recent_messages);
            matched.truncate(limits.lorebook_limit);
            matched
        };

        // 该会话中该角色的绑定。
        let binding = self
            .binding_repo
            .find_by_conversation_id(conversation_id)
            .await?
            .into_iter()
            .find(|b| b.character_id == character.id);

        // 记忆：结合「按重要度的近期记忆」与「按最近消息关键词的结构化检索」，
        // 合并去重后截断到上限（MVP 不做向量检索）。
        let memory = {
            let by_importance = self
                .memory_repo
                .find_by_character_id(character.id, None, limits.memory_limit as i64)
                .await?;
            let keywords = extract_keywords(&recent_messages_text(&recent_messages));
            let by_keyword = if keywords.is_empty() {
                Vec::new()
            } else {
                self.memory_repo
                    .search_by_keywords(character.id, &keywords, limits.memory_limit as i64)
                    .await?
            };
            merge_memories(by_importance, by_keyword, limits.memory_limit)
        };

        // 与当前参与者的关系。
        let relationship = self
            .relationship_repo
            .find(character.id, participant_id)
            .await?;

        // 当前情绪。
        let current_emotion = self.emotion_repo.find_by_character_id(character.id).await?;

        Ok(ConversationContext {
            character: character.clone(),
            conversation,
            recent_messages,
            matching_lorebook,
            binding,
            memory,
            relationship,
            current_emotion,
            scenario: character.definition.scenario.clone(),
            post_history_instructions: character.definition.post_history_instructions.clone(),
        })
    }

    /// 从角色 lorebook 中选出匹配最近消息文本的启用条目，按优先级降序。
    fn match_lorebook(&self, character: &Character, messages: &[Message]) -> Vec<LorebookEntry> {
        let haystack = messages
            .iter()
            .map(message_text)
            .collect::<Vec<_>>()
            .join("\n")
            .to_lowercase();

        let mut matched: Vec<LorebookEntry> = character
            .definition
            .lorebook
            .iter()
            .filter(|entry| entry.enabled)
            .filter(|entry| {
                entry
                    .keywords
                    .iter()
                    .any(|kw| !kw.trim().is_empty() && haystack.contains(&kw.to_lowercase()))
            })
            .cloned()
            .collect();

        // 按 priority 降序（越重要的越靠前）。
        matched.sort_by_key(|e| std::cmp::Reverse(e.priority));
        matched
    }
}

/// 提取一条消息的纯文本内容（供关键词匹配）。
fn message_text(message: &Message) -> String {
    match &message.content {
        MessageContent::Text(s) => s.clone(),
        MessageContent::Mixed(segments) => segments
            .iter()
            .filter_map(|seg| match seg {
                crate::domain::message::MixedContentSegment::Text(s) => Some(s.clone()),
                crate::domain::message::MixedContentSegment::Mention { display_name, .. } => {
                    Some(display_name.clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" "),
        MessageContent::Other(s) => s.clone(),
        // 图片 / 文件无可用文本。
        _ => String::new(),
    }
}

/// 汇总最近消息的纯文本（供关键词检索）。
fn recent_messages_text(messages: &[Message]) -> String {
    messages
        .iter()
        .map(message_text)
        .collect::<Vec<_>>()
        .join("\n")
}

/// 从一段文本中提取检索关键词。
///
/// MVP 采用结构化切分：按空白与常见标点切分，保留 1~12 字符的段作为 LIKE
/// 关键词（过长的段易过度匹配），并剔除常见中文单字助词避免噪声。
fn extract_keywords(text: &str) -> Vec<String> {
    const DELIMITERS: &str = " ，。！？、；：,.!?;:()（）\"'“”‘’@#【】[]";
    const STOPWORDS: &[&str] = &[
        "的", "了", "是", "在", "我", "你", "他", "她", "它", "吗", "呢",
    ];
    text.split(|c: char| c.is_whitespace() || DELIMITERS.contains(c))
        .map(str::trim)
        .filter(|s| {
            let n = s.chars().count();
            (1..=12).contains(&n) && !(n == 1 && STOPWORDS.contains(s))
        })
        .map(String::from)
        .collect()
}

/// 合并「按重要度」与「按关键词」两组记忆：去重、按重要度降序、截断。
fn merge_memories(
    by_importance: Vec<Memory>,
    by_keyword: Vec<Memory>,
    limit: usize,
) -> Vec<Memory> {
    let mut merged: Vec<Memory> = Vec::with_capacity(by_importance.len() + by_keyword.len());
    for m in by_importance.into_iter().chain(by_keyword) {
        // 去重：已持久化的记忆按 id 去重；id 为 0（未持久化）时始终追加。
        if m.id == 0 || !merged.iter().any(|x| x.id == m.id) {
            merged.push(m);
        }
    }
    merged.sort_by(|a, b| {
        b.importance
            .partial_cmp(&a.importance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    merged.truncate(limit);
    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::character::{Character, CharacterDefinition, CharacterState};
    use crate::domain::conversation::ConversationType;
    use crate::domain::relationship::Relationship;
    use crate::error::RepositoryError;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct MemMessageRepo {
        messages: Mutex<Vec<Message>>,
    }
    #[async_trait]
    impl MessageRepository for MemMessageRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Message>, RepositoryError> {
            Ok(None)
        }
        async fn find_recent(
            &self,
            _conversation_id: i64,
            limit: i64,
        ) -> Result<Vec<Message>, RepositoryError> {
            let mut sorted = self.messages.lock().unwrap().clone();
            sorted.sort_by_key(|m| m.timestamp);
            let len = sorted.len();
            let start = len.saturating_sub(limit as usize);
            Ok(sorted[start..].to_vec())
        }
        async fn insert(&self, _m: &Message) -> Result<i64, RepositoryError> {
            Ok(1)
        }
    }

    struct MemConvRepo {
        conversation: Option<Conversation>,
    }
    #[async_trait]
    impl ConversationRepository for MemConvRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Conversation>, RepositoryError> {
            Ok(self.conversation.clone())
        }
        async fn find_by_external_id(
            &self,
            _id: &str,
        ) -> Result<Option<Conversation>, RepositoryError> {
            Ok(None)
        }
        async fn find_all(&self) -> Result<Vec<Conversation>, RepositoryError> {
            Ok(self.conversation.clone().into_iter().collect())
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

    struct MemMemoryRepo {
        memories: Mutex<Vec<Memory>>,
    }
    #[async_trait]
    impl MemoryRepository for MemMemoryRepo {
        async fn find_by_character_id(
            &self,
            character_id: i64,
            _memory_type: Option<crate::domain::memory::MemoryType>,
            limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            let mut all = self
                .memories
                .lock()
                .unwrap()
                .iter()
                .filter(|m| m.character_id == character_id)
                .cloned()
                .collect::<Vec<_>>();
            all.sort_by(|a, b| {
                b.importance
                    .partial_cmp(&a.importance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            all.truncate(limit as usize);
            Ok(all)
        }
        async fn search_by_keywords(
            &self,
            character_id: i64,
            keywords: &[String],
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            let mut hits = self
                .memories
                .lock()
                .unwrap()
                .iter()
                .filter(|m| {
                    m.character_id == character_id
                        && keywords
                            .iter()
                            .any(|kw| m.content.to_lowercase().contains(&kw.to_lowercase()))
                })
                .cloned()
                .collect::<Vec<_>>();
            hits.sort_by(|a, b| {
                b.importance
                    .partial_cmp(&a.importance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            Ok(hits)
        }
        async fn insert(&self, _m: &Memory) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _m: &Memory) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemRelationshipRepo {
        relationships: Mutex<Vec<Relationship>>,
    }
    #[async_trait]
    impl RelationshipRepository for MemRelationshipRepo {
        async fn find(
            &self,
            character_id: i64,
            participant_id: i64,
        ) -> Result<Option<Relationship>, RepositoryError> {
            Ok(self
                .relationships
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.character_id == character_id && r.participant_id == participant_id)
                .cloned())
        }
        async fn find_by_character_id(
            &self,
            _character_id: i64,
        ) -> Result<Vec<Relationship>, RepositoryError> {
            Ok(vec![])
        }
        async fn upsert(&self, _r: &Relationship) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemEmotionRepo {
        states: Mutex<HashMap<i64, EmotionState>>,
    }
    #[async_trait]
    impl EmotionStateRepository for MemEmotionRepo {
        async fn find_by_character_id(
            &self,
            character_id: i64,
        ) -> Result<Option<EmotionState>, RepositoryError> {
            Ok(self.states.lock().unwrap().get(&character_id).cloned())
        }
        async fn upsert(
            &self,
            character_id: i64,
            state: &EmotionState,
        ) -> Result<(), RepositoryError> {
            self.states
                .lock()
                .unwrap()
                .insert(character_id, state.clone());
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
            _character_id: i64,
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
        async fn insert(&self, _b: &CharacterBinding) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    fn text_message(id: i64, content: &str) -> Message {
        Message {
            id,
            conversation_id: 10,
            sender_id: 1,
            content: MessageContent::Text(content.to_string()),
            timestamp: chrono::Utc::now(),
            reply_to: None,
            mentions: vec![],
            attachments: vec![],
            metadata: serde_json::json!({}),
        }
    }

    fn lorebook_entry(keywords: &[&str], priority: i32, enabled: bool) -> LorebookEntry {
        LorebookEntry {
            keywords: keywords.iter().map(|s| s.to_string()).collect(),
            content: format!("content-{priority}"),
            enabled,
            priority,
        }
    }

    fn memory(id: i64, importance: f64) -> Memory {
        Memory {
            id,
            character_id: 1,
            conversation_id: Some(10),
            memory_type: crate::domain::memory::MemoryType::Episodic,
            content: format!("记忆-{id}"),
            importance,
            created_at: chrono::Utc::now(),
            last_accessed: chrono::Utc::now(),
            metadata: serde_json::json!({}),
        }
    }

    fn build_character() -> Character {
        Character {
            id: 1,
            definition: CharacterDefinition {
                name: "Alice".to_string(),
                description: None,
                personality: None,
                scenario: Some("咖啡馆".to_string()),
                style: None,
                background: None,
                greetings: vec![],
                example_messages: vec![],
                system_prompt: None,
                post_history_instructions: Some("请以角色身份回答".to_string()),
                lorebook: vec![
                    lorebook_entry(&["chat"], 2, true),
                    lorebook_entry(&["cat"], 5, true),
                    lorebook_entry(&["disabled"], 9, false),
                ],
                metadata: serde_json::json!({}),
            },
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    async fn build_builder(
        messages: Vec<Message>,
        memories: Vec<Memory>,
        relationships: Vec<Relationship>,
        bindings: Vec<CharacterBinding>,
    ) -> ContextBuilder {
        let msg_repo = Arc::new(MemMessageRepo {
            messages: Mutex::new(messages),
        });
        let conv_repo = Arc::new(MemConvRepo {
            conversation: Some(Conversation {
                id: 10,
                conversation_type: ConversationType::Private,
                external_id: "u1".to_string(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            }),
        });
        let memory_repo = Arc::new(MemMemoryRepo {
            memories: Mutex::new(memories),
        });
        let relationship_repo = Arc::new(MemRelationshipRepo {
            relationships: Mutex::new(relationships),
        });
        let emotion_repo = Arc::new(MemEmotionRepo {
            states: Mutex::new(HashMap::new()),
        });
        let binding_repo = Arc::new(MemBindingRepo {
            bindings: Mutex::new(bindings),
        });
        ContextBuilder::new(
            msg_repo,
            conv_repo,
            memory_repo,
            relationship_repo,
            emotion_repo,
            binding_repo,
        )
    }

    #[tokio::test]
    async fn build_assembles_full_context() {
        let builder = build_builder(
            vec![
                text_message(1, "My cat is lovely"),
                text_message(2, "We chatted all day"),
            ],
            vec![memory(1, 0.9), memory(2, 0.4)],
            vec![],
            vec![],
        )
        .await;
        let character = build_character();
        let ctx = builder
            .build(&character, 10, 99, ContextLimits::default())
            .await
            .expect("build 应成功");

        assert_eq!(ctx.conversation.id, 10);
        assert_eq!(ctx.recent_messages.len(), 2);
        // lorebook：cat(5)、chat(2) 命中。
        assert_eq!(ctx.matching_lorebook.len(), 2);
        // 记忆已装配（按重要度降序：0.9 在前）。
        assert_eq!(ctx.memory.len(), 2);
        assert!(ctx.memory[0].importance > ctx.memory[1].importance);
        // 场景与历史后指令来自角色定义。
        assert_eq!(ctx.scenario.as_deref(), Some("咖啡馆"));
        assert_eq!(
            ctx.post_history_instructions.as_deref(),
            Some("请以角色身份回答")
        );
    }

    #[tokio::test]
    async fn build_populates_binding_and_relationship() {
        let binding = CharacterBinding {
            id: 1,
            character_id: 1,
            conversation_id: 10,
            reply_mode: crate::domain::character::ReplyMode::Natural,
            proactive_enabled: false,
            mute_schedule: None,
            behavior_overrides: serde_json::json!({}),
            context_policy: serde_json::json!({}),
            created_at: chrono::Utc::now(),
        };
        let rel = Relationship {
            character_id: 1,
            participant_id: 99,
            familiarity: 0.5,
            affection: 0.6,
            trust: 0.4,
            respect: 0.4,
            annoyance: 0.1,
            intimacy: 0.2,
            interaction_count: 5,
            last_interaction: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let builder = build_builder(vec![], vec![], vec![rel], vec![binding]).await;
        let ctx = builder
            .build(&build_character(), 10, 99, ContextLimits::default())
            .await
            .unwrap();
        assert!(ctx.binding.is_some());
        assert_eq!(ctx.binding.as_ref().unwrap().character_id, 1);
        assert_eq!(ctx.relationship.as_ref().unwrap().affection, 0.6);
    }

    #[tokio::test]
    async fn memory_limit_truncates() {
        let builder = build_builder(
            vec![],
            vec![memory(1, 0.9), memory(2, 0.4), memory(3, 0.7)],
            vec![],
            vec![],
        )
        .await;
        let limits = ContextLimits {
            memory_limit: 2,
            ..Default::default()
        };
        let ctx = builder
            .build(&build_character(), 10, 99, limits)
            .await
            .unwrap();
        assert_eq!(ctx.memory.len(), 2);
        // 应保留最重要（高重要度）的两条。
        assert!(ctx.memory.iter().all(|m| m.importance >= 0.7));
    }

    #[tokio::test]
    async fn lorebook_limit_truncates() {
        let builder = build_builder(
            vec![text_message(1, "cat and chat happen")],
            vec![],
            vec![],
            vec![],
        )
        .await;
        let limits = ContextLimits {
            lorebook_limit: 1,
            ..Default::default()
        };
        let ctx = builder
            .build(&build_character(), 10, 99, limits)
            .await
            .unwrap();
        assert_eq!(ctx.matching_lorebook.len(), 1);
    }

    #[tokio::test]
    async fn keyword_retrieval_includes_matching_memory() {
        // 一条重要度不高、但内容命中最远消息关键词的记忆，应进入上下文。
        let cat_mem = Memory {
            id: 3,
            character_id: 1,
            conversation_id: Some(10),
            memory_type: crate::domain::memory::MemoryType::Semantic,
            content: "用户喜欢猫".to_string(),
            importance: 0.3,
            created_at: chrono::Utc::now(),
            last_accessed: chrono::Utc::now(),
            metadata: serde_json::json!({}),
        };
        let builder = build_builder(
            vec![text_message(1, "猫，真可爱")],
            vec![cat_mem.clone()],
            vec![],
            vec![],
        )
        .await;
        let ctx = builder
            .build(&build_character(), 10, 99, ContextLimits::default())
            .await
            .expect("build 应成功");

        // 关键词检索应把「用户喜欢猫」这条记忆带进上下文（去重后仍存在）。
        assert!(ctx.memory.iter().any(|m| m.content == cat_mem.content));
    }

    #[test]
    fn extract_keywords_splits_and_filters() {
        // 按标点切分，保留有意义的片段。
        assert_eq!(
            extract_keywords("猫，真可爱"),
            vec!["猫".to_string(), "真可爱".to_string()]
        );
        // 单字助词（的/吗）作为独立片段时被剔除，其余保留。
        assert_eq!(extract_keywords("的 吗 猫"), vec!["猫".to_string()]);
    }

    #[tokio::test]
    async fn build_errors_when_conversation_missing() {
        let msg_repo = Arc::new(MemMessageRepo {
            messages: Mutex::new(vec![]),
        });
        let conv_repo = Arc::new(MemConvRepo { conversation: None });
        let memory_repo = Arc::new(MemMemoryRepo {
            memories: Mutex::new(vec![]),
        });
        let relationship_repo = Arc::new(MemRelationshipRepo {
            relationships: Mutex::new(vec![]),
        });
        let emotion_repo = Arc::new(MemEmotionRepo {
            states: Mutex::new(HashMap::new()),
        });
        let binding_repo = Arc::new(MemBindingRepo {
            bindings: Mutex::new(vec![]),
        });
        let builder = ContextBuilder::new(
            msg_repo,
            conv_repo,
            memory_repo,
            relationship_repo,
            emotion_repo,
            binding_repo,
        );
        let result = builder
            .build(&build_character(), 999, 1, ContextLimits::default())
            .await;
        assert!(result.is_err());
    }
}
