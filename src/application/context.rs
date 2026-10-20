//! 会话上下文组装 —— `ContextBuilder` 深化版。
//!
//! 本模块聚焦**纯数据组装**：把角色、会话、最近消息、命中的 lorebook 条目、
//! 记忆、关系、当前情绪、场景与历史后指令聚合为一个
//! [`ConversationContext`]，供上层（认知 / 行为）使用。它不负责 LLM
//! prompt 格式设计（那在认知层完成）。

use std::collections::HashSet;
use std::sync::Arc;

use crate::application::character_history::CharacterHistoryProvider;
use crate::domain::character::{Character, CharacterBinding, LorebookEntry};
use crate::domain::conversation::Conversation;
use crate::domain::emotion::Mood;
use crate::domain::memory::Memory;
use crate::domain::message::{Message, MessageContent};
use crate::domain::relationship::Relationship;
use crate::domain::repository::{
    CharacterBindingRepository, ConversationRepository, MemoryRepository, MessageRepository,
    MoodRepository, RelationshipRepository,
};
use crate::error::{DomainError, RuntimeError};
use crate::infrastructure::llm::LlmRole;

/// 上下文组装时的各类数量上限。
#[derive(Debug, Clone, Copy)]
pub struct ContextLimits {
    /// 最近消息条数。
    pub context_limit: usize,
    /// 记忆条数上限。
    pub memory_limit: usize,
    /// 命中的 lorebook 条目上限。
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

/// Phase 3: 当前 Turn 的完整信息。
///
/// 用于 LLM Messages 组装。与历史消息分离。
#[derive(Debug, Clone)]
pub struct CurrentTurn {
    /// 原始消息文本。
    pub content: String,
    /// 发送者的 Participant ID。
    pub participant_id: i64,
    /// 发送时间。
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Phase 3: LLM 对话历史中的一条消息（已映射角色）。
///
/// 用于 LlmRequest.messages。
#[derive(Debug, Clone)]
pub struct LlmTurn {
    pub role: LlmRole,
    pub content: String,
}

/// Phase 3: 整合后的 LLM 对话上下文（history + current turn）。
///
/// 用于 LlmRequest.messages。
#[derive(Debug, Clone, Default)]
pub struct LlmTurns {
    /// 历史消息（来自 CharacterHistoryProvider，已映射为 LlmRole）。
    pub history: Vec<LlmTurn>,
    /// 当前 Turn。
    pub current: Option<LlmTurn>,
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
    /// 角色当前情绪状态（Mood 标量，若有）。
    pub current_mood: Option<Mood>,
    /// 角色定义的场景设定（若有）。
    pub scenario: Option<String>,
    /// 角色定义的历史后指令（若有）。
    pub post_history_instructions: Option<String>,
    // Phase 3: 当前 Turn 信息（用于 LLM messages 组装）。
    pub current_turn: Option<CurrentTurn>,
    /// Phase 3: LLM 对话历史（已映射角色）。
    pub llm_turns: LlmTurns,
}

/// 上下文组装器。
///
/// 依赖仓储 trait（而非具体 SQLite 实现），可在测试中用内存实现替换。
#[allow(dead_code)]
pub struct ContextBuilder {
    message_repo: Arc<dyn MessageRepository>,
    character_history: Arc<dyn CharacterHistoryProvider>,
    conversation_repo: Arc<dyn ConversationRepository>,
    memory_repo: Arc<dyn MemoryRepository>,
    relationship_repo: Arc<dyn RelationshipRepository>,
    mood_repo: Arc<dyn MoodRepository>,
    binding_repo: Arc<dyn CharacterBindingRepository>,
}

impl ContextBuilder {
    /// 创建一个上下文组装器。
    pub fn new(
        message_repo: Arc<dyn MessageRepository>,
        character_history: Arc<dyn CharacterHistoryProvider>,
        conversation_repo: Arc<dyn ConversationRepository>,
        memory_repo: Arc<dyn MemoryRepository>,
        relationship_repo: Arc<dyn RelationshipRepository>,
        mood_repo: Arc<dyn MoodRepository>,
        binding_repo: Arc<dyn CharacterBindingRepository>,
    ) -> Self {
        Self {
            message_repo,
            character_history,
            conversation_repo,
            memory_repo,
            relationship_repo,
            mood_repo,
            binding_repo,
        }
    }

    /// 组装一个会话的上下文。
    ///
    /// - 加载会话与最近消息（上限 `limits.context_limit`）；
    /// - 通过 CharacterHistoryProvider 获取归属于当前角色的消息（已完成 character attribution 过滤）；
    /// - 从角色 lorebook 挑选启用且命中过滤后消息关键词的条目，按优先级降序并截断；
    /// - 加载该角色的记忆（关键词检索基于过滤后的消息）、与参与者的关系、当前情绪；
    /// - 携带角色定义的场景与历史后指令。
    ///
    /// # 参数
    /// - `current_sequence`: 如果为 `Some(seq)`，使用 sequence-based 过滤：
    ///   `conversation_sequence < seq`，确保 History 不包含当前消息。
    ///
    /// 使用显式全域用户身份组装上下文；`None` 只读取当前会话记忆且不加载关系。
    pub async fn build(
        &self,
        character: &Character,
        conversation_id: i64,
        user_id: Option<i64>,
        limits: ContextLimits,
        current_sequence: Option<i64>,
    ) -> Result<ConversationContext, RuntimeError> {
        let conversation = self
            .conversation_repo
            .find_by_id(conversation_id)
            .await?
            .ok_or(RuntimeError::Domain(DomainError::ConversationNotFound(
                conversation_id,
            )))?;

        // 通过 CharacterHistoryProvider 获取归属于当前角色的最近消息。
        // 当 current_sequence 为 Some(seq) 时，使用 sequence-based 过滤：
        // SQL: WHERE conversation_id=? AND active_character_id=? AND conversation_sequence < seq
        // 返回结果已反转，最旧的消息排在前面。
        let recent_messages = self
            .character_history
            .get_recent(
                conversation_id,
                character.id,
                limits.context_limit,
                current_sequence,
            )
            .await?;

        // 该会话中该角色的绑定（供后续 context 使用）。
        let binding = self
            .binding_repo
            .find_by_conversation_id(conversation_id)
            .await?
            .into_iter()
            .find(|b| b.character_id == character.id);

        // 世界书由角色卡规则统一选择；检索输入只使用该角色可见历史，避免跨会话泄露。
        let matching_lorebook = character
            .definition
            .select_lorebook(&[recent_messages_text(&recent_messages)])
            .into_iter()
            .take(limits.lorebook_limit)
            .collect();

        // 记忆：结合「按重要度的近期记忆」与「按最近消息关键词的结构化检索」，
        // 合并去重后截断到上限（MVP 不做向量检索）。
        let memory = {
            let by_importance = self
                .memory_repo
                .find_visible_for_user(
                    character.id,
                    conversation_id,
                    user_id,
                    limits.memory_limit as i64,
                )
                .await?;
            let keywords = extract_keywords(&recent_messages_text(&recent_messages));
            let by_keyword = self
                .memory_repo
                .search_visible_by_keywords(
                    character.id,
                    conversation_id,
                    user_id,
                    &keywords,
                    limits.memory_limit as i64,
                )
                .await?;
            merge_memories(by_importance, by_keyword, limits.memory_limit)
        };

        // 与当前参与者的关系。
        let relationship = match user_id {
            Some(user_id) => {
                self.relationship_repo
                    .find_by_user_id(character.id, user_id)
                    .await?
            }
            None => None,
        };

        // 当前情绪（Mood，Character × Conversation 范围）。
        let current_mood = self
            .mood_repo
            .find_by_character_and_conversation(character.id, conversation_id)
            .await?;

        Ok(ConversationContext {
            character: character.clone(),
            conversation,
            recent_messages,
            matching_lorebook,
            binding,
            memory,
            relationship,
            current_mood,
            scenario: character.definition.scenario.clone(),
            post_history_instructions: character.definition.post_history_instructions.clone(),
            // Phase 3: 这些字段由 ContextAssembler 在组装时填充，ContextBuilder 不设置
            current_turn: None,
            llm_turns: Default::default(),
        })
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
/// 合并「按重要度」与「按关键词」两组记忆：去重、按重要度降序、截断。
///
/// 去重逻辑：
/// - 已持久化的记忆（id != 0）按 id 去重；
/// - 同时按内容 hash 去重，避免两条已持久化但内容相同的记忆都被保留；
/// - id 为 0（未持久化）时始终追加，但也会与已有内容去重。
fn merge_memories(
    by_importance: Vec<Memory>,
    by_keyword: Vec<Memory>,
    limit: usize,
) -> Vec<Memory> {
    let mut merged: Vec<Memory> = Vec::with_capacity(by_importance.len() + by_keyword.len());
    let mut seen_ids: HashSet<i64> = HashSet::new();
    let mut seen_content_hashes: HashSet<u64> = HashSet::new();

    for m in by_importance.into_iter().chain(by_keyword) {
        // 计算内容 hash（用于检测内容重复的记忆）。
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        m.content.hash(&mut hasher);
        let content_hash = hasher.finish();

        // 去重：id 为 0 时按内容 hash 去重；id != 0 时按 id 去重（同时检查内容 hash 避免重复内容）。
        let id_ok = m.id == 0 || !seen_ids.contains(&m.id);
        let content_ok = !seen_content_hashes.contains(&content_hash);

        if id_ok && content_ok {
            seen_ids.insert(m.id);
            seen_content_hashes.insert(content_hash);
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
        async fn find_recent_attributed(
            &self,
            _conversation_id: i64,
            character_id: i64,
            limit: i64,
        ) -> Result<Vec<Message>, RepositoryError> {
            let mut filtered: Vec<Message> = self
                .messages
                .lock()
                .unwrap()
                .iter()
                .filter(|m| m.active_character_id == Some(character_id))
                .cloned()
                .collect();
            // 模拟 SQL: ORDER BY timestamp DESC, id DESC
            filtered.sort_by(|a, b| {
                let time_cmp = b.timestamp.cmp(&a.timestamp); // DESC
                if time_cmp == std::cmp::Ordering::Equal {
                    b.id.cmp(&a.id) // DESC
                } else {
                    time_cmp
                }
            });
            filtered.truncate(limit as usize);
            // 反转顺序，使最旧的消息排在前面（与 Repository 契约一致）
            filtered.reverse();
            Ok(filtered)
        }
        async fn insert(&self, _m: &Message) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn latest_message_time(
            &self,
            _conversation_id: i64,
        ) -> Result<Option<chrono::DateTime<chrono::Utc>>, RepositoryError> {
            Ok(None)
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
        async fn allocate_sequence(&self, _conversation_id: i64) -> Result<i64, RepositoryError> {
            Ok(1) // Mock 实现
        }
    }

    struct MemMemoryRepo {
        memories: Mutex<Vec<Memory>>,
    }
    #[async_trait]
    impl MemoryRepository for MemMemoryRepo {
        async fn find_by_id(&self, id: i64) -> Result<Option<Memory>, RepositoryError> {
            Ok(self
                .memories
                .lock()
                .unwrap()
                .iter()
                .find(|m| m.id == id)
                .cloned())
        }
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
        async fn find_all(&self, _limit: i64) -> Result<Vec<Memory>, RepositoryError> {
            Ok(Vec::new())
        }
        async fn find_by_conversation_id(
            &self,
            _conversation_id: i64,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(Vec::new())
        }
        async fn find_filtered(
            &self,
            _character_id: Option<i64>,
            _conversation_id: Option<i64>,
            _memory_type: Option<crate::domain::memory::MemoryType>,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(Vec::new())
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
                .find(|r| r.character_id == character_id && r.user_id == participant_id)
                .cloned())
        }
        async fn find_by_character_id(
            &self,
            _character_id: i64,
        ) -> Result<Vec<Relationship>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_by_user_id(
            &self,
            character_id: i64,
            user_id: i64,
        ) -> Result<Option<Relationship>, RepositoryError> {
            Ok(self
                .relationships
                .lock()
                .unwrap()
                .iter()
                .find(|relationship| {
                    relationship.character_id == character_id && relationship.user_id == user_id
                })
                .cloned())
        }
        async fn upsert(&self, _r: &Relationship) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemMoodRepo {
        moods: Mutex<HashMap<(i64, i64), Mood>>,
    }
    #[async_trait]
    impl MoodRepository for MemMoodRepo {
        async fn find_by_character_and_conversation(
            &self,
            character_id: i64,
            conversation_id: i64,
        ) -> Result<Option<Mood>, RepositoryError> {
            Ok(self
                .moods
                .lock()
                .unwrap()
                .get(&(character_id, conversation_id))
                .cloned())
        }
        async fn upsert(
            &self,
            character_id: i64,
            conversation_id: i64,
            state: &Mood,
        ) -> Result<(), RepositoryError> {
            self.moods
                .lock()
                .unwrap()
                .insert((character_id, conversation_id), state.clone());
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
            active_character_id: None,
            // C2-3 字段
            platform_message_id: None,
            conversation_sequence: None,
            occurred_at: None,
            ingested_at: None,
        }
    }

    /// 构造一条带有 character attribution 的消息（Phase 2 行为）。
    fn attributed_message(id: i64, content: &str, character_id: i64) -> Message {
        let mut msg = text_message(id, content);
        msg.active_character_id = Some(character_id);
        msg
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
            user_id: None,
            conversation_id: Some(10),
            memory_type: crate::domain::memory::MemoryType::Episodic,
            content: format!("记忆-{id}"),
            importance,
            created_at: chrono::Utc::now(),
            last_accessed: chrono::Utc::now(),
            embedding: None,
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
            enabled: true,
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
        let mood_repo = Arc::new(MemMoodRepo {
            moods: Mutex::new(HashMap::new()),
        });
        let binding_repo = Arc::new(MemBindingRepo {
            bindings: Mutex::new(bindings),
        });
        let character_history = Arc::new(
            crate::application::character_history::RepositoryCharacterHistoryProvider::new(
                msg_repo.clone(),
            ),
        );
        ContextBuilder::new(
            msg_repo,
            character_history,
            conv_repo,
            memory_repo,
            relationship_repo,
            mood_repo,
            binding_repo,
        )
    }

    #[tokio::test]
    async fn build_assembles_full_context() {
        // Phase 2: 消息需要设置 active_character_id 才能被 CharacterHistoryProvider 返回
        let builder = build_builder(
            vec![
                attributed_message(1, "My cat is lovely", 1),
                attributed_message(2, "We chatted all day", 1),
            ],
            vec![memory(1, 0.9), memory(2, 0.4)],
            vec![],
            vec![],
        )
        .await;
        let character = build_character();
        let ctx = builder
            .build(&character, 10, Some(99), ContextLimits::default(), None)
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
            switched_at: None,
            cross_reply_enabled: false,
            response_rate_limit: Default::default(),
            created_at: chrono::Utc::now(),
        };
        let rel = Relationship {
            character_id: 1,
            user_id: 99,
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
            .build(
                &build_character(),
                10,
                Some(99),
                ContextLimits::default(),
                None,
            )
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
            .build(&build_character(), 10, Some(99), limits, None)
            .await
            .unwrap();
        assert_eq!(ctx.memory.len(), 2);
        // 应保留最重要（高重要度）的两条。
        assert!(ctx.memory.iter().all(|m| m.importance >= 0.7));
    }

    #[tokio::test]
    async fn lorebook_limit_truncates() {
        // Phase 2: 消息需要设置 active_character_id 才能被 CharacterHistoryProvider 返回
        let builder = build_builder(
            vec![attributed_message(1, "cat and chat happen", 1)],
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
            .build(&build_character(), 10, Some(99), limits, None)
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
            user_id: None,
            conversation_id: Some(10),
            memory_type: crate::domain::memory::MemoryType::Semantic,
            content: "用户喜欢猫".to_string(),
            importance: 0.3,
            created_at: chrono::Utc::now(),
            last_accessed: chrono::Utc::now(),
            embedding: None,
            metadata: serde_json::json!({}),
        };
        let builder = build_builder(
            vec![attributed_message(1, "猫，真可爱", 1)],
            vec![cat_mem.clone()],
            vec![],
            vec![],
        )
        .await;
        let ctx = builder
            .build(
                &build_character(),
                10,
                Some(99),
                ContextLimits::default(),
                None,
            )
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
        let mood_repo = Arc::new(MemMoodRepo {
            moods: Mutex::new(HashMap::new()),
        });
        let binding_repo = Arc::new(MemBindingRepo {
            bindings: Mutex::new(vec![]),
        });
        let builder = ContextBuilder::new(
            msg_repo.clone(),
            Arc::new(
                crate::application::character_history::RepositoryCharacterHistoryProvider::new(
                    msg_repo.clone(),
                ),
            ),
            conv_repo,
            memory_repo,
            relationship_repo,
            mood_repo,
            binding_repo,
        );
        let result = builder
            .build(
                &build_character(),
                999,
                Some(1),
                ContextLimits::default(),
                None,
            )
            .await;
        assert!(result.is_err());
    }

    /// 构造一个会话 10 / 角色 1 的绑定，switched_at 可指定。
    fn binding_with_switched_at(
        switched_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> CharacterBinding {
        CharacterBinding {
            id: 1,
            character_id: 1,
            conversation_id: 10,
            reply_mode: crate::domain::character::ReplyMode::Natural,
            proactive_enabled: false,
            mute_schedule: None,
            behavior_overrides: serde_json::json!({}),
            context_policy: serde_json::json!({}),
            switched_at,
            cross_reply_enabled: false,
            response_rate_limit: Default::default(),
            created_at: chrono::Utc::now(),
        }
    }

    /// 构造一条指定时间戳的消息（其余字段沿用默认）。
    fn text_message_at(
        id: i64,
        content: &str,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Message {
        let mut m = text_message(id, content);
        m.timestamp = timestamp;
        m
    }

    /// 构造一条带有 character attribution 的消息（Phase 2 行为）。
    fn attributed_message_at(
        id: i64,
        content: &str,
        timestamp: chrono::DateTime<chrono::Utc>,
        character_id: i64,
    ) -> Message {
        let mut m = text_message_at(id, content, timestamp);
        m.active_character_id = Some(character_id);
        m
    }

    fn base_time() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    #[tokio::test]
    async fn build_filters_messages_before_switched_at() {
        // Phase 2: switched_at 不再用于过滤，改为 active_character_id 过滤
        let base = base_time();
        let t1 = base;
        let t2 = base + chrono::Duration::minutes(1);
        let t3 = base + chrono::Duration::minutes(2);
        let messages = vec![
            // 角色 1 的消息
            attributed_message_at(1, "第一条", t1, 1),
            attributed_message_at(2, "第二条", t2, 1),
            attributed_message_at(3, "第三条", t3, 1),
            // 角色 2 的消息（不应被 character 1 看到）
            attributed_message_at(4, "第四条", t1, 2),
        ];

        // 绑定 switched_at 不再影响过滤
        let binding = binding_with_switched_at(Some(t2));
        let builder = build_builder(messages, vec![], vec![], vec![binding]).await;
        let ctx = builder
            .build(
                &build_character(),
                10,
                Some(99),
                ContextLimits::default(),
                None,
            )
            .await
            .expect("build 应成功");

        // Phase 2: 只返回 active_character_id = 1 的消息（3条），顺序最旧在前
        assert_eq!(ctx.recent_messages.len(), 3);
        assert_eq!(ctx.recent_messages[0].id, 1);
        assert_eq!(ctx.recent_messages[1].id, 2);
        assert_eq!(ctx.recent_messages[2].id, 3);
    }

    #[tokio::test]
    async fn build_keeps_all_messages_without_switched_at() {
        // Phase 2: switched_at 不再用于过滤，所有有 attribution 的消息都会被返回
        let base = base_time();
        let messages = vec![
            attributed_message_at(1, "第一条", base, 1),
            attributed_message_at(2, "第二条", base + chrono::Duration::minutes(1), 1),
            attributed_message_at(3, "第三条", base + chrono::Duration::minutes(2), 1),
        ];

        // switched_at = None 不再影响过滤逻辑
        let binding = binding_with_switched_at(None);
        let builder = build_builder(messages, vec![], vec![], vec![binding]).await;
        let ctx = builder
            .build(
                &build_character(),
                10,
                Some(99),
                ContextLimits::default(),
                None,
            )
            .await
            .expect("build 应成功");

        // Phase 2: 所有 active_character_id = 1 的消息都被返回
        assert_eq!(ctx.recent_messages.len(), 3);
    }

    #[tokio::test]
    async fn build_does_not_trigger_lorebook_from_filtered_messages() {
        // Phase 2: switched_at 不再用于过滤，改为 active_character_id 过滤
        // 只有 active_character_id = 1 的消息会被返回，lorebook 匹配也只基于这些消息
        let base = base_time();
        let t1 = base;
        let t2 = base + chrono::Duration::minutes(1);
        // t1 含 lorebook 关键词「禁词」，属于角色 2，不应被角色 1 看到
        let messages = vec![
            attributed_message_at(1, "这是禁词消息", t1, 2), // 角色 2 的消息，不应触发角色 1 的 lorebook
            attributed_message_at(2, "第二条", t2, 1),       // 角色 1 的消息
            attributed_message_at(3, "第三条", t2 + chrono::Duration::minutes(1), 1), // 角色 1 的消息
        ];
        let mut character = build_character();
        character
            .definition
            .lorebook
            .push(lorebook_entry(&["禁词"], 3, true));

        let binding = binding_with_switched_at(Some(t2));
        let builder = build_builder(messages, vec![], vec![], vec![binding]).await;
        let ctx = builder
            .build(&character, 10, Some(99), ContextLimits::default(), None)
            .await
            .expect("build 应成功");

        // Phase 2: 只有 active_character_id = 1 的消息被返回，不包含「禁词」
        // 所以 lorebook 不应被触发
        assert!(
            ctx.matching_lorebook.is_empty(),
            "角色 2 的消息（包含禁词）不应被角色 1 看到，不应触发 lorebook"
        );
    }
}
