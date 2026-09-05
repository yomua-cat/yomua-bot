//! 会话上下文组装 —— `ContextBuilder` 深化版。
//!
//! 本模块聚焦**纯数据组装**：把角色、会话、最近消息、命中的 lorebook 条目、
//! 记忆、关系、当前情绪、场景与历史后指令聚合为一个
//! [`ConversationContext`]，供上层（认知 / 行为）使用。它不负责 LLM
//! prompt 格式设计（那在认知层完成）。

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::application::llm_scheduler::EmbeddingScheduler;
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

/// Lorebook 检索的匹配类型。
#[derive(Debug, Clone, PartialEq)]
pub enum MatchType {
    /// 向量相似度匹配。
    Vector,
    /// 关键词匹配。
    Keyword,
    /// 同时被向量和关键词匹配。
    Both,
}

/// Lorebook 匹配结果（包含匹配元数据）。
#[derive(Debug, Clone)]
pub struct LorebookMatch {
    /// Lorebook 条目内容。
    pub entry: String,
    /// 优先级。
    pub priority: i64,
    /// 命中的关键词（若是关键词匹配）。
    pub key: String,
    /// 综合评分（向量分数 * 权重 + 关键词分数 * 权重）。
    pub score: f32,
    /// 匹配类型。
    pub match_type: MatchType,
}

/// Lorebook 检索的限制参数。
#[derive(Debug, Clone, Copy)]
pub struct LorebookLimits {
    /// 向量相似度阈值（默认 0.7）。
    pub vector_threshold: f32,
    /// 向量权重（默认 0.6）。
    pub vector_weight: f32,
    /// 关键词权重（默认 0.4）。
    pub keyword_weight: f32,
    /// 最大返回条数。
    pub limit: i64,
}

impl Default for LorebookLimits {
    fn default() -> Self {
        Self {
            vector_threshold: 0.7,
            vector_weight: 0.6,
            keyword_weight: 0.4,
            limit: 5,
        }
    }
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
    /// 向量嵌入调度器（可选，无时退化为纯关键词匹配）。
    embedding_scheduler: Option<Arc<dyn EmbeddingScheduler>>,
    /// Lorebook 检索的限制参数。
    lorebook_limits: LorebookLimits,
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
            embedding_scheduler: None,
            lorebook_limits: LorebookLimits::default(),
        }
    }

    /// 设置向量嵌入调度器。
    pub fn with_embedding_scheduler(mut self, s: Arc<dyn EmbeddingScheduler>) -> Self {
        self.embedding_scheduler = Some(s);
        self
    }

    /// 设置 Lorebook 检索的限制参数。
    pub fn with_lorebook_limits(mut self, limits: LorebookLimits) -> Self {
        self.lorebook_limits = limits;
        self
    }

    /// 组装一个会话的上下文。
    ///
    /// - 加载会话与最近消息（上限 `limits.context_limit`）；
    /// - 加载该会话中该角色的绑定，并按换角色生效时间（switched_at）过滤消息
    ///   （硬性约束 A：换角色后的上下文只包含切换之后的消息）；
    /// - 从角色 lorebook 挑选启用且命中过滤后消息关键词的条目，按优先级降序并截断；
    /// - 加载该角色的记忆（关键词检索基于过滤后的消息）、与参与者的关系、当前情绪；
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

        // 该会话中该角色的绑定（提前加载，供硬性约束 A 过滤使用）。
        let binding = self
            .binding_repo
            .find_by_conversation_id(conversation_id)
            .await?
            .into_iter()
            .find(|b| b.character_id == character.id);

        // 硬性约束 A：按换角色生效时间过滤。switched_at 为 None（未换过角色）不过滤，
        // 保持既有行为不变（回归保障）。
        let recent_messages = filter_messages_by_switched_at(
            recent_messages,
            binding.as_ref().and_then(|b| b.switched_at),
        );

        // Lorebook 匹配：优先使用混合检索（向量 + 关键词），无 embedding 时退化为纯关键词。
        let matching_lorebook = {
            let prompt_text = recent_messages_text(&recent_messages);
            if self.embedding_scheduler.is_some() {
                // 混合检索：向量相似度 ∪ 关键词命中，取并集后重排
                match self
                    .match_lorebook_hybrid(character, &prompt_text, self.lorebook_limits)
                    .await
                {
                    Ok(matches) => matches
                        .into_iter()
                        .map(|m| LorebookEntry {
                            keywords: vec![m.key],
                            content: m.entry,
                            enabled: true,
                            priority: m.priority as i32,
                        })
                        .collect(),
                    Err(e) => {
                        tracing::warn!("混合检索失败，退化为关键词匹配: {}", e);
                        self.match_lorebook_by_keywords(character, &prompt_text)
                            .into_iter()
                            .map(|(e, _)| e)
                            .collect()
                    }
                }
            } else {
                // 纯关键词匹配（既有行为不变）
                self.match_lorebook_by_keywords(character, &prompt_text)
                    .into_iter()
                    .map(|(e, _)| e)
                    .collect()
            }
        };

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

    /// 从角色 lorebook 中选出匹配文本的启用条目及其命中的关键词。
    ///
    /// 返回 (LorebookEntry, 第一个匹配的关键词)。
    fn match_lorebook_by_keywords(
        &self,
        character: &Character,
        prompt: &str,
    ) -> Vec<(LorebookEntry, String)> {
        let haystack = prompt.to_lowercase();

        let mut matched: Vec<(LorebookEntry, String)> = character
            .definition
            .lorebook
            .iter()
            .filter_map(|entry| {
                if !entry.enabled {
                    return None;
                }
                entry
                    .keywords
                    .iter()
                    .find(|kw| !kw.trim().is_empty() && haystack.contains(&kw.to_lowercase()))
                    .map(|kw| (entry.clone(), kw.clone()))
            })
            .collect();

        // 按 priority 降序（越重要的越靠前）。
        matched.sort_by_key(|(e, _)| std::cmp::Reverse(e.priority));
        matched
    }

    /// 混合检索 lorebook：向量相似度 ∪ 关键词命中，取并集后重排。
    ///
    /// 向量检索使用 EmbeddingScheduler 生成查询向量，从 semantic_memories 表匹配。
    /// 当 embedding_scheduler 不可用时退化为纯关键词匹配。
    async fn match_lorebook_hybrid(
        &self,
        character: &Character,
        query_text: &str,
        limits: LorebookLimits,
    ) -> Result<Vec<LorebookMatch>, RuntimeError> {
        // 1. 向量检索（如有 embedding_scheduler）
        let vector_matches = if let Some(ref scheduler) = self.embedding_scheduler {
            let query_embedding = scheduler
                .submit_embedding(vec![query_text.to_string()])
                .await?;
            let query_vec = &query_embedding[0];

            let results = self
                .memory_repo
                .search_by_embedding(character.id, query_vec, Some("lorebook"), limits.limit)
                .await?;

            // 过滤低于阈值的向量匹配，并标记为 Vector 类型
            results
                .into_iter()
                .filter(|r| r.score >= limits.vector_threshold)
                .map(|r| {
                    (
                        r.memory.content,
                        r.memory.importance as i64,
                        r.score,
                        MatchType::Vector,
                    )
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        // 2. 关键词检索（复用现有逻辑）
        let keyword_matches = self.match_lorebook_by_keywords(character, query_text);
        let keyword_entries: Vec<(String, i64, String)> = keyword_matches
            .into_iter()
            .map(|(e, kw)| (e.content, e.priority as i64, kw))
            .collect();

        // 3. 合并去重（按 entry 内容）
        let mut all_matches: Vec<LorebookMatch> = Vec::new();
        let mut seen_entries: HashSet<String> = HashSet::new();

        for (content, priority, score, match_type) in vector_matches {
            if !seen_entries.contains(&content) {
                seen_entries.insert(content.clone());
                all_matches.push(LorebookMatch {
                    entry: content,
                    priority,
                    key: String::new(),
                    score,
                    match_type,
                });
            }
        }

        for (content, priority, key) in keyword_entries {
            if !seen_entries.contains(&content) {
                seen_entries.insert(content.clone());
                all_matches.push(LorebookMatch {
                    entry: content,
                    priority,
                    key,
                    score: 1.0,
                    match_type: MatchType::Keyword,
                });
            }
        }

        // 4. 重排：score = vector_score * vector_weight + keyword_score * keyword_weight
        // keyword_score = 1.0（命中关键词）
        for m in &mut all_matches {
            m.score = match m.match_type {
                MatchType::Vector => m.score * limits.vector_weight,
                MatchType::Keyword => 1.0 * limits.keyword_weight,
                MatchType::Both => m.score * limits.vector_weight + limits.keyword_weight,
            };
        }
        all_matches.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());

        // 5. 转换为 LorebookMatch 并截断
        let final_matches: Vec<LorebookMatch> = all_matches
            .into_iter()
            .take(limits.limit as usize)
            .collect();

        Ok(final_matches)
    }
}

/// 按换角色生效时间过滤消息：只保留 `switched_at` 之后（含）的消息。
///
/// `switched_at` 为 None（该会话从未换过角色）时不过滤，保持既有行为不变。
/// 被过滤的消息同时不进入后续的 lorebook 匹配与关键词检索（顺序上先过滤再组装）。
fn filter_messages_by_switched_at(
    messages: Vec<Message>,
    switched_at: Option<DateTime<Utc>>,
) -> Vec<Message> {
    match switched_at {
        None => messages,
        Some(t) => messages.into_iter().filter(|m| m.timestamp >= t).collect(),
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
            switched_at: None,
            cross_reply_enabled: false,
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
            embedding: None,
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

    /// 构造一个会话 10 / 角色 1 的绑定，switched_at 可指定。
    fn binding_with_switched_at(switched_at: Option<chrono::DateTime<Utc>>) -> CharacterBinding {
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
            created_at: chrono::Utc::now(),
        }
    }

    /// 构造一条指定时间戳的消息（其余字段沿用默认）。
    fn text_message_at(id: i64, content: &str, timestamp: chrono::DateTime<Utc>) -> Message {
        let mut m = text_message(id, content);
        m.timestamp = timestamp;
        m
    }

    fn base_time() -> chrono::DateTime<Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[tokio::test]
    async fn build_filters_messages_before_switched_at() {
        let base = base_time();
        let t1 = base;
        let t2 = base + chrono::Duration::minutes(1);
        let t3 = base + chrono::Duration::minutes(2);
        let messages = vec![
            text_message_at(1, "第一条", t1),
            text_message_at(2, "第二条", t2),
            text_message_at(3, "第三条", t3),
        ];

        // 绑定 switched_at = t2（t2 时换过角色）。
        let binding = binding_with_switched_at(Some(t2));
        let builder = build_builder(messages, vec![], vec![], vec![binding]).await;
        let ctx = builder
            .build(&build_character(), 10, 99, ContextLimits::default())
            .await
            .expect("build 应成功");

        // 只保留 t2、t3 两条，且顺序保持最旧在前。
        assert_eq!(ctx.recent_messages.len(), 2);
        assert_eq!(ctx.recent_messages[0].id, 2);
        assert_eq!(ctx.recent_messages[1].id, 3);
        assert!(
            ctx.recent_messages.iter().all(|m| m.id != 1),
            "t1 消息（换角色之前）应被过滤"
        );
    }

    #[tokio::test]
    async fn build_keeps_all_messages_without_switched_at() {
        let base = base_time();
        let messages = vec![
            text_message_at(1, "第一条", base),
            text_message_at(2, "第二条", base + chrono::Duration::minutes(1)),
            text_message_at(3, "第三条", base + chrono::Duration::minutes(2)),
        ];

        // 绑定 switched_at = None（从未换过角色）→ 不过滤，保持既有行为。
        let binding = binding_with_switched_at(None);
        let builder = build_builder(messages, vec![], vec![], vec![binding]).await;
        let ctx = builder
            .build(&build_character(), 10, 99, ContextLimits::default())
            .await
            .expect("build 应成功");

        assert_eq!(ctx.recent_messages.len(), 3);
    }

    #[test]
    fn filter_messages_by_switched_at_unit() {
        let base = base_time();
        let t1 = base;
        let t2 = base + chrono::Duration::minutes(1);
        let t3 = base + chrono::Duration::minutes(2);
        let messages = vec![
            text_message_at(1, "第一条", t1),
            text_message_at(2, "第二条", t2),
            text_message_at(3, "第三条", t3),
        ];

        // None 不过滤。
        let kept = filter_messages_by_switched_at(messages.clone(), None);
        assert_eq!(kept.len(), 3);

        // Some(t2)：t2 之前（不含）被过滤，边界 == t2 保留。
        let kept = filter_messages_by_switched_at(messages.clone(), Some(t2));
        assert_eq!(kept.len(), 2);
        assert!(kept.iter().all(|m| m.timestamp >= t2));

        // Some(t3)：只保留 t3 本身。
        let kept = filter_messages_by_switched_at(messages, Some(t3));
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].id, 3);
    }

    #[tokio::test]
    async fn build_does_not_trigger_lorebook_from_filtered_messages() {
        let base = base_time();
        let t1 = base;
        let t2 = base + chrono::Duration::minutes(1);
        // t1 含 lorebook 关键词「禁词」，但在换角色之前被过滤。
        let messages = vec![
            text_message_at(1, "这是禁词消息", t1),
            text_message_at(2, "第二条", t2),
            text_message_at(3, "第三条", t2 + chrono::Duration::minutes(1)),
        ];
        let mut character = build_character();
        character
            .definition
            .lorebook
            .push(lorebook_entry(&["禁词"], 3, true));

        let binding = binding_with_switched_at(Some(t2));
        let builder = build_builder(messages, vec![], vec![], vec![binding]).await;
        let ctx = builder
            .build(&character, 10, 99, ContextLimits::default())
            .await
            .expect("build 应成功");

        // 被过滤的消息不得触发 lorebook 匹配。
        assert!(
            ctx.matching_lorebook.is_empty(),
            "换角色之前的消息不应触发 lorebook"
        );
    }
}
