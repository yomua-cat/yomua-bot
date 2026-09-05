//! 认知层 —— 通过 LLM 调度器协调 LLM 调用。
//!
//! 认知 ≠ 行为 ≠ LLM。LLM 只是认知的一种能力。
//! 本层持有可选的 LLM 调度器（`None` 表示未启用 LLM，此时调用方可选择
//! 确定性回复）。所有 LLM 调用统一经由
//! [`crate::application::llm_scheduler::LlmScheduler`]，绝不直接触碰
//! [`crate::infrastructure::llm::LlmProvider`]。

use std::sync::Arc;

use crate::application::context::{ContextBuilder, ContextLimits, ConversationContext};
use crate::application::llm_scheduler::LlmScheduler;
use crate::domain::character::Character;
use crate::error::RuntimeError;
use crate::infrastructure::llm::{LlmMessage, LlmRequest, LlmResponse, LlmRole};

/// 认知层 —— 构建上下文并（在启用时）调用 LLM。
pub struct CognitionLayer {
    /// 可选的 LLM 调度器；`None` 表示未启用 LLM。
    scheduler: Option<Arc<dyn LlmScheduler>>,
    context_builder: Arc<ContextBuilder>,
}

impl CognitionLayer {
    /// 创建一个认知层。
    ///
    /// - `scheduler`：启用 LLM 时为 `Some(...)`；未启用时为 `None`。
    /// - `context_builder`：上下文组装器。
    pub fn new(
        scheduler: Option<Arc<dyn LlmScheduler>>,
        context_builder: Arc<ContextBuilder>,
    ) -> Self {
        Self {
            scheduler,
            context_builder,
        }
    }

    /// 认知层是否可用（即有 LLM 调度器）。
    pub fn enabled(&self) -> bool {
        self.scheduler.is_some()
    }

    /// 生成一条回复。
    ///
    /// 若 LLM 未启用（`scheduler` 为 `None`），返回 `Ok(None)`，调用方据此
    /// 走确定性回复；否则组装上下文、渲染系统提示词、经调度器提交后返回内容。
    pub async fn generate(
        &self,
        character: &Character,
        conversation_id: i64,
        participant_id: i64,
        user_message: &str,
        is_mentioned: bool,
        limits: ContextLimits,
    ) -> Result<Option<String>, RuntimeError> {
        let Some(scheduler) = &self.scheduler else {
            return Ok(None);
        };

        let context = self
            .context_builder
            .build(character, conversation_id, participant_id, limits)
            .await?;

        let system = Self::render_system_prompt(&context);
        let request = LlmRequest {
            system: Some(system),
            messages: vec![LlmMessage {
                role: LlmRole::User,
                content: user_message.to_string(),
            }],
            model: None, // 让 Provider 选择默认模型
            temperature: Some(0.8),
            max_tokens: None,
            // 被 @ 时实时优先级，否则普通优先级。
            priority: if is_mentioned { 0 } else { 1 },
            metadata: serde_json::json!({
                "character_id": character.id,
                "conversation_id": conversation_id,
                "participant_id": participant_id,
            }),
        };

        let response = scheduler.submit(request).await?;
        Ok(Some(response.content))
    }

    /// 直接提交一组消息给 LLM（插件 API 等无角色上下文的调用方使用）。
    ///
    /// 与 `generate` 一致：LLM 未启用（`scheduler` 为 `None`）时返回
    /// `Ok(None)`，调用方据此返回确定性结果。`system` 直接进入
    /// [`LlmRequest::system`] 字段，不做上下文渲染。
    pub async fn chat(
        &self,
        system: Option<String>,
        messages: Vec<LlmMessage>,
        priority: u8,
    ) -> Result<Option<LlmResponse>, RuntimeError> {
        let Some(scheduler) = &self.scheduler else {
            return Ok(None);
        };

        let request = LlmRequest {
            system,
            messages,
            model: None,            // 让 Provider 选择默认模型
            temperature: Some(0.8), // 与 generate 保持一致
            max_tokens: None,
            priority,
            metadata: serde_json::json!({}), // 无角色上下文，不带额外元数据
        };

        let response = scheduler.submit(request).await?;
        Ok(Some(response))
    }

    /// 渲染系统提示词 —— 纯数据 → 提示词的组装，不访问任何外部依赖。
    ///
    /// 把角色定义（人格、场景、说话风格、历史后指令）、命中的 lorebook、
    /// 关系状态、当前情绪与相关记忆拼装为系统提示。
    pub fn render_system_prompt(ctx: &ConversationContext) -> String {
        let mut parts: Vec<String> = Vec::new();

        let def = &ctx.character.definition;
        // 角色名与自我介绍。
        parts.push(format!("你是{}。", def.name));
        if let Some(desc) = &def.description {
            parts.push(desc.clone());
        }
        if let Some(personality) = &def.personality {
            parts.push(format!("性格：{personality}"));
        }
        if let Some(style) = &def.style {
            parts.push(format!("说话风格：{style}"));
        }
        if let Some(scenario) = &ctx.scenario {
            parts.push(format!("当前场景：{scenario}"));
        }

        // 命中的 lorebook 条目。
        if !ctx.matching_lorebook.is_empty() {
            let lore = ctx
                .matching_lorebook
                .iter()
                .map(|e| e.content.clone())
                .collect::<Vec<_>>()
                .join("\n");
            parts.push(format!("相关设定：\n{lore}"));
        }

        // 关系状态。
        if let Some(rel) = &ctx.relationship {
            parts.push(format!(
                "关系：熟悉度 {:.2}、好感 {:.2}、信任 {:.2}、厌烦 {:.2}、亲密 {:.2}",
                rel.familiarity, rel.affection, rel.trust, rel.annoyance, rel.intimacy
            ));
        }

        // 当前情绪。
        if let Some(emotion) = &ctx.current_emotion {
            parts.push(format!(
                "当前情绪：开心 {:.2}、生气 {:.2}、悲伤 {:.2}、害怕 {:.2}、好感 {:.2}、压力 {:.2}、精力 {:.2}",
                emotion.happiness,
                emotion.anger,
                emotion.sadness,
                emotion.fear,
                emotion.affection,
                emotion.stress,
                emotion.energy
            ));
        }

        // 相关记忆。
        if !ctx.memory.is_empty() {
            let mem = ctx
                .memory
                .iter()
                .map(|m| format!("- {}", m.content))
                .collect::<Vec<_>>()
                .join("\n");
            parts.push(format!("相关记忆：\n{mem}"));
        }

        // 历史后指令（如"以角色身份回答"）。
        if let Some(post) = &ctx.post_history_instructions {
            parts.push(post.clone());
        }

        parts.join("\n\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::character::{Character, CharacterDefinition, CharacterState};
    use crate::domain::conversation::ConversationType;

    fn build_character() -> Character {
        Character {
            id: 1,
            definition: CharacterDefinition {
                name: "Alice".to_string(),
                description: Some("一个温柔的咖啡师".to_string()),
                personality: Some("开朗".to_string()),
                scenario: Some("清晨的咖啡馆".to_string()),
                style: Some("温柔细语".to_string()),
                background: None,
                greetings: vec![],
                example_messages: vec![],
                system_prompt: None,
                post_history_instructions: Some("请以 Alice 的身份回答".to_string()),
                lorebook: vec![],
                metadata: serde_json::json!({}),
            },
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    fn minimal_ctx() -> ConversationContext {
        let character = build_character();
        ConversationContext {
            character,
            conversation: crate::domain::conversation::Conversation {
                id: 1,
                conversation_type: ConversationType::Private,
                external_id: "u1".to_string(),
                name: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
            recent_messages: vec![],
            matching_lorebook: vec![],
            binding: None,
            memory: vec![],
            relationship: None,
            current_emotion: None,
            scenario: Some("清晨的咖啡馆".to_string()),
            post_history_instructions: Some("请以 Alice 的身份回答".to_string()),
        }
    }

    #[test]
    fn render_system_prompt_includes_identity() {
        let ctx = minimal_ctx();
        let prompt = CognitionLayer::render_system_prompt(&ctx);
        assert!(prompt.contains("你是Alice"));
        assert!(prompt.contains("一个温柔的咖啡师"));
        assert!(prompt.contains("清晨的咖啡馆"));
        assert!(prompt.contains("请以 Alice 的身份回答"));
    }

    #[test]
    fn render_system_prompt_includes_emotion_and_relationship() {
        let mut ctx = minimal_ctx();
        ctx.current_emotion = Some(crate::domain::emotion::EmotionState::default());
        ctx.relationship = Some(crate::domain::relationship::Relationship::new(1, 99));
        let prompt = CognitionLayer::render_system_prompt(&ctx);
        assert!(prompt.contains("当前情绪"));
        assert!(prompt.contains("关系"));
    }

    // -----------------------------------------------------------------------
    // chat —— 插件 API 直连 LLM 的无上下文通道
    // -----------------------------------------------------------------------

    use crate::domain::conversation::Conversation;
    use crate::domain::message::Message;
    use crate::domain::repository::{
        CharacterBindingRepository, ConversationRepository, EmotionStateRepository,
        MemoryRepository, MessageRepository, RelationshipRepository,
    };
    use crate::error::RepositoryError;
    use crate::infrastructure::llm::{LlmResponse, TokenUsage};
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// 记录提交请求并返回固定响应的假调度器。
    struct FakeScheduler {
        submitted: Mutex<Vec<LlmRequest>>,
    }
    impl FakeScheduler {
        fn new() -> Self {
            Self {
                submitted: Mutex::new(Vec::new()),
            }
        }
        fn requests(&self) -> Vec<LlmRequest> {
            self.submitted.lock().unwrap().clone()
        }
    }
    #[async_trait]
    impl LlmScheduler for FakeScheduler {
        async fn submit(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
            self.submitted.lock().unwrap().push(request);
            Ok(LlmResponse {
                content: "插件回复".to_string(),
                model: "fake".to_string(),
                usage: TokenUsage {
                    prompt_tokens: 1,
                    completion_tokens: 2,
                    total_tokens: 3,
                },
                truncated: false,
            })
        }
    }

    struct MemMessageRepo;
    #[async_trait]
    impl MessageRepository for MemMessageRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Message>, RepositoryError> {
            Ok(None)
        }
        async fn find_recent(
            &self,
            _conversation_id: i64,
            _limit: i64,
        ) -> Result<Vec<Message>, RepositoryError> {
            Ok(vec![])
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

    struct MemConvRepo;
    #[async_trait]
    impl ConversationRepository for MemConvRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Conversation>, RepositoryError> {
            Ok(None)
        }
        async fn find_by_external_id(
            &self,
            _id: &str,
        ) -> Result<Option<Conversation>, RepositoryError> {
            Ok(None)
        }
        async fn find_all(&self) -> Result<Vec<Conversation>, RepositoryError> {
            Ok(vec![])
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

    struct MemMemoryRepo;
    #[async_trait]
    impl MemoryRepository for MemMemoryRepo {
        async fn find_by_character_id(
            &self,
            _character_id: i64,
            _memory_type: Option<crate::domain::memory::MemoryType>,
            _limit: i64,
        ) -> Result<Vec<crate::domain::memory::Memory>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert(&self, _m: &crate::domain::memory::Memory) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _m: &crate::domain::memory::Memory) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemRelationshipRepo;
    #[async_trait]
    impl RelationshipRepository for MemRelationshipRepo {
        async fn find(
            &self,
            _character_id: i64,
            _participant_id: i64,
        ) -> Result<Option<crate::domain::relationship::Relationship>, RepositoryError> {
            Ok(None)
        }
        async fn find_by_character_id(
            &self,
            _character_id: i64,
        ) -> Result<Vec<crate::domain::relationship::Relationship>, RepositoryError> {
            Ok(vec![])
        }
        async fn upsert(
            &self,
            _r: &crate::domain::relationship::Relationship,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemEmotionRepo;
    #[async_trait]
    impl EmotionStateRepository for MemEmotionRepo {
        async fn find_by_character_id(
            &self,
            _character_id: i64,
        ) -> Result<Option<crate::domain::emotion::EmotionState>, RepositoryError> {
            Ok(None)
        }
        async fn upsert(
            &self,
            _character_id: i64,
            _state: &crate::domain::emotion::EmotionState,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemBindingRepo;
    #[async_trait]
    impl CharacterBindingRepository for MemBindingRepo {
        async fn find_by_character_id(
            &self,
            _character_id: i64,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_by_conversation_id(
            &self,
            _conversation_id: i64,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_all(
            &self,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_all_enabled(
            &self,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert(
            &self,
            _b: &crate::domain::character::CharacterBinding,
        ) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(
            &self,
            _b: &crate::domain::character::CharacterBinding,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    /// 组装一个只含空桩仓储的上下文构建器（chat 不触碰上下文，仅满足构造）。
    fn mem_context_builder() -> Arc<ContextBuilder> {
        Arc::new(ContextBuilder::new(
            Arc::new(MemMessageRepo),
            Arc::new(MemConvRepo),
            Arc::new(MemMemoryRepo),
            Arc::new(MemRelationshipRepo),
            Arc::new(MemEmotionRepo),
            Arc::new(MemBindingRepo),
        ))
    }

    #[tokio::test]
    async fn chat_without_scheduler_returns_none() {
        let layer = CognitionLayer::new(None, mem_context_builder());
        let result = layer
            .chat(
                Some("你是助手".to_string()),
                vec![LlmMessage {
                    role: LlmRole::User,
                    content: "你好".to_string(),
                }],
                2,
            )
            .await
            .expect("未启用 LLM 不应报错");
        assert!(result.is_none(), "未启用 LLM 应返回 None");
    }

    #[tokio::test]
    async fn chat_submits_request_and_returns_response() {
        let scheduler = Arc::new(FakeScheduler::new());
        let layer = CognitionLayer::new(Some(scheduler.clone()), mem_context_builder());

        let result = layer
            .chat(
                Some("你是插件助手".to_string()),
                vec![
                    LlmMessage {
                        role: LlmRole::System,
                        content: "系统提示".to_string(),
                    },
                    LlmMessage {
                        role: LlmRole::User,
                        content: "你好".to_string(),
                    },
                ],
                2,
            )
            .await
            .expect("提交应成功");
        let resp = result.expect("有调度器应返回响应");
        assert_eq!(resp.content, "插件回复");
        assert_eq!(resp.model, "fake");

        // scheduler 恰好收到一次请求，且 payload 组装正确。
        let requests = scheduler.requests();
        assert_eq!(requests.len(), 1, "应只提交一次");
        let req = &requests[0];
        assert_eq!(req.system.as_deref(), Some("你是插件助手"));
        assert_eq!(req.messages.len(), 2);
        assert_eq!(req.messages[0].role, LlmRole::System);
        assert_eq!(req.messages[1].role, LlmRole::User);
        assert_eq!(req.messages[1].content, "你好");
        assert_eq!(req.priority, 2);
        // 与 generate 对齐的默认参数。
        assert_eq!(req.model, None);
        assert_eq!(req.temperature, Some(0.8));
        assert_eq!(req.max_tokens, None);
    }

    #[tokio::test]
    async fn chat_priority_is_passed_through() {
        let scheduler = Arc::new(FakeScheduler::new());
        let layer = CognitionLayer::new(Some(scheduler.clone()), mem_context_builder());

        let _ = layer
            .chat(
                None,
                vec![LlmMessage {
                    role: LlmRole::User,
                    content: "x".to_string(),
                }],
                1,
            )
            .await
            .expect("提交应成功");
        assert_eq!(scheduler.requests()[0].priority, 1);
        assert_eq!(scheduler.requests()[0].system, None);
    }
}
