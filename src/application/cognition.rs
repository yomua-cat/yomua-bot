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
use crate::infrastructure::llm::{LlmMessage, LlmRequest, LlmRole};

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
}
