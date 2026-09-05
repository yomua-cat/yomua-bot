//! 角色（Character）领域模型。
//!
//! Character 是一个虚拟人格，包含定义、状态、情绪、关系、记忆与行为。它不是一个 Prompt。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::DomainError;

/// 运行时中已加载的 Character。
///
/// 结合了相对稳定的定义与可变的运行时状态。
/// Character 按需加载并在运行时缓存。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Character {
    pub id: i64,
    pub definition: CharacterDefinition,
    pub state: CharacterState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// 相对稳定的身份与人格定义。
///
/// 字段对应 SillyTavern Character Card 概念，
/// 但这是内部规范模型 —— 而非外部格式。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CharacterDefinition {
    /// 角色显示名称。
    pub name: String,

    /// 角色是谁的简短描述。
    pub description: Option<String>,

    /// 人格特质（自由文本或结构化）。
    pub personality: Option<String>,

    /// 当前场景或设定。
    pub scenario: Option<String>,

    /// 说话风格与习惯。
    pub style: Option<String>,

    /// 背景故事。
    pub background: Option<String>,

    /// 默认问候消息。
    pub greetings: Vec<String>,

    /// 用于少样本提示的示例消息。
    pub example_messages: Vec<String>,

    /// 供 LLM 使用的系统提示 / 指令。
    pub system_prompt: Option<String>,

    /// 历史之后的指令。
    pub post_history_instructions: Option<String>,

    /// Lorebook 条目（关键词 → 内容）。
    pub lorebook: Vec<LorebookEntry>,

    /// 任意元数据（卡版本、来源等）。
    pub metadata: serde_json::Value,
}

impl CharacterDefinition {
    /// 校验定义的核心约束。
    ///
    /// - `name` 非空且非纯空白。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.name.trim().is_empty() {
            return Err(DomainError::InvalidDefinition(
                "角色名称不能为空".to_string(),
            ));
        }
        Ok(())
    }
}

/// 单个 lorebook 条目。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LorebookEntry {
    /// 触发该条目的关键词。
    pub keywords: Vec<String>,

    /// 注入到上下文的内容。
    pub content: String,

    /// 该条目是否启用。
    pub enabled: bool,

    /// 排序优先级（越高越重要）。
    pub priority: i32,
}

/// Character 的可变运行时状态。
///
/// 所有变更都必须持久化到存储。
/// 内存中的值只是运行时缓存。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CharacterState {
    /// 精力水平（0-100）。影响参与的意愿。
    pub energy: f64,

    /// 注意力水平（0-100）。影响回复质量。
    pub attention: f64,

    /// 当前活动描述。
    pub current_activity: Option<String>,

    /// 社交情绪标签。
    pub social_mood: Option<String>,

    /// 压力水平（0-100）。
    pub stress: f64,

    /// 主动行为最后一次触发的时间（用于冷却判断，None 表示从未主动过）。
    pub last_proactive_at: Option<DateTime<Utc>>,

    /// 该状态最后一次更新的时间。
    pub last_updated: DateTime<Utc>,
}

impl Default for CharacterState {
    fn default() -> Self {
        Self {
            energy: 72.0,
            attention: 50.0,
            current_activity: None,
            social_mood: Some("calm".to_string()),
            stress: 10.0,
            last_proactive_at: None,
            last_updated: Utc::now(),
        }
    }
}

impl CharacterState {
    /// 将数值字段（energy / attention / stress）限制在 [0, 100] 范围内。
    ///
    /// 用于外部输入（例如状态补丁）进入内部模型之前的安全归一化。
    pub fn clamped(mut self) -> Self {
        self.energy = self.energy.clamp(0.0, 100.0);
        self.attention = self.attention.clamp(0.0, 100.0);
        self.stress = self.stress.clamp(0.0, 100.0);
        self
    }
}

/// Character 与 Conversation 之间的绑定。
///
/// 定义角色在特定会话中如何参与。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CharacterBinding {
    pub id: i64,
    pub character_id: i64,
    pub conversation_id: i64,

    /// 角色在此会话中的回复方式。
    pub reply_mode: ReplyMode,

    /// 是否启用主动消息。
    pub proactive_enabled: bool,

    /// 静默日程（类 cron 或时间段）。
    pub mute_schedule: Option<String>,

    /// 此绑定的行为覆盖。
    pub behavior_overrides: serde_json::Value,

    /// 上下文策略（包含多少历史等）。
    pub context_policy: serde_json::Value,

    /// 最近一次换角色的生效时间。None 表示从未换过角色（该会话绑定创建即当前角色）。
    /// 换角色后，新角色的上下文只包含该时间之后的消息（硬性约束 A）。
    #[serde(default)]
    pub switched_at: Option<DateTime<Utc>>,

    /// 是否允许回复其他角色的消息（群聊多 Bot 场景）。
    /// - false（默认）：只回复 sender_id == 当前 Bot participant_id 的消息
    /// - true：回复群内所有消息（由 BehaviorEngine 决定是否回复）
    #[serde(default)]
    pub cross_reply_enabled: bool,

    pub created_at: DateTime<Utc>,
}

/// 角色在会话中的回复模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReplyMode {
    /// 仅在明确提及（@Character）时回复。
    MentionOnly,
    /// 偶尔自然插话。
    Occasionally,
    /// 完全基于上下文的参与。
    #[default]
    Natural,
}
