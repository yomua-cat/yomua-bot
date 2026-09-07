//! 角色领域模型。

use crate::error::DomainError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Character {
    pub id: i64,
    pub definition: CharacterDefinition,
    pub state: CharacterState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CharacterDefinition {
    pub name: String,
    pub description: Option<String>,
    pub personality: Option<String>,
    pub scenario: Option<String>,
    pub style: Option<String>,
    pub background: Option<String>,
    pub greetings: Vec<String>,
    pub example_messages: Vec<String>,
    pub system_prompt: Option<String>,
    pub post_history_instructions: Option<String>,
    pub lorebook: Vec<LorebookEntry>,
    pub metadata: serde_json::Value,
}

impl CharacterDefinition {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.name.trim().is_empty() {
            return Err(DomainError::InvalidDefinition(
                "角色名称不能为空".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LorebookEntry {
    pub keywords: Vec<String>,
    pub content: String,
    pub enabled: bool,
    pub priority: i32,
}

/// 角色全局状态（Character 范围）。
///
/// 已移除 `attention`、`social_mood`、`last_proactive_at`：
/// - `last_proactive_at` 移入 [`BehaviorState`]（Character × Conversation 范围）；
/// - `attention` 与 `social_mood` 在状态领域模型重构中移除。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CharacterState {
    /// 精力水平（0-100）。影响参与的意愿。
    pub energy: f64,
    /// 压力水平（0-100）。
    pub stress: f64,
    /// 当前活动描述。
    pub current_activity: Option<String>,
    /// 该状态最后一次更新的时间。
    pub last_updated: DateTime<Utc>,
}

impl Default for CharacterState {
    fn default() -> Self {
        Self {
            energy: 72.0,
            stress: 10.0,
            current_activity: None,
            last_updated: Utc::now(),
        }
    }
}

impl CharacterState {
    pub fn clamped(mut self) -> Self {
        self.energy = self.energy.clamp(0.0, 100.0);
        self.stress = self.stress.clamp(0.0, 100.0);
        self
    }
}

/// 会话级状态（Character × Conversation 范围）。
///
/// 与角色全局状态并存，提供会话隔离的精力与压力。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationState {
    /// 精力水平（0-100）。
    pub energy: f64,
    /// 压力水平（0-100）。
    pub stress: f64,
    /// 该状态最后一次更新的时间。
    pub last_updated: DateTime<Utc>,
}

impl Default for ConversationState {
    fn default() -> Self {
        Self {
            energy: 50.0,
            stress: 10.0,
            last_updated: Utc::now(),
        }
    }
}

impl ConversationState {
    pub fn clamped(mut self) -> Self {
        self.energy = self.energy.clamp(0.0, 100.0);
        self.stress = self.stress.clamp(0.0, 100.0);
        self
    }
}

/// 行为状态（Character × Conversation 范围）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BehaviorState {
    /// 主动行为最后一次触发时间（用于冷却判断，None 表示从未主动过）。
    pub last_proactive_at: Option<DateTime<Utc>>,
    /// 该状态最后一次更新的时间。
    pub last_updated: DateTime<Utc>,
}

impl Default for BehaviorState {
    fn default() -> Self {
        Self {
            last_proactive_at: None,
            last_updated: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CharacterBinding {
    pub id: i64,
    pub character_id: i64,
    pub conversation_id: i64,
    pub reply_mode: ReplyMode,
    pub proactive_enabled: bool,
    pub mute_schedule: Option<String>,
    pub behavior_overrides: serde_json::Value,
    pub context_policy: serde_json::Value,
    #[serde(default)]
    pub switched_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub cross_reply_enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReplyMode {
    #[default]
    MentionOnly,
    Occasionally,
    Natural,
}
