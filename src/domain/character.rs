//! 角色领域模型。

use crate::error::DomainError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Character {
    pub id: i64,
    pub definition: CharacterDefinition,
    /// 全局启用状态。false 时该角色不参与任何会话。
    pub enabled: bool,
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
    /// 替换常用角色卡宏；未知宏保留，不能猜测用户提供的扩展。
    pub fn expand_macros(&self, text: &str, user_name: &str) -> String {
        text.replace("{{char}}", &self.name)
            .replace("{{user}}", user_name)
    }

    /// 依据最近消息与世界书规则选择条目。消息按时间升序，最后一项为当前轮。
    pub fn select_lorebook(&self, messages: &[String]) -> Vec<LorebookEntry> {
        use serde_json::Value;
        let config = self.metadata.get("lorebook_config");
        let depth = config
            .and_then(|v| v.get("scan_depth"))
            .and_then(Value::as_u64)
            .and_then(|v| usize::try_from(v).ok())
            .unwrap_or(messages.len());
        let source = messages
            .iter()
            .rev()
            .take(depth)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        let budget = config
            .and_then(|v| v.get("token_budget"))
            .and_then(Value::as_u64)
            .and_then(|v| usize::try_from(v).ok())
            .unwrap_or(usize::MAX);
        let recursive = config
            .and_then(|v| v.get("recursive_scanning"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let rules = self
            .metadata
            .get("lorebook_rules")
            .and_then(Value::as_array);
        let mut selected = vec![false; self.lorebook.len()];
        let mut text = source;
        // 递归匹配最多处理条目数轮，循环引用不会无限扩张。
        for _ in 0..self.lorebook.len().max(1) {
            let mut added = false;
            for (i, entry) in self.lorebook.iter().enumerate() {
                if selected[i] || !entry.enabled {
                    continue;
                }
                let rule = rules.and_then(|v| v.get(i));
                let flag = |key: &str| {
                    rule.and_then(|v| {
                        v.get(key)
                            .or_else(|| v.get("extensions").and_then(|e| e.get(key)))
                    })
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                };
                let sensitive = flag("case_sensitive");
                let regex = flag("use_regex");
                let matches = |key: &str| {
                    if key.is_empty() {
                        return false;
                    }
                    if regex {
                        regex::RegexBuilder::new(key)
                            .case_insensitive(!sensitive)
                            .size_limit(256 * 1024)
                            .build()
                            .is_ok_and(|pattern| pattern.is_match(&text))
                    } else if sensitive {
                        text.contains(key)
                    } else {
                        text.to_lowercase().contains(&key.to_lowercase())
                    }
                };
                let primary = entry.keywords.iter().any(|k| matches(k));
                let secondary = rule
                    .and_then(|v| v.get("secondary_keys").or_else(|| v.get("keysecondary")))
                    .and_then(Value::as_array)
                    .is_some_and(|keys| keys.iter().filter_map(Value::as_str).any(matches));
                if flag("constant") || (primary && (!flag("selective") || secondary)) {
                    selected[i] = true;
                    added = true;
                }
            }
            if !recursive || !added {
                break;
            }
            text = format!(
                "{}\n{}",
                text,
                self.lorebook
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| selected[*i])
                    .map(|(_, e)| e.content.as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }
        let mut result = self
            .lorebook
            .iter()
            .enumerate()
            .filter(|(i, _)| selected[*i])
            .map(|(_, e)| e.clone())
            .collect::<Vec<_>>();
        result.sort_by_key(|e| std::cmp::Reverse(e.priority));
        let mut used = 0usize;
        result.retain(|e| {
            // 近似且偏保守：ASCII 四字节一 token，非 ASCII 字符两 token。
            let ascii = e.content.chars().filter(char::is_ascii).count();
            let estimate = ascii
                .div_ceil(4)
                .saturating_add((e.content.chars().count() - ascii).saturating_mul(2));
            if used.saturating_add(estimate) <= budget {
                used = used.saturating_add(estimate);
                true
            } else {
                false
            }
        });
        result
    }

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
    /// 主动额度统计所属日期（使用配置的主动时区计算）。
    #[serde(default)]
    pub proactive_count_date: Option<chrono::NaiveDate>,
    /// 当前统计日已排队的主动消息数量。
    #[serde(default)]
    pub proactive_count: u32,
    /// 该状态最后一次更新的时间。
    pub last_updated: DateTime<Utc>,
}

impl Default for BehaviorState {
    fn default() -> Self {
        Self {
            last_proactive_at: None,
            proactive_count_date: None,
            proactive_count: 0,
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
