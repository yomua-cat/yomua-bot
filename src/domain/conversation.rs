//! 会话（Conversation）领域模型。
//!
//! Conversation 是任何消息上下文的统一抽象（私聊、群聊等）。Core 不理解 QQ 概念。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 一个会话上下文。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversation {
    pub id: i64,

    /// 这是私聊还是群聊会话。
    pub conversation_type: ConversationType,

    /// 平台特定的外部 ID（例如 QQ 群号）。
    /// Core 不解读此值 —— 由适配器提供。
    pub external_id: String,

    /// 显示名称（例如群名）。
    pub name: Option<String>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// 会话类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ConversationType {
    /// 一对一私聊。
    #[default]
    Private,
    /// 多方群聊。
    Group,
}

/// 会话中的参与者。
///
/// 可以是人类用户或 Character。适配器将平台用户 ID
/// 转换为 Participant。Core 不理解 QQ 号码。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Participant {
    pub id: i64,

    /// 该参与者所属的会话。
    pub conversation_id: i64,

    /// 平台特定的外部 ID（例如 QQ 用户号）。
    /// Core 不解读此值。
    pub external_id: String,

    /// 会话中的显示名称。
    pub display_name: String,

    /// 会话中的角色。
    pub role: ParticipantRole,

    /// 来自适配器的任意元数据。
    pub metadata: serde_json::Value,
}

/// 参与者的角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParticipantRole {
    /// 人类用户。
    User,
    /// 由该运行时管理的 Character。
    Character,
    /// 系统 / Bot 操作者。
    System,
}
