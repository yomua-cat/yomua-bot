//! 黑名单领域模型。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 黑名单条目。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlacklistEntry {
    pub id: i64,

    /// 被拉黑的目标（QQ 号或群号，取决于 target_type）。
    pub target: String,

    /// 目标类型。
    pub target_type: BlacklistType,

    /// 拉黑原因（可选）。
    pub reason: Option<String>,

    pub created_at: DateTime<Utc>,

    /// 过期时间。None 表示永久。
    pub expires_at: Option<DateTime<Utc>>,
}

impl BlacklistEntry {
    /// 判断当前条目是否仍然有效（未过期）。
    pub fn is_active(&self) -> bool {
        match self.expires_at {
            Some(expires) => Utc::now() < expires,
            None => true, // 永久
        }
    }

    /// 判断是否为临时封禁。
    pub fn is_temporary(&self) -> bool {
        self.expires_at.is_some()
    }
}

/// 黑名单目标类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlacklistType {
    /// QQ 用户。
    User,
    /// QQ 群。
    Group,
}

impl BlacklistType {
    pub fn as_str(&self) -> &'static str {
        match self {
            BlacklistType::User => "user",
            BlacklistType::Group => "group",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "user" => Some(BlacklistType::User),
            "group" => Some(BlacklistType::Group),
            _ => None,
        }
    }
}

/// 用于创建新黑名单条目的输入（不含 id）。
#[derive(Debug, Clone)]
pub struct NewBlacklistEntry {
    pub target: String,
    pub target_type: BlacklistType,
    pub reason: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}
