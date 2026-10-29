//! 平台级用户身份。
//!
//! `User` 是跨会话稳定的身份；会话中的 `Participant` 仍负责显示名、角色
//! 和消息作者归属。两者不能互相替代。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct User {
    pub id: i64,
    pub platform: String,
    pub external_id: String,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
