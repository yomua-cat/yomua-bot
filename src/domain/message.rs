//! 消息（Message）领域模型。
//!
//! Message 是聊天消息的统一表示。
//! Core 不理解 QQ 消息格式 —— 由适配器负责转换。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 会话中的一条消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: i64,
    pub conversation_id: i64,

    /// 发送者（Participant）的 ID。
    pub sender_id: i64,

    /// 消息内容。
    pub content: MessageContent,

    pub timestamp: DateTime<Utc>,

    /// 所回复消息的 ID（若有）。
    pub reply_to: Option<i64>,

    /// 提及的参与者 ID 列表。
    pub mentions: Vec<i64>,

    /// 附件元数据。
    pub attachments: Vec<Attachment>,

    /// 来自适配器的任意元数据。
    pub metadata: serde_json::Value,
}

/// 消息内容。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageContent {
    /// 纯文本消息。
    Text(String),

    /// 图片消息。
    Image {
        url: Option<String>,
        path: Option<String>,
        description: Option<String>,
    },

    /// 文件附件。
    File {
        url: Option<String>,
        path: Option<String>,
        filename: String,
    },

    /// 混合内容（文本 + 图片等）。
    Mixed(Vec<MixedContentSegment>),

    /// 其他 / 未知内容类型。
    Other(String),
}

/// 混合内容消息中的一个片段。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MixedContentSegment {
    Text(String),
    Image {
        url: Option<String>,
        path: Option<String>,
    },
    Mention {
        participant_id: i64,
        display_name: String,
    },
}

/// 消息上的附件。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment {
    pub filename: String,
    pub url: Option<String>,
    pub path: Option<String>,
    pub mime_type: Option<String>,
    pub size: Option<u64>,
}
