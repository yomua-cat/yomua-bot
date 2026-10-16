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

    /// 平台时间（OneBot/NapCat time），即消息发生时间。
    pub timestamp: DateTime<Utc>,

    /// 所回复消息的 ID（若有）。
    pub reply_to: Option<i64>,

    /// 提及的参与者 ID 列表。
    pub mentions: Vec<i64>,

    /// 附件元数据。
    pub attachments: Vec<Attachment>,

    /// 来自适配器的任意元数据。
    pub metadata: serde_json::Value,

    /// 该消息被记录时，哪个角色正处于 Active 状态。
    /// 用于 Character × Conversation 可见性过滤：只有当时为 Active 的角色才能看到此消息。
    /// 如果为 None（存量数据），过滤时退化为 switched_at 逻辑。
    pub active_character_id: Option<i64>,

    /// 外部平台消息 ID（来自 OneBot/NapCat）。
    /// 用于 correlation identity，不用于 ordering/dedup。
    pub platform_message_id: Option<i64>,

    /// QQBOT Application Ingress Order。
    /// Conversation 内严格递增，不要求连续，gap 合法。
    /// NULL 表示缺少可证明的入口顺序（例如手工导入的数据）。
    pub conversation_sequence: Option<i64>,

    /// 平台时间（OneBot/NapCat），即消息发生时间。
    /// 与 timestamp 相同语义，但明确标注为 platform-provided。
    pub occurred_at: Option<DateTime<Utc>>,

    /// QQBOT Application 接受该消息的时间。
    /// 在 Message 被 commit 到 DB 之前不应使用。
    pub ingested_at: Option<DateTime<Utc>>,
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

/// 持久回复任务状态，发送中的任务在恢复时需要核查。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyTaskStatus {
    Pending,
    Generating,
    Generated,
    Sending,
    Sent,
    Unknown,
    Cancelled,
    Expired,
    Ignored,
}
impl ReplyTaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Sent | Self::Unknown | Self::Cancelled | Self::Expired | Self::Ignored
        )
    }

    /// 只有已完整生成的任务可以复用持久化内容。
    /// `unknown` 即使带有内容也代表平台结果未核查，不能从普通处理路径补发。
    pub fn can_reuse_content(self) -> bool {
        matches!(self, Self::Generated)
    }
}
impl std::fmt::Display for ReplyTaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Pending => "pending",
                Self::Generating => "generating",
                Self::Generated => "generated",
                Self::Sending => "sending",
                Self::Sent => "sent",
                Self::Unknown => "unknown",
                Self::Cancelled => "cancelled",
                Self::Expired => "expired",
                Self::Ignored => "ignored",
            }
        )
    }
}

#[cfg(test)]
mod reply_task_status_tests {
    use super::ReplyTaskStatus;

    #[test]
    fn unknown_content_is_never_reused_by_normal_processing() {
        assert!(ReplyTaskStatus::Generated.can_reuse_content());
        assert!(!ReplyTaskStatus::Sending.can_reuse_content());
        assert!(!ReplyTaskStatus::Unknown.can_reuse_content());
        assert!(!ReplyTaskStatus::Sent.can_reuse_content());
    }
}

#[derive(Debug, Clone)]
pub struct ReplyTask {
    pub status: ReplyTaskStatus,
    pub content: Option<String>,
    pub error: Option<String>,
    /// 平台已返回的消息 ID；任务仍为 sending 时可据此补写 assistant 历史。
    pub platform_message_id: Option<i64>,
}

/// 发送结果未知、需要人工核查的平台消息任务。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnknownReplyTask {
    pub message_id: i64,
    pub character_id: i64,
    pub platform_message_id: Option<i64>,
    pub content: Option<String>,
    pub error: Option<String>,
    pub updated_at: DateTime<Utc>,
}
