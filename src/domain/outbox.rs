//! C2-4 Durable Event Delivery — Outbox Domain Types
//!
//! 本模块定义 outbox 相关的领域类型，不包含任何基础设施实现。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 事件处理结果。
///
/// 用于 outbox 系统的完成信号：
/// - `Success`: 事件处理成功完成
/// - `RetryableFailure`: 可重试的失败（临时性错误，如网络超时）
/// - `PermanentFailure`: 永久性失败（业务错误，不再重试）
///
/// **注意**：`completed` 状态不代表：
/// - QQ 用户已收到消息
/// - OneBot external delivery exactly-once
/// - 下游全局 exactly-once
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProcessingOutcome {
    /// 处理成功完成。
    Success,
    /// 可重试的失败。
    RetryableFailure { error: String },
    /// 永久性失败。
    PermanentFailure { error: String },
}

impl ProcessingOutcome {
    /// 判断是否为成功。
    pub fn is_success(&self) -> bool {
        matches!(self, ProcessingOutcome::Success)
    }

    /// 判断是否为可重试失败。
    pub fn is_retryable(&self) -> bool {
        matches!(self, ProcessingOutcome::RetryableFailure { .. })
    }

    /// 判断是否为永久性失败。
    pub fn is_permanent(&self) -> bool {
        matches!(self, ProcessingOutcome::PermanentFailure { .. })
    }
}

impl std::fmt::Display for ProcessingOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessingOutcome::Success => write!(f, "Success"),
            ProcessingOutcome::RetryableFailure { error } => write!(f, "RetryableFailure({error})"),
            ProcessingOutcome::PermanentFailure { error } => write!(f, "PermanentFailure({error})"),
        }
    }
}

/// Outbox 事件状态。
///
/// 状态转换图：
///
/// ```text
/// pending ──→ processing ──→ completed
///     │          │
///     │          ├──→ retry_wait ──→ pending (重试)
///     │          │
///     └──→ dead_letter (跳过)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxStatus {
    /// 事件已创建，等待被拾取。
    Pending,
    /// 事件正在被处理（已 claim lease）。
    Processing,
    /// 处理失败，等待重试（指数退避）。
    RetryWait,
    /// 处理成功完成。
    Completed,
    /// 永久性失败，不再重试。
    DeadLetter,
}

impl OutboxStatus {
    /// 判断状态是否为终态。
    pub fn is_terminal(&self) -> bool {
        matches!(self, OutboxStatus::Completed | OutboxStatus::DeadLetter)
    }

    /// 判断状态是否可以继续处理（被 claim）。
    pub fn is_claimable(&self) -> bool {
        matches!(self, OutboxStatus::Pending | OutboxStatus::RetryWait)
    }
}

impl std::fmt::Display for OutboxStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutboxStatus::Pending => write!(f, "pending"),
            OutboxStatus::Processing => write!(f, "processing"),
            OutboxStatus::RetryWait => write!(f, "retry_wait"),
            OutboxStatus::Completed => write!(f, "completed"),
            OutboxStatus::DeadLetter => write!(f, "dead_letter"),
        }
    }
}

impl std::str::FromStr for OutboxStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(OutboxStatus::Pending),
            "processing" => Ok(OutboxStatus::Processing),
            "retry_wait" => Ok(OutboxStatus::RetryWait),
            "completed" => Ok(OutboxStatus::Completed),
            "dead_letter" => Ok(OutboxStatus::DeadLetter),
            _ => Err(format!("未知的 outbox 状态: {s}")),
        }
    }
}

/// Outbox 事件行（从数据库读取的结构）。
#[derive(Debug, Clone)]
pub struct OutboxEvent {
    /// 数据库主键。
    pub id: i64,
    /// 事件稳定身份标识。
    pub event_uuid: String,
    /// 关联实体 ID（如 messages.id）。
    pub entity_id: i64,
    /// 实体类型（如 "messages"）。
    pub entity_type: String,
    /// 事件类型（如 "MessageAccepted"）。
    pub event_type: String,
    /// 事件版本。
    pub event_version: i32,
    /// JSON 序列化的完整事件数据。
    pub payload: String,
    /// 排序键（用于 per-conversation 排序）。
    pub ordering_key: String,
    /// 排序序号（conversation_sequence）。
    pub ordering_seq: i64,
    /// 事件创建时间。
    pub created_at: DateTime<Utc>,
    /// 事件可被拾取的时间（用于延迟处理）。
    pub available_at: DateTime<Utc>,
    /// 当前状态。
    pub status: OutboxStatus,
    /// 已尝试次数。
    pub attempts: i32,
    /// 最近一次错误信息。
    pub last_error: Option<String>,
    /// Lease 过期时间（用于分布式 claim）。
    pub locked_until: Option<DateTime<Utc>>,
    /// 处理完成时间。
    pub processed_at: Option<DateTime<Utc>>,
    /// P0 Fix: Lease ownership token，用于 worker 身份验证。
    /// Worker claim 时生成 UUID 并持久化，完成时验证。
    pub lease_token: Option<String>,
}

/// 反序列化后的消息接受事件（从 outbox payload 解析）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OutboxMessageAcceptedPayload {
    pub event_uuid: String,
    pub conversation_id: i64,
    pub sender_id: i64,
    pub message_id: i64,
    pub content: String,
    pub timestamp: DateTime<Utc>,
    pub is_mentioned: bool,
    pub character_id: i64,
    pub conversation_sequence: i64,
    pub platform_message_id: Option<i64>,
    pub ingested_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processing_outcome_display() {
        assert_eq!(ProcessingOutcome::Success.to_string(), "Success");
        assert_eq!(
            ProcessingOutcome::RetryableFailure {
                error: "timeout".to_string()
            }
            .to_string(),
            "RetryableFailure(timeout)"
        );
        assert_eq!(
            ProcessingOutcome::PermanentFailure {
                error: "invalid".to_string()
            }
            .to_string(),
            "PermanentFailure(invalid)"
        );
    }

    #[test]
    fn test_outbox_status_terminal() {
        assert!(!OutboxStatus::Pending.is_terminal());
        assert!(!OutboxStatus::Processing.is_terminal());
        assert!(!OutboxStatus::RetryWait.is_terminal());
        assert!(OutboxStatus::Completed.is_terminal());
        assert!(OutboxStatus::DeadLetter.is_terminal());
    }

    #[test]
    fn test_outbox_status_claimable() {
        assert!(OutboxStatus::Pending.is_claimable());
        assert!(!OutboxStatus::Processing.is_claimable());
        assert!(OutboxStatus::RetryWait.is_claimable());
        assert!(!OutboxStatus::Completed.is_claimable());
        assert!(!OutboxStatus::DeadLetter.is_claimable());
    }

    #[test]
    fn test_outbox_status_parse() {
        assert_eq!(
            "pending".parse::<OutboxStatus>().unwrap(),
            OutboxStatus::Pending
        );
        assert_eq!(
            "processing".parse::<OutboxStatus>().unwrap(),
            OutboxStatus::Processing
        );
        assert_eq!(
            "retry_wait".parse::<OutboxStatus>().unwrap(),
            OutboxStatus::RetryWait
        );
        assert_eq!(
            "completed".parse::<OutboxStatus>().unwrap(),
            OutboxStatus::Completed
        );
        assert_eq!(
            "dead_letter".parse::<OutboxStatus>().unwrap(),
            OutboxStatus::DeadLetter
        );
        assert_eq!(
            "unknown".parse::<OutboxStatus>().unwrap_err(),
            "未知的 outbox 状态: unknown"
        );
    }
}
