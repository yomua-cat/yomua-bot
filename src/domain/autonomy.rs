//! 主动意图与回复批次的可恢复领域模型。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 主动意图状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutonomyIntentStatus {
    Pending,
    Cancelled,
    Enqueued,
    Completed,
    Failed,
}

impl AutonomyIntentStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Cancelled => "cancelled",
            Self::Enqueued => "enqueued",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl std::str::FromStr for AutonomyIntentStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "cancelled" => Ok(Self::Cancelled),
            "enqueued" => Ok(Self::Enqueued),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(format!("未知的主动意图状态：{other}")),
        }
    }
}

/// 可恢复的主动行为意图。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AutonomyIntent {
    pub id: i64,
    pub character_id: i64,
    pub conversation_id: i64,
    pub status: AutonomyIntentStatus,
    pub not_before: DateTime<Utc>,
    pub cooldown_key: String,
    pub quota_reservation: u32,
    pub source_snapshot: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// 回复批次状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyBatchStatus {
    Frozen,
    Processing,
    Completed,
    Cancelled,
}

impl ReplyBatchStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Frozen => "frozen",
            Self::Processing => "processing",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl std::str::FromStr for ReplyBatchStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "frozen" => Ok(Self::Frozen),
            "processing" => Ok(Self::Processing),
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(format!("未知的回复批次状态：{other}")),
        }
    }
}

/// 可恢复的最小回复批次快照。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplyBatch {
    pub id: i64,
    pub conversation_id: i64,
    pub start_sequence: i64,
    pub end_sequence: i64,
    pub message_ids: Vec<i64>,
    pub trigger_event_uuid: String,
    pub context_hash: String,
    pub status: ReplyBatchStatus,
    pub lease_token: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_roundtrip_as_stable_strings() {
        for status in [
            AutonomyIntentStatus::Pending,
            AutonomyIntentStatus::Cancelled,
            AutonomyIntentStatus::Enqueued,
            AutonomyIntentStatus::Completed,
            AutonomyIntentStatus::Failed,
        ] {
            assert_eq!(
                status.as_str().parse::<AutonomyIntentStatus>().unwrap(),
                status
            );
        }
        for status in [
            ReplyBatchStatus::Frozen,
            ReplyBatchStatus::Processing,
            ReplyBatchStatus::Completed,
            ReplyBatchStatus::Cancelled,
        ] {
            assert_eq!(status.as_str().parse::<ReplyBatchStatus>().unwrap(), status);
        }
    }
}
