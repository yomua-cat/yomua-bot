//! 入站 OneBot 事件持久化领域类型。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 入站事件的持久处理状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InboundEventStatus {
    Received,
    Processing,
    Done,
    Ignored,
    Dead,
}

impl InboundEventStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Processing => "processing",
            Self::Done => "done",
            Self::Ignored => "ignored",
            Self::Dead => "dead",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Ignored | Self::Dead)
    }
}

impl std::fmt::Display for InboundEventStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for InboundEventStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "received" => Ok(Self::Received),
            "processing" => Ok(Self::Processing),
            "done" => Ok(Self::Done),
            "ignored" => Ok(Self::Ignored),
            "dead" => Ok(Self::Dead),
            _ => Err(format!("未知的入站事件状态: {value}")),
        }
    }
}

/// 一条持久化的原始 OneBot 入站事件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundEvent {
    pub ingress_seq: i64,
    pub raw_event: String,
    pub received_at: DateTime<Utc>,
    pub status: InboundEventStatus,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub processing_at: Option<DateTime<Utc>>,
    pub processed_at: Option<DateTime<Utc>>,
}
