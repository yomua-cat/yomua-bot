//! 情绪（Emotion）领域模型。
//!
//! 情绪模型使用标量 `Mood`（0-100），按 Character × Conversation 范围隔离。
//! `Mood` 与 `Stress` 是两个独立的状态值，二者互不推导。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 标量情绪状态（0-100），按 Character × Conversation 范围隔离。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mood {
    /// 情绪值（0-100）。
    pub value: f64,
    /// 最后一次更新时间。
    pub last_updated: DateTime<Utc>,
}

impl Default for Mood {
    fn default() -> Self {
        Self {
            value: 50.0,
            last_updated: Utc::now(),
        }
    }
}

impl Mood {
    /// 将情绪值限制在 [0, 100]。
    pub fn clamped(mut self) -> Self {
        self.value = self.value.clamp(0.0, 100.0);
        self
    }
}
