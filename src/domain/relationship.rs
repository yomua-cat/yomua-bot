//! 关系（Relationship）领域模型。
//!
//! 关系是 Character × Participant，不是全局用户属性。
//! 同一个用户面对不同 Character 可以拥有完全不同的关系。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Character 与 Participant 之间的关系。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relationship {
    pub character_id: i64,
    pub participant_id: i64,

    /// 熟悉程度（0.0 - 1.0）。
    pub familiarity: f64,

    /// 好感度（0.0 - 1.0）。
    pub affection: f64,

    /// 信任度（0.0 - 1.0）。
    pub trust: f64,

    /// 尊重程度（0.0 - 1.0）。
    pub respect: f64,

    /// 厌烦程度（0.0 - 1.0）。
    pub annoyance: f64,

    /// 亲密程度（0.0 - 1.0）。
    pub intimacy: f64,

    /// 交互总次数。
    pub interaction_count: i64,

    /// 最后一次交互的时间。
    pub last_interaction: DateTime<Utc>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Relationship {
    /// 以默认（陌生人）值创建一个新关系。
    pub fn new(character_id: i64, participant_id: i64) -> Self {
        let now = Utc::now();
        Self {
            character_id,
            participant_id,
            familiarity: 0.0,
            affection: 0.2,
            trust: 0.1,
            respect: 0.2,
            annoyance: 0.0,
            intimacy: 0.0,
            interaction_count: 0,
            last_interaction: now,
            created_at: now,
            updated_at: now,
        }
    }

    /// 记录一次交互，更新交互次数与时间戳。
    pub fn record_interaction(&mut self) {
        self.interaction_count += 1;
        self.last_interaction = Utc::now();
        self.updated_at = Utc::now();

        // 熟悉度随交互次数对数增长
        self.familiarity = (1.0 + self.interaction_count as f64).ln() / 10.0;
        self.familiarity = self.familiarity.min(1.0);
    }
}
