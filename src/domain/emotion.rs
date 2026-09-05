//! 情绪（Emotion）领域模型。
//!
//! 情绪使用确定性模型：前一个状态 + 事件 + 衰减 = 新状态。
//! LLM 不是情绪的运算器。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 结构化情绪状态。
///
/// 每个维度是一个 0.0 到 1.0 之间的值。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmotionState {
    pub happiness: f64,
    pub anger: f64,
    pub sadness: f64,
    pub fear: f64,
    pub affection: f64,
    pub stress: f64,
    pub energy: f64,

    /// 该状态最后一次更新的时间。
    pub last_updated: DateTime<Utc>,
}

impl Default for EmotionState {
    fn default() -> Self {
        Self {
            happiness: 0.5,
            anger: 0.0,
            sadness: 0.0,
            fear: 0.0,
            affection: 0.3,
            stress: 0.1,
            energy: 0.7,
            last_updated: Utc::now(),
        }
    }
}

impl EmotionState {
    /// 对所有情绪维度应用时间衰减。
    ///
    /// 情绪自然会向基线漂移（多数为 0.5，愤怒/恐惧为 0.0）。
    pub fn apply_decay(&mut self, elapsed_secs: f64, decay_rate: f64) {
        let factor = (-decay_rate * elapsed_secs).exp();

        // 向基线漂移
        let baselines = [
            (&mut self.happiness, 0.5),
            (&mut self.anger, 0.0),
            (&mut self.sadness, 0.0),
            (&mut self.fear, 0.0),
            (&mut self.affection, 0.3),
            (&mut self.stress, 0.1),
            (&mut self.energy, 0.5),
        ];

        for (value, baseline) in baselines {
            *value = baseline + (*value - baseline) * factor;
        }

        self.last_updated = Utc::now();
    }

    /// 在当前状态应用事件后创建新的 EmotionState。
    pub fn apply_event(&self, event: &EmotionEvent) -> Self {
        let mut new_state = self.clone();

        // 应用事件带来的直接调整
        for adjustment in &event.adjustments {
            match adjustment.dimension.as_str() {
                "happiness" => {
                    new_state.happiness = (new_state.happiness + adjustment.value).clamp(0.0, 1.0)
                }
                "anger" => new_state.anger = (new_state.anger + adjustment.value).clamp(0.0, 1.0),
                "sadness" => {
                    new_state.sadness = (new_state.sadness + adjustment.value).clamp(0.0, 1.0)
                }
                "fear" => new_state.fear = (new_state.fear + adjustment.value).clamp(0.0, 1.0),
                "affection" => {
                    new_state.affection = (new_state.affection + adjustment.value).clamp(0.0, 1.0)
                }
                "stress" => {
                    new_state.stress = (new_state.stress + adjustment.value).clamp(0.0, 1.0)
                }
                "energy" => {
                    new_state.energy = (new_state.energy + adjustment.value).clamp(0.0, 1.0)
                }
                _ => {} // 未知维度，忽略
            }
        }

        new_state.last_updated = Utc::now();
        new_state
    }
}

/// 触发情绪变化的事件。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmotionEvent {
    /// 是什么导致了该情绪事件。
    pub event_type: EmotionEventType,

    /// 对情绪维度的直接调整。
    pub adjustments: Vec<EmotionAdjustment>,

    /// 用于日志/调试的可选描述。
    pub description: Option<String>,
}

/// 触发情绪变化的事件类型。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EmotionEventType {
    /// 收到某人的消息。
    MessageReceived,
    /// 发送了一条消息。
    MessageSent,
    /// 关系发生了变化。
    RelationshipChanged,
    /// 活动发生了变化。
    ActivityChanged,
    /// 基于时间的衰减。
    TimeDecay,
    /// 行为结果（例如被忽略、获得了反应）。
    BehaviorResult,
    /// 自定义事件。
    Custom(String),
}

/// 对某个情绪维度的单一调整。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmotionAdjustment {
    /// 调整哪个维度（例如 "happiness"、"anger"）。
    pub dimension: String,

    /// 调整多少（正 = 增加，负 = 减少）。
    pub value: f64,
}
