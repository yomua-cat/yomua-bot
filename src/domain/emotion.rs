//! 情绪（Emotion）领域模型。
//!
//! 情绪模型使用标量 `Mood`（0-100），按 Character × Conversation 范围隔离。
//! `Mood` 与 `Stress` 是两个独立的状态值，二者互不推导。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 兼容投影使用的效价维度名称。
pub const VALENCE_DIMENSION: &str = "valence";

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

    /// 将多维情绪的效价维度投影为现有的标量 Mood。
    pub fn from_affect(affect: &AffectVector) -> Self {
        let valence = affect
            .dimensions
            .get(VALENCE_DIMENSION)
            .copied()
            .unwrap_or(0.0);
        Self {
            value: (50.0 + valence * 50.0).clamp(0.0, 100.0),
            last_updated: affect.updated_at,
        }
    }
}

/// 可选的多维情绪状态。
///
/// 维度值默认使用以零为中性的归一化范围；`residue` 保存每个维度的
/// 长期残留值。该类型只提供纯函数能力，当前运行时仍以 `Mood` 为默认状态。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AffectVector {
    pub dimensions: BTreeMap<String, f64>,
    pub residue: BTreeMap<String, f64>,
    pub updated_at: DateTime<Utc>,
}

impl Default for AffectVector {
    fn default() -> Self {
        Self {
            dimensions: BTreeMap::new(),
            residue: BTreeMap::new(),
            updated_at: Utc::now(),
        }
    }
}

/// 多维情绪衰减策略。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct DecayPolicy {
    /// 半衰期，必须为正数。
    pub half_life: chrono::Duration,
    /// 距离残留值小于该阈值时直接归零到残留值。
    pub noise_floor: f64,
}

impl DecayPolicy {
    pub fn new(half_life: chrono::Duration, noise_floor: f64) -> Option<Self> {
        if half_life <= chrono::Duration::zero() || !noise_floor.is_finite() || noise_floor < 0.0 {
            return None;
        }
        Some(Self {
            half_life,
            noise_floor,
        })
    }
}

/// 衰减结果，用于让调用方区分正常 tick 与时钟回拨。
#[derive(Debug, Clone, PartialEq)]
pub struct DecayResult {
    pub affect: AffectVector,
    pub applied: bool,
    pub clock_rollback: bool,
}

impl AffectVector {
    /// 将状态衰减到给定时间。
    ///
    /// 时间相同的重复调用不会继续改变状态；时钟回拨时保留原状态和时间戳，
    /// 以避免下一次正常 tick 产生不可预测的过度衰减。
    pub fn decay_to(&self, now: DateTime<Utc>, policy: DecayPolicy) -> DecayResult {
        if now < self.updated_at {
            return DecayResult {
                affect: self.clone(),
                applied: false,
                clock_rollback: true,
            };
        }
        if now == self.updated_at {
            return DecayResult {
                affect: self.clone(),
                applied: false,
                clock_rollback: false,
            };
        }

        let elapsed = (now - self.updated_at)
            .to_std()
            .expect("非负时间差应可转换为标准时长")
            .as_secs_f64();
        let half_life = policy
            .half_life
            .to_std()
            .expect("正半衰期应可转换为标准时长")
            .as_secs_f64();
        let factor = 0.5_f64.powf(elapsed / half_life);
        let mut dimensions = self.dimensions.clone();
        for (name, value) in &mut dimensions {
            let target = self.residue.get(name).copied().unwrap_or(0.0);
            let distance = *value - target;
            *value = if distance.abs() <= policy.noise_floor {
                target
            } else {
                target + distance * factor
            };
        }
        DecayResult {
            affect: AffectVector {
                dimensions,
                residue: self.residue.clone(),
                updated_at: now,
            },
            applied: true,
            clock_rollback: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).expect("有效时间戳")
    }

    #[test]
    fn half_life_decay_moves_toward_residue() {
        let mut affect = AffectVector {
            dimensions: BTreeMap::from([(String::from(VALENCE_DIMENSION), 1.0)]),
            residue: BTreeMap::from([(String::from(VALENCE_DIMENSION), 0.2)]),
            updated_at: at(0),
        };
        let policy = DecayPolicy::new(chrono::Duration::seconds(10), 0.0).unwrap();
        let result = affect.decay_to(at(10), policy);
        assert!(result.applied);
        assert!(!result.clock_rollback);
        assert!((result.affect.dimensions[VALENCE_DIMENSION] - 0.6).abs() < 1e-9);
        affect = result.affect;
        assert_eq!(Mood::from_affect(&affect).value, 80.0);
    }

    #[test]
    fn repeated_tick_is_idempotent_and_clock_rollback_is_ignored() {
        let affect = AffectVector {
            dimensions: BTreeMap::from([(String::from(VALENCE_DIMENSION), 0.8)]),
            residue: BTreeMap::new(),
            updated_at: at(100),
        };
        let policy = DecayPolicy::new(chrono::Duration::seconds(20), 0.0).unwrap();
        let first = affect.decay_to(at(120), policy);
        let repeated = first.affect.decay_to(at(120), policy);
        assert_eq!(repeated.affect, first.affect);
        assert!(!repeated.applied);

        let rollback = first.affect.decay_to(at(110), policy);
        assert!(rollback.clock_rollback);
        assert_eq!(rollback.affect, first.affect);
    }

    #[test]
    fn invalid_decay_policy_is_rejected() {
        assert!(DecayPolicy::new(chrono::Duration::zero(), 0.0).is_none());
        assert!(DecayPolicy::new(chrono::Duration::seconds(1), -1.0).is_none());
        assert!(DecayPolicy::new(chrono::Duration::seconds(1), f64::NAN).is_none());
    }
}
