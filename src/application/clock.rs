//! 时间源 —— 把"当前时间"抽象为可注入的依赖。
//!
//! 行为决策（decided_at、mute 评估、proactive cooldown）都需要读取当前时间。
//! 为了让这些逻辑可测试（尤其是 mute / cooldown 这类依赖时间的确定性逻辑），
//! 统一通过 [`Clock`] 获取时间，而不是直接调用 `Utc::now()`。生产环境注入
//! [`SystemClock`]，测试注入可推进的 `FakeClock`。

use std::sync::Arc;

use chrono::{DateTime, Utc};

/// 时钟抽象。
pub trait Clock: Send + Sync {
    /// 返回当前 UTC 时间。
    fn now(&self) -> DateTime<Utc>;
}

/// 使用系统真实时钟的实现。
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// 方便把任意 `Clock` 包装为 `Arc<dyn Clock>`。
pub fn system_clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_returns_recent_time() {
        let before = Utc::now();
        let now = SystemClock.now();
        let after = Utc::now();
        assert!(now >= before, "系统时间不应早于调用前");
        assert!(now <= after, "系统时间不应晚于调用后");
    }
}
