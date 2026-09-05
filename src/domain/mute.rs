//! 静默时段（Mute Schedule）领域模型。
//!
//! 用于把形如 `"22:00-06:00"` 的静默日程解析为一个窗口，并判断某个时刻
//! 是否落在该窗口内（支持跨午夜）。本模块是纯函数，不依赖 chrono，便于
//! 单独测试静默语义。

use crate::error::DomainError;

/// 一天中的某个时刻。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeOfDay {
    /// 小时（0-23）。
    pub hour: u32,
    /// 分钟（0-59）。
    pub minute: u32,
}

impl TimeOfDay {
    /// 由时分构造，越界返回 Err。
    fn new(hour: u32, minute: u32) -> Result<Self, DomainError> {
        if hour > 23 || minute > 59 {
            return Err(DomainError::InvalidState(format!(
                "非法时刻 {hour:02}:{minute:02}"
            )));
        }
        Ok(Self { hour, minute })
    }

    /// 把时刻折合为当天零点起的分钟数。
    fn to_minutes(self) -> u32 {
        self.hour * 60 + self.minute
    }
}

/// 一个静默窗口，区间为 `[from, until)`。
///
/// 支持跨午夜：当 `from` 晚于 `until` 时（如 22:00-06:00），窗口跨过午夜。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MuteWindow {
    pub from: TimeOfDay,
    pub until: TimeOfDay,
}

/// 解析 `"HH:MM-HH:MM"` 形式的静默时段。
///
/// - 空串或纯空白 → `Ok(None)`（无静默）。
/// - 合法 → `Ok(Some(MuteWindow))`。
/// - 格式错误 / 数值越界 → `Err`。
pub fn parse_mute_schedule(s: &str) -> Result<Option<MuteWindow>, DomainError> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(None);
    }

    let (from_part, until_part) = s
        .split_once('-')
        .ok_or_else(|| DomainError::InvalidState(format!("静默时段缺少 '-' 分隔符：{s}")))?;
    let from = parse_time(from_part)?;
    let until = parse_time(until_part)?;

    Ok(Some(MuteWindow { from, until }))
}

/// 解析单个 `"HH:MM"` 时刻。
fn parse_time(s: &str) -> Result<TimeOfDay, DomainError> {
    let s = s.trim();
    let (h, m) = s
        .split_once(':')
        .ok_or_else(|| DomainError::InvalidState(format!("时刻缺少 ':'：{s}")))?;
    let hour: u32 = h
        .trim()
        .parse()
        .map_err(|_| DomainError::InvalidState(format!("非法的时：{h}")))?;
    let minute: u32 = m
        .trim()
        .parse()
        .map_err(|_| DomainError::InvalidState(format!("非法的分：{m}")))?;
    TimeOfDay::new(hour, minute)
}

/// 判断某个时刻是否落在静默窗口内。
///
/// 非跨午夜窗口采用 `[from, until)` 半开区间；跨午夜窗口（from > until）
/// 在 `t >= from || t < until` 时为真。
pub fn is_within_window(window: &MuteWindow, time: &TimeOfDay) -> bool {
    let t = time.to_minutes();
    let from = window.from.to_minutes();
    let until = window.until.to_minutes();

    if from <= until {
        t >= from && t < until
    } else {
        // 跨午夜窗口，例如 22:00-06:00。
        t >= from || t < until
    }
}
