//! SQLite 仓储共享的时间戳解析工具。

use crate::error::RepositoryError;
use chrono::{DateTime, Utc};

/// 解析来自 SQLite 的时间戳字符串。
///
/// 支持两种格式：
/// - RFC 3339：`2026-09-03T12:34:56Z` 或 `2026-09-03T12:34:56+00:00`
/// - SQLite datetime：`2026-09-03 12:34:56`
pub fn parse_timestamp(s: &str) -> Result<DateTime<Utc>, RepositoryError> {
    // 先尝试 RFC 3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }

    // 再尝试 SQLite datetime 格式："YYYY-MM-DD HH:MM:SS"
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(naive.and_utc());
    }

    Err(RepositoryError::Database(format!(
        "invalid timestamp format: {s}"
    )))
}
