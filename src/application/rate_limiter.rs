//! 普通回复限额。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::domain::character::ResponseRateLimitPolicy;
use crate::error::RepositoryError;

/// 普通回复限额的持久化/内存实现接口。
#[async_trait]
pub trait ResponseRateLimiter: Send + Sync {
    /// 在限额允许时原子预留一次回复额度。
    async fn try_reserve(
        &self,
        character_id: i64,
        conversation_id: i64,
        participant_id: Option<i64>,
        policy: &ResponseRateLimitPolicy,
        is_mentioned: bool,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError>;
}

/// 测试和无持久化装配使用的实现，按窗口保存内存事件。
#[derive(Default)]
pub struct InMemoryResponseRateLimiter {
    events: Mutex<HashMap<RateLimitKey, Vec<DateTime<Utc>>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum RateLimitKey {
    User(i64, i64, i64),
    Session(i64, i64),
}

#[async_trait]
impl ResponseRateLimiter for InMemoryResponseRateLimiter {
    async fn try_reserve(
        &self,
        character_id: i64,
        conversation_id: i64,
        participant_id: Option<i64>,
        policy: &ResponseRateLimitPolicy,
        is_mentioned: bool,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        try_reserve_in_memory(
            &self.events,
            character_id,
            conversation_id,
            participant_id,
            policy,
            is_mentioned,
            now,
        )
    }
}

/// 统一的策略判断，供内存和 SQLite 实现复用。
pub fn effective_limits(
    policy: &ResponseRateLimitPolicy,
    is_mentioned: bool,
) -> (Option<u32>, Option<u32>) {
    if is_mentioned && policy.mention_exempt {
        return (None, None);
    }
    let multiplier = if is_mentioned {
        policy.mention_multiplier.max(1)
    } else {
        1
    };
    (
        policy
            .per_user_per_minute
            .map(|limit| limit.saturating_mul(multiplier)),
        policy
            .per_session_per_hour
            .map(|limit| limit.saturating_mul(multiplier)),
    )
}

fn try_reserve_in_memory(
    events: &Mutex<HashMap<RateLimitKey, Vec<DateTime<Utc>>>>,
    character_id: i64,
    conversation_id: i64,
    participant_id: Option<i64>,
    policy: &ResponseRateLimitPolicy,
    is_mentioned: bool,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let (user_limit, session_limit) = effective_limits(policy, is_mentioned);
    if user_limit.is_none() && session_limit.is_none() {
        return Ok(true);
    }

    let mut events = events
        .lock()
        .map_err(|_| RepositoryError::Database("回复限额锁已损坏".into()))?;
    let user_key = participant_id.map(|id| RateLimitKey::User(character_id, conversation_id, id));
    let session_key = RateLimitKey::Session(character_id, conversation_id);

    if let Some(limit) = user_limit {
        let Some(key) = &user_key else {
            // 没有参与者身份时无法套用每用户限额，仍由会话限额保护。
            if session_limit.is_none() {
                return Ok(true);
            }
            return check_and_reserve_session(
                &mut events,
                session_key,
                session_limit.expect("已确认会话限额存在"),
                now,
            );
        };
        prune(&mut events, key, now - Duration::minutes(1));
        if events.get(key).map_or(0, Vec::len) >= limit as usize {
            return Ok(false);
        }
    }

    if let Some(limit) = session_limit {
        prune(&mut events, &session_key, now - Duration::hours(1));
        if events.get(&session_key).map_or(0, Vec::len) >= limit as usize {
            return Ok(false);
        }
    }

    if user_limit.is_some() {
        if let Some(key) = user_key {
            events.entry(key).or_default().push(now);
        }
    }
    if session_limit.is_some() {
        events.entry(session_key).or_default().push(now);
    }
    Ok(true)
}

fn check_and_reserve_session(
    events: &mut HashMap<RateLimitKey, Vec<DateTime<Utc>>>,
    key: RateLimitKey,
    limit: u32,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    prune(events, &key, now - Duration::hours(1));
    if events.get(&key).map_or(0, Vec::len) >= limit as usize {
        return Ok(false);
    }
    events.entry(key).or_default().push(now);
    Ok(true)
}

fn prune(
    events: &mut HashMap<RateLimitKey, Vec<DateTime<Utc>>>,
    key: &RateLimitKey,
    boundary: DateTime<Utc>,
) {
    if let Some(values) = events.get_mut(key) {
        values.retain(|timestamp| *timestamp > boundary);
        if values.is_empty() {
            events.remove(key);
        }
    }
}

/// 测试和装配使用的默认限额器。
pub fn in_memory_rate_limiter() -> Arc<dyn ResponseRateLimiter> {
    Arc::new(InMemoryResponseRateLimiter::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 20, 0, 0, 0).unwrap()
    }

    fn policy(user: Option<u32>, session: Option<u32>) -> ResponseRateLimitPolicy {
        ResponseRateLimitPolicy {
            per_user_per_minute: user,
            per_session_per_hour: session,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn sliding_windows_and_users_are_independent() {
        let limiter = InMemoryResponseRateLimiter::default();
        let policy = policy(Some(2), Some(10));
        let t0 = now();

        assert!(limiter
            .try_reserve(1, 10, Some(11), &policy, false, t0)
            .await
            .unwrap());
        assert!(limiter
            .try_reserve(1, 10, Some(11), &policy, false, t0 + Duration::seconds(10))
            .await
            .unwrap());
        assert!(!limiter
            .try_reserve(1, 10, Some(11), &policy, false, t0 + Duration::seconds(20))
            .await
            .unwrap());
        assert!(limiter
            .try_reserve(1, 10, Some(12), &policy, false, t0 + Duration::seconds(20))
            .await
            .unwrap());
        assert!(limiter
            .try_reserve(1, 10, Some(11), &policy, false, t0 + Duration::seconds(61))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn mentioned_messages_use_wider_limit_or_exemption() {
        let limiter = InMemoryResponseRateLimiter::default();
        let policy = policy(Some(1), Some(1));
        let t0 = now();
        assert!(limiter
            .try_reserve(1, 10, Some(11), &policy, false, t0)
            .await
            .unwrap());
        assert!(!limiter
            .try_reserve(1, 10, Some(11), &policy, false, t0)
            .await
            .unwrap());
        assert!(limiter
            .try_reserve(1, 10, Some(11), &policy, true, t0)
            .await
            .unwrap());

        let exempt = ResponseRateLimitPolicy {
            mention_exempt: true,
            ..policy
        };
        assert!(limiter
            .try_reserve(1, 10, Some(11), &exempt, true, t0)
            .await
            .unwrap());
    }
}
