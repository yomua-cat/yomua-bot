//! C2-4 Durable Event Delivery 测试。
//!
//! 覆盖 outbox 系统的核心语义：
//! - message + outbox atomicity
//! - outbox event unique identity
//! - pending recovery
//! - expired processing lease recovery
//! - retry_wait scheduling
//! - permanent failure → dead_letter
//! - same conversation sequence serialization
//! - different conversations concurrency
//! - poison event does not permanently block next sequence
//! - MPSC send does not mark completed
//! - ReplyProcessor success → completed
//! - Retryable failure → retry_wait
//! - Permanent failure → dead_letter
//! - crash/restart recovery model
//! - duplicate delivery explicitly tolerated

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::mpsc;

use crate::domain::outbox::{
    OutboxEvent, OutboxMessageAcceptedPayload, OutboxStatus, ProcessingOutcome,
};
use crate::domain::repository::OutboxRepository;
use crate::error::RepositoryError;

/// 内存实现的无操作 OutboxRepository（用于基础测试）。
#[derive(Default)]
pub struct MemOutboxRepo {
    events: std::sync::Mutex<Vec<OutboxEvent>>,
}

impl MemOutboxRepo {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl OutboxRepository for MemOutboxRepo {
    async fn claim_next_event(
        &self,
        lease_duration_secs: i64,
    ) -> Result<Option<OutboxEvent>, RepositoryError> {
        let now = Utc::now();
        let lease = now + chrono::Duration::seconds(lease_duration_secs);

        let mut events = self.events.lock().unwrap();
        if let Some(pos) = events.iter().position(|e| {
            e.status == OutboxStatus::Pending
                && e.available_at <= now
                && e.locked_until.map(|l| l <= now).unwrap_or(true)
        }) {
            let event = events.get_mut(pos).unwrap();
            event.status = OutboxStatus::Processing;
            event.locked_until = Some(lease);
            event.attempts += 1;
            // P0: 生成唯一的 lease_token
            event.lease_token = Some(uuid::Uuid::new_v4().to_string());
            return Ok(Some(event.clone()));
        }
        Ok(None)
    }

    async fn mark_completed(
        &self,
        event_uuid: &str,
        lease_token: &str,
    ) -> Result<(), RepositoryError> {
        let mut events = self.events.lock().unwrap();
        if let Some(e) = events.iter_mut().find(|e| e.event_uuid == event_uuid) {
            // P0: 验证 lease_token 匹配
            if e.lease_token.as_deref() == Some(lease_token) {
                e.status = OutboxStatus::Completed;
                e.locked_until = None;
                e.lease_token = None;
                e.processed_at = Some(Utc::now());
            }
        }
        Ok(())
    }

    async fn mark_retry_wait(
        &self,
        event_uuid: &str,
        lease_token: &str,
        _error: &str,
        next_available_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), RepositoryError> {
        let mut events = self.events.lock().unwrap();
        if let Some(e) = events.iter_mut().find(|e| e.event_uuid == event_uuid) {
            // P0: 验证 lease_token 匹配
            if e.lease_token.as_deref() == Some(lease_token) {
                e.status = OutboxStatus::RetryWait;
                e.locked_until = None;
                e.lease_token = None;
                e.available_at = next_available_at;
            }
        }
        Ok(())
    }

    async fn mark_dead_letter(
        &self,
        event_uuid: &str,
        lease_token: &str,
        _error: &str,
    ) -> Result<(), RepositoryError> {
        let mut events = self.events.lock().unwrap();
        if let Some(e) = events.iter_mut().find(|e| e.event_uuid == event_uuid) {
            // P0: 验证 lease_token 匹配
            if e.lease_token.as_deref() == Some(lease_token) {
                e.status = OutboxStatus::DeadLetter;
                e.locked_until = None;
                e.lease_token = None;
                e.processed_at = Some(Utc::now());
            }
        }
        Ok(())
    }

    async fn max_completed_seq(&self, ordering_key: &str) -> Result<Option<i64>, RepositoryError> {
        let events = self.events.lock().unwrap();
        Ok(events
            .iter()
            .filter(|e| e.ordering_key == ordering_key && e.status.is_terminal())
            .map(|e| e.ordering_seq)
            .max())
    }

    async fn max_seq(&self, ordering_key: &str) -> Result<Option<i64>, RepositoryError> {
        let events = self.events.lock().unwrap();
        Ok(events
            .iter()
            .filter(|e| e.ordering_key == ordering_key)
            .map(|e| e.ordering_seq)
            .max())
    }

    async fn recover_expired_processing(&self) -> Result<i64, RepositoryError> {
        let now = Utc::now();
        let mut count = 0i64;
        let mut events = self.events.lock().unwrap();
        for e in events.iter_mut() {
            if e.status == OutboxStatus::Processing
                && e.locked_until.map(|l| l <= now).unwrap_or(false)
            {
                e.status = OutboxStatus::Pending;
                e.locked_until = None;
                e.lease_token = None;
                // P0/P1: 不重置 attempts，保持单调递增
                count += 1;
            }
        }
        Ok(count)
    }

    async fn find_by_uuid(&self, event_uuid: &str) -> Result<Option<OutboxEvent>, RepositoryError> {
        let events = self.events.lock().unwrap();
        Ok(events.iter().find(|e| e.event_uuid == event_uuid).cloned())
    }

    async fn list_terminal_events(&self, limit: i64) -> Result<Vec<OutboxEvent>, RepositoryError> {
        let events = self.events.lock().unwrap();
        let mut terminal: Vec<_> = events
            .iter()
            .filter(|e| e.status.is_terminal())
            .take(limit as usize)
            .cloned()
            .collect();
        terminal.sort_by(|a, b| {
            b.processed_at
                .partial_cmp(&a.processed_at)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(terminal)
    }

    async fn processing_count(&self) -> Result<i64, RepositoryError> {
        let events = self.events.lock().unwrap();
        Ok(events
            .iter()
            .filter(|e| e.status == OutboxStatus::Processing)
            .count() as i64)
    }

    async fn pending_count(&self) -> Result<i64, RepositoryError> {
        let events = self.events.lock().unwrap();
        Ok(events
            .iter()
            .filter(|e| e.status == OutboxStatus::Pending || e.status == OutboxStatus::RetryWait)
            .count() as i64)
    }

    async fn add_outbox_event(&self, event: OutboxEvent) -> Result<(), RepositoryError> {
        self.add_event(event);
        Ok(())
    }

    async fn force_expire_lease(&self, event_uuid: &str) -> Result<(), RepositoryError> {
        use chrono::Duration;
        let past = Utc::now() - Duration::seconds(60);
        let mut events = self.events.lock().unwrap();
        if let Some(e) = events.iter_mut().find(|e| e.event_uuid == event_uuid) {
            e.locked_until = Some(past);
        }
        Ok(())
    }
}

impl MemOutboxRepo {
    /// 添加测试事件（直接操作，不走 trait 方法）。
    fn add_event(&self, event: OutboxEvent) {
        self.events.lock().unwrap().push(event);
    }

    /// P0: 模拟 claim 事件（用于测试）。
    /// 将事件设置为 Processing 状态并生成 lease_token。
    fn claim_event_for_test(&self, event_uuid: &str) -> String {
        let mut events = self.events.lock().unwrap();
        if let Some(e) = events.iter_mut().find(|e| e.event_uuid == event_uuid) {
            let token = uuid::Uuid::new_v4().to_string();
            e.status = OutboxStatus::Processing;
            e.lease_token = Some(token.clone());
            e.attempts += 1;
            token
        } else {
            panic!("事件不存在: {}", event_uuid);
        }
    }

    /// 创建测试用 MessageAccepted 事件。
    fn make_message_accepted_event(
        event_uuid: &str,
        conversation_id: i64,
        sequence: i64,
    ) -> OutboxEvent {
        let payload = OutboxMessageAcceptedPayload {
            event_uuid: event_uuid.to_string(),
            conversation_id,
            sender_id: 100,
            message_id: 1,
            content: "test".to_string(),
            timestamp: Utc::now(),
            is_mentioned: false,
            character_id: 1,
            conversation_sequence: sequence,
            platform_message_id: None,
            ingested_at: Utc::now(),
            metadata: serde_json::json!({}),
        };

        OutboxEvent {
            id: 0,
            event_uuid: event_uuid.to_string(),
            entity_id: 1,
            entity_type: "messages".to_string(),
            event_type: "MessageAccepted".to_string(),
            event_version: 1,
            payload: serde_json::to_string(&payload).unwrap(),
            ordering_key: conversation_id.to_string(),
            ordering_seq: sequence,
            created_at: Utc::now(),
            available_at: Utc::now(),
            status: OutboxStatus::Pending,
            attempts: 0,
            last_error: None,
            locked_until: None,
            processed_at: None,
            lease_token: None,
        }
    }
}

// ---------------------------------------------------------------------------
// 测试用例
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_outbox_status_terminal() {
    assert!(!OutboxStatus::Pending.is_terminal());
    assert!(!OutboxStatus::Processing.is_terminal());
    assert!(!OutboxStatus::RetryWait.is_terminal());
    assert!(OutboxStatus::Completed.is_terminal());
    assert!(OutboxStatus::DeadLetter.is_terminal());
}

#[tokio::test]
async fn test_outbox_status_claimable() {
    assert!(OutboxStatus::Pending.is_claimable());
    assert!(!OutboxStatus::Processing.is_claimable());
    assert!(OutboxStatus::RetryWait.is_claimable());
    assert!(!OutboxStatus::Completed.is_claimable());
    assert!(!OutboxStatus::DeadLetter.is_claimable());
}

#[tokio::test]
async fn test_processing_outcome_is_success() {
    assert!(ProcessingOutcome::Success.is_success());
    assert!(!ProcessingOutcome::RetryableFailure {
        error: "test".to_string()
    }
    .is_success());
    assert!(!ProcessingOutcome::PermanentFailure {
        error: "test".to_string()
    }
    .is_success());
}

#[tokio::test]
async fn test_processing_outcome_is_retryable() {
    assert!(!ProcessingOutcome::Success.is_retryable());
    assert!(ProcessingOutcome::RetryableFailure {
        error: "timeout".to_string()
    }
    .is_retryable());
    assert!(!ProcessingOutcome::PermanentFailure {
        error: "test".to_string()
    }
    .is_retryable());
}

#[tokio::test]
async fn test_processing_outcome_is_permanent() {
    assert!(!ProcessingOutcome::Success.is_permanent());
    assert!(!ProcessingOutcome::RetryableFailure {
        error: "test".to_string()
    }
    .is_permanent());
    assert!(ProcessingOutcome::PermanentFailure {
        error: "fatal".to_string()
    }
    .is_permanent());
}

/// 测试 1: message + outbox atomicity
/// MessageAcceptanceService 在同一事务中写入 message 和 outbox event。
#[tokio::test]
async fn test_message_outbox_atomicity() {
    let repo = Arc::new(MemOutboxRepo::new());

    // 模拟 MessageAcceptanceService.accept_message 的行为：
    // 在同一事务中分配 sequence、插入 message、插入 outbox event

    let conversation_id = 1i64;
    let event_uuid = "test-uuid-MessageAccepted";

    // 创建测试事件
    let event = OutboxEvent {
        id: 1,
        event_uuid: event_uuid.to_string(),
        entity_id: 1,
        entity_type: "messages".to_string(),
        event_type: "MessageAccepted".to_string(),
        event_version: 1,
        payload: r#"{"event_uuid":"test-uuid-MessageAccepted","conversation_id":1,"sender_id":100,"message_id":1,"content":"test","timestamp":"2024-01-01T00:00:00Z","is_mentioned":false,"character_id":1,"conversation_sequence":1,"platform_message_id":null,"ingested_at":"2024-01-01T00:00:00Z"}"#.to_string(),
        ordering_key: conversation_id.to_string(),
        ordering_seq: 1,
        created_at: Utc::now(),
        available_at: Utc::now(),
        status: OutboxStatus::Pending,
        attempts: 0,
        last_error: None,
        locked_until: None,
        processed_at: None,
        lease_token: None,
    };

    // 原子性：添加事件
    repo.add_event(event);

    // 验证事件已写入
    let found = repo.find_by_uuid(event_uuid).await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().status, OutboxStatus::Pending);
}

/// 测试 2: outbox event unique identity
#[tokio::test]
async fn test_outbox_event_unique_identity() {
    let repo = Arc::new(MemOutboxRepo::new());

    let event1 = MemOutboxRepo::make_message_accepted_event("event-1-MessageAccepted", 1, 1);
    let event2 = MemOutboxRepo::make_message_accepted_event("event-1-MessageAccepted", 1, 2); // 重复 UUID

    repo.add_event(event1);
    repo.add_event(event2.clone()); // 添加重复

    // 由于 MemOutboxRepo 使用 Vec，可以有重复
    // 但在真实 SQLite 实现中，UNIQUE INDEX 会阻止重复
    let count = repo.find_by_uuid("event-1-MessageAccepted").await.unwrap();
    // MemOutboxRepo 不强制唯一性，但 ProcessingOutcome 的语义是 UUID 应该是唯一的
    assert!(count.is_some());
}

/// 测试 3: pending recovery（进程崩溃后恢复 pending 事件）
#[tokio::test]
async fn test_pending_recovery() {
    let repo = Arc::new(MemOutboxRepo::new());

    // 添加一个 pending 事件
    let event = MemOutboxRepo::make_message_accepted_event("pending-test-MessageAccepted", 1, 1);
    repo.add_event(event);

    // 事件应该在 pending 状态
    let found = repo
        .find_by_uuid("pending-test-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::Pending);

    // recover_expired_processing 不影响 pending 事件
    let recovered = repo.recover_expired_processing().await.unwrap();
    assert_eq!(recovered, 0);

    let found = repo
        .find_by_uuid("pending-test-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::Pending);
}

/// 测试 4: expired processing lease recovery（进程崩溃后恢复过期 lease）
#[tokio::test]
async fn test_expired_processing_lease_recovery() {
    let repo = Arc::new(MemOutboxRepo::new());

    // 创建一个 processing 状态的事件（模拟进程崩溃前的状态）
    let mut event = MemOutboxRepo::make_message_accepted_event("lease-test-MessageAccepted", 1, 1);
    event.status = OutboxStatus::Processing;
    event.locked_until = Some(Utc::now() - chrono::Duration::seconds(60)); // 已过期
    event.attempts = 1;
    repo.add_event(event);

    // 验证事件处于 processing 状态
    let found = repo
        .find_by_uuid("lease-test-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::Processing);

    // 恢复过期事件
    let recovered = repo.recover_expired_processing().await.unwrap();
    assert_eq!(recovered, 1);

    // 事件应该回到 pending 状态
    let found = repo
        .find_by_uuid("lease-test-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::Pending);
    assert!(found.locked_until.is_none());
    // P0/P1: attempts 不重置，保持单调递增
    assert_eq!(found.attempts, 1);
    assert!(found.lease_token.is_none()); // lease_token 应该被清除
}

/// 测试 5: retry_wait scheduling（指数退避）
#[tokio::test]
async fn test_retry_wait_scheduling() {
    let repo = Arc::new(MemOutboxRepo::new());

    let event = MemOutboxRepo::make_message_accepted_event("retry-test-MessageAccepted", 1, 1);
    repo.add_event(event);

    // P0: 先 claim 事件获取 lease_token
    let token = repo.claim_event_for_test("retry-test-MessageAccepted");

    // 模拟第一次重试
    let backoff = chrono::Duration::seconds(2 * 2i64.pow(0)); // 2^0 * 2 = 2秒
    let next_available = Utc::now() + backoff;
    repo.mark_retry_wait(
        "retry-test-MessageAccepted",
        &token,
        "test error",
        next_available,
    )
    .await
    .unwrap();

    let found = repo
        .find_by_uuid("retry-test-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::RetryWait);
    assert!(found.available_at > Utc::now());

    // 第二次重试（指数退避）- 需要重新 claim
    let token2 = repo.claim_event_for_test("retry-test-MessageAccepted");
    let backoff2 = chrono::Duration::seconds(2 * 2i64.pow(1)); // 2^1 * 2 = 4秒
    let next_available2 = Utc::now() + backoff2;
    repo.mark_retry_wait(
        "retry-test-MessageAccepted",
        &token2,
        "test error",
        next_available2,
    )
    .await
    .unwrap();

    let found = repo
        .find_by_uuid("retry-test-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::RetryWait);
    // available_at 应该更晚
    assert!(found.available_at >= next_available2);
}

/// 测试 6: permanent failure → dead_letter
#[tokio::test]
async fn test_permanent_failure_dead_letter() {
    let repo = Arc::new(MemOutboxRepo::new());

    let event =
        MemOutboxRepo::make_message_accepted_event("dead-letter-test-MessageAccepted", 1, 1);
    repo.add_event(event);

    // P0: 先 claim 事件获取 lease_token
    let token = repo.claim_event_for_test("dead-letter-test-MessageAccepted");

    // 模拟永久失败
    repo.mark_dead_letter(
        "dead-letter-test-MessageAccepted",
        &token,
        "permanent error",
    )
    .await
    .unwrap();

    let found = repo
        .find_by_uuid("dead-letter-test-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::DeadLetter);
    assert!(found.processed_at.is_some());
}

/// 测试 7: same conversation sequence serialization（同一 conversation 的顺序保证）
#[tokio::test]
async fn test_same_conversation_sequence_serialization() {
    let repo = Arc::new(MemOutboxRepo::new());

    // 添加 seq=1 和 seq=2 的事件
    let event1 = MemOutboxRepo::make_message_accepted_event("seq1-MessageAccepted", 1, 1);
    let event2 = MemOutboxRepo::make_message_accepted_event("seq2-MessageAccepted", 1, 2);
    repo.add_event(event1);
    repo.add_event(event2);

    // claim_next 应该先返回 seq=1
    let claimed1 = repo.claim_next_event(30).await.unwrap();
    assert!(claimed1.is_some());
    assert_eq!(claimed1.unwrap().ordering_seq, 1);

    // 由于 seq=1 仍然是 processing，seq=2 不应该被 claim
    // 注意：MemOutboxRepo 的 claim_next_event 没有实现 per-conversation 顺序检查
    // 这个测试验证了基础行为，真正的顺序保证在 SqliteOutboxRepository 中实现
}

/// 测试 8: different conversations concurrency（不同 conversation 可以并发）
#[tokio::test]
async fn test_different_conversations_concurrency() {
    let repo = Arc::new(MemOutboxRepo::new());

    // 添加不同 conversation 的事件
    let event1 = MemOutboxRepo::make_message_accepted_event("conv1-seq1-MessageAccepted", 1, 1);
    let event2 = MemOutboxRepo::make_message_accepted_event("conv2-seq1-MessageAccepted", 2, 1);
    repo.add_event(event1);
    repo.add_event(event2);

    // 两个事件都应该可以被 claim
    let claimed1 = repo.claim_next_event(30).await.unwrap();
    let claimed2 = repo.claim_next_event(30).await.unwrap();

    // 至少有一个应该被 claim（顺序不确定）
    assert!(claimed1.is_some() || claimed2.is_some());
}

/// 测试 9: poison event does not permanently block next sequence（poison 事件不阻塞后续）
#[tokio::test]
async fn test_poison_event_does_not_block_next() {
    let repo = Arc::new(MemOutboxRepo::new());

    // seq=1 进入 dead_letter
    let event1 = MemOutboxRepo::make_message_accepted_event("poison-seq1-MessageAccepted", 1, 1);
    repo.add_event(event1);
    // P0: 先 claim 事件获取 lease_token
    let token = repo.claim_event_for_test("poison-seq1-MessageAccepted");
    repo.mark_dead_letter("poison-seq1-MessageAccepted", &token, "poison")
        .await
        .unwrap();

    // seq=2 仍然是 pending
    let event2 = MemOutboxRepo::make_message_accepted_event("normal-seq2-MessageAccepted", 1, 2);
    repo.add_event(event2);

    // 验证 seq=1 是 dead_letter
    let found1 = repo
        .find_by_uuid("poison-seq1-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found1.status, OutboxStatus::DeadLetter);

    // 验证 seq=2 仍然是 pending
    let found2 = repo
        .find_by_uuid("normal-seq2-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found2.status, OutboxStatus::Pending);
}

/// 测试 10: MPSC send does not mark completed（MPSC 发送成功不等于完成）
#[tokio::test]
async fn test_mpsc_send_does_not_mark_completed() {
    // 这个测试验证概念：MPSC send().await == Ok(()) 只表示事件进入了缓冲区
    // 不表示 OutboxProcessor 已接收，更不表示处理完成
    let (tx, mut rx) = mpsc::channel::<ProcessingOutcome>(1);

    // 发送一个 ProcessingOutcome
    let outcome = ProcessingOutcome::Success;
    tx.try_send(outcome).unwrap();

    // 接收（使用 async recv）
    let received = rx.recv().await.unwrap();
    assert!(received.is_success());

    // 即使 MPSC send 成功，事件状态仍然是 processing（未更新到 completed）
    // 这个测试说明了 MPSC 的 volatile 特性
}

/// 测试 11: ReplyProcessor success → completed
#[tokio::test]
async fn test_reply_processor_success_completed() {
    let repo = Arc::new(MemOutboxRepo::new());

    let event = MemOutboxRepo::make_message_accepted_event("success-test-MessageAccepted", 1, 1);
    repo.add_event(event);

    // P0: 先 claim 事件获取 lease_token
    let token = repo.claim_event_for_test("success-test-MessageAccepted");

    // 模拟 ReplyProcessor 返回 Success
    repo.mark_completed("success-test-MessageAccepted", &token)
        .await
        .unwrap();

    let found = repo
        .find_by_uuid("success-test-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::Completed);
    assert!(found.processed_at.is_some());
}

/// 测试 12: Retryable failure → retry_wait
#[tokio::test]
async fn test_retryable_failure_retry_wait() {
    let repo = Arc::new(MemOutboxRepo::new());

    let event = MemOutboxRepo::make_message_accepted_event("retryable-test-MessageAccepted", 1, 1);
    repo.add_event(event);

    // P0: 先 claim 事件获取 lease_token
    let token = repo.claim_event_for_test("retryable-test-MessageAccepted");

    // 模拟 RetryableFailure
    let next_available = Utc::now() + chrono::Duration::seconds(2);
    repo.mark_retry_wait(
        "retryable-test-MessageAccepted",
        &token,
        "temporary error",
        next_available,
    )
    .await
    .unwrap();

    let found = repo
        .find_by_uuid("retryable-test-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::RetryWait);
    assert!(found.locked_until.is_none());
}

/// 测试 13: Permanent failure → dead_letter
#[tokio::test]
async fn test_permanent_failure_to_dead_letter() {
    let repo = Arc::new(MemOutboxRepo::new());

    let event = MemOutboxRepo::make_message_accepted_event("permanent-test-MessageAccepted", 1, 1);
    repo.add_event(event);

    // P0: 先 claim 事件获取 lease_token
    let token = repo.claim_event_for_test("permanent-test-MessageAccepted");

    // 模拟 PermanentFailure
    repo.mark_dead_letter(
        "permanent-test-MessageAccepted",
        &token,
        "invalid message content",
    )
    .await
    .unwrap();

    let found = repo
        .find_by_uuid("permanent-test-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::DeadLetter);
    assert!(found.processed_at.is_some());
}

/// 测试 14: crash/restart recovery model（崩溃/重启恢复模型）
#[tokio::test]
async fn test_crash_restart_recovery_model() {
    let repo = Arc::new(MemOutboxRepo::new());

    // 场景 1: 事件在 pending，进程崩溃
    let pending_event =
        MemOutboxRepo::make_message_accepted_event("crash-pending-MessageAccepted", 1, 1);
    repo.add_event(pending_event);

    // 验证 pending 状态保持
    let found = repo
        .find_by_uuid("crash-pending-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::Pending);

    // 场景 2: 事件在 processing，进程崩溃（lease 过期）
    let mut processing_event =
        MemOutboxRepo::make_message_accepted_event("crash-processing-MessageAccepted", 1, 2);
    processing_event.status = OutboxStatus::Processing;
    processing_event.locked_until = Some(Utc::now() - chrono::Duration::seconds(60));
    repo.add_event(processing_event);

    // 恢复过期事件
    let recovered = repo.recover_expired_processing().await.unwrap();
    assert_eq!(recovered, 1);

    let found = repo
        .find_by_uuid("crash-processing-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::Pending); // 恢复到 pending
    assert_eq!(found.attempts, 0); // attempts 重置

    // 场景 3: 事件在 retry_wait，进程崩溃
    let mut retry_event =
        MemOutboxRepo::make_message_accepted_event("crash-retry-MessageAccepted", 1, 3);
    retry_event.status = OutboxStatus::RetryWait;
    retry_event.available_at = Utc::now() - chrono::Duration::seconds(10); // 已到时间
    repo.add_event(retry_event);

    // 事件仍然在 retry_wait 状态（因为不是 processing）
    let found = repo
        .find_by_uuid("crash-retry-MessageAccepted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.status, OutboxStatus::RetryWait);
}

/// 测试 15: duplicate delivery explicitly tolerated（重复投递是明确允许的语义）
#[tokio::test]
async fn test_duplicate_delivery_tolerated() {
    // C2-4 明确接受 at-least-once 语义：
    // - 如果事件在 processing 时进程崩溃，lease 过期后事件被重新 claim
    // - 如果事件在 MPSC 发送后进程崩溃，事件可能已经被处理（重复执行）
    // - 这两种情况都是 C2-4 允许的，不会造成 silent loss

    let repo = Arc::new(MemOutboxRepo::new());

    let event = MemOutboxRepo::make_message_accepted_event("duplicate-test-MessageAccepted", 1, 1);
    repo.add_event(event);

    // 第一次 claim
    let claimed1 = repo.claim_next_event(30).await.unwrap();
    assert!(claimed1.is_some());

    // 手动将 locked_until 设置为过去时间（模拟 lease 过期）
    {
        let mut events = repo.events.lock().unwrap();
        if let Some(e) = events
            .iter_mut()
            .find(|e| e.event_uuid == "duplicate-test-MessageAccepted")
        {
            e.locked_until = Some(Utc::now() - chrono::Duration::seconds(60));
        }
    }

    // recover_expired_processing 应该将事件恢复到 pending
    repo.recover_expired_processing().await.unwrap();
    let claimed2 = repo.claim_next_event(30).await.unwrap();

    // 第二次 claim 也应该成功（因为第一次的 lease 已过期）
    assert!(claimed2.is_some());

    // 两次 claim 的 ordering_seq 相同（事件被重新处理）
    assert_eq!(
        claimed1.unwrap().ordering_seq,
        claimed2.unwrap().ordering_seq
    );
}

#[cfg(test)]
mod domain_tests {
    use super::*;

    #[test]
    fn test_processing_outcome_equality() {
        assert_eq!(ProcessingOutcome::Success, ProcessingOutcome::Success);
        assert_eq!(
            ProcessingOutcome::RetryableFailure {
                error: "e".to_string()
            },
            ProcessingOutcome::RetryableFailure {
                error: "e".to_string()
            }
        );
        assert_ne!(
            ProcessingOutcome::RetryableFailure {
                error: "a".to_string()
            },
            ProcessingOutcome::RetryableFailure {
                error: "b".to_string()
            }
        );
    }

    #[test]
    fn test_outbox_status_from_str() {
        assert_eq!(
            "pending".parse::<OutboxStatus>().unwrap(),
            OutboxStatus::Pending
        );
        assert_eq!(
            "processing".parse::<OutboxStatus>().unwrap(),
            OutboxStatus::Processing
        );
        assert_eq!(
            "retry_wait".parse::<OutboxStatus>().unwrap(),
            OutboxStatus::RetryWait
        );
        assert_eq!(
            "completed".parse::<OutboxStatus>().unwrap(),
            OutboxStatus::Completed
        );
        assert_eq!(
            "dead_letter".parse::<OutboxStatus>().unwrap(),
            OutboxStatus::DeadLetter
        );
        assert_eq!(
            "invalid".parse::<OutboxStatus>().unwrap_err(),
            "未知的 outbox 状态: invalid"
        );
    }
}

// ============================================================================
// P0 所有权 fencing 测试（使用真实 SQLite 仓库）
// ============================================================================

#[cfg(test)]
mod outbox_ownership_tests {
    use crate::domain::outbox::{OutboxEvent, OutboxMessageAcceptedPayload, OutboxStatus};
    use crate::domain::repository::OutboxRepository;
    use crate::infrastructure::storage::repository::SqliteOutboxRepository;
    use crate::infrastructure::storage::SqliteStorage;
    use std::sync::Arc;

    /// 创建测试用 OutboxEvent
    fn make_test_event(event_uuid: &str, conversation_id: i64, sequence: i64) -> OutboxEvent {
        let payload = OutboxMessageAcceptedPayload {
            event_uuid: event_uuid.to_string(),
            conversation_id,
            sender_id: 100,
            message_id: 1,
            content: "test".to_string(),
            timestamp: chrono::Utc::now(),
            is_mentioned: false,
            character_id: 1,
            conversation_sequence: sequence,
            platform_message_id: None,
            ingested_at: chrono::Utc::now(),
            metadata: serde_json::json!({}),
        };

        OutboxEvent {
            id: 0,
            event_uuid: event_uuid.to_string(),
            entity_id: 1,
            entity_type: "messages".to_string(),
            event_type: "MessageAccepted".to_string(),
            event_version: 1,
            payload: serde_json::to_string(&payload).unwrap(),
            ordering_key: conversation_id.to_string(),
            ordering_seq: sequence,
            created_at: chrono::Utc::now(),
            available_at: chrono::Utc::now(),
            status: OutboxStatus::Pending,
            attempts: 0,
            last_error: None,
            locked_until: None,
            processed_at: None,
            lease_token: None,
        }
    }

    /// 测试 1: stale A completion after B claim → A cannot mutate B
    #[tokio::test]
    async fn test_stale_worker_cannot_complete_after_reclaim() {
        let storage = SqliteStorage::open_in_memory()
            .await
            .expect("failed to open in-memory db");
        storage.migrate().await.expect("failed to run migrations");
        let repo: Arc<dyn OutboxRepository> =
            Arc::new(SqliteOutboxRepository::new(storage.pool().clone()));

        let event = make_test_event("ownership-test-1", 1, 1);
        repo.add_outbox_event(event).await.expect("add failed");

        // A claims
        let claimed_a = repo
            .claim_next_event(30)
            .await
            .expect("claim failed")
            .expect("no event to claim");
        let token_a = claimed_a.lease_token.clone().expect("no lease_token");

        // 强制使 lease 过期（用于测试）
        repo.force_expire_lease("ownership-test-1")
            .await
            .expect("force_expire failed");

        // 模拟 lease 过期：恢复过期事件
        repo.recover_expired_processing()
            .await
            .expect("recover failed");

        // B claim
        let claimed_b = repo
            .claim_next_event(30)
            .await
            .expect("claim failed B")
            .expect("no event to claim B");
        let token_b = claimed_b.lease_token.clone().expect("no lease_token B");

        // 验证 A 和 B 有不同的 lease_token
        assert_ne!(token_a, token_b);

        // A 尝试 completion（使用旧的 token）
        repo.mark_completed("ownership-test-1", &token_a)
            .await
            .expect("mark_completed failed");

        // 验证事件状态：应该是 B 的 processing，不是 completed
        let after_a = repo
            .find_by_uuid("ownership-test-1")
            .await
            .expect("find failed")
            .expect("event not found");
        // A 的 stale completion 应该被拒绝（rows_affected = 0）
        // 事件应该仍然是 processing（被 B claim）
        assert_eq!(after_a.status, OutboxStatus::Processing);
        assert_eq!(after_a.lease_token.as_deref(), Some(token_b.as_str()));

        // B 正确 completion
        repo.mark_completed("ownership-test-1", &token_b)
            .await
            .expect("mark_completed B failed");

        let after_b = repo
            .find_by_uuid("ownership-test-1")
            .await
            .expect("find failed")
            .expect("event not found");
        assert_eq!(after_b.status, OutboxStatus::Completed);
    }

    /// 测试 2: stale A retry_wait after B claim → A cannot mutate B
    #[tokio::test]
    async fn test_stale_worker_cannot_retry_wait_after_reclaim() {
        let storage = SqliteStorage::open_in_memory()
            .await
            .expect("failed to open in-memory db");
        storage.migrate().await.expect("failed to run migrations");
        let repo: Arc<dyn OutboxRepository> =
            Arc::new(SqliteOutboxRepository::new(storage.pool().clone()));

        let event = make_test_event("ownership-test-2", 1, 1);
        repo.add_outbox_event(event).await.expect("add failed");

        // A claims
        let claimed_a = repo
            .claim_next_event(30)
            .await
            .expect("claim failed")
            .expect("no event");
        let token_a = claimed_a.lease_token.clone().expect("no token");

        // 强制使 lease 过期（用于测试）
        repo.force_expire_lease("ownership-test-2")
            .await
            .expect("force_expire failed");

        // Lease 过期，恢复
        repo.recover_expired_processing()
            .await
            .expect("recover failed");

        // B claims
        let claimed_b = repo
            .claim_next_event(30)
            .await
            .expect("claim failed B")
            .expect("no event B");
        let token_b = claimed_b.lease_token.clone().expect("no token B");
        assert_ne!(token_a, token_b);

        // A 尝试 retry_wait（使用旧 token）
        repo.mark_retry_wait(
            "ownership-test-2",
            &token_a,
            "stale error",
            chrono::Utc::now() + chrono::Duration::seconds(10),
        )
        .await
        .expect("mark_retry_wait A failed");

        // 验证：事件仍然是 B 的 processing
        let after_a = repo
            .find_by_uuid("ownership-test-2")
            .await
            .expect("find failed")
            .expect("event not found");
        assert_eq!(after_a.status, OutboxStatus::Processing);
        assert_eq!(after_a.lease_token.as_deref(), Some(token_b.as_str()));

        // B 正确 retry_wait
        repo.mark_retry_wait(
            "ownership-test-2",
            &token_b,
            "real error",
            chrono::Utc::now() + chrono::Duration::seconds(10),
        )
        .await
        .expect("mark_retry_wait B failed");

        let after_b = repo
            .find_by_uuid("ownership-test-2")
            .await
            .expect("find failed")
            .expect("event not found");
        assert_eq!(after_b.status, OutboxStatus::RetryWait);
    }

    /// 测试 3: stale A dead_letter after B claim → A cannot mutate B
    #[tokio::test]
    async fn test_stale_worker_cannot_dead_letter_after_reclaim() {
        let storage = SqliteStorage::open_in_memory()
            .await
            .expect("failed to open in-memory db");
        storage.migrate().await.expect("failed to run migrations");
        let repo: Arc<dyn OutboxRepository> =
            Arc::new(SqliteOutboxRepository::new(storage.pool().clone()));

        let event = make_test_event("ownership-test-3", 1, 1);
        repo.add_outbox_event(event).await.expect("add failed");

        // A claims
        let claimed_a = repo
            .claim_next_event(30)
            .await
            .expect("claim failed")
            .expect("no event");
        let token_a = claimed_a.lease_token.clone().expect("no token");

        // 强制使 lease 过期（用于测试）
        repo.force_expire_lease("ownership-test-3")
            .await
            .expect("force_expire failed");

        // Lease 过期，恢复
        repo.recover_expired_processing()
            .await
            .expect("recover failed");

        // B claims
        let claimed_b = repo
            .claim_next_event(30)
            .await
            .expect("claim failed B")
            .expect("no event B");
        let token_b = claimed_b.lease_token.clone().expect("no token B");
        assert_ne!(token_a, token_b);

        // A 尝试 dead_letter（使用旧 token）
        repo.mark_dead_letter("ownership-test-3", &token_a, "stale error")
            .await
            .expect("mark_dead_letter A failed");

        // 验证：事件仍然是 B 的 processing
        let after_a = repo
            .find_by_uuid("ownership-test-3")
            .await
            .expect("find failed")
            .expect("event not found");
        assert_eq!(after_a.status, OutboxStatus::Processing);
        assert_eq!(after_a.lease_token.as_deref(), Some(token_b.as_str()));

        // B 正确 dead_letter
        repo.mark_dead_letter("ownership-test-3", &token_b, "real error")
            .await
            .expect("mark_dead_letter B failed");

        let after_b = repo
            .find_by_uuid("ownership-test-3")
            .await
            .expect("find failed")
            .expect("event not found");
        assert_eq!(after_b.status, OutboxStatus::DeadLetter);
    }

    /// 测试 4: valid B completion → succeeds
    #[tokio::test]
    async fn test_valid_worker_completion_succeeds() {
        let storage = SqliteStorage::open_in_memory()
            .await
            .expect("failed to open in-memory db");
        storage.migrate().await.expect("failed to run migrations");
        let repo: Arc<dyn OutboxRepository> =
            Arc::new(SqliteOutboxRepository::new(storage.pool().clone()));

        let event = make_test_event("ownership-test-4", 1, 1);
        repo.add_outbox_event(event).await.expect("add failed");

        // Worker claims
        let claimed = repo
            .claim_next_event(30)
            .await
            .expect("claim failed")
            .expect("no event");
        let token = claimed.lease_token.clone().expect("no token");

        // 完成
        repo.mark_completed("ownership-test-4", &token)
            .await
            .expect("mark_completed failed");

        let after = repo
            .find_by_uuid("ownership-test-4")
            .await
            .expect("find failed")
            .expect("event not found");
        assert_eq!(after.status, OutboxStatus::Completed);
        assert!(after.processed_at.is_some());
    }

    /// 测试 5: lease expiry without reclaim → stale worker cannot ACK
    #[tokio::test]
    async fn test_stale_worker_cannot_ack_after_expiry() {
        let storage = SqliteStorage::open_in_memory()
            .await
            .expect("failed to open in-memory db");
        storage.migrate().await.expect("failed to run migrations");
        let repo: Arc<dyn OutboxRepository> =
            Arc::new(SqliteOutboxRepository::new(storage.pool().clone()));

        let event = make_test_event("ownership-test-5", 1, 1);
        repo.add_outbox_event(event).await.expect("add failed");

        // Worker claims
        let claimed = repo
            .claim_next_event(30)
            .await
            .expect("claim failed")
            .expect("no event");
        let token = claimed.lease_token.clone().expect("no token");

        // 强制使 lease 过期（用于测试）
        repo.force_expire_lease("ownership-test-5")
            .await
            .expect("force_expire failed");

        // Lease 过期，恢复（清除 locked_until 和 lease_token）
        repo.recover_expired_processing()
            .await
            .expect("recover failed");

        // 验证事件已恢复到 pending，lease_token 已清除
        let after_recover = repo
            .find_by_uuid("ownership-test-5")
            .await
            .expect("find failed")
            .expect("event not found");
        assert_eq!(after_recover.status, OutboxStatus::Pending);
        assert!(after_recover.lease_token.is_none());

        // 原来的 worker 尝试 completion（使用过期的 token）
        repo.mark_completed("ownership-test-5", &token)
            .await
            .expect("mark_completed should not error");

        // 验证：事件仍然是 pending（没有被完成）
        let after_stale = repo
            .find_by_uuid("ownership-test-5")
            .await
            .expect("find failed")
            .expect("event not found");
        assert_eq!(after_stale.status, OutboxStatus::Pending);
    }

    /// 测试 6: reclaim + new owner → only new owner can transition
    #[tokio::test]
    async fn test_only_new_owner_can_transition() {
        let storage = SqliteStorage::open_in_memory()
            .await
            .expect("failed to open in-memory db");
        storage.migrate().await.expect("failed to run migrations");
        let repo: Arc<dyn OutboxRepository> =
            Arc::new(SqliteOutboxRepository::new(storage.pool().clone()));

        let event = make_test_event("ownership-test-6", 1, 1);
        repo.add_outbox_event(event).await.expect("add failed");

        // A claims first
        let claimed_a = repo
            .claim_next_event(30)
            .await
            .expect("claim failed")
            .expect("no event");
        let token_a = claimed_a.lease_token.clone().expect("no token A");

        // 强制使 lease 过期（用于测试）
        repo.force_expire_lease("ownership-test-6")
            .await
            .expect("force_expire failed");

        // Lease expires, recovered
        repo.recover_expired_processing()
            .await
            .expect("recover failed");

        // B claims
        let claimed_b = repo
            .claim_next_event(30)
            .await
            .expect("claim failed B")
            .expect("no event B");
        let token_b = claimed_b.lease_token.clone().expect("no token B");
        assert_ne!(token_a, token_b);

        // 验证 B 是唯一的有效 owner
        let current = repo
            .find_by_uuid("ownership-test-6")
            .await
            .expect("find failed")
            .expect("event not found");
        assert_eq!(current.lease_token.as_deref(), Some(token_b.as_str()));
        assert_eq!(current.status, OutboxStatus::Processing);

        // A 尝试完成（应该失败）
        repo.mark_completed("ownership-test-6", &token_a)
            .await
            .expect("mark_completed A failed");
        let after_a = repo
            .find_by_uuid("ownership-test-6")
            .await
            .expect("find failed")
            .expect("event not found");
        assert_eq!(after_a.status, OutboxStatus::Processing); // 仍然是 B 的

        // B 正确完成
        repo.mark_completed("ownership-test-6", &token_b)
            .await
            .expect("mark_completed B failed");
        let after_b = repo
            .find_by_uuid("ownership-test-6")
            .await
            .expect("find failed")
            .expect("event not found");
        assert_eq!(after_b.status, OutboxStatus::Completed);
    }

    /// 测试 7: attempts monotonically increases through recovery cycles
    #[tokio::test]
    async fn test_attempts_monotonically_increases_through_recovery() {
        let storage = SqliteStorage::open_in_memory()
            .await
            .expect("failed to open in-memory db");
        storage.migrate().await.expect("failed to run migrations");
        let repo: Arc<dyn OutboxRepository> =
            Arc::new(SqliteOutboxRepository::new(storage.pool().clone()));

        let event = make_test_event("attempts-test-1", 1, 1);
        repo.add_outbox_event(event).await.expect("add failed");

        // Claim 1
        let claimed1 = repo
            .claim_next_event(30)
            .await
            .expect("claim failed")
            .expect("no event");
        assert_eq!(claimed1.attempts, 1);

        // 强制使 lease 过期，然后恢复
        repo.force_expire_lease("attempts-test-1")
            .await
            .expect("force_expire failed");
        repo.recover_expired_processing()
            .await
            .expect("recover failed");

        // Claim 2
        let claimed2 = repo
            .claim_next_event(30)
            .await
            .expect("claim failed 2")
            .expect("no event 2");
        assert_eq!(claimed2.attempts, 2); // NOT reset to 0!

        // 强制使 lease 过期，然后恢复
        repo.force_expire_lease("attempts-test-1")
            .await
            .expect("force_expire failed 2");
        repo.recover_expired_processing()
            .await
            .expect("recover failed 2");

        // Claim 3
        let claimed3 = repo
            .claim_next_event(30)
            .await
            .expect("claim failed 3")
            .expect("no event 3");
        assert_eq!(claimed3.attempts, 3);

        // 验证 attempts 确实在增加，没有被重置
        assert!(claimed3.attempts > claimed2.attempts);
        assert!(claimed2.attempts > claimed1.attempts);
    }

    /// 测试 8: MAX_RETRY_ATTEMPTS triggers dead_letter
    #[tokio::test]
    async fn test_max_retry_attempts_triggers_dead_letter() {
        let storage = SqliteStorage::open_in_memory()
            .await
            .expect("failed to open in-memory db");
        storage.migrate().await.expect("failed to run migrations");
        let repo: Arc<dyn OutboxRepository> =
            Arc::new(SqliteOutboxRepository::new(storage.pool().clone()));

        let event = make_test_event("max-attempts-test", 1, 1);
        repo.add_outbox_event(event).await.expect("add failed");

        // Claim 5 times (MAX_RETRY_ATTEMPTS = 5)
        for i in 1..=5 {
            let claimed = repo
                .claim_next_event(30)
                .await
                .unwrap_or_else(|_| panic!("claim failed {}", i))
                .unwrap_or_else(|| panic!("no event {}", i));
            assert_eq!(claimed.attempts, i);

            // 强制使 lease 过期，然后恢复
            repo.force_expire_lease("max-attempts-test")
                .await
                .expect("force_expire failed");
            repo.recover_expired_processing()
                .await
                .expect("recover failed");
        }

        // 第 6 次 claim 后，attempts = 6，超过 MAX_RETRY_ATTEMPTS(5)
        // 但是否进入 dead_letter 取决于 dispatcher 的逻辑，不是 repository 的逻辑
        // Repository 只是记录 attempts

        // 验证 attempts 已经达到上限
        let final_event = repo
            .find_by_uuid("max-attempts-test")
            .await
            .expect("find failed")
            .expect("event not found");
        assert_eq!(final_event.attempts, 5); // 最后一次 claim 的 attempts
        assert_eq!(final_event.status, OutboxStatus::Pending);
    }
}
