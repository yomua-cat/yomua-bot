//! Outbox Dispatcher —— C2-4 Durable Event Delivery 的核心调度器。
//!
//! 职责：
//! - 轮询 outbox_events 表，拾取 pending/retry_wait 事件
//! - 原子性地 claim 事件（带 lease，防止多 dispatcher 竞争）
//! - 通过 MPSC 通道将事件发送给 OutboxProcessor
//! - 等待 ACK 信号并根据 ProcessingOutcome 更新 outbox 状态
//! - 实现 per-conversation 严格顺序（同一 conversation 按 sequence 串行）
//! - 启动时恢复已过期的 processing 事件
//!
//! **MPSC 只是 volatile transport**：`mpsc.send().await` 成功只表示事件已进入通道缓冲区，
//! 不表示 EventProcessor 已接收，更不表示处理完成。真正的完成信号是 ACK 通道。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, timeout};
use tracing::{debug, error, info, warn};

use crate::domain::outbox::{OutboxEvent, ProcessingOutcome};
use crate::domain::repository::OutboxRepository;

/// Lease 超时时间（秒）。如果 EventProcessor 在此时间内未返回 ACK，事件可被重新 claim。
const DEFAULT_LEASE_DURATION_SECS: i64 = 30;

/// 重试最大次数。超过此次数后事件进入 dead_letter。
const MAX_RETRY_ATTEMPTS: i64 = 5;

/// 重试退避基数（秒）。实际退避时间 = BASE * 2^attempts。
const RETRY_BACKOFF_BASE_SECS: i64 = 2;

/// Outbox 调度器配置。
#[derive(Debug, Clone)]
pub struct OutboxDispatcherConfig {
    /// Lease 持续时间（秒）。
    pub lease_duration_secs: i64,
    /// 最大重试次数。
    pub max_retry_attempts: i64,
    /// 轮询间隔（毫秒）。
    pub poll_interval_ms: u64,
    /// 退避基数（秒）。
    pub backoff_base_secs: i64,
}

impl Default for OutboxDispatcherConfig {
    fn default() -> Self {
        Self {
            lease_duration_secs: DEFAULT_LEASE_DURATION_SECS,
            max_retry_attempts: MAX_RETRY_ATTEMPTS,
            poll_interval_ms: 100,
            backoff_base_secs: RETRY_BACKOFF_BASE_SECS,
        }
    }
}

/// Outbox 调度器。
///
/// 管理 outbox 事件的完整生命周期：
/// - pending → processing → completed
/// - processing → retry_wait → pending
/// - processing → dead_letter
pub struct OutboxDispatcher {
    repo: Arc<dyn OutboxRepository>,
    config: OutboxDispatcherConfig,
    /// 当前活跃的 ordering_key → max_completed_seq 映射。
    /// 用于 per-conversation 顺序控制。
    active_seqs: tokio::sync::RwLock<HashMap<String, i64>>,
}

impl OutboxDispatcher {
    /// 创建 Outbox 调度器。
    pub fn new(repo: Arc<dyn OutboxRepository>, config: OutboxDispatcherConfig) -> Self {
        Self {
            repo,
            config,
            active_seqs: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// 启动调度循环。
    ///
    /// 此方法会持续运行，直到 `shutdown_rx`收到关停信号。
    ///
    /// # Arguments
    /// * `event_tx` - MPSC sender，用于向 OutboxProcessor 发送事件
    /// * `shutdown_rx` - 关停信号接收器
    pub async fn run(
        &self,
        event_tx: mpsc::Sender<(OutboxEvent, oneshot::Sender<ProcessingOutcome>)>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        info!(target: "outbox", "OutboxDispatcher 启动");

        // 启动恢复：重置所有已过期的 processing 事件
        self.recover_expired_events().await;

        // 初始化 active_seqs：从 outbox 重建当前活跃的 ordering_key 状态
        self.init_active_seqs().await;

        let mut poll_interval = interval(Duration::from_millis(self.config.poll_interval_ms));

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!(target: "outbox", "OutboxDispatcher 收到关停信号，正在停止...");
                        break;
                    }
                }
                _ = poll_interval.tick() => {
                    // 每次 poll 最多处理少量事件，避免阻塞过久
                    for _ in 0..16 {
                        if let Err(_) = self.tick_once(&event_tx).await {
                            // shutdown 或错误，停止
                            break;
                        }
                    }
                }
            }
        }

        info!(target: "outbox", "OutboxDispatcher 已停止");
    }

    /// 单次轮询：尝试 claim 一个事件并发送。
    async fn tick_once(
        &self,
        event_tx: &mpsc::Sender<(OutboxEvent, oneshot::Sender<ProcessingOutcome>)>,
    ) -> Result<(), ()> {
        // 尝试 claim 下一个事件
        let event = match self.claim_next().await {
            Ok(Some(e)) => e,
            Ok(None) => return Ok(()), // 无可用事件
            Err(e) => {
                warn!(target: "outbox", error = %e, "claim_next 失败");
                return Ok(()); // 暂时忽略，等待下次 poll
            }
        };

        let event_uuid = event.event_uuid.clone();
        let ordering_key = event.ordering_key.clone();
        let ordering_seq = event.ordering_seq;
        // P0: 提取并保留 lease_token，用于后续的 ownership 验证
        let lease_token = event
            .lease_token
            .clone()
            .expect("claim 的事件必须包含 lease_token");

        debug!(
            target: "outbox",
            event_uuid = %event_uuid,
            ordering_key = %ordering_key,
            ordering_seq,
            "已 claim 事件，准备发送"
        );

        // 创建 oneshot 通道用于接收 ACK
        let (response_tx, response_rx) = oneshot::channel();

        // 发送到 MPSC（volatile transport）
        match event_tx.send((event.clone(), response_tx)).await {
            Ok(()) => {
                debug!(target: "outbox", event_uuid = %event_uuid, "事件已发送至 MPSC");
            }
            Err(e) => {
                // MPSC 通道已关闭（OutboxProcessor 已停止）
                error!(target: "outbox", event_uuid = %event_uuid, error = %e, "MPSC 发送失败，标记为可恢复");
                // 将事件状态重置为 pending，以便下次 poll 可以重新 claim
                // 注意：这可能导致事件被处理两次（如果事件实际上已被处理）
                if let Err(e) = self
                    .repo
                    .mark_retry_wait(
                        &event_uuid,
                        &lease_token,
                        &format!("MPSC send failed: {}", e),
                        chrono::Utc::now(),
                    )
                    .await
                {
                    warn!(target: "outbox", event_uuid = %event_uuid, error = %e, "重置事件状态失败");
                }
                return Err(()); // 停止轮询
            }
        };

        // 等待 ACK 或超时
        let ack_result = timeout(
            Duration::from_secs(self.config.lease_duration_secs as u64),
            response_rx,
        )
        .await;

        match ack_result {
            Ok(Ok(outcome)) => {
                // 收到 ACK，根据 outcome 更新状态
                self.handle_outcome(
                    &event_uuid,
                    &ordering_key,
                    ordering_seq,
                    &lease_token,
                    outcome,
                )
                .await;
            }
            Ok(Err(_)) => {
                // oneshot 发送方已 dropped（通常意味着 OutboxProcessor 已停止）
                warn!(target: "outbox", event_uuid = %event_uuid, "ACK 通道已关闭");
                // 将事件标记为可重新 claim
                if let Err(e) = self
                    .repo
                    .mark_retry_wait(
                        &event_uuid,
                        &lease_token,
                        "ACK channel closed",
                        chrono::Utc::now(),
                    )
                    .await
                {
                    warn!(target: "outbox", event_uuid = %event_uuid, error = %e, "更新事件状态失败");
                }
                return Err(());
            }
            Err(_) => {
                // ACK 超时：lease 已过期，事件可能被重新 claim
                warn!(
                    target: "outbox",
                    event_uuid = %event_uuid,
                    lease_duration_secs = self.config.lease_duration_secs,
                    "ACK 超时，事件 lease 已过期"
                );
                // 不在这里更新状态：lease 过期的事件会在下次 poll 时被恢复
                // 或者如果 EventProcessor 实际上处理成功了，会发送 ACK 但我们没收到
            }
        }

        Ok(())
    }

    /// 根据 ProcessingOutcome 更新 outbox 状态。
    async fn handle_outcome(
        &self,
        event_uuid: &str,
        ordering_key: &str,
        ordering_seq: i64,
        lease_token: &str,
        outcome: ProcessingOutcome,
    ) {
        match outcome {
            ProcessingOutcome::Success => {
                info!(
                    target: "outbox",
                    event_uuid = %event_uuid,
                    ordering_key = %ordering_key,
                    ordering_seq,
                    "事件处理成功"
                );
                if let Err(e) = self.repo.mark_completed(event_uuid, lease_token).await {
                    error!(target: "outbox", event_uuid = %event_uuid, error = %e, "标记 completed 失败");
                } else {
                    // 更新活跃 seq 映射
                    let mut seqs = self.active_seqs.write().await;
                    let current = seqs.get(ordering_key).copied().unwrap_or(0);
                    if ordering_seq > current {
                        seqs.insert(ordering_key.to_string(), ordering_seq);
                    }
                }
            }
            ProcessingOutcome::RetryableFailure { error } => {
                warn!(
                    target: "outbox",
                    event_uuid = %event_uuid,
                    ordering_key = %ordering_key,
                    ordering_seq,
                    error = %error,
                    "事件处理失败，准备重试"
                );

                // 检查重试次数
                if let Ok(Some(event)) = self.repo.find_by_uuid(event_uuid).await {
                    if event.attempts >= self.config.max_retry_attempts as i32 {
                        // 超过最大重试次数，进入 dead_letter
                        error!(
                            target: "outbox",
                            event_uuid = %event_uuid,
                            attempts = event.attempts,
                            "超过最大重试次数，标记为 dead_letter"
                        );
                        if let Err(e) = self
                            .repo
                            .mark_dead_letter(event_uuid, lease_token, &error)
                            .await
                        {
                            error!(target: "outbox", event_uuid = %event_uuid, error = %e, "标记 dead_letter 失败");
                        }
                        // 更新活跃 seq 映射（dead_letter 不阻塞后续）
                        let mut seqs = self.active_seqs.write().await;
                        let current = seqs.get(ordering_key).copied().unwrap_or(0);
                        if ordering_seq > current {
                            seqs.insert(ordering_key.to_string(), ordering_seq);
                        }
                        return;
                    }
                }

                // 计算下次可用时间（指数退避）
                let attempts = self
                    .repo
                    .find_by_uuid(event_uuid)
                    .await
                    .ok()
                    .flatten()
                    .map(|e| e.attempts)
                    .unwrap_or(1) as i64;
                let backoff_secs = self.config.backoff_base_secs * 2_i64.pow(attempts as u32);
                let next_available = chrono::Utc::now() + chrono::Duration::seconds(backoff_secs);

                if let Err(e) = self
                    .repo
                    .mark_retry_wait(event_uuid, lease_token, &error, next_available)
                    .await
                {
                    error!(target: "outbox", event_uuid = %event_uuid, error = %e, "标记 retry_wait 失败");
                }
            }
            ProcessingOutcome::PermanentFailure { error } => {
                error!(
                    target: "outbox",
                    event_uuid = %event_uuid,
                    ordering_key = %ordering_key,
                    ordering_seq,
                    error = %error,
                    "事件永久失败，标记为 dead_letter"
                );
                if let Err(e) = self
                    .repo
                    .mark_dead_letter(event_uuid, lease_token, &error)
                    .await
                {
                    error!(target: "outbox", event_uuid = %event_uuid, error = %e, "标记 dead_letter 失败");
                } else {
                    // 更新活跃 seq 映射（dead_letter 不阻塞后续）
                    let mut seqs = self.active_seqs.write().await;
                    let current = seqs.get(ordering_key).copied().unwrap_or(0);
                    if ordering_seq > current {
                        seqs.insert(ordering_key.to_string(), ordering_seq);
                    }
                }
            }
        }
    }

    /// 尝试 claim 下一个可处理的事件。
    async fn claim_next(&self) -> Result<Option<OutboxEvent>, crate::error::RuntimeError> {
        // 首先恢复所有已过期的 processing 事件
        if let Err(e) = self.repo.recover_expired_processing().await {
            warn!(target: "outbox", error = %e, "恢复过期事件失败");
        }

        // 尝试 claim
        let event = self
            .repo
            .claim_next_event(self.config.lease_duration_secs)
            .await
            .map_err(|e| crate::error::RuntimeError::Repository(e))?;

        if let Some(ref e) = event {
            debug!(
                target: "outbox",
                event_uuid = %e.event_uuid,
                ordering_key = %e.ordering_key,
                ordering_seq = e.ordering_seq,
                attempts = e.attempts,
                "claim 成功"
            );
        }

        Ok(event)
    }

    /// 恢复所有已过期的 processing 事件。
    async fn recover_expired_events(&self) {
        match self.repo.recover_expired_processing().await {
            Ok(n) if n > 0 => {
                info!(
                    target: "outbox",
                    count = n,
                    "已恢复 {n} 个过期 processing 事件",
                );
            }
            Ok(_) => {}
            Err(e) => {
                warn!(target: "outbox", error = %e, "恢复过期事件时出错");
            }
        }
    }

    /// 从 outbox 表重建活跃的 ordering_key → max_completed_seq 映射。
    async fn init_active_seqs(&self) {
        // 获取所有有 processing 或 pending/retry_wait 事件的 ordering_key
        // 对于每个 ordering_key，找到最大的 completed/dead_letter seq
        // 这个值就是当前活跃的 seq（后续事件应该从 seq+1 开始）

        // 简化处理：清空映射，让 claim_next 的子查询处理顺序检查
        // 在第一次 poll 时自动重建
        let mut seqs = self.active_seqs.write().await;
        seqs.clear();
        info!(target: "outbox", "active_seqs 已初始化");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = OutboxDispatcherConfig::default();
        assert_eq!(config.lease_duration_secs, DEFAULT_LEASE_DURATION_SECS);
        assert_eq!(config.max_retry_attempts, MAX_RETRY_ATTEMPTS);
        assert_eq!(config.poll_interval_ms, 100);
        assert_eq!(config.backoff_base_secs, RETRY_BACKOFF_BASE_SECS);
    }
}
