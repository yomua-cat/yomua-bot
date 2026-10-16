//! 记忆维护后台任务。
//!
//! 该任务只处理程序可以证明安全的候选记忆。模型推断候选不会因为后台
//! 任务运行而被默认提升，避免把不确定内容伪装成事实。

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::domain::repository::MemoryRepository;

/// 候选记忆维护的默认间隔。
pub const MEMORY_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);

pub struct MemoryMaintenanceDriver {
    memory_repo: Arc<dyn MemoryRepository>,
    interval: Duration,
}

impl MemoryMaintenanceDriver {
    pub fn new(memory_repo: Arc<dyn MemoryRepository>) -> Self {
        Self {
            memory_repo,
            interval: MEMORY_MAINTENANCE_INTERVAL,
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub async fn tick(&self) -> Result<u64, crate::error::RepositoryError> {
        let promoted = self.memory_repo.promote_safe_candidates(100).await?;
        if promoted > 0 {
            tracing::info!(promoted, "已确认可安全提升的候选记忆");
        }
        Ok(promoted)
    }

    pub async fn run_until_stopped(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        loop {
            if *stop.borrow() {
                break;
            }
            tokio::select! {
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.interval) => {
                    if let Err(error) = self.tick().await {
                        tracing::warn!(%error, "候选记忆维护失败");
                    }
                }
            }
        }
    }
}
