//! 记忆维护后台任务。
//!
//! 该任务只处理程序可以证明安全的候选记忆。模型推断候选不会因为后台
//! 任务运行而被默认提升，避免把不确定内容伪装成事实。

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::application::control::replay_pending_memory_edit;
use crate::application::llm_scheduler::LlmScheduler;
use crate::domain::repository::MemoryRepository;

/// 候选记忆维护的默认间隔。
pub const MEMORY_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);

pub struct MemoryMaintenanceDriver {
    memory_repo: Arc<dyn MemoryRepository>,
    llm_scheduler: Option<Arc<dyn LlmScheduler>>,
    interval: Duration,
}

impl MemoryMaintenanceDriver {
    pub fn new(memory_repo: Arc<dyn MemoryRepository>) -> Self {
        Self {
            memory_repo,
            llm_scheduler: None,
            interval: MEMORY_MAINTENANCE_INTERVAL,
        }
    }

    /// 配置用于恢复自然语言记忆修改请求的模型调度器。
    pub fn with_llm_scheduler(mut self, scheduler: Arc<dyn LlmScheduler>) -> Self {
        self.llm_scheduler = Some(scheduler);
        self
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
        let Some(scheduler) = &self.llm_scheduler else {
            return Ok(promoted);
        };

        // pending 请求已经持久化；每次只取少量，避免后台恢复占满实时模型额度。
        let pending = self.memory_repo.find_pending_edit_requests(10).await?;
        let mut recovered = 0;
        for memory in pending {
            match replay_pending_memory_edit(self.memory_repo.as_ref(), scheduler.as_ref(), &memory)
                .await
            {
                Ok(true) => recovered += 1,
                Ok(false) => {}
                Err(error) => tracing::warn!(memory_id = memory.id, %error, "恢复记忆修改请求失败"),
            }
        }
        if recovered > 0 {
            tracing::info!(recovered, "已恢复挂起的记忆修改请求");
        }
        Ok(promoted + recovered)
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
