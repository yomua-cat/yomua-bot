//! LLM 调度器 —— 所有 LLM 调用的统一入口。
//!
//! 核心/应用层不得直接调用 [`crate::infrastructure::llm::LlmProvider`]，
//! 必须统一经由本调度器。调度器负责：
//! - 用信号量做背压，限制并发请求数；
//! - 按优先级排队：P0 不被阻塞，P3 在满载时丢弃。
//!
//! LLM 是能力不是生命线：调度器内部委托给内层 Provider，但上层在使用时
//! 随时可以因为没有可用 Provider（`enabled=false`）而选择确定性回复。

use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::error::RuntimeError;
use crate::infrastructure::llm::{LlmProvider, LlmRequest, LlmResponse};

/// LLM 调度器 trait —— 应用层对 LLM 调用的统一抽象。
///
/// 让上层（认知层 / Reply Pipeline）在测试中可以用假实现替换。
#[async_trait::async_trait]
pub trait LlmScheduler: Send + Sync {
    /// 提交一个 LLM 请求并获得响应。
    async fn submit(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError>;
}

/// Embedding 调度器 trait —— 所有 embedding 调用的统一抽象。
///
/// 使用独立的信号量限制并发（默认 4），优先级固定为 P3（后台任务，满载丢弃）。
#[async_trait::async_trait]
pub trait EmbeddingScheduler: Send + Sync {
    /// 提交一个 embedding 请求并返回向量列表。
    async fn submit_embedding(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, RuntimeError>;
}

/// 基于 [`LlmProvider`] 的默认调度器实现。
///
/// 内部使用 `tokio::sync::Semaphore` 限制并发请求数（`pending_limit`，默认 4）。
/// 优先级映射：
/// - priority == 0 → P0：用 `try_acquire` 抢占；若信号量已满**也直接执行**，不被 P3 阻塞；
/// - 1 → P1、2 → P2：正常获取信号量（排队）；
/// - ≥3 → P3：满载时丢弃并记录日志。
pub struct DefaultLlmScheduler {
    inner: Arc<dyn LlmProvider>,
    semaphore: Arc<Semaphore>,
    /// embedding 专用信号量（默认 4）。
    embedding_semaphore: Arc<Semaphore>,
}

impl DefaultLlmScheduler {
    /// 创建一个默认调度器（pending_limit 默认 4，embedding_limit 默认 4）。
    pub fn new(inner: Arc<dyn LlmProvider>) -> Self {
        Self::with_limits(inner, 4, 4)
    }

    /// 创建一个可配置并发上限的调度器（embedding 使用相同上限）。
    pub fn with_limit(inner: Arc<dyn LlmProvider>, pending_limit: usize) -> Self {
        Self::with_limits(inner, pending_limit, pending_limit)
    }

    /// 创建一个可分别配置 LLM 和 embedding 并发上限的调度器。
    pub fn with_limits(
        inner: Arc<dyn LlmProvider>,
        pending_limit: usize,
        embedding_limit: usize,
    ) -> Self {
        Self {
            inner,
            semaphore: Arc::new(Semaphore::new(pending_limit.max(1))),
            embedding_semaphore: Arc::new(Semaphore::new(embedding_limit.max(1))),
        }
    }

    /// 按优先级分发：P0 直通（抢占，不被阻塞），P3 满即弃，其余排队。
    async fn dispatch(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        let priority = request.priority;

        if priority == 0 {
            return self.dispatch_p0(request).await;
        }

        if priority >= 3 {
            return self.dispatch_p3(request).await;
        }

        // P1 / P2：正常排队等待许可。
        let _permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| RuntimeError::Llm(format!("信号量已关闭: {e}")))?;
        self.inner.generate(request).await
    }

    /// P0：优先尝试获取许可；无论信号量是否已满都直接执行（绝不被 P3 阻塞）。
    async fn dispatch_p0(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                let result = self.inner.generate(request).await;
                drop(permit);
                result
            }
            Err(_) => self.inner.generate(request).await,
        }
    }

    /// P3：信号量满载时丢弃并记录日志。
    async fn dispatch_p3(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                let result = self.inner.generate(request).await;
                drop(permit);
                result
            }
            Err(_) => {
                tracing::debug!(target: "llm", "P3 请求在满载时被丢弃（背压保护）");
                Err(RuntimeError::Llm("P3 请求因满载被丢弃".to_string()))
            }
        }
    }
}

#[async_trait::async_trait]
impl LlmScheduler for DefaultLlmScheduler {
    async fn submit(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        self.dispatch(request).await
    }
}

#[async_trait::async_trait]
impl EmbeddingScheduler for DefaultLlmScheduler {
    async fn submit_embedding(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, RuntimeError> {
        // P3：满载时丢弃（背压保护）。
        match self.embedding_semaphore.clone().try_acquire_owned() {
            Ok(_permit) => self.inner.embed(texts).await,
            Err(_) => {
                tracing::debug!(target: "llm", "Embedding 请求在满载时被丢弃（背压保护）");
                Err(RuntimeError::Llm("Embedding 请求因满载被丢弃".to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::llm::{LlmMessage, LlmRole, TokenUsage};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 可编程的假 Provider：可选挂起（sleep）、记录调用次数、按优先级返回内容。
    struct FakeProvider {
        calls: AtomicUsize,
        hang: bool,
    }

    impl FakeProvider {
        fn new(hang: bool) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                hang,
            }
        }
        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl LlmProvider for FakeProvider {
        async fn generate(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.hang {
                // 挂起一小段时间，制造信号量被占用的窗口。
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Ok(LlmResponse {
                content: format!("prio={}", request.priority),
                model: "fake".to_string(),
                usage: TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                },
                truncated: false,
            })
        }

        async fn health_check(&self) -> Result<bool, RuntimeError> {
            Ok(true)
        }

        fn name(&self) -> &str {
            "fake"
        }
    }

    fn request(priority: u8) -> LlmRequest {
        LlmRequest {
            system: None,
            messages: vec![LlmMessage {
                role: LlmRole::User,
                content: "hi".to_string(),
            }],
            model: None,
            temperature: None,
            max_tokens: None,
            priority,
            metadata: serde_json::json!({}),
        }
    }

    /// 启动一个占用唯一许的信号量持有任务（P1，会挂起），返回其 join 句柄。
    ///
    /// 因为 Provider 挂起 50ms，调用方在返回后应立即提交目标请求，
    /// 从而在信号量仍被占用的窗口内观察行为。
    fn spawn_holder(scheduler: Arc<DefaultLlmScheduler>) -> tokio::task::JoinHandle<LlmResponse> {
        tokio::spawn(async move { scheduler.submit(request(1)).await.unwrap() })
    }

    #[tokio::test]
    async fn p0_is_not_blocked_when_full() {
        let provider = Arc::new(FakeProvider::new(true));
        let scheduler = Arc::new(DefaultLlmScheduler::with_limit(provider.clone(), 1));

        // 占用唯一许可（后台任务持有它 50ms）。
        let holder = spawn_holder(scheduler.clone());
        // 给后台任务一点时间真正拿到许可。
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // 信号量此刻已满；P0 仍应成功（不被阻塞）。
        let p0 = scheduler.submit(request(0)).await.unwrap();
        assert_eq!(p0.content, "prio=0");

        // 等持有者完成，确保一切正常收尾。
        holder.await.unwrap();

        // Provider 被调用了两次（一次 P1，一次 P0）。
        assert_eq!(provider.call_count(), 2);
    }

    #[tokio::test]
    async fn p3_is_dropped_when_full_but_p0_still_works() {
        let provider = Arc::new(FakeProvider::new(true));
        let scheduler = Arc::new(DefaultLlmScheduler::with_limit(provider.clone(), 1));

        // 占用唯一许可。
        let holder = spawn_holder(scheduler.clone());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // P3 满载 → 被丢弃（返回错误），且不调用底层 provider。
        let before = provider.call_count();
        let p3 = scheduler.submit(request(3)).await;
        assert!(p3.is_err());
        assert_eq!(provider.call_count(), before, "P3 被丢弃不应触达 provider");

        // P0 满载也能成功（不被 P3 阻塞语义）。
        let p0 = scheduler.submit(request(0)).await.unwrap();
        assert_eq!(p0.content, "prio=0");

        holder.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_requests_do_not_deadlock() {
        let provider = Arc::new(FakeProvider::new(false));
        let scheduler = Arc::new(DefaultLlmScheduler::with_limit(provider.clone(), 4));

        let mut handles = Vec::new();
        for i in 0..8u8 {
            let sched = scheduler.clone();
            handles.push(tokio::spawn(async move {
                // 混合优先级：0（P0）、1（P1）、2（P2）、3（P3）。
                let r = sched
                    .submit(request(match i % 4 {
                        0 => 0,
                        1 => 1,
                        2 => 2,
                        _ => 3,
                    }))
                    .await;
                // 不掉死亡：每个请求都必须返回结果（成功或明确失败）。
                assert!(r.is_ok() || r.is_err(), "请求必须返回，不得挂死");
            }));
        }
        for h in handles {
            h.await.expect("任务应结束");
        }
        // P0/P1/P2 共 6 个请求都绝不会被丢弃；P3 在满载时可能被丢弃，
        // 因此成功调用的数量至少为 6（可能更多，视竞争时序而定）。
        assert!(provider.call_count() >= 6);
    }
}
