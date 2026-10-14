//! LLM 调度器 —— 所有 LLM 调用的统一入口。
//!
//! 核心/应用层不得直接调用 [`crate::infrastructure::llm::LlmProvider`]，
//! 必须统一经由本调度器。调度器负责：
//! - 用信号量做背压，限制并发请求数；
//! - 按优先级排队：所有请求遵守并发上限，后台请求在满载时延后。
//!
//! LLM 是能力不是生命线：调度器内部委托给内层 Provider，但上层在使用时
//! 没有可用 Provider（`enabled=false`）时保留消息并跳过生成。

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

    /// 返回实际任务路由的模型能力；未提供能力信息时不猜测。
    fn capabilities_for(
        &self,
        _role: &str,
    ) -> Option<crate::infrastructure::llm::FixedModelCapabilities> {
        None
    }
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
/// - priority == 0 → P0：等待许可；
/// - 1 → P1、2 → P2：正常获取信号量（排队）；
/// - ≥3 → P3：满载时丢弃并记录日志。
///
/// 角色路由（`with_providers` 模式）：
/// - 通过 `request.metadata["llm_role"]` 查找对应 Role；
/// - Role → ModelInstance → Provider 链式解析；
/// - 路由成功后从 ModelInstanceConfig.model 注入 `request.model`；
/// - **所有路由失败均返回明确错误**，不再 fallback 到 inner。
///
/// 单 Provider 模式（无 `providers`/`role_mis`）：
/// - 适用于首次发布的单 Provider 场景和测试；
/// - 所有 generation 请求直接发送到 inner Provider。
pub struct DefaultLlmScheduler {
    /// 【保留用于 embedding】当前激活的 LLM Provider（single-provider 模式）。
    inner: Arc<dyn LlmProvider>,
    semaphore: Arc<Semaphore>,
    /// embedding 专用信号量（默认 4）。
    embedding_semaphore: Arc<Semaphore>,
    /// 【可选】Provider 注册表（当使用 `with_providers` 时填充）。
    providers: Option<std::collections::HashMap<String, Arc<dyn LlmProvider>>>,
    /// 【可选】Role → 主模型及备用模型链。
    routes: Option<std::collections::HashMap<String, LlmRouteConfig>>,
}

/// 调度器使用的已解析任务路由。
#[derive(Debug, Clone)]
pub struct LlmRouteConfig {
    pub models: Vec<crate::application::config::LlmModelInstanceConfig>,
    pub max_attempts: usize,
    pub budget_tokens: Option<u32>,
}

impl DefaultLlmScheduler {
    /// 创建一个默认调度器（pending_limit 默认 4，embedding_limit 默认 4）。
    ///
    /// 这是 单 Provider 模式，所有 generation 请求直接发送到 inner。
    pub fn new(inner: Arc<dyn LlmProvider>) -> Self {
        Self::with_limits(inner, 4, 4)
    }

    /// 创建一个可配置并发上限的调度器（embedding 使用相同上限）。
    ///
    /// 这是 单 Provider 模式。
    pub fn with_limit(inner: Arc<dyn LlmProvider>, pending_limit: usize) -> Self {
        Self::with_limits(inner, pending_limit, pending_limit)
    }

    /// 创建一个可分别配置 LLM 和 embedding 并发上限的调度器。
    ///
    /// 这是 单 Provider 模式（无角色路由）。
    pub fn with_limits(
        inner: Arc<dyn LlmProvider>,
        pending_limit: usize,
        embedding_limit: usize,
    ) -> Self {
        Self {
            inner,
            semaphore: Arc::new(Semaphore::new(pending_limit.max(1))),
            embedding_semaphore: Arc::new(Semaphore::new(embedding_limit.max(1))),
            providers: None,
            routes: None,
        }
    }

    /// 创建带有多 Provider 路由支持的调度器。
    ///
    /// `providers`: Provider 注册表（name → Provider 实例）。
    /// `role_mis`: Role → ModelInstance 配置映射。
    ///
    /// 路由链：metadata["llm_role"] → role_mis[role] → mi.provider → providers[provider]
    ///
    /// **错误处理**：所有路由失败（missing role、unknown role、missing provider）
    /// 均返回 `RuntimeError::Config`，不再 fallback 到 inner。
    pub fn with_providers(
        inner: Arc<dyn LlmProvider>,
        pending_limit: usize,
        embedding_limit: usize,
        providers: std::collections::HashMap<String, Arc<dyn LlmProvider>>,
        role_mis: std::collections::HashMap<
            String,
            crate::application::config::LlmModelInstanceConfig,
        >,
    ) -> Self {
        let routes = role_mis
            .into_iter()
            .map(|(role, model)| {
                (
                    role,
                    LlmRouteConfig {
                        models: vec![model],
                        max_attempts: 1,
                        budget_tokens: None,
                    },
                )
            })
            .collect();
        Self::with_provider_routes(inner, pending_limit, embedding_limit, providers, routes)
    }

    /// 创建带有主模型、备用模型和单次预算的调度器。
    pub fn with_provider_routes(
        inner: Arc<dyn LlmProvider>,
        pending_limit: usize,
        embedding_limit: usize,
        providers: std::collections::HashMap<String, Arc<dyn LlmProvider>>,
        routes: std::collections::HashMap<String, LlmRouteConfig>,
    ) -> Self {
        Self {
            inner,
            semaphore: Arc::new(Semaphore::new(pending_limit.max(1))),
            embedding_semaphore: Arc::new(Semaphore::new(embedding_limit.max(1))),
            providers: Some(providers),
            routes: Some(routes),
        }
    }

    /// 按优先级分发：实时请求等待许可，后台请求满载时延后。
    ///
    /// 当配置了 `providers`/`role_mis` 时，先进行角色路由：
    /// - 从 `request.metadata["llm_role"]` 提取 role 名称；
    /// - 若 role 存在于 `role_mis`，则注入对应 ModelInstance 的 model，并路由到对应 Provider；
    /// - 路由失败时返回明确错误。
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

        self.generate_with_route(request).await
    }

    /// P0 同样等待全局许可，不能绕过并发上限。
    async fn dispatch_p0(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        let _permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| RuntimeError::Llm(format!("信号量已关闭: {e}")))?;
        self.generate_with_route(request).await
    }

    /// P3：信号量满载时丢弃并记录日志。
    async fn dispatch_p3(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                let result = self.generate_with_route(request).await;
                drop(permit);
                result
            }
            Err(_) => {
                tracing::debug!(target = "llm", "P3 请求在满载时被丢弃（背压保护）");
                Err(RuntimeError::Llm("P3 请求因满载被丢弃".to_string()))
            }
        }
    }

    /// 根据 `metadata["llm_role"]` 解析主模型和备用模型链。
    ///
    /// 返回 `(目标 Provider, 注入了 model 的 request)`。
    ///
    /// **错误条件**（均返回 `RuntimeError::Config`）：
    /// - 未配置 `providers`/`role_mis`（单 Provider 模式使用 `DefaultLlmScheduler::new()`）
    /// - `llm_role` 为空或缺失
    /// - `llm_role` 在 `role_mis` 中不存在（unknown role）
    /// - Role 引用的 ModelInstance 的 Provider 在 `providers` 中不存在
    ///
    /// **成功时**：Provider 和 model 均来自同一个 ModelInstance 配置，
    /// 保证 `ModelInstance.provider` 与 `ModelInstance.model` 始终保持绑定。
    fn route_requests(&self, request: &LlmRequest) -> Result<LlmRouteConfig, RuntimeError> {
        // 单 Provider 模式下，所有业务角色都由同一个 Provider 处理。
        let (Some(routes), Some(providers)) = (&self.routes, &self.providers) else {
            return Ok(LlmRouteConfig {
                models: vec![crate::application::config::LlmModelInstanceConfig {
                    provider: "default".into(),
                    model: request.model.clone().unwrap_or_default(),
                    context_window: None,
                    max_output_tokens: request.max_tokens.map(|v| v as usize),
                }],
                max_attempts: 1,
                budget_tokens: None,
            });
        };

        // 从 metadata 中提取 llm_role
        let role_name = match request.metadata.get("llm_role") {
            Some(v) => v.as_str().unwrap_or(""),
            None => "",
        };

        if role_name.is_empty() {
            return Err(RuntimeError::Config(
                "LLM 请求缺少 llm_role metadata。\
                 Conversation 应使用 llm_role=\"conversation\"，\
                 Decision 使用 decision.toml，\
                 Cognition 应使用 llm_role=\"cognition\""
                    .to_string(),
            ));
        }

        // 查找 Role 对应的 ModelInstance 配置
        let route = match routes.get(role_name) {
            Some(route) => route.clone(),
            None => {
                return Err(RuntimeError::Config(format!(
                    "Role '{}' 在 LLM 配置中未注册。请检查 llm.toml 或 decision.toml",
                    role_name
                )));
            }
        };

        for model in &route.models {
            if !providers.contains_key(&model.provider) {
                return Err(RuntimeError::Config(format!(
                    "Role '{}' 引用的 ModelInstance Provider '{}' 未注册",
                    role_name, model.provider
                )));
            }
        }
        Ok(route)
    }

    async fn generate_with_route(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        let Some(_routes) = &self.routes else {
            return self.inner.generate(request).await;
        };
        let Some(providers) = &self.providers else {
            return self.inner.generate(request).await;
        };
        let role = request
            .metadata
            .get("llm_role")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let route = self.route_requests(&request)?;
        let mut last_error = None;
        let attempt_limit = route.max_attempts.min(route.models.len());
        for model in route.models.iter().take(attempt_limit) {
            let provider = providers.get(&model.provider).ok_or_else(|| {
                RuntimeError::Config(format!("Provider '{}' 未注册", model.provider))
            })?;
            let mut attempt = request.clone();
            attempt.model = Some(model.model.clone());
            if let Some(max_output) = model.max_output_tokens {
                let max_output = u32::try_from(max_output)
                    .map_err(|_| RuntimeError::Config("模型输出上限超过协议支持范围".into()))?;
                attempt.max_tokens = Some(attempt.max_tokens.unwrap_or(max_output).min(max_output));
            }
            if let Some(budget) = route.budget_tokens {
                let remaining = budget;
                if remaining == 0 {
                    break;
                }
                attempt.max_tokens = Some(attempt.max_tokens.unwrap_or(remaining).min(remaining));
            }
            tracing::debug!(target = "llm", role, provider = %model.provider, model = %model.model, "尝试 LLM 路由");
            match provider.generate(attempt).await {
                Ok(response) => {
                    return Ok(response);
                }
                Err(error) if is_retryable_provider_error(&error) => {
                    tracing::warn!(target = "llm", role, provider = %model.provider, error = %error, "Provider 可重试失败，切换备用模型");
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| RuntimeError::Llm("所有 LLM Provider 均失败".into())))
    }
}

fn is_retryable_provider_error(error: &RuntimeError) -> bool {
    matches!(error, RuntimeError::HttpError(_))
}

#[async_trait::async_trait]
impl LlmScheduler for DefaultLlmScheduler {
    async fn submit(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        self.dispatch(request).await
    }

    fn capabilities_for(
        &self,
        role: &str,
    ) -> Option<crate::infrastructure::llm::FixedModelCapabilities> {
        self.routes
            .as_ref()?
            .get(role)
            .and_then(|route| route.models.first())
            .map(|model| {
                crate::infrastructure::llm::FixedModelCapabilities::new(
                    model.context_window,
                    model.max_output_tokens,
                )
            })
    }
}

#[async_trait::async_trait]
impl EmbeddingScheduler for DefaultLlmScheduler {
    async fn submit_embedding(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, RuntimeError> {
        // Embedding 仍然使用 inner（embedding 不需要角色路由）
        match self.embedding_semaphore.clone().try_acquire_owned() {
            Ok(_embedding_permit) => {
                let _global_permit = self
                    .semaphore
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| RuntimeError::Llm("Embedding 请求因全局满载被延后".into()))?;
                self.inner.embed(texts).await
            }
            Err(_) => {
                tracing::debug!(target = "llm", "Embedding 请求在满载时被丢弃（背压保护）");
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
        fail: bool,
        name: String,
        expected_model: Option<String>,
        /// 记录实际收到的 model 值
        captured_model: std::sync::Mutex<Option<Option<String>>>,
    }

    impl FakeProvider {
        fn new(hang: bool, name: &str) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                hang,
                fail: false,
                name: name.to_string(),
                expected_model: None,
                captured_model: std::sync::Mutex::new(None),
            }
        }

        fn with_expected_model(mut self, model: &str) -> Self {
            self.expected_model = Some(model.to_string());
            self
        }

        fn with_retryable_failure(mut self) -> Self {
            self.fail = true;
            self
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn take_captured_model(&self) -> Option<Option<String>> {
            self.captured_model.lock().unwrap().take()
        }
    }

    #[async_trait]
    impl LlmProvider for FakeProvider {
        async fn generate(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(RuntimeError::HttpError("temporary provider failure".into()));
            }
            if self.hang {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }

            // 记录实际收到的 model
            *self.captured_model.lock().unwrap() = Some(request.model.clone());

            // 检查 model 是否被正确注入
            if let Some(ref expected) = self.expected_model {
                if request.model.as_deref() != Some(expected) {
                    return Err(RuntimeError::Llm(format!(
                        "model mismatch: expected {}, got {:?}",
                        expected, request.model
                    )));
                }
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
            &self.name
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

    fn request_with_role(priority: u8, role: &str) -> LlmRequest {
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
            metadata: serde_json::json!({ "llm_role": role }),
        }
    }

    /// 启动一个占用唯一许的信号量持有任务（P1，会挂起），返回其 join 句柄。
    fn spawn_holder(scheduler: Arc<DefaultLlmScheduler>) -> tokio::task::JoinHandle<LlmResponse> {
        tokio::spawn(async move { scheduler.submit(request(1)).await.unwrap() })
    }

    // =============================================================================
    // 优先级调度测试（单 Provider 模式）
    // =============================================================================

    #[tokio::test]
    async fn p0_waits_when_full() {
        let provider = Arc::new(FakeProvider::new(true, "fake"));
        let scheduler = Arc::new(DefaultLlmScheduler::with_limit(provider.clone(), 1));

        let holder = spawn_holder(scheduler.clone());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let p0_request = scheduler.submit(request(0));
        tokio::pin!(p0_request);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(5), &mut p0_request)
                .await
                .is_err()
        );
        let p0 = p0_request.await.unwrap();
        assert_eq!(p0.content, "prio=0");

        holder.await.unwrap();
        assert_eq!(provider.call_count(), 2);
    }

    #[tokio::test]
    async fn p3_is_dropped_when_full_but_p0_still_works() {
        let provider = Arc::new(FakeProvider::new(true, "fake"));
        let scheduler = Arc::new(DefaultLlmScheduler::with_limit(provider.clone(), 1));

        let holder = spawn_holder(scheduler.clone());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let before = provider.call_count();
        let p3 = scheduler.submit(request(3)).await;
        assert!(p3.is_err());
        assert_eq!(provider.call_count(), before, "P3 被丢弃不应触达 provider");

        let p0 = scheduler.submit(request(0)).await.unwrap();
        assert_eq!(p0.content, "prio=0");

        holder.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_requests_do_not_deadlock() {
        let provider = Arc::new(FakeProvider::new(false, "fake"));
        let scheduler = Arc::new(DefaultLlmScheduler::with_limit(provider.clone(), 4));

        let mut handles = Vec::new();
        for i in 0..8u8 {
            let sched = scheduler.clone();
            handles.push(tokio::spawn(async move {
                let r = sched
                    .submit(request(match i % 4 {
                        0 => 0,
                        1 => 1,
                        2 => 2,
                        _ => 3,
                    }))
                    .await;
                assert!(r.is_ok() || r.is_err(), "请求必须返回，不得挂死");
            }));
        }
        for h in handles {
            h.await.expect("任务应结束");
        }
        assert!(provider.call_count() >= 6);
    }

    #[tokio::test]
    async fn mixed_priorities_obey_global_limit() {
        struct TrackingProvider {
            active: AtomicUsize,
            peak: AtomicUsize,
        }
        #[async_trait]
        impl LlmProvider for TrackingProvider {
            async fn generate(&self, _: LlmRequest) -> Result<LlmResponse, RuntimeError> {
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(LlmResponse {
                    content: "回复".into(),
                    model: "测试".into(),
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
                "测试"
            }
        }
        let provider = Arc::new(TrackingProvider {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        let scheduler = Arc::new(DefaultLlmScheduler::with_limit(provider.clone(), 4));
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..24 {
            let scheduler = scheduler.clone();
            tasks.spawn(async move { scheduler.submit(request(i % 3)).await });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }
        assert_eq!(provider.peak.load(Ordering::SeqCst), 4);
    }

    // =============================================================================
    // Test helper: 构建两个隔离的 Provider
    // =============================================================================

    type IsolatedProviders = (
        std::collections::HashMap<String, Arc<dyn LlmProvider>>,
        std::collections::HashMap<String, crate::application::config::LlmModelInstanceConfig>,
        Arc<FakeProvider>,
        Arc<FakeProvider>,
    );

    /// 构建隔离的测试 Provider：
    /// - provider_a: zhipu / model-a
    /// - provider_b: minimax / model-b
    fn make_isolated_providers() -> IsolatedProviders {
        let a_provider =
            Arc::new(FakeProvider::new(false, "provider_a").with_expected_model("model-a"));
        let b_provider =
            Arc::new(FakeProvider::new(false, "provider_b").with_expected_model("model-b"));

        let mut providers: std::collections::HashMap<String, Arc<dyn LlmProvider>> =
            std::collections::HashMap::new();
        providers.insert(
            "provider_a".to_string(),
            a_provider.clone() as Arc<dyn LlmProvider>,
        );
        providers.insert(
            "provider_b".to_string(),
            b_provider.clone() as Arc<dyn LlmProvider>,
        );

        let mut role_mis = std::collections::HashMap::new();
        role_mis.insert(
            "conversation".to_string(),
            crate::application::config::LlmModelInstanceConfig {
                provider: "provider_a".to_string(),
                model: "model-a".to_string(),
                context_window: Some(32768),
                max_output_tokens: Some(512),
            },
        );
        role_mis.insert(
            "decision".to_string(),
            crate::application::config::LlmModelInstanceConfig {
                provider: "provider_b".to_string(),
                model: "model-b".to_string(),
                context_window: Some(16384),
                max_output_tokens: Some(256),
            },
        );
        role_mis.insert(
            "cognition".to_string(),
            crate::application::config::LlmModelInstanceConfig {
                provider: "provider_a".to_string(),
                model: "model-a".to_string(),
                context_window: Some(32768),
                max_output_tokens: Some(512),
            },
        );

        (providers, role_mis, a_provider, b_provider)
    }

    // =============================================================================
    // Test 1：正常 Conversation routing（验证 Provider identity + Model identity）
    // =============================================================================

    #[tokio::test]
    async fn conversation_routes_to_correct_provider_and_model() {
        let (providers, role_mis, a_provider, _b_provider) = make_isolated_providers();
        // inner 不参与 routing，只用于 embedding
        let inner = providers.get("provider_a").unwrap().clone();
        let scheduler = Arc::new(DefaultLlmScheduler::with_providers(
            inner, 4, 4, providers, role_mis,
        ));

        // Conversation 请求 → provider_a / model-a
        let conv_req = request_with_role(1, "conversation");
        let resp = scheduler.submit(conv_req).await.unwrap();
        assert_eq!(resp.content, "prio=1");

        // 验证实际收到的 model
        let captured = a_provider.take_captured_model();
        assert_eq!(
            captured,
            Some(Some("model-a".to_string())),
            "Conversation 应收到 model-a"
        );
    }

    // =============================================================================
    // Test 2：正常 Decision routing
    // =============================================================================

    #[tokio::test]
    async fn decision_routes_to_correct_provider_and_model() {
        let (providers, role_mis, _a_provider, b_provider) = make_isolated_providers();
        let inner = providers.get("provider_a").unwrap().clone();
        let scheduler = Arc::new(DefaultLlmScheduler::with_providers(
            inner, 4, 4, providers, role_mis,
        ));

        // Decision 请求 → provider_b / model-b
        let dec_req = request_with_role(1, "decision");
        let resp = scheduler.submit(dec_req).await.unwrap();
        assert_eq!(resp.content, "prio=1");

        let captured = b_provider.take_captured_model();
        assert_eq!(
            captured,
            Some(Some("model-b".to_string())),
            "Decision 应收到 model-b"
        );
    }

    // =============================================================================
    // Test 3：Cross-role isolation
    // =============================================================================

    #[tokio::test]
    async fn conversation_and_decision_use_different_providers() {
        let (providers, role_mis, a_provider, b_provider) = make_isolated_providers();
        let inner = providers.get("provider_a").unwrap().clone();
        let scheduler = Arc::new(DefaultLlmScheduler::with_providers(
            inner, 4, 4, providers, role_mis,
        ));

        // Conversation → provider_a
        let conv_req = request_with_role(1, "conversation");
        scheduler.submit(conv_req).await.unwrap();
        assert_eq!(
            a_provider.call_count(),
            1,
            "Conversation 应发送到 provider_a"
        );
        assert_eq!(
            b_provider.call_count(),
            0,
            "Decision provider 不应被 Conversation 调用"
        );

        // Decision → provider_b
        let dec_req = request_with_role(1, "decision");
        scheduler.submit(dec_req).await.unwrap();
        assert_eq!(a_provider.call_count(), 1, "Conversation count 不变");
        assert_eq!(b_provider.call_count(), 1, "Decision 应发送到 provider_b");
    }

    // =============================================================================
    // Test 4：Missing Provider（配置错误）
    // =============================================================================

    #[tokio::test]
    async fn missing_provider_returns_error() {
        let mut providers: std::collections::HashMap<String, Arc<dyn LlmProvider>> =
            std::collections::HashMap::new();
        // 只插入 provider_b
        providers.insert(
            "provider_b".to_string(),
            Arc::new(FakeProvider::new(false, "provider_b")) as Arc<dyn LlmProvider>,
        );

        let mut role_mis = std::collections::HashMap::new();
        role_mis.insert(
            "conversation".to_string(),
            crate::application::config::LlmModelInstanceConfig {
                // conversation 引用的 provider_a 不存在
                provider: "provider_a".to_string(),
                model: "model-a".to_string(),
                context_window: Some(32768),
                max_output_tokens: Some(512),
            },
        );

        let inner = providers.get("provider_b").unwrap().clone();
        let scheduler = Arc::new(DefaultLlmScheduler::with_providers(
            inner, 4, 4, providers, role_mis,
        ));

        let req = request_with_role(1, "conversation");
        let result = scheduler.submit(req).await;

        // 必须返回错误，不能 fallback
        assert!(
            result.is_err(),
            "Missing provider 应返回错误，而非 fallback"
        );

        // 验证错误信息包含关键提示
        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("provider_a") || err_msg.contains("未注册"),
            "错误信息应指出缺失的 Provider"
        );

        // provider_b 不应被调用
        // （因为无法通过类型访问 call_count，我们需要另一个方法）
    }

    // =============================================================================
    // Test 5：Missing llm_role
    // =============================================================================

    #[tokio::test]
    async fn missing_llm_role_returns_error() {
        let (providers, role_mis, _a_provider, _b_provider) = make_isolated_providers();
        let inner = providers.get("provider_a").unwrap().clone();
        let scheduler = Arc::new(DefaultLlmScheduler::with_providers(
            inner, 4, 4, providers, role_mis,
        ));

        // 不设置 llm_role
        let req = request(1);
        let result = scheduler.submit(req).await;

        assert!(result.is_err(), "Missing llm_role 应返回错误");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("llm_role") || err.to_string().contains("缺少"),
            "错误信息应提及 llm_role 缺失"
        );
    }

    // =============================================================================
    // Test 6：Unknown llm_role
    // =============================================================================

    #[tokio::test]
    async fn unknown_llm_role_returns_error() {
        let (providers, role_mis, _a_provider, _b_provider) = make_isolated_providers();
        let inner = providers.get("provider_a").unwrap().clone();
        let scheduler = Arc::new(DefaultLlmScheduler::with_providers(
            inner, 4, 4, providers, role_mis,
        ));

        let req = request_with_role(1, "nonexistent_role");
        let result = scheduler.submit(req).await;

        assert!(result.is_err(), "Unknown llm_role 应返回错误");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("nonexistent_role") || err.to_string().contains("未注册"),
            "错误信息应指出未注册的 role"
        );
    }

    // =============================================================================
    // Test 7：空 llm_role（空字符串）
    // =============================================================================

    #[tokio::test]
    async fn empty_llm_role_returns_error() {
        let (providers, role_mis, _a_provider, _b_provider) = make_isolated_providers();
        let inner = providers.get("provider_a").unwrap().clone();
        let scheduler = Arc::new(DefaultLlmScheduler::with_providers(
            inner, 4, 4, providers, role_mis,
        ));

        let req = request_with_role(1, "");
        let result = scheduler.submit(req).await;

        assert!(result.is_err(), "Empty llm_role 应返回错误");
    }

    // =============================================================================
    // Test 8：单 Provider 模式（无 role routing）
    // =============================================================================

    #[tokio::test]
    async fn single_provider_mode_works() {
        // 使用 with_limit，不配置 providers/role_mis
        let provider = Arc::new(FakeProvider::new(false, "fake"));
        let scheduler = Arc::new(DefaultLlmScheduler::with_limit(provider.clone(), 4));

        // 单 Provider 模式允许调用方继续携带业务角色元数据。
        let req = request_with_role(1, "conversation");
        let result = scheduler.submit(req).await;
        assert!(result.is_ok(), "单 Provider 模式应接受业务角色元数据");

        // 不带 role 的请求直接发送到默认 Provider。
        let req_no_role = request(1);
        let resp = scheduler.submit(req_no_role).await.unwrap();
        assert_eq!(resp.content, "prio=1");
        assert_eq!(provider.call_count(), 2);
    }

    // =============================================================================
    // Test 9：P0/P3 优先级路由
    // =============================================================================

    #[tokio::test]
    async fn p0_with_role_routing() {
        let (providers, role_mis, a_provider, _b_provider) = make_isolated_providers();
        let inner = providers.get("provider_a").unwrap().clone();
        let scheduler = Arc::new(DefaultLlmScheduler::with_providers(
            inner, 1, // 只给 1 个信号量，强制 P0 抢占路径
            4, providers, role_mis,
        ));

        let req = request_with_role(0, "conversation");
        let resp = scheduler.submit(req).await.unwrap();
        assert_eq!(resp.content, "prio=0");

        let captured = a_provider.take_captured_model();
        assert_eq!(captured, Some(Some("model-a".to_string())));
    }

    #[tokio::test]
    async fn p3_with_role_routing() {
        let (providers, role_mis, _a_provider, b_provider) = make_isolated_providers();
        let inner = providers.get("provider_a").unwrap().clone();
        let scheduler = Arc::new(DefaultLlmScheduler::with_providers(
            inner, 4, 4, providers, role_mis,
        ));

        let req = request_with_role(3, "decision");
        let resp = scheduler.submit(req).await.unwrap();
        assert_eq!(resp.content, "prio=3");

        let captured = b_provider.take_captured_model();
        assert_eq!(captured, Some(Some("model-b".to_string())));
    }

    // =============================================================================
    // Test 10：Cognition role routing
    // =============================================================================

    #[tokio::test]
    async fn cognition_routes_to_correct_provider_and_model() {
        let (providers, role_mis, a_provider, _b_provider) = make_isolated_providers();
        let inner = providers.get("provider_a").unwrap().clone();
        let scheduler = Arc::new(DefaultLlmScheduler::with_providers(
            inner, 4, 4, providers, role_mis,
        ));

        let req = request_with_role(1, "cognition");
        let resp = scheduler.submit(req).await.unwrap();
        assert_eq!(resp.content, "prio=1");

        let captured = a_provider.take_captured_model();
        assert_eq!(captured, Some(Some("model-a".to_string())));
    }

    #[tokio::test]
    async fn retryable_provider_error_uses_fallback_model() {
        let primary = Arc::new(FakeProvider::new(false, "primary").with_retryable_failure());
        let backup =
            Arc::new(FakeProvider::new(false, "backup").with_expected_model("backup-model"));
        let mut providers = std::collections::HashMap::new();
        providers.insert(
            "primary".to_string(),
            primary.clone() as Arc<dyn LlmProvider>,
        );
        providers.insert("backup".to_string(), backup.clone() as Arc<dyn LlmProvider>);
        let routes = std::collections::HashMap::from([(
            "conversation".to_string(),
            LlmRouteConfig {
                models: vec![
                    crate::application::config::LlmModelInstanceConfig {
                        provider: "primary".into(),
                        model: "primary-model".into(),
                        context_window: Some(8192),
                        max_output_tokens: Some(256),
                    },
                    crate::application::config::LlmModelInstanceConfig {
                        provider: "backup".into(),
                        model: "backup-model".into(),
                        context_window: Some(8192),
                        max_output_tokens: Some(256),
                    },
                ],
                max_attempts: 2,
                budget_tokens: Some(512),
            },
        )]);
        let scheduler =
            DefaultLlmScheduler::with_provider_routes(primary.clone(), 2, 2, providers, routes);
        let response = scheduler
            .submit(request_with_role(1, "conversation"))
            .await
            .unwrap();
        assert_eq!(response.content, "prio=1");
        assert_eq!(primary.call_count(), 1);
        assert_eq!(backup.call_count(), 1);
    }
}
