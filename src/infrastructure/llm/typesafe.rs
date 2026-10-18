//! TypeSafe System One Provider。
//!
//! Jev 不使用聊天补全协议，而是接收 state 和 typed questions，返回结构化 answers。
//! 请求仍经过统一 LlmScheduler，因此保留现有并发、额度和备用模型策略。

use std::time::Duration;

use async_trait::async_trait;

use crate::error::RuntimeError;
use crate::infrastructure::llm::{LlmProvider, LlmRequest, LlmResponse, TokenUsage};

/// TypeSafe Provider 的 HTTP 配置。
pub struct TypeSafeProvider {
    base_url: String,
    api_key: Option<String>,
    timeout: Duration,
    client: reqwest::Client,
}

impl TypeSafeProvider {
    pub fn new(base_url: String, api_key: Option<String>, timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|error| {
                tracing::warn!(target = "llm", error = %error, "构建 TypeSafe HTTP 客户端失败，使用默认客户端");
                reqwest::Client::new()
            });
        Self {
            base_url,
            api_key,
            timeout,
            client,
        }
    }

    fn endpoint(&self) -> String {
        let base = self.base_url.trim_end_matches('/');
        if base.ends_with("/v1/systemone") {
            base.to_string()
        } else {
            format!("{base}/v1/systemone")
        }
    }

    fn model_name(request: &LlmRequest) -> String {
        match request.model.as_deref().map(str::trim) {
            Some(model) if model.eq_ignore_ascii_case("jev") => "jev-latest".to_string(),
            Some(model) if !model.is_empty() => model.to_string(),
            _ => "jev-latest".to_string(),
        }
    }

    fn build_body(&self, request: &LlmRequest) -> Result<serde_json::Value, RuntimeError> {
        request
            .metadata
            .get("typesafe_request")
            .cloned()
            .map(|mut body| {
                if let Some(object) = body.as_object_mut() {
                    object.insert("model".into(), serde_json::json!(Self::model_name(request)));
                }
                body
            })
            .ok_or_else(|| RuntimeError::Config("TypeSafe 请求缺少 typesafe_request 元数据".into()))
    }
}

#[async_trait]
impl LlmProvider for TypeSafeProvider {
    fn name(&self) -> &str {
        "TypeSafeProvider"
    }

    async fn generate(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        let body = self.build_body(&request)?;
        let mut builder = self
            .client
            .post(self.endpoint())
            .timeout(self.timeout)
            .json(&body);
        if let Some(key) = &self.api_key {
            builder = builder.bearer_auth(key);
        }

        let response = builder
            .send()
            .await
            .map_err(|error| RuntimeError::HttpError(format!("请求 TypeSafe 服务失败: {error}")))?;
        let status = response.status();
        let raw = response
            .text()
            .await
            .map_err(|error| RuntimeError::HttpError(format!("读取 TypeSafe 响应失败: {error}")))?;
        if !status.is_success() {
            let message = format!("TypeSafe 服务返回错误 {status}: {raw}");
            return if status.is_server_error() || status.as_u16() == 429 || status.as_u16() == 529 {
                Err(RuntimeError::HttpError(message))
            } else {
                Err(RuntimeError::Llm(message))
            };
        }

        let value: serde_json::Value = serde_json::from_str(&raw)
            .map_err(|error| RuntimeError::Llm(format!("解析 TypeSafe 响应失败: {error}")))?;
        let usage = value.get("usage");
        let input_tokens = usage
            .and_then(|value| value.get("input_tokens"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as u32;
        let output_tokens = usage
            .and_then(|value| value.get("output_tokens"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as u32;
        Ok(LlmResponse {
            content: value.to_string(),
            model: value
                .get("model")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("jev-latest")
                .to_string(),
            usage: TokenUsage {
                prompt_tokens: input_tokens,
                completion_tokens: output_tokens,
                total_tokens: input_tokens.saturating_add(output_tokens),
            },
            truncated: false,
        })
    }

    async fn health_check(&self) -> Result<bool, RuntimeError> {
        Ok(true)
    }

    async fn embed(&self, _: Vec<String>) -> Result<Vec<Vec<f32>>, RuntimeError> {
        Err(RuntimeError::Config(
            "TypeSafe Provider 不提供 embedding 接口".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::TypeSafeProvider;
    use crate::infrastructure::llm::LlmRequest;
    use serde_json::json;
    use std::time::Duration;

    fn provider(base_url: &str) -> TypeSafeProvider {
        TypeSafeProvider::new(base_url.into(), None, Duration::from_secs(1))
    }

    #[test]
    fn endpoint_accepts_root_or_system_one_url() {
        assert_eq!(
            provider("https://api.typesafe.ai").endpoint(),
            "https://api.typesafe.ai/v1/systemone"
        );
        assert_eq!(
            provider("https://api.typesafe.ai/v1/systemone/").endpoint(),
            "https://api.typesafe.ai/v1/systemone"
        );
    }

    #[test]
    fn body_preserves_typed_questions_and_normalizes_jev_alias() {
        let request = LlmRequest {
            system: None,
            messages: Vec::new(),
            model: Some("Jev".into()),
            temperature: None,
            max_tokens: None,
            priority: 1,
            metadata: json!({
                "typesafe_request": {
                    "state": {"message": "你好"},
                    "model": "wrong",
                    "questions": {"action": {"type": "choice"}}
                }
            }),
        };
        let body = provider("https://api.typesafe.ai")
            .build_body(&request)
            .unwrap();
        assert_eq!(body["model"], "jev-latest");
        assert_eq!(body["questions"]["action"]["type"], "choice");
    }
}
