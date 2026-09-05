//! OpenAI-compatible LLM Provider。
//!
//! 通过 HTTP 调用兼容 OpenAI Chat Completions 协议的服务（包括
//! 多数本地推理服务如 Ollama、vLLM、LM Studio 等）。
//! 实现 [`crate::infrastructure::llm::LlmProvider`] trait。
//!
//! 采用 `reqwest`（rustls）作为 HTTP 客户端；单元测试不触网，
//! 只验证请求体组装与响应 JSON 解析。

use std::time::Duration;

use async_trait::async_trait;

use crate::error::RuntimeError;
use crate::infrastructure::llm::{LlmProvider, LlmRequest, LlmResponse, LlmRole, TokenUsage};

/// OpenAI-compatible Provider 的配置。
#[derive(Debug, Clone)]
pub struct OpenAiCompatibleConfig {
    /// 服务根地址（例如 `http://127.0.0.1:11434/v1`）。
    pub base_url: String,
    /// 可选的 API Key（存在则加 `Authorization: Bearer <key>`）。
    pub api_key: Option<String>,
    /// 使用的模型名称。
    pub model: String,
    /// 超时时间。
    pub timeout: Duration,
}

/// OpenAI-compatible 的具体实现。
pub struct OpenAiCompatibleProvider {
    config: OpenAiCompatibleConfig,
    client: reqwest::Client,
}

impl OpenAiCompatibleProvider {
    /// 创建一个 OpenAI-compatible Provider。
    pub fn new(config: OpenAiCompatibleConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .unwrap_or_else(|e| {
                // 构建失败极不常见；降级为无超时默认客户端并记录。
                tracing::warn!(target: "llm", error = %e, "构建 HTTP 客户端失败，使用默认值");
                reqwest::Client::new()
            });
        Self { config, client }
    }

    /// 组装 Chat Completions 请求 JSON body。
    ///
    /// - 若 `request.system` 有值，作为首条 `system` 消息；
    /// - 随后拼接 `request.messages`；
    /// - 若 `request.model` 有值则覆盖配置中的模型，否则用配置默认模型；
    /// - `temperature` / `max_tokens` 仅在请求方提供时设置；
    /// - `stream` 恒为 `false`。
    fn build_body(&self, request: &LlmRequest) -> serde_json::Value {
        let mut messages: Vec<serde_json::Value> = Vec::new();

        if let Some(system) = &request.system {
            messages.push(serde_json::json!({
                "role": "system",
                "content": system,
            }));
        }

        for m in &request.messages {
            messages.push(serde_json::json!({
                "role": role_str(m.role),
                "content": m.content,
            }));
        }

        let mut body = serde_json::json!({
            "model": request.model.clone().unwrap_or_else(|| self.config.model.clone()),
            "messages": messages,
            "stream": false,
        });

        if let Some(temperature) = request.temperature {
            body["temperature"] = serde_json::json!(temperature);
        }
        if let Some(max_tokens) = request.max_tokens {
            body["max_tokens"] = serde_json::json!(max_tokens);
        }

        body
    }

    /// 解析 Chat Completions 响应 JSON。
    ///
    /// 提取 `choices[0].message.content`；`finish_reason == "length"` 时置
    /// `truncated = true`。可选解析 `usage`。
    fn parse_response(&self, raw: &serde_json::Value) -> Result<LlmResponse, RuntimeError> {
        let choices = raw
            .get("choices")
            .and_then(|v| v.as_array())
            .ok_or_else(|| RuntimeError::Llm("响应缺少 choices".to_string()))?;

        let first = choices
            .first()
            .ok_or_else(|| RuntimeError::Llm("响应 choices 为空".to_string()))?;

        let content = first
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(|c| c.as_str())
            .ok_or_else(|| RuntimeError::Llm("响应缺少 message.content".to_string()))?
            .to_string();

        let truncated = first
            .get("finish_reason")
            .and_then(|f| f.as_str())
            .map(|f| f == "length")
            .unwrap_or(false);

        let model = raw
            .get("model")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_string();

        // 解析可选的 usage。
        let (prompt_tokens, completion_tokens, total_tokens) = raw
            .get("usage")
            .map(|u| {
                let prompt = u.get("prompt_tokens").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                let completion = u
                    .get("completion_tokens")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u32;
                let total =
                    u.get("total_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(prompt as u64 + completion as u64) as u32;
                (prompt, completion, total)
            })
            .unwrap_or((0, 0, 0));

        Ok(LlmResponse {
            content,
            model,
            usage: TokenUsage {
                prompt_tokens,
                completion_tokens,
                total_tokens,
            },
            truncated,
        })
    }
}

/// 将角色枚举转为 OpenAI 字符串。
fn role_str(role: LlmRole) -> &'static str {
    match role {
        LlmRole::System => "system",
        LlmRole::User => "user",
        LlmRole::Assistant => "assistant",
    }
}

#[async_trait]
impl LlmProvider for OpenAiCompatibleProvider {
    async fn generate(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
        let url = format!(
            "{}/chat/completions",
            self.config.base_url.trim_end_matches('/')
        );
        let body = self.build_body(&request);

        let mut builder = self.client.post(&url).json(&body);
        if let Some(key) = &self.config.api_key {
            builder = builder.bearer_auth(key);
        }

        let resp = builder
            .send()
            .await
            .map_err(|e| RuntimeError::Llm(format!("请求 OpenAI-compatible 服务失败: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(RuntimeError::Llm(format!(
                "OpenAI-compatible 服务返回错误 {status}: {text}"
            )));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| RuntimeError::Llm(format!("解析响应 JSON 失败: {e}")))?;

        self.parse_response(&body)
    }

    async fn health_check(&self) -> Result<bool, RuntimeError> {
        let url = format!("{}/models", self.config.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| RuntimeError::Llm(format!("健康检查失败: {e}")))?;
        Ok(resp.status().is_success())
    }

    fn name(&self) -> &str {
        "openai-compatible"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::llm::LlmMessage;

    fn provider() -> OpenAiCompatibleProvider {
        OpenAiCompatibleProvider::new(OpenAiCompatibleConfig {
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            api_key: Some("sk-test".to_string()),
            model: "qwen".to_string(),
            timeout: Duration::from_secs(30),
        })
    }

    #[test]
    fn build_body_assembles_system_and_messages() {
        let p = provider();
        let req = LlmRequest {
            system: Some("你是助手".to_string()),
            messages: vec![
                LlmMessage {
                    role: LlmRole::User,
                    content: "你好".to_string(),
                },
                LlmMessage {
                    role: LlmRole::Assistant,
                    content: "你好！".to_string(),
                },
            ],
            model: None,
            temperature: Some(0.7),
            max_tokens: Some(128),
            priority: 0,
            metadata: serde_json::json!({}),
        };

        let body = p.build_body(&req);
        assert_eq!(body["model"], "qwen");
        assert_eq!(body["stream"], false);
        assert_eq!(body["temperature"], 0.7);
        assert_eq!(body["max_tokens"], 128);

        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(messages[0]["content"], "你是助手");
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[2]["role"], "assistant");
    }

    #[test]
    fn build_body_request_model_overrides_config() {
        let p = provider();
        let req = LlmRequest {
            system: None,
            messages: vec![LlmMessage {
                role: LlmRole::User,
                content: "hi".to_string(),
            }],
            model: Some("other-model".to_string()),
            temperature: None,
            max_tokens: None,
            priority: 0,
            metadata: serde_json::json!({}),
        };
        let body = p.build_body(&req);
        assert_eq!(body["model"], "other-model");
        // 未提供 temperature / max_tokens 时不应出现在 body 中。
        assert!(body.get("temperature").is_none());
        assert!(body.get("max_tokens").is_none());
    }

    #[test]
    fn parse_response_extracts_content_and_usage() {
        let p = provider();
        let raw = serde_json::json!({
            "id": "chatcmpl-1",
            "model": "qwen",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "你好，很高兴见到你"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
        });

        let resp = p.parse_response(&raw).unwrap();
        assert_eq!(resp.content, "你好，很高兴见到你");
        assert_eq!(resp.model, "qwen");
        assert!(!resp.truncated);
        assert_eq!(resp.usage.prompt_tokens, 10);
        assert_eq!(resp.usage.completion_tokens, 5);
        assert_eq!(resp.usage.total_tokens, 15);
    }

    #[test]
    fn parse_response_marks_truncated_when_length() {
        let p = provider();
        let raw = serde_json::json!({
            "model": "qwen",
            "choices": [{
                "message": {"content": "..."},
                "finish_reason": "length"
            }]
        });
        let resp = p.parse_response(&raw).unwrap();
        assert!(resp.truncated);
    }

    #[test]
    fn parse_response_errors_on_missing_choices() {
        let p = provider();
        let raw = serde_json::json!({});
        assert!(p.parse_response(&raw).is_err());
    }
}
