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
use serde::Deserialize;

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
    /// 用于 embedding 的模型（可选，默认为 `text-embedding-3-small`）。
    pub embedding_model: Option<String>,
    /// 超时时间。
    pub timeout: Duration,
}

impl OpenAiCompatibleConfig {
    /// 返回用于 embedding 的模型名称。
    pub fn model_for_embedding(&self) -> Option<String> {
        self.embedding_model.clone()
    }
}

/// Embeddings API 响应。
#[derive(Deserialize)]
struct EmbeddingsResponse {
    data: Vec<EmbeddingData>,
}

/// 单个 embedding 数据项。
#[derive(Deserialize)]
struct EmbeddingData {
    index: usize,
    embedding: Vec<f32>,
}

/// OpenAI-compatible 的具体实现。
pub struct OpenAiCompatibleProvider {
    config: OpenAiCompatibleConfig,
    client: reqwest::Client,
}

impl OpenAiCompatibleProvider {
    /// 创建一个 OpenAI-compatible Provider。
    pub fn new(config: OpenAiCompatibleConfig, proxy: Option<&str>) -> Result<Self, RuntimeError> {
        let client = crate::infrastructure::llm::build_http_client(config.timeout, proxy)?;
        Ok(Self { config, client })
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

/// 响应可能乱序；按 index 重组并验证维度，拒绝缺失、重复及非有限向量。
fn validate_embeddings(
    mut data: Vec<EmbeddingData>,
    expected_count: usize,
) -> Result<Vec<Vec<f32>>, RuntimeError> {
    data.sort_by_key(|item| item.index);
    let dimensions = data.first().map_or(0, |item| item.embedding.len());
    if data.len() != expected_count
        || dimensions == 0
        || data.iter().enumerate().any(|(i, item)| {
            item.index != i
                || item.embedding.len() != dimensions
                || item.embedding.iter().any(|v| !v.is_finite())
        })
    {
        return Err(RuntimeError::ParseError(
            "向量响应的数量、索引或维度无效".into(),
        ));
    }
    Ok(data.into_iter().map(|item| item.embedding).collect())
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

        let mut builder = self
            .client
            .post(&url)
            .timeout(self.config.timeout)
            .json(&body);
        if let Some(key) = &self.config.api_key {
            builder = builder.bearer_auth(key);
        }

        let resp = builder
            .send()
            .await
            .map_err(|_| RuntimeError::HttpError("请求 OpenAI-compatible 服务失败".into()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            let detail = sanitize_error_detail(&body);
            let message = if detail.is_empty() {
                format!("OpenAI-compatible 服务返回错误 {status}")
            } else {
                format!("OpenAI-compatible 服务返回错误 {status}: {detail}")
            };
            if status.is_server_error() || status.as_u16() == 429 {
                return Err(RuntimeError::HttpError(message));
            }
            return Err(RuntimeError::Llm(message));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|_| RuntimeError::Llm("解析响应 JSON 失败".into()))?;

        self.parse_response(&body)
    }

    async fn health_check(&self) -> Result<bool, RuntimeError> {
        let url = format!("{}/models", self.config.base_url.trim_end_matches('/'));
        let mut request = self.client.get(&url).timeout(self.config.timeout);
        if let Some(key) = &self.config.api_key {
            request = request.bearer_auth(key);
        }
        let resp = request
            .send()
            .await
            .map_err(|_| RuntimeError::Llm("健康检查失败".into()))?;
        Ok(resp.status().is_success())
    }

    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, RuntimeError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let expected_count = texts.len();
        let url = format!("{}/embeddings", self.config.base_url.trim_end_matches('/'));
        let model = self
            .config
            .model_for_embedding()
            .ok_or_else(|| RuntimeError::Config("未配置向量模型，跳过向量增强".into()))?;
        let body = serde_json::json!({
            "model": model,
            "input": texts,
        });

        let mut builder = self
            .client
            .post(&url)
            .timeout(self.config.timeout)
            .json(&body);
        if let Some(key) = &self.config.api_key {
            builder = builder.bearer_auth(key);
        }

        let resp = builder
            .send()
            .await
            .map_err(|_| RuntimeError::HttpError("请求 Embeddings API 失败".into()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            return Err(RuntimeError::Llm(format!(
                "Embeddings API 返回错误 {status}"
            )));
        }

        let parsed: EmbeddingsResponse = resp
            .json()
            .await
            .map_err(|_| RuntimeError::ParseError("解析 Embeddings API 响应失败".into()))?;

        validate_embeddings(parsed.data, expected_count)
    }

    fn name(&self) -> &str {
        "openai-compatible"
    }
}

/// 只保留结构化错误说明，避免持久化服务端回显的请求正文。
fn sanitize_error_detail(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
        return "服务端返回非结构化错误内容".into();
    };
    let detail = value
        .pointer("/error/message")
        .or_else(|| value.pointer("/base_resp/status_msg"))
        .or_else(|| value.get("message"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("服务端未提供错误说明");
    let mut detail = detail
        .split_whitespace()
        .map(|part| {
            if part.starts_with("sk-") || part.starts_with("apikey_") {
                "[REDACTED]"
            } else {
                part
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    if detail.len() > 1000 {
        detail.truncate(1000);
        detail.push_str("...");
    }
    detail
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::llm::LlmMessage;

    fn provider() -> OpenAiCompatibleProvider {
        OpenAiCompatibleProvider::new(
            OpenAiCompatibleConfig {
                base_url: "http://127.0.0.1:11434/v1".to_string(),
                api_key: Some("sk-test".to_string()),
                model: "qwen".to_string(),
                embedding_model: None,
                timeout: Duration::from_secs(30),
            },
            None,
        )
        .unwrap()
    }

    #[test]
    fn error_detail_keeps_message_without_serializing_echoed_request() {
        let detail = sanitize_error_detail(
            r#"{"error":{"message":"invalid parameter"},"request":{"messages":[{"content":"private prompt"}]}}"#,
        );
        assert_eq!(detail, "invalid parameter");
        assert!(!detail.contains("private prompt"));
    }

    #[test]
    fn error_detail_redacts_secret_like_words_and_bounds_length() {
        let detail = sanitize_error_detail(
            &serde_json::json!({
                "message": format!("bad key sk-secret {}", "x".repeat(1200))
            })
            .to_string(),
        );
        assert!(detail.starts_with("bad key [REDACTED]"));
        assert!(detail.len() <= 1003);
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

    #[tokio::test]
    async fn configured_proxy_receives_openai_compatible_request() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0_u8; 1024];
            while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&chunk[..count]);
            }
            let request = String::from_utf8_lossy(&bytes).into_owned();
            let body = r#"{"model":"test-model","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}"
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            request
        });
        let provider = OpenAiCompatibleProvider::new(
            OpenAiCompatibleConfig {
                base_url: "http://openai-compatible.invalid/v1".into(),
                api_key: None,
                model: "test-model".into(),
                embedding_model: None,
                timeout: Duration::from_secs(2),
            },
            Some(&format!("http://{address}")),
        )
        .unwrap();
        let request = LlmRequest {
            system: None,
            messages: vec![LlmMessage {
                role: LlmRole::User,
                content: "test".into(),
            }],
            model: None,
            temperature: None,
            max_tokens: None,
            priority: 0,
            metadata: serde_json::json!({}),
        };

        provider.generate(request).await.unwrap();
        assert!(server
            .await
            .unwrap()
            .starts_with("POST http://openai-compatible.invalid/v1/chat/completions "));
    }
}
