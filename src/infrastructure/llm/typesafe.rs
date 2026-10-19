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
    pub fn new(
        base_url: String,
        api_key: Option<String>,
        timeout: Duration,
        proxy: Option<&str>,
    ) -> Result<Self, RuntimeError> {
        let client = crate::infrastructure::llm::build_http_client(timeout, proxy)?;
        Ok(Self {
            base_url,
            api_key,
            timeout,
            client,
        })
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
            .map_err(|_| RuntimeError::HttpError("请求 TypeSafe 服务失败".into()))?;
        let status = response.status();
        let raw = response
            .text()
            .await
            .map_err(|_| RuntimeError::HttpError("读取 TypeSafe 响应失败".into()))?;
        if !status.is_success() {
            if status.as_u16() == 451 {
                return Err(RuntimeError::Llm(
                    "TypeSafe 服务所在区域不可用（HTTP 451），请更换可用网络区域或配置其他官方支持的 System One 入口"
                        .into(),
                ));
            }
            let message = format!("TypeSafe 服务返回错误 {status}");
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
        let mut builder = self.client.get(self.endpoint()).timeout(self.timeout);
        if let Some(key) = &self.api_key {
            builder = builder.bearer_auth(key);
        }
        let response = builder
            .send()
            .await
            .map_err(|_| RuntimeError::HttpError("检查 TypeSafe 服务失败".into()))?;
        if response.status().is_success() {
            Ok(true)
        } else if response.status().as_u16() == 451 {
            Err(RuntimeError::Llm(
                "TypeSafe 服务所在区域不可用（HTTP 451）".into(),
            ))
        } else {
            Ok(false)
        }
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
    use crate::infrastructure::llm::{LlmProvider, LlmRequest};
    use serde_json::json;
    use std::time::Duration;

    async fn read_headers(stream: &mut tokio::net::TcpStream) -> String {
        use tokio::io::AsyncReadExt;
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1024];
        while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = stream.read(&mut chunk).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&chunk[..count]);
        }
        String::from_utf8_lossy(&bytes).into_owned()
    }

    fn generate_request() -> LlmRequest {
        LlmRequest {
            system: None,
            messages: Vec::new(),
            model: None,
            temperature: None,
            max_tokens: None,
            priority: 0,
            metadata: json!({"typesafe_request": {"state": {}, "questions": []}}),
        }
    }

    fn success_response() -> &'static [u8] {
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"model\":\"jev-latest\",\"usage\":{}}"
    }

    #[tokio::test]
    async fn configured_proxy_receives_typesafe_request() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_headers(&mut stream).await;
            stream.write_all(success_response()).await.unwrap();
            request
        });
        let provider = TypeSafeProvider::new(
            "http://typesafe.invalid".into(),
            None,
            Duration::from_secs(2),
            Some(&format!("http://proxy-user:proxy-secret@{address}")),
        )
        .unwrap();

        provider.generate(generate_request()).await.unwrap();
        let request = server.await.unwrap();
        assert!(request.starts_with("POST http://typesafe.invalid/v1/systemone "));
        assert!(request.contains("proxy-authorization: Basic "));
    }

    #[tokio::test]
    async fn disabled_proxy_connects_directly_to_provider() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_headers(&mut stream).await;
            stream.write_all(success_response()).await.unwrap();
            request
        });
        let provider = TypeSafeProvider::new(
            format!("http://{address}"),
            None,
            Duration::from_secs(2),
            None,
        )
        .unwrap();

        provider.generate(generate_request()).await.unwrap();
        let request = server.await.unwrap();
        assert!(request.starts_with("POST /v1/systemone "));
    }

    #[tokio::test]
    async fn proxy_failure_summary_does_not_expose_credentials() {
        let proxy = "http://proxy-user:proxy-secret@127.0.0.1:1";
        let provider = TypeSafeProvider::new(
            "http://typesafe.invalid".into(),
            None,
            Duration::from_millis(200),
            Some(proxy),
        )
        .unwrap();
        let error = provider
            .generate(generate_request())
            .await
            .unwrap_err()
            .to_string();
        assert!(!error.contains("proxy-user"));
        assert!(!error.contains("proxy-secret"));
    }

    fn provider(base_url: &str) -> TypeSafeProvider {
        TypeSafeProvider::new(base_url.into(), None, Duration::from_secs(1), None).unwrap()
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
