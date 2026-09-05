//! LLM 提供商抽象。
//!
//! 核心层从不直接调用 LLM。所有请求都经由 LLM 调度器。
//! 本模块定义提供商 trait — 不依赖任何具体的 SDK。

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::RuntimeError;

pub mod openai_compatible;

/// 发送给 LLM 提供商的请求。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmRequest {
    /// 系统提示 / 指令。
    pub system: Option<String>,

    /// 对话消息。
    pub messages: Vec<LlmMessage>,

    /// 要使用的模型（取决于提供商）。
    pub model: Option<String>,

    /// 采样温度。
    pub temperature: Option<f64>,

    /// 响应中的最大 token 数。
    pub max_tokens: Option<u32>,

    /// 优先级（供调度器使用）。
    pub priority: u8,

    /// 请求元数据（character_id、conversation_id 等）。
    pub metadata: serde_json::Value,
}

/// LLM 请求中的一条消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmMessage {
    pub role: LlmRole,
    pub content: String,
}

/// LLM 上下文中消息的角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LlmRole {
    System,
    User,
    Assistant,
}

/// LLM 提供商的响应。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmResponse {
    /// 生成的内容。
    pub content: String,

    /// 实际使用的模型。
    pub model: String,

    /// token 使用情况。
    pub usage: TokenUsage,

    /// 响应是否被截断。
    pub truncated: bool,
}

/// token 使用统计。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

/// LLM 提供商 trait。
///
/// 具体实现（OpenAI、Anthropic 等）位于基础设施层。
/// 应用层只依赖本 trait。
#[async_trait]
pub trait LlmProvider: Send + Sync {
    /// 从 LLM 生成一个响应。
    async fn generate(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError>;

    /// 检查提供商是否可用。
    async fn health_check(&self) -> Result<bool, RuntimeError>;

    /// 提供商名称（用于日志）。
    fn name(&self) -> &str;
}
