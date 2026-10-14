//! LLM 提供商抽象 + Token 预算层。
//!
//! 核心层从不直接调用 LLM。所有请求都经由 LLM 调度器。
//! 本模块定义提供商 trait — 不依赖任何具体的 SDK。

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::RuntimeError;

pub mod openai_compatible;

// ---------------------------------------------------------------------------
// Token 预算 / 模型能力
// ---------------------------------------------------------------------------

/// Token 估算器。
///
/// 永远不应被描述为"精确 token 计数"。不同的模型、语言、tokenization 策略
/// 都会影响真实 token 数。本接口的目的是提供一致的、可替换的估算策略，
/// 而非保证数学精度。
pub trait TokenEstimator: Send + Sync {
    /// 估算给定文本的 token 数（近似值）。
    fn estimate_text(&self, text: &str) -> usize;

    /// 估算一条 LLM 消息的 token 数（近似值）。
    fn estimate_message(&self, message: &LlmMessage) -> usize {
        // base message wrapper overhead + content estimate
        4 + self.estimate_message_content(message)
    }

    /// 估算消息内容的 token 数（不含 role/content wrapper 开销）。
    fn estimate_message_content(&self, message: &LlmMessage) -> usize;
}

/// 默认估算器：ASCII 每四字符近似一 token，非 ASCII 字符近似两 token。
///
/// 注意：
/// - 中日韩文本使用更保守的估算，仍与实际 tokenizer 存在差异
/// - 本估算器不能保证模型上下文不超限
/// - 未来应替换为 provider tokenizer 或 embedding-count 策略
pub struct ApproximateTokenEstimator;

impl TokenEstimator for ApproximateTokenEstimator {
    fn estimate_text(&self, text: &str) -> usize {
        // ASCII 采用常见的四字符近似；非 ASCII 保留更宽裕的预算。
        let ascii = text.chars().filter(char::is_ascii).count();
        ascii
            .div_ceil(4)
            .saturating_add((text.chars().count() - ascii).saturating_mul(2))
    }

    fn estimate_message_content(&self, message: &LlmMessage) -> usize {
        self.estimate_text(&message.content)
    }
}

/// 模型能力描述。
///
/// 用于 Context Budget 计算：已知 context_window 才能在发送前估算可用空间。
/// 不应伪造默认值。未提供 context_window 的 Provider 返回 None。
pub trait ModelCapabilities: Send + Sync {
    /// 模型的最大上下文窗口（输入 token 上限）。
    fn context_window(&self) -> Option<usize>;

    /// 模型的最大输出 token 上限（若已知）。
    fn max_output_tokens(&self) -> Option<usize>;
}

/// 固定的模型能力（用于无法从 Provider 动态获取时的最小配置）。
///
/// 不应用于生产默认值 — 只是在完全无法获取模型信息时提供最小可用值。
/// 实际 context_window 应由 LLM config 或 provider 动态决定。
#[derive(Debug, Clone, Copy)]
pub struct FixedModelCapabilities {
    pub context_window: Option<usize>,
    pub max_output: Option<usize>,
}

impl FixedModelCapabilities {
    pub fn new(context_window: Option<usize>, max_output: Option<usize>) -> Self {
        Self {
            context_window,
            max_output,
        }
    }
}

impl ModelCapabilities for FixedModelCapabilities {
    fn context_window(&self) -> Option<usize> {
        self.context_window
    }

    fn max_output_tokens(&self) -> Option<usize> {
        self.max_output
    }
}

/// 默认保留的 output token 预算（估算值）。
///
/// 这个值应随 ModelCapabilities 动态计算，不应硬编码。
pub const DEFAULT_OUTPUT_RESERVATION: usize = 512;

/// 发送前估算可用 input 空间。
pub fn available_input_budget(
    model: &dyn ModelCapabilities,
    output_reservation: usize,
) -> Option<usize> {
    model
        .context_window()
        .map(|cw| cw.saturating_sub(output_reservation))
}

/// 估算 LlmRequest 的总 token 开销（system + messages）。
pub fn estimate_request_tokens(
    estimator: &dyn TokenEstimator,
    system: &Option<String>,
    messages: &[LlmMessage],
) -> usize {
    let system_tokens = system.as_ref().map_or(0, |s| estimator.estimate_text(s));
    let messages_tokens: usize = messages.iter().map(|m| estimator.estimate_message(m)).sum();
    system_tokens + messages_tokens
}
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

/// LLM 业务调用角色（用于路由到对应 Provider/Model）。
///
/// Role 是调用方视角的语义，不是 Model Instance 的固有属性。
/// 同一个 Model Instance 可以被多个 Role 引用（例如 dev 场景）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LlmCallRole {
    /// 对话内容生成角色。
    #[default]
    Conversation,
    /// 行为/状态决策角色（未来 Decision LLM 使用）。
    Decision,
}

impl LlmCallRole {
    /// 从字符串解析角色，解析失败时返回 Conversation（默认）。
    #[allow(clippy::should_implement_trait)]
    pub fn parse_role(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "decision" => LlmCallRole::Decision,
            "conversation" | "" => LlmCallRole::Conversation,
            other => {
                tracing::warn!(target: "llm", role = other, "未知的 LLM 角色，使用默认值 Conversation");
                LlmCallRole::Conversation
            }
        }
    }

    /// 角色对应的标准字符串表示。
    pub fn as_str(&self) -> &'static str {
        match self {
            LlmCallRole::Conversation => "conversation",
            LlmCallRole::Decision => "decision",
        }
    }
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

/// Embedding 请求。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbedRequest {
    /// 要嵌入的文本列表。
    pub texts: Vec<String>,
    /// 可选的模型名称。
    pub model: Option<String>,
}

/// Embedding 响应。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbedResponse {
    /// 嵌入向量列表。
    pub embeddings: Vec<Vec<f32>>,
    /// 实际使用的模型。
    pub model: String,
    /// token 使用统计。
    pub usage: TokenUsage,
}

/// LLM 提供商 trait。
///
/// 具体实现（OpenAI、Anthropic 等）位于基础设施层。
/// 应用层只依赖本 trait。
#[async_trait]
pub trait LlmProvider: Send + Sync {
    /// 从 LLM 生成一个响应。
    async fn generate(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError>;

    /// 生成文本的嵌入向量。
    ///
    /// 默认实现返回"未实现"错误，OpenAI-compatible 实现提供完整功能。
    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, RuntimeError> {
        let _ = texts;
        Err(RuntimeError::Llm(
            "当前模型 Provider 未实现向量生成".to_string(),
        ))
    }

    /// 检查提供商是否可用。
    async fn health_check(&self) -> Result<bool, RuntimeError>;

    /// 提供商名称（用于日志）。
    fn name(&self) -> &str;
}
