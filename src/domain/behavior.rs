//! 行为领域模型。
//!
//! BehaviorEngine 做出确定性决策：ShouldReply、ShouldIgnore、
//! ShouldDelay、ShouldInitiate。仅在需要认知时才调用 LLM。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 行为引擎做出的决策。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BehaviorDecision {
    /// 要采取的动作。
    pub action: BehaviorAction,

    /// 优先级级别。
    pub priority: Priority,

    /// 所需的认知级别。
    pub cognition_level: CognitionLevel,

    /// 执行前的延迟（毫秒）。
    pub delay_ms: u64,

    /// 此决策的原因（用于调试/日志）。
    pub reason: String,

    pub decided_at: DateTime<Utc>,
}

/// 行为引擎决定采取的动作。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BehaviorAction {
    /// 回复消息。
    Reply,
    /// 忽略消息（不做任何事）。
    Ignore,
    /// 延迟后再做决定。
    Delay,
    /// 主动发起对话。
    InitiateProactive,
    /// 仅更新内部状态。
    UpdateState,
}

/// 行为决策的优先级级别。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Priority {
    /// 用户正在等待回复。
    Realtime = 0,
    /// 紧急的系统动作。
    Urgent = 1,
    /// 正常优先级。
    Normal = 2,
    /// 后台/维护。
    Background = 3,
}

/// 所需的认知级别（LLM 参与程度）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CognitionLevel {
    /// 无需 LLM——确定性规则即可满足。
    None,
    /// 简单响应生成所需的单次 LLM 调用。
    Light,
    /// 深度处理（复杂推理、多轮规划）。
    Deep,
}

/// 由运行时执行的动作。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    /// 发送一条消息。
    SendMessage {
        conversation_id: i64,
        content: String,
    },

    /// 更新角色状态。
    UpdateState {
        character_id: i64,
        state_patch: serde_json::Value,
    },

    /// 创建一条记忆。
    CreateMemory {
        character_id: i64,
        content: String,
        memory_type: String,
        importance: f64,
    },

    /// 安排一个未来的动作。
    Schedule {
        run_at: DateTime<Utc>,
        action: Box<Action>,
    },

    /// 请求认知（调用 LLM）。
    CognitionRequest {
        character_id: i64,
        conversation_id: i64,
        request_type: String,
    },

    /// 无需动作。
    DoNothing,
}

/// 行为引擎 trait。
///
/// 决定角色应如何回应的确定性逻辑。
/// 第一版使用简单规则；未来版本可以更复杂。
#[async_trait::async_trait]
pub trait BehaviorEngine: Send + Sync {
    /// 决定如何回应一条传入的消息。
    async fn decide_response(
        &self,
        character_id: i64,
        conversation_id: i64,
        message_content: &str,
        is_mentioned: bool,
        participant_id: Option<i64>,
    ) -> Result<BehaviorDecision, crate::error::DomainError>;

    /// 决定是否发起主动行为。
    async fn decide_proactive(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<BehaviorDecision, crate::error::DomainError>;
}
