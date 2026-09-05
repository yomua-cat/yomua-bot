//! 核心事件模型——运行时处理的平台无关事件。
//!
//! 事件通过 Event Bus 流转。它们不得携带平台特定的类型。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 由运行时处理的核心事件。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoreEvent {
    /// 从用户处收到了一条消息。
    MessageReceived(MessageReceivedEvent),

    /// 一条消息被发出（由角色或系统）。
    MessageSent(MessageSentEvent),

    /// 角色的状态发生了变化。
    CharacterStateChanged(CharacterStateChangedEvent),

    /// 角色的情绪发生了变化。
    EmotionChanged(EmotionChangedEvent),

    /// 一段关系发生了变化。
    RelationshipChanged(RelationshipChangedEvent),

    /// 创建了一条新记忆。
    MemoryCreated(MemoryCreatedEvent),

    /// 做出了一项行为决策。
    BehaviorDecided(BehaviorDecidedEvent),

    /// 生成了一个响应（由 LLM 或规则引擎）。
    ResponseGenerated(ResponseGeneratedEvent),

    /// 适配器已连接。
    AdapterConnected(AdapterConnectedEvent),

    /// 适配器已断开。
    AdapterDisconnected(AdapterDisconnectedEvent),

    /// 定时任务被触发。
    ScheduledTaskTriggered(ScheduledTaskTriggeredEvent),
}

/// 从用户处收到了一条消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageReceivedEvent {
    pub conversation_id: i64,
    pub sender_id: i64,
    pub message_id: i64,
    pub content: String,
    pub timestamp: DateTime<Utc>,
    /// 发送者是否在消息中提及（@）了角色。
    pub is_mentioned: bool,
}

/// 一条消息被发出。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageSentEvent {
    pub conversation_id: i64,
    pub character_id: Option<i64>,
    pub message_id: i64,
    pub content: String,
    pub timestamp: DateTime<Utc>,
}

/// 角色的状态发生了变化。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CharacterStateChangedEvent {
    pub character_id: i64,
    pub timestamp: DateTime<Utc>,
}

/// 角色的情绪发生了变化。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmotionChangedEvent {
    pub character_id: i64,
    pub timestamp: DateTime<Utc>,
}

/// 一段关系发生了变化。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipChangedEvent {
    pub character_id: i64,
    pub participant_id: i64,
    pub timestamp: DateTime<Utc>,
}

/// 创建了一条新记忆。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryCreatedEvent {
    pub character_id: i64,
    pub memory_id: i64,
    pub timestamp: DateTime<Utc>,
}

/// 做出了一项行为决策。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BehaviorDecidedEvent {
    pub character_id: i64,
    pub conversation_id: i64,
    pub action: String,
    pub reason: String,
    pub timestamp: DateTime<Utc>,
}

/// 生成了一个响应。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseGeneratedEvent {
    pub character_id: i64,
    pub conversation_id: i64,
    pub content: String,
    pub source: ResponseSource,
    pub timestamp: DateTime<Utc>,
}

/// 响应来自何处。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseSource {
    /// 确定性规则引擎。
    Rule,
    /// LLM 提供方。
    Llm,
    /// 插件。
    Plugin,
}

/// 适配器已连接。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterConnectedEvent {
    pub adapter_name: String,
    pub timestamp: DateTime<Utc>,
}

/// 适配器已断开。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterDisconnectedEvent {
    pub adapter_name: String,
    pub reason: Option<String>,
    pub timestamp: DateTime<Utc>,
}

/// 定时任务被触发。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledTaskTriggeredEvent {
    pub task_id: i64,
    pub task_type: String,
    pub timestamp: DateTime<Utc>,
}
