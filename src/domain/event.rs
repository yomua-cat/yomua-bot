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

    /// 收到了一条系统指令。
    ///
    /// 与 `MessageReceived` 互斥：指令消息在消息发布位被截流，
    /// 只发布本事件（硬性约束 B）。
    CommandReceived(CommandReceivedEvent),

    /// 一条消息已被持久化并分配了 canonical identity 和 ingress order。
    ///
    /// 在 `MessageAcceptanceService` 完成 commit 后发布。
    /// 用于替代 `MessageReceived` 触发下游处理（ReplyProcessor）。
    MessageAccepted(MessageAcceptedEvent),
}

/// 一条消息已被持久化并分配了 canonical identity 和 ingress order。
///
/// 由 `MessageAcceptanceService` 在 commit 后发布。
/// 携带：
/// - message_id（messages.id，canonical local identity）
/// - conversation_sequence（ingress order）
/// - platform_message_id（外部平台 ID）
/// - ingested_at（QQBOT 接受时间）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageAcceptedEvent {
    /// Stable event identity for durable outbox. Format: "{message_id}-MessageAccepted".
    pub event_uuid: String,
    pub conversation_id: i64,
    pub sender_id: i64,
    /// QQBOT canonical local message identity（messages.id）。
    pub message_id: i64,
    pub content: String,
    /// 平台时间（OneBot/NapCat）。
    pub timestamp: DateTime<Utc>,
    pub is_mentioned: bool,
    pub character_id: i64,
    /// QQBOT Application Ingress Order。
    pub conversation_sequence: i64,
    /// 外部平台消息 ID。
    pub platform_message_id: Option<i64>,
    /// QQBOT 接受该消息的时间（来自 accept_message 事务内生成）。
    pub ingested_at: DateTime<Utc>,
    /// 入站附加信息：作者类型、平台作者 ID、显示名、提及、引用和绑定版本等。
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// 从用户处收到了一条消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageReceivedEvent {
    pub conversation_id: i64,
    pub sender_id: i64,
    /// QQBOT canonical local message identity（messages.id）。
    pub message_id: i64,
    pub content: String,
    /// 平台时间（OneBot time），即消息发生时间。
    pub timestamp: DateTime<Utc>,
    /// 发送者是否在消息中提及（@）了角色。
    pub is_mentioned: bool,
    /// 该消息被接收时，当前会话的 active character id。
    /// 在事件进入 EventBus 之前已确定，是 immutable processing attribution。
    pub character_id: i64,
    /// QQBOT Application Ingress Order。
    /// 在 Conversation 内严格递增，不要求连续。
    pub conversation_sequence: i64,
    /// 外部平台消息 ID（来自 OneBot/NapCat）。
    /// 用于 correlation identity，不用于 ordering。
    pub platform_message_id: Option<i64>,
    /// QQBOT Application 接受该消息的时间。
    pub ingested_at: DateTime<Utc>,
    /// 与消息一同接受的结构化附加信息，不能在 Outbox 转换时丢失。
    #[serde(default)]
    pub metadata: serde_json::Value,
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
    pub conversation_id: i64,
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

/// 系统指令（平台无关语义）。
///
/// 由适配层在消息发布位识别产生；指令消息不发布 `MessageReceived`，
/// 因此不落库、不进角色上下文、不进插件 message 订阅（硬性约束 B）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// 把当前会话切换到指定角色。
    SwitchCharacter { character_name: String },
}

/// 收到了一条系统指令。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandReceivedEvent {
    pub conversation_id: i64,
    pub sender_id: i64,
    /// 发送者外部平台 ID（如 QQ 号），供权限校验使用。
    pub external_sender_id: String,
    pub message_id: i64,
    pub content: String,
    pub timestamp: DateTime<Utc>,
    pub command: Command,
}
