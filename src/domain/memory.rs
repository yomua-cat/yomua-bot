//! 记忆（Memory）领域模型。
//!
//! MVP 使用消息历史 + 持久化长期记忆。
//! 第一阶段不使用嵌入或向量数据库。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 一条持久化记忆条目。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Memory {
    pub id: i64,
    pub character_id: i64,

    /// 该记忆来源的会话（若有）。
    pub conversation_id: Option<i64>,

    /// 记忆类型。
    pub memory_type: MemoryType,

    /// 记忆内容。
    pub content: String,

    /// 重要度评分（0.0 - 1.0）。越高越重要。
    pub importance: f64,

    /// 该记忆创建的时间。
    pub created_at: DateTime<Utc>,

    /// 该记忆最后一次访问的时间（用于近因加权）。
    pub last_accessed: DateTime<Utc>,

    /// 任意元数据。
    pub metadata: serde_json::Value,
}

/// 持久化记忆的类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryType {
    /// 记住某个具体事件或对话。
    Episodic,

    /// 一般性知识或事实。
    Semantic,

    /// 与关系相关的知识。
    Relationship,

    /// 系统级长期设定。
    System,
}

impl Memory {
    /// 创建一条新记忆。
    pub fn new(
        character_id: i64,
        conversation_id: Option<i64>,
        memory_type: MemoryType,
        content: String,
        importance: f64,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: 0, // 由存储自动生成
            character_id,
            conversation_id,
            memory_type,
            content,
            importance: importance.clamp(0.0, 1.0),
            created_at: now,
            last_accessed: now,
            metadata: serde_json::Value::Null,
        }
    }

    /// 标记该记忆已被访问（更新 last_accessed 时间戳）。
    pub fn touch(&mut self) {
        self.last_accessed = Utc::now();
    }
}
