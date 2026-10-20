//! 记忆（Memory）领域模型。
//!
//! MVP 使用消息历史 + 持久化长期记忆。
//! 第一阶段不使用嵌入或向量数据库。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 向量检索结果（包含分数），用于语义相似度搜索。
#[derive(Debug, Clone)]
pub struct SemanticMatchResult {
    /// 匹配到的记忆。
    pub memory: Memory,
    /// 向量相似度分数（余弦相似度）。
    pub score: f32,
}

/// 一条持久化记忆条目。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Memory {
    pub id: i64,
    pub character_id: i64,

    /// 该记忆关联的全域用户；系统记忆或无法可靠归属的旧数据可为空。
    pub user_id: Option<i64>,

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

    /// 语义向量（JSON 数组），由 EmbeddingScheduler 生成后填充。
    pub embedding: Option<String>,

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

/// 决策模型对候选记忆的审核意见；是否正式化仍由应用规则与仓储约束共同决定。
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryCandidateReview {
    pub candidate_id: i64,
    pub decision: CandidateReviewDecision,
    pub confidence: f64,
    pub reason: String,
    pub model: String,
    pub reviewed_at: DateTime<Utc>,
}

/// 受程序约束的记忆修订操作。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryRevisionOperation {
    Replace,
    Append,
}

impl MemoryRevisionOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Replace => "replace",
            Self::Append => "append",
        }
    }
}

/// 一次明确目标记忆的修订请求。
///
/// `expected_old` 是模型调用开始前保存的快照，仓储必须在事务内再次校验，
/// 避免旧模型结果覆盖并发修改。
#[derive(Debug, Clone)]
pub struct MemoryRevisionRequest {
    pub expected_old: Memory,
    pub operation: MemoryRevisionOperation,
    pub content: String,
    pub metadata: serde_json::Value,
    pub reason: String,
    pub source_request_id: String,
    pub model_version: String,
    pub applied_at: DateTime<Utc>,
}

/// 记忆修订的持久化结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRevisionResult {
    pub old_memory_id: i64,
    pub new_memory_id: i64,
    pub applied: bool,
}

/// 结构化事实声明。每一行都保留来源记忆，不覆盖或删除历史 claim。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryClaim {
    pub id: i64,
    pub claim_key: String,
    pub subject_user_id: i64,
    pub predicate: String,
    pub value: String,
    pub source_memory_id: i64,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub current: bool,
}

/// Claim 更新动作。模型只能提出动作，仓储负责校验和执行。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryClaimOperation {
    Replace,
    Append,
    Reject,
}

impl MemoryClaimOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Replace => "replace",
            Self::Append => "append",
            Self::Reject => "reject",
        }
    }
}

/// 待验证的 claim 建议。该类型不代表已经写入数据库。
#[derive(Debug, Clone)]
pub struct MemoryClaimProposal {
    pub source_memory_id: i64,
    pub claim_key: String,
    pub subject_user_id: i64,
    pub predicate: String,
    pub value: String,
    pub operation: MemoryClaimOperation,
    pub proposed_at: DateTime<Utc>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryClaimResult {
    pub claim_id: Option<i64>,
    pub changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateReviewDecision {
    Confirm,
    Reject,
    Defer,
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
            user_id: None,
            conversation_id,
            memory_type,
            content,
            importance: importance.clamp(0.0, 1.0),
            created_at: now,
            last_accessed: now,
            embedding: None,
            metadata: serde_json::Value::Null,
        }
    }

    /// 标记该记忆已被访问（更新 last_accessed 时间戳）。
    pub fn touch(&mut self) {
        self.last_accessed = Utc::now();
    }
}
