//! 调度器 —— 管理延迟、定时与周期任务。
//!
//! 调度器不会直接调用 LLM。它评估条件，
//! 当需要采取行动时通过行为引擎进行路由。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 一个计划任务。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledTask {
    pub id: i64,
    pub task_type: TaskType,
    pub character_id: Option<i64>,
    pub payload: serde_json::Value,
    pub run_at: DateTime<Utc>,
    pub recurring: bool,
    pub interval_secs: Option<u64>,
    pub enabled: bool,
}

/// 计划任务的类型。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskType {
    /// 情感衰减更新。
    EmotionDecay,
    /// 状态衰减更新。
    StateDecay,
    /// 记忆维护。
    MemoryMaintenance,
    /// 关系维护。
    RelationshipMaintenance,
    /// 主动行为检查。
    ProactiveCheck,
    /// 适配器健康检查。
    AdapterHealthCheck,
    /// 插件健康检查。
    PluginHealthCheck,
    /// 自定义任务。
    Custom(String),
}

/// 计划任务的优先级。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TaskPriority {
    /// 用户交互（P0）。
    Realtime = 0,
    /// 紧急的系统操作（P1）。
    Urgent = 1,
    /// 主动交互（P2）。
    Proactive = 2,
    /// 维护 / 后台（P3）。
    Maintenance = 3,
}

/// 调度器 trait。
///
/// 管理任务调度、执行与背压控制。
#[async_trait::async_trait]
pub trait Scheduler: Send + Sync {
    /// 调度一个一次性任务。
    async fn schedule_once(
        &self,
        task_type: TaskType,
        character_id: Option<i64>,
        payload: serde_json::Value,
        run_at: DateTime<Utc>,
        priority: TaskPriority,
    ) -> Result<i64, crate::error::RuntimeError>;

    /// 调度一个周期任务。
    async fn schedule_recurring(
        &self,
        task_type: TaskType,
        character_id: Option<i64>,
        payload: serde_json::Value,
        interval_secs: u64,
        priority: TaskPriority,
    ) -> Result<i64, crate::error::RuntimeError>;

    /// 取消一个计划任务。
    async fn cancel(&self, task_id: i64) -> Result<(), crate::error::RuntimeError>;

    /// 获取某个角色的所有待处理任务。
    async fn pending_for_character(
        &self,
        character_id: i64,
    ) -> Result<Vec<ScheduledTask>, crate::error::RuntimeError>;
}
