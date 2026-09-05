//! 插件系统 — 进程隔离、IPC、生命周期管理。
//!
//! 插件在独立进程中运行。核心通过 Unix 域套接字 + MessagePack 通信。
//! 本模块定义接口 — 具体的插件宿主实现留待后续工作。

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::RuntimeError;

/// 描述插件元数据与权限的插件清单。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    /// 唯一的插件名称。
    pub name: String,

    /// 插件版本。
    pub version: String,

    /// 人类可读的描述。
    pub description: String,

    /// 所需的权限。
    pub permissions: Vec<PluginPermission>,

    /// 插件二进制文件的路径。
    pub executable: String,

    /// 插件专属配置。
    pub config: serde_json::Value,
}

/// 插件可以请求的一种权限。
///
/// wire/TOML 统一使用 dotted 字符串（见各变体的 serde rename）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginPermission {
    /// 读取收到的消息。
    #[serde(rename = "message.read")]
    MessageRead,

    /// 发送消息。
    #[serde(rename = "message.send")]
    MessageSend,

    /// 读取角色定义。
    #[serde(rename = "character.read")]
    CharacterRead,

    /// 读取角色状态。
    #[serde(rename = "character.state.read")]
    CharacterStateRead,

    /// 写入角色状态。
    #[serde(rename = "character.state.write")]
    CharacterStateWrite,

    /// 读取记忆。
    #[serde(rename = "memory.read")]
    MemoryRead,

    /// 写入记忆。
    #[serde(rename = "memory.write")]
    MemoryWrite,

    /// 读取关系。
    #[serde(rename = "relationship.read")]
    RelationshipRead,

    /// 写入关系。
    #[serde(rename = "relationship.write")]
    RelationshipWrite,

    /// 创建定时任务。
    #[serde(rename = "scheduler.create")]
    ScheduleCreate,

    /// 调用 LLM。
    #[serde(rename = "llm.call")]
    LlmCall,
}

/// 插件的生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginState {
    Discovered,
    Validating,
    Starting,
    Running,
    Stopping,
    Stopped,
    Crashed,
}

/// 关于运行中的插件的信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginInfo {
    pub name: String,
    pub state: PluginState,
    pub pid: Option<u32>,
}

/// 插件宿主 trait — 管理插件的生命周期。
///
/// 具体实现使用进程监督 + IPC。
#[async_trait]
pub trait PluginHost: Send + Sync {
    /// 从目录中发现插件。
    async fn discover(&self, path: &str) -> Result<Vec<PluginManifest>, RuntimeError>;

    /// 根据清单启动插件。
    async fn start(&self, manifest: &PluginManifest) -> Result<(), RuntimeError>;

    /// 按名称停止插件。
    async fn stop(&self, name: &str) -> Result<(), RuntimeError>;

    /// 获取所有运行中插件的状态。
    async fn list(&self) -> Result<Vec<PluginInfo>, RuntimeError>;

    /// 检查插件是否健康。
    async fn health_check(&self, name: &str) -> Result<bool, RuntimeError>;
}

/// 插件传输 — IPC 抽象。
///
/// 第一阶段使用 Unix 域套接字 + MessagePack。
#[async_trait]
pub trait PluginTransport: Send + Sync {
    /// 向插件发送请求并等待响应。
    async fn request(
        &self,
        plugin_name: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, RuntimeError>;

    /// 发送通知（无需等待响应）。
    async fn notify(
        &self,
        plugin_name: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<(), RuntimeError>;
}

// 本批实现：线协议、清单发现与校验、权限判定、运行时注册表、UDS 传输、
// 生命周期监督、事件桥接。前三批完成 API/协议/权限逻辑（全绿）。
pub mod event_bridge;
pub mod manifest;
pub mod permissions;
pub mod protocol;
pub mod registry;
pub mod supervisor;
pub mod transport;
