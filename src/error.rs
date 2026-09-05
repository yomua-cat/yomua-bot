//! QQBot Character Runtime 的错误类型。

/// 运行时顶层错误类型。
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("存储错误：{0}")]
    Storage(#[from] StorageError),

    #[error("仓储错误：{0}")]
    Repository(#[from] RepositoryError),

    #[error("领域错误：{0}")]
    Domain(#[from] DomainError),

    #[error("配置错误：{0}")]
    Config(String),

    #[error("适配器错误：{0}")]
    Adapter(String),

    #[error("插件错误：{0}")]
    Plugin(String),

    #[error("LLM 错误：{0}")]
    Llm(String),

    #[error("角色卡导入错误：{0}")]
    CardImport(String),

    #[error("内部错误：{0}")]
    Internal(String),
}

/// 领域层错误。此处不包含任何基础设施类型。
#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("角色未找到：{0}")]
    CharacterNotFound(i64),

    #[error("角色已存在：{0}")]
    CharacterAlreadyExists(String),

    #[error("会话未找到：{0}")]
    ConversationNotFound(i64),

    #[error("消息未找到：{0}")]
    MessageNotFound(i64),

    #[error("记忆未找到：{0}")]
    MemoryNotFound(i64),

    #[error("关系未找到：{character_id}/{participant_id}")]
    RelationshipNotFound {
        character_id: i64,
        participant_id: i64,
    },

    #[error("状态无效：{0}")]
    InvalidState(String),

    #[error("角色定义无效：{0}")]
    InvalidDefinition(String),

    #[error("内部错误：{0}")]
    Internal(String),
}

/// 存储层错误（包装数据库错误）。
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("数据库错误：{0}")]
    Database(String),

    #[error("连接错误：{0}")]
    Connection(String),

    #[error("迁移错误：{0}")]
    Migration(String),

    #[error("序列化错误：{0}")]
    Serialization(String),
}

impl From<sqlx::Error> for StorageError {
    fn from(err: sqlx::Error) -> Self {
        StorageError::Database(err.to_string())
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(err: serde_json::Error) -> Self {
        StorageError::Serialization(err.to_string())
    }
}

/// 仓储层错误。
#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    #[error("未找到：{0}")]
    NotFound(String),

    #[error("已存在：{0}")]
    AlreadyExists(String),

    #[error("数据库错误：{0}")]
    Database(String),

    #[error("内部错误：{0}")]
    Internal(String),
}

impl From<sqlx::Error> for RepositoryError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::RowNotFound => RepositoryError::NotFound("row not found".to_string()),
            other => RepositoryError::Database(other.to_string()),
        }
    }
}
