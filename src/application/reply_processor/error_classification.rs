use super::ReplyProcessor;
use crate::domain::outbox::ProcessingOutcome;
use crate::error::RuntimeError;

impl ReplyProcessor {
    /// 将回复链路错误统一映射为 Outbox 结果。
    ///
    /// 受租约保护的入口和旧的无租约入口必须使用完全相同的分类规则，否则
    /// 同一类故障会在不同启动路径下产生不同的重试行为。
    pub(crate) fn classify_outbox_error_for_event(
        error: RuntimeError,
        conversation_id: i64,
    ) -> ProcessingOutcome {
        if matches!(
            &error,
            RuntimeError::Repository(crate::error::RepositoryError::OwnershipLost)
        ) {
            // 会话删除、角色切换或租约回收后的旧事件已经失效；继续重试只会
            // 让已删除会话永久占用队列。将其确认完成即可安全清理 outbox。
            return ProcessingOutcome::Success;
        }
        let error_text = error.to_string();
        let (outcome, error_class) = match error {
            RuntimeError::LlmResponseInvalid(_) => (
                ProcessingOutcome::RetryableLlmFailure {
                    error: error_text.clone(),
                    retry_after_secs: None,
                },
                "llm_response_invalid",
            ),
            RuntimeError::RateLimited {
                retry_after_secs, ..
            } => (
                ProcessingOutcome::RetryableLlmFailure {
                    error: error_text.clone(),
                    retry_after_secs,
                },
                "llm_rate_limited",
            ),
            RuntimeError::HttpError(_) => (
                ProcessingOutcome::RetryableLlmFailure {
                    error: error_text.clone(),
                    retry_after_secs: None,
                },
                "llm_transport",
            ),
            RuntimeError::Repository(crate::error::RepositoryError::Database(message))
                if Self::is_transient_database_error(&message) =>
            {
                (
                    ProcessingOutcome::RetryableFailure {
                        error: error_text.clone(),
                    },
                    "database_transient",
                )
            }
            RuntimeError::Storage(crate::error::StorageError::Connection(_)) => (
                ProcessingOutcome::RetryableFailure {
                    error: error_text.clone(),
                },
                "storage_connection",
            ),
            RuntimeError::Storage(crate::error::StorageError::Database(message))
                if Self::is_transient_database_error(&message) =>
            {
                (
                    ProcessingOutcome::RetryableFailure {
                        error: error_text.clone(),
                    },
                    "database_transient",
                )
            }
            RuntimeError::Llm(_)
            | RuntimeError::Config(_)
            | RuntimeError::ParseError(_)
            | RuntimeError::SendRejected(_)
            | RuntimeError::Repository(_)
            | RuntimeError::Storage(_)
            | RuntimeError::Domain(_)
            | RuntimeError::Adapter(_)
            | RuntimeError::Plugin(_)
            | RuntimeError::SendUnknown(_)
            | RuntimeError::Internal(_)
            | RuntimeError::UserInput(_, _) => (
                ProcessingOutcome::PermanentFailure {
                    error: error_text.clone(),
                },
                "permanent",
            ),
            _ => (
                ProcessingOutcome::RetryableFailure {
                    error: error_text.clone(),
                },
                "transient",
            ),
        };

        if outcome.is_permanent() {
            tracing::error!(
                target: "outbox",
                conversation_id,
                error_class,
                error = %error_text,
                "永久性错误"
            );
        } else {
            tracing::warn!(
                target: "outbox",
                conversation_id,
                error_class,
                error = %error_text,
                "可重试错误"
            );
        }
        outcome
    }

    /// 识别 SQLite/连接层明确表示“稍后重试”的错误。
    ///
    /// 未列出的数据库错误保持永久失败，避免把 schema 错误、约束错误或
    /// 数据损坏无限重试并掩盖真实问题。
    fn is_transient_database_error(message: &str) -> bool {
        let message = message.to_ascii_lowercase();
        [
            "database is locked",
            "database is busy",
            "sqlite_busy",
            "pool timed out",
            "connection reset",
            "connection closed",
            "broken pipe",
            "temporarily unavailable",
        ]
        .iter()
        .any(|marker| message.contains(marker))
    }
}
