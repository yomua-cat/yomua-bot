//! 动作执行 —— 将核心动作分派到适配器（Core Action → 平台调用）。
//!
//! 核心产生的 `Action::SendMessage` 只携带核心 `conversation_id`，不含平台地址。
//! 本模块负责：解析会话（external_id + 类型），并调用适配器 trait 方法发送。
//! OneBot 的 `send_group_msg` / `send_private_msg` 细节始终留在适配器内部。

use std::sync::Arc;

use crate::adapters::onebot::OneBotAdapter;
use crate::domain::behavior::Action;
use crate::domain::conversation::ConversationType;
use crate::domain::repository::ConversationRepository;
use crate::error::{DomainError, RuntimeError};

/// 动作执行器 —— 把核心动作分派给适配器。
pub struct ActionDispatcher {
    conversation_repo: Arc<dyn ConversationRepository>,
    adapter: Arc<dyn OneBotAdapter>,
}

impl ActionDispatcher {
    /// 创建一个动作执行器。
    pub fn new(
        conversation_repo: Arc<dyn ConversationRepository>,
        adapter: Arc<dyn OneBotAdapter>,
    ) -> Self {
        Self {
            conversation_repo,
            adapter,
        }
    }

    /// 执行一个核心动作。
    ///
    /// 当前支持 `Action::SendMessage`；其他动作返回"暂不支持"，不影响调用方。
    pub async fn execute(&self, action: &Action) -> Result<(), RuntimeError> {
        match action {
            Action::SendMessage {
                conversation_id,
                content,
            } => self.execute_send_message(*conversation_id, content).await,
            _ => {
                // 第一阶段尚未实现主动行为 / 状态更新等动作。
                tracing::debug!(target: "runtime", action = ?action, "该动作暂不支持，忽略");
                Ok(())
            }
        }
    }

    /// 执行发送消息动作。
    async fn execute_send_message(
        &self,
        conversation_id: i64,
        content: &str,
    ) -> Result<(), RuntimeError> {
        let conversation = self
            .conversation_repo
            .find_by_id(conversation_id)
            .await?
            .ok_or(RuntimeError::Domain(DomainError::ConversationNotFound(
                conversation_id,
            )))?;

        match conversation.conversation_type {
            ConversationType::Group => {
                self.adapter
                    .send_group_message(&conversation.external_id, content)
                    .await
            }
            ConversationType::Private => {
                self.adapter
                    .send_private_message(&conversation.external_id, content)
                    .await
            }
        }
    }
}
