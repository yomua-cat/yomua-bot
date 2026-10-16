//! 控制服务 —— 通过 UDS socket 接收 CLI 命令。
//!
//! 控制 socket 路径：`<data_dir>/control.sock`
//!
//! 控制协议（JSON over UDS）：
//! ```json
//! {"version":1,"id":123,"request":{"operation":"reload_config","params":{}}}
//! {"version":1,"id":456,"request":{"operation":"status","params":{}}}
//! {"version":1,"id":789,"request":{"operation":"shutdown","params":{"confirm":true}}}
//! {"version":1,"id":112,"request":{"operation":"character_state_set","params":{"character_id":1,"conversation_id":10,"mood":80,"energy":90,"stress":20}}}
//! ```
//!
//! `character state set` / `character state get` 通过领域仓储（State System）读写角色状态：
//! - 不带 `conversation_id`：读写全局 Character State（energy / stress / activity）；
//! - 带 `conversation_id`：读写 Character × Conversation 状态（energy / stress → ConversationState，mood → Mood）。
//!
//! `mood` 属于 Character × Conversation 范围，必须提供 `conversation_id`；
//! `activity` 仅存在于全局 Character State；会话与角色都必须已存在
//! （否则返回 `CONVERSATION_NOT_FOUND` / `CHARACTER_NOT_FOUND`，防止孤儿状态行）。
//!
//! 响应格式：
//! ```json
//! {"id": 123, "ok": true, "data": {...}}
//! {"id": 456, "ok": false, "error": {"code": "...", "message": "..."}}
//! ```
//!
//! `CommandRegistry` 只负责把结构化操作交给应用处理器；人类命令、别名和帮助文本由 ctl 维护。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{watch, Notify, RwLock};

use crate::adapters::onebot::{OneBotAdapter, OneBotAdapterImpl};
use crate::application::config::{ConfigurationBundle, RuntimeConfig};
use crate::application::llm_scheduler::LlmScheduler;
use crate::application::runtime::CharacterRuntime;
use crate::domain::character::ConversationState;
use crate::domain::emotion::Mood;

use crate::domain::repository::{
    BlacklistRepository, CharacterRepository, CharacterStateRepository, ConversationRepository,
    ConversationStateRepository, MemoryRepository, MessageRepository, MoodRepository,
};
use crate::error::RuntimeError;
use crate::infrastructure::llm::{LlmMessage, LlmRequest, LlmRole};
use crate::infrastructure::plugin::supervisor::PluginSupervisor;
use crate::infrastructure::storage::SqliteStorage;

use crate::application::character_service::CharacterService;
use crate::application::control_schema::{self, ControlEnvelope, ControlOperation, ControlRequest};
use crate::application::session_service::SessionService;
use crate::domain::repository::ParticipantRepository;

// ---------------------------------------------------------------------------
// 命令处理器类型与注册表
// ---------------------------------------------------------------------------

/// 命令描述（用于 help 命令）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct CommandDesc {
    pub name: &'static str,
    pub usage: &'static str,
    pub group: &'static str,
    pub description: &'static str,
    pub destructive: bool,
    pub requires_confirmation: bool,
}

/// 命令处理函数签名（异步）。接受 RuntimeHandle、请求 ID 与请求参数。
type CommandHandler = fn(&RuntimeHandle, id: u64, params: serde_json::Value) -> CommandFuture;

/// 命令处理结果 Future 的类型别名。
type CommandFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<ControlResponse, RuntimeError>> + Send>,
>;

/// 命令注册表。
#[derive(Default, Clone)]
pub struct CommandRegistry {
    handlers: HashMap<String, (CommandHandler, CommandDesc, bool)>,
}

impl CommandRegistry {
    /// 注册一个新命令。
    pub fn register(
        &mut self,
        name: &'static str,
        fallback_description: &'static str,
        handler: CommandHandler,
    ) {
        // 内置 canonical 命令的描述只从 control_schema 读取，避免服务端
        // 注册表、help 和客户端各自维护一份文字。
        let spec = control_schema::canonical_name(name)
            .and_then(|canonical| {
                control_schema::specs()
                    .iter()
                    .find(|spec| spec.name == canonical)
                    .copied()
            })
            .unwrap_or(control_schema::ControlCommandSpec {
                name,
                usage: name,
                group: "扩展",
                description: fallback_description,
                destructive: false,
                requires_confirmation: false,
                positional_min: 0,
                positional_max: 0,
                allowed_options: &[],
                supports_raw: false,
            });
        self.handlers.insert(
            name.to_string(),
            (
                handler,
                CommandDesc {
                    name: spec.name,
                    usage: spec.usage,
                    group: spec.group,
                    description: spec.description,
                    destructive: spec.destructive,
                    requires_confirmation: spec.requires_confirmation,
                },
                true,
            ),
        );
    }

    /// 分派结构化请求。返回 None 表示请求操作没有对应的处理器。
    pub fn dispatch_request(
        &self,
        request: &ControlRequest,
        handle: &RuntimeHandle,
        id: u64,
    ) -> Option<CommandFuture> {
        self.handlers
            .get(request.operation.canonical_name())
            .map(|(handler, _, _)| handler(handle, id, request.params.clone()))
    }

    /// 返回所有已注册命令的描述。
    pub fn descriptions(&self) -> Vec<CommandDesc> {
        let mut descriptions: Vec<_> = self
            .handlers
            .values()
            .filter(|(_, _, visible)| *visible)
            .map(|(_, desc, _)| desc.clone())
            .collect();
        descriptions.sort_by_key(|description| description.name);
        descriptions
    }

    /// 构建包含所有内置命令的注册表。
    pub fn builtin() -> Self {
        let mut registry = Self::default();
        registry.register("status", "查询运行时状态", handle_status);
        registry.register(
            "reload-config",
            "重新加载并校验配置文件",
            handle_reload_config,
        );
        registry.register("shutdown", "发送优雅关停信号", handle_shutdown);
        registry.register("help", "显示所有可用命令", handle_help);
        registry.register(
            control_schema::USER_MERGE,
            "合并两个全域用户",
            handle_user_merge,
        );
        registry.register(
            control_schema::USER_DELETE,
            "删除全域用户",
            handle_user_delete,
        );
        registry.register(
            control_schema::CHARACTER_STATE_SET,
            "修改角色状态（全局 energy/stress/activity，或 Character×Conversation 的 energy/stress/mood）",
            handle_state_set,
        );
        registry.register(
            control_schema::CHARACTER_STATE_GET,
            "查询角色状态（全局与 Character×Conversation 范围）",
            handle_state_get,
        );
        registry.register(
            control_schema::CHARACTER_LIST,
            "列出所有角色",
            handle_character_list,
        );
        registry.register(
            control_schema::CHARACTER_CARD_LIST,
            "列出所有角色卡（含启用状态）",
            handle_character_card_list,
        );
        registry.register(
            control_schema::CHARACTER_SHOW,
            "显示角色详情（需要 id 参数）",
            handle_character_show,
        );
        registry.register(
            control_schema::CONVERSATION_LIST,
            "列出所有会话",
            handle_conversation_list,
        );
        // 黑名单命令
        registry.register(
            control_schema::BLACKLIST_LIST,
            "列出所有黑名单条目",
            handle_blacklist_list,
        );
        registry.register(
            control_schema::BLACKLIST_ADD,
            "添加永久黑名单",
            handle_blacklist_add,
        );
        registry.register(
            control_schema::BLACKLIST_REMOVE,
            "移除黑名单条目",
            handle_blacklist_remove,
        );
        registry.register(
            control_schema::BLACKLIST_BAN,
            "临时封禁",
            handle_blacklist_ban,
        );
        // 记忆命令
        registry.register(
            control_schema::MEMORY_LIST,
            "查询记忆列表",
            handle_memory_list,
        );
        registry.register(
            control_schema::MEMORY_DELETE,
            "删除记忆",
            handle_memory_delete,
        );
        registry.register(
            control_schema::MEMORY_UPDATE,
            "确认或更正记忆",
            handle_memory_update,
        );
        registry.register(
            control_schema::REPLY_UNKNOWN_LIST,
            "列出发送结果未知、等待核查的回复任务",
            handle_reply_unknown_list,
        );
        registry.register(
            control_schema::REPLY_UNKNOWN_CHECK,
            "查询平台是否存在未知结果对应的消息",
            handle_reply_unknown_check,
        );
        registry.register(
            control_schema::REPLY_UNKNOWN_RETRY,
            "确认平台未发送后重新排队回复任务",
            handle_reply_unknown_retry,
        );
        // 会话删除命令
        registry.register(
            control_schema::SESSION_DELETE_CHARACTER,
            "删除角色关联的所有会话数据",
            handle_session_delete_character,
        );
        registry.register(
            control_schema::SESSION_DELETE_ALL,
            "删除所有会话数据",
            handle_session_delete_all,
        );
        // 角色卡命令
        registry.register(
            control_schema::CHARACTER_CARD_ENABLE,
            "启用角色卡",
            handle_character_card_enable,
        );
        registry.register(
            control_schema::CHARACTER_CARD_DISABLE,
            "停用角色卡",
            handle_character_card_disable,
        );
        registry.register(
            control_schema::CHARACTER_CARD_REASSIGN,
            "修改角色卡 ID",
            handle_character_card_reassign,
        );
        // 状态刷新命令
        registry.register(
            control_schema::CHARACTER_STATE_REFRESH,
            "强制刷新所有角色的全局状态时间戳",
            handle_character_state_refresh,
        );
        registry
    }
}

// ---------------------------------------------------------------------------
// 命令处理函数
// ---------------------------------------------------------------------------

fn handle_status(handle: &RuntimeHandle, id: u64, _params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move { Ok(ControlResponse::ok(id, handle.status().await)) })
}

fn handle_reload_config(
    handle: &RuntimeHandle,
    id: u64,
    _params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let next = match ConfigurationBundle::load(&handle.config_dir) {
            Ok(next) => next,
            Err(error) => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("VALIDATION_ERROR", error.to_string()),
                ))
            }
        };
        let Some(current) = &handle.launch_config else {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new("RELOAD_UNAVAILABLE", "当前运行时未装配配置重载服务"),
            ));
        };
        let restart_required = current.read().await.restart_required(&next);
        if !restart_required.is_empty() {
            return Ok(ControlResponse::ok(
                id,
                serde_json::json!({"applied": false, "restart_required": restart_required, "message": "配置需要重启，当前配置保持原样"}),
            ));
        }
        let Some(settings) = &handle.live_settings else {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new("RELOAD_UNAVAILABLE", "当前运行时没有热更新通道"),
            ));
        };
        if let Some(reload) = &handle.log_reload {
            reload(&next.runtime.log_level)?;
        }
        settings.send_replace(next.runtime.clone());
        *current.write().await = next.clone();
        tracing::info!(target: "control", "运行配置已整体生效");
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({
                "applied": true, "restart_required": [], "log_level": next.runtime.log_level,
                "admin_users": next.runtime.admin_users, "shutdown_timeout_secs": next.runtime.shutdown_timeout_secs,
            }),
        ))
    })
}

fn handle_shutdown(handle: &RuntimeHandle, id: u64, _params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let completed = handle.shutdown_complete.notified();
        handle.shutdown();
        let wait_for =
            Duration::from_secs(handle.effective_config().shutdown_timeout_secs.max(1) + 5);
        if tokio::time::timeout(wait_for, completed).await.is_err() {
            return Ok(ControlResponse::ok(
                id,
                serde_json::json!({
                    "completed": false,
                    "timed_out": true,
                    "unfinished_tasks": serde_json::Value::Null,
                    "message": "已发出关停请求，但未在等待期限内确认完成"
                }),
            ));
        }
        let result = handle
            .shutdown_result
            .read()
            .await
            .clone()
            .unwrap_or_else(|| ShutdownResult {
                completed: false,
                timed_out: true,
                unfinished_tasks: None,
                message: "关停已触发，但缺少完成报告".to_string(),
            });
        Ok(ControlResponse::ok(id, result))
    })
}

fn handle_help(handle: &RuntimeHandle, id: u64, _params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let commands = handle.commands.descriptions();
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({
                "commands": commands,
            }),
        ))
    })
}

fn handle_state_set(handle: &RuntimeHandle, id: u64, params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ))
            }
        };

        // character_id 必填。
        let character_id = match obj.get("character_id").and_then(|v| v.as_i64()) {
            Some(cid) if cid > 0 => cid,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 character_id"),
                ))
            }
        };

        // 至少提供一个目标字段。
        let has_field = ["mood", "energy", "stress", "activity"]
            .iter()
            .any(|k| obj.contains_key(*k));
        if !has_field {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "INVALID_PARAMS",
                    "至少提供一个字段：mood / energy / stress / activity",
                ),
            ));
        }

        // 角色必须存在（与 plugin_api.character.state.write 一致，防止孤儿状态行）。
        let exists = handle
            .character_repo
            .find_by_id(character_id)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?
            .is_some();
        if !exists {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "CHARACTER_NOT_FOUND",
                    format!("角色不存在：{character_id}"),
                ),
            ));
        }

        // conversation_id 可选（缺省表示全局 Character State）。
        let conversation_id = match obj.get("conversation_id").and_then(|v| v.as_i64()) {
            None => None,
            Some(0) => None,
            Some(cid) if cid > 0 => Some(cid),
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "非法参数 conversation_id"),
                ))
            }
        };

        // mood 属于 Character × Conversation 范围：必须提供 conversation_id。
        if obj.contains_key("mood") && conversation_id.is_none() {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "MOOD_REQUIRES_CONVERSATION",
                    "mood 属于 Character × Conversation 范围，必须提供 conversation_id",
                ),
            ));
        }

        // 会话级 activity 在当前状态模型中未定义（仅全局 Character State 有 current_activity）。
        if obj.contains_key("activity") && conversation_id.is_some() {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "SCOPED_ACTIVITY_UNSUPPORTED",
                    "activity 当前仅存在于全局 Character State；会话级 activity 模型未定义",
                ),
            ));
        }

        // 会话必须存在（与角色存在校验一致，防止孤儿状态行）。
        if let Some(cid) = conversation_id {
            let exists = handle
                .conversation_repo
                .find_by_id(cid)
                .await
                .map_err(|e| RuntimeError::Internal(e.to_string()))?
                .is_some();
            if !exists {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("CONVERSATION_NOT_FOUND", format!("会话不存在：{cid}")),
                ));
            }
        }

        let mut result = serde_json::json!({ "character_id": character_id });

        match conversation_id {
            // 全局 Character State：energy / stress / activity。
            None => {
                let mut patch = serde_json::Map::new();
                for key in ["energy", "stress"] {
                    if let Some(value) = obj.get(key) {
                        if !value.is_number() {
                            return Ok(ControlResponse::err(
                                id,
                                ControlErrorDetail::new(
                                    "INVALID_PARAMS",
                                    format!("{key} 必须是数字"),
                                ),
                            ));
                        }
                        patch.insert(key.to_string(), value.clone());
                    }
                }
                if let Some(activity) = obj.get("activity") {
                    if !activity.is_null() && !activity.is_string() {
                        return Ok(ControlResponse::err(
                            id,
                            ControlErrorDetail::new(
                                "INVALID_PARAMS",
                                "activity 必须是字符串或 null",
                            ),
                        ));
                    }
                    patch.insert("current_activity".to_string(), activity.clone());
                }
                let state = if let Some(runtime) = &handle.runtime {
                    runtime
                        .apply_state_patch(character_id, &serde_json::Value::Object(patch))
                        .await?
                } else {
                    let state = handle
                        .state_repo
                        .find_by_character_id(character_id)
                        .await?
                        .unwrap_or_default();
                    let mut value = serde_json::to_value(state)
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                    for (key, entry) in patch {
                        value[key] = entry;
                    }
                    let state: crate::domain::character::CharacterState =
                        serde_json::from_value(value)
                            .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                    let state = state.clamped();
                    handle.state_repo.upsert(character_id, &state).await?;
                    state
                };
                result["character_state"] = serde_json::to_value(state)
                    .map_err(|e| RuntimeError::Internal(e.to_string()))?;
            }

            // Character × Conversation：energy / stress → ConversationState，mood → Mood。
            Some(cid) => {
                result["conversation_id"] = serde_json::json!(cid);

                let patch_conv = obj.contains_key("energy") || obj.contains_key("stress");
                if patch_conv {
                    let mut state: ConversationState = handle
                        .conversation_state_repo
                        .find_by_character_and_conversation(character_id, cid)
                        .await
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?
                        .unwrap_or_default();

                    if let Some(v) = obj.get("energy") {
                        state.energy = v.as_f64().ok_or_else(|| {
                            RuntimeError::Internal("energy 必须是数字".to_string())
                        })?;
                    }
                    if let Some(v) = obj.get("stress") {
                        state.stress = v.as_f64().ok_or_else(|| {
                            RuntimeError::Internal("stress 必须是数字".to_string())
                        })?;
                    }

                    let state = state.clamped();
                    handle
                        .conversation_state_repo
                        .upsert(character_id, cid, &state)
                        .await
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                    result["conversation_state"] = serde_json::to_value(&state)
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                }

                if obj.contains_key("mood") {
                    let mood_value = obj
                        .get("mood")
                        .and_then(|v| v.as_f64())
                        .ok_or_else(|| RuntimeError::Internal("mood 必须是数字".to_string()))?;
                    let mut mood: Mood = handle
                        .mood_repo
                        .find_by_character_and_conversation(character_id, cid)
                        .await
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?
                        .unwrap_or_default();
                    mood.value = mood_value;
                    let mood = mood.clamped();
                    handle
                        .mood_repo
                        .upsert(character_id, cid, &mood)
                        .await
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                    result["mood"] = serde_json::to_value(&mood)
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                }
            }
        }

        Ok(ControlResponse::ok(id, result))
    })
}

fn handle_state_get(handle: &RuntimeHandle, id: u64, params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ))
            }
        };

        let character_id = match obj.get("character_id").and_then(|v| v.as_i64()) {
            Some(cid) if cid > 0 => cid,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 character_id"),
                ))
            }
        };
        let conversation_id = match obj.get("conversation_id").and_then(|v| v.as_i64()) {
            None => None,
            Some(0) => None,
            Some(cid) if cid > 0 => Some(cid),
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "非法参数 conversation_id"),
                ))
            }
        };

        let mut result = serde_json::json!({ "character_id": character_id });

        // 全局 Character State（无记录返回 null，不做副作用写入）。
        match handle
            .state_repo
            .find_by_character_id(character_id)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?
        {
            Some(state) => {
                result["character_state"] = serde_json::to_value(&state)
                    .map_err(|e| RuntimeError::Internal(e.to_string()))?;
            }
            None => {
                result["character_state"] = serde_json::Value::Null;
            }
        }

        // Character × Conversation State。
        if let Some(cid) = conversation_id {
            result["conversation_id"] = serde_json::json!(cid);
            match handle
                .conversation_state_repo
                .find_by_character_and_conversation(character_id, cid)
                .await
                .map_err(|e| RuntimeError::Internal(e.to_string()))?
            {
                Some(state) => {
                    result["conversation_state"] = serde_json::to_value(&state)
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                }
                None => {
                    result["conversation_state"] = serde_json::Value::Null;
                }
            }
            match handle
                .mood_repo
                .find_by_character_and_conversation(character_id, cid)
                .await
                .map_err(|e| RuntimeError::Internal(e.to_string()))?
            {
                Some(mood) => {
                    result["mood"] = serde_json::to_value(&mood)
                        .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                }
                None => {
                    result["mood"] = serde_json::Value::Null;
                }
            }
        }

        Ok(ControlResponse::ok(id, result))
    })
}

fn handle_character_list(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let object = params.as_object();
        let limit = object
            .and_then(|value| value.get("limit"))
            .map(|value| {
                value
                    .as_i64()
                    .filter(|limit| (1..=1000).contains(limit))
                    .ok_or_else(|| RuntimeError::Config("limit 必须是 1 到 1000 之间的整数".into()))
            })
            .transpose()?
            .map(|limit| limit as usize);
        let offset = object
            .and_then(|value| value.get("offset"))
            .map(|value| {
                value
                    .as_i64()
                    .filter(|offset| *offset >= 0)
                    .ok_or_else(|| RuntimeError::Config("offset 必须是非负整数".into()))
            })
            .transpose()?
            .unwrap_or(0) as usize;
        let characters = handle
            .character_repo
            .find_all()
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;
        let total = characters.len();
        let list: Vec<serde_json::Value> = characters
            .into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .map(|c| {
                serde_json::json!({
                    "id": c.id,
                    "name": c.definition.name,
                    "description": c.definition.description,
                    "enabled": c.enabled,
                })
            })
            .collect();
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({
                "characters": list,
                "total": total,
                "offset": offset,
                "limit": limit,
            }),
        ))
    })
}

fn handle_character_card_list(
    handle: &RuntimeHandle,
    id: u64,
    _params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let characters = handle
            .character_repo
            .find_all()
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;
        let list: Vec<serde_json::Value> = characters
            .into_iter()
            .map(|c| {
                serde_json::json!({
                    "id": c.id,
                    "name": c.definition.name,
                    "description": c.definition.description,
                    "enabled": c.enabled,
                })
            })
            .collect();
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({ "characters": list }),
        ))
    })
}

fn handle_character_show(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ));
            }
        };
        let character_id = match obj.get("id").and_then(|v| v.as_i64()) {
            Some(cid) if cid > 0 => cid,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 id"),
                ));
            }
        };
        let character = handle
            .character_repo
            .find_by_id(character_id)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;
        match character {
            Some(c) => Ok(ControlResponse::ok(
                id,
                serde_json::json!({
                    "id": c.id,
                    "name": c.definition.name,
                    "description": c.definition.description,
                    "personality": c.definition.personality,
                    "scenario": c.definition.scenario,
                    "style": c.definition.style,
                }),
            )),
            None => Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "CHARACTER_NOT_FOUND",
                    format!("角色不存在：{character_id}"),
                ),
            )),
        }
    })
}

fn handle_conversation_list(
    handle: &RuntimeHandle,
    id: u64,
    _params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let conversations = handle
            .conversation_repo
            .find_all()
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;
        let list: Vec<serde_json::Value> = conversations
            .into_iter()
            .map(|c| {
                serde_json::json!({
                    "id": c.id,
                    "external_id": c.external_id,
                })
            })
            .collect();
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({ "conversations": list }),
        ))
    })
}

// ---------------------------------------------------------------------------
// 黑名单处理器
// ---------------------------------------------------------------------------

fn handle_blacklist_list(
    handle: &RuntimeHandle,
    id: u64,
    _params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let entries = handle
            .blacklist_repo
            .find_all()
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;

        let list: Vec<serde_json::Value> = entries
            .into_iter()
            .map(|e| {
                let status = if e.is_active() {
                    if e.is_temporary() {
                        "临时"
                    } else {
                        "永久"
                    }
                } else {
                    "已过期"
                };
                serde_json::json!({
                    "id": e.id,
                    "target": e.target,
                    "target_type": e.target_type.as_str(),
                    "reason": e.reason,
                    "status": status,
                    "created_at": e.created_at.to_rfc3339(),
                    "expires_at": e.expires_at.map(|dt| dt.to_rfc3339()),
                })
            })
            .collect();

        Ok(ControlResponse::ok(
            id,
            serde_json::json!({ "blacklist": list }),
        ))
    })
}

fn handle_blacklist_add(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ));
            }
        };

        let target = match obj.get("target").and_then(|v| v.as_str()) {
            Some(t) if !t.is_empty() => t.to_string(),
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 target"),
                ));
            }
        };

        let target_type_str = obj
            .get("target_type")
            .and_then(|v| v.as_str())
            .unwrap_or("user");
        let target_type = match target_type_str {
            "user" => crate::domain::blacklist::BlacklistType::User,
            "group" => crate::domain::blacklist::BlacklistType::Group,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "target_type 必须是 user 或 group"),
                ));
            }
        };

        let reason = obj.get("reason").and_then(|v| v.as_str()).map(String::from);

        // 检查是否已存在且有效
        if let Some(existing) = handle
            .blacklist_repo
            .find_by_target(&target, target_type)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?
        {
            if existing.is_active() {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("ALREADY_EXISTS", "目标已在黑名单中"),
                ));
            }
            // 已过期：更新为永久
            handle
                .blacklist_repo
                .set_expiration(existing.id, None)
                .await
                .map_err(|e| RuntimeError::Internal(e.to_string()))?;
            if reason.is_some() {
                handle
                    .blacklist_repo
                    .update_reason(existing.id, reason)
                    .await
                    .map_err(|e| RuntimeError::Internal(e.to_string()))?;
            }
            return Ok(ControlResponse::ok(
                id,
                serde_json::json!({ "id": existing.id, "reactivated": true }),
            ));
        }

        let entry = crate::domain::blacklist::NewBlacklistEntry {
            target,
            target_type,
            reason,
            expires_at: None,
        };

        let new_id = handle
            .blacklist_repo
            .insert(&entry)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;

        Ok(ControlResponse::ok(id, serde_json::json!({ "id": new_id })))
    })
}

fn handle_blacklist_remove(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ));
            }
        };

        let target = match obj.get("target").and_then(|v| v.as_str()) {
            Some(t) if !t.is_empty() => t.to_string(),
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 target"),
                ));
            }
        };

        let target_type_str = obj
            .get("target_type")
            .and_then(|v| v.as_str())
            .unwrap_or("user");
        let target_type = match target_type_str {
            "user" => crate::domain::blacklist::BlacklistType::User,
            "group" => crate::domain::blacklist::BlacklistType::Group,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "target_type 必须是 user 或 group"),
                ));
            }
        };

        handle
            .blacklist_repo
            .delete_by_target(&target, target_type)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;

        Ok(ControlResponse::ok(
            id,
            serde_json::json!({ "removed": true }),
        ))
    })
}

fn handle_blacklist_ban(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ));
            }
        };

        let target = match obj.get("target").and_then(|v| v.as_str()) {
            Some(t) if !t.is_empty() => t.to_string(),
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 target"),
                ));
            }
        };

        let target_type_str = obj
            .get("target_type")
            .and_then(|v| v.as_str())
            .unwrap_or("user");
        let target_type = match target_type_str {
            "user" => crate::domain::blacklist::BlacklistType::User,
            "group" => crate::domain::blacklist::BlacklistType::Group,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "target_type 必须是 user 或 group"),
                ));
            }
        };

        let duration_secs = match obj.get("duration_secs").and_then(|v| v.as_i64()) {
            Some(d) if d > 0 => d,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 duration_secs"),
                ));
            }
        };

        let reason = obj.get("reason").and_then(|v| v.as_str()).map(String::from);

        let expires_at = chrono::Utc::now() + chrono::Duration::seconds(duration_secs);

        // 检查是否已存在
        if let Some(existing) = handle
            .blacklist_repo
            .find_by_target(&target, target_type)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?
        {
            handle
                .blacklist_repo
                .set_expiration(existing.id, Some(expires_at))
                .await
                .map_err(|e| RuntimeError::Internal(e.to_string()))?;
            if reason.is_some() {
                handle
                    .blacklist_repo
                    .update_reason(existing.id, reason)
                    .await
                    .map_err(|e| RuntimeError::Internal(e.to_string()))?;
            }
            return Ok(ControlResponse::ok(
                id,
                serde_json::json!({ "id": existing.id, "updated": true }),
            ));
        }

        let entry = crate::domain::blacklist::NewBlacklistEntry {
            target,
            target_type,
            reason,
            expires_at: Some(expires_at),
        };

        let new_id = handle
            .blacklist_repo
            .insert(&entry)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;

        Ok(ControlResponse::ok(id, serde_json::json!({ "id": new_id })))
    })
}

// ---------------------------------------------------------------------------
// 记忆处理器
// ---------------------------------------------------------------------------

fn handle_reply_unknown_list(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let limit = params
            .get("limit")
            .and_then(|value| value.as_i64())
            .unwrap_or(100)
            .clamp(1, 1000);
        let tasks = handle
            .message_repo
            .find_unknown_reply_tasks(limit)
            .await
            .map_err(|error| RuntimeError::Internal(error.to_string()))?;
        let raw = params
            .get("raw")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let items: Vec<_> = tasks
            .into_iter()
            .map(|task| {
                let mut item = serde_json::json!({
                    "message_id": task.message_id,
                    "character_id": task.character_id,
                    "platform_message_id": task.platform_message_id,
                    "content_available": task.content.is_some(),
                    "error": task.error,
                    "updated_at": task.updated_at.to_rfc3339(),
                });
                if raw {
                    item["content"] = task
                        .content
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null);
                }
                item
            })
            .collect();
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({ "tasks": items, "count": items.len() }),
        ))
    })
}

fn handle_reply_unknown_check(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let message_id = params
            .get("message_id")
            .and_then(|value| value.as_i64())
            .ok_or_else(|| RuntimeError::Internal("缺少 message_id".into()))?;
        let task = handle
            .message_repo
            .find_unknown_reply_task(message_id)
            .await?
            .ok_or_else(|| RuntimeError::Internal("未找到未知发送结果任务".into()))?;
        let Some(platform_message_id) = task.platform_message_id else {
            return Ok(ControlResponse::ok(
                id,
                serde_json::json!({
                    "message_id": message_id,
                    "status": "manual_required",
                    "reason": "任务没有平台消息 ID，无法用 get_msg 自动核查"
                }),
            ));
        };
        let response = match handle.adapter.get_message(platform_message_id).await {
            Ok(response) => response,
            Err(error) => {
                return Ok(ControlResponse::ok(
                    id,
                    serde_json::json!({
                        "message_id": message_id,
                        "platform_message_id": platform_message_id,
                        "status": "check_failed",
                        "recorded": false,
                        "reason": error.to_string(),
                    }),
                ));
            }
        };
        if response.is_ok() {
            let confirmed = handle
                .message_repo
                .confirm_unknown_reply_task(message_id, platform_message_id)
                .await
                .map_err(|error| RuntimeError::Internal(error.to_string()))?;
            if !confirmed {
                return Ok(ControlResponse::ok(
                    id,
                    serde_json::json!({
                        "message_id": message_id,
                        "platform_message_id": platform_message_id,
                        "status": "already_handled"
                    }),
                ));
            }
            return Ok(ControlResponse::ok(
                id,
                serde_json::json!({
                    "message_id": message_id,
                    "platform_message_id": platform_message_id,
                    "status": "confirmed_sent",
                    "recorded": true,
                    "queued": false,
                    "platform_response": response.data,
                    "platform_error": response.error_message(),
                }),
            ));
        }
        // 只有平台明确说明“这条消息不存在/未发送”时，才能安全重排。
        // 单独的 retcode 只表示本次核查请求失败，不能证明原发送没有发生；
        // 网络错误和未知 API 错误必须继续保持 unresolved，避免重复发送。
        let confirmed_not_sent = response_explicitly_confirms_not_sent(&response);
        let queued = if confirmed_not_sent {
            handle
                .message_repo
                .requeue_unknown_reply_task(message_id)
                .await
                .map_err(|error| RuntimeError::Internal(error.to_string()))?
        } else {
            false
        };
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({
                "message_id": message_id,
                "platform_message_id": platform_message_id,
                "status": if queued { "confirmed_not_sent" } else { "check_unresolved" },
                "recorded": false,
                "queued": queued,
                "platform_response": response.data,
                "platform_error": response.error_message(),
            }),
        ))
    })
}

/// 判断 `get_msg` 的失败是否明确证明目标消息不存在。
///
/// OneBot/NapCat 的错误码并没有跨实现稳定的语义，因此不能仅凭非零
/// `retcode` 重排 unknown 任务。只有错误文本明确包含“消息不存在/未找到”
/// 等语义时，才允许进入安全重试路径。
fn response_explicitly_confirms_not_sent(
    response: &crate::adapters::onebot::ActionResponse,
) -> bool {
    if response.is_ok() {
        return false;
    }
    let message = response.error_message().to_ascii_lowercase();
    [
        "消息不存在",
        "消息未找到",
        "未找到消息",
        "消息不存在或已被撤回",
        "消息未发送",
        "不存在该消息",
        "message not found",
        "msg not found",
        "message does not exist",
        "message is not found",
        "message was not found",
        "message not sent",
        "msg not sent",
        "message was not sent",
    ]
    .iter()
    .any(|marker| message.contains(&marker.to_ascii_lowercase()))
}

fn handle_reply_unknown_retry(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let message_id = params
            .get("message_id")
            .and_then(|value| value.as_i64())
            .ok_or_else(|| RuntimeError::Internal("缺少 message_id".into()))?;
        let changed = handle
            .message_repo
            .requeue_unknown_reply_task(message_id)
            .await?;
        if !changed {
            return Err(RuntimeError::Internal(
                "任务不是 unknown，或已经被其他操作处理".into(),
            ));
        }
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({
                "message_id": message_id,
                "status": "queued",
                "message": "已重新排队；只有明确确认平台未发送后才应执行此命令"
            }),
        ))
    })
}

fn handle_memory_list(handle: &RuntimeHandle, id: u64, params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let limit = params
            .get("limit")
            .and_then(|v| v.as_i64())
            .unwrap_or(100)
            .clamp(1, 1000);

        let character_id = params.get("character_id").and_then(|v| v.as_i64());
        let conversation_id = params.get("conversation_id").and_then(|v| v.as_i64());
        let raw = params.get("raw").and_then(|v| v.as_bool()).unwrap_or(false);
        let memory_type = params.get("memory_type").and_then(|v| v.as_str()).map(|s| {
            match s.trim().to_lowercase().as_str() {
                "episodic" => Ok(crate::domain::memory::MemoryType::Episodic),
                "semantic" => Ok(crate::domain::memory::MemoryType::Semantic),
                "relationship" => Ok(crate::domain::memory::MemoryType::Relationship),
                "system" => Ok(crate::domain::memory::MemoryType::System),
                other => Err(format!("未知记忆类型：{other}")),
            }
        });

        // memory_type 解析失败时返回错误
        let mem_type_opt = match memory_type {
            Some(Ok(t)) => Some(t),
            Some(Err(e)) => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", e),
                ));
            }
            None => None,
        };

        let memories = handle
            .memory_repo
            .find_filtered(character_id, conversation_id, mem_type_opt, limit)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;

        let list: Vec<serde_json::Value> = memories
            .into_iter()
            .map(|m| {
                let mut item = serde_json::json!({
                    "id": m.id,
                    "character_id": m.character_id,
                    "conversation_id": m.conversation_id,
                    "type": format!("{:?}", m.memory_type),
                    "importance": m.importance,
                    "status": m.metadata.get("status").and_then(|v| v.as_str()).unwrap_or("formal"),
                    "shared": m.metadata.get("shared").and_then(|v| v.as_bool()).unwrap_or(false),
                    "created_at": m.created_at.to_rfc3339(),
                    "last_accessed": m.last_accessed.to_rfc3339(),
                    "content_available": raw,
                });
                if raw {
                    item["content"] = serde_json::Value::String(m.content);
                } else {
                    item["content_fingerprint"] =
                        serde_json::Value::String(memory_fingerprint(&m.content));
                }
                item
            })
            .collect();

        Ok(ControlResponse::ok(
            id,
            serde_json::json!({ "memories": list, "count": list.len() }),
        ))
    })
}

fn handle_memory_delete(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let memory_id = match params.get("memory_id").and_then(|v| v.as_i64()) {
            Some(v) if v > 0 => v,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 memory_id"),
                ));
            }
        };

        // 检查记忆是否存在
        let existing = handle
            .memory_repo
            .find_by_id(memory_id)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;

        if existing.is_none() {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new("MEMORY_NOT_FOUND", format!("记忆不存在：{memory_id}")),
            ));
        }

        handle
            .memory_repo
            .delete(memory_id)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;

        Ok(ControlResponse::ok(
            id,
            serde_json::json!({ "memory_id": memory_id }),
        ))
    })
}

fn handle_memory_update(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let memory_id = match params.get("memory_id").and_then(|v| v.as_i64()) {
            Some(v) if v > 0 => v,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 memory_id"),
                ));
            }
        };
        let mut memory = match handle.memory_repo.find_by_id(memory_id).await? {
            Some(memory) => memory,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("MEMORY_NOT_FOUND", format!("记忆不存在：{memory_id}")),
                ));
            }
        };
        let instruction = match params.get("instruction").and_then(|v| v.as_str()) {
            Some(value) if !value.trim().is_empty() => value.trim(),
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new(
                        "INVALID_PARAMS",
                        "需要非空 instruction，自然语言描述修改意图",
                    ),
                ));
            }
        };
        let Some(scheduler) = &handle.llm_scheduler else {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "MEMORY_EDIT_UNAVAILABLE",
                    "当前未配置可用的模型，无法根据自然语言修改记忆",
                ),
            ));
        };

        let requested_at = chrono::Utc::now().to_rfc3339();
        memory.metadata["edit_request"] = serde_json::json!({
            "instruction": instruction,
            "status": "pending",
            "requested_at": requested_at,
        });

        // 记忆原文只发送给已配置的 LLM 调度器；响应必须是受限 JSON，
        // 后续只允许程序审核过的操作落库。
        let request = LlmRequest {
            system: Some(
                "你是记忆修改建议器。只返回 JSON，不要 Markdown，不要解释。格式必须是："
                    .to_string()
                    + r#"{"action":"replace|append|no_change","content":"...","reason":"..."}。"#
                    + "只能根据原记忆和用户意图提出建议；不能删除记忆，不能输出额外字段。",
            ),
            messages: vec![LlmMessage {
                role: LlmRole::User,
                content: serde_json::json!({
                    "memory_id": memory.id,
                    "memory_type": format!("{:?}", memory.memory_type).to_lowercase(),
                    "current_content": memory.content,
                    "instruction": instruction,
                })
                .to_string(),
            }],
            model: None,
            temperature: Some(0.0),
            max_tokens: Some(512),
            priority: 0,
            metadata: serde_json::json!({
                "llm_role": "decision",
                "purpose": "memory_edit_suggestion",
                "memory_id": memory.id,
            }),
        };

        let response = match scheduler.submit(request).await {
            Ok(response) => response,
            Err(error) => {
                memory.metadata["edit_request"] = serde_json::json!({
                    "instruction": instruction,
                    "status": "failed",
                    "requested_at": requested_at,
                    "error": error.to_string(),
                });
                handle.memory_repo.update(&memory).await?;
                return Err(error);
            }
        };
        let suggestion = match parse_memory_edit_suggestion(&response.content) {
            Ok(suggestion) => suggestion,
            Err(error) => {
                memory.metadata["edit_request"] = serde_json::json!({
                    "instruction": instruction,
                    "status": "rejected",
                    "requested_at": requested_at,
                    "error": error,
                });
                handle.memory_repo.update(&memory).await?;
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("MEMORY_EDIT_REJECTED", error),
                ));
            }
        };

        let old_content = memory.content.clone();
        match suggestion.action.as_str() {
            "replace" => memory.content = suggestion.content.clone(),
            "append" => {
                memory.content = format!("{}\n{}", memory.content.trim_end(), suggestion.content)
            }
            "no_change" => {}
            _ => unreachable!("parse_memory_edit_suggestion 已校验 action"),
        }
        if memory.content.chars().count() > 20_000 {
            memory.content = old_content;
            memory.metadata["edit_request"] = serde_json::json!({
                "instruction": instruction,
                "status": "rejected",
                "requested_at": requested_at,
                "error": "修改后的记忆超过 20000 个字符",
            });
            handle.memory_repo.update(&memory).await?;
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new("MEMORY_EDIT_REJECTED", "修改后的记忆过长"),
            ));
        }
        let applied_at = chrono::Utc::now().to_rfc3339();
        memory.metadata["edit_request"] = serde_json::json!({
            "instruction": instruction,
            "status": "applied",
            "action": suggestion.action,
            "reason": suggestion.reason,
            "requested_at": requested_at,
            "applied_at": applied_at,
            "model": response.model,
        });
        memory.metadata["updated_by"] = serde_json::Value::String("llm_memory_edit".to_string());
        handle.memory_repo.update(&memory).await?;
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({
                "memory_id": memory.id,
                "updated": true,
                "action": memory.metadata["edit_request"]["action"],
                "content_changed": old_content != memory.content,
            }),
        ))
    })
}

#[derive(Debug, serde::Deserialize)]
struct MemoryEditSuggestion {
    action: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    reason: String,
}

fn parse_memory_edit_suggestion(raw: &str) -> Result<MemoryEditSuggestion, String> {
    let trimmed = raw.trim();
    let json = trimmed
        .strip_prefix("```json")
        .and_then(|value| value.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(trimmed);
    let suggestion: MemoryEditSuggestion = serde_json::from_str(json)
        .map_err(|error| format!("模型没有返回合法的记忆修改 JSON：{error}"))?;
    if !matches!(
        suggestion.action.as_str(),
        "replace" | "append" | "no_change"
    ) {
        return Err("模型返回了不允许的修改动作".into());
    }
    if suggestion.reason.chars().count() > 2_000 {
        return Err("模型返回的修改理由过长".into());
    }
    if suggestion.action != "no_change" && suggestion.content.trim().is_empty() {
        return Err("修改动作需要非空 content".into());
    }
    if suggestion.content.chars().count() > 20_000 {
        return Err("模型返回的修改内容过长".into());
    }
    Ok(suggestion)
}

fn memory_fingerprint(content: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    content.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

// ---------------------------------------------------------------------------
// 会话删除处理器
// ---------------------------------------------------------------------------

fn handle_session_delete_character(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ));
            }
        };

        let character_id = match obj.get("character_id").and_then(|v| v.as_i64()) {
            Some(cid) if cid > 0 => cid,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 character_id"),
                ));
            }
        };

        // 验证角色存在
        let exists = handle
            .character_repo
            .find_by_id(character_id)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?
            .is_some();
        if !exists {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "CHARACTER_NOT_FOUND",
                    format!("角色不存在：{character_id}"),
                ),
            ));
        }

        match handle
            .session_service
            .delete_character_sessions(character_id)
            .await
        {
            Ok(deleted) => Ok(ControlResponse::ok(
                id,
                serde_json::json!({ "deleted": deleted }),
            )),
            Err(e) => Ok(ControlResponse::err(
                id,
                ControlErrorDetail::internal(&e.to_string()),
            )),
        }
    })
}

fn handle_session_delete_all(
    handle: &RuntimeHandle,
    id: u64,
    _params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        match handle.session_service.delete_all_sessions().await {
            Ok(deleted) => Ok(ControlResponse::ok(
                id,
                serde_json::json!({ "deleted": deleted }),
            )),
            Err(e) => Ok(ControlResponse::err(
                id,
                ControlErrorDetail::internal(&e.to_string()),
            )),
        }
    })
}

// ---------------------------------------------------------------------------
// 全域用户处理器
// ---------------------------------------------------------------------------

fn handle_user_merge(handle: &RuntimeHandle, id: u64, params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ));
            }
        };
        let source_user_id = obj.get("source_user_id").and_then(|v| v.as_i64());
        let target_user_id = obj.get("target_user_id").and_then(|v| v.as_i64());
        let (source_user_id, target_user_id) = match (source_user_id, target_user_id) {
            (Some(source), Some(target)) if source > 0 && target > 0 => (source, target),
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new(
                        "INVALID_PARAMS",
                        "缺少或非法参数 source_user_id / target_user_id",
                    ),
                ));
            }
        };
        if source_user_id == target_user_id {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "INVALID_PARAMS",
                    "source_user_id 与 target_user_id 不能相同",
                ),
            ));
        }
        let merged = match handle
            .participant_repo
            .merge_users(source_user_id, target_user_id)
            .await
        {
            Ok(merged) => merged,
            Err(crate::error::RepositoryError::NotFound(message)) => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("USER_NOT_FOUND", message),
                ));
            }
            Err(e) => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::internal(&e.to_string()),
                ));
            }
        };
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({
                "merged_user_id": merged,
                "removed_user_id": source_user_id,
            }),
        ))
    })
}

fn handle_user_delete(handle: &RuntimeHandle, id: u64, params: serde_json::Value) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ));
            }
        };
        let user_id = match obj.get("user_id").and_then(|v| v.as_i64()) {
            Some(uid) if uid > 0 => uid,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 user_id"),
                ));
            }
        };
        if let Err(e) = handle.participant_repo.delete_user(user_id).await {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::internal(&e.to_string()),
            ));
        }
        Ok(ControlResponse::ok(
            id,
            serde_json::json!({ "deleted_user_id": user_id }),
        ))
    })
}

// ---------------------------------------------------------------------------
// 角色卡处理器
// ---------------------------------------------------------------------------

fn handle_character_card_enable(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ));
            }
        };

        let character_id = match obj.get("id").and_then(|v| v.as_i64()) {
            Some(cid) if cid > 0 => cid,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 id"),
                ));
            }
        };

        // 角色必须存在
        let exists = handle
            .character_repo
            .find_by_id(character_id)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?
            .is_some();
        if !exists {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "CHARACTER_NOT_FOUND",
                    format!("角色不存在：{}", character_id),
                ),
            ));
        }

        match handle
            .character_service
            .enable_character(character_id)
            .await
        {
            Ok(()) => {
                if let Some(runtime) = &handle.runtime {
                    runtime.evict(character_id).await;
                }
                Ok(ControlResponse::ok(
                    id,
                    serde_json::json!({ "id": character_id, "enabled": true }),
                ))
            }
            Err(RuntimeError::UserInput(code, msg)) => Ok(ControlResponse::err(
                id,
                ControlErrorDetail::with_code(code, msg),
            )),
            Err(e) => Ok(ControlResponse::err(
                id,
                ControlErrorDetail::internal(&e.to_string()),
            )),
        }
    })
}

fn handle_character_card_disable(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ));
            }
        };

        let character_id = match obj.get("id").and_then(|v| v.as_i64()) {
            Some(cid) if cid > 0 => cid,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 id"),
                ));
            }
        };

        // 角色必须存在
        let exists = handle
            .character_repo
            .find_by_id(character_id)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?
            .is_some();
        if !exists {
            return Ok(ControlResponse::err(
                id,
                ControlErrorDetail::new(
                    "CHARACTER_NOT_FOUND",
                    format!("角色不存在：{}", character_id),
                ),
            ));
        }

        match handle
            .character_service
            .disable_character(character_id)
            .await
        {
            Ok(()) => {
                if let Some(runtime) = &handle.runtime {
                    runtime.evict(character_id).await;
                }
                Ok(ControlResponse::ok(
                    id,
                    serde_json::json!({ "id": character_id, "enabled": false }),
                ))
            }
            Err(RuntimeError::UserInput(code, msg)) => Ok(ControlResponse::err(
                id,
                ControlErrorDetail::with_code(code, msg),
            )),
            Err(e) => Ok(ControlResponse::err(
                id,
                ControlErrorDetail::internal(&e.to_string()),
            )),
        }
    })
}

fn handle_character_card_reassign(
    handle: &RuntimeHandle,
    id: u64,
    params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        let obj = match params.as_object() {
            Some(o) => o,
            None => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "params 必须是 JSON 对象"),
                ));
            }
        };

        let old_id = match obj.get("old_id").and_then(|v| v.as_i64()) {
            Some(cid) if cid > 0 => cid,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 old_id"),
                ));
            }
        };

        let new_id = match obj.get("new_id").and_then(|v| v.as_i64()) {
            Some(cid) if cid > 0 => cid,
            _ => {
                return Ok(ControlResponse::err(
                    id,
                    ControlErrorDetail::new("INVALID_PARAMS", "缺少或非法参数 new_id"),
                ));
            }
        };

        match handle.character_service.reassign_id(old_id, new_id).await {
            Ok(()) => {
                if let Some(runtime) = &handle.runtime {
                    runtime.evict(old_id).await;
                    runtime.evict(new_id).await;
                }
                Ok(ControlResponse::ok(
                    id,
                    serde_json::json!({ "old_id": old_id, "new_id": new_id }),
                ))
            }
            Err(RuntimeError::UserInput(code, msg)) => Ok(ControlResponse::err(
                id,
                ControlErrorDetail::with_code(code, msg),
            )),
            Err(e) => Ok(ControlResponse::err(
                id,
                ControlErrorDetail::internal(&e.to_string()),
            )),
        }
    })
}

// ---------------------------------------------------------------------------
// 状态刷新处理器
// ---------------------------------------------------------------------------

fn handle_character_state_refresh(
    handle: &RuntimeHandle,
    id: u64,
    _params: serde_json::Value,
) -> CommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        // 读取所有角色
        let characters = handle
            .character_repo
            .find_all()
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;

        let now = chrono::Utc::now();
        let mut refreshed = 0;

        for character in characters {
            // 读取当前状态
            if let Some(state) = handle
                .state_repo
                .find_by_character_id(character.id)
                .await
                .map_err(|e| RuntimeError::Internal(e.to_string()))?
            {
                // 更新时间戳并写回（force refresh = 更新 last_updated）
                let updated = crate::domain::character::CharacterState {
                    energy: state.energy,
                    stress: state.stress,
                    current_activity: state.current_activity,
                    last_updated: now,
                };
                handle
                    .state_repo
                    .upsert(character.id, &updated)
                    .await
                    .map_err(|e| RuntimeError::Internal(e.to_string()))?;
                refreshed += 1;
            }
        }

        Ok(ControlResponse::ok(
            id,
            serde_json::json!({ "refreshed": refreshed }),
        ))
    })
}

// ---------------------------------------------------------------------------
// 响应类型
// ---------------------------------------------------------------------------

/// 控制协议错误码。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ControlErrorDetail {
    pub code: String,
    pub message: String,
}

impl ControlErrorDetail {
    pub fn new(code: &'static str, message: impl std::fmt::Display) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
        }
    }

    /// 从动态代码字符串创建（用于从 RuntimeError::UserInput 提取 code）。
    pub fn with_code(code: impl AsRef<str>, message: impl std::fmt::Display) -> Self {
        Self {
            code: code.as_ref().to_string(),
            message: message.to_string(),
        }
    }

    /// 请求操作没有对应处理器。
    pub fn unsupported_operation(operation: ControlOperation) -> Self {
        Self::new(
            "UNSUPPORTED_OPERATION",
            format!("控制操作未启用: {}", operation.canonical_name()),
        )
    }

    /// 缺少结构化请求字段。
    pub fn missing_request() -> Self {
        Self::new("MISSING_REQUEST", "请求缺少 request 字段")
    }

    /// 无效的 JSON。
    pub fn invalid_json(e: &str) -> Self {
        Self::new("INVALID_JSON", format!("无效的 JSON: {e}"))
    }

    /// 内部错误。
    pub fn internal(msg: &str) -> Self {
        Self::new("INTERNAL_ERROR", msg)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ControlResponse {
    /// 请求 ID，用于关联请求与响应。
    pub id: u64,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ControlErrorDetail>,
}

impl ControlResponse {
    fn ok(id: u64, data: impl serde::Serialize) -> Self {
        Self {
            id,
            ok: true,
            data: Some(serde_json::to_value(data).unwrap_or_default()),
            error: None,
        }
    }

    fn err(id: u64, detail: ControlErrorDetail) -> Self {
        Self {
            id,
            ok: false,
            data: None,
            error: Some(detail),
        }
    }
}

// ---------------------------------------------------------------------------
// RuntimeHandle
// ---------------------------------------------------------------------------

pub type LogReload = Arc<dyn Fn(&str) -> Result<(), RuntimeError> + Send + Sync>;

/// 运行时句柄 —— 持有所有关键组件，供命令处理器使用。
#[derive(Clone)]
pub struct RuntimeHandle {
    /// 配置目录。
    pub config_dir: PathBuf,
    /// 当前运行时配置。
    pub runtime_cfg: RuntimeConfig,
    /// 生效的热更新配置与启动装配快照。
    pub live_settings: Option<watch::Sender<RuntimeConfig>>,
    pub launch_config: Option<Arc<tokio::sync::RwLock<ConfigurationBundle>>>,
    pub log_reload: Option<LogReload>,
    pub runtime: Option<Arc<CharacterRuntime>>,
    /// 数据目录。
    pub data_dir: PathBuf,
    /// 插件监督器。
    pub supervisor: Arc<PluginSupervisor>,
    /// 适配器。
    pub adapter: Arc<OneBotAdapterImpl>,
    /// 存储。
    pub storage: Arc<SqliteStorage>,
    /// 关停信号发送端。
    pub shutdown_tx: watch::Sender<bool>,
    /// 关停完成通知与结果。控制客户端等待这一通知后再报告成功。
    pub shutdown_complete: Arc<Notify>,
    pub shutdown_result: Arc<RwLock<Option<ShutdownResult>>>,
    /// 命令注册表（可外部扩展）。
    pub commands: CommandRegistry,
    /// 角色仓储（State System：校验角色存在）。
    pub character_repo: Arc<dyn CharacterRepository>,
    /// 全局角色状态仓储（State System：Character State）。
    pub state_repo: Arc<dyn CharacterStateRepository>,
    /// 会话级状态仓储（State System：Character × Conversation State）。
    pub conversation_state_repo: Arc<dyn ConversationStateRepository>,
    /// Mood 仓储（State System：Character × Conversation Mood）。
    pub mood_repo: Arc<dyn MoodRepository>,
    /// 会话仓储（State System：会话作用域校验，防止孤儿状态行）。
    pub conversation_repo: Arc<dyn ConversationRepository>,
    /// 消息仓储（用于查询会话活动状态）。
    pub message_repo: Arc<dyn MessageRepository>,
    /// 统一 LLM 调度器；控制层只通过它请求受限的记忆修改建议。
    pub llm_scheduler: Option<Arc<dyn LlmScheduler>>,
    /// 进程启动时刻（单调时钟），用于计算运行时长。
    pub started_at: Instant,
    /// 黑名单仓储。
    pub blacklist_repo: Arc<dyn BlacklistRepository>,
    /// 会话删除服务。
    pub session_service: SessionService,
    /// 角色卡服务（enable/disable/reassign）。
    pub character_service: CharacterService,
    /// 记忆仓储（memory list/delete）。
    pub memory_repo: Arc<dyn MemoryRepository>,
    /// 参与者仓储（全域用户合并 / 删除）。
    pub participant_repo: Arc<dyn ParticipantRepository>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ShutdownResult {
    pub completed: bool,
    pub timed_out: bool,
    pub unfinished_tasks: Option<usize>,
    pub message: String,
}

impl RuntimeHandle {
    pub fn effective_config(&self) -> RuntimeConfig {
        self.live_settings
            .as_ref()
            .map(|settings| settings.borrow().clone())
            .unwrap_or_else(|| self.runtime_cfg.clone())
    }

    /// 返回当前运行时状态。
    pub async fn status(&self) -> RuntimeStatus {
        // 系统时间（UTC，RFC3339）
        let system_time = chrono::Utc::now();

        // 主程序运行时长（秒）
        let main_uptime_secs = self.started_at.elapsed().as_secs();

        // 会话统计
        let (active_sessions, total_sessions) = self.get_session_stats().await;

        // 进程内存（RSS）
        let memory_bytes = self.get_memory_bytes();

        // OneBot 连接状态
        let adapter_state = format!("{:?}", self.adapter.state().await);

        RuntimeStatus {
            system_time: system_time.to_rfc3339(),
            main_uptime_secs,
            active_sessions,
            total_sessions,
            memory_bytes,
            onebot_status: adapter_state,
            plugin_count: self.supervisor.plugin_count(),
            data_dir: self.data_dir.to_string_lossy().to_string(),
            log_level: self.effective_config().log_level,
        }
    }

    /// 获取会话统计。
    ///
    /// active 定义：最近 30 分钟内有消息活动的会话。
    async fn get_session_stats(&self) -> (usize, usize) {
        let conversations = match self.conversation_repo.find_all().await {
            Ok(c) => c,
            Err(_) => return (0, 0),
        };

        let total = conversations.len();
        let now = chrono::Utc::now();
        let window = chrono::Duration::minutes(30);

        let mut active = 0;
        for conv in &conversations {
            match self.message_repo.latest_message_time(conv.id).await {
                Ok(Some(latest)) if now.signed_duration_since(latest) < window => {
                    active += 1;
                }
                _ => {}
            }
        }

        (active, total)
    }

    /// 获取进程内存（RSS，单位字节）。
    fn get_memory_bytes(&self) -> Option<u64> {
        // 使用 sysinfo crate 获取进程内存
        let pid = sysinfo::Pid::from_u32(std::process::id());
        let mut sys = sysinfo::System::new_all();
        sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid][..]), true);
        sys.process(pid).map(|p| p.memory())
    }

    /// 发送关停信号。
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        tracing::info!(target: "control", "关停信号已发送");
    }
}

/// 运行时状态快照。
#[derive(Debug, serde::Serialize)]
pub struct RuntimeStatus {
    /// 系统时间（RFC 3339 格式）。
    pub system_time: String,
    /// 主程序运行时长（秒）。
    pub main_uptime_secs: u64,
    /// 活跃会话数（最近 30 分钟内有消息活动）。
    pub active_sessions: usize,
    /// 全部会话数。
    pub total_sessions: usize,
    /// 进程内存占用（RSS，字节）。None 表示无法获取。
    pub memory_bytes: Option<u64>,
    /// OneBot 连接状态。
    pub onebot_status: String,
    pub plugin_count: usize,
    pub data_dir: String,
    pub log_level: String,
}

// ---------------------------------------------------------------------------
// 控制服务入口
// ---------------------------------------------------------------------------

/// 启动控制服务（后台任务）。
pub fn start_control_service(
    handle: RuntimeHandle,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let socket_path = handle.data_dir.join("control.sock");
    tokio::spawn(async move {
        if let Err(e) = run_control_server(handle, socket_path, shutdown_rx).await {
            tracing::error!(target: "control", error = %e, "控制服务异常退出");
        }
    })
}

async fn run_control_server(
    handle: RuntimeHandle,
    socket_path: PathBuf,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    // 确保 socket 目录存在。
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            RuntimeError::Internal(format!("无法创建 socket 目录 {}: {e}", parent.display()))
        })?;
    }

    prepare_socket_path(&socket_path).await?;

    let listener = UnixListener::bind(&socket_path).map_err(|e| {
        RuntimeError::Internal(format!(
            "无法绑定控制 socket {}: {e}",
            socket_path.display()
        ))
    })?;

    std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| RuntimeError::Internal(format!("设置控制 socket 权限失败：{e}")))?;
    let _socket_guard = SocketCleanup(socket_path.clone());
    let mut connections = tokio::task::JoinSet::new();
    tracing::info!(target: "control", path = %socket_path.display(), "控制服务已启动");

    loop {
        tokio::select! {
            // 接受新连接。
            accept = listener.accept() => {
                let (mut stream, _) = accept.map_err(|e| {
                    RuntimeError::Internal(format!("accept 控制 socket 失败: {e}"))
                })?;
                let handle = handle.clone();
                connections.spawn(async move {
                    if let Err(e) = handle_connection(&handle, &mut stream).await {
                        tracing::warn!(target: "control", error = %e, "处理命令出错");
                    }
                });
            }
            // 收到关停信号，退出。
            _ = connections.join_next(), if !connections.is_empty() => {}
            result = shutdown_rx.changed() => {
                if result.is_err() || *shutdown_rx.borrow() {
                    tracing::info!(target: "control", "控制服务收到关停信号，正在退出");
                    break;
                }
            }
        }
    }

    // 已接受的 shutdown 命令有机会写完应答；其余连接受整体关停时限约束。
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while connections.join_next().await.is_some() {}
    })
    .await;
    Ok(())
}

async fn handle_connection(
    handle: &RuntimeHandle,
    stream: &mut UnixStream,
) -> Result<(), RuntimeError> {
    let envelope: ControlEnvelope = match read_control_json(stream, CONTROL_REQUEST_LIMIT).await {
        Ok(value) => value,
        Err(error) => {
            let response =
                ControlResponse::err(0, ControlErrorDetail::invalid_json(&error.to_string()));
            write_response(stream, response).await?;
            return Ok(());
        }
    };

    let id = envelope.id;
    if envelope.version != control_schema::CONTROL_PROTOCOL_VERSION {
        let response = ControlResponse::err(
            id,
            ControlErrorDetail::new(
                "INVALID_REQUEST",
                format!("不支持的控制协议版本（版本 {}）", envelope.version),
            ),
        );
        write_response(stream, response).await?;
        return Ok(());
    }

    let operation = envelope.request.operation;
    let command_name = operation.canonical_name();
    if control_schema::requires_confirmation(command_name)
        && envelope
            .request
            .params
            .get("confirm")
            .and_then(|value| value.as_bool())
            != Some(true)
    {
        let response = ControlResponse::err(
            id,
            ControlErrorDetail::new(
                "CONFIRMATION_REQUIRED",
                "此操作会修改或删除数据，必须在 request.params.confirm 中明确传入 true",
            ),
        );
        write_response(stream, response).await?;
        return Ok(());
    }

    let resp = match handle
        .commands
        .dispatch_request(&envelope.request, handle, id)
    {
        Some(future) => match tokio::time::timeout(CONTROL_COMMAND_TIMEOUT, future).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => ControlResponse::err(
                id,
                ControlErrorDetail::new("COMMAND_FAILED", error.to_string()),
            ),
            Err(_) => ControlResponse::err(
                id,
                ControlErrorDetail::new("COMMAND_TIMEOUT", "命令处理超时"),
            ),
        },
        None => {
            let resp =
                ControlResponse::err(id, ControlErrorDetail::unsupported_operation(operation));
            write_response(stream, resp).await?;
            return Ok(());
        }
    };

    write_response(stream, resp).await?;
    Ok(())
}

async fn write_response(
    stream: &mut UnixStream,
    resp: ControlResponse,
) -> Result<(), RuntimeError> {
    write_control_json(stream, &resp, CONTROL_RESPONSE_LIMIT).await
}

/// 控制接口每条连接一问一答，JSON 可分片传输，大小与总等待时间均受限。
pub const CONTROL_REQUEST_LIMIT: usize = 1024 * 1024;
pub const CONTROL_RESPONSE_LIMIT: usize = 8 * 1024 * 1024;
// 客户端等待响应 45 秒；协议读写的单次超时必须更长，不能在长命令完成前
// 先由服务端读超时。命令处理保留少量响应写入余量。
pub const CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(50);
pub const CONTROL_COMMAND_TIMEOUT: Duration = Duration::from_secs(40);

pub async fn read_control_json<R: AsyncRead + Unpin, T: serde::de::DeserializeOwned>(
    reader: &mut R,
    limit: usize,
) -> Result<T, RuntimeError> {
    tokio::time::timeout(CONTROL_IO_TIMEOUT, async {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let read_limit = chunk.len().min(limit.saturating_sub(buffer.len()) + 1);
            let length = reader
                .read(&mut chunk[..read_limit])
                .await
                .map_err(|e| RuntimeError::Internal(format!("读取控制数据失败：{e}")))?;
            if length == 0 {
                return Err(RuntimeError::Internal(
                    "控制连接在完整 JSON 到达前关闭".to_string(),
                ));
            }
            buffer.extend_from_slice(&chunk[..length]);
            if buffer.len() > limit {
                return Err(RuntimeError::Internal(format!(
                    "控制数据超过 {limit} 字节上限"
                )));
            }
            match serde_json::from_slice(&buffer) {
                Ok(value) => return Ok(value),
                Err(error) if error.is_eof() => {}
                Err(error) => {
                    return Err(RuntimeError::Internal(format!(
                        "控制 JSON 格式错误：{error}"
                    )))
                }
            }
        }
    })
    .await
    .map_err(|_| RuntimeError::Internal("读取控制数据超时".to_string()))?
}

pub async fn write_control_json<W: AsyncWrite + Unpin, T: serde::Serialize>(
    writer: &mut W,
    value: &T,
    limit: usize,
) -> Result<(), RuntimeError> {
    let mut buffer = serde_json::to_vec(value)
        .map_err(|e| RuntimeError::Internal(format!("序列化控制数据失败：{e}")))?;
    if buffer.len() + 1 > limit {
        return Err(RuntimeError::Internal(format!(
            "控制数据超过 {limit} 字节上限"
        )));
    }
    buffer.push(b'\n');
    tokio::time::timeout(CONTROL_IO_TIMEOUT, writer.write_all(&buffer))
        .await
        .map_err(|_| RuntimeError::Internal("写入控制数据超时".to_string()))?
        .map_err(|e| RuntimeError::Internal(format!("写入控制数据失败：{e}")))
}

struct SocketCleanup(PathBuf);
impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

async fn prepare_socket_path(path: &std::path::Path) -> Result<(), RuntimeError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_socket() {
                return Err(RuntimeError::Internal(format!(
                    "控制 socket 路径已有非 socket 文件：{}",
                    path.display()
                )));
            }
            match UnixStream::connect(path).await {
                Ok(_) => {
                    return Err(RuntimeError::Internal(
                        "控制 socket 已被运行中的进程占用".to_string(),
                    ))
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                    ) =>
                {
                    std::fs::remove_file(path).map_err(|e| {
                        RuntimeError::Internal(format!("清理失效 socket 失败：{e}"))
                    })?;
                }
                Err(error) => {
                    return Err(RuntimeError::Internal(format!(
                        "检查控制 socket 失败：{error}"
                    )))
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(RuntimeError::Internal(format!(
                "检查控制 socket 文件失败：{error}"
            )))
        }
    }
    Ok(())
}

#[cfg(test)]
mod transport_tests {
    use super::*;

    fn action_response(
        error: Option<&str>,
        retcode: Option<i32>,
    ) -> crate::adapters::onebot::ActionResponse {
        crate::adapters::onebot::ActionResponse {
            id: 1,
            action: "get_msg".to_string(),
            status: "failed".to_string(),
            retcode,
            data: None,
            error: error.map(str::to_string),
        }
    }

    #[test]
    fn unknown_reply_check_only_requeues_for_explicit_missing_message() {
        assert!(response_explicitly_confirms_not_sent(&action_response(
            Some("消息不存在或已被撤回"),
            Some(100),
        )));
        assert!(response_explicitly_confirms_not_sent(&action_response(
            Some("message not found"),
            Some(100),
        )));
        assert!(!response_explicitly_confirms_not_sent(&action_response(
            Some("temporary upstream error"),
            Some(100),
        )));
        assert!(!response_explicitly_confirms_not_sent(&action_response(
            Some("request not sent"),
            Some(100),
        )));
        assert!(!response_explicitly_confirms_not_sent(&action_response(
            None,
            Some(100),
        )));
    }

    #[test]
    fn control_timeouts_leave_room_for_client_response() {
        assert!(CONTROL_IO_TIMEOUT > Duration::from_secs(45));
        assert!(CONTROL_COMMAND_TIMEOUT < Duration::from_secs(45));
    }

    #[test]
    fn memory_edit_suggestion_accepts_only_safe_actions() {
        let suggestion = parse_memory_edit_suggestion(
            r#"{"action":"replace","content":"用户喜欢猫","reason":"明确事实"}"#,
        )
        .expect("合法建议应被接受");
        assert_eq!(suggestion.action, "replace");
        assert!(
            parse_memory_edit_suggestion(r#"{"action":"delete","content":"","reason":""}"#,)
                .is_err()
        );
        assert!(
            parse_memory_edit_suggestion(r#"{"action":"append","content":"","reason":""}"#,)
                .is_err()
        );
    }

    #[test]
    fn memory_edit_suggestion_accepts_fenced_json_without_exposing_extra_fields() {
        let suggestion = parse_memory_edit_suggestion(
            "```json\n{\"action\":\"no_change\",\"reason\":\"证据不足\",\"extra\":true}\n```",
        )
        .expect("代码围栏中的合法 JSON 应被接受");
        assert_eq!(suggestion.action, "no_change");
        assert_eq!(suggestion.reason, "证据不足");
    }

    #[test]
    fn builtin_help_exposes_canonical_names_only() {
        let registry = CommandRegistry::builtin();
        let names: Vec<_> = registry
            .descriptions()
            .into_iter()
            .map(|description| description.name)
            .collect();
        assert!(names.contains(&"character list"));
        assert!(names.contains(&"character state get"));
        assert!(!names.contains(&"character_list"));
        assert!(!names.contains(&"state-get"));
        assert!(!registry.handlers.contains_key("character_list"));
    }

    #[tokio::test]
    async fn fragmented_and_large_json_roundtrip() {
        let (mut sender, mut receiver) = tokio::io::duplex(64);
        let payload = serde_json::json!({"id": 12, "text": "中文消息".repeat(10000)});
        let expected = payload.clone();
        let writer = tokio::spawn(async move {
            let bytes = serde_json::to_vec(&payload).unwrap();
            for chunk in bytes.chunks(7) {
                sender.write_all(chunk).await.unwrap();
            }
        });
        let received: serde_json::Value = read_control_json(&mut receiver, CONTROL_RESPONSE_LIMIT)
            .await
            .unwrap();
        assert_eq!(received, expected);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_and_truncated_json_rejected() {
        let mut oversized = &b"{\"text\":\"too long\"}"[..];
        assert!(read_control_json::<_, serde_json::Value>(&mut oversized, 5)
            .await
            .unwrap_err()
            .to_string()
            .contains("上限"));
        let mut truncated = &b"{\"text\":"[..];
        assert!(
            read_control_json::<_, serde_json::Value>(&mut truncated, 100)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn active_socket_and_regular_file_are_preserved() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.sock");
        std::fs::write(&path, "用户文件").unwrap();
        assert!(prepare_socket_path(&path).await.is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "用户文件");
        std::fs::remove_file(&path).unwrap();
        match UnixListener::bind(&path) {
            Ok(_listener) => {
                assert!(prepare_socket_path(&path).await.is_err());
                assert!(path.exists());
            }
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                // 受限 CI / 沙箱可能禁止创建 UDS；路径判定已由普通文件分支覆盖。
            }
            Err(error) => panic!("创建测试 socket 失败：{error}"),
        }
    }
}
