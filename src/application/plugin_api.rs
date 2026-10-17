//! Plugin API 分发层 —— 把插件的方法调用分发到领域/应用能力。
//!
//! 本模块实现方法权限校验（`plugin_data.*` 免权限但强制插件命名空间）、
//! 参数解析与各 API 方法的处理逻辑。错误统一返回中文 `String`，wire 直传。

use std::sync::Arc;

#[allow(unused_imports)]
use crate::adapters::onebot::ActionResponse;
use crate::application::action::ActionDispatcher;
use crate::application::cognition::CognitionLayer;
use crate::domain::behavior::Action;
use crate::domain::memory::{Memory, MemoryType};
use crate::domain::repository::{
    CharacterBindingRepository, CharacterRepository, CharacterStateRepository, MemoryRepository,
    MessageRepository, PluginDataRepository, RelationshipRepository,
};
use crate::infrastructure::llm::{LlmMessage, LlmRole};
use crate::infrastructure::plugin::permissions::check_permission;
use crate::infrastructure::plugin::registry::PluginRegistry;

/// Plugin API 分发器。
pub struct PluginApi {
    character_repo: Arc<dyn CharacterRepository>,
    state_repo: Arc<dyn CharacterStateRepository>,
    binding_repo: Arc<dyn CharacterBindingRepository>,
    message_repo: Arc<dyn MessageRepository>,
    memory_repo: Arc<dyn MemoryRepository>,
    relationship_repo: Arc<dyn RelationshipRepository>,
    plugin_data_repo: Arc<dyn PluginDataRepository>,
    dispatcher: Arc<ActionDispatcher>,
    cognition: Arc<CognitionLayer>,
    registry: Arc<PluginRegistry>,
    runtime: Option<Arc<crate::application::runtime::CharacterRuntime>>,
}

impl PluginApi {
    /// 创建一个 Plugin API 分发器。
    ///
    /// 依赖注入构造（10 个依赖均为必需），按设计签名展开，故允许该 lint。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        character_repo: Arc<dyn CharacterRepository>,
        state_repo: Arc<dyn CharacterStateRepository>,
        binding_repo: Arc<dyn CharacterBindingRepository>,
        message_repo: Arc<dyn MessageRepository>,
        memory_repo: Arc<dyn MemoryRepository>,
        relationship_repo: Arc<dyn RelationshipRepository>,
        plugin_data_repo: Arc<dyn PluginDataRepository>,
        dispatcher: Arc<ActionDispatcher>,
        cognition: Arc<CognitionLayer>,
        registry: Arc<PluginRegistry>,
    ) -> Self {
        Self {
            character_repo,
            state_repo,
            binding_repo,
            message_repo,
            memory_repo,
            relationship_repo,
            plugin_data_repo,
            dispatcher,
            cognition,
            registry,
            runtime: None,
        }
    }

    pub fn with_runtime(
        mut self,
        runtime: Arc<crate::application::runtime::CharacterRuntime>,
    ) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// 分发一个插件方法调用。
    ///
    /// 流程：插件注册校验 → 权限校验（`plugin_data.*` 免权限但强制插件
    /// 命名空间）→ 方法分发。错误统一为中文 `String`，wire 直传。
    pub async fn dispatch(
        &self,
        plugin_name: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        // 1. 插件必须已注册（注册表提供其被授予的权限）。
        let granted = self
            .registry
            .permissions_for(plugin_name)
            .ok_or_else(|| "插件未注册".to_string())?;

        // 2. `plugin_data.*` 免权限（数据自作用域），其余方法走权限判定。
        if !method.starts_with("plugin_data.") {
            check_permission(method, &granted)?;
        }

        if matches!(
            method,
            "message.send" | "character.state.write" | "memory.write" | "relationship.write"
        ) {
            return Err(format!("插件方法 {method} 当前禁止直接修改核心运行状态"));
        }

        // 3. 方法分发。
        match method {
            "message.send" => self.api_message_send(params).await,
            "message.read" => self.api_message_read(params).await,
            "character.read" => self.api_character_read(params).await,
            "character.state.read" => self.api_character_state_read(params).await,
            "character.state.write" => self.api_character_state_write(params).await,
            "binding.read" => self.api_binding_read(params).await,
            "memory.read" => self.api_memory_read(params).await,
            "memory.write" => self.api_memory_write(params).await,
            "relationship.read" => self.api_relationship_read(params).await,
            "relationship.write" => self.api_relationship_write(params).await,
            "plugin_data.get" => self.api_plugin_data_get(plugin_name, params).await,
            "plugin_data.set" => self.api_plugin_data_set(plugin_name, params).await,
            "plugin_data.delete" => self.api_plugin_data_delete(plugin_name, params).await,
            "plugin_data.list" => self.api_plugin_data_list(plugin_name, params).await,
            "llm.call" => self.api_llm_call(params).await,
            // 权限层已拦；此处兜底，防止绕过权限判定直入分发。
            "scheduler.create" => Err("scheduler.create 本期不开放".to_string()),
            other => Err(format!("未知方法：{other}")),
        }
    }

    // -----------------------------------------------------------------------
    // message.send
    // -----------------------------------------------------------------------

    async fn api_message_send(
        &self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let conversation_id = require_i64(&params, "conversation_id")?;
        let content = require_str(&params, "content")?;
        self.dispatcher
            .execute(&Action::SendMessage {
                conversation_id,
                content,
            })
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({}))
    }

    // -----------------------------------------------------------------------
    // message.read
    // -----------------------------------------------------------------------

    async fn api_message_read(
        &self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let conversation_id = require_i64(&params, "conversation_id")?;
        let limit = params.get("limit").and_then(|v| v.as_i64()).unwrap_or(50);
        let messages = self
            .message_repo
            .find_recent(conversation_id, limit)
            .await
            .map_err(|e| e.to_string())?;
        to_json(&messages)
    }

    // -----------------------------------------------------------------------
    // character.read
    // -----------------------------------------------------------------------

    async fn api_character_read(
        &self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let character_id = require_i64(&params, "character_id")?;
        let character = self
            .character_repo
            .find_by_id(character_id)
            .await
            .map_err(|e| e.to_string())?;
        match character {
            Some(c) => to_json(&c),
            None => Ok(serde_json::Value::Null),
        }
    }

    // -----------------------------------------------------------------------
    // character.state.read / character.state.write
    // -----------------------------------------------------------------------

    async fn api_character_state_read(
        &self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let character_id = require_i64(&params, "character_id")?;
        let state = self
            .state_repo
            .find_by_character_id(character_id)
            .await
            .map_err(|e| e.to_string())?;
        match state {
            Some(s) => to_json(&s),
            None => Ok(serde_json::Value::Null),
        }
    }

    async fn api_character_state_write(
        &self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let character_id = require_i64(&params, "character_id")?;
        // 角色必须存在。
        let character = self
            .character_repo
            .find_by_id(character_id)
            .await
            .map_err(|e| e.to_string())?;
        if character.is_none() {
            return Err("角色不存在".to_string());
        }

        let patch = params
            .get("state")
            .and_then(|v| v.as_object())
            .ok_or_else(|| "缺少参数：state（或 state 必须是对象）".to_string())?;

        for (key, value) in patch {
            let valid = match key.as_str() {
                "energy" | "stress" => value.is_number(),
                "current_activity" => value.is_null() || value.is_string(),
                _ => false,
            };
            if !valid {
                return Err(format!("非法状态字段或类型：{key}"));
            }
        }
        if let Some(runtime) = &self.runtime {
            return to_json(
                &runtime
                    .apply_state_patch(character_id, &serde_json::Value::Object(patch.clone()))
                    .await
                    .map_err(|error| error.to_string())?,
            );
        }
        // 读现有状态；无则用默认状态，再逐字段覆盖。
        let mut state = self
            .state_repo
            .find_by_character_id(character_id)
            .await
            .map_err(|e| e.to_string())?
            .unwrap_or_default();

        if let Some(v) = patch.get("energy") {
            state.energy = v
                .as_f64()
                .ok_or_else(|| "参数 energy 必须是数字".to_string())?;
        }
        if let Some(v) = patch.get("stress") {
            state.stress = v
                .as_f64()
                .ok_or_else(|| "参数 stress 必须是数字".to_string())?;
        }
        if let Some(v) = patch.get("current_activity") {
            state.current_activity = Some(
                v.as_str()
                    .ok_or_else(|| "参数 current_activity 必须是字符串".to_string())?
                    .to_string(),
            );
        }
        // 安全归一化：数值字段限制在 [0, 100]，并刷新最后更新时间。
        let mut state = state.clamped();
        state.last_updated = chrono::Utc::now();

        self.state_repo
            .upsert(character_id, &state)
            .await
            .map_err(|e| e.to_string())?;
        to_json(&state)
    }

    // -----------------------------------------------------------------------
    // binding.read
    // -----------------------------------------------------------------------

    async fn api_binding_read(
        &self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let conversation_id = require_i64(&params, "conversation_id")?;
        let bindings = self
            .binding_repo
            .find_by_conversation_id(conversation_id)
            .await
            .map_err(|e| e.to_string())?;
        to_json(&bindings)
    }

    // -----------------------------------------------------------------------
    // memory.read / memory.write
    // -----------------------------------------------------------------------

    async fn api_memory_read(
        &self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let character_id = require_i64(&params, "character_id")?;
        let conversation_id = require_i64(&params, "conversation_id")?;
        let raw = params
            .get("raw")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);

        // limit：默认 20，封顶 500。
        let limit = optional_i64(&params, "limit")?.unwrap_or(20).clamp(1, 500);

        // memory_type：大小写不敏感。
        let memory_type = match params.get("memory_type") {
            None | Some(serde_json::Value::Null) => None,
            Some(v) => {
                let s = v
                    .as_str()
                    .ok_or_else(|| "参数 memory_type 必须是字符串".to_string())?;
                Some(parse_memory_type(s)?)
            }
        };

        // 有 keywords 走关键词检索，否则按角色 + 类型过滤。
        let keywords = match params.get("keywords") {
            None | Some(serde_json::Value::Null) => None,
            Some(v) => {
                let arr = v
                    .as_array()
                    .ok_or_else(|| "参数 keywords 必须是字符串数组".to_string())?;
                let mut kws = Vec::with_capacity(arr.len());
                for item in arr {
                    kws.push(
                        item.as_str()
                            .ok_or_else(|| "参数 keywords 的元素必须是字符串".to_string())?
                            .to_string(),
                    );
                }
                Some(kws)
            }
        };

        // 插件只能读取当前会话可见的正式记忆，先由仓储按范围筛选。
        let mut memories = self
            .memory_repo
            .find_visible(character_id, conversation_id, 500)
            .await
            .map_err(|e| e.to_string())?;
        if let Some(memory_type) = memory_type {
            memories.retain(|memory| memory.memory_type == memory_type);
        }
        if let Some(keywords) = keywords {
            let keywords = keywords
                .into_iter()
                .map(|keyword| keyword.trim().to_lowercase())
                .filter(|keyword| !keyword.is_empty())
                .collect::<Vec<_>>();
            memories.retain(|memory| {
                keywords
                    .iter()
                    .any(|keyword| memory.content.to_lowercase().contains(keyword))
            });
        }
        memories.truncate(limit as usize);
        if raw {
            return to_json(&memories);
        }
        let redacted = memories
            .into_iter()
            .map(|memory| {
                let fingerprint = {
                    use std::hash::{Hash, Hasher};
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    memory.content.hash(&mut hasher);
                    format!("{:016x}", hasher.finish())
                };
                serde_json::json!({
                    "id": memory.id,
                    "character_id": memory.character_id,
                    "conversation_id": memory.conversation_id,
                    "memory_type": memory.memory_type,
                    "importance": memory.importance,
                    "metadata": memory.metadata,
                    "content_available": false,
                    "content_fingerprint": fingerprint,
                })
            })
            .collect::<Vec<_>>();
        Ok(serde_json::Value::Array(redacted))
    }

    async fn api_memory_write(
        &self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let character_id = require_i64(&params, "character_id")?;
        let content = require_str(&params, "content")?;
        let memory_type = match params.get("memory_type") {
            None | Some(serde_json::Value::Null) => MemoryType::Episodic,
            Some(v) => {
                let s = v
                    .as_str()
                    .ok_or_else(|| "参数 memory_type 必须是字符串".to_string())?;
                parse_memory_type(s)?
            }
        };
        let importance = optional_f64(&params, "importance")?
            .unwrap_or(0.5)
            .clamp(0.0, 1.0);
        let conversation_id = optional_i64(&params, "conversation_id")?;

        let memory = Memory::new(
            character_id,
            conversation_id,
            memory_type,
            content,
            importance,
        );
        let memory_id = self
            .memory_repo
            .insert(&memory)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "memory_id": memory_id }))
    }

    // -----------------------------------------------------------------------
    // relationship.read / relationship.write
    // -----------------------------------------------------------------------

    async fn api_relationship_read(
        &self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let character_id = require_i64(&params, "character_id")?;
        let user_id = require_i64(&params, "user_id")?;
        let relationship = self
            .relationship_repo
            .find_by_user_id(character_id, user_id)
            .await
            .map_err(|e| e.to_string())?;
        match relationship {
            Some(r) => to_json(&r),
            None => Ok(serde_json::Value::Null),
        }
    }

    async fn api_relationship_write(
        &self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let character_id = require_i64(&params, "character_id")?;
        let participant_id = require_i64(&params, "participant_id")?;

        // 读现有关系；无则用默认（陌生人）值，未提供的维度保持默认/既有值。
        let mut relationship = match self
            .relationship_repo
            .find(character_id, participant_id)
            .await
            .map_err(|e| e.to_string())?
        {
            Some(r) => r,
            None => self
                .relationship_repo
                .create_for_participant(character_id, participant_id)
                .await
                .map_err(|e| e.to_string())?,
        };

        if let Some(v) = optional_f64(&params, "familiarity")? {
            relationship.familiarity = v.clamp(0.0, 1.0);
        }
        if let Some(v) = optional_f64(&params, "affection")? {
            relationship.affection = v.clamp(0.0, 1.0);
        }
        if let Some(v) = optional_f64(&params, "trust")? {
            relationship.trust = v.clamp(0.0, 1.0);
        }
        if let Some(v) = optional_f64(&params, "respect")? {
            relationship.respect = v.clamp(0.0, 1.0);
        }
        if let Some(v) = optional_f64(&params, "annoyance")? {
            relationship.annoyance = v.clamp(0.0, 1.0);
        }
        relationship.updated_at = chrono::Utc::now();

        self.relationship_repo
            .upsert(&relationship)
            .await
            .map_err(|e| e.to_string())?;
        to_json(&relationship)
    }

    // -----------------------------------------------------------------------
    // plugin_data.* —— 免权限，但强制按插件名命名空间隔离
    // -----------------------------------------------------------------------

    async fn api_plugin_data_get(
        &self,
        plugin_name: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let key = require_str(&params, "key")?;
        let value = self
            .plugin_data_repo
            .get(plugin_name, &key)
            .await
            .map_err(|e| e.to_string())?;
        Ok(value.unwrap_or(serde_json::Value::Null))
    }

    async fn api_plugin_data_set(
        &self,
        plugin_name: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let key = require_str(&params, "key")?;
        let value = params
            .get("value")
            .ok_or_else(|| "缺少参数：value".to_string())?
            .clone();
        self.plugin_data_repo
            .set(plugin_name, &key, &value)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({}))
    }

    async fn api_plugin_data_delete(
        &self,
        plugin_name: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let key = require_str(&params, "key")?;
        // 先查存在性（trait 的 delete 不返回影响行数）。
        let existed = self
            .plugin_data_repo
            .get(plugin_name, &key)
            .await
            .map_err(|e| e.to_string())?
            .is_some();
        self.plugin_data_repo
            .delete(plugin_name, &key)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "deleted": existed }))
    }

    async fn api_plugin_data_list(
        &self,
        plugin_name: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let _ = params;
        let keys = self
            .plugin_data_repo
            .list_keys(plugin_name)
            .await
            .map_err(|e| e.to_string())?;
        to_json(&keys)
    }

    // -----------------------------------------------------------------------
    // llm.call
    // -----------------------------------------------------------------------

    async fn api_llm_call(&self, params: serde_json::Value) -> Result<serde_json::Value, String> {
        let system = optional_str(&params, "system")?;

        let messages_value = params
            .get("messages")
            .ok_or_else(|| "缺少参数：messages".to_string())?;
        let messages_array = messages_value
            .as_array()
            .ok_or_else(|| "参数 messages 必须是数组".to_string())?;
        if messages_array.is_empty() {
            return Err("messages 不能为空".to_string());
        }

        let mut messages = Vec::with_capacity(messages_array.len());
        for item in messages_array {
            let role = parse_llm_role(
                item.get("role")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "消息缺少 role 字段".to_string())?,
            )?;
            let content = item
                .get("content")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "消息缺少 content 字段".to_string())?
                .to_string();
            messages.push(LlmMessage { role, content });
        }

        let priority = optional_u8(&params, "priority")?.unwrap_or(2);
        match self
            .cognition
            .chat(system, messages, priority)
            .await
            .map_err(|e| e.to_string())?
        {
            Some(response) => to_json(&response),
            None => Err("LLM 未启用".to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// 参数解析辅助（错误均为中文，wire 直传）
// ---------------------------------------------------------------------------

/// 读取可选的 i64 字段；缺失或显式 `null` 视为 `None`，类型错误报中文错。
fn optional_i64(params: &serde_json::Value, key: &str) -> Result<Option<i64>, String> {
    match params.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(v) => v
            .as_i64()
            .map(Some)
            .ok_or_else(|| format!("参数 {key} 必须是整数")),
    }
}

/// 读取可选的 f64 字段。
fn optional_f64(params: &serde_json::Value, key: &str) -> Result<Option<f64>, String> {
    match params.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(v) => v
            .as_f64()
            .map(Some)
            .ok_or_else(|| format!("参数 {key} 必须是数字")),
    }
}

/// 读取可选的 u8 字段。
fn optional_u8(params: &serde_json::Value, key: &str) -> Result<Option<u8>, String> {
    match params.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(v) => v
            .as_u64()
            .and_then(|n| u8::try_from(n).ok())
            .map(Some)
            .ok_or_else(|| format!("参数 {key} 必须是 0..=255 的整数")),
    }
}

/// 读取可选的字符串字段。
fn optional_str(params: &serde_json::Value, key: &str) -> Result<Option<String>, String> {
    match params.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(v) => v
            .as_str()
            .map(|s| Some(s.to_string()))
            .ok_or_else(|| format!("参数 {key} 必须是字符串")),
    }
}

/// 读取必填的 i64 字段。
fn require_i64(params: &serde_json::Value, key: &str) -> Result<i64, String> {
    optional_i64(params, key)?.ok_or_else(|| format!("缺少参数：{key}"))
}

/// 读取必填的字符串字段。
fn require_str(params: &serde_json::Value, key: &str) -> Result<String, String> {
    optional_str(params, key)?.ok_or_else(|| format!("缺少参数：{key}"))
}

/// 把可序列化对象转为 JSON 值；序列化失败转中文错误。
fn to_json<T: serde::Serialize>(value: &T) -> Result<serde_json::Value, String> {
    serde_json::to_value(value).map_err(|e| format!("序列化失败：{e}"))
}

/// 解析记忆类型（episodic / semantic / relationship / system，大小写不敏感）。
fn parse_memory_type(s: &str) -> Result<MemoryType, String> {
    match s.trim().to_lowercase().as_str() {
        "episodic" => Ok(MemoryType::Episodic),
        "semantic" => Ok(MemoryType::Semantic),
        "relationship" => Ok(MemoryType::Relationship),
        "system" => Ok(MemoryType::System),
        other => Err(format!("未知记忆类型：{other}")),
    }
}

/// 解析 LLM 消息角色（user / assistant / system，大小写不敏感）。
fn parse_llm_role(s: &str) -> Result<LlmRole, String> {
    match s.trim().to_lowercase().as_str() {
        "user" => Ok(LlmRole::User),
        "assistant" => Ok(LlmRole::Assistant),
        "system" => Ok(LlmRole::System),
        other => Err(format!("未知消息角色：{other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::onebot::{OneBotAdapter, OneBotConnectionState};
    use crate::application::context::ContextBuilder;
    use crate::application::context_assembler::{
        ContextAssembler, ModelCapabilitiesProviderAdapter, TokenEstimatorAdapter,
    };
    use crate::application::llm_scheduler::LlmScheduler;
    use crate::domain::character::{Character, CharacterDefinition, CharacterState};
    use crate::domain::conversation::{Conversation, ConversationType};
    use crate::domain::message::Message;
    use crate::domain::relationship::Relationship;
    use crate::domain::repository::{
        CharacterBindingRepository, ConversationRepository, MessageRepository, MoodRepository,
    };
    use crate::error::{RepositoryError, RuntimeError};
    use crate::infrastructure::llm::{
        ApproximateTokenEstimator, FixedModelCapabilities, LlmRequest, LlmResponse, TokenUsage,
    };
    use crate::infrastructure::plugin::{PluginManifest, PluginPermission};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // -----------------------------------------------------------------------
    // 测试桩
    // -----------------------------------------------------------------------

    #[derive(Default)]
    struct MemCharacterRepo {
        characters: Mutex<Vec<Character>>,
    }
    #[async_trait]
    impl CharacterRepository for MemCharacterRepo {
        async fn find_by_id(&self, id: i64) -> Result<Option<Character>, RepositoryError> {
            Ok(self
                .characters
                .lock()
                .unwrap()
                .iter()
                .find(|c| c.id == id)
                .cloned())
        }
        async fn find_all(&self) -> Result<Vec<Character>, RepositoryError> {
            Ok(self.characters.lock().unwrap().clone())
        }
        async fn insert(&self, c: &Character) -> Result<i64, RepositoryError> {
            let mut chars = self.characters.lock().unwrap();
            let id = chars.len() as i64 + 1;
            let mut c = c.clone();
            c.id = id;
            chars.push(c);
            Ok(id)
        }
        async fn update(&self, c: &Character) -> Result<(), RepositoryError> {
            let mut chars = self.characters.lock().unwrap();
            if let Some(existing) = chars.iter_mut().find(|x| x.id == c.id) {
                *existing = c.clone();
            }
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemStateRepo {
        states: Mutex<HashMap<i64, CharacterState>>,
    }
    #[async_trait]
    impl CharacterStateRepository for MemStateRepo {
        async fn find_by_character_id(
            &self,
            character_id: i64,
        ) -> Result<Option<CharacterState>, RepositoryError> {
            Ok(self.states.lock().unwrap().get(&character_id).cloned())
        }
        async fn upsert(
            &self,
            character_id: i64,
            state: &CharacterState,
        ) -> Result<(), RepositoryError> {
            self.states
                .lock()
                .unwrap()
                .insert(character_id, state.clone());
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemMemoryRepo {
        memories: Mutex<Vec<Memory>>,
        last_limit: Mutex<Option<i64>>,
    }
    #[async_trait]
    impl MemoryRepository for MemMemoryRepo {
        async fn find_by_character_id(
            &self,
            character_id: i64,
            memory_type: Option<MemoryType>,
            limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            *self.last_limit.lock().unwrap() = Some(limit);
            Ok(self
                .memories
                .lock()
                .unwrap()
                .iter()
                .filter(|m| {
                    m.character_id == character_id && memory_type.is_none_or(|t| m.memory_type == t)
                })
                .cloned()
                .collect())
        }
        async fn search_by_keywords(
            &self,
            character_id: i64,
            keywords: &[String],
            limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            *self.last_limit.lock().unwrap() = Some(limit);
            Ok(self
                .memories
                .lock()
                .unwrap()
                .iter()
                .filter(|m| {
                    m.character_id == character_id
                        && keywords
                            .iter()
                            .any(|kw| m.content.to_lowercase().contains(&kw.to_lowercase()))
                })
                .cloned()
                .collect())
        }
        async fn insert(&self, m: &Memory) -> Result<i64, RepositoryError> {
            let mut mems = self.memories.lock().unwrap();
            let id = mems.len() as i64 + 1;
            let mut m = m.clone();
            m.id = id;
            mems.push(m);
            Ok(id)
        }
        async fn update(&self, _m: &Memory) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn find_by_id(&self, id: i64) -> Result<Option<Memory>, RepositoryError> {
            Ok(self
                .memories
                .lock()
                .unwrap()
                .iter()
                .find(|m| m.id == id)
                .cloned())
        }
        async fn find_all(&self, _limit: i64) -> Result<Vec<Memory>, RepositoryError> {
            Ok(Vec::new())
        }
        async fn find_by_conversation_id(
            &self,
            _conversation_id: i64,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(Vec::new())
        }
        async fn find_filtered(
            &self,
            _character_id: Option<i64>,
            _conversation_id: Option<i64>,
            _memory_type: Option<crate::domain::memory::MemoryType>,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(Vec::new())
        }
        async fn search_visible_by_embedding(
            &self,
            _character_id: i64,
            _conversation_id: i64,
            _user_id: Option<i64>,
            _query_embedding: &[f32],
            _memory_type: Option<&str>,
            _limit: i64,
        ) -> Result<Vec<crate::domain::memory::SemanticMatchResult>, RepositoryError> {
            Ok(Vec::new())
        }
        async fn insert_semantic(
            &self,
            _character_id: i64,
            _conversation_id: Option<i64>,
            _memory_type: &str,
            _content: &str,
            _embedding: &[f32],
            _importance: f64,
            _metadata: &str,
        ) -> Result<i64, RepositoryError> {
            Ok(1)
        }
    }

    #[derive(Default)]
    struct MemRelationshipRepo {
        rels: Mutex<Vec<Relationship>>,
    }
    #[async_trait]
    impl RelationshipRepository for MemRelationshipRepo {
        async fn find(
            &self,
            character_id: i64,
            participant_id: i64,
        ) -> Result<Option<Relationship>, RepositoryError> {
            Ok(self
                .rels
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.character_id == character_id && r.user_id == participant_id)
                .cloned())
        }
        async fn find_by_character_id(
            &self,
            character_id: i64,
        ) -> Result<Vec<Relationship>, RepositoryError> {
            Ok(self
                .rels
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.character_id == character_id)
                .cloned()
                .collect())
        }
        async fn find_by_user_id(
            &self,
            character_id: i64,
            user_id: i64,
        ) -> Result<Option<Relationship>, RepositoryError> {
            Ok(self
                .rels
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.character_id == character_id && r.user_id == user_id)
                .cloned())
        }
        async fn upsert(&self, relationship: &Relationship) -> Result<(), RepositoryError> {
            let mut all = self.rels.lock().unwrap();
            if let Some(existing) = all.iter_mut().find(|r| {
                r.character_id == relationship.character_id && r.user_id == relationship.user_id
            }) {
                *existing = relationship.clone();
            } else {
                all.push(relationship.clone());
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemPluginDataRepo {
        data: Mutex<HashMap<(String, String), serde_json::Value>>,
    }
    #[async_trait]
    impl PluginDataRepository for MemPluginDataRepo {
        async fn get(
            &self,
            plugin_name: &str,
            key: &str,
        ) -> Result<Option<serde_json::Value>, RepositoryError> {
            Ok(self
                .data
                .lock()
                .unwrap()
                .get(&(plugin_name.to_string(), key.to_string()))
                .cloned())
        }
        async fn set(
            &self,
            plugin_name: &str,
            key: &str,
            value: &serde_json::Value,
        ) -> Result<(), RepositoryError> {
            self.data
                .lock()
                .unwrap()
                .insert((plugin_name.to_string(), key.to_string()), value.clone());
            Ok(())
        }
        async fn delete(&self, plugin_name: &str, key: &str) -> Result<(), RepositoryError> {
            self.data
                .lock()
                .unwrap()
                .remove(&(plugin_name.to_string(), key.to_string()));
            Ok(())
        }
        async fn list_keys(&self, plugin_name: &str) -> Result<Vec<String>, RepositoryError> {
            let mut keys: Vec<String> = self
                .data
                .lock()
                .unwrap()
                .keys()
                .filter(|(p, _)| p == plugin_name)
                .map(|(_, k)| k.clone())
                .collect();
            keys.sort();
            Ok(keys)
        }
    }

    struct MemConvRepo {
        convs: Mutex<Vec<Conversation>>,
    }
    #[async_trait]
    impl ConversationRepository for MemConvRepo {
        async fn find_by_id(&self, id: i64) -> Result<Option<Conversation>, RepositoryError> {
            Ok(self
                .convs
                .lock()
                .unwrap()
                .iter()
                .find(|c| c.id == id)
                .cloned())
        }
        async fn find_by_external_id(
            &self,
            _id: &str,
        ) -> Result<Option<Conversation>, RepositoryError> {
            Ok(None)
        }
        async fn find_all(&self) -> Result<Vec<Conversation>, RepositoryError> {
            Ok(self.convs.lock().unwrap().clone())
        }
        async fn insert(&self, _c: &Conversation) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(&self, _c: &Conversation) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn allocate_sequence(&self, _conversation_id: i64) -> Result<i64, RepositoryError> {
            Ok(1) // Mock 实现
        }
    }

    /// 记录发出的群/私聊消息的假适配器。
    #[derive(Default)]
    struct FakeAdapter {
        sent_group: Mutex<Vec<(String, String)>>,
        sent_private: Mutex<Vec<(String, String)>>,
    }
    #[async_trait]
    impl OneBotAdapter for FakeAdapter {
        async fn start(&self) -> Result<(), RuntimeError> {
            Ok(())
        }
        async fn stop(&self) -> Result<(), RuntimeError> {
            Ok(())
        }
        async fn state(&self) -> OneBotConnectionState {
            OneBotConnectionState::Connected
        }
        async fn send_group_message(
            &self,
            group_id: &str,
            content: &str,
        ) -> Result<ActionResponse, RuntimeError> {
            self.sent_group
                .lock()
                .unwrap()
                .push((group_id.to_string(), content.to_string()));
            Ok(ActionResponse {
                id: 0,
                action: "send_group_msg".to_string(),
                status: "ok".to_string(),
                retcode: Some(0),
                data: None,
                error: None,
                message: None,
                wording: None,
            })
        }
        async fn send_private_message(
            &self,
            user_id: &str,
            content: &str,
        ) -> Result<ActionResponse, RuntimeError> {
            self.sent_private
                .lock()
                .unwrap()
                .push((user_id.to_string(), content.to_string()));
            Ok(ActionResponse {
                id: 0,
                action: "send_private_msg".to_string(),
                status: "ok".to_string(),
                retcode: Some(0),
                data: None,
                error: None,
                message: None,
                wording: None,
            })
        }
    }

    /// 记录请求并返回固定响应的假 LLM 调度器。
    struct FakeScheduler {
        submitted: Mutex<Vec<LlmRequest>>,
    }
    impl FakeScheduler {
        fn new() -> Self {
            Self {
                submitted: Mutex::new(Vec::new()),
            }
        }
        fn requests(&self) -> Vec<LlmRequest> {
            self.submitted.lock().unwrap().clone()
        }
    }
    #[async_trait]
    impl LlmScheduler for FakeScheduler {
        async fn submit(&self, request: LlmRequest) -> Result<LlmResponse, RuntimeError> {
            self.submitted.lock().unwrap().push(request);
            Ok(LlmResponse {
                content: "插件回复".to_string(),
                model: "fake".to_string(),
                usage: TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                },
                truncated: false,
            })
        }
    }

    struct MemMessageRepo;
    #[async_trait]
    impl MessageRepository for MemMessageRepo {
        async fn find_by_id(&self, _id: i64) -> Result<Option<Message>, RepositoryError> {
            Ok(None)
        }
        async fn find_recent(
            &self,
            _conversation_id: i64,
            _limit: i64,
        ) -> Result<Vec<Message>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_recent_attributed(
            &self,
            _conversation_id: i64,
            _character_id: i64,
            _limit: i64,
        ) -> Result<Vec<Message>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert(&self, _m: &Message) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn latest_message_time(
            &self,
            _conversation_id: i64,
        ) -> Result<Option<chrono::DateTime<chrono::Utc>>, RepositoryError> {
            Ok(None)
        }
    }

    struct MemMoodRepo;
    #[async_trait]
    impl MoodRepository for MemMoodRepo {
        async fn find_by_character_and_conversation(
            &self,
            _character_id: i64,
            _conversation_id: i64,
        ) -> Result<Option<crate::domain::emotion::Mood>, RepositoryError> {
            Ok(None)
        }
        async fn upsert(
            &self,
            _character_id: i64,
            _conversation_id: i64,
            _state: &crate::domain::emotion::Mood,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    struct MemBindingRepo;
    #[async_trait]
    impl CharacterBindingRepository for MemBindingRepo {
        async fn find_by_character_id(
            &self,
            _character_id: i64,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_by_conversation_id(
            &self,
            _conversation_id: i64,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_all(
            &self,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn find_all_enabled(
            &self,
        ) -> Result<Vec<crate::domain::character::CharacterBinding>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert(
            &self,
            _b: &crate::domain::character::CharacterBinding,
        ) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(
            &self,
            _b: &crate::domain::character::CharacterBinding,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    // -----------------------------------------------------------------------
    // 装配辅助
    // -----------------------------------------------------------------------

    fn manifest(name: &str, permissions: Vec<PluginPermission>) -> PluginManifest {
        PluginManifest {
            name: name.to_string(),
            version: "0.1.0".to_string(),
            description: "示例插件".to_string(),
            permissions,
            executable: "plugin-bin".to_string(),
            config: serde_json::json!({}),
        }
    }

    fn registry_with_plugins() -> Arc<PluginRegistry> {
        let registry = Arc::new(PluginRegistry::new());
        registry
            .register(manifest(
                "alpha",
                vec![
                    PluginPermission::MessageSend,
                    PluginPermission::MessageRead,
                    PluginPermission::CharacterRead,
                    PluginPermission::CharacterStateRead,
                    PluginPermission::CharacterStateWrite,
                    PluginPermission::BindingRead,
                    PluginPermission::MemoryRead,
                    PluginPermission::MemoryWrite,
                    PluginPermission::RelationshipRead,
                    PluginPermission::RelationshipWrite,
                    PluginPermission::LlmCall,
                ],
            ))
            .unwrap();
        registry
            .register(manifest("beta", vec![PluginPermission::MemoryRead]))
            .unwrap();
        registry.register(manifest("gamma", vec![])).unwrap();
        registry
    }

    fn sample_character(id: i64, name: &str) -> Character {
        Character {
            id,
            definition: CharacterDefinition {
                name: name.to_string(),
                description: Some("测试角色".to_string()),
                personality: None,
                scenario: None,
                style: None,
                background: None,
                greetings: vec![],
                example_messages: vec![],
                system_prompt: None,
                post_history_instructions: None,
                lorebook: vec![],
                metadata: serde_json::json!({}),
            },
            enabled: true,
            state: CharacterState::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[allow(dead_code)]
    struct Harness {
        api: PluginApi,
        character_repo: Arc<MemCharacterRepo>,
        binding_repo: Arc<MemBindingRepo>,
        message_repo: Arc<MemMessageRepo>,
        memory_repo: Arc<MemMemoryRepo>,
        relationship_repo: Arc<MemRelationshipRepo>,
        conv_repo: Arc<MemConvRepo>,
        adapter: Arc<FakeAdapter>,
        scheduler: Arc<FakeScheduler>,
    }

    async fn build_harness(llm_enabled: bool) -> Harness {
        let character_repo = Arc::new(MemCharacterRepo::default());
        let state_repo = Arc::new(MemStateRepo::default());
        let binding_repo = Arc::new(MemBindingRepo);
        let message_repo = Arc::new(MemMessageRepo);
        let memory_repo = Arc::new(MemMemoryRepo::default());
        let relationship_repo = Arc::new(MemRelationshipRepo::default());
        let plugin_data_repo = Arc::new(MemPluginDataRepo::default());
        let conv_repo = Arc::new(MemConvRepo {
            convs: Mutex::new(vec![]),
        });
        let adapter = Arc::new(FakeAdapter::default());
        let scheduler = Arc::new(FakeScheduler::new());

        let dispatcher = Arc::new(ActionDispatcher::new(
            conv_repo.clone() as Arc<dyn ConversationRepository>,
            adapter.clone() as Arc<dyn OneBotAdapter>,
        ));

        let context_builder = Arc::new(ContextBuilder::new(
            message_repo.clone(),
            Arc::new(
                crate::application::character_history::RepositoryCharacterHistoryProvider::new(
                    message_repo.clone(),
                ),
            ),
            conv_repo.clone() as Arc<dyn ConversationRepository>,
            memory_repo.clone() as Arc<dyn MemoryRepository>,
            relationship_repo.clone() as Arc<dyn RelationshipRepository>,
            Arc::new(MemMoodRepo),
            binding_repo.clone(),
        ));

        // Phase 3: ContextAssembler for Token Budget + Layered Degradation
        let token_estimator = Arc::new(TokenEstimatorAdapter(Arc::new(ApproximateTokenEstimator)));
        let model_capabilities = Arc::new(ModelCapabilitiesProviderAdapter(Arc::new(
            FixedModelCapabilities::new(None, None),
        )));
        let context_assembler =
            Arc::new(ContextAssembler::new(token_estimator, model_capabilities));

        let cognition = Arc::new(CognitionLayer::new(
            if llm_enabled {
                Some(scheduler.clone() as Arc<dyn LlmScheduler>)
            } else {
                None
            },
            context_builder,
            context_assembler,
        ));

        let api = PluginApi::new(
            character_repo.clone() as Arc<dyn CharacterRepository>,
            state_repo.clone() as Arc<dyn CharacterStateRepository>,
            binding_repo.clone() as Arc<dyn CharacterBindingRepository>,
            message_repo.clone() as Arc<dyn MessageRepository>,
            memory_repo.clone() as Arc<dyn MemoryRepository>,
            relationship_repo.clone() as Arc<dyn RelationshipRepository>,
            plugin_data_repo.clone() as Arc<dyn PluginDataRepository>,
            dispatcher,
            cognition,
            registry_with_plugins(),
        );

        Harness {
            api,
            character_repo,
            binding_repo,
            message_repo,
            memory_repo,
            relationship_repo,
            conv_repo,
            adapter,
            scheduler,
        }
    }

    fn group_conversation(id: i64, external_id: &str) -> Conversation {
        Conversation {
            id,
            conversation_type: ConversationType::Group,
            external_id: external_id.to_string(),
            name: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    // -----------------------------------------------------------------------
    // message.send
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn message_send_is_rejected_for_plugins() {
        let h = build_harness(true).await;
        h.conv_repo
            .convs
            .lock()
            .unwrap()
            .push(group_conversation(1, "12345"));

        let err = h
            .api
            .dispatch(
                "alpha",
                "message.send",
                serde_json::json!({ "conversation_id": 1, "content": "你好，插件消息" }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("禁止直接修改核心运行状态"));
        assert!(h.adapter.sent_group.lock().unwrap().is_empty());
        assert!(h.adapter.sent_private.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn message_send_unknown_conversation_translates_error() {
        let h = build_harness(true).await;
        let err = h
            .api
            .dispatch(
                "alpha",
                "message.send",
                serde_json::json!({ "conversation_id": 99, "content": "hi" }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("禁止直接修改核心运行状态"), "错误信息：{err}");
    }

    // -----------------------------------------------------------------------
    // 权限 / 校验
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn dispatch_rejects_missing_permission() {
        let h = build_harness(true).await;
        // beta 只有 memory.read 权限。
        let err = h
            .api
            .dispatch(
                "beta",
                "message.send",
                serde_json::json!({ "conversation_id": 1, "content": "x" }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("权限不足"), "错误信息：{err}");
        assert!(err.contains("message.send"), "错误信息应含所需权限：{err}");
    }

    #[tokio::test]
    async fn dispatch_unknown_method_rejected() {
        let h = build_harness(true).await;
        let err = h
            .api
            .dispatch("alpha", "no.such.method", serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(err, "未知方法：no.such.method");
    }

    #[tokio::test]
    async fn dispatch_unregistered_plugin_rejected() {
        let h = build_harness(true).await;
        let err = h
            .api
            .dispatch("stranger", "character.read", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(err.contains("插件未注册"), "错误信息：{err}");
    }

    #[tokio::test]
    async fn dispatch_param_errors_are_chinese() {
        let h = build_harness(true).await;
        // 缺少必填参数。
        let err = h
            .api
            .dispatch(
                "alpha",
                "message.send",
                serde_json::json!({ "conversation_id": 1 }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("禁止直接修改核心运行状态"), "错误信息：{err}");
        // 参数类型错误。
        let err = h
            .api
            .dispatch(
                "alpha",
                "message.send",
                serde_json::json!({ "conversation_id": "abc", "content": "x" }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("禁止直接修改核心运行状态"), "错误信息：{err}");
        // state 非对象。
        h.character_repo
            .insert(&sample_character(1, "Alice"))
            .await
            .unwrap();
        let err = h
            .api
            .dispatch(
                "alpha",
                "character.state.write",
                serde_json::json!({ "character_id": 1, "state": "bad" }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("禁止直接修改核心运行状态"), "错误信息：{err}");
        // 非对象 params 不 panic（当作空对象 → 缺少必填参数）。
        let err = h
            .api
            .dispatch("alpha", "memory.read", serde_json::json!(5))
            .await
            .unwrap_err();
        assert!(err.contains("缺少参数"), "错误信息：{err}");
    }

    #[tokio::test]
    async fn message_read_requires_conversation_id() {
        let h = build_harness(true).await;
        let err = h
            .api
            .dispatch("alpha", "message.read", serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(err, "缺少参数：conversation_id");
    }

    #[tokio::test]
    async fn scheduler_create_rejected() {
        let h = build_harness(true).await;
        let err = h
            .api
            .dispatch("alpha", "scheduler.create", serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(err, "scheduler.create 本期不开放");
    }

    // -----------------------------------------------------------------------
    // character.read / character.state.read / character.state.write
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn character_read_requires_scope_and_reads_by_id() {
        let h = build_harness(true).await;
        h.character_repo
            .insert(&sample_character(1, "Alice"))
            .await
            .unwrap();
        h.character_repo
            .insert(&sample_character(2, "Bob"))
            .await
            .unwrap();

        let missing_scope = h
            .api
            .dispatch("alpha", "character.read", serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(missing_scope, "缺少参数：character_id");

        let one = h
            .api
            .dispatch(
                "alpha",
                "character.read",
                serde_json::json!({ "character_id": 1 }),
            )
            .await
            .unwrap();
        assert_eq!(one["definition"]["name"], "Alice");

        let none = h
            .api
            .dispatch(
                "alpha",
                "character.read",
                serde_json::json!({ "character_id": 99 }),
            )
            .await
            .unwrap();
        assert!(none.is_null(), "未知角色应返回 null");
    }

    #[tokio::test]
    async fn character_state_read_null_then_value() {
        let h = build_harness(true).await;
        h.character_repo
            .insert(&sample_character(1, "Alice"))
            .await
            .unwrap();

        let none = h
            .api
            .dispatch(
                "alpha",
                "character.state.read",
                serde_json::json!({ "character_id": 1 }),
            )
            .await
            .unwrap();
        assert!(none.is_null(), "尚未写入状态应返回 null");

        let err = h.api
            .dispatch(
                "alpha",
                "character.state.write",
                serde_json::json!({ "character_id": 1, "state": { "energy": 80.0, "stress": 20.0 } }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("禁止直接修改核心运行状态"));
    }

    #[tokio::test]
    async fn binding_read_requires_conversation_scope() {
        let h = build_harness(true).await;
        let err = h
            .api
            .dispatch("alpha", "binding.read", serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(err, "缺少参数：conversation_id");
    }

    #[tokio::test]
    async fn character_state_write_partial_patch_merge_and_clamp() {
        let h = build_harness(true).await;
        h.character_repo
            .insert(&sample_character(1, "Alice"))
            .await
            .unwrap();

        let err = h
            .api
            .dispatch(
                "alpha",
                "character.state.write",
                serde_json::json!({ "character_id": 1, "state": { "energy": 250.0 } }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("禁止直接修改核心运行状态"));
    }

    // -----------------------------------------------------------------------
    // memory.read / memory.write
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn memory_read_filters_and_searches() {
        let h = build_harness(true).await;
        h.memory_repo
            .insert(&Memory::new(
                1,
                Some(1),
                MemoryType::Semantic,
                "用户喜欢猫".to_string(),
                0.8,
            ))
            .await
            .unwrap();
        h.memory_repo
            .insert(&Memory::new(
                1,
                Some(1),
                MemoryType::Episodic,
                "用户今天散步".to_string(),
                0.6,
            ))
            .await
            .unwrap();
        let mut hidden = Memory::new(
            1,
            Some(2),
            MemoryType::Episodic,
            "另一个会话的私密经历".to_string(),
            1.0,
        );
        hidden.metadata["status"] = serde_json::Value::String("candidate".to_string());
        h.memory_repo.insert(&hidden).await.unwrap();

        // 按类型过滤（大小写不敏感）。
        let semantic = h
            .api
            .dispatch(
                "alpha",
                "memory.read",
                serde_json::json!({ "character_id": 1, "conversation_id": 1, "memory_type": "SEMANTIC" }),
            )
            .await
            .unwrap();
        let arr = semantic.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["memory_type"], "Semantic");

        let visible = h
            .api
            .dispatch(
                "alpha",
                "memory.read",
                serde_json::json!({ "character_id": 1, "conversation_id": 1 }),
            )
            .await
            .unwrap();
        assert_eq!(visible.as_array().unwrap().len(), 2);
        assert!(visible
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["content_available"] == false));

        // 按关键词走 search_by_keywords。
        let hits = h
            .api
            .dispatch(
                "alpha",
                "memory.read",
                serde_json::json!({ "character_id": 1, "conversation_id": 1, "keywords": ["猫"], "raw": true }),
            )
            .await
            .unwrap();
        let arr = hits.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert!(arr[0]["content"].as_str().unwrap().contains("猫"));

        // 非法 memory_type → 中文错误。
        let err = h
            .api
            .dispatch(
                "alpha",
                "memory.read",
                serde_json::json!({ "character_id": 1, "conversation_id": 1, "memory_type": "bogus" }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("未知记忆类型"), "错误信息：{err}");
    }

    #[tokio::test]
    async fn memory_read_limit_default_and_cap() {
        let h = build_harness(true).await;
        let _ = h
            .api
            .dispatch(
                "alpha",
                "memory.read",
                serde_json::json!({ "character_id": 1, "conversation_id": 1 }),
            )
            .await
            .unwrap();
        assert_eq!(
            *h.memory_repo.last_limit.lock().unwrap(),
            Some(i64::MAX),
            "默认读取仍先按仓储可见范围过滤"
        );

        let _ = h
            .api
            .dispatch(
                "alpha",
                "memory.read",
                serde_json::json!({ "character_id": 1, "conversation_id": 1, "limit": 9999 }),
            )
            .await
            .unwrap();
        assert_eq!(
            *h.memory_repo.last_limit.lock().unwrap(),
            Some(i64::MAX),
            "可见范围过滤在默认仓储实现中先读取角色范围再截断"
        );
    }

    #[tokio::test]
    async fn memory_write_is_rejected_for_plugins() {
        let h = build_harness(true).await;
        let err = h
            .api
            .dispatch(
                "alpha",
                "memory.write",
                serde_json::json!({ "character_id": 1, "content": "用户喜欢茶" }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("禁止直接修改核心运行状态"));
        assert!(h.memory_repo.memories.lock().unwrap().is_empty());
    }

    // -----------------------------------------------------------------------
    // relationship.read / relationship.write
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn relationship_read_requires_global_user_scope() {
        let h = build_harness(true).await;
        h.relationship_repo
            .rels
            .lock()
            .unwrap()
            .push(Relationship::new(1, 10));
        h.relationship_repo
            .rels
            .lock()
            .unwrap()
            .push(Relationship::new(1, 11));

        let missing_user = h
            .api
            .dispatch(
                "alpha",
                "relationship.read",
                serde_json::json!({ "character_id": 1 }),
            )
            .await
            .unwrap_err();
        assert_eq!(missing_user, "缺少参数：user_id");

        let one = h
            .api
            .dispatch(
                "alpha",
                "relationship.read",
                serde_json::json!({ "character_id": 1, "user_id": 11 }),
            )
            .await
            .unwrap();
        assert_eq!(one["user_id"], 11);

        let none = h
            .api
            .dispatch(
                "alpha",
                "relationship.read",
                serde_json::json!({ "character_id": 1, "user_id": 99 }),
            )
            .await
            .unwrap();
        assert!(none.is_null(), "未知关系应返回 null");
    }

    #[tokio::test]
    async fn relationship_write_is_rejected_for_plugins() {
        let h = build_harness(true).await;
        let err = h
            .api
            .dispatch(
                "alpha",
                "relationship.write",
                serde_json::json!({ "character_id": 1, "participant_id": 10, "familiarity": 0.5 }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("禁止直接修改核心运行状态"));
    }

    // -----------------------------------------------------------------------
    // plugin_data.*
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn plugin_data_roundtrip_and_scope_isolation() {
        let h = build_harness(true).await;

        // set → {}；get → 值。
        let r = h
            .api
            .dispatch(
                "alpha",
                "plugin_data.set",
                serde_json::json!({ "key": "counter", "value": 42 }),
            )
            .await
            .unwrap();
        assert_eq!(r, serde_json::json!({}));
        let v = h
            .api
            .dispatch(
                "alpha",
                "plugin_data.get",
                serde_json::json!({ "key": "counter" }),
            )
            .await
            .unwrap();
        assert_eq!(v, serde_json::json!(42));

        // 复杂 JSON 值。
        h.api
            .dispatch(
                "alpha",
                "plugin_data.set",
                serde_json::json!({ "key": "cfg", "value": { "a": [1, 2, 3] } }),
            )
            .await
            .unwrap();
        let cfg = h
            .api
            .dispatch(
                "alpha",
                "plugin_data.get",
                serde_json::json!({ "key": "cfg" }),
            )
            .await
            .unwrap();
        assert_eq!(cfg["a"][1], 2);

        // list → 键数组。
        let keys = h
            .api
            .dispatch("alpha", "plugin_data.list", serde_json::json!({}))
            .await
            .unwrap();
        let mut keys: Vec<String> = keys
            .as_array()
            .unwrap()
            .iter()
            .map(|k| k.as_str().unwrap().to_string())
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["cfg", "counter"]);

        // delete → deleted 标志；不存在时 false。
        let del = h
            .api
            .dispatch(
                "alpha",
                "plugin_data.delete",
                serde_json::json!({ "key": "counter" }),
            )
            .await
            .unwrap();
        assert_eq!(del["deleted"], true);
        let del2 = h
            .api
            .dispatch(
                "alpha",
                "plugin_data.delete",
                serde_json::json!({ "key": "counter" }),
            )
            .await
            .unwrap();
        assert_eq!(del2["deleted"], false);

        // 作用域隔离：beta 读不到 / 写不进 alpha 的数据。
        let beta_get = h
            .api
            .dispatch(
                "beta",
                "plugin_data.get",
                serde_json::json!({ "key": "cfg" }),
            )
            .await
            .unwrap();
        assert!(beta_get.is_null(), "beta 不能读 alpha 的数据");
        h.api
            .dispatch(
                "beta",
                "plugin_data.set",
                serde_json::json!({ "key": "cfg", "value": "beta-own" }),
            )
            .await
            .unwrap();
        let alpha_still = h
            .api
            .dispatch(
                "alpha",
                "plugin_data.get",
                serde_json::json!({ "key": "cfg" }),
            )
            .await
            .unwrap();
        assert_eq!(alpha_still["a"][1], 2, "beta 写入不得影响 alpha");
        let beta_own = h
            .api
            .dispatch(
                "beta",
                "plugin_data.get",
                serde_json::json!({ "key": "cfg" }),
            )
            .await
            .unwrap();
        assert_eq!(beta_own, serde_json::json!("beta-own"));
    }

    // -----------------------------------------------------------------------
    // llm.call
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn llm_call_success_and_request_passthrough() {
        let h = build_harness(true).await;
        let r = h
            .api
            .dispatch(
                "alpha",
                "llm.call",
                serde_json::json!({
                    "system": "你是助手",
                    "messages": [
                        { "role": "user", "content": "你好" },
                        { "role": "assistant", "content": "嗨" },
                        { "role": "system", "content": "背景" },
                    ],
                    "priority": 0,
                }),
            )
            .await
            .unwrap();
        assert_eq!(r["content"], "插件回复");
        assert_eq!(r["model"], "fake");
        assert_eq!(r["truncated"], false);

        let reqs = h.scheduler.requests();
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].system.as_deref(), Some("你是助手"));
        assert_eq!(reqs[0].messages.len(), 3);
        assert_eq!(reqs[0].messages[0].role, LlmRole::User);
        assert_eq!(reqs[0].messages[1].role, LlmRole::Assistant);
        assert_eq!(reqs[0].messages[2].role, LlmRole::System);
        assert_eq!(reqs[0].priority, 0);

        // 未提供 priority 时默认 2。
        let _ = h
            .api
            .dispatch(
                "alpha",
                "llm.call",
                serde_json::json!({ "messages": [{ "role": "user", "content": "hi" }] }),
            )
            .await
            .unwrap();
        assert_eq!(h.scheduler.requests().len(), 2);
        assert_eq!(h.scheduler.requests()[1].priority, 2);
    }

    #[tokio::test]
    async fn llm_call_validation_errors() {
        let h = build_harness(true).await;
        // 非法角色。
        let err = h
            .api
            .dispatch(
                "alpha",
                "llm.call",
                serde_json::json!({ "messages": [{ "role": "bogus", "content": "x" }] }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("未知消息角色"), "错误信息：{err}");
        // 空 messages。
        let err = h
            .api
            .dispatch("alpha", "llm.call", serde_json::json!({ "messages": [] }))
            .await
            .unwrap_err();
        assert!(err.contains("不能为空"), "错误信息：{err}");
        // messages 非数组。
        let err = h
            .api
            .dispatch(
                "alpha",
                "llm.call",
                serde_json::json!({ "messages": { "role": "user" } }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("必须是数组"), "错误信息：{err}");
    }

    #[tokio::test]
    async fn llm_call_without_scheduler_reports_not_enabled() {
        let h = build_harness(false).await;
        let err = h
            .api
            .dispatch(
                "alpha",
                "llm.call",
                serde_json::json!({ "messages": [{ "role": "user", "content": "hi" }] }),
            )
            .await
            .unwrap_err();
        assert_eq!(err, "LLM 未启用");
    }
}
