//! 控制协议的唯一 canonical 定义。
//!
//! ctl 以此清单生成面向人的帮助、补全、别名和请求参数。
//!
//! Unix socket 上传输的内容使用本文件定义的结构化请求。人类命令名、短别名
//! 和参数语法只在 ctl 中存在；bot 收到的是枚举操作，不解析人类输入。

use serde::{Deserialize, Serialize};

pub const CONTROL_PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlOperation {
    Status,
    Help,
    ReloadConfig,
    Shutdown,
    CharacterList,
    CharacterShow,
    CharacterCardList,
    CharacterCardEnable,
    CharacterCardDisable,
    CharacterCardReassign,
    ConversationList,
    CharacterStateGet,
    CharacterStateSet,
    CharacterStateRefresh,
    UserMerge,
    UserDelete,
    BlacklistList,
    BlacklistAdd,
    BlacklistRemove,
    BlacklistBan,
    MemoryList,
    MemoryDelete,
    MemoryUpdate,
    MemorySharingRequest,
    MemorySharingConfirm,
    MemorySharingCancel,
    ReplyUnknownList,
    ReplyUnknownCheck,
    ReplyUnknownRetry,
    SessionDeleteAll,
    SessionDeleteCharacter,
}

impl ControlOperation {
    pub const fn canonical_name(self) -> &'static str {
        match self {
            Self::Status => STATUS,
            Self::Help => HELP,
            Self::ReloadConfig => RELOAD_CONFIG,
            Self::Shutdown => SHUTDOWN,
            Self::CharacterList => CHARACTER_LIST,
            Self::CharacterShow => CHARACTER_SHOW,
            Self::CharacterCardList => CHARACTER_CARD_LIST,
            Self::CharacterCardEnable => CHARACTER_CARD_ENABLE,
            Self::CharacterCardDisable => CHARACTER_CARD_DISABLE,
            Self::CharacterCardReassign => CHARACTER_CARD_REASSIGN,
            Self::ConversationList => CONVERSATION_LIST,
            Self::CharacterStateGet => CHARACTER_STATE_GET,
            Self::CharacterStateSet => CHARACTER_STATE_SET,
            Self::CharacterStateRefresh => CHARACTER_STATE_REFRESH,
            Self::UserMerge => USER_MERGE,
            Self::UserDelete => USER_DELETE,
            Self::BlacklistList => BLACKLIST_LIST,
            Self::BlacklistAdd => BLACKLIST_ADD,
            Self::BlacklistRemove => BLACKLIST_REMOVE,
            Self::BlacklistBan => BLACKLIST_BAN,
            Self::MemoryList => MEMORY_LIST,
            Self::MemoryDelete => MEMORY_DELETE,
            Self::MemoryUpdate => MEMORY_UPDATE,
            Self::MemorySharingRequest => MEMORY_SHARING_REQUEST,
            Self::MemorySharingConfirm => MEMORY_SHARING_CONFIRM,
            Self::MemorySharingCancel => MEMORY_SHARING_CANCEL,
            Self::ReplyUnknownList => REPLY_UNKNOWN_LIST,
            Self::ReplyUnknownCheck => REPLY_UNKNOWN_CHECK,
            Self::ReplyUnknownRetry => REPLY_UNKNOWN_RETRY,
            Self::SessionDeleteAll => SESSION_DELETE_ALL,
            Self::SessionDeleteCharacter => SESSION_DELETE_CHARACTER,
        }
    }

    /// 按 canonical 命令名解析协议操作。人类别名先由 `canonicalize` 归一化。
    pub fn from_canonical_name(name: &str) -> Option<Self> {
        match name {
            STATUS => Some(Self::Status),
            HELP => Some(Self::Help),
            RELOAD_CONFIG => Some(Self::ReloadConfig),
            SHUTDOWN => Some(Self::Shutdown),
            CHARACTER_LIST => Some(Self::CharacterList),
            CHARACTER_SHOW => Some(Self::CharacterShow),
            CHARACTER_CARD_LIST => Some(Self::CharacterCardList),
            CHARACTER_CARD_ENABLE => Some(Self::CharacterCardEnable),
            CHARACTER_CARD_DISABLE => Some(Self::CharacterCardDisable),
            CHARACTER_CARD_REASSIGN => Some(Self::CharacterCardReassign),
            CONVERSATION_LIST => Some(Self::ConversationList),
            CHARACTER_STATE_GET => Some(Self::CharacterStateGet),
            CHARACTER_STATE_SET => Some(Self::CharacterStateSet),
            CHARACTER_STATE_REFRESH => Some(Self::CharacterStateRefresh),
            USER_MERGE => Some(Self::UserMerge),
            USER_DELETE => Some(Self::UserDelete),
            BLACKLIST_LIST => Some(Self::BlacklistList),
            BLACKLIST_ADD => Some(Self::BlacklistAdd),
            BLACKLIST_REMOVE => Some(Self::BlacklistRemove),
            BLACKLIST_BAN => Some(Self::BlacklistBan),
            MEMORY_LIST => Some(Self::MemoryList),
            MEMORY_DELETE => Some(Self::MemoryDelete),
            MEMORY_UPDATE => Some(Self::MemoryUpdate),
            MEMORY_SHARING_REQUEST => Some(Self::MemorySharingRequest),
            MEMORY_SHARING_CONFIRM => Some(Self::MemorySharingConfirm),
            MEMORY_SHARING_CANCEL => Some(Self::MemorySharingCancel),
            REPLY_UNKNOWN_LIST => Some(Self::ReplyUnknownList),
            REPLY_UNKNOWN_CHECK => Some(Self::ReplyUnknownCheck),
            REPLY_UNKNOWN_RETRY => Some(Self::ReplyUnknownRetry),
            SESSION_DELETE_ALL => Some(Self::SessionDeleteAll),
            SESSION_DELETE_CHARACTER => Some(Self::SessionDeleteCharacter),
            _ => None,
        }
    }

    /// 返回该操作接受的 JSON 参数字段及类型。此处定义服务端协议字段，
    /// ctl 负责把人类输入转换成这些字段；handler 继续负责业务约束。
    pub const fn params(self) -> &'static [ControlParamSpec] {
        use param_specs::*;
        use ControlOperation::*;
        match self {
            Status
            | Help
            | ReloadConfig
            | Shutdown
            | CharacterCardList
            | ConversationList
            | CharacterStateRefresh
            | BlacklistList
            | SessionDeleteAll => NONE,
            CharacterList => CHARACTER_LIST,
            CharacterShow | CharacterCardEnable | CharacterCardDisable => ID,
            CharacterCardReassign => REASSIGN,
            CharacterStateGet => STATE_GET,
            CharacterStateSet => STATE_SET,
            UserMerge => USER_MERGE,
            UserDelete => USER_DELETE,
            BlacklistAdd => BLACKLIST_ADD,
            BlacklistRemove => BLACKLIST_REMOVE,
            BlacklistBan => BLACKLIST_BAN,
            MemoryList => MEMORY_LIST,
            MemoryDelete => MEMORY_DELETE,
            MemoryUpdate => MEMORY_UPDATE,
            MemorySharingRequest | MemorySharingConfirm | MemorySharingCancel => MEMORY_SHARING,
            ReplyUnknownList => REPLY_UNKNOWN_LIST,
            ReplyUnknownCheck | ReplyUnknownRetry => MESSAGE_ID,
            SessionDeleteCharacter => CHARACTER_ID,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlParamKind {
    PositiveInteger,
    NonNegativeInteger,
    Number,
    String,
    Boolean,
    TargetType,
    MemoryType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlParamSpec {
    pub name: &'static str,
    pub kind: ControlParamKind,
}

mod param_specs {
    use super::{ControlParamKind as K, ControlParamSpec as P};

    const fn p(name: &'static str, kind: K) -> P {
        P { name, kind }
    }

    pub const NONE: &[P] = &[];
    pub const CHARACTER_LIST: &[P] = &[
        p("limit", K::PositiveInteger),
        p("offset", K::NonNegativeInteger),
    ];
    pub const ID: &[P] = &[p("id", K::PositiveInteger)];
    pub const REASSIGN: &[P] = &[
        p("old_id", K::PositiveInteger),
        p("new_id", K::PositiveInteger),
    ];
    pub const STATE_GET: &[P] = &[
        p("character_id", K::PositiveInteger),
        p("conversation_id", K::PositiveInteger),
    ];
    pub const STATE_SET: &[P] = &[
        p("character_id", K::PositiveInteger),
        p("conversation_id", K::PositiveInteger),
        p("energy", K::Number),
        p("stress", K::Number),
        p("mood", K::Number),
        p("activity", K::String),
    ];
    pub const USER_MERGE: &[P] = &[
        p("source_user_id", K::PositiveInteger),
        p("target_user_id", K::PositiveInteger),
    ];
    pub const USER_DELETE: &[P] = &[p("user_id", K::PositiveInteger)];
    pub const BLACKLIST_ADD: &[P] = &[
        p("target", K::String),
        p("target_type", K::TargetType),
        p("reason", K::String),
    ];
    pub const BLACKLIST_REMOVE: &[P] = &[p("target", K::String), p("target_type", K::TargetType)];
    pub const BLACKLIST_BAN: &[P] = &[
        p("target", K::String),
        p("target_type", K::TargetType),
        p("duration_secs", K::PositiveInteger),
        p("reason", K::String),
    ];
    pub const MEMORY_LIST: &[P] = &[
        p("character_id", K::PositiveInteger),
        p("conversation_id", K::PositiveInteger),
        p("memory_type", K::MemoryType),
        p("limit", K::PositiveInteger),
        p("raw", K::Boolean),
    ];
    pub const MEMORY_DELETE: &[P] = &[p("memory_id", K::PositiveInteger)];
    pub const MEMORY_UPDATE: &[P] = &[
        p("memory_id", K::PositiveInteger),
        p("instruction", K::String),
    ];
    pub const MEMORY_SHARING: &[P] = &[
        p("character_id", K::PositiveInteger),
        p("user_id", K::PositiveInteger),
        p("reason", K::String),
    ];
    pub const REPLY_UNKNOWN_LIST: &[P] = &[p("limit", K::PositiveInteger), p("raw", K::Boolean)];
    pub const MESSAGE_ID: &[P] = &[p("message_id", K::PositiveInteger)];
    pub const CHARACTER_ID: &[P] = &[p("character_id", K::PositiveInteger)];
}

/// 在命令 handler 前验证协议参数的对象形状、未知字段和字段类型。
/// 必填项及字段间关系仍由各 handler 的业务校验负责。
pub fn validate_request(request: &ControlRequest) -> Result<(), String> {
    let params = request
        .params
        .as_object()
        .ok_or_else(|| "params 必须是 JSON 对象".to_string())?;
    let operation = request.operation;
    let specs = operation.params();
    for (name, value) in params {
        if name == "confirm" {
            if !requires_confirmation(operation.canonical_name()) {
                return Err("参数 confirm 仅适用于需要确认的命令".into());
            }
            if !value.is_boolean() {
                return Err("参数 confirm 必须是布尔值".into());
            }
            continue;
        }
        let Some(spec) = specs.iter().find(|spec| spec.name == name) else {
            return Err(format!(
                "命令 {} 不接受参数 {name}",
                operation.canonical_name()
            ));
        };
        let valid = match spec.kind {
            ControlParamKind::PositiveInteger => value.as_i64().is_some_and(|v| v > 0),
            ControlParamKind::NonNegativeInteger => value.as_i64().is_some_and(|v| v >= 0),
            ControlParamKind::Number => value.as_f64().is_some(),
            ControlParamKind::String => value.as_str().is_some(),
            ControlParamKind::Boolean => value.is_boolean(),
            ControlParamKind::TargetType => {
                matches!(value.as_str(), Some("user" | "group"))
            }
            ControlParamKind::MemoryType => matches!(
                value.as_str(),
                Some("episodic" | "semantic" | "relationship" | "system")
            ),
        };
        if !valid {
            return Err(format!("参数 {name} 的类型或取值不正确"));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlRequest {
    pub operation: ControlOperation,
    #[serde(default)]
    pub params: serde_json::Value,
}

impl ControlRequest {
    pub fn new(operation: ControlOperation, params: serde_json::Value) -> Self {
        Self { operation, params }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlEnvelope {
    pub version: u16,
    pub id: u64,
    pub request: ControlRequest,
}

impl ControlEnvelope {
    pub fn new(id: u64, request: ControlRequest) -> Self {
        Self {
            version: CONTROL_PROTOCOL_VERSION,
            id,
            request,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlCommandSpec {
    pub name: &'static str,
    pub usage: &'static str,
    pub group: &'static str,
    pub description: &'static str,
    pub destructive: bool,
    pub requires_confirmation: bool,
    /// 位置参数的最小数量。
    pub positional_min: u8,
    /// 位置参数的最大数量。
    pub positional_max: u8,
    /// 允许的选项；`raw`/`confirm` 之外的选项必须在此列出。
    pub allowed_options: &'static [&'static str],
    /// 是否支持 `--raw`。
    pub supports_raw: bool,
}

/// 选项集合的便捷常量，供 specs() 复用。
pub mod options {
    pub const LIMIT: &[&str] = &["limit"];
    pub const LIMIT_OFFSET: &[&str] = &["limit", "offset"];
    pub const CONVERSATION: &[&str] = &["conversation"];
    pub const STATE_VALUES: &[&str] = &[
        "character",
        "conversation",
        "energy",
        "stress",
        "mood",
        "activity",
    ];
    pub const MEMORY_LIST: &[&str] = &["character", "conversation", "type", "limit"];
    pub const MEMORY_UPDATE: &[&str] = &["instruction"];
    pub const MEMORY_SHARING: &[&str] = &["character", "user", "reason"];
    pub const BLACKLIST_REASON: &[&str] = &["reason"];
    pub const BLACKLIST_BAN: &[&str] = &["duration", "reason"];
    pub const REPLY_UNKNOWN_LIST: &[&str] = &["limit"];
}

pub const STATUS: &str = "status";
pub const HELP: &str = "help";
pub const RELOAD_CONFIG: &str = "reload-config";
pub const SHUTDOWN: &str = "shutdown";
pub const CHARACTER_LIST: &str = "character list";
pub const CHARACTER_SHOW: &str = "character show";
pub const CHARACTER_CARD_LIST: &str = "character card list";
pub const CHARACTER_CARD_ENABLE: &str = "character card enable";
pub const CHARACTER_CARD_DISABLE: &str = "character card disable";
pub const CHARACTER_CARD_REASSIGN: &str = "character card reassign";
pub const CONVERSATION_LIST: &str = "conversation list";
pub const CHARACTER_STATE_GET: &str = "character state get";
pub const CHARACTER_STATE_SET: &str = "character state set";
pub const CHARACTER_STATE_REFRESH: &str = "character state refresh";
pub const USER_MERGE: &str = "user merge";
pub const USER_DELETE: &str = "user delete";
pub const BLACKLIST_LIST: &str = "blacklist list";
pub const BLACKLIST_ADD: &str = "blacklist add";
pub const BLACKLIST_REMOVE: &str = "blacklist remove";
pub const BLACKLIST_BAN: &str = "blacklist ban";
pub const MEMORY_LIST: &str = "memory list";
pub const MEMORY_DELETE: &str = "memory delete";
pub const MEMORY_UPDATE: &str = "memory update";
pub const MEMORY_SHARING_REQUEST: &str = "memory sharing request";
pub const MEMORY_SHARING_CONFIRM: &str = "memory sharing confirm";
pub const MEMORY_SHARING_CANCEL: &str = "memory sharing cancel";
pub const REPLY_UNKNOWN_LIST: &str = "reply unknown list";
pub const REPLY_UNKNOWN_CHECK: &str = "reply unknown check";
pub const REPLY_UNKNOWN_RETRY: &str = "reply unknown retry";
pub const SESSION_DELETE_ALL: &str = "session delete-all";
pub const SESSION_DELETE_CHARACTER: &str = "session delete-character";

pub fn specs() -> &'static [ControlCommandSpec] {
    &[
        ControlCommandSpec {
            name: STATUS,
            usage: "status [--raw]",
            group: "系统",
            description: "查询运行时状态",
            destructive: false,
            requires_confirmation: false,
            positional_min: 0,
            positional_max: 0,
            allowed_options: &[],
            supports_raw: true,
        },
        ControlCommandSpec {
            name: HELP,
            usage: "help",
            group: "系统",
            description: "显示所有可用命令",
            destructive: false,
            requires_confirmation: false,
            positional_min: 0,
            positional_max: 0,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: RELOAD_CONFIG,
            usage: "reload-config",
            group: "系统",
            description: "重新加载并校验配置",
            destructive: false,
            requires_confirmation: false,
            positional_min: 0,
            positional_max: 0,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: SHUTDOWN,
            usage: "shutdown --confirm",
            group: "系统",
            description: "优雅关闭运行时",
            destructive: true,
            requires_confirmation: true,
            positional_min: 0,
            positional_max: 0,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: CHARACTER_LIST,
            usage: "character list [--limit N] [--offset N]",
            group: "角色",
            description: "列出角色",
            destructive: false,
            requires_confirmation: false,
            positional_min: 0,
            positional_max: 0,
            allowed_options: options::LIMIT_OFFSET,
            supports_raw: true,
        },
        ControlCommandSpec {
            name: CHARACTER_SHOW,
            usage: "character show <id> [--raw]",
            group: "角色",
            description: "显示角色详情",
            destructive: false,
            requires_confirmation: false,
            positional_min: 1,
            positional_max: 1,
            allowed_options: &[],
            supports_raw: true,
        },
        ControlCommandSpec {
            name: CHARACTER_CARD_LIST,
            usage: "character card list [--raw]",
            group: "角色",
            description: "列出角色卡及启用状态",
            destructive: false,
            requires_confirmation: false,
            positional_min: 0,
            positional_max: 0,
            allowed_options: &[],
            supports_raw: true,
        },
        ControlCommandSpec {
            name: CHARACTER_CARD_ENABLE,
            usage: "character card enable <id>",
            group: "角色",
            description: "启用角色卡",
            destructive: false,
            requires_confirmation: false,
            positional_min: 1,
            positional_max: 1,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: CHARACTER_CARD_DISABLE,
            usage: "character card disable <id> --confirm",
            group: "角色",
            description: "停用角色卡",
            destructive: true,
            requires_confirmation: true,
            positional_min: 1,
            positional_max: 1,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: CHARACTER_CARD_REASSIGN,
            usage: "character card reassign <old-id> <new-id> --confirm",
            group: "角色",
            description: "修改角色卡 ID",
            destructive: true,
            requires_confirmation: true,
            positional_min: 2,
            positional_max: 2,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: CONVERSATION_LIST,
            usage: "conversation list",
            group: "会话",
            description: "列出会话",
            destructive: false,
            requires_confirmation: false,
            positional_min: 0,
            positional_max: 0,
            allowed_options: &[],
            supports_raw: true,
        },
        ControlCommandSpec {
            name: CHARACTER_STATE_GET,
            usage: "character state get <character-id> [--conversation <id>] [--raw]",
            group: "状态",
            description: "查询角色状态",
            destructive: false,
            requires_confirmation: false,
            positional_min: 1,
            positional_max: 1,
            allowed_options: options::CONVERSATION,
            supports_raw: true,
        },
        ControlCommandSpec {
            name: CHARACTER_STATE_SET,
            usage: "character state set [<character-id>] [--character <id>] [--conversation <id>] [--energy N] [--stress N] [--mood N] [--activity <text>] --confirm",
            group: "状态",
            description: "修改角色状态",
            destructive: true,
            requires_confirmation: true,
            positional_min: 0,
            positional_max: 1,
            allowed_options: options::STATE_VALUES,
            supports_raw: false,
        },
        ControlCommandSpec {
            name: CHARACTER_STATE_REFRESH,
            usage: "character state refresh",
            group: "状态",
            description: "刷新角色状态",
            destructive: false,
            requires_confirmation: false,
            positional_min: 0,
            positional_max: 0,
            allowed_options: &[],
            supports_raw: true,
        },
        ControlCommandSpec {
            name: USER_MERGE,
            usage: "user merge <source-user-id> <target-user-id> --confirm",
            group: "用户",
            description: "把一个全域用户的数据合并到另一个用户并删除来源用户",
            destructive: true,
            requires_confirmation: true,
            positional_min: 2,
            positional_max: 2,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: USER_DELETE,
            usage: "user delete <user-id> --confirm",
            group: "用户",
            description: "删除全域用户及其关联关系",
            destructive: true,
            requires_confirmation: true,
            positional_min: 1,
            positional_max: 1,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: BLACKLIST_LIST,
            usage: "blacklist list",
            group: "黑名单",
            description: "列出黑名单",
            destructive: false,
            requires_confirmation: false,
            positional_min: 0,
            positional_max: 0,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: BLACKLIST_ADD,
            usage: "blacklist add <qq-id> [--reason <text>] --confirm",
            group: "黑名单",
            description: "添加黑名单",
            destructive: true,
            requires_confirmation: true,
            positional_min: 1,
            positional_max: 1,
            allowed_options: options::BLACKLIST_REASON,
            supports_raw: false,
        },
        ControlCommandSpec {
            name: BLACKLIST_REMOVE,
            usage: "blacklist remove <qq-id> --confirm",
            group: "黑名单",
            description: "移除黑名单",
            destructive: true,
            requires_confirmation: true,
            positional_min: 1,
            positional_max: 1,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: BLACKLIST_BAN,
            usage: "blacklist ban <qq-id> --duration <dur> [--reason <text>] --confirm",
            group: "黑名单",
            description: "临时封禁用户",
            destructive: true,
            requires_confirmation: true,
            positional_min: 1,
            positional_max: 1,
            allowed_options: options::BLACKLIST_BAN,
            supports_raw: false,
        },
        ControlCommandSpec {
            name: MEMORY_LIST,
            usage: "memory list [--character <id>] [--conversation <id>] [--type <type>] [--limit N] [--raw]",
            group: "记忆",
            description: "查询记忆",
            destructive: false,
            requires_confirmation: false,
            positional_min: 0,
            positional_max: 0,
            allowed_options: options::MEMORY_LIST,
            supports_raw: true,
        },
        ControlCommandSpec {
            name: MEMORY_DELETE,
            usage: "memory delete <memory-id> --confirm",
            group: "记忆",
            description: "删除记忆",
            destructive: true,
            requires_confirmation: true,
            positional_min: 1,
            positional_max: 1,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: MEMORY_UPDATE,
            usage: "memory update <memory-id> --instruction <中文修改说明> --confirm",
            group: "记忆",
            description: "确认或更正记忆",
            destructive: true,
            requires_confirmation: true,
            positional_min: 1,
            positional_max: 1,
            allowed_options: options::MEMORY_UPDATE,
            supports_raw: false,
        },
        ControlCommandSpec {
            name: MEMORY_SHARING_REQUEST,
            usage: "memory sharing request <character-id> <user-id> [--reason <text>]",
            group: "记忆",
            description: "记录用户提出的跨会话记忆禁用请求",
            destructive: false,
            requires_confirmation: false,
            positional_min: 2,
            positional_max: 2,
            allowed_options: options::MEMORY_SHARING,
            supports_raw: false,
        },
        ControlCommandSpec {
            name: MEMORY_SHARING_CONFIRM,
            usage: "memory sharing confirm <character-id> <user-id> --confirm [--reason <text>]",
            group: "记忆",
            description: "确认跨会话记忆禁用请求",
            destructive: true,
            requires_confirmation: true,
            positional_min: 2,
            positional_max: 2,
            allowed_options: options::MEMORY_SHARING,
            supports_raw: false,
        },
        ControlCommandSpec {
            name: MEMORY_SHARING_CANCEL,
            usage: "memory sharing cancel <character-id> <user-id> --confirm [--reason <text>]",
            group: "记忆",
            description: "取消跨会话记忆禁用策略",
            destructive: true,
            requires_confirmation: true,
            positional_min: 2,
            positional_max: 2,
            allowed_options: options::MEMORY_SHARING,
            supports_raw: false,
        },
        ControlCommandSpec {
            name: REPLY_UNKNOWN_LIST,
            usage: "reply unknown list [--limit N] [--raw]",
            group: "发送",
            description: "列出发送结果未知、等待核查的回复任务",
            destructive: false,
            requires_confirmation: false,
            positional_min: 0,
            positional_max: 0,
            allowed_options: options::REPLY_UNKNOWN_LIST,
            supports_raw: true,
        },
        ControlCommandSpec {
            name: REPLY_UNKNOWN_CHECK,
            usage: "reply unknown check <message-id>",
            group: "发送",
            description: "查询平台是否存在未知结果对应的消息",
            destructive: false,
            requires_confirmation: false,
            positional_min: 1,
            positional_max: 1,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: REPLY_UNKNOWN_RETRY,
            usage: "reply unknown retry <message-id> --confirm",
            group: "发送",
            description: "确认平台未发送后重新排队回复任务",
            destructive: true,
            requires_confirmation: true,
            positional_min: 1,
            positional_max: 1,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: SESSION_DELETE_ALL,
            usage: "session delete-all --confirm",
            group: "会话",
            description: "删除全部会话数据",
            destructive: true,
            requires_confirmation: true,
            positional_min: 0,
            positional_max: 0,
            allowed_options: &[],
            supports_raw: false,
        },
        ControlCommandSpec {
            name: SESSION_DELETE_CHARACTER,
            usage: "session delete-character <character-id> --confirm",
            group: "会话",
            description: "删除角色关联会话数据",
            destructive: true,
            requires_confirmation: true,
            positional_min: 1,
            positional_max: 1,
            allowed_options: &[],
            supports_raw: false,
        },
    ]
}

/// 面向人的短命令。canonical 名称仍是协议的唯一名称，短命令只在 ctl
/// 展示和输入阶段展开，避免把内部命令名变成需要记忆的密码。
pub fn aliases(canonical: &str) -> &'static [&'static str] {
    match canonical {
        STATUS => &["s"],
        HELP => &["h", "?"],
        RELOAD_CONFIG => &["reload", "config reload", "cfg reload"],
        SHUTDOWN => &["stop"],
        CHARACTER_LIST => &["c list", "char list"],
        CHARACTER_SHOW => &["c show", "char show"],
        CHARACTER_CARD_LIST => &["c card list", "char card list"],
        CHARACTER_CARD_ENABLE => &["c card enable", "char card enable"],
        CHARACTER_CARD_DISABLE => &["c card disable", "char card disable"],
        CHARACTER_CARD_REASSIGN => &["c card reassign", "char card reassign"],
        CONVERSATION_LIST => &["session list", "ss list", "sessions"],
        CHARACTER_STATE_GET => &["state get", "st get"],
        CHARACTER_STATE_SET => &["state set", "st set"],
        CHARACTER_STATE_REFRESH => &["state refresh", "st refresh"],
        USER_MERGE => &["u merge"],
        USER_DELETE => &["u delete"],
        BLACKLIST_LIST => &["bl list"],
        BLACKLIST_ADD => &["bl add"],
        BLACKLIST_REMOVE => &["bl remove"],
        BLACKLIST_BAN => &["bl ban"],
        MEMORY_LIST => &["m list", "mem list"],
        MEMORY_DELETE => &["m delete", "mem delete"],
        MEMORY_UPDATE => &["m update", "mem update"],
        MEMORY_SHARING_REQUEST => &["m sharing request", "memory share request"],
        MEMORY_SHARING_CONFIRM => &["m sharing confirm", "memory share confirm"],
        MEMORY_SHARING_CANCEL => &["m sharing cancel", "memory share cancel"],
        REPLY_UNKNOWN_LIST => &["reply pending list"],
        REPLY_UNKNOWN_CHECK => &["reply pending check"],
        REPLY_UNKNOWN_RETRY => &["reply pending retry"],
        SESSION_DELETE_ALL => &["ss delete-all", "session clear-all"],
        SESSION_DELETE_CHARACTER => &["ss delete-character", "session clear-character"],
        _ => &[],
    }
}

/// 返回 canonical 命令名。ctl 用它把短命令归一化。
pub fn canonicalize(input: &str) -> Option<&'static str> {
    for spec in specs() {
        if spec.name == input || aliases(spec.name).contains(&input) {
            return Some(spec.name);
        }
    }
    None
}

pub fn canonical_name(input: &str) -> Option<&'static str> {
    specs()
        .iter()
        .find(|spec| spec.name == input)
        .map(|spec| spec.name)
}

pub fn requires_confirmation(input: &str) -> bool {
    canonical_name(input)
        .and_then(|name| specs().iter().find(|spec| spec.name == name))
        .is_some_and(|spec| spec.requires_confirmation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_names_share_confirmation_policy() {
        assert_eq!(
            canonical_name("character card disable"),
            Some(CHARACTER_CARD_DISABLE)
        );
        assert!(requires_confirmation("character card disable"));
        assert_eq!(canonical_name("character_card_disable"), None);
        assert!(!requires_confirmation("character_card_disable"));
        assert!(!requires_confirmation("character list"));
    }

    #[test]
    fn human_aliases_share_canonical_policy() {
        assert_eq!(canonicalize("s"), Some(STATUS));
        assert_eq!(canonicalize("m list"), Some(MEMORY_LIST));
        assert_eq!(canonicalize("ss delete-all"), Some(SESSION_DELETE_ALL));
        assert!(!requires_confirmation("ss delete-all"));
    }

    #[test]
    fn destructive_help_usage_requires_explicit_confirmation() {
        for spec in specs().iter().filter(|spec| spec.requires_confirmation) {
            assert!(
                spec.usage.contains("--confirm"),
                "危险命令帮助必须显示 --confirm: {}",
                spec.name
            );
        }
    }

    #[test]
    fn structured_envelope_roundtrips_without_human_command_text() {
        let envelope = ControlEnvelope::new(
            42,
            ControlRequest::new(
                ControlOperation::CharacterStateSet,
                serde_json::json!({"character_id": 7, "energy": 80.0}),
            ),
        );
        let encoded = serde_json::to_value(&envelope).expect("结构化请求应可序列化");
        assert_eq!(encoded["version"], CONTROL_PROTOCOL_VERSION);
        assert_eq!(encoded["request"]["operation"], "character_state_set");
        assert!(encoded.get("cmd").is_none());
        let decoded: ControlEnvelope =
            serde_json::from_value(encoded).expect("结构化请求应可反序列化");
        assert_eq!(decoded, envelope);
    }
}
