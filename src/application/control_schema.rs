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
    BlacklistList,
    BlacklistAdd,
    BlacklistRemove,
    BlacklistBan,
    MemoryList,
    MemoryDelete,
    MemoryUpdate,
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
            Self::BlacklistList => BLACKLIST_LIST,
            Self::BlacklistAdd => BLACKLIST_ADD,
            Self::BlacklistRemove => BLACKLIST_REMOVE,
            Self::BlacklistBan => BLACKLIST_BAN,
            Self::MemoryList => MEMORY_LIST,
            Self::MemoryDelete => MEMORY_DELETE,
            Self::MemoryUpdate => MEMORY_UPDATE,
            Self::ReplyUnknownList => REPLY_UNKNOWN_LIST,
            Self::ReplyUnknownCheck => REPLY_UNKNOWN_CHECK,
            Self::ReplyUnknownRetry => REPLY_UNKNOWN_RETRY,
            Self::SessionDeleteAll => SESSION_DELETE_ALL,
            Self::SessionDeleteCharacter => SESSION_DELETE_CHARACTER,
        }
    }
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
pub const BLACKLIST_LIST: &str = "blacklist list";
pub const BLACKLIST_ADD: &str = "blacklist add";
pub const BLACKLIST_REMOVE: &str = "blacklist remove";
pub const BLACKLIST_BAN: &str = "blacklist ban";
pub const MEMORY_LIST: &str = "memory list";
pub const MEMORY_DELETE: &str = "memory delete";
pub const MEMORY_UPDATE: &str = "memory update";
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
        },
        ControlCommandSpec {
            name: HELP,
            usage: "help",
            group: "系统",
            description: "显示所有可用命令",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: RELOAD_CONFIG,
            usage: "reload-config",
            group: "系统",
            description: "重新加载并校验配置",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: SHUTDOWN,
            usage: "shutdown --confirm",
            group: "系统",
            description: "优雅关闭运行时",
            destructive: true,
            requires_confirmation: true,
        },
        ControlCommandSpec {
            name: CHARACTER_LIST,
            usage: "character list [--limit N] [--offset N]",
            group: "角色",
            description: "列出角色",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: CHARACTER_SHOW,
            usage: "character show <id> [--raw]",
            group: "角色",
            description: "显示角色详情",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: CHARACTER_CARD_LIST,
            usage: "character card list [--raw]",
            group: "角色",
            description: "列出角色卡及启用状态",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: CHARACTER_CARD_ENABLE,
            usage: "character card enable <id>",
            group: "角色",
            description: "启用角色卡",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: CHARACTER_CARD_DISABLE,
            usage: "character card disable <id> --confirm",
            group: "角色",
            description: "停用角色卡",
            destructive: true,
            requires_confirmation: true,
        },
        ControlCommandSpec {
            name: CHARACTER_CARD_REASSIGN,
            usage: "character card reassign <old-id> <new-id> --confirm",
            group: "角色",
            description: "修改角色卡 ID",
            destructive: true,
            requires_confirmation: true,
        },
        ControlCommandSpec {
            name: CONVERSATION_LIST,
            usage: "conversation list",
            group: "会话",
            description: "列出会话",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: CHARACTER_STATE_GET,
            usage: "character state get <character-id> [--conversation <id>] [--raw]",
            group: "状态",
            description: "查询角色状态",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: CHARACTER_STATE_SET,
            usage: "character state set --character <id> [--conversation <id>] [--energy N] [--stress N] [--mood N] [--activity <text>]",
            group: "状态",
            description: "修改角色状态",
            destructive: true,
            requires_confirmation: true,
        },
        ControlCommandSpec {
            name: CHARACTER_STATE_REFRESH,
            usage: "character state refresh",
            group: "状态",
            description: "刷新角色状态",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: BLACKLIST_LIST,
            usage: "blacklist list",
            group: "黑名单",
            description: "列出黑名单",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: BLACKLIST_ADD,
            usage: "blacklist add <qq-id> [--reason <text>]",
            group: "黑名单",
            description: "添加黑名单",
            destructive: true,
            requires_confirmation: true,
        },
        ControlCommandSpec {
            name: BLACKLIST_REMOVE,
            usage: "blacklist remove <qq-id>",
            group: "黑名单",
            description: "移除黑名单",
            destructive: true,
            requires_confirmation: true,
        },
        ControlCommandSpec {
            name: BLACKLIST_BAN,
            usage: "blacklist ban <qq-id> [--duration <dur>] [--reason <text>] --confirm",
            group: "黑名单",
            description: "临时封禁用户",
            destructive: true,
            requires_confirmation: true,
        },
        ControlCommandSpec {
            name: MEMORY_LIST,
            usage: "memory list [--character <id>] [--conversation <id>] [--type <type>] [--limit N] [--raw]",
            group: "记忆",
            description: "查询记忆",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: MEMORY_DELETE,
            usage: "memory delete <memory-id> --confirm",
            group: "记忆",
            description: "删除记忆",
            destructive: true,
            requires_confirmation: true,
        },
        ControlCommandSpec {
            name: MEMORY_UPDATE,
            usage: "memory update <memory-id> --instruction <中文修改说明> --confirm",
            group: "记忆",
            description: "确认或更正记忆",
            destructive: true,
            requires_confirmation: true,
        },
        ControlCommandSpec {
            name: REPLY_UNKNOWN_LIST,
            usage: "reply unknown list [--limit N] [--raw]",
            group: "发送",
            description: "列出发送结果未知、等待核查的回复任务",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: REPLY_UNKNOWN_CHECK,
            usage: "reply unknown check <message-id>",
            group: "发送",
            description: "查询平台是否存在未知结果对应的消息",
            destructive: false,
            requires_confirmation: false,
        },
        ControlCommandSpec {
            name: REPLY_UNKNOWN_RETRY,
            usage: "reply unknown retry <message-id> --confirm",
            group: "发送",
            description: "确认平台未发送后重新排队回复任务",
            destructive: true,
            requires_confirmation: true,
        },
        ControlCommandSpec {
            name: SESSION_DELETE_ALL,
            usage: "session delete-all --confirm",
            group: "会话",
            description: "删除全部会话数据",
            destructive: true,
            requires_confirmation: true,
        },
        ControlCommandSpec {
            name: SESSION_DELETE_CHARACTER,
            usage: "session delete-character <character-id> --confirm",
            group: "会话",
            description: "删除角色关联会话数据",
            destructive: true,
            requires_confirmation: true,
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
        BLACKLIST_LIST => &["bl list"],
        BLACKLIST_ADD => &["bl add"],
        BLACKLIST_REMOVE => &["bl remove"],
        BLACKLIST_BAN => &["bl ban"],
        MEMORY_LIST => &["m list", "mem list"],
        MEMORY_DELETE => &["m delete", "mem delete"],
        MEMORY_UPDATE => &["m update", "mem update"],
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
