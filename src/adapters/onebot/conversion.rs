//! OneBot 11 协议转换 —— 纯函数，不依赖网络或持久化。
//!
//! 职责：
//! - OneBot JSON 事件 → 平台无关的 `InboundMessage`（入站）
//! - 一次已解析的发信请求 → OneBot API 请求（出站）
//!
//! 本模块不含 WebSocket、SQLite 或任何外部交互，便于单元测试。

use serde::{Deserialize, Serialize};

use crate::domain::conversation::ConversationType;
use crate::error::RuntimeError;

/// 一条到达的 OneBot 事件（WebSocket 消息帧内容）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OneBotEvent {
    /// 事件类型（"message"、"meta_event"、"notice"、"request" 等）。
    pub post_type: String,
    /// 事件发生的时间戳（Unix 秒）。
    pub time: i64,
    /// 接收事件的机器人 QQ 号。
    pub self_id: i64,

    #[serde(flatten)]
    pub data: serde_json::Value,
}

/// 一条已归一化、但仍是平台域的消息（仍包含外部 ID）。
///
/// 适配器据此调用会话管理器解析核心 ID 并生成 CoreEvent。
#[derive(Debug, Clone)]
pub struct InboundMessage {
    /// 会话类型（私聊 / 群聊）。
    pub conversation_type: ConversationType,
    /// 外部会话 ID（群号或用户号）。
    pub external_conversation_id: String,
    /// 外部发送者 ID（QQ 用户号）。
    pub external_sender_id: String,
    /// 发送者显示名称。
    pub display_name: String,
    /// 平台消息 ID。
    pub platform_message_id: i64,
    /// 消息文本内容。
    pub content: String,
    /// 消息时间（Unix 秒）。
    pub unix_time: i64,
    /// 消息中是否包含「at」段（即 @ 了某个对象，很可能是角色）。
    pub is_mentioned: bool,
}

/// 一次解析完成的出站发信请求的 OneBot API 描述。
#[derive(Debug, Clone)]
pub struct OutgoingRequest {
    /// OneBot API 动作名称（例如 "send_group_msg"）。
    pub action: String,
    /// API 参数。
    pub params: serde_json::Value,
}

/// 将一条 OneBot JSON 事件（`Value`）转换为 `InboundMessage`。
///
/// 仅处理 `post_type == "message"` 的群聊与私聊消息。
/// 其他事件类型（元事件、通知、请求、以及损坏的数据）返回错误，
/// 由调用方选择忽略或记录。
pub fn onebot_event_to_inbound(event: &OneBotEvent) -> Result<InboundMessage, RuntimeError> {
    if event.post_type != "message" {
        return Err(RuntimeError::Adapter(format!(
            "非消息事件: {}",
            event.post_type
        )));
    }

    let data = &event.data;
    let message_type = data
        .get("message_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| RuntimeError::Adapter("缺少 message_type".to_string()))?;

    let conversation_type = match message_type {
        "group" => ConversationType::Group,
        "private" => ConversationType::Private,
        other => return Err(RuntimeError::Adapter(format!("不支持的消息类型: {other}"))),
    };

    // 会话外部 ID：群聊取 group_id，私聊取 user_id。
    let external_conversation_id = match conversation_type {
        ConversationType::Group => data.get("group_id").and_then(|v| v.as_i64()),
        ConversationType::Private => data.get("user_id").and_then(|v| v.as_i64()),
    }
    .ok_or_else(|| RuntimeError::Adapter("缺少会话 ID".to_string()))?
    .to_string();

    let sender_id = data
        .get("user_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| RuntimeError::Adapter("缺少 user_id".to_string()))?;

    let platform_message_id = data
        .get("message_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| RuntimeError::Adapter("缺少 message_id".to_string()))?;

    // 提取显示名称：优先 sender.card，其次 sender.nickname。
    let display_name = data
        .get("sender")
        .and_then(|v| v.get("card"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            data.get("sender")
                .and_then(|v| v.get("nickname"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or("未知用户")
        .to_string();

    // 提取消息内容（支持 CQ 码字符串或消息分段数组）。
    let content = extract_text_content(data.get("message"))?;

    // 检测是否包含「at」段（即 @ 了某个对象）。
    let is_mentioned = contains_at_segment(data.get("message"));

    Ok(InboundMessage {
        conversation_type,
        external_conversation_id,
        external_sender_id: sender_id.to_string(),
        display_name,
        platform_message_id,
        content,
        unix_time: event.time,
        is_mentioned,
    })
}

/// 从 OneBot 的 `message` 字段提取纯文本。
///
/// `message` 可以是 CQ 码字符串，也可以是分段数组（OneBot 11 两种格式均合法）。
fn extract_text_content(message: Option<&serde_json::Value>) -> Result<String, RuntimeError> {
    let Some(message) = message else {
        return Ok(String::new());
    };

    match message {
        serde_json::Value::String(s) => Ok(strip_cq_codes(s)),
        serde_json::Value::Array(segments) => {
            let mut text = String::new();
            for segment in segments {
                let Some(seg_type) = segment.get("type").and_then(|v| v.as_str()) else {
                    continue;
                };
                if seg_type == "text" {
                    if let Some(t) = segment.get("data").and_then(|v| v.get("text")) {
                        if let Some(s) = t.as_str() {
                            text.push_str(s);
                        }
                    }
                }
            }
            Ok(text.trim_end().to_string())
        }
        // 其他（没有 content 字段）—— 视为空文本。
        _ => Ok(String::new()),
    }
}

/// 判断消息分段中是否包含「at」段（即 @ 了某个对象）。
///
/// CQ 码字符串（例如 `[CQ:at,qq=...]`）中的 at 不被解析为分段，
/// 仅针对 OneBot 分段数组形式；对纯 CQ 码字符串的 at 检测可留待后续阶段。
fn contains_at_segment(message: Option<&serde_json::Value>) -> bool {
    let Some(message) = message else {
        return false;
    };

    match message {
        serde_json::Value::Array(segments) => segments.iter().any(|segment| {
            segment
                .get("type")
                .and_then(|v| v.as_str())
                .map(|t| t == "at")
                .unwrap_or(false)
        }),
        // 字符串形式的 CQ 码：本阶段视作未提及（atom 片段暂未解析）。
        _ => false,
    }
}

/// 简单移除 CQ 码（例如 `[CQ:image,file=...]`），仅保留其中的纯文本。
///
/// 第一阶段仅处理基础文本；图片等富媒体由未来版本实现。
fn strip_cq_codes(input: &str) -> String {
    // 移除形如 [CQ:xxx] 的分段。
    let without_cq = {
        let mut out = String::with_capacity(input.len());
        let mut rest = input;
        while let Some(start) = rest.find("[CQ:") {
            out.push_str(&rest[..start]);
            if let Some(end) = rest[start..].find(']') {
                rest = &rest[start + end + 1..];
            } else {
                // 未闭合 —— 直接丢弃剩余部分。
                rest = "";
                break;
            }
        }
        out.push_str(rest);
        out
    };
    without_cq.trim().to_string()
}

/// 构建一条 OneBot 群消息发送请求。
pub fn build_group_send_request(group_id: &str, content: &str) -> OutgoingRequest {
    OutgoingRequest {
        action: "send_group_msg".to_string(),
        params: serde_json::json!({
            "group_id": parse_qq_id(group_id),
            "message": content,
        }),
    }
}

/// 构建一条 OneBot 私聊消息发送请求。
pub fn build_private_send_request(user_id: &str, content: &str) -> OutgoingRequest {
    OutgoingRequest {
        action: "send_private_msg".to_string(),
        params: serde_json::json!({
            "user_id": parse_qq_id(user_id),
            "message": content,
        }),
    }
}

/// 将数字字符串解析为 i64（OneBot 的群号 / 用户号）。
///
/// 若解析失败则回退为 0（不应发生，因为外部 ID 来自数字字符串）。
fn parse_qq_id(id: &str) -> i64 {
    id.trim().parse().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn group_event() -> OneBotEvent {
        OneBotEvent {
            post_type: "message".to_string(),
            time: 1690000000,
            self_id: 123456,
            data: json!({
                "message_type": "group",
                "sub_type": "normal",
                "message_id": 1001,
                "group_id": 500000,
                "user_id": 900001,
                "message": [
                    {"type": "text", "data": {"text": "你好，"}},
                    {"type": "text", "data": {"text": "世界！"}},
                    {"type": "face", "data": {"id": "1"}}
                ],
                "sender": {"user_id": 900001, "nickname": "小明", "card": "", "role": "member"}
            }),
        }
    }

    #[test]
    fn group_message_converts() {
        let inbound = onebot_event_to_inbound(&group_event()).unwrap();
        assert!(matches!(inbound.conversation_type, ConversationType::Group));
        assert_eq!(inbound.external_conversation_id, "500000");
        assert_eq!(inbound.external_sender_id, "900001");
        assert_eq!(inbound.display_name, "小明"); // card 为空 → 回退 nickname
        assert_eq!(inbound.platform_message_id, 1001);
        assert_eq!(inbound.content, "你好，世界！");
        assert_eq!(inbound.unix_time, 1690000000);
    }

    #[test]
    fn private_message_converts() {
        let event = OneBotEvent {
            post_type: "message".to_string(),
            time: 100,
            self_id: 1,
            data: json!({
                "message_type": "private",
                "message_id": 7,
                "user_id": 555,
                "message": "直接给你一段文本",
                "sender": {"user_id": 555, "nickname": "小红"}
            }),
        };
        let inbound = onebot_event_to_inbound(&event).unwrap();
        assert!(matches!(
            inbound.conversation_type,
            ConversationType::Private
        ));
        assert_eq!(inbound.external_conversation_id, "555");
        assert_eq!(inbound.content, "直接给你一段文本");
        assert!(!inbound.is_mentioned);
    }

    #[test]
    fn at_segment_sets_is_mentioned() {
        // 消息分段数组中出现 "at" 段 → 视为被提及（@）。
        let event = OneBotEvent {
            post_type: "message".to_string(),
            time: 1,
            self_id: 1,
            data: json!({
                "message_type": "group",
                "message_id": 1,
                "group_id": 500000,
                "user_id": 900001,
                "message": [
                    {"type": "at", "data": {"qq": "123456", "name": "小助手"}},
                    {"type": "text", "data": {"text": "你好！"}}
                ],
                "sender": {"user_id": 900001, "nickname": "小明"}
            }),
        };
        let inbound = onebot_event_to_inbound(&event).unwrap();
        assert!(inbound.is_mentioned);
        // at 段不参与文本内容。
        assert_eq!(inbound.content, "你好！");
    }

    #[test]
    fn no_at_segment_is_not_mentioned() {
        let event = OneBotEvent {
            post_type: "message".to_string(),
            time: 1,
            self_id: 1,
            data: json!({
                "message_type": "group",
                "message_id": 1,
                "group_id": 500000,
                "user_id": 900001,
                "message": [
                    {"type": "text", "data": {"text": "大家早上好"}},
                    {"type": "face", "data": {"id": "1"}}
                ],
                "sender": {"user_id": 900001, "nickname": "小明"}
            }),
        };
        let inbound = onebot_event_to_inbound(&event).unwrap();
        assert!(!inbound.is_mentioned);
    }

    #[test]
    fn non_message_event_is_error() {
        let event = OneBotEvent {
            post_type: "meta_event".to_string(),
            time: 1,
            self_id: 1,
            data: json!({"meta_event_type": "heartbeat"}),
        };
        assert!(onebot_event_to_inbound(&event).is_err());
    }

    #[test]
    fn cq_code_string_is_stripped() {
        let event = OneBotEvent {
            post_type: "message".to_string(),
            time: 1,
            self_id: 1,
            data: json!({
                "message_type": "group",
                "message_id": 1,
                "group_id": 1,
                "user_id": 2,
                "message": "看看这张图 [CQ:image,file=img.png] 怎么样",
                "sender": {"user_id": 2, "nickname": "a"}
            }),
        };
        let inbound = onebot_event_to_inbound(&event).unwrap();
        assert_eq!(inbound.content, "看看这张图  怎么样");
    }

    #[test]
    fn build_group_request() {
        let req = build_group_send_request("500000", "回复你");
        assert_eq!(req.action, "send_group_msg");
        assert_eq!(req.params["group_id"], 500000);
        assert_eq!(req.params["message"], "回复你");
    }

    #[test]
    fn build_private_request() {
        let req = build_private_send_request("555", "私聊回复");
        assert_eq!(req.action, "send_private_msg");
        assert_eq!(req.params["user_id"], 555);
        assert_eq!(req.params["message"], "私聊回复");
    }
}
