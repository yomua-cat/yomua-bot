//! 插件 IPC 线协议 —— 语言无关的长度前缀 + MessagePack 帧。
//!
//! 帧格式：4 字节大端 u32 长度前缀 + MessagePack（rmp-serde）序列化体。
//! 每帧独立，天然扛半包/粘包。
//!
//! 本模块只处理「完整帧字节 ↔ WireMessage」与「字节流累计去帧」这类
//! 不依赖 tokio 的纯逻辑；异步 IO 的读写职责归 transport 层。

use serde::de::Error as _;
use serde::{Deserialize, Serialize};

use thiserror::Error;

/// 单帧最大长度（字节）。超长帧直接拒绝，防止畸形/恶意长度前缀撑爆内存。
pub const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;
/// 当前稳定协议版本。主版本不兼容时握手失败。
pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Error)]
#[error("{message}")]
pub struct ProtocolError {
    message: String,
}

impl ProtocolError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

fn error_dto(message: &str) -> serde_json::Value {
    let code = if message.contains("protocol_version") {
        "UNSUPPORTED_PROTOCOL_VERSION"
    } else if message.contains("权限不足") {
        "PERMISSION_DENIED"
    } else if message.contains("插件名不匹配") {
        "IDENTITY_MISMATCH"
    } else if message.contains("未知事件") {
        "UNKNOWN_EVENT"
    } else if message.contains("协议") || message.contains("帧") {
        "PROTOCOL_ERROR"
    } else if message.contains("握手") {
        "HANDSHAKE_FAILED"
    } else if message.contains("未知方法") {
        "UNKNOWN_METHOD"
    } else if message.contains("参数") || message.contains("必须") || message.contains("缺少")
    {
        "INVALID_ARGUMENT"
    } else if message.contains("不存在") {
        "NOT_FOUND"
    } else {
        "INTERNAL_ERROR"
    };
    serde_json::json!({"code": code, "message": message})
}

fn error_message(value: &serde_json::Value) -> Option<String> {
    value
        .as_object()?
        .get("message")?
        .as_str()
        .map(str::to_string)
}

/// 长度前缀占用的字节数。
const LEN_PREFIX_BYTES: usize = 4;

/// 插件与核心之间的线协议消息。
///
/// Rust 枚举是宿主侧便利表示；线上始终使用版本化 envelope：
/// `{protocol_version, kind, payload}`。payload 只包含协议数据。
#[derive(Debug, Clone)]
pub enum WireMessage {
    /// 插件 → Core 握手。
    Hello(Hello),

    /// Core → 插件 握手应答。
    HelloAck { ok: bool, reason: Option<String> },

    /// 插件 → Core RPC 请求。
    Request {
        id: u64,
        method: String,
        params: serde_json::Value,
    },

    /// Core → 插件 RPC 响应。
    Response {
        id: u64,
        ok: bool,
        result: Option<serde_json::Value>,
        error: Option<String>,
    },

    /// Core → 插件 事件通知。
    Notify {
        event: String,
        data: serde_json::Value,
    },
}

/// 握手负载：插件声明自身身份与订阅的事件。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    /// 插件名称。
    pub name: String,

    /// 插件版本。
    pub version: String,

    /// 订阅的事件名（dotted，见 `EventType::name`）。
    pub subscribe: Vec<String>,
}

/// 手写序列化：wire 上始终为含 `type` 键的对象。
impl Serialize for WireMessage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let value = match self {
            WireMessage::Hello(h) => {
                serde_json::json!({"protocol_version": PROTOCOL_VERSION, "kind":"hello", "payload":{"name":h.name,"version":h.version,"subscribe":h.subscribe}})
            }
            WireMessage::HelloAck { ok, reason } => {
                serde_json::json!({"protocol_version": PROTOCOL_VERSION, "kind":"hello_ack", "payload":{"ok":ok,"error":reason.as_deref().map(error_dto)}})
            }
            WireMessage::Request { id, method, params } => serde_json::json!({
                "protocol_version": PROTOCOL_VERSION, "kind":"request",
                "payload":{"request_id":id,"method":method,"params":params},
            }),
            WireMessage::Response {
                id,
                ok,
                result,
                error,
            } => serde_json::json!({
                "protocol_version": PROTOCOL_VERSION, "kind":"response",
                "payload":{"request_id":id,"ok":ok,"result":result,"error":error.as_deref().map(error_dto)},
            }),
            WireMessage::Notify { event, data } => {
                serde_json::json!({"protocol_version":PROTOCOL_VERSION,"kind":"event","payload":{"event":event,"data":data}})
            }
        };
        value.serialize(serializer)
    }
}

/// 手写反序列化：按 `type` 判别消息种类并逐字段解析。
impl<'de> Deserialize<'de> for WireMessage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        let map = value
            .as_object()
            .ok_or_else(|| D::Error::custom("帧体必须是对象（含 type 字段）"))?;
        let version = map
            .get("protocol_version")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| D::Error::custom("缺少 protocol_version"))?;
        if version != PROTOCOL_VERSION as u64 {
            return Err(D::Error::custom("不支持的 protocol_version"));
        }
        let payload = map
            .get("payload")
            .and_then(|v| v.as_object())
            .ok_or_else(|| D::Error::custom("payload 必须是对象"))?;
        let ty = map
            .get("kind")
            .and_then(|v| v.as_str())
            .ok_or_else(|| D::Error::custom("帧体缺少 type 字段"))?;

        match ty {
            "hello" => {
                let name = payload
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| D::Error::custom("hello 消息缺少 name 字段"))?;
                let plugin_version = payload
                    .get("version")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| D::Error::custom("hello 消息缺少 version 字段"))?;
                let subscribe = match payload.get("subscribe") {
                    Some(serde_json::Value::Array(items)) => items
                        .iter()
                        .map(|i| {
                            i.as_str()
                                .map(str::to_string)
                                .ok_or_else(|| D::Error::custom("subscribe 元素必须是字符串"))
                        })
                        .collect::<Result<Vec<String>, D::Error>>()?,
                    Some(_) => return Err(D::Error::custom("subscribe 必须是数组")),
                    None => return Err(D::Error::custom("hello 消息缺少 subscribe 字段")),
                };
                Ok(WireMessage::Hello(Hello {
                    name: name.to_string(),
                    version: plugin_version.to_string(),
                    subscribe,
                }))
            }
            "hello_ack" => {
                let ok = payload
                    .get("ok")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| D::Error::custom("hello_ack 消息缺少 ok 字段"))?;
                let reason = match payload.get("error") {
                    None | Some(serde_json::Value::Null) => None,
                    Some(v) => Some(
                        error_message(v)
                            .ok_or_else(|| D::Error::custom("error 必须含 code 和 message"))?,
                    ),
                };
                Ok(WireMessage::HelloAck { ok, reason })
            }
            "request" => {
                let id = payload
                    .get("request_id")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| D::Error::custom("request 消息缺少 id 字段"))?;
                let method = payload
                    .get("method")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| D::Error::custom("request 消息缺少 method 字段"))?;
                let params = payload
                    .get("params")
                    .cloned()
                    .ok_or_else(|| D::Error::custom("request 消息缺少 params 字段"))?;
                Ok(WireMessage::Request {
                    id,
                    method: method.to_string(),
                    params,
                })
            }
            "response" => {
                let id = payload
                    .get("request_id")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| D::Error::custom("response 消息缺少 id 字段"))?;
                let ok = payload
                    .get("ok")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| D::Error::custom("response 消息缺少 ok 字段"))?;
                let result = match payload.get("result") {
                    None | Some(serde_json::Value::Null) => None,
                    Some(v) => Some(v.clone()),
                };
                let error = match payload.get("error") {
                    None | Some(serde_json::Value::Null) => None,
                    Some(v) => Some(
                        error_message(v)
                            .ok_or_else(|| D::Error::custom("error 必须含 code 和 message"))?,
                    ),
                };
                Ok(WireMessage::Response {
                    id,
                    ok,
                    result,
                    error,
                })
            }
            "event" => {
                let event = payload
                    .get("event")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| D::Error::custom("notify 消息缺少 event 字段"))?;
                let data = payload
                    .get("data")
                    .cloned()
                    .ok_or_else(|| D::Error::custom("notify 消息缺少 data 字段"))?;
                Ok(WireMessage::Notify {
                    event: event.to_string(),
                    data,
                })
            }
            other => Err(D::Error::custom(format!("未知消息类型：{other}"))),
        }
    }
}

/// 将消息编码为一个完整帧（4 字节大端长度前缀 + MessagePack 体）。
pub fn encode_frame(msg: &WireMessage) -> Result<Vec<u8>, ProtocolError> {
    let body =
        rmp_serde::to_vec(msg).map_err(|e| ProtocolError::new(format!("帧编码失败：{e}")))?;
    if body.len() as u32 > MAX_FRAME_LEN {
        return Err(ProtocolError::new(format!(
            "帧体超过长度上限（{} 字节）",
            MAX_FRAME_LEN
        )));
    }
    let mut frame = Vec::with_capacity(LEN_PREFIX_BYTES + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// 将一个完整帧解码为消息。
///
/// 校验长度上限、截断体与未知 tag；乱字节返回错误而非 panic。
pub fn decode_frame(full_frame: &[u8]) -> Result<WireMessage, ProtocolError> {
    if full_frame.len() < LEN_PREFIX_BYTES {
        return Err(ProtocolError::new(format!(
            "帧过短：{} 字节",
            full_frame.len()
        )));
    }
    let len = u32::from_be_bytes(
        full_frame[..LEN_PREFIX_BYTES]
            .try_into()
            .map_err(|_| ProtocolError::new("帧长度前缀读取失败"))?,
    ) as usize;
    if len > MAX_FRAME_LEN as usize {
        return Err(ProtocolError::new(format!(
            "帧长度前缀 {len} 超过上限 {}",
            MAX_FRAME_LEN
        )));
    }
    if full_frame.len() != LEN_PREFIX_BYTES + len {
        return Err(ProtocolError::new(format!(
            "帧体长度不符：期望 {len} 字节，实际 {} 字节",
            full_frame.len() - LEN_PREFIX_BYTES
        )));
    }
    rmp_serde::from_slice(&full_frame[LEN_PREFIX_BYTES..])
        .map_err(|e| ProtocolError::new(format!("帧解码失败：{e}")))
}

/// 从已累积的字节流中尝试取出一个完整帧。
///
/// 返回 `(消费字节数, 消息)`；数据不足（半包）返回 `None`。
/// 纯函数，不依赖任何异步 IO，transport 层可直接复用。
pub fn decode_full_read(bytes_so_far: &[u8]) -> Option<(usize, WireMessage)> {
    if bytes_so_far.len() < LEN_PREFIX_BYTES {
        return None;
    }
    let len = u32::from_be_bytes(bytes_so_far[..LEN_PREFIX_BYTES].try_into().ok()?) as usize;
    if len > MAX_FRAME_LEN as usize {
        // 前缀超长视为无法完成，交由调用方按协议错误处理
        return None;
    }
    let total = LEN_PREFIX_BYTES + len;
    if bytes_so_far.len() < total {
        return None;
    }
    let msg = decode_frame(&bytes_so_far[..total]).ok()?;
    Some((total, msg))
}

/// 订阅的事件类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    /// 收到一条消息。
    MessageReceived,

    /// 发出了一条消息。
    MessageSent,

    /// 角色状态变化。
    CharacterStateChanged,

    /// 情绪变化。
    EmotionChanged,

    /// 关系变化。
    RelationshipChanged,

    /// 新建了一条记忆。
    MemoryCreated,

    /// 做出了一项行为决策。
    BehaviorDecided,

    /// 生成了一个响应。
    ResponseGenerated,

    /// 适配器已连接。
    AdapterConnected,

    /// 适配器已断开。
    AdapterDisconnected,

    /// 定时任务被触发。
    ScheduledTaskTriggered,
}

impl EventType {
    /// dotted 风格的事件名（协议与订阅契约）。
    pub fn name(self) -> &'static str {
        match self {
            EventType::MessageReceived => "message.received",
            EventType::MessageSent => "message.sent",
            EventType::CharacterStateChanged => "character.state.changed",
            EventType::EmotionChanged => "emotion.changed",
            EventType::RelationshipChanged => "relationship.changed",
            EventType::MemoryCreated => "memory.created",
            EventType::BehaviorDecided => "behavior.decided",
            EventType::ResponseGenerated => "response.generated",
            EventType::AdapterConnected => "adapter.connected",
            EventType::AdapterDisconnected => "adapter.disconnected",
            EventType::ScheduledTaskTriggered => "scheduler.task.triggered",
        }
    }

    /// 将订阅名（dotted）解析为事件类型；未知事件名报错。
    pub fn parse(s: &str) -> Result<EventType, ProtocolError> {
        match s {
            "message.received" => Ok(EventType::MessageReceived),
            "message.sent" => Ok(EventType::MessageSent),
            "character.state.changed" => Ok(EventType::CharacterStateChanged),
            "emotion.changed" => Ok(EventType::EmotionChanged),
            "relationship.changed" => Ok(EventType::RelationshipChanged),
            "memory.created" => Ok(EventType::MemoryCreated),
            "behavior.decided" => Ok(EventType::BehaviorDecided),
            "response.generated" => Ok(EventType::ResponseGenerated),
            "adapter.connected" => Ok(EventType::AdapterConnected),
            "adapter.disconnected" => Ok(EventType::AdapterDisconnected),
            "scheduler.task.triggered" => Ok(EventType::ScheduledTaskTriggered),
            _ => Err(ProtocolError::new(format!("未知事件名：{s}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_hello() -> Hello {
        Hello {
            name: "echo".to_string(),
            version: "0.1.0".to_string(),
            subscribe: vec!["message.received".to_string()],
        }
    }

    /// 断言某消息编码后 wire 上的 tag 字符串（语言无关契约）。
    fn assert_wire_tag(msg: &WireMessage, expected: &str) {
        let frame = encode_frame(msg).unwrap();
        let value: serde_json::Value = rmp_serde::from_slice(&frame[LEN_PREFIX_BYTES..]).unwrap();
        assert_eq!(
            value["kind"].as_str(),
            Some(expected),
            "wire tag 不匹配，实际序列化为 {value:#?}"
        );
    }

    #[test]
    fn wire_tag_strings_match_contract() {
        assert_wire_tag(&WireMessage::Hello(sample_hello()), "hello");
        assert_wire_tag(
            &WireMessage::HelloAck {
                ok: true,
                reason: None,
            },
            "hello_ack",
        );
        assert_wire_tag(
            &WireMessage::Request {
                id: 1,
                method: "message.send".to_string(),
                params: serde_json::Value::Null,
            },
            "request",
        );
        assert_wire_tag(
            &WireMessage::Response {
                id: 2,
                ok: true,
                result: None,
                error: None,
            },
            "response",
        );
        assert_wire_tag(
            &WireMessage::Notify {
                event: "message.received".to_string(),
                data: serde_json::Value::Null,
            },
            "event",
        );
    }

    #[test]
    fn roundtrip_all_five_message_kinds() {
        let msgs = vec![
            WireMessage::Hello(Hello {
                name: "echo".to_string(),
                version: "0.1.0".to_string(),
                subscribe: vec![],
            }),
            WireMessage::HelloAck {
                ok: true,
                reason: Some("已连接".to_string()),
            },
            WireMessage::HelloAck {
                ok: false,
                reason: None,
            },
            WireMessage::Request {
                id: 7,
                method: "memory.write".to_string(),
                params: serde_json::json!({}),
            },
            WireMessage::Request {
                id: 8,
                method: "character.read".to_string(),
                params: serde_json::Value::Null,
            },
            WireMessage::Response {
                id: 9,
                ok: false,
                result: None,
                error: Some("权限不足".to_string()),
            },
            WireMessage::Response {
                id: 10,
                ok: true,
                result: Some(serde_json::json!({"k": [1, 2, 3]})),
                error: None,
            },
            WireMessage::Notify {
                event: "memory.created".to_string(),
                data: serde_json::json!({}),
            },
            WireMessage::Notify {
                event: "adapter.disconnected".to_string(),
                data: serde_json::Value::Null,
            },
        ];
        for msg in msgs {
            let frame = encode_frame(&msg).unwrap();
            let decoded = decode_frame(&frame).unwrap();
            // 用 serde_json 归一化后比较，字段顺序差异不影响断言
            assert_eq!(
                serde_json::to_value(&msg).unwrap(),
                serde_json::to_value(&decoded).unwrap()
            );
        }
    }

    #[test]
    fn oversized_prefix_rejected() {
        // 长度前缀 (0xFFFFFFFF) 超过 MAX_FRAME_LEN
        let frame = [0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x02, 0x03];
        assert!(decode_frame(&frame).is_err());
    }

    #[test]
    fn truncated_body_rejected() {
        let full = encode_frame(&WireMessage::Hello(sample_hello())).unwrap();
        let truncated = &full[..full.len() / 2];
        assert!(decode_frame(truncated).is_err());
    }

    #[test]
    fn unknown_tag_rejected() {
        // 手工构造未知 kind 的 MessagePack 体
        let body = rmp_serde::to_vec(
            &serde_json::json!({ "protocol_version": 1, "kind": "bogus", "payload": {} }),
        )
        .unwrap();
        let mut frame = (body.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        assert!(decode_frame(&frame).is_err());
    }

    #[test]
    fn version_and_unknown_fields_are_explicit() {
        let frame = encode_frame(&WireMessage::Hello(sample_hello())).unwrap();
        let mut value: serde_json::Value =
            rmp_serde::from_slice(&frame[LEN_PREFIX_BYTES..]).unwrap();
        value["future_field"] = serde_json::json!(true);
        let body = rmp_serde::to_vec(&value).unwrap();
        let mut altered = (body.len() as u32).to_be_bytes().to_vec();
        altered.extend_from_slice(&body);
        assert!(decode_frame(&altered).is_ok());

        value["protocol_version"] = serde_json::json!(99);
        let body = rmp_serde::to_vec(&value).unwrap();
        let mut altered = (body.len() as u32).to_be_bytes().to_vec();
        altered.extend_from_slice(&body);
        assert!(decode_frame(&altered).is_err());
    }

    #[test]
    fn error_wire_has_machine_code_and_message() {
        let frame = encode_frame(&WireMessage::Response {
            id: 1,
            ok: false,
            result: None,
            error: Some("权限不足：需要 message.read 权限".to_string()),
        })
        .unwrap();
        let value: serde_json::Value = rmp_serde::from_slice(&frame[LEN_PREFIX_BYTES..]).unwrap();
        assert_eq!(value["payload"]["error"]["code"], "PERMISSION_DENIED");
        assert_eq!(
            value["payload"]["error"]["message"],
            "权限不足：需要 message.read 权限"
        );
    }

    #[test]
    fn garbage_bytes_rejected_without_panic() {
        let garbage_cases: [&[u8]; 5] = [
            &[0x00, 0x00],                   // 长度前缀都不完整
            &[0x81, 0x91, 0x92],             // 畸形 msgpack
            &[0xFF],                         // 无效 marker
            &[0xC1],                         // 保留 marker
            &[0x00, 0x00, 0x00, 0x01, 0x81], // 声明 1 字节但内容不完整
        ];
        for garbage in garbage_cases {
            let result = decode_frame(garbage);
            assert!(result.is_err(), "乱字节 {:?} 应报错而非 panic", garbage);
        }
    }

    #[test]
    fn event_type_name_parse_bidirectional() {
        let cases = [
            (EventType::MessageReceived, "message.received"),
            (EventType::MessageSent, "message.sent"),
            (EventType::CharacterStateChanged, "character.state.changed"),
            (EventType::EmotionChanged, "emotion.changed"),
            (EventType::RelationshipChanged, "relationship.changed"),
            (EventType::MemoryCreated, "memory.created"),
            (EventType::BehaviorDecided, "behavior.decided"),
            (EventType::ResponseGenerated, "response.generated"),
            (EventType::AdapterConnected, "adapter.connected"),
            (EventType::AdapterDisconnected, "adapter.disconnected"),
            (
                EventType::ScheduledTaskTriggered,
                "scheduler.task.triggered",
            ),
        ];
        for (ty, name) in cases {
            assert_eq!(ty.name(), name, "name() 映射错误");
            assert_eq!(EventType::parse(name).unwrap(), ty, "parse() 映射错误");
        }
        assert!(EventType::parse("message.event").is_err());
        assert!(EventType::parse("").is_err());
    }

    /*
    fn from_core_event_covers_all_variants() {
        let events = vec![
            CoreEvent::MessageReceived(MessageReceivedEvent {
                conversation_id: 1,
                sender_id: 2,
                message_id: 3,
                content: "hi".to_string(),
                timestamp: ts(),
                is_mentioned: true,
                character_id: 1,
                // C2-3 字段
                conversation_sequence: 0,
                platform_message_id: None,
                ingested_at: chrono::Utc::now(),
                metadata: serde_json::json!({}),
            }),
            CoreEvent::MessageSent(MessageSentEvent {
                conversation_id: 1,
                character_id: Some(4),
                message_id: 5,
                content: "hello".to_string(),
                timestamp: ts(),
            }),
            CoreEvent::CharacterStateChanged(CharacterStateChangedEvent {
                character_id: 4,
                timestamp: ts(),
            }),
            CoreEvent::EmotionChanged(EmotionChangedEvent {
                character_id: 4,
                conversation_id: 1,
                timestamp: ts(),
            }),
            CoreEvent::RelationshipChanged(RelationshipChangedEvent {
                character_id: 4,
                user_id: 2,
                timestamp: ts(),
            }),
            CoreEvent::MemoryCreated(MemoryCreatedEvent {
                character_id: 4,
                memory_id: 9,
                timestamp: ts(),
            }),
            CoreEvent::BehaviorDecided(BehaviorDecidedEvent {
                character_id: 4,
                conversation_id: 1,
                action: "greet".to_string(),
                reason: "felt like it".to_string(),
                timestamp: ts(),
            }),
            CoreEvent::ResponseGenerated(ResponseGeneratedEvent {
                character_id: 4,
                conversation_id: 1,
                content: "resp".to_string(),
                source: crate::domain::event::ResponseSource::Llm,
                timestamp: ts(),
            }),
            CoreEvent::AdapterConnected(AdapterConnectedEvent {
                adapter_name: "onebot".to_string(),
                timestamp: ts(),
            }),
            CoreEvent::AdapterDisconnected(AdapterDisconnectedEvent {
                adapter_name: "onebot".to_string(),
                reason: None,
                timestamp: ts(),
            }),
            CoreEvent::ScheduledTaskTriggered(ScheduledTaskTriggeredEvent {
                task_id: 1,
                task_type: "proactive".to_string(),
                timestamp: ts(),
            }),
        ];
        let expected = [
            EventType::MessageReceived,
            EventType::MessageSent,
            EventType::CharacterStateChanged,
            EventType::EmotionChanged,
            EventType::RelationshipChanged,
            EventType::MemoryCreated,
            EventType::BehaviorDecided,
            EventType::ResponseGenerated,
            EventType::AdapterConnected,
            EventType::AdapterDisconnected,
            EventType::ScheduledTaskTriggered,
        ];
        for (event, ty) in events.iter().zip(expected) {
            assert_eq!(EventType::from_core_event(event), Some(ty));
        }
    }

    #[test]
    fn from_core_event_command_received_is_none() {
        // 系统指令不对插件暴露（硬性约束 B）：插件收不到指令消息。
        let event = CoreEvent::CommandReceived(crate::domain::event::CommandReceivedEvent {
            conversation_id: 1,
            sender_id: 2,
            external_sender_id: "900001".to_string(),
            message_id: 3,
            content: "换角色 木然".to_string(),
            timestamp: ts(),
            command: crate::domain::event::Command::SwitchCharacter {
                character_name: "木然".to_string(),
            },
            source_message_id: 2,
            character_id: 1,
            conversation_sequence: 1,
            platform_message_id: Some(3),
            ingested_at: Some(ts()),
        });
        assert_eq!(EventType::from_core_event(&event), None);
    }
    */

    #[test]
    fn half_packet_recovery() {
        // 半包：先到一半，未完整时不能去帧
        let frame = encode_frame(&WireMessage::HelloAck {
            ok: true,
            reason: Some("已连接".to_string()),
        })
        .unwrap();
        let mut stream: Vec<u8> = Vec::new();
        let mid = frame.len() / 2;
        stream.extend_from_slice(&frame[..mid]);
        assert!(decode_full_read(&stream).is_none(), "半包不应去帧");
        // 补齐另一半后可完整去帧
        stream.extend_from_slice(&frame[mid..]);
        let (consumed, msg) = decode_full_read(&stream).unwrap();
        assert_eq!(consumed, frame.len());
        assert!(matches!(msg, WireMessage::HelloAck { ok: true, .. }));

        // 粘包：两帧相连，分两次去帧
        let f1 = encode_frame(&WireMessage::Request {
            id: 1,
            method: "message.send".to_string(),
            params: serde_json::json!({}),
        })
        .unwrap();
        let f2 = encode_frame(&WireMessage::Notify {
            event: "message.received".to_string(),
            data: serde_json::json!({ "x": 1 }),
        })
        .unwrap();
        let mut joined = f1.clone();
        joined.extend_from_slice(&f2);
        let (c1, m1) = decode_full_read(&joined).unwrap();
        assert_eq!(c1, f1.len());
        assert!(matches!(m1, WireMessage::Request { .. }));
        let (c2, m2) = decode_full_read(&joined[c1..]).unwrap();
        assert_eq!(c2, f2.len());
        assert!(matches!(m2, WireMessage::Notify { .. }));
    }

    #[test]
    fn event_type_serde_snake_case_roundtrip() {
        // EventType 的 serde 形态为 snake_case（内部表示），与 dotted 事件名无关
        let json = serde_json::to_string(&EventType::ScheduledTaskTriggered).unwrap();
        assert_eq!(json, "\"scheduled_task_triggered\"");
        assert_eq!(
            serde_json::from_str::<EventType>(&json).unwrap(),
            EventType::ScheduledTaskTriggered
        );
    }
}
