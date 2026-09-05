//! 插件 IPC 线协议 —— 语言无关的长度前缀 + MessagePack 帧。
//!
//! 帧格式：4 字节大端 u32 长度前缀 + MessagePack（rmp-serde）序列化体。
//! 每帧独立，天然扛半包/粘包。
//!
//! 本模块只处理「完整帧字节 ↔ WireMessage」与「字节流累计去帧」这类
//! 不依赖 tokio 的纯逻辑；异步 IO 的读写职责归 transport 层。

use serde::de::Error as _;
use serde::{Deserialize, Serialize};

use crate::domain::event::CoreEvent;
use crate::error::RuntimeError;

/// 单帧最大长度（字节）。超长帧直接拒绝，防止畸形/恶意长度前缀撑爆内存。
pub const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// 长度前缀占用的字节数。
const LEN_PREFIX_BYTES: usize = 4;

/// 插件与核心之间的线协议消息。
///
/// wire 形态为 `{"type": "<tag>", ...字段}`（内部标签风格）。
/// rmp-serde 不支持 serde 内部标签枚举的 map 展开序列化（会把 newtype 变体拍成数组），
/// 故手写 `Serialize`/`Deserialize`：序列化前转成 serde_json 对象，保证 wire 契约明确。
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
            WireMessage::Hello(h) => serde_json::json!({
                "type": "hello",
                "name": h.name,
                "version": h.version,
                "subscribe": h.subscribe,
            }),
            WireMessage::HelloAck { ok, reason } => {
                serde_json::json!({ "type": "hello_ack", "ok": ok, "reason": reason })
            }
            WireMessage::Request { id, method, params } => serde_json::json!({
                "type": "request",
                "id": id,
                "method": method,
                "params": params,
            }),
            WireMessage::Response {
                id,
                ok,
                result,
                error,
            } => serde_json::json!({
                "type": "response",
                "id": id,
                "ok": ok,
                "result": result,
                "error": error,
            }),
            WireMessage::Notify { event, data } => {
                serde_json::json!({ "type": "notify", "event": event, "data": data })
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
        let ty = map
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| D::Error::custom("帧体缺少 type 字段"))?;

        match ty {
            "hello" => {
                let name = map
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| D::Error::custom("hello 消息缺少 name 字段"))?;
                let version = map
                    .get("version")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| D::Error::custom("hello 消息缺少 version 字段"))?;
                let subscribe = match map.get("subscribe") {
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
                    version: version.to_string(),
                    subscribe,
                }))
            }
            "hello_ack" => {
                let ok = map
                    .get("ok")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| D::Error::custom("hello_ack 消息缺少 ok 字段"))?;
                let reason = match map.get("reason") {
                    None | Some(serde_json::Value::Null) => None,
                    Some(v) => Some(
                        v.as_str()
                            .map(str::to_string)
                            .ok_or_else(|| D::Error::custom("reason 字段必须是字符串"))?,
                    ),
                };
                Ok(WireMessage::HelloAck { ok, reason })
            }
            "request" => {
                let id = map
                    .get("id")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| D::Error::custom("request 消息缺少 id 字段"))?;
                let method = map
                    .get("method")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| D::Error::custom("request 消息缺少 method 字段"))?;
                let params = map
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
                let id = map
                    .get("id")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| D::Error::custom("response 消息缺少 id 字段"))?;
                let ok = map
                    .get("ok")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| D::Error::custom("response 消息缺少 ok 字段"))?;
                let result = match map.get("result") {
                    None | Some(serde_json::Value::Null) => None,
                    Some(v) => Some(v.clone()),
                };
                let error = match map.get("error") {
                    None | Some(serde_json::Value::Null) => None,
                    Some(v) => Some(
                        v.as_str()
                            .map(str::to_string)
                            .ok_or_else(|| D::Error::custom("error 字段必须是字符串"))?,
                    ),
                };
                Ok(WireMessage::Response {
                    id,
                    ok,
                    result,
                    error,
                })
            }
            "notify" => {
                let event = map
                    .get("event")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| D::Error::custom("notify 消息缺少 event 字段"))?;
                let data = map
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
pub fn encode_frame(msg: &WireMessage) -> Result<Vec<u8>, RuntimeError> {
    let body =
        rmp_serde::to_vec(msg).map_err(|e| RuntimeError::Plugin(format!("帧编码失败：{e}")))?;
    if body.len() as u32 > MAX_FRAME_LEN {
        return Err(RuntimeError::Plugin(format!(
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
pub fn decode_frame(full_frame: &[u8]) -> Result<WireMessage, RuntimeError> {
    if full_frame.len() < LEN_PREFIX_BYTES {
        return Err(RuntimeError::Plugin(format!(
            "帧过短：{} 字节",
            full_frame.len()
        )));
    }
    let len = u32::from_be_bytes(
        full_frame[..LEN_PREFIX_BYTES]
            .try_into()
            .map_err(|_| RuntimeError::Plugin("帧长度前缀读取失败".to_string()))?,
    ) as usize;
    if len > MAX_FRAME_LEN as usize {
        return Err(RuntimeError::Plugin(format!(
            "帧长度前缀 {len} 超过上限 {}",
            MAX_FRAME_LEN
        )));
    }
    if full_frame.len() != LEN_PREFIX_BYTES + len {
        return Err(RuntimeError::Plugin(format!(
            "帧体长度不符：期望 {len} 字节，实际 {} 字节",
            full_frame.len() - LEN_PREFIX_BYTES
        )));
    }
    rmp_serde::from_slice(&full_frame[LEN_PREFIX_BYTES..])
        .map_err(|e| RuntimeError::Plugin(format!("帧解码失败：{e}")))
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
    /// 将核心事件映射为事件类型（忽略数据字段）。
    pub fn from_core_event(e: &CoreEvent) -> EventType {
        match e {
            CoreEvent::MessageReceived(_) => EventType::MessageReceived,
            CoreEvent::MessageSent(_) => EventType::MessageSent,
            CoreEvent::CharacterStateChanged(_) => EventType::CharacterStateChanged,
            CoreEvent::EmotionChanged(_) => EventType::EmotionChanged,
            CoreEvent::RelationshipChanged(_) => EventType::RelationshipChanged,
            CoreEvent::MemoryCreated(_) => EventType::MemoryCreated,
            CoreEvent::BehaviorDecided(_) => EventType::BehaviorDecided,
            CoreEvent::ResponseGenerated(_) => EventType::ResponseGenerated,
            CoreEvent::AdapterConnected(_) => EventType::AdapterConnected,
            CoreEvent::AdapterDisconnected(_) => EventType::AdapterDisconnected,
            CoreEvent::ScheduledTaskTriggered(_) => EventType::ScheduledTaskTriggered,
        }
    }

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
    pub fn parse(s: &str) -> Result<EventType, RuntimeError> {
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
            _ => Err(RuntimeError::Plugin(format!("未知事件名：{s}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::event::{
        AdapterConnectedEvent, AdapterDisconnectedEvent, BehaviorDecidedEvent,
        CharacterStateChangedEvent, EmotionChangedEvent, MemoryCreatedEvent, MessageReceivedEvent,
        MessageSentEvent, RelationshipChangedEvent, ResponseGeneratedEvent,
        ScheduledTaskTriggeredEvent,
    };
    use chrono::{DateTime, TimeZone, Utc};

    fn ts() -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000, 0).unwrap()
    }

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
            value["type"].as_str(),
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
            "notify",
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
        // 手工构造 {"type": "bogus"} 的 MessagePack 体
        let body = rmp_serde::to_vec(&serde_json::json!({ "type": "bogus" })).unwrap();
        let mut frame = (body.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        assert!(decode_frame(&frame).is_err());
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

    #[test]
    fn from_core_event_covers_all_variants() {
        let events = vec![
            CoreEvent::MessageReceived(MessageReceivedEvent {
                conversation_id: 1,
                sender_id: 2,
                message_id: 3,
                content: "hi".to_string(),
                timestamp: ts(),
                is_mentioned: true,
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
                timestamp: ts(),
            }),
            CoreEvent::RelationshipChanged(RelationshipChangedEvent {
                character_id: 4,
                participant_id: 2,
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
            assert_eq!(EventType::from_core_event(event), ty);
        }
    }

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
