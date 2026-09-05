//! 核心事件模型的测试。

use crate::domain::event::*;

#[test]
fn core_event_serialization_roundtrip() {
    let event = CoreEvent::MessageReceived(MessageReceivedEvent {
        conversation_id: 1,
        sender_id: 2,
        message_id: 3,
        content: "hello".to_string(),
        timestamp: chrono::Utc::now(),
        is_mentioned: true,
    });

    let json = serde_json::to_string(&event).unwrap();
    let deserialized: CoreEvent = serde_json::from_str(&json).unwrap();

    match deserialized {
        CoreEvent::MessageReceived(e) => {
            assert_eq!(e.conversation_id, 1);
            assert_eq!(e.sender_id, 2);
            assert_eq!(e.content, "hello");
            assert!(e.is_mentioned);
        }
        _ => panic!("expected MessageReceived"),
    }
}

#[test]
fn adapter_events() {
    let connected = CoreEvent::AdapterConnected(AdapterConnectedEvent {
        adapter_name: "onebot".to_string(),
        timestamp: chrono::Utc::now(),
    });

    let disconnected = CoreEvent::AdapterDisconnected(AdapterDisconnectedEvent {
        adapter_name: "onebot".to_string(),
        reason: Some("NapCat crashed".to_string()),
        timestamp: chrono::Utc::now(),
    });

    // 两者都应当能够序列化
    let _ = serde_json::to_string(&connected).unwrap();
    let _ = serde_json::to_string(&disconnected).unwrap();
}

#[test]
fn response_source_variants() {
    assert_ne!(ResponseSource::Rule, ResponseSource::Llm);
    assert_ne!(ResponseSource::Llm, ResponseSource::Plugin);
}

#[test]
fn command_received_roundtrip() {
    // 系统指令事件（硬性约束 B 的发布载体）可正常序列化往返，
    // 平台无关语义字段完整保留。
    let event = CoreEvent::CommandReceived(CommandReceivedEvent {
        conversation_id: 1,
        sender_id: 2,
        external_sender_id: "900001".to_string(),
        message_id: 1001,
        content: "换角色 木然".to_string(),
        timestamp: chrono::Utc::now(),
        command: Command::SwitchCharacter {
            character_name: "木然".to_string(),
        },
    });

    let json = serde_json::to_string(&event).unwrap();
    let deserialized: CoreEvent = serde_json::from_str(&json).unwrap();

    match deserialized {
        CoreEvent::CommandReceived(e) => {
            assert_eq!(e.conversation_id, 1);
            assert_eq!(e.sender_id, 2);
            assert_eq!(e.external_sender_id, "900001");
            assert_eq!(e.message_id, 1001);
            assert_eq!(e.content, "换角色 木然");
            assert_eq!(
                e.command,
                Command::SwitchCharacter {
                    character_name: "木然".to_string(),
                }
            );
        }
        _ => panic!("期望 CommandReceived"),
    }
}
