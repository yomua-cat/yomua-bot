//! 控制层集成测试
//!
//! 验证真实的 Unix Domain Socket 传输层工作正常。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::net::UnixStream;
use tokio::sync::watch;

use yomua_bot::adapters::onebot::{OneBotAdapter, OneBotAdapterImpl, OneBotConfig};
use yomua_bot::application::config::RuntimeConfig;
use yomua_bot::application::control::server::{start_control_service, RuntimeHandle};
use yomua_bot::application::control::types::*;
use yomua_bot::application::conversation::ConversationManager;
use yomua_bot::application::event_bus::EventBus;
use yomua_bot::application::plugin_api::PluginApi;
use yomua_bot::domain::repository::{ConversationRepository, ParticipantRepository};
use yomua_bot::error::{RepositoryError, RuntimeError};
use yomua_bot::infrastructure::plugin::registry::PluginRegistry;
use yomua_bot::infrastructure::plugin::supervisor::{PluginSupervisor, SupervisorConfig};
use yomua_bot::infrastructure::storage::SqliteStorage;

// ============================================================================
// Mock 组件
// ============================================================================

/// Mock WsTransport（不建立真实连接）
struct MockTransport;

#[async_trait]
impl yomua_bot::adapters::onebot::connection::WsTransport for MockTransport {
    async fn read_text(&mut self) -> Result<Option<String>, RuntimeError> {
        Ok(None)
    }
    async fn send_text(&mut self, _text: &str) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn ping(&mut self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

/// Mock WsConnector（不建立真实连接）
struct MockConnector;

#[async_trait]
impl yomua_bot::adapters::onebot::connection::WsConnector for MockConnector {
    async fn connect(
        &self,
        _config: &OneBotConfig,
    ) -> Result<Box<dyn yomua_bot::adapters::onebot::connection::WsTransport>, RuntimeError> {
        Ok(Box::new(MockTransport))
    }
}

// 内存 Repository 实现
struct MemConvRepo;
#[async_trait]
impl ConversationRepository for MemConvRepo {
    async fn find_by_id(
        &self,
        _id: i64,
    ) -> Result<Option<yomua_bot::domain::conversation::Conversation>, RepositoryError> {
        Ok(None)
    }
    async fn find_by_external_id(
        &self,
        _id: &str,
    ) -> Result<Option<yomua_bot::domain::conversation::Conversation>, RepositoryError> {
        Ok(None)
    }
    async fn find_all(
        &self,
    ) -> Result<Vec<yomua_bot::domain::conversation::Conversation>, RepositoryError> {
        Ok(vec![])
    }
    async fn insert(
        &self,
        _c: &yomua_bot::domain::conversation::Conversation,
    ) -> Result<i64, RepositoryError> {
        Ok(1)
    }
    async fn update(
        &self,
        _c: &yomua_bot::domain::conversation::Conversation,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }
    async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
        Ok(())
    }
}

struct MemPartRepo;
#[async_trait]
impl ParticipantRepository for MemPartRepo {
    async fn find_by_id(
        &self,
        _id: i64,
    ) -> Result<Option<yomua_bot::domain::conversation::Participant>, RepositoryError> {
        Ok(None)
    }
    async fn find_by_external_id(
        &self,
        _conversation_id: i64,
        _id: &str,
    ) -> Result<Option<yomua_bot::domain::conversation::Participant>, RepositoryError> {
        Ok(None)
    }
    async fn find_by_conversation_id(
        &self,
        _conversation_id: i64,
    ) -> Result<Vec<yomua_bot::domain::conversation::Participant>, RepositoryError> {
        Ok(vec![])
    }
    async fn insert(
        &self,
        _p: &yomua_bot::domain::conversation::Participant,
    ) -> Result<i64, RepositoryError> {
        Ok(1)
    }
}

// 最小化 PluginApi（只包含必需的 trait 对象）
#[derive(Clone)]
struct MinimalPluginApi;

#[async_trait]
impl yomua_bot::domain::repository::CharacterRepository for MinimalPluginApi {
    async fn find_by_id(
        &self,
        _id: i64,
    ) -> Result<Option<yomua_bot::domain::character::Character>, RepositoryError> {
        Ok(None)
    }
    async fn find_all(
        &self,
    ) -> Result<Vec<yomua_bot::domain::character::Character>, RepositoryError> {
        Ok(vec![])
    }
    async fn insert(
        &self,
        _c: &yomua_bot::domain::character::Character,
    ) -> Result<i64, RepositoryError> {
        Ok(1)
    }
    async fn update(
        &self,
        _c: &yomua_bot::domain::character::Character,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }
    async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
        Ok(())
    }
}

#[async_trait]
impl yomua_bot::domain::repository::CharacterStateRepository for MinimalPluginApi {
    async fn find_by_character_id(
        &self,
        _id: i64,
    ) -> Result<Option<yomua_bot::domain::character::CharacterState>, RepositoryError> {
        Ok(None)
    }
    async fn upsert(
        &self,
        _id: i64,
        _s: &yomua_bot::domain::character::CharacterState,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }
}

#[async_trait]
impl yomua_bot::domain::repository::CharacterBindingRepository for MinimalPluginApi {
    async fn find_by_character_id(
        &self,
        _id: i64,
    ) -> Result<Vec<yomua_bot::domain::character::CharacterBinding>, RepositoryError> {
        Ok(vec![])
    }
    async fn find_by_conversation_id(
        &self,
        _id: i64,
    ) -> Result<Vec<yomua_bot::domain::character::CharacterBinding>, RepositoryError> {
        Ok(vec![])
    }
    async fn find_all(
        &self,
    ) -> Result<Vec<yomua_bot::domain::character::CharacterBinding>, RepositoryError> {
        Ok(vec![])
    }
    async fn find_all_enabled(
        &self,
    ) -> Result<Vec<yomua_bot::domain::character::CharacterBinding>, RepositoryError> {
        Ok(vec![])
    }
    async fn insert(
        &self,
        _b: &yomua_bot::domain::character::CharacterBinding,
    ) -> Result<i64, RepositoryError> {
        Ok(1)
    }
    async fn update(
        &self,
        _b: &yomua_bot::domain::character::CharacterBinding,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }
    async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
        Ok(())
    }
}

#[async_trait]
impl yomua_bot::domain::repository::MessageRepository for MinimalPluginApi {
    async fn find_by_id(
        &self,
        _id: i64,
    ) -> Result<Option<yomua_bot::domain::message::Message>, RepositoryError> {
        Ok(None)
    }
    async fn find_recent(
        &self,
        _conversation_id: i64,
        _limit: i64,
    ) -> Result<Vec<yomua_bot::domain::message::Message>, RepositoryError> {
        Ok(vec![])
    }
    async fn insert(
        &self,
        _m: &yomua_bot::domain::message::Message,
    ) -> Result<i64, RepositoryError> {
        Ok(1)
    }
    async fn latest_message_time(
        &self,
        _conversation_id: i64,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, RepositoryError> {
        Ok(None)
    }
}

#[async_trait]
impl yomua_bot::domain::repository::MemoryRepository for MinimalPluginApi {
    async fn find_by_character_id(
        &self,
        _id: i64,
        _t: Option<yomua_bot::domain::memory::MemoryType>,
        _l: i64,
    ) -> Result<Vec<yomua_bot::domain::memory::Memory>, RepositoryError> {
        Ok(vec![])
    }
    async fn insert(&self, _m: &yomua_bot::domain::memory::Memory) -> Result<i64, RepositoryError> {
        Ok(1)
    }
    async fn update(&self, _m: &yomua_bot::domain::memory::Memory) -> Result<(), RepositoryError> {
        Ok(())
    }
    async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
        Ok(())
    }
    async fn search_by_embedding(
        &self,
        _id: i64,
        _e: &[f32],
        _t: Option<&str>,
        _l: i64,
    ) -> Result<Vec<yomua_bot::domain::memory::SemanticMatchResult>, RepositoryError> {
        Ok(vec![])
    }
    async fn insert_semantic(
        &self,
        _id: i64,
        _c: Option<i64>,
        _t: &str,
        _content: &str,
        _e: &[f32],
        _importance: f64,
        _metadata: &str,
    ) -> Result<i64, RepositoryError> {
        Ok(1)
    }
}

#[async_trait]
impl yomua_bot::domain::repository::RelationshipRepository for MinimalPluginApi {
    async fn find(
        &self,
        _cid: i64,
        _pid: i64,
    ) -> Result<Option<yomua_bot::domain::relationship::Relationship>, RepositoryError> {
        Ok(None)
    }
    async fn find_by_character_id(
        &self,
        _id: i64,
    ) -> Result<Vec<yomua_bot::domain::relationship::Relationship>, RepositoryError> {
        Ok(vec![])
    }
    async fn upsert(
        &self,
        _r: &yomua_bot::domain::relationship::Relationship,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }
}

#[async_trait]
impl yomua_bot::domain::repository::EmotionStateRepository for MinimalPluginApi {
    #[allow(deprecated)]
    async fn find_by_character_id(
        &self,
        _id: i64,
    ) -> Result<Option<yomua_bot::domain::emotion::EmotionState>, RepositoryError> {
        Ok(None)
    }
    async fn find_by_character_and_conversation(
        &self,
        _character_id: i64,
        _conversation_id: i64,
    ) -> Result<Option<yomua_bot::domain::emotion::EmotionState>, RepositoryError> {
        Ok(None)
    }
    #[allow(deprecated)]
    async fn upsert(
        &self,
        _id: i64,
        _s: &yomua_bot::domain::emotion::EmotionState,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }
    async fn upsert_scoped(
        &self,
        _character_id: i64,
        _conversation_id: i64,
        _s: &yomua_bot::domain::emotion::EmotionState,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }
}

#[async_trait]
impl yomua_bot::domain::repository::PluginDataRepository for MinimalPluginApi {
    async fn get(&self, _p: &str, _k: &str) -> Result<Option<serde_json::Value>, RepositoryError> {
        Ok(None)
    }
    async fn set(&self, _p: &str, _k: &str, _v: &serde_json::Value) -> Result<(), RepositoryError> {
        Ok(())
    }
    async fn delete(&self, _p: &str, _k: &str) -> Result<(), RepositoryError> {
        Ok(())
    }
    async fn list_keys(&self, _p: &str) -> Result<Vec<String>, RepositoryError> {
        Ok(vec![])
    }
}

// ============================================================================
// 测试工具
// ============================================================================

/// 创建测试用 RuntimeHandle
async fn create_test_handle(socket_dir: &Path) -> (RuntimeHandle, PathBuf) {
    // 服务器会在 data_dir 下创建 control.sock
    let socket_path = socket_dir.join("control.sock");

    let config = RuntimeConfig {
        data_dir: socket_dir.to_string_lossy().to_string(),
        log_level: "info".to_string(),
        shutdown_timeout_secs: 10,
        plugins_dir: None,
        admin_users: None,
        broadcast_capacity: Some(256),
    };

    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("创建内存存储失败");

    let bus = EventBus::new();
    let conversation_manager =
        ConversationManager::new(Arc::new(MemConvRepo), Arc::new(MemPartRepo));

    let (shutdown_tx, _) = watch::channel(false);

    // 创建适配器（使用 MockConnector 不建立真实连接）
    let adapter = OneBotAdapterImpl::with_connector(
        OneBotConfig::default(),
        bus,
        conversation_manager,
        Arc::new(MockConnector),
    );
    let adapter = Arc::new(adapter);

    // 创建最小的 PluginApi
    let minimal_api = MinimalPluginApi;
    let plugin_api = PluginApi::new(
        Arc::new(minimal_api.clone())
            as Arc<dyn yomua_bot::domain::repository::CharacterRepository>,
        Arc::new(minimal_api.clone())
            as Arc<dyn yomua_bot::domain::repository::CharacterStateRepository>,
        Arc::new(minimal_api.clone())
            as Arc<dyn yomua_bot::domain::repository::CharacterBindingRepository>,
        Arc::new(minimal_api.clone()) as Arc<dyn yomua_bot::domain::repository::MessageRepository>,
        Arc::new(minimal_api.clone()) as Arc<dyn yomua_bot::domain::repository::MemoryRepository>,
        Arc::new(minimal_api.clone())
            as Arc<dyn yomua_bot::domain::repository::RelationshipRepository>,
        Arc::new(minimal_api.clone())
            as Arc<dyn yomua_bot::domain::repository::PluginDataRepository>,
        Arc::new(yomua_bot::application::action::ActionDispatcher::new(
            Arc::new(MemConvRepo) as Arc<dyn ConversationRepository>,
            adapter.clone() as Arc<dyn OneBotAdapter>,
        )),
        Arc::new(yomua_bot::application::cognition::CognitionLayer::new(
            None,
            Arc::new(yomua_bot::application::context::ContextBuilder::new(
                Arc::new(minimal_api.clone())
                    as Arc<dyn yomua_bot::domain::repository::MessageRepository>,
                Arc::new(MemConvRepo) as Arc<dyn ConversationRepository>,
                Arc::new(minimal_api.clone())
                    as Arc<dyn yomua_bot::domain::repository::MemoryRepository>,
                Arc::new(minimal_api.clone())
                    as Arc<dyn yomua_bot::domain::repository::RelationshipRepository>,
                Arc::new(minimal_api.clone())
                    as Arc<dyn yomua_bot::domain::repository::EmotionStateRepository>,
                Arc::new(minimal_api.clone())
                    as Arc<dyn yomua_bot::domain::repository::CharacterBindingRepository>,
            )),
        )),
        Arc::new(PluginRegistry::default()),
    );

    let handle = RuntimeHandle {
        config_dir: socket_dir.to_path_buf(),
        runtime_cfg: config,
        data_dir: socket_dir.to_path_buf(),
        supervisor: Arc::new(PluginSupervisor::new(
            SupervisorConfig::default(),
            Arc::new(PluginRegistry::default()),
            Arc::new(plugin_api),
        )),
        adapter,
        storage: Arc::new(storage),
        shutdown_tx,
    };

    (handle, socket_path)
}

async fn send_request(
    socket: &PathBuf,
    request: ControlRequest,
) -> Result<ControlResponse, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(|e| format!("连接失败: {}", e))?;

    let json = serde_json::to_string(&request).map_err(|e| format!("序列化失败: {}", e))?;
    stream
        .write_all(json.as_bytes())
        .await
        .map_err(|e| format!("发送失败: {}", e))?;
    // 关闭写端以通知服务器请求已结束
    stream
        .shutdown()
        .await
        .map_err(|e| format!("关闭写端失败: {}", e))?;

    // 读取所有响应数据
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(|e| format!("接收失败: {}", e))?;

    if buf.is_empty() {
        return Err("服务器关闭连接".to_string());
    }

    serde_json::from_slice(&buf).map_err(|e| format!("解析失败: {}", e))
}

// ============================================================================
// 测试用例
// ============================================================================

/// 测试服务器启动并创建 socket
#[tokio::test]
async fn test_server_startup_and_socket_created() {
    let socket_dir = std::env::temp_dir().join(format!(
        "yomua-test-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&socket_dir);

    let (handle, socket_path) = create_test_handle(&socket_dir).await;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let _server = start_control_service(handle, shutdown_rx);

    // 等待服务器启动
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 验证 socket 存在
    assert!(
        socket_path.exists(),
        "socket 文件应该被创建: {:?}",
        socket_path
    );

    let _ = std::fs::remove_dir_all(&socket_dir);
}

/// 测试正常命令执行
#[tokio::test]
async fn test_system_version_command() {
    let socket_dir = std::env::temp_dir().join(format!(
        "yomua-test-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&socket_dir);

    let (handle, socket_path) = create_test_handle(&socket_dir).await;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let _server = start_control_service(handle, shutdown_rx);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let request = ControlRequest {
        id: RequestId::new(),
        cmd: "system.version".to_string(),
        args: vec![],
        options: std::collections::HashMap::new(),
        session: None,
    };

    let resp = send_request(&socket_path, request).await.unwrap();
    assert_eq!(resp.code, Code::Success);

    let _ = std::fs::remove_dir_all(&socket_dir);
}

/// 测试未知命令返回 CommandNotFound
#[tokio::test]
async fn test_unknown_command() {
    let socket_dir = std::env::temp_dir().join(format!(
        "yomua-test-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&socket_dir);

    let (handle, socket_path) = create_test_handle(&socket_dir).await;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let _server = start_control_service(handle, shutdown_rx);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let request = ControlRequest {
        id: RequestId::new(),
        cmd: "nonexistent.command".to_string(),
        args: vec![],
        options: std::collections::HashMap::new(),
        session: None,
    };

    let resp = send_request(&socket_path, request).await.unwrap();
    // 不存在的命令应该返回 CommandNotFound
    assert_eq!(resp.code, Code::CommandNotFound);

    let _ = std::fs::remove_dir_all(&socket_dir);
}

/// 测试 __list_commands 命令（REPL command discovery）
#[tokio::test]
async fn test_list_commands() {
    let socket_dir = std::env::temp_dir().join(format!(
        "yomua-test-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&socket_dir);

    let (handle, socket_path) = create_test_handle(&socket_dir).await;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let _server = start_control_service(handle, shutdown_rx);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let request = ControlRequest {
        id: RequestId::new(),
        cmd: "__list_commands".to_string(),
        args: vec![],
        options: std::collections::HashMap::new(),
        session: Some(SessionInfo {
            id: None,
            source: SessionSource::Repl,
            user_id: None,
        }),
    };

    let resp = send_request(&socket_path, request).await.unwrap();
    assert_eq!(resp.code, Code::Success);
    assert!(resp.data.is_some());

    let data = resp.data.unwrap();
    assert!(data.get("commands").is_some());
    assert!(data.get("top_level_commands").is_some());

    let commands = data.get("commands").unwrap().as_array().unwrap();
    // 应该有内置命令
    assert!(!commands.is_empty());

    // 验证命令格式
    for cmd in commands {
        assert!(cmd.get("path").is_some());
        assert!(cmd.get("description").is_some());
    }

    let tops = data.get("top_level_commands").unwrap().as_array().unwrap();
    assert!(!tops.is_empty());
    // 顶级命令应该有 status, config, system 等
    let tops_str: Vec<&str> = tops.iter().filter_map(|v| v.as_str()).collect();
    assert!(tops_str.contains(&"status"));
    assert!(tops_str.contains(&"config"));
    assert!(tops_str.contains(&"system"));

    let _ = std::fs::remove_dir_all(&socket_dir);
}

/// 测试请求 ID 匹配
#[tokio::test]
async fn test_request_id_matching() {
    let socket_dir = std::env::temp_dir().join(format!(
        "yomua-test-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&socket_dir);

    let (handle, socket_path) = create_test_handle(&socket_dir).await;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let _server = start_control_service(handle, shutdown_rx);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let request_id = RequestId::new();
    let request = ControlRequest {
        id: request_id,
        cmd: "system.version".to_string(),
        args: vec![],
        options: std::collections::HashMap::new(),
        session: None,
    };

    let resp = send_request(&socket_path, request).await.unwrap();
    assert_eq!(resp.id, request_id);

    let _ = std::fs::remove_dir_all(&socket_dir);
}

/// 测试并发请求
#[tokio::test]
async fn test_concurrent_requests() {
    let socket_dir = std::env::temp_dir().join(format!(
        "yomua-test-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&socket_dir);

    let (handle, socket_path) = create_test_handle(&socket_dir).await;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let _server = start_control_service(handle, shutdown_rx);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut handles = Vec::new();
    for _ in 0..5 {
        let socket_path = socket_path.clone();
        handles.push(tokio::spawn(async move {
            let request = ControlRequest {
                id: RequestId::new(),
                cmd: "system.version".to_string(),
                args: vec![],
                options: std::collections::HashMap::new(),
                session: None,
            };
            send_request(&socket_path, request).await
        }));
    }

    for handle in handles {
        let resp = handle.await.unwrap().unwrap();
        assert_eq!(resp.code, Code::Success);
    }

    let _ = std::fs::remove_dir_all(&socket_dir);
}

/// 测试 malformed JSON
#[tokio::test]
async fn test_malformed_json() {
    let socket_dir = std::env::temp_dir().join(format!(
        "yomua-test-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&socket_dir);

    let (handle, socket_path) = create_test_handle(&socket_dir).await;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let _server = start_control_service(handle, shutdown_rx);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // malformed JSON 测试需要自己写原始数据
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = UnixStream::connect(&socket_path).await.unwrap();
    stream.write_all(b"{ invalid json }").await.unwrap();
    stream.shutdown().await.unwrap();

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();

    if !buf.is_empty() {
        let resp: ControlResponse = serde_json::from_slice(&buf).unwrap();
        assert_eq!(resp.code, Code::ProtocolError);
    }

    let _ = std::fs::remove_dir_all(&socket_dir);
}

/// 测试 help 命令
#[tokio::test]
async fn test_help_command() {
    let socket_dir = std::env::temp_dir().join(format!(
        "yomua-test-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&socket_dir);

    let (handle, socket_path) = create_test_handle(&socket_dir).await;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let _server = start_control_service(handle, shutdown_rx);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let request = ControlRequest {
        id: RequestId::new(),
        cmd: "help".to_string(),
        args: vec![],
        options: std::collections::HashMap::new(),
        session: None,
    };

    let resp = send_request(&socket_path, request).await.unwrap();
    assert_eq!(resp.code, Code::Success);
    assert!(resp.data.is_some());

    let _ = std::fs::remove_dir_all(&socket_dir);
}

/// 测试 status 命令（需要 runtime status_fn）
#[tokio::test]
async fn test_status_command() {
    let socket_dir = std::env::temp_dir().join(format!(
        "yomua-test-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&socket_dir);

    let (handle, socket_path) = create_test_handle(&socket_dir).await;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let _server = start_control_service(handle, shutdown_rx);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let request = ControlRequest {
        id: RequestId::new(),
        cmd: "status".to_string(),
        args: vec![],
        options: std::collections::HashMap::new(),
        session: None,
    };

    let resp = send_request(&socket_path, request).await.unwrap();
    assert_eq!(resp.code, Code::Success);

    let _ = std::fs::remove_dir_all(&socket_dir);
}

/// 测试 runtime.status 命令
#[tokio::test]
async fn test_runtime_status_command() {
    let socket_dir = std::env::temp_dir().join(format!(
        "yomua-test-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&socket_dir);

    let (handle, socket_path) = create_test_handle(&socket_dir).await;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let _server = start_control_service(handle, shutdown_rx);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let request = ControlRequest {
        id: RequestId::new(),
        cmd: "runtime.status".to_string(),
        args: vec![],
        options: std::collections::HashMap::new(),
        session: None,
    };

    let resp = send_request(&socket_path, request).await.unwrap();
    assert_eq!(resp.code, Code::Success);

    let _ = std::fs::remove_dir_all(&socket_dir);
}
