//! 插件运行时注册表 —— 记录已接入插件的元数据、状态、事件订阅与实时连接。
//!
//! 本模块支撑 Plugin API 的权限查询（`permissions_for`）、事件订阅、
//! socket 连接的挂接/摘除与生命周期状态机。所有方法为同步调用，内部通过
//! `Mutex` 保护共享映射。
//!
//! `PluginRecord` 上的 `conn` 字段持有指向插件连接写通道的发送端。它保留在
//! `Clone` 派生的复制里（发送端克隆语义安全——同一通道可被多处引用），
//! 因此 `PluginRecord::clone()` 得到的副本共享同一连接句柄，调用方在使用
//! 副本并发发送时会公平竞争写通道，不会产生语义问题。

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::mpsc;

use crate::error::RuntimeError;
use crate::infrastructure::plugin::protocol::{EventType, WireMessage};
use crate::infrastructure::plugin::{PluginManifest, PluginPermission, PluginState};

/// 一条插件运行时记录。
#[derive(Debug, Clone)]
pub struct PluginRecord {
    pub manifest: PluginManifest,
    pub state: PluginState,
    pub pid: Option<u32>,
    pub restart_count: u32,
    pub last_error: Option<String>,
    pub subscriptions: Vec<EventType>,
    /// 插件实时连接的写通道发送端；`None` 表示未连接。见模块文档关于 Clone 语义的说明。
    pub conn: Option<mpsc::Sender<WireMessage>>,
}

/// 插件运行时注册表。
#[derive(Debug, Default)]
pub struct PluginRegistry {
    inner: Mutex<HashMap<String, PluginRecord>>,
}

impl PluginRegistry {
    /// 创建一个空注册表。
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册一个插件；同名插件拒绝注册并返回错误。
    pub fn register(&self, manifest: PluginManifest) -> Result<(), RuntimeError> {
        let mut map = self.inner.lock().unwrap();
        if map.contains_key(&manifest.name) {
            return Err(RuntimeError::Plugin(format!(
                "插件名冲突，无法注册：{}",
                manifest.name
            )));
        }
        map.insert(
            manifest.name.clone(),
            PluginRecord {
                manifest,
                state: PluginState::Discovered,
                pid: None,
                restart_count: 0,
                last_error: None,
                subscriptions: Vec::new(),
                conn: None,
            },
        );
        Ok(())
    }

    /// 更新插件生命周期状态。插件未注册时静默忽略。
    pub fn set_state(&self, name: &str, state: PluginState) {
        if let Some(record) = self.inner.lock().unwrap().get_mut(name) {
            record.state = state;
        }
    }

    /// 返回一个插件的运行时记录副本；未注册返回 `None`。
    pub fn get(&self, name: &str) -> Option<PluginRecord> {
        self.inner.lock().unwrap().get(name).cloned()
    }

    /// 返回全部插件的运行时记录副本。
    pub fn all(&self) -> Vec<PluginRecord> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    /// 返回某个插件已被授予的权限；未注册返回 `None`。
    pub fn permissions_for(&self, name: &str) -> Option<Vec<PluginPermission>> {
        self.inner
            .lock()
            .unwrap()
            .get(name)
            .map(|r| r.manifest.permissions.clone())
    }

    /// 设置插件进程 PID（`None` 表示未启动）。未注册时静默忽略。
    pub fn set_pid(&self, name: &str, pid: Option<u32>) {
        if let Some(record) = self.inner.lock().unwrap().get_mut(name) {
            record.pid = pid;
        }
    }

    /// 记录插件最近一次错误。未注册时静默忽略。
    pub fn set_last_error(&self, name: &str, last_error: Option<String>) {
        if let Some(record) = self.inner.lock().unwrap().get_mut(name) {
            record.last_error = last_error;
        }
    }

    /// 重启计数加一。未注册时静默忽略。
    pub fn record_restart(&self, name: &str) {
        if let Some(record) = self.inner.lock().unwrap().get_mut(name) {
            record.restart_count += 1;
        }
    }

    /// 清零重启计数。未注册时静默忽略。
    pub fn clear_restarts(&self, name: &str) {
        if let Some(record) = self.inner.lock().unwrap().get_mut(name) {
            record.restart_count = 0;
        }
    }

    /// 设置插件的订阅事件列表。未注册时静默忽略。
    pub fn set_subscriptions(&self, name: &str, subscriptions: Vec<EventType>) {
        if let Some(record) = self.inner.lock().unwrap().get_mut(name) {
            record.subscriptions = subscriptions;
        }
    }

    /// 返回订阅了指定事件的插件名列表（按名称排序，保证顺序可预期）。
    ///
    /// 供事件桥接层复用：事件到达时用它枚举应通知的插件。
    /// 未知事件名在 [`EventType::parse`] 阶段已被拒绝，因此这里只按
    /// [`EventType`] 精确匹配即可。
    pub fn subscribed_plugins(&self, event: EventType) -> Vec<String> {
        let mut names: Vec<String> = self
            .inner
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.subscriptions.contains(&event))
            .map(|r| r.manifest.name.clone())
            .collect();
        names.sort();
        names
    }

    /// 挂接一个插件连接并推进到 `Running`。
    ///
    /// 成功条件：插件已注册、尚未存在连接、且当前状态为 `Starting`。
    /// 成功时写入连接句柄并置 `Running`。重启预算**不再在此清零** ——
    /// 清零改由 supervisor 在插件稳定运行超过 `stable_window` 后统一执行，
    /// 否则“连上即崩”的插件会因每次 attach 清零预算而被无限重启。
    pub fn attach_conn(
        &self,
        name: &str,
        tx: mpsc::Sender<WireMessage>,
    ) -> Result<(), RuntimeError> {
        let mut map = self.inner.lock().unwrap();
        let record = map
            .get_mut(name)
            .ok_or_else(|| RuntimeError::Plugin("插件未注册".to_string()))?;
        if record.conn.is_some() {
            return Err(RuntimeError::Plugin("插件连接已存在".to_string()));
        }
        if record.state != PluginState::Starting {
            return Err(RuntimeError::Plugin(
                "插件不在启动状态，拒绝连接".to_string(),
            ));
        }
        record.conn = Some(tx);
        record.state = PluginState::Running;
        Ok(())
    }

    /// 摘除插件的连接句柄（置 `None`）。状态由 supervisor 控制，本方法不动状态。
    pub fn clear_conn(&self, name: &str) {
        if let Some(record) = self.inner.lock().unwrap().get_mut(name) {
            record.conn = None;
        }
    }

    /// 返回插件当前连接的写通道发送端；未连接返回 `None`。
    pub fn connected_plugin(&self, name: &str) -> Option<mpsc::Sender<WireMessage>> {
        self.inner.lock().unwrap().get(name)?.conn.clone()
    }

    /// 声明式状态迁移辅助 —— 仅当插件当前处于 `expected_from` 时迁移到 `to`。
    ///
    /// 供 supervisor 显式声明状态流（如 Starting → Crashed），测试钉死状态机。
    /// 状态不匹配或插件未注册均返回中文错误。
    pub fn try_transition(
        &self,
        name: &str,
        expected_from: PluginState,
        to: PluginState,
    ) -> Result<(), RuntimeError> {
        let mut map = self.inner.lock().unwrap();
        let record = map
            .get_mut(name)
            .ok_or_else(|| RuntimeError::Plugin("插件未注册".to_string()))?;
        if record.state != expected_from {
            return Err(RuntimeError::Plugin(format!(
                "插件 {name} 状态不匹配：当前 {:?}，期望从 {:?} 迁移到 {:?}",
                record.state, expected_from, to
            )));
        }
        record.state = to;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn register_then_get_returns_record() {
        let registry = PluginRegistry::new();
        registry
            .register(manifest("alpha", vec![PluginPermission::MessageSend]))
            .unwrap();

        let record = registry.get("alpha").expect("注册后应可查到");
        assert_eq!(record.manifest.name, "alpha");
        assert_eq!(
            record.state,
            PluginState::Discovered,
            "初始状态应为 Discovered"
        );
        assert_eq!(record.pid, None);
        assert_eq!(record.restart_count, 0);
        assert!(record.last_error.is_none());
        assert!(record.subscriptions.is_empty());

        assert!(registry.get("stranger").is_none(), "未注册插件应查不到");
    }

    #[test]
    fn duplicate_register_rejected() {
        let registry = PluginRegistry::new();
        registry.register(manifest("alpha", vec![])).unwrap();
        let err = registry.register(manifest("alpha", vec![])).unwrap_err();
        assert!(err.to_string().contains("插件名冲突"), "错误信息：{err}");
    }

    #[test]
    fn state_and_pid_and_error_update() {
        let registry = PluginRegistry::new();
        registry.register(manifest("alpha", vec![])).unwrap();

        registry.set_state("alpha", PluginState::Running);
        assert_eq!(registry.get("alpha").unwrap().state, PluginState::Running);

        registry.set_pid("alpha", Some(42));
        assert_eq!(registry.get("alpha").unwrap().pid, Some(42));
        registry.set_pid("alpha", None);
        assert_eq!(registry.get("alpha").unwrap().pid, None);

        registry.set_last_error("alpha", Some("启动超时".to_string()));
        assert_eq!(
            registry.get("alpha").unwrap().last_error.as_deref(),
            Some("启动超时")
        );
        registry.set_last_error("alpha", None);
        assert!(registry.get("alpha").unwrap().last_error.is_none());

        // 对未注册插件操作静默忽略，不 panic。
        registry.set_state("stranger", PluginState::Running);
        registry.set_pid("stranger", Some(1));
        assert!(registry.get("stranger").is_none());
    }

    #[test]
    fn restart_counting_and_clear() {
        let registry = PluginRegistry::new();
        registry.register(manifest("alpha", vec![])).unwrap();

        registry.record_restart("alpha");
        registry.record_restart("alpha");
        assert_eq!(registry.get("alpha").unwrap().restart_count, 2);

        registry.clear_restarts("alpha");
        assert_eq!(registry.get("alpha").unwrap().restart_count, 0);
    }

    #[test]
    fn permissions_for_returns_manifest_permissions() {
        let registry = PluginRegistry::new();
        registry
            .register(manifest(
                "alpha",
                vec![PluginPermission::MemoryRead, PluginPermission::LlmCall],
            ))
            .unwrap();

        assert_eq!(
            registry.permissions_for("alpha").unwrap(),
            vec![PluginPermission::MemoryRead, PluginPermission::LlmCall]
        );
        assert_eq!(registry.permissions_for("stranger"), None);
    }

    #[test]
    fn subscriptions_and_filtering() {
        let registry = PluginRegistry::new();
        registry.register(manifest("alpha", vec![])).unwrap();
        registry.register(manifest("beta", vec![])).unwrap();
        registry.register(manifest("gamma", vec![])).unwrap();

        // 空订阅：初始不订阅任何事件。
        registry.set_subscriptions(
            "alpha",
            vec![EventType::MessageReceived, EventType::MemoryCreated],
        );
        registry.set_subscriptions("beta", vec![EventType::MessageReceived]);
        registry.set_subscriptions("gamma", vec![]);

        // 按事件过滤，未订阅者不出现。
        assert_eq!(
            registry.subscribed_plugins(EventType::MessageReceived),
            vec!["alpha", "beta"]
        );
        assert_eq!(
            registry.subscribed_plugins(EventType::MemoryCreated),
            vec!["alpha"]
        );
        assert!(
            registry
                .subscribed_plugins(EventType::EmotionChanged)
                .is_empty(),
            "无人订阅的事件不应下发"
        );

        // 重复订阅同一事件 → 一个插件只出现一次。
        registry.set_subscriptions(
            "alpha",
            vec![EventType::MessageReceived, EventType::MessageReceived],
        );
        assert_eq!(
            registry.subscribed_plugins(EventType::MessageReceived),
            vec!["alpha", "beta"]
        );
    }

    #[test]
    fn all_returns_every_plugin() {
        let registry = PluginRegistry::new();
        registry.register(manifest("alpha", vec![])).unwrap();
        registry.register(manifest("beta", vec![])).unwrap();

        let mut names: Vec<String> = registry
            .all()
            .into_iter()
            .map(|r| r.manifest.name)
            .collect();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn attach_conn_sets_running_keeps_restart_count() {
        let registry = PluginRegistry::new();
        registry.register(manifest("alpha", vec![])).unwrap();
        registry.record_restart("alpha");
        registry.record_restart("alpha");
        registry.set_state("alpha", PluginState::Starting);

        let (tx, _rx) = mpsc::channel::<WireMessage>(16);
        registry.attach_conn("alpha", tx).unwrap();

        let record = registry.get("alpha").unwrap();
        assert_eq!(
            record.state,
            PluginState::Running,
            "attach 成功应置 Running"
        );
        assert_eq!(
            record.restart_count, 2,
            "attach 不应清零重启计数：预算清零改由稳定运行窗口负责（supervisor）"
        );
        assert!(
            registry.connected_plugin("alpha").is_some(),
            "attach 后应可从 connected_plugin 取得发送端"
        );
    }

    #[tokio::test]
    async fn attach_conn_rejects_duplicate() {
        let registry = PluginRegistry::new();
        registry.register(manifest("alpha", vec![])).unwrap();
        registry.set_state("alpha", PluginState::Starting);

        let (tx1, _rx1) = mpsc::channel::<WireMessage>(16);
        let (tx2, _rx2) = mpsc::channel::<WireMessage>(16);
        registry.attach_conn("alpha", tx1).unwrap();
        let err = registry.attach_conn("alpha", tx2).unwrap_err();
        assert!(
            err.to_string().contains("插件连接已存在"),
            "错误信息：{err}"
        );
    }

    #[tokio::test]
    async fn attach_conn_rejects_non_starting_state() {
        let registry = PluginRegistry::new();
        registry.register(manifest("alpha", vec![])).unwrap();
        // 状态为 Discovered，非 Starting。
        let (tx, _rx) = mpsc::channel::<WireMessage>(16);
        let err = registry.attach_conn("alpha", tx).unwrap_err();
        assert!(err.to_string().contains("不在启动状态"), "错误信息：{err}");

        // 已 Running 的状态同样拒绝。
        registry.set_state("alpha", PluginState::Running);
        let (tx2, _rx2) = mpsc::channel::<WireMessage>(16);
        let err = registry.attach_conn("alpha", tx2).unwrap_err();
        assert!(err.to_string().contains("不在启动状态"), "错误信息：{err}");
    }

    #[tokio::test]
    async fn attach_conn_rejects_unregistered() {
        let registry = PluginRegistry::new();
        let (tx, _rx) = mpsc::channel::<WireMessage>(16);
        let err = registry.attach_conn("stranger", tx).unwrap_err();
        assert!(err.to_string().contains("插件未注册"), "错误信息：{err}");
    }

    #[tokio::test]
    async fn clear_conn_detaches() {
        let registry = PluginRegistry::new();
        registry.register(manifest("alpha", vec![])).unwrap();
        registry.set_state("alpha", PluginState::Starting);
        let (tx, _rx) = mpsc::channel::<WireMessage>(16);
        registry.attach_conn("alpha", tx).unwrap();
        assert!(registry.connected_plugin("alpha").is_some());

        registry.clear_conn("alpha");
        assert!(
            registry.connected_plugin("alpha").is_none(),
            "clear_conn 后应无连接"
        );
        // clear_conn 不动状态。
        assert_eq!(registry.get("alpha").unwrap().state, PluginState::Running);
        // 对未注册插件静默安全。
        registry.clear_conn("stranger");
    }

    #[tokio::test]
    async fn connected_plugin_returns_none_when_not_connected() {
        let registry = PluginRegistry::new();
        registry.register(manifest("alpha", vec![])).unwrap();
        assert!(registry.connected_plugin("alpha").is_none());
        assert!(registry.connected_plugin("stranger").is_none());
    }

    #[test]
    fn try_transition_success_and_failure() {
        let registry = PluginRegistry::new();
        registry.register(manifest("alpha", vec![])).unwrap();

        // 未注册插件。
        let err = registry
            .try_transition("stranger", PluginState::Discovered, PluginState::Starting)
            .unwrap_err();
        assert!(err.to_string().contains("插件未注册"), "错误信息：{err}");

        // 状态不匹配。
        // 当前状态 Discovered，期望从 Running 迁移会失败。
        let err = registry
            .try_transition("alpha", PluginState::Running, PluginState::Stopped)
            .unwrap_err();
        assert!(err.to_string().contains("状态不匹配"), "错误信息：{err}");

        // 成功迁移。
        registry
            .try_transition("alpha", PluginState::Discovered, PluginState::Starting)
            .unwrap();
        assert_eq!(registry.get("alpha").unwrap().state, PluginState::Starting);

        registry
            .try_transition("alpha", PluginState::Starting, PluginState::Crashed)
            .unwrap();
        assert_eq!(registry.get("alpha").unwrap().state, PluginState::Crashed);
    }
}
