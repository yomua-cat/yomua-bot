//! 角色运行时 —— 核心应用编排器。
//!
//! 管理角色生命周期、按需加载与事件分发。

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{Mutex, RwLock};

use crate::application::event_bus::EventBus;
use crate::domain::character::{Character, CharacterState};
use crate::domain::event::{CharacterStateChangedEvent, CoreEvent};
use crate::domain::repository::{
    CharacterRepository, CharacterStateRepository, ConversationRepository,
};
use crate::error::{DomainError, RuntimeError};

/// 角色运行时。
///
/// 管理已加载的角色、缓存状态，并协调事件处理。
/// 不直接依赖 SQLite、OneBot 或任何 LLM。
pub struct CharacterRuntime {
    /// 角色持久化仓库。
    character_repo: Arc<dyn CharacterRepository>,

    /// 角色状态持久化仓库。
    state_repo: Arc<dyn CharacterStateRepository>,

    /// 会话仓库。
    #[allow(dead_code)]
    conversation_repo: Arc<dyn crate::domain::repository::ConversationRepository>,

    /// 事件总线（发布状态变更等核心事件）。
    event_bus: EventBus,

    /// 已加载角色的内存缓存（character_id → Character）。
    cache: RwLock<HashMap<i64, Character>>,

    /// 每个角色的状态转换锁，用于串行化同一角色的状态读-改-写流程。
    ///
    /// 保证同一角色的 `load_character` / `apply_state_patch` 之间的
    /// 「持久化先于事件」原子成立，避免并发下重复初始化默认状态、
    /// 重复发布事件或丢失更新（读与写之间不被其他任务覆盖）。
    per_character_locks: RwLock<HashMap<i64, Arc<Mutex<()>>>>,
}

impl CharacterRuntime {
    /// 使用给定的仓库创建一个新的运行时（不带事件总线）。
    ///
    /// 内部转发到 [`Self::with_event_bus`]，使用一个独立的空总线。
    pub fn new(
        character_repo: Arc<dyn CharacterRepository>,
        state_repo: Arc<dyn CharacterStateRepository>,
        conversation_repo: Arc<dyn ConversationRepository>,
    ) -> Self {
        Self::with_event_bus(
            character_repo,
            state_repo,
            conversation_repo,
            EventBus::new(),
        )
    }

    /// 使用给定的仓库与事件总线创建一个新的运行时。
    pub fn with_event_bus(
        character_repo: Arc<dyn CharacterRepository>,
        state_repo: Arc<dyn CharacterStateRepository>,
        conversation_repo: Arc<dyn ConversationRepository>,
        event_bus: EventBus,
    ) -> Self {
        Self {
            character_repo,
            state_repo,
            conversation_repo,
            event_bus,
            cache: RwLock::new(HashMap::new()),
            per_character_locks: RwLock::new(HashMap::new()),
        }
    }

    /// 按 ID 加载角色（从缓存或数据库）。
    ///
    /// 加载时若该角色在数据库中还没有持久化状态记录，
    /// 则写入默认状态（保证状态有明确的 source of truth）并发布状态变更事件。
    pub async fn load_character(&self, id: i64) -> Result<Character, RuntimeError> {
        // 先检查缓存（命中即已加载，无需进入串行化流程）
        {
            let cache = self.cache.read().await;
            if let Some(character) = cache.get(&id) {
                return Ok(character.clone());
            }
        }

        // 串行化同一角色的状态初始化，避免并发下重复写入默认状态与重复发布事件。
        let lock = self.character_lock(id).await;
        let _guard = lock.lock().await;
        self.load_character_unlocked(id).await
    }

    /// 在已持有该角色状态锁的前提下加载角色（确保状态已被初始化并写入缓存）。
    async fn load_character_unlocked(&self, id: i64) -> Result<Character, RuntimeError> {
        // 从数据库加载
        let mut character =
            self.character_repo
                .find_by_id(id)
                .await?
                .ok_or(RuntimeError::Domain(
                    crate::error::DomainError::CharacterNotFound(id),
                ))?;

        // 加载状态；若无持久化状态则写入默认值，保证状态存在 source of truth。
        if let Some(state) = self.state_repo.find_by_character_id(id).await? {
            character.state = state;
        } else {
            let default_state = CharacterState::default();
            self.state_repo.upsert(id, &default_state).await?;
            character.state = default_state;
            self.event_bus.publish(&CoreEvent::CharacterStateChanged(
                CharacterStateChangedEvent {
                    character_id: id,
                    timestamp: Utc::now(),
                },
            ));
        }

        // 写入缓存
        {
            let mut cache = self.cache.write().await;
            cache.insert(id, character.clone());
        }

        Ok(character)
    }

    /// 获取（或创建）指定角色的状态转换锁。
    async fn character_lock(&self, id: i64) -> Arc<Mutex<()>> {
        let mut locks = self.per_character_locks.write().await;
        locks
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// 仅从缓存获取角色（不访问数据库）。
    pub async fn get_cached(&self, id: i64) -> Option<Character> {
        let cache = self.cache.read().await;
        cache.get(&id).cloned()
    }

    /// 更新角色状态（持久化到数据库、更新缓存并发布状态变更事件）。
    pub async fn update_state(
        &self,
        character_id: i64,
        state: &CharacterState,
    ) -> Result<(), RuntimeError> {
        // 先持久化到数据库
        self.state_repo.upsert(character_id, state).await?;

        // 再更新缓存
        let mut cache = self.cache.write().await;
        if let Some(character) = cache.get_mut(&character_id) {
            character.state = state.clone();
        }

        // 最后发布事件（保持「持久化先于事件」的生命周期顺序）
        self.event_bus.publish(&CoreEvent::CharacterStateChanged(
            CharacterStateChangedEvent {
                character_id,
                timestamp: Utc::now(),
            },
        ));

        Ok(())
    }

    /// 从行为层的状态补丁（`Action::UpdateState`）更新现有状态。
    ///
    /// 补丁以 JSON 对象给出（例如 `{ "energy": 30, "social_mood": "开心" }`），
    /// 只合并其中出现的字段；数值字段（energy / attention / stress）被 clamp 到 [0, 100]。
    /// 错误时返回 `RuntimeError::Domain(InvalidState)` 或 `RuntimeError::Internal`。
    pub async fn apply_state_patch(
        &self,
        character_id: i64,
        patch: &serde_json::Value,
    ) -> Result<CharacterState, RuntimeError> {
        // 串行化同一角色的状态读-改-写，避免并发补丁互相覆盖（丢失更新）。
        let lock = self.character_lock(character_id).await;
        let _guard = lock.lock().await;

        // 获取当前状态（确保状态已持久化存在）。
        // 这里直接调用不加锁的内部版本，避免对同一角色的锁重复加锁导致死锁。
        let character = self.load_character_unlocked(character_id).await?;

        let mut current = serde_json::to_value(&character.state)
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;

        // 将补丁字段合并进当前状态对象
        if let Some(patch_obj) = patch.as_object() {
            let cur_obj = current
                .as_object_mut()
                .ok_or_else(|| RuntimeError::Internal("状态序列化异常".to_string()))?;
            for (key, value) in patch_obj {
                cur_obj.insert(key.clone(), value.clone());
            }
        }

        let mut new_state: CharacterState = serde_json::from_value(current)
            .map_err(|e| RuntimeError::Domain(DomainError::InvalidState(e.to_string())))?;
        new_state.last_updated = Utc::now();
        let new_state = new_state.clamped();

        // 先持久化，再更新缓存，最后发布事件
        self.state_repo.upsert(character_id, &new_state).await?;
        {
            let mut cache = self.cache.write().await;
            if let Some(c) = cache.get_mut(&character_id) {
                c.state = new_state.clone();
            }
        }
        self.event_bus.publish(&CoreEvent::CharacterStateChanged(
            CharacterStateChangedEvent {
                character_id,
                timestamp: Utc::now(),
            },
        ));

        Ok(new_state)
    }

    /// 将角色从缓存中移除（不会从数据库删除）。
    pub async fn evict(&self, id: i64) {
        let mut cache = self.cache.write().await;
        cache.remove(&id);
    }

    /// 获取缓存中所有已加载角色的 ID。
    pub async fn loaded_ids(&self) -> Vec<i64> {
        let cache = self.cache.read().await;
        cache.keys().copied().collect()
    }

    /// 获取缓存中的角色数量。
    pub async fn cache_size(&self) -> usize {
        let cache = self.cache.read().await;
        cache.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::character::{Character, CharacterDefinition};
    use crate::domain::repository::{
        CharacterRepository, CharacterStateRepository, ConversationRepository,
    };
    use crate::error::RepositoryError;
    use async_trait::async_trait;
    use std::sync::Mutex;

    fn sample_definition() -> CharacterDefinition {
        CharacterDefinition {
            name: "Alice".to_string(),
            description: None,
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
        }
    }

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

    struct MemStateRepo {
        states: Mutex<HashMap<i64, CharacterState>>,
    }

    impl MemStateRepo {
        fn new() -> Self {
            Self {
                states: Mutex::new(HashMap::new()),
            }
        }
        fn persisted_count(&self) -> usize {
            self.states.lock().unwrap().len()
        }
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

    struct MemConvRepo;

    #[async_trait]
    impl ConversationRepository for MemConvRepo {
        async fn find_by_id(
            &self,
            _id: i64,
        ) -> Result<Option<crate::domain::conversation::Conversation>, RepositoryError> {
            Ok(None)
        }
        async fn find_by_external_id(
            &self,
            _id: &str,
        ) -> Result<Option<crate::domain::conversation::Conversation>, RepositoryError> {
            Ok(None)
        }
        async fn find_all(
            &self,
        ) -> Result<Vec<crate::domain::conversation::Conversation>, RepositoryError> {
            Ok(vec![])
        }
        async fn insert(
            &self,
            _c: &crate::domain::conversation::Conversation,
        ) -> Result<i64, RepositoryError> {
            Ok(1)
        }
        async fn update(
            &self,
            _c: &crate::domain::conversation::Conversation,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    async fn setup_runtime() -> (
        CharacterRuntime,
        Arc<MemCharacterRepo>,
        Arc<MemStateRepo>,
        EventBus,
    ) {
        let char_repo = Arc::new(MemCharacterRepo::default());
        let state_repo = Arc::new(MemStateRepo::new());
        let bus = EventBus::new();

        char_repo
            .insert(&Character {
                id: 1,
                definition: sample_definition(),
                state: CharacterState::default(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
            .await
            .unwrap();

        let runtime = CharacterRuntime::with_event_bus(
            char_repo.clone(),
            state_repo.clone(),
            Arc::new(MemConvRepo),
            bus.clone(),
        );
        (runtime, char_repo, state_repo, bus)
    }

    #[tokio::test]
    async fn load_character_persists_default_state_and_publishes_event() {
        let (runtime, _, state_repo, bus) = setup_runtime().await;

        // 加载前没有持久化状态
        assert_eq!(state_repo.persisted_count(), 0);

        let mut sub = bus.subscribe();
        let character = runtime.load_character(1).await.expect("load 应成功");

        // 状态被持久化（source of truth）
        assert_eq!(state_repo.persisted_count(), 1);
        assert_eq!(character.state.energy, CharacterState::default().energy);

        // 发布了状态变更事件
        let event = sub.recv().await.expect("应收到状态变更事件");
        match event {
            CoreEvent::CharacterStateChanged(e) => assert_eq!(e.character_id, 1),
            other => panic!("期望 CharacterStateChanged，实际 {other:?}"),
        }
    }

    #[tokio::test]
    async fn load_character_uses_persisted_state_when_present() {
        let (runtime, _, state_repo, _) = setup_runtime().await;

        // 预先写入持久化状态
        let persisted = CharacterState {
            energy: 20.0,
            ..Default::default()
        };
        state_repo.upsert(1, &persisted).await.unwrap();

        let character = runtime.load_character(1).await.unwrap();
        assert_eq!(character.state.energy, 20.0);
    }

    #[tokio::test]
    async fn apply_state_patch_merges_clamps_and_publishes() {
        let (runtime, _, state_repo, bus) = setup_runtime().await;
        runtime.load_character(1).await.unwrap();

        let mut sub = bus.subscribe();

        // 补丁只改 energy（超出上限会被 clamp）
        let new_state = runtime
            .apply_state_patch(
                1,
                &serde_json::json!({ "energy": 150.0, "social_mood": "开心" }),
            )
            .await
            .expect("patch 应成功");

        assert_eq!(new_state.energy, 100.0);
        assert_eq!(new_state.social_mood.as_deref(), Some("开心"));

        // 持久化后的值与 clamp 后一致
        let loaded = state_repo.find_by_character_id(1).await.unwrap().unwrap();
        assert_eq!(loaded.energy, 100.0);

        // 发布事件
        let event = sub.recv().await.expect("应收到事件");
        match event {
            CoreEvent::CharacterStateChanged(e) => assert_eq!(e.character_id, 1),
            other => panic!("期望 CharacterStateChanged，实际 {other:?}"),
        }
    }

    #[tokio::test]
    async fn update_state_publishes_event() {
        let (runtime, _, _, bus) = setup_runtime().await;

        let mut sub = bus.subscribe();
        runtime
            .update_state(1, &CharacterState::default())
            .await
            .expect("update_state 应成功");

        let event = sub.recv().await.expect("应收到事件");
        match event {
            CoreEvent::CharacterStateChanged(e) => assert_eq!(e.character_id, 1),
            other => panic!("期望 CharacterStateChanged，实际 {other:?}"),
        }
    }

    #[tokio::test]
    async fn concurrent_load_character_initializes_state_once() {
        // 多个并发 load_character 同时命中「无持久化状态」时，
        // 借助 per-character 锁应只初始化一次并恰好发布一次默认状态事件。
        let (runtime, _, _, bus) = setup_runtime().await;
        let runtime = Arc::new(runtime);

        // 订阅在并发加载之前建立，确保能捕获到全部事件。
        let mut sub = bus.subscribe();

        let mut handles = Vec::new();
        for _ in 0..16 {
            let rt = runtime.clone();
            handles.push(tokio::spawn(async move {
                rt.load_character(1).await.expect("load 应成功");
            }));
        }
        for handle in handles {
            handle.await.expect("任务应结束");
        }

        // 只应发布一次默认状态事件。
        let mut seen = 0usize;
        while let Some(event) = sub.try_recv() {
            if matches!(event, CoreEvent::CharacterStateChanged(_)) {
                seen += 1;
            }
        }
        assert_eq!(seen, 1);
    }
}
