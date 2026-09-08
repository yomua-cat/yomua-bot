//! SQLite 模式迁移。
//!
//! 迁移顺序与幂等性说明：
//! - 001 定义全新数据库的完整模式（不含旧的 `emotion_states`，包含新的
//!   `moods` / `conversation_states` / `behavior_states` 三张范围状态表）。
//! - 002/003/004/005 为既有旧表补列，均带列存在性探测，重复执行安全。
//! - 006 仅在旧库（不存在 `moods` 表）上运行：创建新状态表并把旧
//!   `emotion_states` / `character_states.last_proactive_at` 的历史数据
//!   回填到新表，保证升级不丢数据。

use sqlx::SqlitePool;

use crate::error::StorageError;

/// 运行所有待执行的迁移。
pub async fn run_migrations(pool: &SqlitePool) -> Result<(), StorageError> {
    // 迁移 001：初始模式
    sqlx::query(MIGRATION_001)
        .execute(pool)
        .await
        .map_err(|e| StorageError::Migration(format!("migration 001 failed: {e}")))?;

    // 迁移 003：conversation_bindings 新增 switched_at 列（换角色生效时间）。
    let has_switched_at: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('conversation_bindings')
           WHERE name = 'switched_at'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 003 probe failed: {e}")))?;

    if has_switched_at == 0 {
        sqlx::query(MIGRATION_003_ADD_SWITCHED_AT)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Migration(format!("migration 003 failed: {e}")))?;
    }

    // 会话唯一约束（G1）：仅当无重复 conversation_id 时创建唯一索引。
    // 存在脏数据（同一会话多角色）时 warn 并跳过，不自动删除、不崩。
    let duplicates: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM (
            SELECT conversation_id FROM conversation_bindings
            GROUP BY conversation_id HAVING COUNT(*) > 1
        )"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 003 duplicate probe failed: {e}")))?;

    if duplicates == 0 {
        sqlx::query(MIGRATION_003_CONVERSATION_UNIQUE)
            .execute(pool)
            .await
            .map_err(|e| {
                StorageError::Migration(format!("migration 003 unique index failed: {e}"))
            })?;
    } else {
        tracing::warn!(target: "storage", duplicates, "检测到同一会话存在多个角色绑定（脏数据），跳过会话唯一索引创建；行为层将取第一个绑定");
    }

    // 迁移 004：为 memories 表新增 `embedding` 列，并创建 semantic_memories 表。
    let has_embedding: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('memories')
           WHERE name = 'embedding'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 004 probe failed: {e}")))?;

    if has_embedding == 0 {
        sqlx::query(MIGRATION_004_ADD_EMBEDDING)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Migration(format!("migration 004 failed: {e}")))?;
    }

    // semantic_memories 表（使用 CREATE TABLE IF NOT EXISTS 保证幂等）。
    sqlx::query(MIGRATION_004_SEMANTIC_MEMORIES)
        .execute(pool)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("migration 004 semantic_memories failed: {e}"))
        })?;

    // 迁移 005：conversation_bindings 新增 cross_reply_enabled 列（群聊多 Bot 场景）。
    let has_cross_reply_enabled: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('conversation_bindings')
           WHERE name = 'cross_reply_enabled'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 005 probe failed: {e}")))?;

    if has_cross_reply_enabled == 0 {
        sqlx::query(MIGRATION_005_CROSS_REPLY_ENABLED)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Migration(format!("migration 005 failed: {e}")))?;
    }

    // 迁移 006：双范围状态表（Mood / ConversationState / BehaviorState）。
    // 全新库由 MIGRATION_001 直接建好三张表；旧库（残留 `emotion_states` 表，
    // 新 001 不会创建它）在此补齐新表，并把历史 `emotion_states` 与
    // `character_states.last_proactive_at` 回填到新表中。
    let has_emotion_states: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='emotion_states'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 006 probe failed: {e}")))?;

    if has_emotion_states > 0 {
        sqlx::query(MIGRATION_006_CREATE_SCOPED_TABLES)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Migration(format!("migration 006 create failed: {e}")))?;
        tracing::info!(target: "storage", "迁移 006：已补齐 moods / conversation_states / behavior_states 表");

        // 旧 emotion_states 有历史数据 → 回填为新表初值。
        let moods_empty: i64 = sqlx::query_scalar(r#"SELECT COUNT(*) FROM moods"#)
            .fetch_one(pool)
            .await
            .map_err(|e| StorageError::Migration(format!("migration 006 count failed: {e}")))?;

        if moods_empty == 0 {
            // 旧表是否已带 conversation_id（此前已做过一次范围化迁移）？
            let scoped: i64 = sqlx::query_scalar(
                r#"SELECT COUNT(*) FROM pragma_table_info('emotion_states')
                   WHERE name = 'conversation_id'"#,
            )
            .fetch_one(pool)
            .await
            .map_err(|e| StorageError::Migration(format!("migration 006 probe failed: {e}")))?;

            let (mood_sql, conv_sql) = if scoped > 0 {
                (
                    MIGRATION_006_BACKFILL_MOOD_SCOPED,
                    MIGRATION_006_BACKFILL_CONV_STATE_SCOPED,
                )
            } else {
                (
                    MIGRATION_006_BACKFILL_MOOD_LEGACY,
                    MIGRATION_006_BACKFILL_CONV_STATE_LEGACY,
                )
            };
            sqlx::query(mood_sql).execute(pool).await.map_err(|e| {
                StorageError::Migration(format!("migration 006 mood backfill failed: {e}"))
            })?;
            sqlx::query(conv_sql).execute(pool).await.map_err(|e| {
                StorageError::Migration(format!("migration 006 conv backfill failed: {e}"))
            })?;
            tracing::info!(target: "storage", "迁移 006：已从旧 emotion_states 回填 mood / conversation_states");
        }

        // 旧 character_states.last_proactive_at 列存在 → 回填行为状态（保持冷却连续）。
        let has_last_proactive: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM pragma_table_info('character_states')
               WHERE name = 'last_proactive_at'"#,
        )
        .fetch_one(pool)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("migration 006 proactive probe failed: {e}"))
        })?;

        if has_last_proactive > 0 {
            sqlx::query(MIGRATION_006_BACKFILL_BEHAVIOR_STATE)
                .execute(pool)
                .await
                .map_err(|e| {
                    StorageError::Migration(format!("migration 006 behavior backfill failed: {e}"))
                })?;
        }
    }

    // 迁移 007：messages 表新增 active_character_id 列，
    // 用于记录该消息是哪个角色在 Active 时观察到的（用于可见性过滤）。
    // 旧消息此列为 NULL，过滤时退化为 switched_at 逻辑。
    let has_active_character_id: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('messages')
           WHERE name = 'active_character_id'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 007 probe failed: {e}")))?;

    if has_active_character_id == 0 {
        sqlx::query(
            r#"ALTER TABLE messages ADD COLUMN active_character_id INTEGER REFERENCES characters(id)"#,
        )
        .execute(pool)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("migration 007 failed: {e}"))
        })?;
    }

    // 迁移 008：messages 表 dedup key 的数据库层 UNIQUE 约束。
    // 应用层 dedup (conversation_id, sender_id, timestamp, content) 在并发情况下
    // 可能两侧都判断"不存在"并各自插入；加唯一约束确保不会出现重复行。
    // 已有重复数据时跳过（warn），不自动删除。
    let idx_exists: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM sqlite_master
           WHERE type = 'index' AND name = 'idx_messages_dedup'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 008 probe failed: {e}")))?;

    if idx_exists == 0 {
        // 先检查是否有重复（conversation_id, sender_id, timestamp, content 都相同）。
        let dup_count: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM (
                SELECT conversation_id, sender_id, timestamp, content,
                       COUNT(*) as cnt
                  FROM messages
              GROUP BY conversation_id, sender_id, timestamp, content
                HAVING cnt > 1
            )"#,
        )
        .fetch_one(pool)
        .await
        .map_err(|e| StorageError::Migration(format!("migration 008 dup probe failed: {e}")))?;

        if dup_count > 0 {
            tracing::warn!(
                target: "storage",
                "messages 表存在 {} 组重复记录，跳过 dedup UNIQUE 索引创建",
                dup_count
            );
        } else {
            sqlx::query(
                r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_dedup
                      ON messages (conversation_id, sender_id, timestamp, content)"#,
            )
            .execute(pool)
            .await
            .map_err(|e| StorageError::Migration(format!("migration 008 failed: {e}")))?;
        }
    }

    Ok(())
}

const MIGRATION_001: &str = r#"
-- Characters
CREATE TABLE IF NOT EXISTS characters (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    description     TEXT,
    personality     TEXT,
    scenario        TEXT,
    style           TEXT,
    background      TEXT,
    greetings       TEXT NOT NULL DEFAULT '[]',    -- JSON array
    example_messages TEXT NOT NULL DEFAULT '[]',   -- JSON array
    system_prompt   TEXT,
    post_history_instructions TEXT,
    lorebook        TEXT NOT NULL DEFAULT '[]',    -- JSON array
    metadata        TEXT NOT NULL DEFAULT '{}',    -- JSON object
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Character runtime state（全局范围：energy / stress / current_activity）
CREATE TABLE IF NOT EXISTS character_states (
    character_id    INTEGER PRIMARY KEY REFERENCES characters(id) ON DELETE CASCADE,
    energy          REAL NOT NULL DEFAULT 72.0,
    stress          REAL NOT NULL DEFAULT 10.0,
    current_activity TEXT,
    last_updated    TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Conversations
CREATE TABLE IF NOT EXISTS conversations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_type TEXT NOT NULL CHECK (conversation_type IN ('private', 'group')),
    external_id     TEXT NOT NULL,
    name            TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_external_id
    ON conversations(external_id);

-- Character ↔ Conversation bindings
CREATE TABLE IF NOT EXISTS conversation_bindings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    character_id    INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    reply_mode      TEXT NOT NULL DEFAULT 'mention_only'
                    CHECK (reply_mode IN ('mention_only', 'occasional', 'natural')),
    proactive_enabled INTEGER NOT NULL DEFAULT 0,
    mute_schedule   TEXT,
    behavior_overrides TEXT NOT NULL DEFAULT '{}',
    context_policy  TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_conversation_bindings_unique
    ON conversation_bindings(character_id, conversation_id);

-- Participants (users or characters in a conversation)
CREATE TABLE IF NOT EXISTS participants (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    external_id     TEXT NOT NULL,
    display_name    TEXT NOT NULL,
    role            TEXT NOT NULL CHECK (role IN ('user', 'character', 'system')),
    metadata        TEXT NOT NULL DEFAULT '{}',
    UNIQUE (conversation_id, external_id)
);

-- Messages
CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    sender_id       INTEGER NOT NULL REFERENCES participants(id),
    content         TEXT NOT NULL,                  -- JSON MessageContent
    timestamp       TEXT NOT NULL DEFAULT (datetime('now')),
    reply_to        INTEGER REFERENCES messages(id),
    mentions        TEXT NOT NULL DEFAULT '[]',     -- JSON array of participant IDs
    attachments     TEXT NOT NULL DEFAULT '[]',     -- JSON array
    metadata        TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_messages_conversation
    ON messages(conversation_id, timestamp);

-- Persistent memories
CREATE TABLE IF NOT EXISTS memories (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    character_id    INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    conversation_id INTEGER REFERENCES conversations(id) ON DELETE SET NULL,
    memory_type     TEXT NOT NULL CHECK (memory_type IN ('episodic', 'semantic', 'relationship', 'system')),
    content         TEXT NOT NULL,
    importance      REAL NOT NULL DEFAULT 0.5,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    last_accessed   TEXT NOT NULL DEFAULT (datetime('now')),
    metadata        TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_memories_character
    ON memories(character_id, memory_type);

-- Relationships (Character × Participant)
CREATE TABLE IF NOT EXISTS relationships (
    character_id    INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    participant_id  INTEGER NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
    familiarity     REAL NOT NULL DEFAULT 0.0,
    affection       REAL NOT NULL DEFAULT 0.2,
    trust           REAL NOT NULL DEFAULT 0.1,
    respect         REAL NOT NULL DEFAULT 0.2,
    annoyance       REAL NOT NULL DEFAULT 0.0,
    intimacy        REAL NOT NULL DEFAULT 0.0,
    interaction_count INTEGER NOT NULL DEFAULT 0,
    last_interaction TEXT NOT NULL DEFAULT (datetime('now')),
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (character_id, participant_id)
);

-- Mood（Character × Conversation 标量心情，0-100）
CREATE TABLE IF NOT EXISTS moods (
    character_id    INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    value           REAL NOT NULL DEFAULT 50.0,
    last_updated    TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (character_id, conversation_id)
);

-- ConversationState（Character × Conversation 的精力 / 压力）
CREATE TABLE IF NOT EXISTS conversation_states (
    character_id    INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    energy          REAL NOT NULL DEFAULT 50.0,
    stress          REAL NOT NULL DEFAULT 10.0,
    last_updated    TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (character_id, conversation_id)
);

-- BehaviorState（Character × Conversation 的主动行为冷却状态）
CREATE TABLE IF NOT EXISTS behavior_states (
    character_id    INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    last_proactive_at TEXT,
    last_updated    TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (character_id, conversation_id)
);

-- Scheduled tasks
CREATE TABLE IF NOT EXISTS schedules (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    character_id    INTEGER REFERENCES characters(id) ON DELETE CASCADE,
    task_type       TEXT NOT NULL,
    payload         TEXT NOT NULL DEFAULT '{}',
    run_at          TEXT NOT NULL,
    recurring       INTEGER NOT NULL DEFAULT 0,
    interval_secs   INTEGER,
    enabled         INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_schedules_run_at
    ON schedules(run_at) WHERE enabled = 1;

-- Event log (for debug, recovery, audit)
CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type      TEXT NOT NULL,
    payload         TEXT NOT NULL,                  -- JSON event data
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_events_type
    ON events(event_type, created_at);

-- Plugin persistent data
CREATE TABLE IF NOT EXISTS plugin_data (
    plugin_name     TEXT NOT NULL,
    key             TEXT NOT NULL,
    value           TEXT NOT NULL,                  -- JSON
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (plugin_name, key)
);
"#;

/// 迁移 003：为 conversation_bindings 新增 `switched_at` 列（换角色生效时间）。
const MIGRATION_003_ADD_SWITCHED_AT: &str = r#"
ALTER TABLE conversation_bindings ADD COLUMN switched_at TEXT;
"#;

/// 迁移 003b：会话唯一索引——一个会话最多一个角色绑定（G1 强制单绑定）。
const MIGRATION_003_CONVERSATION_UNIQUE: &str = r#"
CREATE UNIQUE INDEX IF NOT EXISTS idx_conversation_bindings_conversation_unique
    ON conversation_bindings(conversation_id);
"#;

/// 迁移 004：为 memories 表新增 `embedding` 列（可空 TEXT，存储 JSON 数组）。
const MIGRATION_004_ADD_EMBEDDING: &str = r#"
ALTER TABLE memories ADD COLUMN embedding TEXT;
"#;

/// 迁移 004：语义记忆表（独立的 embedding 存储，不依赖外部向量数据库）。
const MIGRATION_004_SEMANTIC_MEMORIES: &str = r#"
CREATE TABLE IF NOT EXISTS semantic_memories (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    character_id    INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    conversation_id INTEGER REFERENCES conversations(id) ON DELETE SET NULL,
    memory_type     TEXT NOT NULL CHECK (memory_type IN ('semantic', 'relationship', 'system')),
    content         TEXT NOT NULL,
    embedding       TEXT NOT NULL,
    importance      REAL NOT NULL DEFAULT 0.5,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    last_accessed   TEXT NOT NULL DEFAULT (datetime('now')),
    metadata        TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_semantic_memories_character
    ON semantic_memories(character_id, memory_type);
"#;

/// 迁移 005：conversation_bindings 新增 cross_reply_enabled 列（群聊多 Bot 场景）。
const MIGRATION_005_CROSS_REPLY_ENABLED: &str = r#"
ALTER TABLE conversation_bindings ADD COLUMN cross_reply_enabled INTEGER NOT NULL DEFAULT 0;
"#;

/// 迁移 006a：为旧库补齐三张范围状态表（全新库由 001 直接创建）。
const MIGRATION_006_CREATE_SCOPED_TABLES: &str = r#"
CREATE TABLE IF NOT EXISTS moods (
    character_id    INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    value           REAL NOT NULL DEFAULT 50.0,
    last_updated    TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (character_id, conversation_id)
);

CREATE TABLE IF NOT EXISTS conversation_states (
    character_id    INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    energy          REAL NOT NULL DEFAULT 50.0,
    stress          REAL NOT NULL DEFAULT 10.0,
    last_updated    TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (character_id, conversation_id)
);

CREATE TABLE IF NOT EXISTS behavior_states (
    character_id    INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    last_proactive_at TEXT,
    last_updated    TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (character_id, conversation_id)
);
"#;

/// 迁移 006b：从旧 `emotion_states`（已带 conversation_id 的范围表）回填 `moods`。
///
/// 旧情绪为多维（0-1），新 Mood 为 0-100 标量：以 happiness 作为初始心情值（×100），
/// 与旧默认值（happiness=0.5 → 50）保持一致。
const MIGRATION_006_BACKFILL_MOOD_SCOPED: &str = r#"
INSERT OR IGNORE INTO moods (character_id, conversation_id, value, last_updated)
SELECT e.character_id,
       e.conversation_id,
       ROUND(MIN(100.0, MAX(0.0, e.happiness * 100.0)), 2),
       e.last_updated
FROM emotion_states e;
"#;

/// 迁移 006b（legacy）：从旧 `emotion_states`（无 conversation_id，全局情绪）回填 `moods`，
/// 把每个角色的历史情绪复制到其绑定的每个会话（与原「全局情绪」语义一致）。
const MIGRATION_006_BACKFILL_MOOD_LEGACY: &str = r#"
INSERT OR IGNORE INTO moods (character_id, conversation_id, value, last_updated)
SELECT DISTINCT
    e.character_id,
    cb.conversation_id,
    ROUND(MIN(100.0, MAX(0.0, e.happiness * 100.0)), 2),
    e.last_updated
FROM emotion_states e
JOIN conversation_bindings cb ON cb.character_id = e.character_id;
"#;

/// 迁移 006b：从旧 `emotion_states`（已带 conversation_id 的范围表）回填 `conversation_states`。
const MIGRATION_006_BACKFILL_CONV_STATE_SCOPED: &str = r#"
INSERT OR IGNORE INTO conversation_states (character_id, conversation_id, energy, stress, last_updated)
SELECT DISTINCT
    e.character_id,
    e.conversation_id,
    ROUND(MIN(100.0, MAX(0.0, e.energy * 100.0)), 2),
    ROUND(MIN(100.0, MAX(0.0, e.stress * 100.0)), 2),
    e.last_updated
FROM emotion_states e;
"#;

/// 迁移 006b（legacy）：从旧 `emotion_states`（无 conversation_id，全局情绪）回填
/// `conversation_states`，按角色绑定分发到每个会话。
const MIGRATION_006_BACKFILL_CONV_STATE_LEGACY: &str = r#"
INSERT OR IGNORE INTO conversation_states (character_id, conversation_id, energy, stress, last_updated)
SELECT DISTINCT
    e.character_id,
    cb.conversation_id,
    ROUND(MIN(100.0, MAX(0.0, e.energy * 100.0)), 2),
    ROUND(MIN(100.0, MAX(0.0, e.stress * 100.0)), 2),
    e.last_updated
FROM emotion_states e
JOIN conversation_bindings cb ON cb.character_id = e.character_id;
"#;

/// 迁移 006c：把旧 `character_states.last_proactive_at` 回填到 `behavior_states`
/// （按角色绑定的每个会话复制），保证主动冷却在升级后连续。
const MIGRATION_006_BACKFILL_BEHAVIOR_STATE: &str = r#"
INSERT OR IGNORE INTO behavior_states (character_id, conversation_id, last_proactive_at, last_updated)
SELECT DISTINCT
    cs.character_id,
    cb.conversation_id,
    cs.last_proactive_at,
    cs.last_updated
FROM character_states cs
JOIN conversation_bindings cb ON cb.character_id = cs.character_id
WHERE cs.last_proactive_at IS NOT NULL;
"#;
