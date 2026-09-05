//! SQLite 模式迁移。

use sqlx::SqlitePool;

use crate::error::StorageError;

/// 运行所有待执行的迁移。
pub async fn run_migrations(pool: &SqlitePool) -> Result<(), StorageError> {
    // 迁移 001：初始模式
    sqlx::query(MIGRATION_001)
        .execute(pool)
        .await
        .map_err(|e| StorageError::Migration(format!("migration 001 failed: {e}")))?;

    // 迁移 002：为既有数据库补上 character_states.last_proactive_at 列。
    // MIGRATION_001 已把该列写入新建表的定义，因此这里只在列缺失时执行，保证幂等。
    let has_last_proactive_at: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('character_states')
           WHERE name = 'last_proactive_at'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 002 probe failed: {e}")))?;

    if has_last_proactive_at == 0 {
        sqlx::query(MIGRATION_002)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Migration(format!("migration 002 failed: {e}")))?;
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

-- Character runtime state
CREATE TABLE IF NOT EXISTS character_states (
    character_id    INTEGER PRIMARY KEY REFERENCES characters(id) ON DELETE CASCADE,
    energy          REAL NOT NULL DEFAULT 72.0,
    attention       REAL NOT NULL DEFAULT 50.0,
    current_activity TEXT,
    social_mood     TEXT DEFAULT 'calm',
    stress          REAL NOT NULL DEFAULT 10.0,
    last_proactive_at TEXT,
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

-- Emotion states (Character's persistent emotional state)
CREATE TABLE IF NOT EXISTS emotion_states (
    character_id    INTEGER PRIMARY KEY REFERENCES characters(id) ON DELETE CASCADE,
    happiness       REAL NOT NULL DEFAULT 0.5,
    anger           REAL NOT NULL DEFAULT 0.0,
    sadness         REAL NOT NULL DEFAULT 0.0,
    fear            REAL NOT NULL DEFAULT 0.0,
    affection       REAL NOT NULL DEFAULT 0.3,
    stress          REAL NOT NULL DEFAULT 0.1,
    energy          REAL NOT NULL DEFAULT 0.7,
    last_updated    TEXT NOT NULL DEFAULT (datetime('now'))
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

/// 迁移 002：为既有数据库的 character_states 表新增 `last_proactive_at` 列。
///
/// 用于记录主动行为最后一次触发的时间，供 proactive cooldown 判断。
const MIGRATION_002: &str = r#"
ALTER TABLE character_states ADD COLUMN last_proactive_at TEXT;
"#;
