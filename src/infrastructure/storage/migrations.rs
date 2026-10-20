//! SQLite 模式迁移。
//!
//! 迁移顺序与幂等性说明：
//! - 001 定义全新数据库的完整模式（不含旧的 `emotion_states`，包含新的
//!   `moods` / `conversation_states` / `behavior_states` 三张范围状态表）。
//! - 002/003/004/005 为既有旧表补列，均带列存在性探测，重复执行安全。
//! - 006 创建新状态表并按行补齐旧 `emotion_states` /
//!   `character_states.last_proactive_at` 的历史数据，允许中断后继续执行。
//! - 016/017 建立全域用户身份并将关系、交互、记忆归属迁移到该身份。

use sqlx::SqlitePool;

use crate::error::StorageError;

/// 运行所有待执行的迁移。
pub async fn run_migrations(pool: &SqlitePool) -> Result<(), StorageError> {
    // 旧库可能已经存在 conversations 表，但还没有当前版本的唯一索引。
    // 在执行初始 schema（其中也会尝试创建索引）前先拒绝重复身份，避免
    // 迁移在错误的阶段失败并留下难以诊断的半迁移状态。
    let has_conversations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='conversations'",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("conversation table probe failed: {e}")))?;
    if has_conversations > 0 {
        let duplicate_count: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM (
                   SELECT external_id, conversation_type
                     FROM conversations
                    GROUP BY external_id, conversation_type
                   HAVING COUNT(*) > 1
               )"#,
        )
        .fetch_one(pool)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("conversation identity preflight failed: {e}"))
        })?;
        if duplicate_count > 0 {
            return Err(StorageError::Migration(format!(
                "检测到 {duplicate_count} 组重复会话身份 (external_id, conversation_type)，拒绝迁移"
            )));
        }
    }

    // 迁移 001：初始模式
    sqlx::query(MIGRATION_001)
        .execute(pool)
        .await
        .map_err(|e| StorageError::Migration(format!("migration 001 failed: {e}")))?;

    // 会话外部 ID 按平台类型分命名空间：同一个数字可以同时是私聊用户号和群号。
    // 先检查新键下的重复数据，再在事务中替换旧索引；任何失败都保留原约束。
    let identity_duplicates: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM (
               SELECT external_id, conversation_type
                 FROM conversations
                GROUP BY external_id, conversation_type
               HAVING COUNT(*) > 1
           )"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("conversation identity probe failed: {e}")))?;
    if identity_duplicates > 0 {
        return Err(StorageError::Migration(format!(
            "检测到 {identity_duplicates} 组重复会话身份 (external_id, conversation_type)，拒绝替换唯一索引"
        )));
    }
    let mut identity_tx = pool.begin().await.map_err(|e| {
        StorageError::Migration(format!(
            "conversation identity transaction begin failed: {e}"
        ))
    })?;
    sqlx::query("DROP INDEX IF EXISTS idx_conversations_external_id")
        .execute(&mut *identity_tx)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("conversation identity migration failed: {e}"))
        })?;
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_external_type ON conversations(external_id, conversation_type)",
    )
    .execute(&mut *identity_tx)
    .await
    .map_err(|e| StorageError::Migration(format!("conversation identity index failed: {e}")))?;
    identity_tx.commit().await.map_err(|e| {
        StorageError::Migration(format!(
            "conversation identity transaction commit failed: {e}"
        ))
    })?;

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
    // 多绑定意味着运行时无法确定当前角色，迁移必须拒绝启动而不是继续运行在
    // 一个没有数据库约束保护的状态。
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
        return Err(StorageError::Migration(format!(
            "检测到 {duplicates} 个会话存在多个角色绑定；请先明确保留的角色绑定，再启动服务"
        )));
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
        // 结构探测必须在开启事务前完成。测试和单实例部署的连接池可能只有
        // 一个连接，事务占用它后再从 pool 查询会永久等待。
        let scoped: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM pragma_table_info('emotion_states')
               WHERE name = 'conversation_id'"#,
        )
        .fetch_one(pool)
        .await
        .map_err(|e| StorageError::Migration(format!("migration 006 probe failed: {e}")))?;
        let has_last_proactive: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM pragma_table_info('character_states')
               WHERE name = 'last_proactive_at'"#,
        )
        .fetch_one(pool)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("migration 006 proactive probe failed: {e}"))
        })?;

        // 旧状态表的创建和三组回填必须作为一个整体提交。否则进程在中途
        // 失败后，部分新表/数据可能已经落盘，下一次启动无法区分哪些行
        // 已经可靠迁移，最终留下半迁移状态。
        let mut migration_006_tx = pool.begin().await.map_err(|e| {
            StorageError::Migration(format!("migration 006 transaction begin failed: {e}"))
        })?;
        sqlx::query(MIGRATION_006_CREATE_SCOPED_TABLES)
            .execute(&mut *migration_006_tx)
            .await
            .map_err(|e| StorageError::Migration(format!("migration 006 create failed: {e}")))?;
        tracing::info!(target: "storage", "迁移 006：已补齐 moods / conversation_states / behavior_states 表");

        // 每次启动都逐行补齐缺失回填。INSERT OR IGNORE 让已迁移行保持不变，
        // 同时允许上次迁移中断后补完剩余行。
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
        sqlx::query(mood_sql)
            .execute(&mut *migration_006_tx)
            .await
            .map_err(|e| {
                StorageError::Migration(format!("migration 006 mood backfill failed: {e}"))
            })?;
        sqlx::query(conv_sql)
            .execute(&mut *migration_006_tx)
            .await
            .map_err(|e| {
                StorageError::Migration(format!("migration 006 conv backfill failed: {e}"))
            })?;
        tracing::info!(target: "storage", "迁移 006：已从旧 emotion_states 补齐 mood / conversation_states");

        // 旧 character_states.last_proactive_at 列存在 → 回填行为状态（保持冷却连续）。
        if has_last_proactive > 0 {
            sqlx::query(MIGRATION_006_BACKFILL_BEHAVIOR_STATE)
                .execute(&mut *migration_006_tx)
                .await
                .map_err(|e| {
                    StorageError::Migration(format!("migration 006 behavior backfill failed: {e}"))
                })?;
        }
        migration_006_tx.commit().await.map_err(|e| {
            StorageError::Migration(format!("migration 006 transaction commit failed: {e}"))
        })?;
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

    // 迁移 008：旧版本曾尝试用“会话 + 作者 + 时间 + 文本”去重。
    // 该键会把用户再次发送相同内容误判为重复；当前入站幂等以平台消息身份
    // 为准，因此只清理旧索引，避免把错误的内容键继续带入新库。
    sqlx::query("DROP INDEX IF EXISTS idx_messages_dedup")
        .execute(pool)
        .await
        .map_err(|e| StorageError::Migration(format!("migration 008 cleanup failed: {e}")))?;

    // 迁移 009：C2-4 Durable Event Delivery Outbox
    // 存储所有需要可靠传递的核心事件。
    let has_outbox_table: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='outbox_events'",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 009 table probe failed: {e}")))?;
    let has_outbox: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('outbox_events')
           WHERE name = 'event_uuid'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 009 probe failed: {e}")))?;

    if has_outbox_table > 0 && has_outbox == 0 {
        return Err(StorageError::Migration(
            "迁移 009 发现已有 outbox_events 表但缺少 event_uuid 列，拒绝猜测其结构".to_string(),
        ));
    }

    if has_outbox == 0 {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS outbox_events (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                event_uuid      TEXT NOT NULL UNIQUE,
                entity_id       INTEGER NOT NULL,
                entity_type     TEXT NOT NULL,
                event_type      TEXT NOT NULL,
                event_version   INTEGER NOT NULL DEFAULT 1,
                payload         TEXT NOT NULL,
                ordering_key    TEXT NOT NULL,
                ordering_seq    INTEGER NOT NULL,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                available_at    TEXT NOT NULL DEFAULT (datetime('now')),
                status          TEXT NOT NULL DEFAULT 'pending',
                attempts        INTEGER NOT NULL DEFAULT 0,
                last_error      TEXT,
                locked_until    TEXT,
                processed_at    TEXT
            )"#,
        )
        .execute(pool)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("migration 009 outbox_events table failed: {e}"))
        })?;
    }

    for (name, ddl) in [
        (
            "idx_outbox_status_available",
            "CREATE INDEX IF NOT EXISTS idx_outbox_status_available ON outbox_events(status, available_at)",
        ),
        (
            "idx_outbox_ordering",
            "CREATE INDEX IF NOT EXISTS idx_outbox_ordering ON outbox_events(ordering_key, ordering_seq)",
        ),
    ] {
        sqlx::query(ddl).execute(pool).await.map_err(|e| {
            StorageError::Migration(format!("migration 009 {name} failed: {e}"))
        })?;
    }

    // 迁移 010：C2-4 Lease Ownership Token
    // 为 outbox_events 添加 lease_token 列，实现真正的 worker ownership fencing。
    let has_lease_token: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('outbox_events')
           WHERE name = 'lease_token'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 010 probe failed: {e}")))?;

    if has_lease_token == 0 {
        sqlx::query(r#"ALTER TABLE outbox_events ADD COLUMN lease_token TEXT"#)
            .execute(pool)
            .await
            .map_err(|e| {
                StorageError::Migration(format!("migration 010 lease_token failed: {e}"))
            })?;
    }

    // 迁移 011：characters 表新增 enabled 列 + idx_messages_attribution 索引。
    let has_enabled: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('characters')
           WHERE name = 'enabled'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 011 probe failed: {e}")))?;

    if has_enabled == 0 {
        sqlx::query(r#"ALTER TABLE characters ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1"#)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Migration(format!("migration 011 enabled failed: {e}")))?;
    }

    // idx_messages_attribution 索引：用于消息归因查询（conversation_id, active_character_id, timestamp, id）
    let has_attribution_idx: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM sqlite_master
           WHERE type = 'index' AND name = 'idx_messages_attribution'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 011 idx probe failed: {e}")))?;

    if has_attribution_idx == 0 {
        sqlx::query(
            r#"CREATE INDEX idx_messages_attribution
               ON messages(conversation_id, active_character_id, timestamp, id)"#,
        )
        .execute(pool)
        .await
        .map_err(|e| {
            StorageError::Migration(format!(
                "migration 011 idx_messages_attribution failed: {e}"
            ))
        })?;
    }

    // 迁移 012：messages 表新增 C2-3 Temporal Architecture 字段。
    // 添加 platform_message_id、conversation_sequence、occurred_at、ingested_at 列。
    let has_platform_message_id: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('messages')
           WHERE name = 'platform_message_id'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 012 probe failed: {e}")))?;

    if has_platform_message_id == 0 {
        sqlx::query(r#"ALTER TABLE messages ADD COLUMN platform_message_id INTEGER"#)
            .execute(pool)
            .await
            .map_err(|e| {
                StorageError::Migration(format!("migration 012 platform_message_id failed: {e}"))
            })?;
    }

    let has_conversation_sequence: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('messages')
           WHERE name = 'conversation_sequence'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 012 probe 2 failed: {e}")))?;

    if has_conversation_sequence == 0 {
        sqlx::query(r#"ALTER TABLE messages ADD COLUMN conversation_sequence INTEGER"#)
            .execute(pool)
            .await
            .map_err(|e| {
                StorageError::Migration(format!("migration 012 conversation_sequence failed: {e}"))
            })?;
    }

    let has_occurred_at: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('messages')
           WHERE name = 'occurred_at'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 012 probe 3 failed: {e}")))?;

    if has_occurred_at == 0 {
        sqlx::query(r#"ALTER TABLE messages ADD COLUMN occurred_at TEXT"#)
            .execute(pool)
            .await
            .map_err(|e| {
                StorageError::Migration(format!("migration 012 occurred_at failed: {e}"))
            })?;
    }

    let has_ingested_at: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('messages')
           WHERE name = 'ingested_at'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 012 probe 4 failed: {e}")))?;

    if has_ingested_at == 0 {
        sqlx::query(r#"ALTER TABLE messages ADD COLUMN ingested_at TEXT"#)
            .execute(pool)
            .await
            .map_err(|e| {
                StorageError::Migration(format!("migration 012 ingested_at failed: {e}"))
            })?;
    }

    // 迁移 013：C2-4 Message Sequence Counter
    // conversations 表新增 sequence_counter 列，用于 C2-4 Temporal Architecture 的消息定序。
    // accept_message() 和 allocate_sequence() 依赖此列。
    let has_sequence_counter: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM pragma_table_info('conversations')
           WHERE name = 'sequence_counter'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 013 probe failed: {e}")))?;

    if has_sequence_counter == 0 {
        sqlx::query(
            r#"ALTER TABLE conversations ADD COLUMN sequence_counter INTEGER NOT NULL DEFAULT 0"#,
        )
        .execute(pool)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("migration 013 sequence_counter failed: {e}"))
        })?;
    }

    // 迁移 014：blacklist_entries 表（拉黑用户/群）。
    // 用于控制哪些用户/群可以与 Bot 交互。
    // schema 与 SqliteBlacklistRepository 的 SQL 完全对齐。
    let has_blacklist: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM sqlite_master
           WHERE type='table' AND name='blacklist_entries'"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 014 probe failed: {e}")))?;

    if has_blacklist == 0 {
        sqlx::query(
            r#"CREATE TABLE blacklist_entries (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                target          TEXT NOT NULL,
                target_type     TEXT NOT NULL,
                reason          TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                expires_at      TEXT
            )"#,
        )
        .execute(pool)
        .await
        .map_err(|e| StorageError::Migration(format!("migration 014 table failed: {e}")))?;
    }
    // 对已有表同样核验唯一约束。若旧数据重复，CREATE 会明确失败而不会让
    // 应用静默进入允许重复封禁记录的状态。
    sqlx::query(
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_blacklist_target_type
           ON blacklist_entries(target, target_type)"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 014 index failed: {e}")))?;

    sqlx::query(r#"
      CREATE INDEX IF NOT EXISTS idx_messages_platform ON messages(conversation_id, platform_message_id);
      CREATE TABLE IF NOT EXISTS command_receipts (
        conversation_id INTEGER NOT NULL,
        platform_message_id INTEGER NOT NULL,
        received_at TEXT NOT NULL,
        PRIMARY KEY (conversation_id, platform_message_id)
      );
      CREATE TABLE IF NOT EXISTS platform_message_echoes (
        conversation_id INTEGER NOT NULL,
        platform_message_id INTEGER NOT NULL,
        sender_external_id TEXT NOT NULL,
        content TEXT NOT NULL,
        metadata TEXT NOT NULL,
        received_at TEXT NOT NULL,
        PRIMARY KEY (conversation_id, platform_message_id)
      );
      CREATE TABLE IF NOT EXISTS reply_tasks (
        message_id INTEGER PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
        character_id INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
        content TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        error TEXT,
        platform_message_id INTEGER,
        updated_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS cognition_progress (
        character_id INTEGER NOT NULL, conversation_id INTEGER NOT NULL,
        last_message_id INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(character_id,conversation_id)
      );
    "#).execute(pool).await.map_err(|e|StorageError::Migration(e.to_string()))?;

    let has_reply_platform_id: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('reply_tasks') WHERE name='platform_message_id'",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("reply task delivery probe failed: {e}")))?;
    if has_reply_platform_id == 0 {
        sqlx::query("ALTER TABLE reply_tasks ADD COLUMN platform_message_id INTEGER")
            .execute(pool)
            .await
            .map_err(|e| {
                StorageError::Migration(format!("reply task delivery migration failed: {e}"))
            })?;
    }

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS relationship_interactions (
            character_id INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
            participant_id INTEGER NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
            source_message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (character_id, participant_id, source_message_id)
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 014 interaction table failed: {e}")))?;

    // 稳定用户身份过渡：历史参与者以会话内 participant_id 建立，但平台外部 ID
    // 才是跨会话稳定身份。把现有 user participant 的 external_id 回填到 metadata，
    // 供关系查询按稳定键归并；不覆盖已有显式键。
    sqlx::query(
        r#"UPDATE participants
              SET metadata = json_set(CASE
                    WHEN json_valid(metadata) THEN metadata ELSE '{}'
                  END, '$.stable_user_key', external_id)
            WHERE role = 'user'
              AND CASE WHEN json_valid(metadata)
                       THEN json_extract(metadata, '$.stable_user_key') END IS NULL"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("stable user metadata backfill failed: {e}")))?;

    // 迁移 015：持久化每个会话的主动发言日额度，避免进程重启绕过每日上限。
    for (column, definition) in [
        ("proactive_count_date", "TEXT"),
        ("proactive_count", "INTEGER NOT NULL DEFAULT 0"),
    ] {
        let exists: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM pragma_table_info('behavior_states') WHERE name = '{column}'"
        ))
        .fetch_one(pool)
        .await
        .map_err(|e| StorageError::Migration(format!("migration 015 probe failed: {e}")))?;
        if exists == 0 {
            sqlx::query(&format!(
                "ALTER TABLE behavior_states ADD COLUMN {column} {definition}"
            ))
            .execute(pool)
            .await
            .map_err(|e| StorageError::Migration(format!("migration 015 {column} failed: {e}")))?;
        }
    }

    // 迁移 016：建立平台级全域用户，并把旧的会话参与者关系归并到 user_id。
    // 迁移在每次启动时重复探测，所有写入均使用 IF NOT EXISTS / 条件更新，因而可重入。
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT NOT NULL,
            external_id TEXT NOT NULL,
            metadata TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(platform, external_id)
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 016 users failed: {e}")))?;

    let has_participant_user_id: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('participants') WHERE name='user_id'",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 016 participant probe failed: {e}")))?;
    if has_participant_user_id == 0 {
        sqlx::query(
            "ALTER TABLE participants ADD COLUMN user_id INTEGER REFERENCES users(id) ON DELETE SET NULL",
        )
        .execute(pool)
        .await
        .map_err(|e| StorageError::Migration(format!("migration 016 participant column failed: {e}")))?;
    }

    // 旧数据没有平台字段时按现有 OneBot 适配器命名空间回填；未来适配器必须显式写入平台。
    sqlx::query(
        r#"INSERT OR IGNORE INTO users(platform, external_id)
           SELECT COALESCE(NULLIF(CASE WHEN json_valid(metadata) THEN json_extract(metadata, '$.platform') END, ''), 'onebot_qq'),
                  COALESCE(NULLIF(CASE WHEN json_valid(metadata) THEN json_extract(metadata, '$.stable_user_key') END, ''), external_id)
             FROM participants
            WHERE role='user'"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 016 user backfill failed: {e}")))?;
    sqlx::query(
        r#"UPDATE participants
              SET user_id = (
                  SELECT u.id FROM users u
                   WHERE u.platform = COALESCE(NULLIF(CASE WHEN json_valid(participants.metadata) THEN json_extract(participants.metadata, '$.platform') END, ''), 'onebot_qq')
                     AND u.external_id = COALESCE(NULLIF(CASE WHEN json_valid(participants.metadata) THEN json_extract(participants.metadata, '$.stable_user_key') END, ''), participants.external_id)
              ),
                  metadata = json_set(CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END,
                                      '$.platform', COALESCE(NULLIF(CASE WHEN json_valid(metadata) THEN json_extract(metadata, '$.platform') END, ''), 'onebot_qq'))
            WHERE role='user' AND user_id IS NULL"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 016 participant backfill failed: {e}")))?;

    let has_memory_user_id: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='user_id'",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 016 memory probe failed: {e}")))?;
    if has_memory_user_id == 0 {
        sqlx::query("ALTER TABLE memories ADD COLUMN user_id INTEGER REFERENCES users(id) ON DELETE SET NULL")
            .execute(pool)
            .await
            .map_err(|e| StorageError::Migration(format!("migration 016 memory column failed: {e}")))?;
    }
    // 候选记忆沿用已有 source_participant_id；无法可靠映射的旧记忆保持 NULL。
    sqlx::query(
        r#"UPDATE memories
              SET user_id = (SELECT p.user_id FROM participants p
                               WHERE p.id = CASE WHEN json_valid(memories.metadata)
                                                 THEN json_extract(memories.metadata, '$.source_participant_id') END)
            WHERE user_id IS NULL
              AND CASE WHEN json_valid(metadata)
                       THEN json_extract(metadata, '$.source_participant_id') END IS NOT NULL"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 016 memory backfill failed: {e}")))?;

    migrate_relationships_to_users(pool).await?;
    migrate_relationship_interactions_to_users(pool).await?;
    ensure_semantic_memory_user_id(pool).await?;
    ensure_memory_sharing_policies(pool).await?;
    sqlx::query("DROP TABLE IF EXISTS relationship_stable_users")
        .execute(pool)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("migration 016 legacy mapping cleanup failed: {e}"))
        })?;
    enforce_user_participant_identity(pool).await?;

    // 迁移 018：明确目标记忆修订审计表。
    // 旧 memories 行通过 COALESCE(status, 'formal') 继续视为正式记忆，
    // 无需回写大表；source_request_id 唯一约束保证重放幂等。
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS memory_revisions (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            old_memory_id     INTEGER NOT NULL,
            new_memory_id     INTEGER NOT NULL,
            operation         TEXT NOT NULL CHECK(operation IN ('replace', 'append')),
            reason            TEXT NOT NULL,
            source_request_id TEXT NOT NULL UNIQUE,
            model_version     TEXT NOT NULL,
            created_at        TEXT NOT NULL
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 018 memory_revisions failed: {e}")))?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_memory_revisions_old_memory ON memory_revisions(old_memory_id, created_at)",
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 018 revision index failed: {e}")))?;

    // 迁移 019：结构化 claim 历史。旧 claim 永远保留，current 只表示当前有效投影。
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS memory_claims (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            claim_key         TEXT NOT NULL,
            subject_user_id   INTEGER NOT NULL,
            predicate         TEXT NOT NULL,
            value             TEXT NOT NULL,
            source_memory_id  INTEGER NOT NULL,
            valid_from        TEXT NOT NULL,
            valid_to          TEXT,
            current           INTEGER NOT NULL DEFAULT 1 CHECK(current IN (0, 1)),
            created_at        TEXT NOT NULL DEFAULT (datetime('now'))
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 019 memory_claims failed: {e}")))?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_memory_claims_current ON memory_claims(claim_key, subject_user_id, current)",
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 019 claim index failed: {e}")))?;
    Ok(())
}

/// 跨会话记忆共享的高优先级人工确认策略。
/// `requested` 只记录请求，不影响读取；只有 `confirmed` 才阻止未来跨会话检索。
async fn ensure_memory_sharing_policies(pool: &SqlitePool) -> Result<(), StorageError> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS memory_sharing_policies (
            character_id INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            status TEXT NOT NULL CHECK(status IN ('requested', 'confirmed', 'cancelled')),
            requested_at TEXT NOT NULL DEFAULT (datetime('now')),
            confirmed_at TEXT,
            reason TEXT,
            PRIMARY KEY(character_id, user_id)
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("memory sharing policy table failed: {e}")))?;
    Ok(())
}

/// 向量记忆是可选增强，但仍必须遵守全域用户的删除和合并边界。
/// 旧库的归属从 metadata 中的 user_id/source_participant_id 回填；无法解析的
/// 记录保持 NULL，仍可按角色和会话读取，但不会被误归到某个用户。
async fn ensure_semantic_memory_user_id(pool: &SqlitePool) -> Result<(), StorageError> {
    let has_column: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('semantic_memories') WHERE name='user_id'",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("semantic memory user_id probe failed: {e}")))?;
    if has_column == 0 {
        sqlx::query(
            "ALTER TABLE semantic_memories ADD COLUMN user_id INTEGER REFERENCES users(id) ON DELETE SET NULL",
        )
        .execute(pool)
        .await
        .map_err(|e| StorageError::Migration(format!("semantic memory user_id column failed: {e}")))?;
    }
    sqlx::query(
        r#"UPDATE semantic_memories
              SET user_id = COALESCE(
                  CAST(json_extract(CASE WHEN json_valid(metadata) THEN metadata ELSE '{}' END, '$.user_id') AS INTEGER),
                  (SELECT p.user_id FROM participants p
                    WHERE p.id = CAST(json_extract(CASE WHEN json_valid(semantic_memories.metadata) THEN semantic_memories.metadata ELSE '{}' END,
                                             '$.source_participant_id') AS INTEGER))
              )
            WHERE user_id IS NULL"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("semantic memory user_id backfill failed: {e}")))?;
    Ok(())
}

/// 确保每个用户参与者都映射到全域用户；删除账户时此类历史占位会先转成 system。
async fn enforce_user_participant_identity(pool: &SqlitePool) -> Result<(), StorageError> {
    let missing_user_id: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM participants WHERE role='user' AND user_id IS NULL",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("user participant identity probe failed: {e}")))?;
    if missing_user_id > 0 {
        return Err(StorageError::Migration(format!(
            "存在 {missing_user_id} 个没有全域身份的用户参与者，拒绝完成迁移"
        )));
    }

    sqlx::query(
        r#"CREATE TRIGGER IF NOT EXISTS participants_fill_user_identity_on_insert
           AFTER INSERT ON participants
           FOR EACH ROW WHEN NEW.role = 'user' AND NEW.user_id IS NULL
           BEGIN
               INSERT OR IGNORE INTO users(platform, external_id)
               VALUES (
                   COALESCE(NULLIF(json_extract(CASE WHEN json_valid(NEW.metadata) THEN NEW.metadata ELSE '{}' END, '$.platform'), ''), 'onebot_qq'),
                   NEW.external_id
               );
               UPDATE participants
                  SET user_id = (
                      SELECT id FROM users
                       WHERE platform = COALESCE(NULLIF(json_extract(CASE WHEN json_valid(NEW.metadata) THEN NEW.metadata ELSE '{}' END, '$.platform'), ''), 'onebot_qq')
                         AND external_id = NEW.external_id
                  )
                WHERE id = NEW.id AND user_id IS NULL;
           END"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("user participant insert guard failed: {e}")))?;
    sqlx::query(
        r#"CREATE TRIGGER IF NOT EXISTS participants_fill_user_identity_on_update
           AFTER UPDATE OF role, user_id, metadata, external_id ON participants
           FOR EACH ROW WHEN NEW.role = 'user' AND NEW.user_id IS NULL
           BEGIN
               INSERT OR IGNORE INTO users(platform, external_id)
               VALUES (
                   COALESCE(NULLIF(json_extract(CASE WHEN json_valid(NEW.metadata) THEN NEW.metadata ELSE '{}' END, '$.platform'), ''), 'onebot_qq'),
                   NEW.external_id
               );
               UPDATE participants
                  SET user_id = (
                      SELECT id FROM users
                       WHERE platform = COALESCE(NULLIF(json_extract(CASE WHEN json_valid(NEW.metadata) THEN NEW.metadata ELSE '{}' END, '$.platform'), ''), 'onebot_qq')
                         AND external_id = NEW.external_id
                  )
                WHERE id = NEW.id AND user_id IS NULL;
           END"#,
    )
    .execute(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("user participant update guard failed: {e}")))?;
    Ok(())
}

/// 将关系交互幂等表从会话参与者归并到全域用户。
///
/// 旧表使用 `(character_id, participant_id, source_message_id)`，同一用户
/// 跨会话时会出现多套幂等记录。新表使用 `(character_id, user_id,
/// source_message_id)`；重复记录只保留一条，因为它们代表同一个来源消息。
async fn migrate_relationship_interactions_to_users(pool: &SqlitePool) -> Result<(), StorageError> {
    let has_table: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='relationship_interactions'",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 017 interaction table probe failed: {e}")))?;
    if has_table == 0 {
        return Ok(());
    }

    let has_user_id: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('relationship_interactions') WHERE name='user_id'",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        StorageError::Migration(format!(
            "migration 017 interaction column probe failed: {e}"
        ))
    })?;
    if has_user_id > 0 {
        return Ok(());
    }

    let orphan_count: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*)
             FROM relationship_interactions i
             LEFT JOIN participants p ON p.id = i.participant_id
            WHERE p.id IS NULL OR p.user_id IS NULL"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        StorageError::Migration(format!(
            "migration 017 interaction orphan probe failed: {e}"
        ))
    })?;
    if orphan_count > 0 {
        return Err(StorageError::Migration(format!(
            "关系交互表存在 {orphan_count} 条无法映射到全域用户的记录，已保留旧表并拒绝迁移"
        )));
    }

    let mut tx = pool.begin().await.map_err(|e| {
        StorageError::Migration(format!("migration 017 transaction begin failed: {e}"))
    })?;
    sqlx::query(
        r#"CREATE TABLE relationship_interactions_new (
            character_id INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            source_message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (character_id, user_id, source_message_id)
        )"#,
    )
    .execute(&mut *tx)
    .await
    .map_err(|e| StorageError::Migration(format!("migration 017 interaction table failed: {e}")))?;
    sqlx::query(
        r#"INSERT OR IGNORE INTO relationship_interactions_new
             (character_id, user_id, source_message_id, created_at)
           SELECT i.character_id, p.user_id, i.source_message_id, i.created_at
             FROM relationship_interactions i
             JOIN participants p ON p.id = i.participant_id
            WHERE p.user_id IS NOT NULL"#,
    )
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        StorageError::Migration(format!("migration 017 interaction backfill failed: {e}"))
    })?;
    sqlx::query("DROP TABLE relationship_interactions")
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("migration 017 interaction drop failed: {e}"))
        })?;
    sqlx::query("ALTER TABLE relationship_interactions_new RENAME TO relationship_interactions")
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("migration 017 interaction rename failed: {e}"))
        })?;
    tx.commit().await.map_err(|e| {
        StorageError::Migration(format!("migration 017 transaction commit failed: {e}"))
    })?;
    Ok(())
}

/// 将旧的 Character × Participant 关系表一次性转换为 Character × User。
/// 合并规则：交互次数求和；熟悉度、好感、信任、尊重、厌烦、亲密取最大值；
/// 创建时间取最早，最后交互和更新时间取最新。该规则避免丢失最强关系信号，
/// 同时保留跨会话累积次数；重复运行会检测 user_id 列并直接跳过。
async fn migrate_relationships_to_users(pool: &SqlitePool) -> Result<(), StorageError> {
    let has_user_id: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('relationships') WHERE name='user_id'",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        StorageError::Migration(format!("migration 016 relationship probe failed: {e}"))
    })?;
    if has_user_id > 0 {
        return Ok(());
    }

    let orphan_count: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*)
             FROM relationships r
             LEFT JOIN participants p ON p.id = r.participant_id
            WHERE p.id IS NULL OR p.user_id IS NULL"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        StorageError::Migration(format!(
            "migration 016 relationship orphan probe failed: {e}"
        ))
    })?;
    if orphan_count > 0 {
        return Err(StorageError::Migration(format!(
            "关系表存在 {orphan_count} 条无法映射到全域用户的记录，已保留旧表并拒绝迁移"
        )));
    }

    // 关系表重建包含 DROP/RENAME，必须在同一事务内完成；否则进程在
    // DROP 后中断会留下无表或半迁移状态，下一次启动无法可靠恢复。
    let mut tx = pool.begin().await.map_err(|e| {
        StorageError::Migration(format!("migration 016 transaction begin failed: {e}"))
    })?;

    sqlx::query(
        r#"CREATE TABLE relationships_new (
            character_id INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            familiarity REAL NOT NULL DEFAULT 0.0,
            affection REAL NOT NULL DEFAULT 0.2,
            trust REAL NOT NULL DEFAULT 0.1,
            respect REAL NOT NULL DEFAULT 0.2,
            annoyance REAL NOT NULL DEFAULT 0.0,
            intimacy REAL NOT NULL DEFAULT 0.0,
            interaction_count INTEGER NOT NULL DEFAULT 0,
            last_interaction TEXT NOT NULL DEFAULT (datetime('now')),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY(character_id, user_id)
        )"#,
    )
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        StorageError::Migration(format!("migration 016 relationship table failed: {e}"))
    })?;
    sqlx::query(
        r#"INSERT INTO relationships_new
           (character_id,user_id,familiarity,affection,trust,respect,annoyance,intimacy,
            interaction_count,last_interaction,created_at,updated_at)
           SELECT r.character_id, p.user_id,
                  MAX(r.familiarity), MAX(r.affection), MAX(r.trust), MAX(r.respect),
                  MAX(r.annoyance), MAX(r.intimacy), SUM(r.interaction_count),
                  MAX(r.last_interaction), MIN(r.created_at), MAX(r.updated_at)
             FROM relationships r
             JOIN participants p ON p.id=r.participant_id
            WHERE p.user_id IS NOT NULL
            GROUP BY r.character_id,p.user_id"#,
    )
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        StorageError::Migration(format!("migration 016 relationship backfill failed: {e}"))
    })?;
    sqlx::query("DROP TABLE relationships")
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("migration 016 relationship drop failed: {e}"))
        })?;
    sqlx::query("ALTER TABLE relationships_new RENAME TO relationships")
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            StorageError::Migration(format!("migration 016 relationship rename failed: {e}"))
        })?;
    tx.commit().await.map_err(|e| {
        StorageError::Migration(format!("migration 016 transaction commit failed: {e}"))
    })?;
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

-- 会话外部 ID 只在会话类型内唯一；同一个平台 ID 可能同时是私聊用户号和群号。
CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_external_type
    ON conversations(external_id, conversation_type);

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
    user_id         INTEGER REFERENCES users(id) ON DELETE SET NULL,
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
    metadata        TEXT NOT NULL DEFAULT '{}',
    user_id         INTEGER REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_memories_character
    ON memories(character_id, memory_type);

-- Relationships (Character × User)
CREATE TABLE IF NOT EXISTS relationships (
    character_id    INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
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
    PRIMARY KEY (character_id, user_id)
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
    proactive_count_date TEXT,
    proactive_count INTEGER NOT NULL DEFAULT 0,
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
    metadata        TEXT NOT NULL DEFAULT '{}',
    user_id         INTEGER REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS memory_sharing_policies (
    character_id INTEGER NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK(status IN ('requested', 'confirmed', 'cancelled')),
    requested_at TEXT NOT NULL DEFAULT (datetime('now')),
    confirmed_at TEXT,
    reason TEXT,
    PRIMARY KEY(character_id, user_id)
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
    proactive_count_date TEXT,
    proactive_count INTEGER NOT NULL DEFAULT 0,
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
