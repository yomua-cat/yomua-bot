Storage

1. MVP

使用：

SQLite

SQLite 是第一阶段唯一持久化 Source of Truth。

2. Repository Boundary

Domain 不直接访问 SQLite。

使用：

CharacterRepository
ConversationRepository
MessageRepository
MemoryRepository
RelationshipRepository
ScheduleRepository
PluginDataRepository

3. 数据表

第一阶段至少：

characters
character_states

conversations
conversation_bindings

participants
messages

memories
relationships

schedules

events
plugin_data

4. Source of Truth

Database
   ↑
Repository
   ↑
Application
   ↑
Domain

内存：

Runtime Cache

不是 Source of Truth。

5. Event Log

重要事件可以持久化：

MessageReceived
StateChanged
EmotionChanged
RelationshipChanged
MemoryCreated
ResponseGenerated
MessageSent

事件日志用于：

- Debug
- 恢复
- 行为分析
- 后续审计

MVP 不要求完整 Event Sourcing。

6. Migration

数据库必须支持 migration。

不要依赖：

CREATE TABLE IF NOT EXISTS

来代替正式 schema migration。

7. Future

未来如果 SQLite 不够：

SQLite
   ↓
PostgreSQL

Domain 不应该需要重写。