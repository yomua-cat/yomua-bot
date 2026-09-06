Development Rules

1. Rust

Core 使用 Rust。

目标：

- 长时间运行
- 高并发
- 低资源占用
- 安全的异步任务
- 进程管理
- IPC
- 单二进制部署

2. 模块边界优先

宁愿增加一个模块，也不要把所有逻辑塞进：

character.rs
bot.rs
runtime.rs
manager.rs

这些巨型文件。

3. 禁止 God Object

以下对象不得拥有整个系统：

CharacterManager
BotManager
RuntimeManager
AppState

如果一个对象开始同时管理：

Character
Memory
Emotion
LLM
SQLite
QQ
Plugin
Scheduler

必须拆分。

4. Trait Boundary

外部能力通过 trait：

LLMProvider
MessageSender
CharacterRepository
MemoryRepository
RelationshipRepository
PluginHost

5. Domain 不依赖 Infrastructure

错误：

Character → SqliteCharacterRepository

正确：

Character
   ↓
CharacterRepository trait
   ↓
SqliteCharacterRepository

6. LLM 调用纪律

禁止：

任意函数 → LLM

必须：

Cognition
   ↓
LLM Scheduler
   ↓
Provider

7. 持久化纪律

禁止：

修改内存状态
然后认为数据已经保存

状态修改必须有明确 Persistence Boundary。

8. Error Handling

外部系统失败不能导致 Core 直接退出。

尤其：

- OneBot
- LLM
- Plugin
- SQLite temporary failure
- Network

必须有明确错误处理。

9. Logging

日志至少区分：

runtime
adapter
character
conversation
llm
plugin
scheduler
storage

敏感数据不得无条件打印。

10. 测试

优先测试：

1. Domain
2. Behavior
3. Emotion
4. Relationship
5. Conversation
6. Scheduler
7. Adapter conversion
8. Persistence
9. Plugin IPC

LLM 本身不应该成为绝大多数单元测试的依赖。

11. MVP 原则

每加入一个功能，先回答：

它属于哪个模块？
它依赖谁？
谁依赖它？
是否需要持久化？
是否需要 LLM？
是否应该进入 Plugin？

如果无法回答，先不要实现。