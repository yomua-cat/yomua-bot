Project Scope

1. 项目定位

本项目是一个开源 Character Runtime / Character Agent Framework。

它不是单纯的 QQ Bot，而是一个能够让多个虚拟角色长期运行、拥有独立人格、状态、关系、记忆和行为逻辑的运行时。

QQ 只是第一种消息接入方式。

核心目标：

- 支持 QQ 私聊
- 支持 QQ 群聊
- 支持多个 Character
- Character 可以绑定到多个 Conversation
- 支持一个群聊中存在多个 Character
- 支持 SillyTavern Character Card / Lorebook
- 支持长期记忆
- 支持持久化情绪和关系
- 支持主动行为
- 支持自然的人类式对话
- 支持多种 LLM Provider
- 支持插件扩展
- 支持 Linux 长时间运行
- 支持 NapCat/OneBot 断线后 Core 独立存活并恢复

2. 核心原则

2.1 LLM != Character

LLM 只是 Character 的一种认知能力。

Character 本身由以下部分组成：

Character
├── Definition
├── State
├── Emotion
├── Relationship
├── Memory
├── Behavior
├── Schedule
└── Cognition / LLM

不能把 Character 简化成一个 System Prompt。

2.2 Core 不依赖 QQ

Core 不允许出现：

QQ
OneBot
NapCat
GroupMessage
PrivateMessage

等平台概念。

Core 只处理统一的：

Conversation
Message
Participant
Event
Action

2.3 LLM 不是行为循环

不能设计成：

每个 Tick
    ↓
调用 LLM
    ↓
让 LLM 思考
    ↓
决定是否说话

正确方向：

事件 / 状态变化
        ↓
确定性行为规则
        ↓
行为决策
        ↓
必要时才调用 LLM

2.4 状态必须持久化

以下数据不能只存在内存：

- CharacterState
- Emotion
- Relationship
- Memory
- Conversation
- Message
- Schedule
- Plugin persistent data

内存只能作为 cache。

SQLite 是第一阶段的 Source of Truth。

2.5 一个 Runtime 一个 QQ 账号

第一阶段不支持一个 Runtime 同时登录多个 QQ 账号。

部署模型：

Runtime Instance
    │
    └── One QQ Account

多个 QQ 账号使用多个 Runtime 实例。

3. 非目标

MVP 不实现：

- WebUI
- PostgreSQL
- Redis
- Kafka
- Vector Database
- Embedding/RAG
- TTS
- Image Generation
- MCP
- WASM Plugin
- 分布式 Runtime
- 多 QQ 账号
- 复杂自主意识模拟

这些功能未来可以加入，但不得破坏当前核心边界。

4. 技术方向

第一阶段：

- Rust Core
- SQLite
- Linux Native
- OneBot 11
- NapCat
- Process-based Plugin
- IPC
- 多 LLM Provider
- Character Card Importer

5. 设计目标

最重要的不是功能数量，而是：

1. 模块职责清晰
2. 依赖方向明确
3. LLM 调用可控
4. 状态可靠持久化
5. 插件故障隔离
6. Adapter 与 Core 解耦
7. 后续演进不会形成巨型模块