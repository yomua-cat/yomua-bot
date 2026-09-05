Roadmap

Phase 0 — Foundation

目标：

Rust Runtime
SQLite
Event Bus
Configuration
Logging
Graceful Shutdown

Phase 1 — QQ MVP

实现：

OneBot 11 Adapter
NapCat connection
Private Message
Group Message
Message persistence

Phase 2 — Character

实现：

Character Definition
Character Card Import
Character Binding
Character State
Conversation Context

Phase 3 — AI Conversation

实现：

LLM Provider
Context Builder
Behavior Engine
Reply Decision
Basic Emotion
Basic Relationship

Phase 4 — Memory

实现：

Long-term Memory
Memory extraction
Memory retrieval
Relationship memory

Phase 5 — Human-like Behavior

实现（已完成）：

Reply probability —— 确定性内容哈希 + reply_mode 基础阈值调制
Typing/reply delay —— 决策延迟在发送前真实生效，并截断到 3s 安全上限
Ignore —— 未达回复阈值的消息被忽略（不占用 LLM）
Proactive behavior —— 后台 ProactiveDriver（60s tick / 30min 冷却），MVP 仅做状态更新与落库
Mute schedule —— "HH:MM-HH:MM" 静默时段（支持跨午夜），命中时降低未提及消息回复概率、被 @ 消息仍回复，并硬禁主动行为
Different treatment —— 亲密度（熟悉 / 好感 / 信任 / 亲密综合）提高或降低回复意愿与延迟
State-driven behavior —— 精力 / 注意力 / 压力驱动回复概率与延迟

后续扩展（不在本阶段）：

InitiateProactive —— 真实主动发消息
主动对话的 LLM 参与
群聊多角色主动行为

Phase 6 — Plugin

实现：

Plugin Manifest
Plugin Supervisor
IPC
Permissions
Event Subscription
Plugin API

Phase 7 — Multiple Characters

实现：

Multiple Character Runtime
Multiple Character per Group
Character interaction
Character-specific relationship

Phase 8 — Advanced Cognition

未来：

Background cognition
Semantic memory
RAG
Embedding
Advanced Lorebook retrieval
Complex group simulation

这些功能必须建立在已有架构上，而不是重新设计 Core。

Phase 9 — WebUI

最后再考虑：

Character management
Conversation management
Memory management
Relationship visualization
Scheduler management
Plugin management
Runtime monitoring

WebUI 不得成为 Core 的依赖。