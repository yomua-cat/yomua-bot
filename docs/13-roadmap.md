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

本阶段进展（按实施批次推进，每批四命令全绿）：

1. 清单/发现/校验、权限判定、Plugin API 与线协议（长度前缀 + MessagePack）已完成。
2. 运行时注册表（状态机 + 连接挂接）、UDS 传输（握手/请求/通知/协议错误隔离）、
   Supervisor 生命周期（spawn/监控/崩溃重启预算/优雅停止）与事件订阅桥接已完成。
3. main.rs 装配接线（Supervisor/EventBridge 条件启用与优雅关停）、示例 echo 插件
   与协议文档已完成。

Phase 7 — Multiple Characters

实现（已按定稿行为模型实施）：

- Multiple Character Runtime —— 系统可注册/管理多个角色并存，每个角色独立
  人格/状态/情绪/关系/记忆（按 character_id 逻辑隔离，数据互相不可见）
- Multiple Character per Group（重定义）—— 不是"同群多角色"，而是
  "多角色可用、每会话单角色"：每个群/私聊恰好绑定一个角色，不同会话可绑
  不同角色；换角色 = 更新该会话绑定的 character_id（保留会话配置字段）
- Character interaction —— 本期不做（后置）
- Character-specific relationship —— 现状已满足（character_id + participant_id
  复合键），本期做验证回归
- 角色管理 —— 列出所有角色 / 查看各会话绑定关系 / 查看角色状态
- 硬性约束 A —— 换角色后角色内容严格隔离，新角色看不见该会话此前任何内容（不出戏）
- 硬性约束 B —— 系统指令消息（如"换角色 X"）绝不落库、不进角色上下文、
  不进插件 message 订阅；仅管理员/绑定者可触发

本阶段进展（按实施批次推进，每批四命令全绿）：

1. G1 会话唯一绑定（每会话单角色 + 启动脏数据检测）、G2 换角色保留会话配置字段并记录
   switched_at、G3 上下文按 switched_at 过滤历史（迁移 003）已完成。
2. G4 角色管理与绑定查询 CLI（list-characters / list-bindings / switch-character）、
   G5 QQ 指令截流（"换角色 X"在消息发布位被截流，只发布 CommandReceived，
   不落库 / 不进角色上下文 / 不进插件 message 订阅）、G6 指令权限
   （runtime.toml 配置 admin_users，未配置则无人可执行指令）已完成。

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