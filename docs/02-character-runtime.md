Character Runtime

1. Character 定义

Character 不是 Prompt。

内部模型：

Character
├── Definition
├── RuntimeState
├── EmotionState
├── Relationships
├── Memories
├── BehaviorProfile
├── Schedule
└── CognitionProfile

2. CharacterDefinition

负责描述相对稳定的身份：

name
description
personality
scenario
style
background
greetings
example_messages
system_prompt
post_history_instructions
lorebook
metadata

这些数据来自：

- Character Card
- 自定义 JSON
- 未来数据库管理

3. CharacterState

描述角色当前状态。

示例：

{
  "energy": 72,
  "attention": 38,
  "current_activity": "看番",
  "social_mood": "平静",
  "stress": 12
}

状态必须持久化。

内存对象只是运行时缓存。

4. CharacterBinding

Character 与 Conversation 之间通过 Binding 关联。

Character
    │
    ├── Binding → Private Conversation A
    ├── Binding → Group Conversation B
    └── Binding → Group Conversation C

Binding 可以定义：

reply_mode
proactive_enabled
mute_schedule
behavior_overrides
context_policy

5. 多 Character

一个 Conversation 可以绑定多个 Character。

例如：

Group 123456

Character A
Character B
Character C

不同 Character 可以根据：

- mention
- conversation context
- behavior probability
- relationship
- current state

决定是否参与。

6. Character Runtime 生命周期

load definition
      ↓
load persistent state
      ↓
load active relationships
      ↓
load relevant memory
      ↓
attach conversation
      ↓
receive events
      ↓
update state
      ↓
behavior decision
      ↓
optional cognition
      ↓
action

7. Load-on-Demand

Character 数量理论上不限制。

但是：

全部 Character 常驻内存

不是目标。

使用：

Character Registry
        ↓
Metadata
        ↓
Lazy Load
        ↓
Runtime Cache

长期不活跃的 Character 可以卸载。

8. Character Identity

角色身份优先于 Runtime 身份。

例如 Character 是普通人：

用户：

你是不是 AI？

不能默认回答：

我是人工智能模型……

应该首先根据 CharacterDefinition 判断角色如何处理该问题。

只有 Character 本身设定为 AI 时，才自然地承认 AI 身份。

9. Character 不负责

Character 模块不负责：

- QQ API
- OneBot
- SQLite implementation
- HTTP
- LLM Provider implementation
- Plugin process
- Message sending

这些属于其他模块。