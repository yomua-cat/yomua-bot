Conversation System

1. Conversation 是统一抽象

私聊和群聊都表示为：

Conversation

通过类型区分：

Private
Group

而不是创建两个完全不同的聊天系统。

2. Conversation

核心字段：

id
type
external_id
created_at
updated_at
state

其中：

external_id

由 Adapter 提供，但 Core 不理解其平台语义。

3. Participant

Conversation 中存在 Participant：

Participant
├── id
├── display_name
├── role
└── metadata

QQ Adapter 可以把 OneBot User 转换成 Participant。

Core 不知道 QQ Number 的含义。

4. Message

统一 Message：

Message
├── id
├── conversation_id
├── sender
├── content
├── timestamp
├── reply_to
├── mentions
├── attachments
└── metadata

消息必须保存。

5. GroupContext（后置）

群聊额外拥有（本期未实现，后置）：

GroupContext
├── participants
├── active_characters   ← 依赖多角色并存；当前模型为每会话单角色
├── recent_topics
├── group_state
└── group_memory

6. Reply Modes

每个 CharacterBinding 可以配置：

mention_only

只有：

@Character

时回复。

occasional

角色可以自然插话。

natural

完全根据上下文决定是否参与。

7. 回复决策

群聊收到消息：

Message
  ↓
是否明确提及？
  ↓
Character Behavior
  ↓
Relationship
  ↓
当前 State
  ↓
Conversation Context
  ↓
Reply Probability
  ↓
Reply / Ignore

不要默认：

收到消息 → LLM → 回复

8. Ignore 是合法行为

Character 可以：

- 不回复
- 延迟回复
- 简短回复
- 敷衍
- 转移话题
- 主动追问

Ignore 不应该被视为异常。

9. Message History

MVP 保存完整消息历史。

Context Builder 根据需要选择：

最近消息
+
相关长期记忆
+
Character Definition
+
Relationship
+
当前 State

不要无限制把全部历史塞给 LLM。