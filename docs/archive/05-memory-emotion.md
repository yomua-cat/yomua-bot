Memory, Emotion and Relationship

1. Memory

MVP 不使用 Vector DB。

第一阶段：

Message History
+
Persistent Long-Term Memory

2. Memory 类型

至少区分：

episodic
semantic
relationship
system

示例：

episodic:
昨天用户告诉角色自己考试失败。

semantic:
用户喜欢猫。

relationship:
角色对用户有较高好感。

system:
Character 的长期设定。

3. Memory 生命周期

Message
   ↓
Memory Candidate
   ↓
Importance Evaluation
   ↓
Persistent Memory

不要求每条消息都成为 Memory。

4. Memory Retrieval

MVP 可以使用：

recent messages
+
keyword / structured retrieval
+
relationship memory
+
character memory

未来再引入：

embedding
vector search
RAG

5. Emotion

Emotion 使用结构化状态。

示例：

happiness
anger
sadness
fear
affection
stress
energy

不要求这些字段全部暴露给用户。

6. Emotion Update

Emotion 可以由：

message event
relationship
current activity
previous emotion
time decay
behavior result

共同影响。

核心计算尽可能确定性。

7. Relationship

Relationship 是：

Character × Participant

而不是 User 全局属性。

因为同一个用户面对不同 Character 可以完全不同。

例如：

Character A → User X = friend
Character B → User X = stranger

8. Relationship State

可以包含：

familiarity
affection
trust
respect
annoyance
intimacy
interaction_count
last_interaction

这些数据必须持久化。

9. State Persistence

任何永久变化：

Emotion
Relationship
Memory
CharacterState

都必须最终写入 Storage。

不能只存在：

HashMap
Arc<Mutex<_>>

中。

10. 防止内存无限增长

运行时可以 cache：

active characters
active conversations
recent context

但必须：

- 有 TTL
- 有容量限制
- 可以 eviction
- 数据库是真实来源