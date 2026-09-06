# Character Runtime State Model

> Status: Design Baseline  
> Scope: Character、Conversation、User Relationship、Memory、Emotion、Behavior State 的职责与隔离模型  
> Purpose: 为 QQBOT Character Runtime 的领域模型、数据库模型、消息处理流程及后续实现提供约束  
> 
> 本文记录已经确认的架构决策。实现不得擅自改变本文定义的状态归属、可见性或角色隔离规则。

---

## 1. 核心目标

QQBOT 的核心不是一个简单的 QQ Bot，而是一个以 Runtime 为核心的多角色 Character Runtime。

Runtime 负责管理：

- Character
- Conversation
- User
- Character 与 User 的长期关系
- Character 与 Conversation 的上下文状态
- Behavior Engine
- Emotion
- Memory
- LLM 调用
- QQ 等外部通信适配器

核心原则：

> Runtime 是核心，QQ 只是外部通信能力。

QQ、OneBot、NapCat 等不应该成为 Character Runtime 领域模型的一部分。

---

# 2. 核心架构原则

必须遵守以下边界：

- LLM != Character
- Character != Conversation
- Conversation != QQ
- QQ != Runtime
- Emotion != LLM
- Behavior != LLM
- Memory != Prompt
- Plugin != Core
- Storage != Domain

进一步针对本模型：

> Conversation 决定“在哪里发生”。

> Character 决定“是谁在经历”。

> Character × Conversation State 决定“这个角色在这个会话中经历过什么”。

> Character × User Relationship 决定“这个角色认识这个用户多少”。

---

# 3. Character Model

## 3.1 Character Card

Character Card 是角色的定义。

它描述角色“是什么样的人”，而不是运行时调度器。

典型内容包括：

- Identity
- Personality
- Speaking Style
- Background
- Lore
- Cognition / Thinking Style
- 行为倾向
- 其他人格设定

例如：

    Character Card: Alice

    Identity:
        Alice

    Personality:
        主动
        好奇
        对熟人比较亲近

    Speaking Style:
        轻松
        口语化

    Cognition:
        比较谨慎
        不轻易相信陌生信息

    Behavior Tendencies:
        喜欢主动寻找话题
        对长时间沉默比较敏感

Character Card 可以提供 Behavior Engine 所需的人格倾向或默认配置。

但是 Character Card 不负责描述运行时调度。

---

# 4. Character Card 不负责 Runtime 调度

以下内容不应该写入 Character Card：

    “每 10 分钟检查一次当前对话。”

    “每天 23:00 主动发送一条消息。”

    “每隔 30 秒检查一次是否应该发言。”

这些属于 Runtime / Behavior Engine / Scheduler 的职责。

原因：

- 时间调度不是人格
- 定时器不是人格
- Scheduler 不是人格
- 是否触发行为不是 LLM 自己决定的事情

正确关系是：

    Character Card
        ↓
    人格与行为倾向
        ↓
    Behavior Engine
        ↓
    当前状态、规则、时间、概率等判断
        ↓
    决定是否触发行为
        ↓
    必要时调用 LLM

Behavior Engine 不应该周期性调用 LLM 并询问：

    “我现在要不要说话？”

应该首先通过低成本的规则、状态、时间、概率等机制判断是否存在行为机会。

只有真正需要生成内容时，才调用 LLM。

---

# 5. Character State

每一个 Character 都拥有属于自己的状态。

不同 Character 不共享 Character State。

例如：

    Alice
        └── Alice State

    Bob
        └── Bob State

不能存在：

    Shared Character State
        ├── Alice
        └── Bob

然后通过 Prompt 决定谁可以使用。

这是错误的模型。

正确原则：

> 每一个 Character 都是独立的状态主体。

Alice 的状态属于 Alice。

Bob 的状态属于 Bob。

---

# 6. Character × Conversation State

Character 本身虽然拥有自己的状态，但涉及具体会话的状态必须进一步绑定到 Conversation。

因此需要区分：

    Character

    Character × Conversation

一个 Character 可以同时参与多个 Conversation，但不同 Conversation 之间的会话状态必须独立。

例如：

    Alice × Q
    Alice × W

这两个状态不能互相污染。

因此：

    Alice 在 Q 群中的 Emotion
    !=
    Alice 在 W 群中的 Emotion

同样：

    Alice 在 Q 群中的 Conversation Memory
    !=
    Alice 在 W 群中的 Conversation Memory

---

# 7. Conversation

Conversation 表示一个独立的交互上下文。

Conversation 不等于 QQ 群。

QQ 群、私聊、其他未来通信平台都应该通过 Adapter 转换成 Runtime 可以理解的 Conversation。

Conversation 应该能够表达：

- Conversation Identity
- Participants
- Message
- 当前 Active Character
- Conversation-level metadata
- Character-specific conversation state

但 Conversation 本身不能成为所有角色共享历史和状态的容器。

---

# 8. Character × Conversation 的状态隔离

一个 Conversation 可以先后使用多个 Character。

例如：

    Conversation Q

    Alice active
        ↓
    Bob active
        ↓
    Alice active

这并不意味着 Alice 和 Bob 共享 Q 的状态。

应该理解为：

    Conversation Q
    ├── Alice × Q State
    └── Bob × Q State

每个 Character 在这个 Conversation 中拥有自己的独立状态。

---

# 9. 角色切换

角色切换不是删除旧角色状态，也不是把 Conversation 的全部状态交给新角色。

角色切换的真正语义：

> 改变 Conversation 当前的 Active Character。

例如：

    Conversation Q

    Active Character = Alice

切换后：

    Active Character = Bob

这只是：

    Alice → Bob

不会：

- 删除 Alice 的状态
- 把 Alice 的 Memory 复制给 Bob
- 把 Alice 的 Emotion 复制给 Bob
- 把 Alice 的历史复制给 Bob
- 把 Alice 的 User Relationship 复制给 Bob

---

# 10. 角色切换后的状态恢复

如果某 Character 曾经在某 Conversation 中运行过，那么再次切换回来时，应恢复该 Character 在该 Conversation 中存续至今的状态。

例如：

    Conversation Q

    Alice
        └── 已存在 Alice × Q State

    Bob
        └── 已存在 Bob × Q State

当前：

    Active = Alice

切换：

    Active = Bob

Bob 应加载：

    Bob × Q State

而不是从 Alice 当前状态开始。

之后：

    Active = Alice

则重新使用：

    Alice × Q State

因此角色切换本质上是状态上下文的切换，而不是角色状态重建。

---

# 11. 新角色进入已有 Conversation

如果某 Character 从未参与过某 Conversation，那么该 Character 在这个 Conversation 中没有历史状态。

例如：

    Conversation Q

    Alice × Q
        └── 已存在状态

    Bob × Q
        └── 已存在状态

    C × Q
        └── 不存在

从 Alice 切换到 C：

    Active Character = C

C 应从自己的初始状态开始。

不能因为 Conversation Q 已经存在大量消息，就自动让 C 获得这些消息。

---

# 12. Character History Visibility

这是本系统非常重要的隔离规则。

> Character 不能看到自己未在场期间发生的其他 Character 的经历。

例如：

    Conversation Q

    10:00
    Alice active

    小明：
    “我喜欢猫。”

    Alice：
    “真的吗？”

    10:10
    切换为 Bob

此时 Bob 不能因为 Conversation Q 的数据库中存在：

    小明：“我喜欢猫。”
    Alice：“真的吗？”

就自动知道这些内容。

原因：

> 当这些消息发生时，Active Character 是 Alice，而不是 Bob。

Bob 当时不在场，因此 Bob 没有这些认知。

---

# 13. Message 与 Character Visibility

因此 Message 不能简单地被视为：

    Conversation History
        ↓
    所有 Character 都可以读取

正确模型必须考虑：

    Message
        ├── Conversation
        ├── Sender
        ├── Timestamp
        └── 当时的 Active Character / 角色上下文

消息存在于 Conversation 中，不代表所有 Character 都有资格读取并使用该消息作为自己的认知。

---

# 14. Character 的“在场”概念

对于 Character 而言：

> 能否知道某件事情，取决于该事情发生时 Character 是否处于该 Conversation 的 Active Context。

例如：

    10:00
    Active = Alice

    小明：
    “我喜欢猫。”

那么：

    Alice
        └── 可以获得这条信息

    Bob
        └── 不可以获得这条信息

即使 10:30 切换到 Bob：

    Bob
        └── 仍然不知道 10:00 的这件事情

除非之后小明在 Bob 的在场期间再次告诉 Bob。

---

# 15. Conversation Memory

Conversation Memory 属于具体 Conversation 的上下文记忆。

例如：

    Conversation Q

    Memory:
        当前正在讨论某个游戏
        小明准备去东京
        上一轮讨论确定了某个共同决定

这些内容描述的是：

> 这个 Conversation 发生过什么。

但是 Conversation Memory 不能自动变成所有 Character 的认知。

如果某件事情发生时 Alice 是 Active Character：

    Alice 可以获得相关认知。

如果 Bob 当时不是 Active Character：

    Bob 不会因为 Conversation Memory 中存在这条信息而自动知道。

因此：

> “信息存储在哪里”和“哪个 Character 知道这件事情”是两个不同的问题。

---

# 16. Character × User Relationship

系统允许一部分信息跨 Conversation 保存。

但是这种跨 Conversation 的长期信息仍然属于 Character，而不是 User 全局共享信息。

正确模型：

    Alice
        └── User Relationship
            └── 小明

    Bob
        └── User Relationship
            └── 小明

而不是：

    小明
        └── Shared Memory
            ├── Alice
            └── Bob

---

# 17. User Impression

User Impression 表示某 Character 对某 User 形成的长期印象。

例如：

    Alice × 小明

    Impression:
        比较喜欢猫
        性格比较安静
        对游戏感兴趣

这些印象：

- 属于 Alice
- 针对小明
- 可以跨 Conversation
- 不自动共享给 Bob

例如：

    Alice 在 Q 群认识小明。

之后：

    Alice 在 W 群再次遇到小明。

Alice 可以继续使用自己对小明的既有印象。

---

# 18. Character-specific User Memory

User Memory 不是系统级全局 User Memory。

正确模型：

    Alice × 小明
        └── Memory

    Bob × 小明
        └── Memory

两个 Memory 完全独立。

例如：

    Alice × 小明

    Memory:
        小明小时候养过一只叫“团子”的猫。

那么：

    Bob × 小明

不应该自动拥有：

    小明小时候养过一只叫“团子”的猫。

---

# 19. User Memory 的跨 Conversation 行为

Character × User Memory 可以跨 Conversation。

例如：

    Q 群

    Active Character = Alice

    小明：
    “我喜欢猫。”

Alice 获得：

    Alice × 小明
        └── Memory:
            喜欢猫

之后：

    W 群

    Active Character = Alice

Alice 可以继续使用：

    Alice × 小明
        └── Memory:
            喜欢猫

因为这是 Alice 对小明形成的长期认知。

---

# 20. User Memory 不跨 Character

同一个例子：

    Q 群

    Active Character = Alice

    小明：
    “我喜欢猫。”

Alice 获得：

    Alice × 小明
        └── 喜欢猫

之后：

    W 群

    Active Character = Bob

Bob 不应该自动知道：

    小明喜欢猫

因为：

    Alice × 小明 Memory
        !=
    Bob × 小明 Memory

---

# 21. Character 不会通过其他 Character 的经历获得 User Memory

例如：

    Q 群

    Active Character = Alice

    小明：
    “我喜欢猫。”

Alice 获得：

    Alice × 小明
        └── 喜欢猫

之后切换：

    Active Character = Bob

Bob 不能因为：

    Conversation Q
        └── 历史中存在“小明喜欢猫”

就生成：

    Bob × 小明
        └── 喜欢猫

原因：

> Bob 当时不在场，因此 Bob 无从得知。

---

# 22. 信息获得与信息存在必须严格区分

系统必须区分：

1. Information Exists
2. Character Has Observed Information
3. Character Has Remembered Information

例如：

    Conversation Q
        └── Message:
            小明：“我喜欢猫。”

这只能证明：

    Information Exists

如果当时：

    Active Character = Alice

那么可以进一步得到：

    Alice Observed Information

如果 Memory 系统决定长期保存：

    Alice × 小明 Memory
        └── 喜欢猫

但不能得到：

    Bob Observed Information

更不能直接得到：

    Bob × 小明 Memory
        └── 喜欢猫

---

# 23. Emotion

Emotion 至少属于 Character × Conversation 状态。

例如：

    Alice × Q
        └── Emotion = Happy

不能因为：

    Alice × Q = Happy

就推导：

    Alice × W = Happy

同样：

    Bob × Q

也不应该继承：

    Alice × Q

的 Emotion。

因此：

    Alice × Q Emotion
    Alice × W Emotion
    Bob × Q Emotion
    Bob × W Emotion

都是独立状态。

---

# 24. Behavior Engine State

Behavior Engine 的运行状态也属于具体 Character × Conversation 上下文。

例如：

    Alice × Q

    Behavior State:
        最近一次主动发言时间
        当前冷却状态
        当前行为上下文
        相关计数器
        概率状态
        其他运行时状态

不能让：

    Alice × Q

的 Behavior State 直接影响：

    Alice × W

更不能影响：

    Bob × Q

---

# 25. Character State Scope 总结

当前已经确定的数据作用域：

| 数据 | 作用域 | 是否跨 Conversation | 是否跨 Character |
|---|---|---:|---:|
| Character Card | Character | 是 | 否 |
| Character 自身状态 | Character | 是 | 否 |
| Conversation History | Character × Conversation 的可见上下文 | 否 | 否 |
| Conversation Memory | Character × Conversation | 否 | 否 |
| Emotion | Character × Conversation | 否 | 否 |
| Behavior State | Character × Conversation | 否 | 否 |
| User Impression | Character × User | 是 | 否 |
| User Memory | Character × User | 是 | 否 |

其中“跨 Conversation”表示该信息可以从一个 Conversation 延续到另一个 Conversation。

这不意味着它变成系统全局信息。

---

# 26. 推荐的领域关系

概念上的关系可以表示为：

    Runtime
    │
    ├── Characters
    │   │
    │   ├── Character Card
    │   │
    │   ├── Character State
    │   │
    │   └── User Relationships
    │       │
    │       └── Character × User
    │           ├── Impression
    │           └── Memory
    │
    └── Conversations
        │
        ├── Conversation Metadata
        │
        └── Character × Conversation
            ├── Active Character
            ├── History Context
            ├── Conversation Memory
            ├── Emotion
            ├── Behavior State
            └── Other Character-specific Context

---

# 27. 一个完整示例

假设存在：

    Character:
        Alice
        Bob

    User:
        小明

    Conversation:
        Q 群
        W 群

## Q 群

最初：

    Active Character = Alice

小明说：

    “我喜欢猫。”

Alice 获得：

    Alice × 小明
        └── User Memory:
            喜欢猫

同时：

    Alice × Q
        └── Conversation State
            └── 当前上下文中出现了“小明喜欢猫”

然后切换：

    Active Character = Bob

Bob 不获得：

    Alice × 小明 Memory
    Alice × Q History
    Alice × Q Emotion
    Alice × Q Behavior State

如果 Bob 在此之后与小明交流：

    Bob × Q

则 Bob 从自己的状态继续。

---

# 28. 再进入 W 群

W 群：

    Active Character = Alice

Alice 遇到小明。

此时 Alice 可以使用：

    Alice × 小明
        └── User Memory:
            喜欢猫

但 W 群的会话状态仍然是：

    Alice × W

因此：

    Alice × Q State
        !=
    Alice × W State

Alice 可以认识同一个小明，但不能把 Q 群的整个会话上下文直接带入 W 群。

这正是：

> Character × User 长期认知

与：

> Character × Conversation 短期上下文

之间的区别。

---

# 29. 角色切换的最终语义

角色切换必须遵循以下规则：

### 规则 1

切换不会删除旧 Character 状态。

### 规则 2

切换不会复制旧 Character 状态。

### 规则 3

切换不会让新 Character 自动获得旧 Character 的历史。

### 规则 4

如果新 Character 曾经参与过该 Conversation，则恢复其已有的 Character × Conversation State。

### 规则 5

如果新 Character 从未参与过该 Conversation，则从该 Character 在该 Conversation 中的初始状态开始。

### 规则 6

新 Character 只能从自己成为 Active Character 后获得新的 Conversation 信息。

### 规则 7

Character × User Memory 可以跨 Conversation 继续存在。

### 规则 8

Character × User Memory 不跨 Character。

---

# 30. 不允许的实现方式

以下实现方式属于架构错误。

## 30.1 所有角色共享 Conversation History

错误：

    Conversation
        └── History
            └── 所有 Character 都可以直接读取

因为这会导致新角色自动知道旧角色经历。

---

## 30.2 所有角色共享 User Memory

错误：

    User
        └── Shared Memory

因为这会破坏 Character-specific cognition。

---

## 30.3 切换角色时复制状态

错误：

    Alice × Q State
        ↓ copy
    Bob × Q State

因为这会导致角色之间的认知污染。

---

## 30.4 通过 Prompt 临时过滤解决角色隔离

不推荐：

    Database
        └── 所有角色所有历史
                ↓
            Prompt Filter
                ↓
            LLM

角色隔离应该首先存在于领域模型和数据访问层，而不是只依赖 Prompt 拼装阶段。

Prompt 层可以做最后的上下文组装，但不能成为唯一的安全边界。

---

## 30.5 让 LLM 决定 Behavior 调度

错误：

    Timer
        ↓
    LLM:
        “我要不要发言？”

正确：

    Scheduler / Event
        ↓
    Behavior Engine
        ↓
    Rule / State / Time / Probability
        ↓
    是否需要行为
        ↓
    必要时调用 LLM

---

# 31. 实现约束

后续实现必须保证：

1. Character State 有明确的 Character 归属。
2. Conversation State 有明确的 Character × Conversation 归属。
3. User Impression / Memory 有明确的 Character × User 归属。
4. 不允许通过全局 User Memory 让不同 Character 自动共享认知。
5. 不允许通过 Conversation History 让新进入的 Character 自动获得旧角色经历。
6. 角色切换必须是 Active Character 切换，而不是状态复制。
7. 角色重新进入 Conversation 时必须恢复自己的历史状态。
8. 新角色第一次进入 Conversation 时不得自动读取该 Conversation 的旧角色历史。
9. Emotion 不得跨 Conversation 自动共享。
10. Behavior State 不得跨 Conversation 自动共享。
11. Character Card 不承担 Scheduler / Timer / Runtime 调度职责。
12. Behavior Engine 必须独立于 LLM。
13. LLM 是生成能力，而不是 Runtime 的状态管理器。
14. Memory 系统不能简单等同于 Prompt。
15. Adapter 不得改变上述领域模型。

---

# 32. 当前未定义内容

以下内容本文件暂不做决定：

- Character Card 的完整 Schema
- Character State 的完整字段
- Character Global State 的具体内容
- Emotion Model 的具体实现
- Memory 的具体数据结构
- Memory consolidation / summarization
- Memory retrieval 算法
- User Impression 如何生成
- User Memory 如何生成
- Memory 删除与遗忘机制
- Conversation History 的具体存储方式
- Message visibility 的最终数据库实现
- Behavior Engine 的完整规则模型
- Scheduler 的具体实现
- LLM Context Builder 的具体实现
- Character 如何获得新信息的完整消息生命周期
- Plugin 如何访问 Character / Conversation / Memory
- Control API 如何操作 Character / Conversation
- 多 Adapter 场景下 Conversation Identity 的具体规范

这些内容必须在后续设计阶段单独确认，不得由实现者自行推导为架构事实。

---

# 33. 当前最重要的架构原则

最终可以将本阶段设计浓缩为以下原则：

> 一个 Character 是独立的认知主体。

> 一个 Conversation 是独立的交互上下文。

> Character 在不同 Conversation 中拥有相互独立的会话状态。

> Character 对 User 的 Impression 和 Memory 可以跨 Conversation 延续。

> Character 对 User 的认知不跨 Character 共享。

> 角色切换只是改变 Active Character，不是复制或重置角色状态。

> 角色只能知道自己在场期间获得的信息。

> Conversation 中存在一条消息，不代表所有 Character 都知道这条消息。

> Character Card 描述人格与行为倾向，不负责 Runtime 调度。

> Behavior Engine 根据规则、状态、时间和概率决定是否触发行为，必要时才调用 LLM。

> LLM 是 Character Runtime 的生成能力之一，而不是 Character Runtime 本身。

---

# 34. 后续设计入口

本阶段领域模型完成后，下一阶段应设计：

    外部消息
        ↓
    Adapter
        ↓
    Message Normalization
        ↓
    Conversation Resolution
        ↓
    Active Character Resolution
        ↓
    Character Context / Visibility Resolution
        ↓
    User Relationship Resolution
        ↓
    Memory Retrieval
        ↓
    Emotion / Behavior Evaluation
        ↓
    是否需要 LLM
        ↓
    LLM Generation
        ↓
    Response / Action
        ↓
    State Update
        ↓
    Memory Update

下一阶段重点不是立即实现代码，而是首先把上述消息生命周期逐步骤定义清楚。

尤其需要明确：

- 一条消息什么时候进入 Conversation
- 什么时候算 Character “看到”消息
- Conversation History 如何形成 Character-specific View
- User Memory 在什么条件下写入
- Conversation Memory 在什么条件下写入
- Emotion 在什么时候更新
- Behavior Engine 在消息事件和时间事件下如何运行
- LLM Context 到底可以读取哪些数据
- Character 切换发生在消息处理链的哪个阶段
- Action 成功与否如何反馈到 Runtime State

这些定义完成后，才能进入数据库 Schema、Application Service 和具体实现。