System Architecture

1. 总体结构

                         ┌───────────────────────┐
                         │        NapCat         │
                         │          QQ           │
                         └───────────┬───────────┘
                                     │
                              OneBot 11
                                     │
                         ┌───────────▼───────────┐
                         │    OneBot Adapter      │
                         └───────────┬───────────┘
                                     │
                              Core Event
                                     │
┌────────────────────────────────────▼────────────────────────┐
│                     Character Runtime                       │
│                                                             │
│  Event Bus                                                  │
│      │                                                      │
│      ▼                                                      │
│  Conversation Manager                                       │
│      │                                                      │
│      ▼                                                      │
│  Character Runtime                                          │
│      │                                                      │
│      ├── Character Definition                               │
│      ├── Character State                                    │
│      ├── Emotion                                            │
│      ├── Relationship                                       │
│      ├── Memory                                             │
│      └── Behavior                                           │
│              │                                              │
│              ▼                                              │
│        Behavior Engine                                      │
│              │                                              │
│       ┌──────┼───────────────┐                              │
│       │      │               │                              │
│       ▼      ▼               ▼                              │
│      Ignore Simple       Cognition                          │
│                         │                                    │
│                         ▼                                    │
│                    LLM Scheduler                             │
│                         │                                    │
│                         ▼                                    │
│                    LLM Provider                              │
│                         │                                    │
│                         ▼                                    │
│                    Action Plan                               │
│                         │                                    │
│                         ▼                                    │
│                    Event / Action                            │
│                                                             │
│  Scheduler ──────────────────────────────┐                  │
│  Plugin Supervisor ──────────────────────┤                  │
│  Storage ────────────────────────────────┘                  │
└─────────────────────────────────────────────────────────────┘

2. 模块

core/
├── character/
├── conversation/
├── cognition/
├── behavior/
├── emotion/
├── relationship/
├── memory/
├── scheduler/
├── llm/
├── event/
├── storage/
└── plugin/

adapters/
└── onebot/

plugins/

3. 依赖方向

允许：

adapter
    ↓
application/core
    ↓
domain
    ↓
storage abstraction

不允许：

domain → adapter
domain → OneBot
domain → NapCat
character → SQLite implementation
character → OpenAI SDK
behavior → OneBot

Domain 层必须保持平台无关。

4. Domain / Application / Infrastructure

采用三层边界。

Domain

负责：

- Character
- Conversation
- Message
- Emotion
- Relationship
- Memory
- Behavior
- State

不负责：

- 网络
- SQLite
- HTTP
- OneBot
- Plugin IPC

Application

负责：

- Event Processing
- Conversation Orchestration
- Character Runtime
- Behavior Decision
- Cognition Scheduling
- Action Execution

Infrastructure

负责：

- SQLite
- HTTP
- LLM SDK
- OneBot
- Plugin IPC
- Process Supervisor

5. Event Driven

核心通过统一 Event 流转。

例如：

UserMessageReceived
        ↓
ConversationManager
        ↓
CharacterRuntime
        ↓
StateUpdate
        ↓
BehaviorDecision
        ↓
CognitionRequest
        ↓
LLM
        ↓
ResponseGenerated
        ↓
SendMessage Action
        ↓
Adapter

事件本身不应该携带平台特有类型。

6. 核心设计规则

Rule 1

任何 Core 模块都不能直接调用 QQ。

Rule 2

任何 Domain 模块都不能直接调用 LLM。

Rule 3

任何模块都不能直接操作 SQLite。

必须通过 Repository / Storage abstraction。

Rule 4

LLM 调用必须经过 LLM Scheduler。

Rule 5

插件不能直接访问 Core 内部实现。

插件只能使用 Plugin API。

Rule 6

Runtime 崩溃风险高的组件必须进程隔离。

7. 长生命周期模型

Runtime 是长期运行的 Service：

start
  ↓
initialize
  ↓
load persistent state
  ↓
connect adapters
  ↓
event loop
  ↓
schedule tasks
  ↓
recover adapter
  ↓
continue

Adapter 断线不能导致 Runtime 退出。

8. 一个 Runtime 一个 QQ

Runtime A
 ├── QQ Account A
 ├── Character A
 ├── Character B
 └── Character C

Runtime B
 ├── QQ Account B
 └── Characters...

不要在 Core 内设计 Multi-Account Manager。