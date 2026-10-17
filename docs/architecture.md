# 架构

本页描述当前程序的装配与调用关系。模型类型或接口存在，不等于相应能力已接入运行链路；接线缺口见[项目现状](status.md)。

## 分层与入口

| 层 | 职责与入口 |
| --- | --- |
| Domain | 角色、会话、消息、状态、决策、事件、Outbox 类型及仓储接口；[模块入口](../src/domain/mod.rs) |
| Application | 运行时、行为判断、上下文、认知、控制、导入与后台任务；[模块入口](../src/application/mod.rs) |
| Infrastructure | SQLite、HTTP 模型服务、角色卡解析、插件进程与 IPC；[模块入口](../src/infrastructure/mod.rs) |
| Adapters | OneBot 协议转换、连接、入站消息及出站调用；[模块入口](../src/adapters/mod.rs) |

[主程序](../src/main.rs)负责装配具体实现。Domain 通过仓储接口表达持久化需求；实际装配中 Application 也直接引用部分适配器和基础设施类型，不能把分层目标当成已验证的严格依赖隔离。

`yomua-bot` 是常驻程序及角色卡管理工具；`yomua-bot-ctl` 是 Unix socket 控制客户端。它们共享[库入口](../src/lib.rs)。

## 启动与消息链路

启动流程为：读取配置并校验 → 初始化日志 → 创建数据目录、打开 SQLite 并迁移 → 装配仓储与服务 → 按配置启动插件 → 启动后台任务和控制 socket，并确认 socket 已成功绑定 → 启动 OneBot 适配器 → 等待控制关停信号。控制 socket 启动失败会阻止 OneBot 进入工作态并进入统一清理流程。

```mermaid
flowchart TD
    A[OneBot WebSocket 入站] --> B[转换事件、解析会话和参与者]
    B --> C{系统指令}
    C -->|是| D[已有会话/身份检查后持久化系统消息]
    D --> E[MessageAccepted Outbox]
    C -->|否| F[查询会话角色绑定]
    F --> G[MessageAcceptanceService]
    G --> H[SQLite 事务：序号、消息、Outbox]
    H --> I[OutboxDispatcher：领取租约]
    I --> J[MPSC 与完成应答]
    J --> K[OutboxProcessor]
    K --> L{系统指令?}
    L -->|是| M[CommandHandler：权限与绑定变更]
    L -->|否| N[ReplyProcessor：状态、关系、规则、上下文与生成]
    M --> O[控制回复任务 → OneBot]
    N --> P[回复任务 → OneBot]
    H --> Q[EventBus MessageAccepted → 观察者与插件]
```

入站实现见[OneBot 适配器](../src/adapters/onebot/mod.rs)。系统指令先截流，只接受已有会话、参与者和角色绑定，然后作为 `author_kind=system_command` 的消息写入消息表和 Outbox；它不会进入普通角色上下文或插件消息订阅。普通消息无角色绑定时不进入消息落库及回复流程，但允许按需创建会话与发送者。

[消息接受服务](../src/application/message_acceptance.rs)调用[消息仓储](../src/infrastructure/storage/repository/message_repository.rs)，在同一事务内递增会话序号并写入消息和 Outbox 行。广播事件保留给插件和观察者；普通消息回复只由 Outbox Dispatcher/Processor 链路处理。

[OutboxDispatcher](../src/application/outbox_dispatcher.rs)轮询、领取租约并等待消费者应答；[OutboxProcessor](../src/application/outbox_processor.rs)把 payload 转交[回复处理器](../src/application/reply_processor.rs)。租约、轮询和最大重试次数是代码默认值，当前没有对应 TOML 字段。消费者成功后标记完成，失败可转入等待重试或死信；永久失败事件记录后不会阻塞同一会话后续事件。

这条链路不构成端到端可靠投递保证：控制回复和普通回复都已进入持久回复任务，但 OneBot 超时通常没有平台消息 ID，未知外部回执仍需要人工核查。完成状态只能按实际实现解释，不能据此推断 QQ 用户已收到消息。

## 角色与状态边界

| 数据 | 作用域 | 当前内容 |
| --- | --- | --- |
| 角色定义 | Character | 人格、场景、提示、示例、Lorebook 与元数据 |
| CharacterState | Character | energy、stress、current_activity、last_updated |
| ConversationState | Character × Conversation | 会话精力、压力与更新时间 |
| Mood | Character × Conversation | mood 标量与更新时间 |
| BehaviorState | Character × Conversation | 主动行为冷却时间 |
| User | `(platform, external_id)` | 跨会话稳定的平台用户身份；支持审计合并与删除 |
| Participant | Conversation × external_id | 会话内显示名、作者身份和消息归属，不能替代 User |
| Relationship | Character × User | 关系数值与跨会话交互记录；领域对象保存 `user_id`，会话 `participant_id` 只用于入站解析；交互幂等表同样按 `user_id` 归并 |
| Memory | Character × 可选 User，可附会话 | 记忆来源和用户归属；稳定事实和关系信息默认可跨会话召回，且只作为角色内部上下文使用，不向用户透露来源会话；确认禁用策略后停止未来跨会话检索，非共享经历留在来源会话，候选记忆仍不可见 |
| CharacterBinding | Conversation → Character | 回复模式、主动行为、静默时段等绑定设置 |

[领域模型](../src/domain/character.rs)与[情绪模型](../src/domain/emotion.rs)定义状态。[角色运行时](../src/application/runtime.rs)按需加载并缓存角色，以每角色锁保护状态更新。控制命令直接写仓储时与缓存同步的关系仍需整顿。

当前绑定管理强制每会话一个角色；迁移发现重复绑定会拒绝启动并要求先明确保留项。接收时记录的角色 ID 与消费时重新读取绑定的方式共同参与发送前校验，绑定版本变化会取消旧任务。

## 决策、上下文与模型

[规则行为引擎](../src/application/behavior_engine.rs)结合回复模式、提及、静默时段、状态和关系决定是否回复及延迟。[决策编排器](../src/application/decision_orchestrator.rs)以规则结果为基线；`decision.toml` 启用后接入 advisory 决策模型，模型输出必须经过验证器和硬约束策略。

[ContextBuilder](../src/application/context.rs)加载角色归属的历史、Lorebook、记忆、关系及会话情绪；关系查询会先把当前 participant 映射到全域 User，跨会话记忆默认纳入该 User 的稳定事实和关系信息，确认禁用策略后由数据库层排除，并只进入角色内部上下文，不把来源会话写入对用户可见的回复，其他记忆限于来源会话。[CharacterHistoryProvider](../src/application/character_history.rs)负责历史归属和序号边界。[ContextAssembler](../src/application/context_assembler.rs)估算 token、预留输出空间并按层降级，需要已知的 `context_window`。主程序从 `llm.toml` 的 `[options]` 读取预算，即使使用多模型配置也是如此。

全域用户删除由 `ParticipantRepository` 在 SQLite 事务内执行：删除该用户来源的普通/向量记忆和关系数据，匿名化会话参与者、OneBot 私聊路由、消息/outbox 作者元数据与用户黑名单，再删除 `users` 身份。为保留历史审计与引用，消息行、正文和内部引用关系保留；历史作者显示为“已删除用户”，旧私聊主动发言会关闭。任何步骤失败都会回滚整笔删除。

[认知层](../src/application/cognition.rs)通过[调度器](../src/application/llm_scheduler.rs)发起请求，HTTP 实现在[OpenAI-compatible Provider](../src/infrastructure/llm/openai_compatible.rs)。Provider 连接与对话模型来自 `llm.toml`，独立决策模型来自 `decision.toml`，同一 Provider 可复用多个模型。LLM 关闭时保留消息并跳过生成，不发送伪造回复；启用后模型或上下文错误会按 Outbox 失败路径处理。

主程序没有给 ContextBuilder 注入 embedding scheduler，因此当前对话上下文使用关键词 Lorebook 与记忆检索。向量检索接口若被调用，必须带当前会话和稳定用户身份，由 SQL 先限制可见范围后再排序；存在语义存储或混合检索接口不代表对话已经使用 RAG。

## 后台任务与持久化

- 常驻任务：事件观察、主动行为评估、Outbox 调度和消费、控制服务、适配器连接与入站处理；系统指令在 Outbox 消费任务内处理。
- LLM 启用后启动后台认知任务，使用 `cognition` role；插件启用后增加事件桥接、传输和进程监督任务。
- 主动行为驱动会在已有话题依据、冷却、静默和每日上限通过后，以 SQLite 原子事务预留额度并创建持久回复任务，复用普通回复发送链路；没有已有话题依据时不会发言。主动合成消息不会更新用户关系或记忆。
- 回复任务在实际发送前取得当前 Outbox 租约栅栏；发送结果、未知回执和历史修复也必须通过同一租约条件写入，失去租约的旧 worker 不能覆盖新状态。平台网络调用本身仍没有幂等键，租约在网络调用期间失效时依靠 unknown 核查流程兜底。
- 自然语言记忆修改先持久化为 pending；后台维护任务会限量扫描并重放崩溃遗留请求，失败保留重试信息，成功或拒绝均写回审计状态，不把记忆正文放进普通控制响应。
- SQLite 使用 WAL、外键约束和连接池。[迁移](../src/infrastructure/storage/migrations.rs)在代码中执行并探测已有表列，没有独立迁移文件或自动回滚机制。
- 控制关停会发广播信号、停止插件与适配器、等待后台任务并关闭存储；超出 `shutdown_timeout_secs` 后取消剩余任务，保留未完成回复供核查，详见[部署运维](deployment.md)。
