# OneBot 入站事件持久化设计

设计日期：2026-10-21  
范围：只分析当前 OneBot 入站可靠性并提出实现方案；本轮不修改代码，不改变当前行为。

## 1. 当前链路与丢失窗口

当前普通 OneBot 帧经过以下路径：

1. `src/adapters/onebot/connection.rs` 的 `handle_connected_io` 读取 WebSocket 文本帧，并由 `classify_incoming_message` 按 `echo` 区分 Action 回执和事件。回执直接交给 `ConnectionShared::complete_request`；事件文本先放入连接循环内存中的 `pending_events` FIFO。
2. FIFO 中的事件再通过容量为 256 的 `event_tx` 通道发给 `InboundProcessor`。连接循环最多在 FIFO 暂存 1024 条事件；到达上限时停止读取 WebSocket，形成背压，以便仍可在读取范围内及时路由回执。
3. `src/adapters/onebot/mod.rs` 的 `InboundProcessor::run` 串行接收事件；`handle_raw` 调用 `handle_raw_once`，后者解析协议事件、处理自身回声、黑名单、管理命令、会话/参与者解析和角色绑定，再调用 `MessageAcceptanceService`。
4. `src/application/message_acceptance.rs` 的 `accept_inbound` 调用消息仓储。`src/infrastructure/storage/repository/message_repository.rs` 的 `accept_message_with_status` 在一个 SQLite 事务中检查平台消息 ID、分配会话序号、写入消息和 `MessageAccepted` Outbox 事件；事务提交后才发布内存 `EventBus` 通知。

由此可精确界定当前丢失窗口：从 OneBot 帧被读取到消息/Outbox 事务成功提交之间，事件只有内存副本。进程在此期间崩溃，事件随进程内存消失，QQ 不会重推。这个窗口包括连接 FIFO、已发送到 256 容量通道但尚未处理的事件、处理器当前正在执行但事务未提交的事件，以及暂时性数据库错误重试期间的当前事件。若进程正常关停，连接循环收到信号会退出，处理器也会退出；当前没有持久队列接管这些尚未接受的事件，因此有限关停或强制取消也可能丢弃它们。

处理器遇到可重试的数据库/连接错误时，最多重试 3 次，退避后继续处理当前事件；重试最终失败时记录错误并丢弃该次处理，不会阻塞后续事件。解析错误、无绑定、黑名单或其他业务忽略也不会重试。相关实现位置：

- 读取、FIFO 上限和回执路由：[`connection.rs`](../../src/adapters/onebot/connection.rs)，`MAX_PENDING_INBOUND_EVENTS`、`classify_incoming_message`、`handle_connected_io`。
- 接收通道和处理任务装配：[`mod.rs`](../../src/adapters/onebot/mod.rs)，`OneBotAdapterImpl::with_connector`、`start`。
- 串行处理、业务入口和有限重试：[`mod.rs`](../../src/adapters/onebot/mod.rs)，`InboundProcessor::run`、`handle_raw`、`handle_raw_once`、`retry_inbound_operation`。
- 消息接受与事务后通知：[`message_acceptance.rs`](../../src/application/message_acceptance.rs)，`MessageAcceptanceService::accept_inbound`。
- 平台消息 ID 去重及消息/Outbox 原子提交：[`message_repository.rs`](../../src/infrastructure/storage/repository/message_repository.rs)，`accept_message_with_status`。
- SQLite 现有平台消息索引：[`migrations.rs`](../../src/infrastructure/storage/migrations.rs)，`idx_messages_platform`。

当前平台消息去重发生在消息接受事务中，键为 `(conversation_id, platform_message_id)`。它能在业务接受阶段抑制同一会话的重复投递；不能找回尚未抵达该事务的内存事件，也不能独立作为原始事件队列或确认记录。

## 2. 方案比较

| 方案 | 复杂度 | Action 回执路由 | SQLite 写入压力 | 与现有幂等的关系 | 崩溃恢复与顺序 |
| --- | --- | --- | --- | --- | --- |
| A. 读到事件后先写轻量 inbox，再异步处理 | 中高：新增 inbox 表、入队/领取/完成状态、恢复扫描、保留策略和与消息接受事务的协调 | 若连接读取循环同步等待每条事件的 SQLite 提交，积压/锁竞争会延迟之后的回执；可将事件持久化写入与回执处理分流，但必须有界且保证事件提交前不声称已持久接收 | 每条事件至少一次 inbox 插入和一次完成状态写入；消息事件还会有现有消息/Outbox 写入。是否可接受需实测，不预设吞吐结论 | Inbox 的本地事件 ID 负责原始事件生命周期；平台 ID 去重继续保护业务接受，处理器崩溃后重复执行可收敛到既有消息 | `received_at` 或单调 `ingress_seq` 可用于有序恢复；单消费者按序处理。若仅按收到顺序入 inbox，先前事件必须完成或明确终结后才能越过，避免重启后顺序改变 |
| B. 缩短窗口但不持久化 | 低：减少处理前等待、提升处理并发或缩小内存队列等局部调整 | 若不改变回执独立读取机制，影响较小；提高并发可能增加 SQLite 写竞争，但不会消除队列满时的背压 | 不增加入站队列表写入；已有消息/Outbox 写入不变 | 仍只在消息事务阶段去重；平台重投重复可过滤，但未提交前崩溃仍无法恢复 | 只能减少事件平均停留时间，不能恢复任何尚未提交的内存事件，也不能提供持久顺序恢复 |
| C. 读帧后先完成完整消息接受事务，再继续读取 | 中：需把 OneBot 解析/业务依赖放入读取链或增加同步接受边界 | SQLite 写入、会话解析或锁等待期间连接循环不能读取后续 Action 回执，直接扩大回执延迟；增加独立读任务后实质上又需要方案 A 的队列/协调 | 每条消息接受前同步执行完整事务；比轻量 inbox 更早触发消息、Outbox 和关联索引写入 | 复用现有平台 ID 去重，仍需处理事务提交后但确认状态更新前的重复 | SQLite 接受成功后消息顺序可靠；但帧刚读到、接受事务尚提交时仍存在崩溃窗口，不是“读到就持久化” |

### 回执与入站数据的边界

Action 回执携带 `echo`，只用于完成本进程 `pending` 请求；它不是普通可重放事件，也不应进入 inbox。入站事件应在连接层分类之后才入队。方案 A 若在 `handle_connected_io` 内直接对 SQLite 做同步 `await`，会让连接循环在持久写入期间无法继续读取回执。要维持回执响应性，需要单独的有界持久化入口/任务，并明确容量耗尽时的背压；不能以无界内存队列掩盖数据库变慢。

## 3. 推荐方案与设计约束

推荐方案 A：增加轻量 SQLite `inbound_events`（名称可在实现前按仓储约定确定），持久保存原始 OneBot 事件文本及接收元数据，再由单一有序消费者完成现有业务处理。该方案把未提交消息事务前的长内存窗口缩短为“帧已读、inbox 插入事务尚未提交”的短窗口；提交 inbox 后，即使业务处理或进程崩溃，仍可恢复。它不可能消除“帧已从网络读取但本地尚未完成任何持久写入”的瞬间窗口，OneBot/QQ 未提供重推时该边界仍存在。

推荐的数据与处理语义：

- Inbox 记录至少保存本地唯一 ID、同一连接/进程可排序的 `ingress_seq`、原始事件文本、`received_at`、状态、尝试次数、最后错误和处理时间。原始正文可能包含 QQ 消息内容，应设置明确的终结记录清理/保留规则，并避免把正文写入普通日志。
- Action 回执仍由连接循环按 `echo` 路由，不插入 inbox。事件写入采用有界入口；只有 inbox 事务提交后才允许消费者处理该记录。入口满或数据库不可写时，施加 WebSocket 读取背压，而不是丢弃事件或无限增长内存。
- 使用单消费者和持久 `ingress_seq` 保序。恢复时先处理较小序号的 `received`/可重试记录；永久解析错误或业务忽略必须落为可审计终态，之后才能推进序列。是否允许某类终态错误跳过应保持与当前业务行为一致。
- 对成功接受的消息，最好在同一个 SQLite 事务中写入消息、Outbox 并把对应 inbox 记录标记为完成。需要扩展消息接受事务入口以携带 inbox ID。这样避免“消息/Outbox 已提交但 inbox 未完成”造成额外重放；即使暂时采用分开的完成事务，现有平台消息 ID 去重仍可避免重复消息和 Outbox，但只有对所有有副作用分支逐一验证后才能依赖它。
- 对非普通消息、自身回声、管理指令、黑名单和无绑定分支，也需给出完成/忽略终态。自身回声现由 `(conversation_id, platform_message_id)` 持久去重；管理指令走现有消息接受幂等；其余明确忽略分支不应永久留在 inbox。
- SQLite 是同一持久化边界。启动迁移后先恢复 inbox，再允许新事件越过旧序号处理；恢复过程必须纳入启动/关停生命周期和容量监控。具体 checkpoint、保留周期和容量值需由运维约束决定，不在本设计中凭空指定。

### 迁移步骤

1. 定义 inbox 状态机、容量与终态保留规则；确认原始事件文本的隐私和磁盘占用策略。
2. 新增 SQLite 迁移和仓储接口；实现按序插入、领取/恢复、重试计数、终态记录及清理查询，并保持 Action 回执只走内存 pending 路由。
3. 将连接帧读取接到有界持久化入口。对每个入站事件，只有持久化提交成功后才通知消费者；容量或数据库不可用时停止继续读取，确保不静默丢弃。
4. 把现有 `InboundProcessor` 的业务逻辑改为消费 inbox ID；将可恢复错误留在 inbox 中供后续重试，而不是重试耗尽后删除唯一副本。
5. 将普通消息/命令接受和 inbox 完成标记纳入同一事务；为自身回声及所有忽略/错误终态添加显式完成处理。通过启动恢复顺序和单消费者约束维持事件顺序。
6. 分阶段发布并验证：旧版本遗留的内存事件无法迁移；升级后只保证新写入 inbox 的事件可在进程重启后恢复。观察 inbox 积压、最老事件年龄、失败状态和 SQLite 忙错误，再决定是否调整批量写入或并发。

## 4. 新增测试清单

- 写入 inbox 后强制模拟处理器/进程重启，确认记录可恢复且按 `ingress_seq` 递增消费。
- 事件已从 WebSocket 读取但 inbox 插入失败时，不允许业务消费者先处理；达到容量上限时验证背压且不静默丢弃。
- Action 回执与入站事件交错到达时，回执仍按 `echo` 完成正确 pending 请求，且不进入 inbox。
- 普通消息在消息/Outbox/inbox 完成事务提交前后注入失败，验证事务原子性；提交成功后重复处理不新增消息、会话序号或 Outbox。
- 测试 inbox 完成状态落盘前发生崩溃的恢复路径，并验证平台消息 ID 去重将重放收敛到原消息。
- 管理指令重放不重复执行命令副作用；自身回声重放不重复插入回声记录或重复确认未知发送。
- 黑名单、无绑定、非消息事件、解析失败和永久业务拒绝均进入预期终态，不阻塞后续有序事件。
- SQLite 锁/连接暂时错误时记录保留并可重试；永久错误按设计保留可诊断信息，不导致无限热循环。
- 关停期间验证待处理 inbox 的持久状态、消费者退出和重启后续处理；确认未处理记录不会被误标完成。
- 按真实配置与隔离 SQLite 对持久化频率/积压行为做测量，再决定批量提交和容量参数；测试本身不连接真实 QQ 或外部模型。

## 5. 不能解决的问题与限制

- 本地 inbox 只能保证 SQLite 提交之后的恢复。WebSocket 帧已经被本地读取、但 inbox 写入尚未提交时崩溃，仍可能丢失；QQ 不重推时无法补回。若要消除该窗口，需要平台提供可重放游标/历史拉取或上游确认协议，当前 OneBot 路径没有已验证的此类能力。
- SQLite 磁盘损坏、磁盘耗尽、文件系统未持久化已确认写入、设备丢失等仍可能破坏 inbox；需要备份和磁盘告警策略。
- Inbox 不能保证模型回复或 OneBot 出站最终送达；后续处理仍受 Outbox、模型和平台发送语义约束。
- 平台消息 ID 去重依赖会话解析成功且 ID 在会话范围内稳定。它不能替代 inbox 的接收顺序、重试状态或原始事件保存。
- 增加持久写会增加 SQLite 写入和磁盘占用；具体压力、吞吐和回执延迟没有在本设计阶段测量，不能据此声称性能改善或无回归。
- 当前 `EventBus` 仍是尽力而为广播，不应把 inbox 设计误解为其所有订阅者都获得可靠投递；可靠业务消费仍应使用已持久化的 Outbox。
