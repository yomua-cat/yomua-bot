# AI 伴侣运行时源码审计与迁移设计

审计日期：2026-10-20  
审计范围：`yomua-bot` 当前工作树、三个公开参考项目的当前主分支源码与官方文档。  
限制：本阶段只读审计和设计，不修改业务代码，不创建提交。参考项目只用于设计研究，不复制代码、提示词文本或角色素材。

## 证据分级

- **源码实现**：当前分支存在对应类型、函数和调用路径。
- **已接线但未验证**：装配或接口存在，但没有足够测试或真实运行证据证明行为。
- **自动化测试通过**：本次或项目现有测试明确覆盖并通过；只证明测试场景，不等于长期业务正确。
- **业务行为已验证**：有真实外部系统或持久运行证据；本次不把项目文档中的自述当作重新验证结果。
- **设计中未实现**：规格、注释或文档提出，但当前分支没有对应实现路径。
- **未找到证据**：在本次审计范围内没有找到可支持该结论的源码或测试。

## 1. 当前架构与真实执行链

### 1.1 责任边界

| 领域 | 当前模块 | 当前结论 |
| --- | --- | --- |
| 角色定义 | `src/domain/character.rs` 的 `CharacterDefinition` | 已实现。包含人格、场景、风格、背景、示例消息、系统提示、后历史指令、Lorebook 和元数据。 |
| 全局动态状态 | `CharacterState`、`CharacterRuntime` | 已实现。`energy`、`stress`、`current_activity`、`last_updated`，由 `CharacterRuntime` 串行化读改写并写 SQLite。 |
| 会话动态状态 | `ConversationState`、`Mood`、`MoodService` | 已实现。按角色×会话隔离；控制集成测试覆盖作用域和回滚。 |
| 关系 | `RelationshipService`、`relationship_repository.rs` | 已实现。按角色×全域用户，交互以来源消息幂等。 |
| 记忆 | `MemoryService`、`MemoryMaintenanceDriver`、`SqliteMemoryRepository` | 已实现候选、正式记忆、关键词/可选向量检索、共享策略和 pending 编辑恢复；压缩、系统化冲突修订未实现。 |
| 行为决策 | `RuleBehaviorEngine`、`DecisionOrchestrator`、`DecisionLlmPipeline` | 已实现规则优先、硬约束、可选决策模型和回退。模型只能返回动作建议，不能直接写状态或回复正文。 |
| 提示词 | `ContextBuilder`、`ContextAssembler`、`SystemPromptComponents` | 已实现独立组装和 token 预算降级；当前没有类似 SillyTavern 的可观测 slot/depth/outlet 调试协议。 |
| 模型适配 | `DefaultLlmScheduler`、OpenAI-compatible、TypeSafe Provider | 已实现 Provider/Model Instance/Role 路由和备用链；真实外部模型路由、延迟和额度仍需部署验收。 |
| 后台任务 | `OutboxDispatcher/Processor`、`ProactiveDriver`、`MemoryMaintenanceDriver`、认知后台、插件监督 | 已实现持久任务、租约、取消/关停信号和部分恢复；进程级长期运行和所有重启场景尚未完全验收。 |

### 1.2 一条完整普通消息链

1. OneBot 适配器在 `src/adapters/onebot/mod.rs` 接收事件，并由 `conversion.rs` 解析会话、参与者、平台消息 ID、提及、引用和媒体摘要。
2. 黑名单、已有绑定和系统指令分类在适配器/命令入口完成。普通消息进入 `MessageAcceptanceService::accept_inbound`（`src/application/message_acceptance.rs:111`）。
3. `accept_inbound` 生成事件 UUID，并调用 `MessageRepository::accept_message_with_status`，在同一 SQLite 事务内分配会话序号、写消息、写 `MessageAccepted` Outbox。平台消息身份提供入站幂等。
4. 事务成功后才通过 `EventBus` 发布内存广播。EventBus 是尽力而为通知通道，不承担可靠消费。
5. `OutboxDispatcher` 领取租约；`OutboxProcessor` 反序列化 payload，根据 `event_type` 调用 `ReplyProcessor::process_for_outbox`。处理器有并发上限和事件截止时间。
6. `ReplyProcessor` 重新核对会话 fence、角色绑定/启用状态和当前租约，冻结本轮连续消息批次，调用 `RuleBehaviorEngine` 和 `DecisionOrchestrator`。
7. `ContextBuilder::build` 读取角色归属历史、Lorebook、记忆、关系、心情和会话状态；`ContextAssembler::assemble_with_capabilities` 按能力和预算组装 `SystemPromptComponents`、历史和当前轮输入。
8. `CognitionLayer` 通过 `LlmScheduler` 调用配置的聊天模型。模型失败、上下文失败或未配置模型时走 Outbox 的失败/跳过路径，不伪造回复。
9. 生成文本先写入 `reply_tasks`，发送前再次验证租约、绑定版本和任务状态；OneBot 发送结果区分成功、明确失败、可重试网络失败和未知结果。
10. 关系交互、记忆候选和状态变化在回复处理/认知路径持久化；关系交互以来源消息 ID 幂等。发送结果和 Outbox 终态由租约保护，旧 worker 不能覆盖新 worker。

### 1.3 自主行为与后台入口

- 主动行为入口：`src/application/proactive.rs` 的 `ProactiveDriver` 定时 tick；规则引擎检查绑定开关、静默窗口、冷却、每日额度和话题依据，SQLite 事务原子预留额度，再入队持久回复任务。
- Outbox 入口：`OutboxDispatcher::run` 轮询和租约恢复；`OutboxProcessor::run` 并发消费并在关停时有限 drain。
- 记忆后台：`MemoryMaintenanceDriver::run_until_stopped`，限量审核候选记忆并恢复 pending 编辑请求。
- 认知后台：`CognitionDriver` 按绑定和冷却扫描需要认知的会话。
- 插件后台：插件 EventBridge、Transport、Supervisor 独立运行并受关停广播约束。
- 主程序装配：`src/main.rs` 负责 SQLite 迁移、仓储、服务、后台任务和 OneBot/控制 socket 启动。

### 1.4 当前可靠性边界

已找到实现和测试：入站平台 ID 幂等、消息与 Outbox 同事务、Outbox 租约 fencing、过期任务恢复、关系交互幂等、候选记忆幂等、主动额度原子预留、明确平台失败不重复发送、未知发送结果保留核查状态。

尚未达到业务验收：入站事件在进程崩溃前仍位于内存 FIFO 时不能恢复；回复批次没有独立持久化实体；OneBot 网络调用没有平台幂等键，租约在发送期间失效仍可能进入 `unknown`；真实 QQ 长期并发、热重载、重启恢复、积压和模型路由尚未在本次重新执行。

## 2. 参考项目事实核对

### 2.1 companion-emergence

当前主分支：`10856ab9a9f6ba26e740f2b5196a1e2c7dc6b636`。`LICENSE` 为 MIT。

目标路径均存在：

- `docs/source-spec/2026-04-21-framework-rebuild-design.md`：存在，但第 5 行标记为 `Draft — awaiting Hana's review`；第 63 行明确其目标是新身体/并行重建计划，规格不能当作已交付代码。
- `brain/emotion/`：实际存在 `state.py`、`decay.py`、`aggregate.py`、`blend.py`、`arousal.py`、`expression.py`、`influence.py` 等。
- `brain/memory/`：实际存在 SQLite store、pending queue、embedding、Hebbian matrix、semantic recall、reranker、clustering 和 forgetting 相关模块。
- `brain/soul/`、`brain/self_model/`：实际存在持久化、审核/撤销、cadence、gap、reconcile、resolve 等模块。
- `brain/personality/`：规格第 132-134 行列出目标目录，但当前主分支未找到该目录；人格演化部分仍属于设计或分散实现，不能按规格宣称已交付。

成熟实现证据：

- `brain/emotion/decay.py:22-62` 按每个情绪的 half-life 做指数衰减，identity-level 情绪不衰减，低于噪声地板的情绪删除。
- `brain/emotion/state.py` 有 `ResidueEntry`、clamp、词汇校验、有限 residue 和 JSON round-trip。
- `brain/memory/pending.py` 把自动生成内容先放入独立 pending queue；`tests/test_consolidation_gate.py` 覆盖候选不进入 recall、reject 无副作用、promote、embedding-on-promote 和同批近重复去重。
- `brain/memory/semantic_recall.py` 将候选池、向量召回、rerank、floor gate 和诊断日志分开；`hebbian.py` 提供连接强化、衰减、垃圾回收和 spreading activation。
- `brain/soul/store.py` 的 crystallization 是 append-only，支持撤销、来源记忆链接和 active/revoked 查询；cadence 有持久化、退避和 catch-up。
- `brain/self_model/` 有 gap 生命周期、短/长 EMA、corrupt recovery、冷却和 fail-soft resolve；后台入口实际在 `brain.bridge.supervisor`，而不是规格中拟议的单一 `brain/supervisor.py`。
- 测试覆盖 forgetting、soul review/revoke、self-model、supervisor fault isolation、pending stranded write recovery 等；这些证明代码路径和故障处理，不证明长期人格效果。

### 2.2 SillyTavern

当前仓库提交：`06bde939fb1e9c4c8d8641d810f0a916b5bce127`。仓库 `LICENSE` 为 AGPL-3.0。

`default/content/presets/openai/Default.json` 实际存在，提供：

- 明确的 `prompts` 槽位：main、worldInfoBefore/After、charDescription、charPersonality、scenario、personaDescription、dialogueExamples、chatHistory、jailbreak 等。
- `prompt_order` 为按角色配置的启用开关和显式顺序。
- 上下文、输出上限、温度和格式化模板等可配置字段。

官方 Prompt Manager 文档描述了 Quick/Utility Prompt、拖拽顺序、`In-Chat` 位置、Depth/Order、默认 prompt 的启用约束、格式化变量和 prompt 调试控制。

官方 World Info 文档描述了关键词/正则触发、常量条目、递归、包含组/优先级、概率、上下文范围、插入位置/Depth、`outlet` 显式插槽以及 token budget/Context% 竞争；向量激活被明确视为非确定性增强。

这些是可借鉴的协议设计，不应复制其 AGPL 源码、默认 prompt 文本、角色卡或第三方素材。

### 2.3 eros-engine

当前主分支：`9715af2ce7d8524ca5ce8771059d436c31951977`，版本文档标为 v2.3.0。目标 Rust 文件和 `docs/model-config.md` 实际存在；文件 SPDX 和仓库 `Cargo.toml` 为 `AGPL-3.0-only`。

- `crates/eros-engine-core/src/pde.rs`：`decide` 先执行确定性规则，再将可选 LLM 动作映射为 `ActionPlan`；关系/亲密度变化由程序计算，不由模型任意写入。内嵌测试覆盖 ghost 阈值、新关系保护、长消息、过时张力和零成本场景。
- `crates/eros-engine-server/src/pipeline/stream.rs`：使用显式 typed protocol frame（Meta、Delta、Done、Final、Error、ActionResponse、BodyState 等），有生成超时、可重试错误和审计元数据。
- `crates/eros-engine-server/src/pipeline/post_process.rs`：回复后处理并行运行 insight、memory、affinity、character/user insight；每阶段有独立持久化和失败隔离，关系行更新带 live gate/row-lock 语义。
- `docs/model-config.md`：按 task 配置 model/fallback/retry_depth/temperature/max_tokens/sampling/allow_traits，限制 tier 只能出现在 chat 任务，Provider body 参数有启动校验；PDE、输入/输出过滤、embedding、后处理各有任务边界。

这些模式适合研究决策边界、typed 流事件、后处理隔离和任务级模型路由；不能复制 AGPL 实现。

## 3. 对照表

| 方向 | 当前项目 | 参考设计/实现 | 差异与成熟度 | 可迁移设计 | 不应迁移/风险 |
| --- | --- | --- | --- | --- | --- |
| 人格与动态状态分离 | `CharacterDefinition` 与 Character/Conversation/Mood/Behavior 状态分开；自动化测试覆盖状态作用域。 | companion 的情绪、soul、self-model 与 persona 目录分层；但 personality 目录在当前分支缺失。 | 当前结构清楚，动态状态仍以少量标量为主；长期人格演化未实现。 | 保持静态 persona 与动态 `StateSnapshot` 分离，增加版本化来源和时间窗口。 | 不照搬“人格即会自动成长”的规格承诺；先定义可审计更新规则。 |
| 提示词独立组装 | `ContextBuilder` + `ContextAssembler`，有 token 预算和降级测试。 | SillyTavern 的 declarative slot/order/depth/outlet；eros 的 task/trait 注入审计。 | 已有组件化，但 slot 顺序和注入来源缺少统一 trace DTO。 | 引入 `PromptBlock`/`PromptTrace`，每块有 id、来源、优先级、预算、位置和裁剪原因。 | 不复制 AGPL prompt manager 或默认文本；World Info 向量激活不可直接视为确定性记忆。 |
| 记忆分层/检索/去重/压缩 | candidate/formal、episodic/semantic/relationship/system、关键词和可选向量、共享策略、来源幂等。 | companion 有 pending gate、embedding、Hebbian、semantic floor、rerank、近重复合并、forgetting；eros 后处理分阶段写入。 | 候选和检索已接线并测试；没有真正 consolidation/summary/冲突修订/衰减任务。 | 增加独立 `MemoryConsolidator`：候选→合并/摘要/冲突集→正式记忆，保留来源和审计。 | 不复制 Hebbian/forgetting 代码；先避免自动删除或不可逆摘要覆盖来源。 |
| 情绪/关系模型 | mood、energy、stress、relationship 数值和幂等交互。 | companion 按情绪维度 half-life/residue；eros 用确定性关系更新。 | 当前关系更新强，情绪表达维度较窄；没有按维度衰减和 residue。 | 增加可选 `AffectVector` 与 `DecayPolicy`，先只读计算并写回审计事件。 | 不让模型直接写关系/情绪；不得把参考项目的“人格”语义当业务事实。 |
| 自主行为 | `ProactiveDriver` 有冷却、静默、每日额度、话题依据和 SQLite 原子预留。 | companion supervisor 有 cadence/backoff/pending sweep；eros PDE 先规则后模型。 | 当前硬约束和额度较完整；取消、恢复和长期节流仍需进程级验收。 | 抽出 `AutonomyDecision`、`QuotaReservation`、`CancellationToken`、持久 cadence。 | 不引入不可解释的后台“人格行为”；任何主动副作用必须先持久化任务。 |
| 后台任务 | Outbox 租约/fencing、失败分类、关停 drain、记忆维护和插件监督。 | companion supervisor fault isolation、persisted cadence、stranded write recovery；eros stream/post-process 分阶段失败隔离。 | 当前 Outbox 可靠性较好；入站内存队列、回复批次和外部发送幂等仍有缺口。 | 统一 `JobRecord` 状态机和 stage-level audit；为回复批次增加持久边界。 | 不把“任务完成”当作平台送达；未知发送必须保留核查状态。 |
| 模型路由 | Provider/Model Instance/Role、备用链、共享预算、TypeSafe/兼容协议。 | eros task-level fallback/retry_depth/body validation；companion provider-neutral influence。 | 当前路由已实现但主要靠配置和单元测试；真实额度/失败语义未全面验证。 | 保持任务级 route contract，增加可观测的尝试链和预算账本。 | 不复制 eros 的 Provider/OpenRouter 规则；授权和供应商约束需另审。 |
| 长期人格一致性测试 | 有稳定决策、上下文顺序、关系幂等和持久化测试；没有长期轨迹基准。 | companion 有 emotional fidelity、forgetting、soul/self-model、supervisor 测试；规格中的长期方法仍是提案。 | 当前短链路回归强，长期语义验证弱。 | 建立 deterministic replay fixture、状态轨迹黄金文件和跨重启等价测试。 | 不把 LLM 文本相似度当人格一致性唯一指标。 |

## 4. 发现的问题及证据

### P0：记忆“正式化”有审核，但没有冲突修订闭环

- 证据：`MemoryMaintenanceDriver` 只处理 `confirm/reject/defer`；`SqliteMemoryRepository::apply_candidate_review` 写入审核 metadata 和 visibility。当前没有按事实键/来源建立冲突集合、选择旧/新来源或保留 superseded 链的应用服务。
- 影响：旧记忆与新信息冲突时，系统可能同时召回两个正式事实；验收要求“保留来源并执行明确更新规则”尚未满足。
- 分类：候选审核为**已实现且有自动化测试**；冲突修订为**设计缺口/未找到证据**。

### P0：没有可证明的记忆压缩/总结状态机

- 证据：存在历史读取、关键词/向量检索和候选审核，但未找到 `MemoryConsolidator`、摘要版本链、压缩 checkpoint 或压缩失败恢复状态。
- 影响：长期消息只能靠上下文预算降级，无法证明“压缩后仍保留来源和语义”。
- 分类：**未找到证据**。

### P1：动态情绪模型无法表达参考项目的衰减和残留

- 证据：当前 `Mood` 为按会话单值，`CharacterState` 主要为 energy/stress；未找到按情绪维度 half-life、residue、noise floor 或离线补偿任务。
- 影响：状态变化可以影响决策和提示词，但不能验证情绪随时间离线衰减、恢复或残留。
- 分类：状态持久化和提示词影响为**已实现/测试通过**；衰减和离线行为为**未实现**。

### P1：提示词可组装，但不可完整追踪“为什么出现/被裁剪”

- 证据：`SystemPromptComponents::render` 和 `ContextAssembler::degrade` 存在；测试覆盖 token 预算和降级。没有统一向调用方返回每个 block 的来源、顺序、预算、实际 token 和裁剪原因。
- 影响：出现人格漂移或上下文遗漏时，难以复现具体提示词构成。
- 分类：组装为**已实现/测试通过**；调试 trace 为**未找到证据**。

### P1：回复批次没有独立持久化边界

- 证据：`ReplyProcessor` 在处理时创建 `FrozenBatch`，依赖当前消息序号读取连续输入；项目文档也明确“当前没有独立持久化的批次实体”。
- 影响：进程在批次冻结、模型调用或发送之间重启时，只能依赖 Outbox/reply task 推断，无法完整重放批次快照。
- 分类：当前时序和租约为**已实现/自动化测试**；批次重启恢复为**未验收**。

### P1：自主行为具备硬约束，但缺少统一取消/恢复语义

- 证据：主动额度和冷却写入 `behavior_states`，入队失败回滚；后台使用 watch 关停信号。没有独立的主动意图记录、用户级取消标记或跨重启“已计划但未执行”的主动任务模型。
- 影响：取消主要依赖 Outbox/绑定 fence，不能把所有主动行为视为可查询、可取消的任务。
- 分类：节流/幂等为**已实现/测试通过**；完整取消和重启恢复为**未证明**。

### P2：跨会话记忆共享规则已存在，但语义验证不足

- 证据：仓储查询按 `user_id` 和 `memory_sharing_policies` 过滤；控制集成测试覆盖请求/确认/取消的数据库行为。尚未找到真实回复级测试证明模型不会暴露来源会话、原始群聊或内部记忆 ID。
- 影响：数据访问边界正确不等于生成内容不泄露来源。
- 分类：存储边界为**自动化测试通过**；回复语义为**业务行为未验证**。

### P2：参考项目的 AGPL 设计只能抽象迁移

- 证据：SillyTavern 和 eros-engine 当前仓库均为 AGPL；companion-emergence 为 MIT，但其规格和代码也包含具体实现与提示词/角色资产边界。
- 影响：直接复制代码、默认 prompt 或角色素材可能触发衍生作品和第三方授权问题。
- 分类：**授权风险已确认**。

## 5. 建议的模块边界和数据结构

下面的边界以渐进式增加为原则，保留现有消息、Outbox、仓储和 Provider 接口。

### 5.1 `PromptPlan` / `PromptTrace`

建议新增应用层 DTO，而不是让 LLM Provider 重新负责拼接：

```text
PromptBlock {
  id: String,
  source: Character | State | Relationship | Memory | Lorebook | History | CurrentTurn,
  role: system | user | assistant,
  position: OrderedSlot,
  priority: i32,
  text: String,
  estimated_tokens: u32,
  included: bool,
  omission_reason: Option<String>,
}
PromptPlan {
  blocks: Vec<PromptBlock>,
  context_window: u32,
  output_reserve: u32,
  trace_id: String,
}
```

`ContextAssembler` 继续负责预算；`PromptTrace` 只保存结构化元数据，默认不记录完整敏感正文。先为现有 block 加稳定 ID 和顺序测试，再考虑可配置 slot/depth。

### 5.2 `MemoryCandidate`、`MemoryClaim`、`MemoryRevision`

保留当前 `Memory` 表，新增逻辑层而非立即迁移表：

```text
MemoryCandidate {
  id, source_message_id, source_user_id, source_conversation_id,
  extracted_text, kind, confidence, status, review_metadata
}
MemoryClaim {
  claim_key, subject_user_id, predicate, value,
  valid_from, valid_to, current, source_memory_id
}
MemoryRevision {
  claim_key, old_memory_id, new_memory_id,
  reason, model_version, created_at
}
```

规则由程序决定：同一 `claim_key` 的新事实只能通过 `confirm/replace/append/reject` 之一更新；旧记录保留为 superseded，来源消息和审核理由不可删除。压缩摘要必须成为新的带来源记忆，不能覆盖原始消息或直接删除旧记忆。

### 5.3 `AffectVector` 与 `StateTransition`

第一步只增加纯函数和审计，不改变默认行为：

```text
AffectVector { dimensions: Map<String, f32>, residue: Vec<ResidueEntry> }
DecayPolicy { half_life: Map<String, Duration>, noise_floor: f32 }
StateTransition { source_event_id, before, delta, after, occurred_at, reason }
```

现有 `Mood`、energy/stress 可以作为兼容投影；离线 tick 计算必须幂等，依据 `last_updated` 和明确时钟，不在模型调用中隐式改变。

### 5.4 `AutonomyIntent` 与持久 cadence

建议把主动行为的“评估通过”和“回复任务入队”之间增加可查询实体：

```text
AutonomyIntent {
  id, character_id, conversation_id, trigger_kind,
  status: proposed | reserved | queued | cancelled | completed | expired,
  not_before, cooldown_key, quota_reservation, source_snapshot,
  created_at, updated_at
}
```

取消、绑定切换和重启恢复只操作 intent/outbox 状态，不依赖内存任务句柄。现有 `behavior_states` 继续作为额度和冷却的聚合源。

### 5.5 `ReplyBatch`

建议为 `FrozenBatch` 建立最小持久化实体：批次 ID、起止会话序号、消息 ID 列表、触发事件 UUID、上下文快照 hash、状态和租约。第一阶段可只保存消息 ID/序号和状态，不保存完整 prompt 正文。

## 6. 分阶段迁移计划

### 阶段 0：基线和可观测性

- 目标：让现有行为可以回放和定位，不改变业务语义。
- 修改范围：`context_assembler.rs`、`reply_processor.rs`、日志 DTO、测试 fixture。
- 产物：`PromptTrace`、稳定 block ID、decision/outbox attempt trace、状态转换审计事件。
- 验收：同一输入和持久状态生成相同的规则决策、状态 patch、PromptPlan hash；模型失败时 trace 仍落库或可从事件恢复。

### 阶段 1：提示词计划组件

- 目标：把角色定义、状态、关系、记忆、Lorebook、历史和当前轮明确表示为有序 block。
- 修改范围：`ContextBuilder`/`ContextAssembler`，不改 Provider 协议。
- 兼容：`SystemPromptComponents::render` 保留为最终渲染层。
- 验收：顺序、重复当前消息、角色 assistant attribution、预算降级和敏感来源隐藏均有单测/集成测试。

### 阶段 2：记忆声明和冲突修订

- 目标：完成候选→正式→superseded 的可审计状态机。
- 修改范围：`domain/memory.rs`、`memory_service.rs`、`memory_maintenance.rs`、memory repository/migrations。
- 先做：同 claim key 的 replace/append/no_change；旧来源保留；重复事件和重复审核幂等。
- 后做：摘要/压缩 worker，限制每次预算，失败可重试，不改变来源消息。
- 验收：冲突输入、重复输入、审核失败、重启扫描、摘要失败和恢复全部可回放。

### 阶段 3：情绪衰减和状态轨迹

- 目标：在现有 mood/energy/stress 之上增加可选 affect vector 和离线衰减。
- 修改范围：`domain/emotion.rs`、`mood_service.rs`、新纯函数模块、状态仓储和后台 tick。
- 兼容：默认维度映射到现有字段，旧数据库无需一次性重写。
- 验收：相同时间戳和状态快照得到相同结果；离线一小时/一天的衰减可预测；重复 tick 不重复应用；异常时间和时钟回拨有定义。

### 阶段 4：主动意图与回复批次持久化

- 目标：让主动行为取消、恢复和批次重启可证明。
- 修改范围：`proactive.rs`、`reply_processor.rs`、outbox/reply task migrations。
- 验收：主动任务可取消；绑定切换/会话删除 fence 生效；重启后只恢复定义为可恢复的 intent；同一批次不重复生成或发送。

### 阶段 5：真实环境验收

- 目标：隔离配置下验证 OneBot、外部模型、长运行和重启。
- 范围：不改变核心代码，使用 mock/测试账号/临时数据目录和受限日志。
- 验收：真实消息链、模型 429/5xx/超时、平台拒绝、unknown 核查、重启恢复、24 小时后台节流和资源增长。

## 7. 测试与验收方案

| 场景 | 测试层 | 必要断言 |
| --- | --- | --- |
| 相同输入+相同持久状态 | 领域纯函数/决策集成 | 状态 patch、规则决策、PromptPlan 顺序和 hash 相同；随机性必须由输入快照显式控制。 |
| 记忆提取失败 | 回复处理集成 | 对话消息、Outbox、reply task 已保存；记忆失败只产生可重试维护结果，不回滚已接受对话。 |
| 同一事件重复处理 | SQLite 集成 | 候选、关系交互、Outbox 和回复任务均不重复创建；第二次处理返回幂等结果。 |
| 旧记忆与新信息冲突 | 记忆修订集成 | 旧来源保留为 superseded；新记录指向来源和 reason；召回只返回当前有效 claim。 |
| 状态影响提示词 | ContextAssembler 测试 | 改变 mood/energy/stress/relationship 后对应 block 变化，且 trace 标记来源。 |
| 提示词顺序可追踪 | PromptPlan golden test | block ID、role、position、token、裁剪原因稳定；重新组装可复现。 |
| 主动联系频率和取消 | proactive + SQLite | 冷却/每日额度原子；取消后不入队；重启不丢失已预留但未完成的定义状态。 |
| 重启恢复 | 进程/仓储故障注入 | 角色状态、记忆、Outbox、reply task、intent 按状态机恢复；未知发送不自动重复。 |
| 模型失败 | Provider mock + Outbox | 4xx/协议错误不重试；429/5xx/网络错误按配置重试；不会重复回复或丢失持久状态。 |
| 记忆压缩失败 | maintenance worker | 原始消息和旧记忆不变；压缩任务留 pending/retry 信息；成功后新摘要带来源。 |
| 跨会话隐私 | 回复级黑盒测试 | 数据库过滤和模型输出都不能泄露来源会话、内部 ID 或原始群聊。 |

当前已重新执行的基线：

- `cargo test --locked --lib --offline -- --test-threads=1`：512 passed，0 failed，15 ignored。
- `cargo test --locked --lib infrastructure::plugin::transport::tests --offline -- --ignored --test-threads=1`：15 passed，0 failed。
- `cargo test --locked --all-targets --offline --no-run`：编译通过。

这些结果证明当前测试场景通过；不证明真实 QQ、真实外部 LLM、长期运行或长期人格一致性。

## 8. 许可证与依赖风险清单

| 项目 | 当前许可证 | 可用范围 | 风险控制 |
| --- | --- | --- | --- |
| `yomua-bot` | MIT（仓库 `LICENSE`、`Cargo.toml`） | 可继续在 MIT 下开发和发布。 | 继续检查新增 Rust 依赖的许可证，尤其是网络/模型 SDK。 |
| companion-emergence | MIT（当前主分支 LICENSE） | 可研究并在独立评估后复用兼容代码；本任务只抽象设计。 | 逐文件检查第三方模型、角色卡、prompt 文本和依赖；规格 Draft 不等于实现承诺。 |
| SillyTavern | AGPL-3.0 | 本任务仅研究 prompt slot/order/World Info 概念。 | 不复制源码、默认 prompt、角色素材或实现细节；若未来链接/改造其代码，先做 AGPL 兼容评估。 |
| eros-engine | AGPL-3.0-only | 本任务仅研究 PDE、typed stream、post-process、task routing。 | 不复制 Rust 实现、Provider body 规则或文档片段；任何复用需单独法律评估。 |
| 第三方角色卡/提示词/模型 | 可能各自独立授权 | 当前角色卡和本地配置不应进入提交。 | 按项目 AGENTS 规则检查 `assets/characters/`、配置、运行数据和日志；发布前做 SPDX/NOTICE 清单。 |

## 结论与停止点

当前项目已经具备可继续演进的消息、Outbox、状态、关系、记忆候选和模型路由骨架，不需要以参考项目为理由进行整体重写。最高优先级是补齐可追踪提示词计划、记忆冲突/压缩状态机和可恢复的主动意图/回复批次；情绪多维衰减属于后续增强，应建立在这些可观测边界之上。

本报告完成后停止，等待用户审阅和批准；未开始任何代码迁移。

## 参考来源

- companion-emergence：<https://github.com/hanamorix/companion-emergence/tree/10856ab9a9f6ba26e740f2b5196a1e2c7dc6b636>
- companion 规格：<https://github.com/hanamorix/companion-emergence/blob/10856ab9a9f6ba26e740f2b5196a1e2c7dc6b636/docs/source-spec/2026-04-21-framework-rebuild-design.md>
- SillyTavern preset：<https://github.com/SillyTavern/SillyTavern/blob/06bde939fb1e9c4c8d8641d810f0a916b5bce127/default/content/presets/openai/Default.json>
- SillyTavern Prompt Manager：<https://docs.sillytavern.app/usage/prompts/prompt-manager/>
- SillyTavern World Info：<https://docs.sillytavern.app/usage/core-concepts/worldinfo/>
- eros-engine：<https://github.com/etherfunlab/eros-engine/tree/9715af2ce7d8524ca5ce8771059d436c31951977>
- eros model config：<https://github.com/etherfunlab/eros-engine/blob/9715af2ce7d8524ca5ce8771059d436c31951977/docs/model-config.md>
