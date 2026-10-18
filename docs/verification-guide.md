# 功能验收说明

本文档按当前源码状态编写，用于逐项检查功能、模块和实际行为。项目仍处于开发中；通过自动化测试不代表真实 QQ、外部模型或长期运行已经验收。

## 建议顺序

1. 使用临时数据目录启动程序，确认配置加载、数据库迁移和控制 socket 创建。
2. 使用 `yomua-bot-ctl help` 核对公开命令；命令使用空格分段，例如 `character list`、`character state get`。
3. 先验证角色绑定、停用、静默和会话删除，再验证消息链路。
4. 使用模拟 OneBot 和本地模型验证消息去重、回复任务状态和失败重试。
5. 最后再使用真实 QQ 或外部模型做人工体验验收。

## 已有证据

- `cargo check --locked --all-targets --offline`：通过。
- `cargo test --locked --all-targets --no-run --offline`：通过编译检查。
- `cargo fmt --all -- --check`：通过。
- `cargo test --locked --all-targets`：库 461 个、主程序 5 个、ctl 39 个、alpha 集成 8 个、控制集成 84 个通过；15 个插件 Unix socket 测试按 `#[ignore]` 标记跳过。
- `cargo test --locked --lib infrastructure::plugin::transport::tests --offline -- --ignored --test-threads=1`：15 个插件 Unix socket 测试全部通过。
- 临时 Unix socket 模拟服务配合真实 `yomua-bot-ctl status --raw`：通过，确认发送的是版本化 `ControlEnvelope`，不含旧 `cmd` 字段。
- CLI 解析测试：覆盖一段、二段、三段命令、短别名归一化和危险命令确认。
- SQLite 存储测试：覆盖平台消息去重、稳定用户关系、角色归属历史和记忆可见范围。
- 配置模板验证：首次启动在临时目录生成 `runtime.toml`、`onebot.toml`、`llm.toml`、`decision.toml`；可选 OneBot 字段留空时仍可解析，真实配置密钥未参与验证。

本次核查的基线为：库 458 个通过、主程序 5 个、ctl 39 个、alpha 集成 8 个、控制集成 84 个；另有 15 个插件 Unix socket 测试在普通全目标测试中按 `#[ignore]` 标记跳过，并在本次使用 `--ignored --test-threads=1` 显式运行通过。

## 已实现的重点行为

- 入站消息保存作者类型、稳定用户标识、提及、引用、绑定和绑定版本等 metadata，并通过同一事务创建 `MessageAccepted` Outbox 事件；适配器不再为同一消息额外发布第二条 `MessageReceived` 事件。
- 平台消息身份用于去重；相同文本的再次发送不会被当成重复投递。
- 角色真正发送的消息才会在上下文中使用 `assistant`，用户和其他 Bot 保留成员身份。
- 角色切换或停用后，发送前会再次检查绑定和启用状态。
- 会话删除与已领取任务使用同一会话 fencing；删除会等待当前任务完成，失效的系统指令和回复事件会被确认清理，不再产生副作用。
- 记忆候选以 `candidate` 状态保存，普通入站消息按来源消息幂等生成；候选不会直接进入正式可见记忆或默认 ctl 列表；事实/关系默认共享，具体经历默认保留会话范围。
- 共享记忆可以在其他会话进入角色的内部上下文，但回复不得主动暴露来源会话、原始群聊或内部记忆标识；需要用带来源的提示词和真实聊天场景单独验收这一点。
- 跨会话隐私策略可以这样验收：先执行 `memory sharing request <character-id> <user-id> --reason "用户请求"`，确认跨会话仍可读；再执行 `memory sharing confirm <character-id> <user-id> --confirm`，确认其他会话不再读到该用户的共享记忆，而来源会话仍可读；最后执行 `memory sharing cancel <character-id> <user-id> --confirm`，确认跨会话恢复。三条命令只返回状态和计数，不返回记忆正文。
- ctl 的 `memory list` 默认只返回记忆摘要和不可逆指纹；只有显式 `--raw` 才读取原文。
- `memory update` 不默认返回记忆原文；它先持久化 pending 审计，再把自然语言意图交给配置的 decision LLM，程序只接受受限的 `replace`、`append`、`no_change` 建议并做并发版本检查。未配置可用模型时明确返回 `MEMORY_EDIT_UNAVAILABLE`；pending/failed/rejected/applied 状态保留在记忆元数据中。
- `user delete <ID> --confirm` 删除与用户关联的普通/向量记忆及关系，将历史参与者匿名化并清除消息/outbox 元数据、私聊路由和用户黑名单中的旧 QQ 号和昵称；历史消息正文及消息引用保留。测试同时验证非目标用户数据不变、私聊主动发言关闭、外键无孤儿和数据库失败时完整回滚。
- ctl 服务端和客户端使用同一套 canonical 命令定义；危险命令需要 `--confirm`，服务端也会再次校验。
- ctl REPL 支持引号和反斜杠转义，中文长参数不会因空格被截断；命令路径按注册表最长匹配。
- 没有可用对话模型时保留原消息，不发送伪造回复。
- 黑名单会在创建会话/参与者之前拦截入站消息；黑名单仓储故障时按拒绝处理。
- 非文本消息保留媒体类型、平台字段和引用关系；当前没有识别能力时，角色看到明确的不可理解提示。
- 关系熟悉度的交互计数绑定来源消息 ID，重复投递或 Outbox 重试只计一次。
- 主动发言每日额度持久化在 `behavior_states`，重启不会清空当日计数；静默时段当前仍按 UTC 解释。
- 未知发送结果不会被普通 Outbox 重试自动补发；`reply unknown check` 在平台明确确认未找到时自动恢复回复任务和原始事件，网络错误或缺少平台消息 ID 时仍需人工核查；手动 `reply unknown retry` 仍必须带 `--confirm`。
- 统一关停在配置时限内等待任务；超时会取消剩余任务、清理 OneBot/插件句柄并关闭数据库，持久化任务可在下次启动恢复。
- 备用模型链应验证共享 `budget_tokens`：主模型遇到可重试错误后，备用模型只能使用剩余输出额度；极端控制参数应返回业务错误而不关闭 socket，插件异常退出时关停应及时结束等待。

## 模块核对清单

按下面顺序检查源码和行为，括号内是主要入口：

- 消息接入与 OneBot 转换：`src/adapters/onebot/conversion.rs`、`src/adapters/onebot/mod.rs`。核对 CQ 文本、提及目标、引用、媒体摘要、平台消息 ID 和自身回声。
- 入库与 Outbox：`src/application/message_acceptance.rs`、`src/infrastructure/storage/repository/message_repository.rs`、`src/infrastructure/storage/repository/outbox_repository.rs`。核对同事务写入、平台 ID 去重、会话序号、租约和失败后继续处理。
- 回复任务租约：运行 `reply_delivery_result_cannot_be_overwritten_after_lease_changes`，确认旧 worker 的生成状态、失败回退和发送结果写入会被拒绝，新租约可以接管过期的 `generating` 任务。
- 回复处理：`src/application/reply_processor.rs`。核对角色绑定/停用、连续消息批次、当前消息只出现一次、生成落库、发送前复核和 unknown 状态。
- 上下文：`src/application/context_assembler.rs`、`src/application/character_history.rs`。核对硬约束、角色卡、动态状态、可见记忆、历史、当前批次和预算降级顺序。
- 参与决策：`src/application/decision_orchestrator.rs`、`src/application/decision_pipeline.rs`、`src/application/behavior_engine.rs`。核对明确 @、普通群消息、静默和决策模型失败回退。
- LLM 备用链：运行 `capabilities_for_uses_smallest_model_in_fallback_chain` 与 `capabilities_for_ignores_fallbacks_beyond_attempt_limit`，确认上下文能力只覆盖本次可能尝试的模型。
- 记忆与关系：`src/application/memory_service.rs`、`src/application/cognition_driver.rs`、`src/infrastructure/storage/repository/memory_repository.rs`、`relationship_repository.rs`。核对候选/正式记忆、会话可见范围、稳定用户身份和幂等交互。
- 控制协议与 ctl：`src/application/control_schema.rs`、`src/application/control.rs`、`src/bin/yomua-bot-ctl.rs`。核对一段、二段、三段命令，帮助文本，中文错误，危险命令确认和 unknown 查询/重试。
- 控制帮助别名：运行 `test_help_lists_all_builtin_commands`，确认服务端 `help` 响应包含 `status` 的 `s` 和 `character list` 的 `c list` / `char list`，且与 ctl 本地帮助一致。
- 插件：`src/infrastructure/plugin/manifest.rs`、`registry.rs`、`transport.rs`、`supervisor.rs`。核对握手归属、订阅权限、失败隔离、任务退出和核心状态写保护。
- 生命周期与配置：`src/main.rs`、`src/application/config.rs`、`src/application/runtime.rs`。核对初始化、迁移、热重载、关停超时和 socket 清理。
- 向量配置：在隔离配置中为 `[providers.<name>]` 设置 `embedding_model`，确认配置解析保留模型名；未设置时确认认知任务仍能保存普通记忆而不调用向量接口。
- 语音接入（尚未实现）：当前文本链路不能直接承载实时音频；后续应新增独立 VoiceGateway，把最终转写写入本地 Conversation/Participant/Message，再复用记忆和上下文。外部 voice session/call_id 只作为关联键，QQ/NapCat 音频发送需单独验证。

每项记录“已由自动化验证”“仅静态确认”或“尚未验证”，不要把单元测试通过直接等同于真实 QQ 体验通过。

## 当前限制和未完成项

- 连续消息在回复任务边界冻结：普通消息等待 1.5 秒，@ 消息等待 0.5 秒；从当前消息的会话序号之后读取候选，避免旧历史挤出新消息；原消息逐条保存，生成期间新消息留给下一轮。当前没有独立持久化的批次实体，仍需用真实 OneBot 流量验证时序和体验。
- `unknown` 发送结果记录为终态并等待核查。先执行 `reply unknown list`；有平台消息 ID 时再执行 `reply unknown check <消息ID>`。平台明确返回未找到时会自动重新排队；没有平台消息 ID 或查询失败都不能当作未发送证明，此时才使用 `reply unknown retry <消息ID> --confirm`。
- 记忆候选会由后台 `decision` 模型审核，程序仍校验来源消息、用户、会话、类型和置信度；模型不可用或意见不合规时保留候选。自然语言修改请求具备后台恢复；矛盾记忆的系统化修订仍未完成。当前 ctl 提供摘要查询、显式原文读取、删除、修改请求登记，以及跨会话隐私策略的请求、确认和取消。
- 世界书暂不参与上下文组装。
- 决策模型通过独立 `decision.toml` 接入，支持主模型、备用链、调用次数和本次 token 预算；具体开源模型、部署位置和价格仍需填写后按真实摘要测量。
- 当前 8GB M2 的桌面负载不适合常驻本地决策模型；后续应优先采用远程或局域网 sidecar，本地实验限制为单模型、短上下文、单并发和关闭缓存。
- 严格 Clippy 已通过；向量记忆接口明确为可选能力，当前对话不默认调用；消息/认知层装配已完成结构化整理。
- 常驻任务的统一关停和配置热重载仍需真实进程级验收；插件连接失败、握手、订阅和清理的 socket 测试已在本机显式通过。
- 测试环境未装配真实配置热重载通道，因此 reload 测试只验证明确返回 `RELOAD_UNAVAILABLE` 或完整热重载结果；真实常驻进程仍需单独验收。
- 真实 QQ 消息收发、真实外部 LLM、长时间运行和重启恢复尚未在本轮作为证据确认。

## 建议命令

```text
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo test --locked --lib --offline -- --test-threads=1
cargo test --locked --all-targets --offline
cargo clippy --locked --offline --all-targets --all-features -- -D warnings
```

执行测试时使用临时数据目录和占位凭据；不要把本地 `data/`、NapCat 数据、角色卡素材或真实密钥纳入测试结果。

## 后续源码整顿顺序

1. 评估是否需要持久化回复批次，并补齐批次冻结与进程重启之间的故障注入测试。
2. 完成矛盾记忆的来源修订与冲突处理，并在隔离模型环境验证候选审核体验。
3. 补齐配置热重载、统一关停和插件进程级测试。
4. 将结构化控制协议纳入真实 ctl 子进程测试，再提升 CI 门槛。
5. 为决策模型增加分级会议、权重、超时和预算配置，并在本地模型环境验收。
