# 项目现状与后续整顿

本文记录截至 **2026-10-12** 的仓库检查结果。结论按证据分为：

- **已验证**：在当前环境或隔离临时目录实际执行并观察到。
- **静态确认**：由当前源码、配置或接口调用关系直接确认。
- **未验证**：需要真实 OneBot、模型服务、Linux 部署或更完整故障环境，当前没有足够证据。

## 当前基线

| 检查 | 结果 | 证据与边界 |
| --- | --- | --- |
| `cargo build --locked --lib --bins --examples` | 已验证通过 | 当前锁文件和本机工具链；存在编译警告 |
| `cargo check --locked --lib --bins --examples` | 已验证通过 | 无编译错误 |
| `cargo test --locked --lib --bins` | 已验证通过 | 389 个库测试、8 个主程序测试、18 个控制客户端测试通过；需要允许 Unix socket 与子进程 |
| `cargo test --locked --all-targets` | 已验证失败 | 两个外部集成测试仍调用旧的 `OneBotAdapterImpl::with_connector` 接口，未进入测试执行 |
| `cargo fmt --all -- --check` | 已验证失败 | 当前源码已有格式差异；本次保留源码不动 |
| `cargo clippy --locked --all-targets -- -D warnings` | 已验证失败 | 普通 Clippy 可完成，但存在未使用项、冗余闭包和复杂度警告 |
| 文档配置、插件清单与本地链接 | 已验证通过 | TOML 解析、工作流 YAML/shell 语法及 11 份 Markdown 本地文件链接检查通过 |
| 隔离启动、导入、控制查询、重载语义和关停 | 已验证通过 | LLM 关闭、OneBot 指向不可达本地端口，数据写入临时目录 |
| 真实 QQ 消息收发 | 未验证 | 当前没有把真实消息注入 OneBot 的验收证据 |
| 外部 LLM 请求和模型能力 | 未验证 | 没有使用真实凭据或外部服务 |
| Linux 长期运行、升级回滚和故障恢复 | 未验证 | 尚无生产环境演练 |

本机使用 Rust `1.98.0`。依赖解析结果中已有包声明 Rust `1.88`，因此 Cargo 清单不再承诺 Rust `1.75` 最低版本；后续应在目标 Linux toolchain 上确定并记录 MSRV。

## 已确认的主要问题

### P0：消息可靠性链路需要整顿

- `[MessageAcceptanceService](../src/application/message_acceptance.rs)` 接收入站消息时构造的临时 Outbox payload 使用 `message_id = 0`、`conversation_sequence = 0`、`is_mentioned = false`；真正的消息 ID 和序号只在事务返回后生成，payload 没有回写。静态确认。
- OneBot 入站事件在接受之后发布的 `MessageReceivedEvent` 仍然带有真实消息 ID、序号和提及状态，但当前 Outbox 消费路径从 payload 重新构造事件，因此两条数据来源不一致。静态确认。
- ReplyProcessor 发送失败只记录错误，没有独立的出站 Outbox 或发送重试路径。静态确认。

### P1：角色与过滤语义需要统一

- `admin_users` 用于系统指令权限；控制 Unix socket 没有应用层身份认证，能访问 socket 的本机进程可以执行管理和删除命令。静态确认。
- 黑名单仓储和控制命令已存在，但入站适配器没有在消息接受前调用黑名单服务。黑名单目前不能作为消息拦截保证。静态确认。
- `default_character_id` 已进入运行配置和模板，但主程序没有接入无绑定会话的自动绑定逻辑。静态确认。
- 接收时把当前绑定角色写入消息；消费时 ReplyProcessor 重新枚举会话绑定。切换角色与延迟消费之间的归属语义尚未统一。静态确认。
- `CharacterRuntime` 缓存角色状态；控制服务直接通过仓储更新状态，缓存失效和同步策略尚未统一。静态确认。
- 回复处理需要会话中的 Bot 角色参与者来识别自身消息。`import-card --participant` 创建的是用户参与者，完整 Bot 参与者生命周期尚未在文档化流程中解决。静态确认。

### P1：后台生命周期和配置生效需要整顿

- `ProactiveDriver::run` 是固定 tick 循环，没有接收 shutdown receiver；关停时未统一等待它退出。静态确认。
- `CognitionDriver` 有 `stop()`，但主程序 spawn 后没有保存句柄或在关停流程中调用。静态确认。
- `shutdown_timeout_secs` 校验为正数，但没有限制整个关停流程。静态确认。
- 主程序等待 watch 关停信号；源码没有注册 SIGINT/SIGTERM 处理。终端提示 Ctrl+C 与实际关停语义不一致。静态确认。
- `reload-config` 当前只读取和校验 `runtime.toml`，返回部分值，不重建 Provider、OneBot、日志过滤器或后台任务，也不读取 `onebot.toml` 和 `llm.toml`。隔离运行已验证。

### P2：工具和工程质量缺口

- `yomua-bot-ctl` 的路径解析最多收集两个词；服务端已注册的 `character card ...`、`character state ...` 三段式命令无法从 CLI 到达。`session list` 也与实际 `conversation list` 路径不一致。隔离运行已验证。
- 控制客户端一次读取响应，分片或大响应尚未验收。静态确认。
- 全目标测试的两份集成测试与当前适配器构造函数签名漂移。构建时静态确认。
- 普通 Clippy 有 dead code、unused import、复杂度及冗余代码警告；格式检查未通过。已验证。
- 多模型配置中的 model instance 能力字段不会自动替代 `[options]` 的上下文预算；主程序没有接入决策 LLM pipeline，也没有给 ContextBuilder 注入 embedding scheduler。静态确认。

## 后续整顿顺序

1. 先修复 Outbox payload 的权威字段和入站/消费事件模型，补充真实的消息链路测试。
2. 统一角色绑定、Bot 参与者、缓存失效、切换角色及黑名单拦截的语义，再修复相关控制命令。
3. 建立统一后台任务生命周期：shutdown receiver、JoinHandle、超时和信号处理一起验收。
4. 修复集成测试 API 漂移、CLI 路由和控制响应读取，再把格式与严格 Clippy 提升为 CI 必过。
5. 在隔离 Linux 环境验收 OneBot、LLM、插件、升级迁移和恢复流程后，才重新评估生产部署等级。

本页只记录当前源码证据和验证缺口。修复其中问题时，应同步更新架构、配置、部署和 CLI 文档，并重新运行 CI 全部检查。
