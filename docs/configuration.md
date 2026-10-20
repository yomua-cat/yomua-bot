# 配置参考

配置解析见[配置模块](../src/application/config.rs)，装配见[主程序](../src/main.rs)，OneBot 字段见[适配器](../src/adapters/onebot/mod.rs)。示例值用于说明字段，需要按部署环境替换。

## 路径与首次启动

`--config-dir` 指定配置文件目录，省略时使用启动目录下的 `config/`。配置目录包含 `runtime.toml`、`onebot.toml`、`llm.toml` 和 `decision.toml`；裸位置参数会被拒绝。部署时建议把服务工作目录设为项目/服务根目录，或者显式传入绝对路径。

任一配置文件不存在时，常驻入口会在配置目录补齐缺失模板，然后以退出码 1 退出。模板中的 `admin_users = []` 必须修改；启用 OneBot 鉴权或 LLM 时也必须替换占位字段。配置文件全部存在后才会进行完整校验，不会因为缺少某一份文件而悄悄使用不完整默认值。

文件存在时直接反序列化，缺少必要字段会报错，不是按字段逐项合并默认值。`llm.toml` 统一使用 Provider、Model Instance 和 Role 三层结构；旧版顶层 `provider` 与 `[options]` 会被拒绝，需要迁移到新结构。

`data_dir` 和 `plugins_dir` 的相对路径基于进程工作目录，不基于配置目录。部署时建议使用绝对路径。控制客户端不读取 TOML，需要独立传入 `--data-dir` 或 `--socket`。

## 绑定级普通回复限额

普通回复限额属于 `CharacterBinding` 策略，不写入 TOML。绑定可以设置以下字段：

| 字段 | 默认值 | 实际行为 |
| --- | --- | --- |
| `per_user_per_minute` | `null` | 同一会话内同一参与者每分钟最多触发的回复数；`null` 表示关闭 |
| `per_session_per_hour` | `null` | 同一会话每小时最多触发的回复数；`null` 表示关闭 |
| `mention_exempt` | `false` | 被 @ 的消息是否完全豁免限额；默认不豁免 |
| `mention_multiplier` | `4` | 被 @ 且未豁免时，两个限额使用的倍率；用于给明确召回留出更宽窗口 |

两个窗口都按滑动时间统计。生产运行把每次额度预留写入 SQLite，重启不会清空最近窗口；过期事件会在同一绑定的下一次检查中清理。没有参与者身份的消息不能套用每用户限额，但仍受会话限额保护。默认两个限额均关闭，因此旧绑定行为不变。

根目录中可能已有旧的本地 `runtime.toml`、`onebot.toml`、`llm.toml`。它们不会被删除或覆盖；首次发布的标准位置是 `config/`，需要继续使用旧位置时请显式传入 `--config-dir .`。

## runtime.toml

```toml
# 替换为独立的运行数据目录。
data_dir = "/absolute/path/to/yomua-data"
log_level = "info"
shutdown_timeout_secs = 10

# 可选：所有模型 HTTP 请求共用的代理；删除注释表示直连。
# 含账号密码时只在本地配置中填写，不要提交。
# proxy = "http://user:password@127.0.0.1:7890"

# 替换为实际管理员的外部用户 ID，至少一个。
admin_users = ["REPLACE_WITH_ADMIN_ID"]

# 可选设置。
# broadcast_capacity = 256
# plugins_dir = "/absolute/path/to/plugins"
# 主动行为和静默时段使用的 UTC 偏移，默认北京时间。
timezone = "+8"
```

| 字段 | 实际行为 |
| --- | --- |
| `data_dir` | SQLite、控制及插件 socket 的目录；缺失整个文件时默认 `data` |
| `log_level` | `trace/debug/info/warn/error`；初始化时 `RUST_LOG` 优先 |
| `shutdown_timeout_secs` | 必须大于 0；限制适配器、后台任务、插件和存储关闭的整体等待时间，超时后取消剩余任务 |
| `admin_users` | 必须非空，否则常驻启动校验失败；不用于控制 socket 身份认证 |
| `plugins_dir` | 省略时不启动插件；不存在或无效清单会记录日志 |
| `broadcast_capacity` | 可选正数；默认 256；广播消费滞后可能丢事件 |
| `default_character_id` | 可解析，但主程序没有接入自动绑定逻辑；使用显式导入绑定 |
| `timezone` | 主动行为、静默时段和每日主动额度使用的 UTC 小时偏移；格式为 `+?` 或 `-?`，默认 `+8` |
| `proxy` | 可选的模型出站 HTTP(S) 代理；URL 格式错误会阻止启动，修改后需要重启 |

`runtime.proxy` 由主程序显式注入 TypeSafe 和 OpenAI-compatible 的专用 `reqwest` 客户端，覆盖模型生成、健康检查和 embedding 请求；未配置时维持直连。代理仅作用于这些模型 HTTP 请求。OneBot/NapCat WebSocket 使用独立的 `tokio-tungstenite` 连接器，始终直连配置的本地或局域网地址，不读取此代理；插件 Unix socket 与其他非模型流量也不受影响。代理凭据支持写在 URL 中，程序不会将代理 URL 或客户端网络错误写入日志/错误摘要。

### 日志过滤

默认按 `runtime.log_level` 过滤，日志输出到标准错误。启动时若 `RUST_LOG` 是有效的 EnvFilter，则整条过滤规则优先于 `log_level`；配置热重载只更新 `log_level` 级别过滤器，不会覆盖启动时的 `RUST_LOG` 规则。可用 target 包括 `runtime`、`adapter`、`message`、`outbox`、`llm`、`decision`、`control` 和 `cli`；未显式设置 target 的 tracing 事件仍使用 Rust 模块路径。

真实 QQ 测试建议先用 `RUST_LOG=info` 保留状态迁移与失败摘要；定位单条链路时用 `RUST_LOG=info,outbox=debug,llm=debug,adapter=debug`。`trace` 和全局 `debug` 会增加存储、插件及请求级日志量，建议仅短时启用。日志字段包含内部会话/消息 ID、平台消息 ID、事件 UUID、角色和模型路由信息，应按运行数据权限保护；不记录聊天正文、API key 或模型服务错误响应正文。

## onebot.toml

```toml
websocket_url = "ws://127.0.0.1:3001"
access_token = "REPLACE_WITH_ONEBOT_TOKEN"
# bot_self_id = 123456789
reconnect_interval_secs = 1
max_reconnect_interval_secs = 30
heartbeat_interval_secs = 30
action_timeout_secs = 10
```

令牌与 OneBot 服务端保持一致；不认证时省略 `access_token`。`bot_self_id` 用于回复处理中的自身参与者匹配，不能替代角色参与者记录的创建。

未提供整个文件时，最大重连间隔默认 60 秒；程序生成的模板使用 30 秒。重连、心跳和动作超时字段目前缺少完整启动校验，使用正数。仅配置文件可解析不代表连接或认证成功。

## llm.toml：关闭模型

```toml
enabled = false
```

LLM 关闭时保留入站消息，但不会伪造固定回复，也不发起模型请求。回复仍受角色绑定、参与者记录与行为判断约束。

## llm.toml：Provider 与对话模型

```toml
enabled = true

[providers.primary]
provider_type = "openai-compatible"
base_url = "https://llm.example.invalid/v1"
api_key = "REPLACE_WITH_LLM_KEY"
# 可选；配置后认知任务才会尝试生成向量
embedding_model = "REPLACE_WITH_EMBEDDING_MODEL"
timeout_secs = 60

[providers.backup]
provider_type = "openai-compatible"
base_url = "https://backup.example.invalid/v1"
api_key = "REPLACE_WITH_BACKUP_KEY"
# 可选；备用 Provider 也可以单独配置向量模型
embedding_model = "REPLACE_WITH_BACKUP_EMBEDDING_MODEL"
timeout_secs = 60

[model_instances.dialogue]
provider = "primary"
model = "REPLACE_WITH_MODEL_NAME"
context_window = 8192
max_output_tokens = 1024

[model_instances.dialogue_backup]
provider = "backup"
model = "REPLACE_WITH_BACKUP_MODEL_NAME"
context_window = 8192
max_output_tokens = 1024

[roles.conversation]
model_instance = "dialogue"
fallback_model_instances = ["dialogue_backup"]
max_attempts = 2
budget_tokens = 4096

[roles.cognition]
model_instance = "dialogue"
```

Provider 登记连接地址、密钥、超时和可选的 `embedding_model`；同一个普通对话 Provider 可以被多个 model instance 复用。普通聊天使用 `provider_type = "openai-compatible"`，Jev 决策 Provider 则单独登记在 `decision.toml`，使用 `provider_type = "typesafe"`；Jev 请求会发送到 TypeSafe System One 的 `/v1/systemone`，不会走 `/chat/completions`。model instance 只填写模型名和能力，URL 不重复写。对话和插件聊天使用 `conversation`；后台认知使用 `cognition`。未配置 `embedding_model` 时，向量增强会跳过，文本生成不受影响。

`fallback_model_instances` 按顺序尝试，`max_attempts` 限制本次请求最多尝试的模型数，`budget_tokens` 是主模型与备用模型共享的本次请求输出 token 上限。调度器会在每次尝试前保守预留本次允许的最大输出；网络失败且没有可用用量数据时，已预留额度不会回收，以避免备用链超过上限。只有网络错误、超时、HTTP 429 或 5xx 会切换备用模型；认证失败、参数错误和响应格式错误会立即返回。每次请求只生成一次，备用模型只在尚未得到有效响应时使用。

Grok 不需要专用代码。把 xAI 的 OpenAI-compatible 地址配置成一个 Provider，再为不同 Grok 模型各建一个 model instance 即可。

每个 model instance 的能力字段用于上下文和输出限制；`budget_tokens`、备用实例和最大尝试数属于对应的 Role。不同任务使用不同模型时，应分别填写各 model instance 的能力，不要把决策模型的窗口当成对话模型窗口。

## decision.toml：独立决策模型

决策模型不与对话模型混在同一个任务表中。决策 Provider、地址和密钥直接登记在本文件的 `[providers.*]`；普通对话和认知 Provider 仍登记在 `llm.toml`。

```toml
enabled = true
max_attempts = 2
budget_tokens = 1024

[providers.jev]
provider_type = "typesafe"
base_url = "https://api.typesafe.ai/v1/systemone"
api_key = "REPLACE_WITH_JEV_API_KEY"
timeout_secs = 60

[primary]
provider = "jev"
model = "jev-latest"
context_window = 8192
max_output_tokens = 256

[[fallbacks]]
provider = "jev_backup"
model = "REPLACE_WITH_DECISION_BACKUP_MODEL"
context_window = 8192
max_output_tokens = 256
```

`decision.toml` 的备用链同样共享本次请求的输出 token 上限，并只在网络错误、超时、HTTP 429、529 或 5xx 时切换。Jev 决策使用同一 `state` 下的多个 typed questions：`action` 用 `choice` 选择动作，`hard_constraints` 用 `noul` 返回硬约束成立概率（至少 0.9 才允许 override），普通回复额外用 `score` 给出延迟变化。程序保留每个问题的原始值、概率、置信度和模型版本，聚合置信度后再执行验证；审计日志只写入问题名/类型、值的形状或长度、概率摘要、模型版本、验证状态和回退原因，不写入完整状态、消息正文或凭据；主动情境不会发送延迟问题，也只接受 `keep` 和 `suggest_ignore`。规则引擎和程序校验仍拥有最终裁决权。

## 修改与重载

`reload-config` 会读取并校验 `runtime.toml`、`onebot.toml`、`llm.toml` 和 `decision.toml` 四份配置。运行时设置会整体替换当前快照，并同步更新日志过滤器；其中 `timezone` 会被后续行为决策和主动额度检查立即读取。数据目录、插件目录、代理、OneBot 连接、LLM Provider/模型、决策模型和 `shutdown_timeout_secs` 等启动装配项会返回 `restart_required`，当前配置保持不变，需要受控关停后重启。关停超时值在启动时被插件监督器等组件读取，不能只更新配置快照。

配置文件可能包含令牌和密钥，全部 TOML 默认忽略，只有根 `Cargo.toml` 入库。提交问题时使用占位值，不粘贴真实配置。
