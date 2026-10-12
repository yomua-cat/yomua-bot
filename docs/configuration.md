# 配置参考

配置解析见[配置模块](../src/application/config.rs)，装配见[主程序](../src/main.rs)，OneBot 字段见[适配器](../src/adapters/onebot/mod.rs)。示例值用于说明字段，需要按部署环境替换。

## 路径与首次启动

`--config-dir` 指定配置文件目录，默认进程工作目录；兼容把配置目录作为位置参数。该目录包含 `runtime.toml`、`onebot.toml` 和 `llm.toml`。

`runtime.toml` 不存在时，常驻入口尝试生成三个模板，然后以退出码 1 退出。模板中的 `admin_users = []` 必须修改。此流程只由缺失 `runtime.toml` 触发；其已存在时，不会自动补齐其他模板。OneBot 和 LLM 文件缺失时分别使用默认配置和关闭 LLM 的默认值。

文件存在时直接反序列化，缺少必要字段会报错，不是按字段逐项合并默认值。`llm.toml` 应保留 `[options]`，即使关闭 LLM 或使用多模型格式。

`data_dir` 和 `plugins_dir` 的相对路径基于进程工作目录，不基于配置目录。部署时建议使用绝对路径。控制客户端不读取 TOML，需要独立传入 `--data-dir` 或 `--socket`。

## runtime.toml

```toml
# 替换为独立的运行数据目录。
data_dir = "/absolute/path/to/yomua-data"
log_level = "info"
shutdown_timeout_secs = 10

# 替换为实际管理员的外部用户 ID，至少一个。
admin_users = ["REPLACE_WITH_ADMIN_ID"]

# 可选设置。
# broadcast_capacity = 256
# plugins_dir = "/absolute/path/to/plugins"
```

| 字段 | 实际行为 |
| --- | --- |
| `data_dir` | SQLite、控制及插件 socket 的目录；缺失整个文件时默认 `data` |
| `log_level` | `trace/debug/info/warn/error`；初始化时 `RUST_LOG` 优先 |
| `shutdown_timeout_secs` | 必须大于 0；尚未用于限制整个运行时关停时间 |
| `admin_users` | 必须非空，否则常驻启动校验失败；不用于控制 socket 身份认证 |
| `plugins_dir` | 省略时不启动插件；不存在或无效清单会记录日志 |
| `broadcast_capacity` | 可选正数；默认 256；广播消费滞后可能丢事件 |
| `default_character_id` | 可解析，但主程序没有接入自动绑定逻辑；使用显式导入绑定 |

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
[options]
```

LLM 关闭时使用确定性回复，不发起模型请求。回复仍受角色绑定、参与者记录与行为判断约束。

## llm.toml：单模型

```toml
enabled = true
provider = "openai-compatible"

[options]
base_url = "https://llm.example.invalid/v1"
api_key = "REPLACE_WITH_LLM_KEY"
model = "REPLACE_WITH_MODEL_NAME"
timeout_secs = 60
# 示例预算值，必须按模型实际能力修改。
context_window = 8192
max_output_tokens = 1024
output_reservation = 1024
```

`base_url` 是兼容 API 的根地址，Provider 会拼接 Chat Completions 路径。示例域名不可用于请求。省略时实现默认使用本地地址 `http://127.0.0.1:11434/v1`；模型名依次取 `options.model`、`provider` 或 `default-model`。建议显式填写，避免错误兜底。

`context_window` 必须已知才能组装对话；`output_reservation` 默认 512，必须留出可用输入空间。服务端可用性、凭据及上下文窗口真实性需要单独验证。

## llm.toml：多模型路由

```toml
enabled = true

# 当前对话预算仍从这里读取。
[options]
context_window = 8192
max_output_tokens = 1024
output_reservation = 1024

[providers.primary]
provider_type = "openai-compatible"
base_url = "https://llm.example.invalid/v1"
api_key = "REPLACE_WITH_LLM_KEY"
timeout_secs = 60

[model_instances.dialogue]
provider = "primary"
model = "REPLACE_WITH_MODEL_NAME"
context_window = 8192
max_output_tokens = 1024

[roles.conversation]
model_instance = "dialogue"

[roles.cognition]
model_instance = "dialogue"
```

Provider 注册连接参数，model instance 指定服务、模型及能力，role 引用 model instance。`providers` 非空时主程序选择多模型模式，要求非空 `roles`，并验证引用存在。对话和插件聊天使用 `conversation`；后台认知使用 `cognition`。只配置 `decision` 不能让实际对话工作，且当前主程序没有接入决策 LLM。

model instance 中的能力字段不自动替代 `[options]` 的上下文预算；两处应保持与实际模型一致，不能据此认为已实现按模型动态预算。

## 修改与重载

`reload-config` 当前只读取并校验 `runtime.toml`，返回部分配置值。它不会重建 Provider、适配器、日志过滤器或修改运行中配置，也不读取另外两个文件。需要生效的修改应通过受控关停和重启应用。

配置文件可能包含令牌和密钥，全部 TOML 默认忽略，只有根 `Cargo.toml` 入库。提交问题时使用占位值，不粘贴真实配置。
