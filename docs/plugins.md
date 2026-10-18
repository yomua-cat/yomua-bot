# 插件

插件在独立进程中运行，由宿主通过 Unix socket 和 MessagePack 通信。它们拥有运行用户的操作系统权限；进程隔离与 API 权限检查不是操作系统沙箱。只运行可信插件。

## 布局与示例

```text
<plugins_dir>/
└── echo/
    ├── plugin.toml
    └── echo_plugin
```

清单以 Markdown 示例提供，实际 `plugin.toml` 是本地配置：

```toml
name = "echo"
version = "0.1.0"
description = "事件订阅示例"
executable = "echo_plugin"
permissions = []
```

名称必须与子目录一致、非空且不超过 48 个字符；版本、描述必须非空。`executable` 必须是插件目录内的相对路径，不能含 `..`。缺失或无效清单会记日志并跳过。完整规则见[清单解析](../src/infrastructure/plugin/manifest.rs)。

```bash
cargo build --locked --example echo_plugin
mkdir -p /absolute/path/to/plugins/echo
cp target/debug/examples/echo_plugin /absolute/path/to/plugins/echo/echo_plugin
```

在该目录手动创建上面的清单，并在 `runtime.toml` 填写绝对 `plugins_dir`。源码[echo_plugin](../examples/echo_plugin.rs)演示握手、订阅事件和插件私有数据操作，不会自动发送聊天回复。

推荐同时使用绝对 `data_dir` 和 `plugins_dir`。监督器改变子进程工作目录，当前相对路径装配可能导致 socket 或可执行文件位置不正确。Unix socket 路径还受系统长度限制，避免过深目录。

## 启动、重启与关停

宿主先创建 `<data_dir>/plugin-sockets/<name>.sock`，启动可执行文件，并注入 `YOMUA_PLUGIN_SOCKET` 与 `YOMUA_PLUGIN_NAME`。标准输出和标准错误继承到宿主日志。

插件连接后发送 Hello，宿主确认身份、版本及订阅后回复 HelloAck。默认握手超时 10 秒、连续重启预算 3 次、重启退避从 500 毫秒到 8 秒、稳定窗口 60 秒。关停通知后默认等待 10 秒，超时终止子进程；这些是[SupervisorConfig](../src/infrastructure/plugin/supervisor.rs)的代码默认值，不是运行时 TOML 设置。

## IPC 协议

每帧为 **4 字节大端长度 + MessagePack 对象**，最大帧体 16 MiB。对象含 `protocol_version`、`kind` 和 `payload`；`kind` 可以是 `hello`、`hello_ack`、`request`、`response` 或 `event`。实现见[协议模块](../src/infrastructure/plugin/protocol.rs)和[传输层](../src/infrastructure/plugin/transport.rs)。

下面展示解码后的对象形状，线上应编码为 MessagePack。协议版本为 `1`；未知字段必须忽略，未知 `kind` 或不支持的主版本必须拒绝：

```json
{"protocol_version":1,"kind":"hello","payload":{"name":"echo","version":"0.1.0","subscribe":["message.received","adapter.connected","adapter.disconnected"]}}
```

RPC 请求使用 `payload.request_id`、`method`、`params`，响应使用相同 `request_id`、`ok`、`result`、`error`。`error` 是 `{code, message}`，插件按 `code` 判断，`message` 仅供日志和用户显示。事件使用 `kind = "event"` 与 `event`、`data`；收到 `shutdown` 事件后插件应自行退出。

错误码为稳定 ASCII 标识：`PERMISSION_DENIED`、`UNKNOWN_METHOD`、`INVALID_ARGUMENT`、`NOT_FOUND`、`FORBIDDEN`、`PROTOCOL_ERROR` 和 `INTERNAL_ERROR`。错误码由 API 错误类型产生，不从中文 message 推断；未单独分类的宿主执行错误归为 `INTERNAL_ERROR`。

事件 DTO 由[事件桥接](../src/infrastructure/plugin/event_bridge.rs)显式投影，插件不会收到 `CoreEvent` 枚举或数据库对象。消息广播不是持久订阅或可靠队列，不保证断线补发，也不能仅凭收到 `message.received` 推断回复处理已完成。

## API 与权限

| 方法 | 清单权限 |
| --- | --- |
| `message.read` | 同名权限 |
| `message.send` | 当前拒绝；插件不能代表角色发言 |
| `character.read` | 同名权限 |
| `character.state.read` | 同名权限 |
| `character.state.write` | 当前拒绝；插件不能直接修改核心状态 |
| `binding.read` | 同名权限 |
| `memory.read` | 同名权限 |
| `memory.write` | 当前拒绝；记忆由核心流程整理 |
| `relationship.read` | 同名权限 |
| `relationship.write` | 当前拒绝；关系变化由核心事件驱动 |
| `llm.call` | 同名权限 |
| `plugin_data.get/set/delete/list` | 无需声明；宿主按插件名隔离 |
| `scheduler.create` | 当前拒绝，即使声明权限 |

例如 `permissions = ["message.read", "character.read"]`。权限名必须使用清单支持的 dotted 名称。读取接口必须带明确范围：`message.read` 使用 `conversation_id`，`character.read` 使用 `character_id`，`binding.read` 使用 `conversation_id`，`relationship.read` 使用 `character_id` 和全域 `user_id`。插件不能通过省略范围参数读取全局数据。参数、返回值和作用域应按[PluginApi](../src/application/plugin_api.rs)核对，授权规则见[权限模块](../src/infrastructure/plugin/permissions.rs)。

读取 API 返回显式版本 1 DTO，而非 Domain 序列化：`character.read` 仅返回 `id/name/description/enabled/created_at/updated_at`；消息内容和附件使用带 `kind` 的稳定对象，附件不暴露本机路径；绑定仅包含基本路由和生命周期字段，不包含宿主策略 JSON；关系字段显式列出。`memory.read` 的默认结果是脱敏摘要，`raw: true` 会返回显式字段白名单中的记忆内容与用户提供 metadata，不包含 embedding 或仓储内部字段。新增 DTO 字段只会在明确扩展协议时加入。

当前角色状态 API 面向全局精力、压力和活动；不能据此假定具备全部会话状态管理。插件 LLM 请求仍受 LLM 开关与 `conversation` 路由约束。`memory.read` 必须提供当前 `conversation_id`，核心先按会话可见范围排除候选和其他会话记忆；默认返回脱敏摘要，只有显式 `raw: true` 才返回原文。

`relationship.changed` 事件中的 `user_id` 是全域用户 ID，可以直接用于 `relationship.read`；它不是当前会话的 `participant_id`。消息事件仍同时保留会话消息作者的 `sender_id`，需要显示名或会话成员信息时再按会话查询参与者。
