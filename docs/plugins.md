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

每帧为 **4 字节大端长度 + MessagePack 对象**，最大帧体 16 MiB。对象含 `type`，可以是 `hello`、`hello_ack`、`request`、`response` 或 `notify`。实现见[协议模块](../src/infrastructure/plugin/protocol.rs)和[传输层](../src/infrastructure/plugin/transport.rs)。

下面展示解码后的对象形状，线上应编码为 MessagePack：

```json
{"type":"hello","name":"echo","version":"0.1.0","subscribe":["message.received","adapter.connected","adapter.disconnected"]}
```

RPC 请求使用 `id`、`method`、`params`，响应使用相同 `id`、`ok`、`result`、`error`。通知使用 `event` 与 `data`；收到 `shutdown` 通知后插件应自行退出。

事件名称与数据由[事件桥接](../src/infrastructure/plugin/event_bridge.rs)决定。消息广播不是持久订阅或可靠队列，不保证断线补发，也不能仅凭收到 `message.received` 推断回复处理已完成。

## API 与权限

| 方法 | 清单权限 |
| --- | --- |
| `message.send` / `message.read` | 同名权限 |
| `character.read` | 同名权限 |
| `character.state.read` / `character.state.write` | 同名权限 |
| `binding.read` | 同名权限 |
| `memory.read` / `memory.write` | 同名权限 |
| `relationship.read` / `relationship.write` | 同名权限 |
| `llm.call` | 同名权限 |
| `plugin_data.get/set/delete/list` | 无需声明；宿主按插件名隔离 |
| `scheduler.create` | 当前拒绝，即使声明权限 |

例如 `permissions = ["message.read", "character.read"]`。权限名必须使用清单支持的 dotted 名称。`message.read` 当前有实际处理器；参数、返回值和作用域应按[PluginApi](../src/application/plugin_api.rs)核对，授权规则见[权限模块](../src/infrastructure/plugin/permissions.rs)。

当前角色状态 API 面向全局精力、压力和活动；不能据此假定具备全部会话状态管理。插件 LLM 请求仍受 LLM 开关与 `conversation` 路由约束。权限授权范围较宽，细粒度会话限制尚未验收。
