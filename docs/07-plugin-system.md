Plugin System

1. 目标

插件必须：

- 可独立开发
- 可独立崩溃
- 不拖垮 Core
- 有明确 API
- 有权限边界
- 可以扩展功能
- 尽量不依赖 Core 内部实现

2. 架构

参考 MaiBot 当前 Host / Runner 思路，但不直接复制其内部实现。

Core
 │
 └── Plugin Supervisor
          │
          ├── Plugin A Runner
          ├── Plugin B Runner
          └── Plugin C Runner

2.1 部署布局

```
plugins/
└── <name>/                    # 插件名 = 目录名（清单 name 必须与目录名一致）
    ├── plugin.toml            # 清单：元数据 + 权限 + 可执行文件
    └── <executable>           # 可执行文件（相对插件目录，任意语言）
```

- `runtime.toml` 新增 `plugins_dir` 字段（相对进程工作目录解析，建议使用绝对路径）；
  不配置该字段（默认 `None`）时插件系统完全禁用，Core 不启动任何插件相关任务，
  保持既有部署行为不变。

```toml
data_dir = "data"
log_level = "info"
shutdown_timeout_secs = 10
plugins_dir = "plugins"        # 缺省时不启用插件系统
```

- socket 文件存放在 `<data_dir>/plugin-sockets/<name>.sock`（由 Core 自动创建）。

2.2 plugin.toml 格式

```toml
name = "echo"
version = "0.1.0"
description = "示例 echo 插件"
permissions = ["message.send", "memory.read"]   # 可缺省（默认空权限）
executable = "echo_plugin"

[config]                        # 可缺省，插件专属配置，原样传给插件
key = "value"
```

字段约定：

- `name` / `version` / `description`：非空；`name` 不得超过 48 字符
  （缓解 macOS sun_path 104 字节限制），且必须与插件目录名一致。
- `permissions`：dotted 字符串列表，合法值共 11 个（见下表）；出现未知值
  时清单解析失败、该插件被跳过。
- `executable`：**必须为相对路径**，不得是绝对路径、不得以 `..` 开头、
  不得包含 `..` 路径段；解析后的路径不得越出插件目录。Core 以插件目录为
  工作目录启动该可执行文件。
- `config`：任意 TOML 值，转换后原样保留。

合法权限值（wire / TOML 统一使用 dotted 字符串）：

| 权限 | 说明 |
| --- | --- |
| `message.read` | 读取收到的消息 |
| `message.send` | 发送消息 |
| `character.read` | 读取角色定义 |
| `character.state.read` | 读取角色状态 |
| `character.state.write` | 写入角色状态 |
| `memory.read` | 读取记忆 |
| `memory.write` | 写入记忆 |
| `relationship.read` | 读取关系 |
| `relationship.write` | 写入关系 |
| `scheduler.create` | 创建定时任务 |
| `llm.call` | 调用 LLM |

3. 进程隔离

插件运行在独立进程。

Core Process
    │
    │ IPC
    │
Plugin Process

插件崩溃：

Plugin crash
    ↓
Supervisor detects
    ↓
record error
    ↓
restart / disable
    ↓
Core continues

4. IPC

第一阶段设计为：

Unix Domain Socket
+
MessagePack

协议必须语言无关。

未来可以拥有：

Rust Plugin
TypeScript Plugin
Python Plugin

但 Core 不需要知道插件使用什么语言。

4.1 帧格式

- 每个帧 = 4 字节**大端** u32 长度前缀 + MessagePack（rmp-serde）序列化体。
- 单帧体最大 **16 MiB**（`MAX_FRAME_LEN`）；超长帧直接拒绝。
- 帧独立成包，天然扛半包 / 粘包：接收方累积字节流，读到完整长度前缀后
  按声明长度取整帧。

4.2 消息结构（五类）

wire 形态统一为含 `type` 字段的对象（JSON 展示字段；实际传输为 MessagePack）：

hello（插件 → Core，握手指令）：

```json
{ "type": "hello", "name": "echo", "version": "0.1.0", "subscribe": ["message.received"] }
```

hello_ack（Core → 插件，握手应答；`reason` 允许 `null`）：

```json
{ "type": "hello_ack", "ok": true, "reason": null }
```

request（插件 → Core，RPC 调用）：

```json
{ "type": "request", "id": 1, "method": "message.send", "params": { "conversation_id": 1, "content": "hi" } }
```

response（Core → 插件，RPC 应答；`result` / `error` 允许 `null`，
`ok=false` 时错误信息在 `error`）：

```json
{ "type": "response", "id": 1, "ok": true, "result": {}, "error": null }
```

notify（Core → 插件，事件通知；`data` 允许 `null`）：

```json
{ "type": "notify", "event": "message.received", "data": { "MessageReceived": { "conversation_id": 1 } } }
```

4.3 事件名表（订阅契约）

Hello 的 `subscribe` 声明 dotted 事件名；未知事件名导致握手失败。

| dotted 事件名 | CoreEvent 变体 | 触发时机 |
| --- | --- | --- |
| `message.received` | `MessageReceived` | 收到一条消息 |
| `message.sent` | `MessageSent` | 发出了一条消息 |
| `character.state.changed` | `CharacterStateChanged` | 角色状态变化 |
| `emotion.changed` | `EmotionChanged` | 情绪变化 |
| `relationship.changed` | `RelationshipChanged` | 关系变化 |
| `memory.created` | `MemoryCreated` | 新建了一条记忆 |
| `behavior.decided` | `BehaviorDecided` | 做出了一项行为决策 |
| `response.generated` | `ResponseGenerated` | 生成了一个响应 |
| `adapter.connected` | `AdapterConnected` | 适配器已连接 |
| `adapter.disconnected` | `AdapterDisconnected` | 适配器已断开 |
| `scheduler.task.triggered` | `ScheduledTaskTriggered` | 定时任务被触发 |

事件载荷为对应 CoreEvent 变体的 JSON 序列化（如
`{"MessageReceived": {conversation_id, sender_id, message_id, content, timestamp, is_mentioned}}`），
以 `notify` 帧的 `data` 下发。

4.4 API 方法表

| 方法 | 所需权限 | 本期状态 |
| --- | --- | --- |
| `message.send` | `message.send`（MessageSend） | 可用 |
| `character.read` | `character.read`（CharacterRead） | 可用 |
| `character.state.read` | `character.state.read`（CharacterStateRead） | 可用 |
| `character.state.write` | `character.state.write`（CharacterStateWrite） | 可用 |
| `memory.read` | `memory.read`（MemoryRead） | 可用 |
| `memory.write` | `memory.write`（MemoryWrite） | 可用 |
| `relationship.read` | `relationship.read`（RelationshipRead） | 可用 |
| `relationship.write` | `relationship.write`（RelationshipWrite） | 可用 |
| `llm.call` | `llm.call`（LlmCall） | 可用（经 CognitionLayer；LLM 未启用时报错） |
| `plugin_data.get` / `set` / `delete` / `list` | 免权限（自作用域） | 可用 |
| `message.read` | `message.read`（MessageRead） | 暂未实现 |
| `scheduler.create` | `scheduler.create`（ScheduleCreate） | 本期不开放 |

4.5 生命周期与握手流程

状态流：`Discovered → Starting → Running`；主动停止 `→ Stopped`；
崩溃预算耗尽 `→ Crashed`。

运行期崩溃后的重启会先把状态归口 `Running → Starting`（`decide_restart`
完成，并清掉已死子进程的 pid），再重新 spawn —— 新实例 connect + Hello
才有机会 attach 成功；否则 attach 会因“插件不在启动状态”被拒，运行期崩溃
的插件将永远无法恢复（见 §4.8）。

```
发现（discover_plugins）
  ↓ 校验清单（validate_manifest）
spawn 子进程（注入环境变量）
  ↓
插件连接 socket 并发 Hello{name, version, subscribe}
  ↓ Core 校验
  · Hello.name 必须与 socket 绑定名一致（防伪冒）
  · subscribe 事件名必须全部合法
  · 插件必须已注册且处于 Starting 状态
  ↓
HelloAck{ok: true} → 注册表挂接连接 → Running
```

- **订阅在握手中声明**：`subscribe` 是 Hello 的一部分，握手完成即生效；
  运行中不可变更。
- 握手超时（默认 10 秒）：spawn 后未在时限内完成 Hello → 该实例失败，
  计入崩溃预算重启。

4.6 环境变量契约

Core 在 spawn 插件子进程时注入：

- `YOMUA_PLUGIN_SOCKET`：本插件专属 UDS socket 的绝对路径。
- `YOMUA_PLUGIN_NAME`：插件名（与目录名 / Hello.name 一致）。

插件只需读这两个变量即可完成连接与握手；会话身份由 socket 文件名固化。

4.7 关停语义

- `stop()` 的停止意图是**持久标志**：置位后立即把状态置为 `Stopped`，
  监控循环入口与 spawn 子进程前都会检查，因此即便在实例启动窗口内调用
  `stop()` 也不会丢失意图、不会让插件复活；该标志由对应生命周期任务
  退出时统一清除。
- Core 收到关停信号后对每个插件：若存在连接则发
  `notify{event: "shutdown"}`，插件应自行退出（推荐退出码 0）；
- 等待 `shutdown_timeout`（默认 10 秒）后仍未退出 → SIGKILL 强杀；
- 任何停止路径都会终止子进程并清理 socket 文件、连接与 pid
  （不残留子进程 / 已死 pid），最后关闭数据库。

4.8 崩溃重启

- 崩溃重启决策发生在监控判定“实例失败”之后：先做状态归口（运行期崩溃的
  `Running → Starting`；握手前失败本就处于 `Starting`），再重新 spawn，
  保证新实例握手可成功。
- 每个插件有独立崩溃预算：**最多重启 3 次**，指数退避
  `0.5s → 1s → 2s → 4s → 8s`（封顶 8 秒）。
- 预算语义 = **连续崩溃重启上限**：插件保持 `Running` 稳定存活超过
  `stable_window`（默认 60 秒）后预算重新计数（`restart_count` 清零）；
  attach 成功**不再**清零预算，避免“连上即崩”型插件退避恒为 0.5s 且
  永远不耗尽预算。
- 预算耗尽 → 状态置 `Crashed`，`last_error` 记录"重启次数已达上限"，
  并清理连接与子进程 pid（Crashed 插件不残留已死 pid）。
- 插件崩溃 / 重启只影响该插件自身，不中断 Core。

5. Plugin API

插件通过 SDK 使用能力。

示例：

message.read
message.send

character.read
character.state.read
character.state.write

memory.read
memory.write

relationship.read
relationship.write

scheduler.create

llm.call

说明：方法清单以 §4.4 API 方法表为准；`scheduler.create` 本期不开放，
`message.read` 暂未实现。

6. 权限

插件声明权限（TOML 格式；早期 YAML 草案已废弃）：

```toml
permissions = ["message.read", "message.send", "memory.read"]
```

未授权能力必须拒绝。

- 权限在权限判定层按方法名精确匹配（`check_permission`），缺失即返回
  "权限不足"。
- `plugin_data.*` 四个方法**免权限**——数据天然按插件名命名空间隔离，
  插件只能读写自己的数据（见 §9）。

7. 插件不能

不能：

- 直接访问 Core 内存
- 直接修改 SQLite
- 直接读取其他插件私有数据
- 直接调用 OneBot
- import Core internal modules
- 修改 Character Runtime 内部对象

8. Plugin Lifecycle

discover
  ↓
validate manifest
  ↓
spawn
  ↓
handshake
  ↓
register
  ↓
running
  ↓
shutdown / crash

实际状态机（注册表）：

- 发现并注册：`Discovered`
- 拉起实例：`Starting`
- 握手成功（连接挂接）：`Running`
- 主动停止：`Stopped`
- 崩溃预算耗尽：`Crashed`
- 崩溃重启先归口：`Running → Starting`（再 spawn，见 §4.5）

9. Plugin Data

插件自己的持久化数据必须进入：

plugin_data

并使用 Plugin Namespace 隔离。

- **命名空间 = 插件名**：所有 `plugin_data.*` 方法都以插件名为第一个键
  （由 Core 从连接身份取得，插件无法伪造他人命名空间）。
- **免权限**：读写自己命名空间下的数据不需要任何权限声明。
- 表结构（SQLite）：

```
plugin_data(
    plugin_name  TEXT,
    key          TEXT,
    value        TEXT,      -- JSON 文本
    created_at,  updated_at,
    PRIMARY KEY (plugin_name, key)
)
```

- API：`plugin_data.set(key, value)` 写入/覆盖、`plugin_data.get(key)` 读取
  （无值返回 null）、`plugin_data.delete(key)` 删除
  （返回 `{deleted: bool}`）、`plugin_data.list()` 列出全部 key。
- 参考客户端实现：`examples/echo_plugin.rs`（收到 `message.received` 时
  写入 `last_event` 并读回，演示落库往返）。

10. MVP

先实现：

- Plugin Manifest
- Process Supervisor
- IPC
- Handshake
- Lifecycle
- Event subscription
- Basic API
- Permission check
- Plugin restart

暂时不实现复杂插件市场。

已完成的实施批次（四命令全绿）：

1. 清单/发现/校验、权限判定、Plugin API 与线协议（长度前缀 + MessagePack）。
2. 运行时注册表（状态机 + 连接挂接）、UDS 传输（握手/请求/通知/协议错误隔离）、
   Supervisor 生命周期（spawn/监控/崩溃重启预算/优雅停止）与事件订阅桥接。
3. main.rs 装配接线（Supervisor / EventBridge 条件启用 + 优雅关停）、
   示例 echo 插件（`examples/echo_plugin.rs`）与本协议文档。