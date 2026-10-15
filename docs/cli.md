# CLI 参考

## 主程序与独立工具

```bash
yomua-bot [--config-dir <目录>]
yomua-bot --help
```

省略 `--config-dir` 时使用启动目录下的 `config/`。首次启动会在那里生成模板并退出，填写配置后再启动。

下列命令直接打开数据库并执行迁移，不要求常驻进程运行。即使是列表命令也可能创建数据目录或初始化数据库；试运行时使用隔离目录。

| 命令 | 参数与行为 |
| --- | --- |
| `import-card <路径>` | 支持 JSON/PNG；`--config-dir`、`--conversation <外部ID>`、`--group`、`--participant <外部ID>`、`--participant-name`、`--reply-mode natural\|mention\|occasional`、`--proactive` |
| `list-characters` | `--config-dir`；列出角色 ID 和名称 |
| `list-bindings` | `--config-dir`；列出已有会话绑定 |
| `switch-character <角色名>` | 必须指定 `--conversation <外部ID>`，可加 `--config-dir`、`--group`；会话和角色须已存在 |

`import-card` 默认不绑定、默认私聊、默认 `natural`；指定 `--conversation` 才创建或复用会话并绑定。`--participant` 创建的是人类参与者，不能替代回复链路所需的 Bot 角色参与者。`--proactive` 只启用当前的状态评估能力。

```bash
cargo run --locked --bin yomua-bot -- import-card /path/to/character.png \
  --config-dir /path/to/config --conversation 123456 --group \
  --participant 10001 --participant-name 测试用户 --reply-mode natural

cargo run --locked --bin yomua-bot -- list-bindings --config-dir /path/to/config
cargo run --locked --bin yomua-bot -- switch-character 测试角色 \
  --config-dir /path/to/config --conversation 123456 --group
```

当前 `switch-character` 按**角色名**查找；`--group` 可解析，但查询会话的实现仍只依据外部 ID，存在群号与用户号冲突风险。

## 控制客户端

```bash
yomua-bot-ctl [--socket <路径> | --data-dir <目录>] <命令> [参数]
yomua-bot-ctl --help
yomua-bot-ctl --tui
```

全局目录选项放在命令前。默认 socket 为当前工作目录的 `data/control.sock`，显式 `--socket` 优先。客户端不读取主程序配置。

无命令时进入 REPL，使用 `exit`、`quit`、`q`、Ctrl+D 或 Ctrl+C 退出客户端；退出客户端不等于关闭服务。REPL 支持单引号、双引号和反斜杠转义，支持上下键历史、光标编辑、Tab 命令补全，并把历史保存到 socket 所在数据目录的 `.yomua-bot-ctl.history`。输入 `tui` 或执行 `yomua-bot-ctl --tui` 可进入全屏命令面板，方向键选择、Enter 执行、Esc/q 退出；需要 ID、文本或确认的命令会临时打开同一个带历史的输入行。

`--help` 和 `help` 都由 ctl 本地显示命令本，不连接服务；服务端保留结构化 `help` 操作供其他控制客户端查询能力，但不维护别名和交互提示。ctl 负责把别名和参数输入组装成版本化结构化请求，bot 只负责解码请求、确认校验和业务处理。`--raw` 始终输出完整控制响应 JSON（成功和失败都写标准输出），一次性命令收到 `ok=false` 时返回退出码 1。删除、停用、状态修改、黑名单修改和关停等危险操作必须追加 `--confirm`，服务端也会再次校验。`--raw` 和 `--confirm` 可放在命令前或命令后。

ctl 按命令校验参数：选项只对声明的命令开放，`status --energy 20`、`shutdown --id 1`、`memory update --raw` 这类不适用参数会直接报错并提示可用选项，不会被静默忽略。`--raw` 仅对读取类命令开放（status、角色/会话/状态查询、memory list、reply unknown list）。

命令帮助同时显示面向人的短别名，例如 `s`（`status`）、`c list`（`character list`）、`ss list`（`conversation list`）、`m list`（`memory list`）和 `cfg reload`（`reload-config`）。协议和脚本应继续使用完整命令名；别名只用于交互输入。

## 当前可解析的命令

所有角色、会话及记忆 ID 均为数据库内部 ID；QQ 号或群号只有明确标注为外部 ID 时才使用。

| 命令 | 说明 |
| --- | --- |
| `status [--raw]` | 查询进程与连接状态 |
| `help` | 显示 ctl 本地命令本；不连接服务 |
| `reload-config` | 读取并校验配置；可热更新项整体生效，需要重启的项会明确返回 |
| `shutdown --confirm` | 发出关停请求并等待确认 |
| `character list [--limit N] [--offset N]` | 分页列出角色；交互输入可用 `c list` |
| `character show <id> [--raw]` | 角色详情 |
| `conversation list [--raw]` | 会话列表；交互输入可用 `ss list` |
| `character state get <角色ID> [--conversation <ID>] [--raw]` | 全局或会话状态；交互输入可用 `st get` |
| `character state set --character <ID> [--conversation <ID>] [--energy N] [--stress N] [--mood N] [--activity 文本] --confirm` | 状态修改；会话范围 mood 与全局 activity 各有作用域 |
| `character state refresh [--raw]` | 更新全局状态时间戳 |
| `blacklist list` | 已存储的黑名单列表 |
| `blacklist add <外部用户ID> [--reason 文本] --confirm` | 添加用户黑名单记录 |
| `blacklist remove <外部用户ID> --confirm` | 删除用户黑名单记录 |
| `blacklist ban <外部用户ID> --duration 30m [--reason 文本] --confirm` | 临时记录，必须指定时长 |
| `memory list [--character <ID>] [--conversation <ID>] [--type 类型] [--limit N] [--raw]` | 查看记忆摘要；只有显式 `--raw` 才读取原文 |
| `memory delete <记忆ID> --confirm` | 删除记忆 |
| `memory update <记忆ID> --instruction "请把这条记忆改成……" --confirm` | 记录自然语言修改请求，由后台处理 |
| `reply unknown list [--limit N] [--raw]` | 列出发送结果未知、等待平台核查的回复任务；默认只显示元数据，`--raw` 才返回已保存的回复原文 |
| `reply unknown check <消息ID>` | 若任务保存了平台消息 ID，通过 OneBot `get_msg` 查询；没有平台 ID 时明确返回需要人工核查 |
| `reply unknown retry <消息ID> --confirm` | 你已确认平台未发送后，清除未知状态并把原任务重新排队；不会根据查询失败自动执行 |
| `user merge <来源用户ID> <目标用户ID> --confirm` | 把一个全域用户的数据合并到目标用户并删除来源用户；合并规则与迁移一致 |
| `user delete <用户ID> --confirm` | 删除全域用户、关联记忆和关系；匿名保留历史消息及其引用，清理旧账号 ID、私聊路由和用户黑名单 |
| `session delete-character <角色ID> --confirm` | 删除与角色关联的会话数据 |
| `session delete-all --confirm` | 删除全部会话数据 |

全屏面板适合查看状态、列出角色/会话/记忆和重载配置；删除、停用、状态修改等命令不会被方向键误执行，会先要求在输入行中补齐参数和 `--confirm`。

删除命令会改变持久化数据，执行前完成备份并核对范围。黑名单会在 OneBot 入站解析阶段过滤；数据库连接或查询失败按拒绝处理。角色全局状态修改会经过运行时服务并发布状态变更事件，真实热重载和长时间运行仍需部署环境演练。

```bash
yomua-bot-ctl --data-dir /path/to/data character show 1 --raw
yomua-bot-ctl --data-dir /path/to/data character state get 1 --raw
yomua-bot-ctl --data-dir /path/to/data memory list --character 1 --limit 10
yomua-bot-ctl --data-dir /path/to/data reply unknown list --limit 20
yomua-bot-ctl --data-dir /path/to/data reply unknown check 123
yomua-bot-ctl --data-dir /path/to/data reply unknown retry 123 --confirm
```

## 控制协议

三段命令（如 `character card list`、`character state get`）只在 ctl 内按最长路径匹配。控制服务在 Unix socket 上接收一条 JSON 加换行，返回一条 JSON 加换行。请求格式为：

```json
{"version":1,"id":1,"request":{"operation":"status","params":{}}}
```

`operation` 是固定枚举，不接受任意人类命令字符串；`params` 只承载该操作的结构化参数。响应仍返回 `id`、`ok`、`data` 或 `error`。请求上限 1 MiB、响应上限 8 MiB，读写超时 10 秒，命令处理超时 30 秒，客户端总等待 45 秒。协议定义位于 `src/application/control_schema.rs`，ctl 维护人类命令本，bot 只解码、校验和执行请求。

主程序错误和服务端 `ok=false` 响应在一次性模式下均返回非零；REPL 会显示错误并继续等待下一条命令。自动化仍应同时检查响应内容和退出码。实现见[主程序](../src/main.rs)、[ctl](../src/bin/yomua-bot-ctl.rs)和[控制服务](../src/application/control.rs)。
