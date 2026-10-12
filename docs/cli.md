# CLI 参考

## 主程序与独立工具

```bash
yomua-bot [--config-dir <目录>]
yomua-bot <配置目录>
yomua-bot --help
```

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
```

全局目录选项放在命令前。默认 socket 为当前工作目录的 `data/control.sock`，显式 `--socket` 优先。客户端不读取主程序配置。

无命令时进入 REPL，使用 `exit`、`quit`、Ctrl+D 或 Ctrl+C 退出客户端；退出客户端不等于关闭服务。REPL 按空白拆分输入，不能完整处理带空格的引号参数，复杂文本优先使用一次性 shell 命令。

`--help` 显示本地帮助，不连接服务；`help` 请求运行中服务列出已注册命令。`--raw` 将成功响应的数据打印为 JSON，错误仍输出文字。

## 当前可解析的命令

所有角色、会话及记忆 ID 均为数据库内部 ID；QQ 号或群号只有明确标注为外部 ID 时才使用。

| 命令 | 说明 |
| --- | --- |
| `status [--raw]` | 查询进程与连接状态 |
| `help` | 请求服务命令列表 |
| `reload-config` | 读取、校验 `runtime.toml` 并返回部分值；不动态应用 |
| `shutdown` | 发出关停请求 |
| `character list [--raw]` | 列出角色；CLI 可传 limit/offset，但服务目前不分页 |
| `character show <id> [--raw]` | 角色详情 |
| `conversation list [--raw]` | 会话列表；帮助中的 `session list` 目前不匹配 |
| `state get <角色ID> [--conversation <ID>] [--raw]` | 全局或会话状态 |
| `state set --character <ID> [--conversation <ID>] [--energy N] [--stress N] [--mood N] [--activity 文本]` | 状态修改；会话范围 mood 与全局 activity 各有作用域 |
| `state refresh [--raw]` | 更新全局状态时间戳 |
| `blacklist list` | 已存储的黑名单列表 |
| `blacklist add <外部用户ID> [--reason 文本]` | 添加用户黑名单记录 |
| `blacklist remove <外部用户ID>` | 删除用户黑名单记录 |
| `blacklist ban <外部用户ID> --duration 30m [--reason 文本]` | 临时记录，必须指定时长 |
| `memory list [--character <ID>] [--conversation <ID>] [--type 类型] [--limit N]` | 记忆查询 |
| `memory delete <记忆ID>` | 删除记忆 |
| `session delete-character <角色ID>` | 删除与角色关联的会话数据 |
| `session delete-all` | 删除全部会话数据 |

删除命令会改变持久化数据，执行前完成备份并核对范围。黑名单记录目前没有接入入站过滤，不能当作拦截保证。角色全局状态修改与运行时缓存一致性也尚未验收。

```bash
yomua-bot-ctl --data-dir /path/to/data character show 1 --raw
yomua-bot-ctl --data-dir /path/to/data state get 1 --raw
yomua-bot-ctl --data-dir /path/to/data memory list --character 1 --limit 10
```

## 已注册但 CLI 无法到达的命令

当前解析器最多收集两个路径词。因此帮助中的 `character card list/enable/disable/reassign` 和 `character state get/set/refresh` 三段式命令不能按显示语法调用。状态操作可以使用上表的 `state ...`；角色卡启停和 ID 重分配的 CLI 入口仍需修复。

服务端已注册 `character_card_list`、`character_card_enable`、`character_card_disable`、`character_card_reassign` 等协议命令，但不能把服务端支持等同于 CLI 可用。

控制服务在 Unix socket 上接收 JSON 对象，例如 `{"id":1,"cmd":"status"}`，返回 `id`、`ok`、`data` 或 `error`。控制协议不是插件 MessagePack 协议。客户端当前一次读取响应，大响应与分片的可靠性未验收。

主程序错误通常返回非零；ctl 的参数、连接或解析错误返回 1，但服务端 `ok=false` 响应在一次性模式下仍可能以 0 退出。自动化应同时检查响应内容，不能仅依赖退出码。实现见[主程序](../src/main.rs)、[ctl](../src/bin/yomua-bot-ctl.rs)和[控制服务](../src/application/control.rs)。
