# 部署指南

## 环境要求

- Rust 1.75+
- SQLite
- NapCat（OneBot 11 服务端）

## 编译

```bash
cargo build --release
```

产物：
- `target/release/yomua-bot` — 主程序
- `target/release/yomua-ctl` — 控制客户端

## 配置文件

首次启动时，如果配置文件不存在，程序会自动生成模板文件并退出，提示用户修改。

### runtime.toml

```toml
# 数据目录
data_dir = "data"

# 日志级别：trace / debug / info / warn / error
log_level = "info"

# 优雅关停超时（秒）
shutdown_timeout_secs = 10

# 插件目录（可选，null 则禁用插件系统）
# plugins_dir = "plugins"

# 管理员用户列表（QQ 号等）。【重要】未配置将导致系统指令无法执行！
admin_users = ["10001", "10002"]

# 事件总线容量（可选，默认 256）
# broadcast_capacity = 256
```

### onebot.toml

```toml
# NapCat WebSocket 地址
websocket_url = "ws://127.0.0.1:3001"

# 访问令牌（与 NapCat 配置一致，留空则不认证）
# access_token = ""

# 重连间隔（秒）
reconnect_interval_secs = 1
max_reconnect_interval_secs = 30
heartbeat_interval_secs = 30
```

### llm.toml（可选）

```toml
enabled = false

# provider = "ollama"
# [options]
# model = "qwen2.5"
```

## 启动

```bash
# 基本启动（使用当前目录的配置文件）
./yomua-bot

# 指定配置目录
./yomua-bot /path/to/config-dir
```

## 控制命令

使用 `yomua-ctl` 向运行中的 bot 发送命令：

```bash
# 查询状态
./yomua-ctl status

# 重新加载配置
./yomua-ctl reload-config

# 优雅关闭
./yomua-ctl shutdown

# 查看帮助
./yomua-ctl help
```

socket 路径默认 `<data_dir>/control.sock`，可通过 `--socket` 或 `--data-dir` 指定。

## 数据目录结构

```
<data_dir>/
├── runtime.db           # SQLite 数据库
├── plugin-sockets/      # 插件 UDS socket
│   └── *.sock
└── control.sock         # 控制 socket（由 yomua-ctl 连接）
```

## 插件系统

将插件可执行文件放入 `plugins_dir` 目录，每个插件需包含 `plugin.toml` 清单文件。见 `archive/07-plugin-system.md`。
