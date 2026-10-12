# 部署与运维

目标环境为 Linux，macOS 可用于开发验证；控制服务和插件依赖 Unix socket，Windows 尚未支持验收。当前程序适合隔离试运行，部署前先阅读[项目现状](status.md)。

## 构建与准备

需要 Rust stable、链接器和正常的编译环境。当前依赖锁定下的最低 Rust 版本尚未验证。SQLite 通过 Rust 依赖构建；`sqlite3` 命令仅在下面的备份示例中使用。NapCat 或其他 OneBot 11 服务应独立部署。

```bash
cargo build --release --locked --bin yomua-bot --bin yomua-bot-ctl
```

产物为 `target/release/yomua-bot` 和 `target/release/yomua-bot-ctl`。开发验证工具 `yomua-audit-harness` 不属于日常部署命令。

准备配置目录，按照[配置参考](configuration.md)设置非空管理员列表、独立数据目录及 OneBot 地址。先检查帮助与导入角色卡：

```bash
./target/release/yomua-bot --help
./target/release/yomua-bot-ctl --help

./target/release/yomua-bot import-card /path/to/character.json \
  --config-dir /absolute/path/to/config \
  --conversation 123456 --group
```

每个实例使用独立数据目录与控制 socket。当前没有可靠的单实例锁，不能让两个实例共用同一目录。

## 启动和检查

```bash
./target/release/yomua-bot --config-dir /absolute/path/to/config

# 在另一个终端执行。
./target/release/yomua-bot-ctl --data-dir /absolute/path/to/yomua-data status --raw
./target/release/yomua-bot-ctl --data-dir /absolute/path/to/yomua-data character list
./target/release/yomua-bot-ctl --data-dir /absolute/path/to/yomua-data conversation list
```

常驻启动会校验 runtime 配置、打开并迁移数据库。即使 OneBot 断线，连接循环仍会尝试重连。启动日志和控制 socket 存在只说明局部组件启动，需继续检查 `onebot_status`、角色绑定、实际消息接受及回复日志。

状态包含运行时间、进程内存、会话数量和适配器状态。活跃会话按最近 30 分钟内的消息时间统计，不是独立健康探针。进程尚无 HTTP 健康检查、完整指标端点或经过验收的系统服务部署配置。

## 数据与日志

```text
<data_dir>/
├── runtime.db
├── runtime.db-wal       运行时可能存在
├── runtime.db-shm       运行时可能存在
├── control.sock
└── plugin-sockets/      插件启用时按需创建
```

日志默认输出到终端，由部署环境负责收集与轮转。`RUST_LOG` 优先于 `log_level`，例如 `RUST_LOG=debug`。日志可能包含会话内容及服务错误响应，按运行数据保护。

控制 socket 没有应用层身份认证，能够访问它的进程可以执行管理和删除命令。运行在专用用户下，限制配置、数据和 socket 所在目录的访问权限；不要把控制 socket 暴露给其他用户或网络代理。

## 关停与配置生效

```bash
./target/release/yomua-bot-ctl --data-dir /absolute/path/to/yomua-data shutdown
```

该命令请求运行时停止插件和适配器并关闭存储。响应表示已发出关停请求，仍需确认主进程确实退出。

当前主程序未注册 SIGINT/SIGTERM 处理；终端提示中的 Ctrl+C 不能当成已经实现的优雅关停。后台任务也尚未统一等待结束，`shutdown_timeout_secs` 不限制整个关停流程。服务管理器集成前应完成这一部分验收。

`reload-config` 仅校验 runtime 配置快照，实际变更通过关停、确认退出、修改配置、再启动生效。

## 备份与升级

停机备份时，先请求关停并确认进程退出，再备份配置和整个数据目录。数据库采用 WAL；运行中直接复制单独的 `runtime.db` 可能漏掉未合并数据。

如已安装 `sqlite3`，可使用其在线备份命令创建一致的数据库副本（路径需要替换）：

```bash
sqlite3 /absolute/path/to/yomua-data/runtime.db \
  ".backup '/absolute/path/to/backup/runtime.db'"
```

升级前保留旧二进制及一致备份；在备份副本上试启动并检查迁移、角色和会话数据。当前迁移自动执行，没有自动降级回滚。回退时使用对应旧二进制与升级前备份，不让旧版本直接读取已经升级的数据库。

## 常见问题

| 现象 | 排查方向 |
| --- | --- |
| 首次启动退出码 1 | 模板生成后的预期退出；填写管理员和连接参数 |
| 修改配置未生效 | `reload-config` 没有动态应用配置，需受控重启 |
| 已连接但不回复 | 角色绑定、角色参与者记录、Outbox payload、行为决策与模型错误 |
| `无法连接到 control socket` | 主进程状态、数据目录、socket 权限或控制服务错误日志 |
| 插件启动失败 | 绝对目录、可执行文件、权限、socket 路径长度及握手日志 |
| 模型上下文组装失败 | `[options].context_window`、输出预留及实际 token 预算 |

真实 QQ 收发、外部模型、故障恢复和长期运行仍需独立验收，具体记录见[项目现状](status.md)。
