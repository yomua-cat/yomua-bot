# CLI 命令参考

## yomua-bot（主程序）

```bash
yomua-bot [配置目录]
```

无参数时使用当前目录作为配置目录。

### 子命令（独立工具）

| 命令 | 工具 | 说明 |
|------|------|------|
| `import-card <路径>` | yomua-bot | 导入角色卡 |
| `list-characters` | yomua-bot | 列出所有角色 |
| `list-bindings` | yomua-bot | 列出所有绑定 |
| `switch-character <角色ID>` | yomua-bot | 切换会话角色 |

### 示例

```bash
# 启动常驻进程
yomua-bot

# 导入角色卡
yomua-bot import-card ./character.json

# 列出角色
yomua-bot list-characters

# 列出绑定
yomua-bot list-bindings

# 切换角色
yomua-bot switch-character 42
```

---

## yomua-ctl（控制客户端）

通过 Unix 域套接字向运行中的 yomua-bot 发送控制命令。

```bash
yomua-ctl [选项] <命令>

选项：
  --socket <路径>   控制 socket 路径（默认：<data-dir>/control.sock）
  --data-dir <路径> 数据目录（默认：data）
```

### 命令

#### `status`

查询运行时状态。

```bash
yomua-ctl status
```

返回示例：
```json
{
  "adapter_state": "Connected",
  "data_dir": "data",
  "log_level": "info",
  "plugin_count": 0
}
```

#### `reload-config`

重新加载并校验配置文件（runtime.toml / onebot.toml / llm.toml）。

```bash
yomua-ctl reload-config
```

返回校验后的配置快照。配置文件存在错误时返回错误信息。

#### `shutdown`

发送优雅关停信号。bot 收到后执行完整关停流程（停止插件 → 停止适配器 → 关闭数据库）。

```bash
yomua-ctl shutdown
```

#### `help`

显示所有可用命令。

```bash
yomua-ctl help
```

---

## 信号

| 信号 | 效果 |
|------|------|
| SIGINT / Ctrl+C | 优雅关停 |

---

## 退出码

| 退出码 | 含义 |
|--------|------|
| 0 | 正常退出 |
| 1 | 配置校验失败 / 启动失败 |
| 2 | 命令行参数错误 |
