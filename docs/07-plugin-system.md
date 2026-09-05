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

6. 权限

插件声明权限。

例如：

permissions:
  - message.read
  - message.send
  - memory.read

未授权能力必须拒绝。

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

9. Plugin Data

插件自己的持久化数据必须进入：

plugin_data

并使用 Plugin Namespace 隔离。

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