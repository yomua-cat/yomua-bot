OneBot Adapter

1. 职责

OneBot Adapter 是 Core 与 QQ 之间的边界。

NapCat
  ↓
OneBot 11
  ↓
Adapter
  ↓
Core Event

2. Adapter 负责

- WebSocket 连接
- OneBot Event 解析
- OneBot Action
- 重连
- 心跳
- 平台字段转换
- Message → Core Message
- Core Action → OneBot Action

3. Core 不允许

Core 不允许：

直接访问 OneBot WebSocket
直接解析 OneBot JSON
直接调用 send_group_msg
直接调用 send_private_msg

4. Event 转换

例如：

OneBot Group Message
        ↓
MessageReceived {
    conversation: Group(...)
    sender: Participant(...)
    content: ...
}

Core 只看到统一事件。

5. Action 转换

Core：

SendMessage {
    conversation_id,
    content
}

Adapter：

SendMessage
    ↓
OneBot API
    ↓
NapCat
    ↓
QQ

6. 连接故障

如果 NapCat：

- 崩溃
- 断线
- 重启
- OneBot 暂不可用

Core 不退出。

Adapter 进入：

Disconnected
    ↓
Backoff
    ↓
Reconnect
    ↓
Connected

Core 继续：

Scheduler
Memory
State
Plugin
Character

7. 第一阶段平台

只实现：

OneBot 11

未来可以增加：

Discord
Telegram
Matrix
Web
其他平台

但不能修改 Character Domain。