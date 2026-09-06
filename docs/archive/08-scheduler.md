Scheduler

1. 职责

Scheduler 负责：

- 延迟任务
- 定时任务
- 主动行为
- 后台维护
- Character mute schedule
- LLM request priority

2. 不负责

Scheduler 不负责：

- Character 人格
- Emotion 计算
- Message parsing
- LLM prompt 构建
- OneBot

3. 主动行为

主动行为不能简单实现为：

every N minutes → LLM

正确模型：

Scheduler Tick
      ↓
State / Schedule Evaluation
      ↓
是否存在行为机会？
      ↓
Behavior Engine
      ↓
是否需要 Cognition？
      ↓
LLM Scheduler

4. Mute

Conversation 可以配置：

proactive_enabled
mute_schedule（"HH:MM-HH:MM" 时段，支持跨午夜）

Mute 时：

禁止主动发送

但不影响：

用户主动消息（被 @ 的实时消息仍回复，仅降低未提及消息的回复概率）

除非未来显式配置。

5. Background Tasks

优先使用确定性任务：

emotion decay
state decay
memory maintenance
relationship maintenance
adapter health check
plugin health check

不要把后台 Tick 设计成 LLM Tick。

6. Priority

P0 = user interaction
P1 = urgent system action
P2 = proactive interaction
P3 = maintenance / cognition

调度器必须防止低优先级任务挤占实时聊天资源。

7. Backpressure

当 LLM 请求过多时：

P3 可以丢弃
P2 可以延迟
P1 应尽量执行
P0 优先执行

不能无限创建任务。