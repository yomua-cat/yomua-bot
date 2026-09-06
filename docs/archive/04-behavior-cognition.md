Behavior and Cognition

1. 核心原则

行为系统和 LLM 必须分离。

Behavior ≠ Cognition ≠ LLM

LLM 是 Cognition 的一种实现。

2. 三层认知

Level 0 — Reactive

无需 LLM。

例如：

忽略消息
延迟
简单确认
根据规则发送固定动作
更新状态

Level 1 — Lightweight Cognition

一次 LLM 调用。

用于：

- 普通聊天
- 情绪化回应
- 日常闲聊
- 简单角色扮演

Level 2 — Deep Cognition

复杂情况下允许更深层处理。

例如：

- 长剧情
- 冲突
- 复杂关系
- 多 Character 互动

但必须受到调度器限制。

3. 禁止的设计

不要：

Emotion
  ↓
LLM
  ↓
Emotion
  ↓
LLM
  ↓
Behavior
  ↓
LLM
  ↓
Reply

这种架构会产生：

- Token 爆炸
- 延迟
- 多次网络请求
- 行为不稳定
- LLM 成为整个系统的单点依赖

4. Emotion

情绪计算以确定性模型为主。

例如：

previous emotion
+
event
+
relationship
+
character personality
+
decay
=
new emotion

LLM 可以提供高层语义信号，但不能成为情绪系统的唯一计算器。

5. Behavior Engine

Behavior Engine 负责：

ShouldReply?
ShouldWait?
ShouldInitiate?
ShouldIgnore?
WhichCharacter?
WhatPriority?
NeedCognition?

输出：

BehaviorDecision

例如：

{
  "action": "reply",
  "priority": "normal",
  "cognition_level": 1,
  "delay_ms": 2400
}

6. Action

行为系统最终生成 Action：

SendMessage
Delay
Ignore
React
Schedule
UpdateState
CreateMemory

Adapter 负责把 Action 转换成平台行为。

7. 拟人化

拟人化不是依靠 Prompt 单独实现。

系统层面的拟人化包括：

- 不一定回复
- 回复延迟
- 不同用户不同态度
- 情绪变化
- 关系变化
- 主动聊天
- 偶尔犯错
- 自我修正
- 记住事情
- 忘记事情
- 当前活动
- 疲劳
- 注意力
- 敷衍
- 群聊插话概率

8. LLM Scheduler

所有 LLM 请求进入统一调度器。

优先级：

P0 用户实时消息
P1 高优先级主动行为
P2 普通主动行为
P3 后台认知

低优先级任务不得阻塞 P0。

9. Background Cognition

后台思考默认不需要 LLM。

可以执行：

state decay
emotion decay
relationship update
memory maintenance
schedule evaluation

只有需要语言理解或内容生成时才申请 LLM。

10. 核心目标

系统必须做到：

没有 LLM 时
Character Runtime 仍然能够运行。

LLM 恢复后
Character 可以恢复完整语言能力。

LLM 是能力，不是生命线。