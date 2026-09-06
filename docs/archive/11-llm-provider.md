LLM Provider

1. 目标

支持：

- OpenAI
- Anthropic / Claude
- Google Gemini
- 国内模型 API
- OpenAI-compatible API
- Local LLM

2. Provider 抽象

Core 不允许直接使用：

OpenAI SDK
Anthropic SDK
Gemini SDK

Core 只依赖统一接口：

LLMProvider

3. 基础能力

Provider 至少支持：

generate
stream
model metadata
token usage
timeout
error

4. Request

LLM Request 由 Cognition Layer 创建：

LLMRequest
├── messages
├── system
├── model
├── temperature
├── max_tokens
├── tools
└── metadata

5. Context Builder

Context Builder 负责组合：

Character Definition
+
Scenario
+
Relevant Lorebook
+
Conversation Context
+
Relevant Memory
+
Relationship
+
Current State
+
Post History Instructions

不要让 Character Runtime 自己拼 Prompt。

6. Provider Failover

未来支持：

Primary Provider
      ↓ failure
Fallback Provider
      ↓ failure
Local Provider

MVP 可以先实现 Provider abstraction 和一个或多个 provider。

7. Token 控制

系统必须控制：

- 最大 Context
- 最大 Output
- 历史消息数量
- Lorebook 数量
- Memory 数量

不能无限增长。

8. LLM Scheduler

所有调用统一经过：

Cognition
   ↓
LLM Scheduler
   ↓
LLM Provider

禁止其他模块直接调用 Provider。