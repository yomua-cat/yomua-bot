Character Card and Lorebook

1. 目标

尽可能兼容 SillyTavern Character Card 生态。

重点支持：

- Character Card V1
- Character Card V2
- Character Card V3
- PNG Character Card
- JSON Character Card
- Character Book / Lorebook

2. 不直接使用外部 Schema 作为 Runtime Model

不要让 Runtime：

直接依赖 SillyTavern Card JSON

而应该：

Character Card
      ↓
Importer
      ↓
Canonical CharacterDefinition
      ↓
Character Runtime

3. Canonical Model

内部统一模型：

CharacterDefinition
├── identity
├── personality
├── appearance
├── background
├── scenario
├── speech_style
├── greetings
├── examples
├── system_instructions
└── lorebook

具体字段可根据实现逐步细化。

4. Lorebook

Lorebook 不直接拼进 Character Definition。

应该由：

Context Builder

根据当前 Conversation / Message 动态选择相关 entries。

5. Lorebook Pipeline

Current Context
      ↓
Keyword / Rule Matching
      ↓
Relevant Entries
      ↓
Context Builder
      ↓
LLM Request

未来可以增加：

Semantic Retrieval
Embedding
Vector Search

但 MVP 不实现。

6. Importer

Importer 负责：

- 识别 Card Version
- 解析 JSON
- 解析 PNG metadata
- V1/V2/V3 → Canonical Model
- Schema validation
- Import error reporting

Runtime 不负责兼容历史格式。

7. Future Compatibility

Character Card 规范变化时：

新 Schema
   ↓
新增 Importer
   ↓
Canonical Model

而不是修改整个 Runtime。