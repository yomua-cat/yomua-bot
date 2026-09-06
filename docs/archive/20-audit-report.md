# yomua-bot Pre-Alpha Audit Report

## 1. Executive Summary

**项目整体健康度：良好，但存在阻塞性问题**

项目当前处于 Phase 7（多角色）完成、Phase 8/9 尚未实施的阶段。架构设计清晰，分层边界基本遵守，Repository 抽象正确，事件驱动设计合理。

**关键阻塞问题：**
- **测试失败**：`lorebook_limit_truncates` 测试失败，代码 bug 导致 `lorebook_limit` 限制在纯关键词检索路径下不生效
- **Clippy 阻塞**：`cargo clippy -D warnings` 因 1 处 dead code + 2 处 `or_insert_with` 应改为 `or_default()` 而失败

**是否适合继续路线图：** 是，但需先修复上述两个阻塞问题（P1）。

**最大风险：**
1. WebUI 阶段（Phase 9）的 API 设计与 Core 内部结构耦合风险
2. Memory 的 token 爆炸风险（无 context 长度限制）
3. Plugin API 权限模型不够细致

---

## 2. Critical Issues

### P0 — 阻塞（无）

### P1 — 高优先级

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-001 | P1 | Testing / Logic | `src/application/context.rs:942-960` | `lorebook_limit_truncates` 测试失败：纯关键词匹配路径（无 embedding_scheduler）中 `lorebook_limit` 限制不生效，lorebook 匹配结果未截断 | 上下文可能包含超过限制的 lorebook 条目，导致 token 消耗超出预期 | 修复 `ContextBuilder::build` 中纯关键词匹配路径的截断逻辑 |
| AUDIT-002 | P1 | Build | `src/application/cognition_driver.rs:399,409` | Clippy `unwrap_or_default` 警告，`or_insert_with(Vec::new)` 应改为 `or_default()` | `cargo clippy -D warnings` 失败，阻塞 CI | 使用 `or_default()` 替代 `or_insert_with(Vec::new)` |
| AUDIT-003 | P1 | Build | `src/application/plugin_api.rs:1280-1281` | `Harness` 结构体中 `binding_repo` 和 `message_repo` 字段从未被读取（dead code） | `cargo clippy -D warnings` 失败，阻塞 CI | 删除或正确使用这两个字段 |

---

## 3. Architecture Issues

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-004 | P2 | Architecture | `src/application/context.rs:241-247` | 纯关键词匹配路径与混合匹配路径行为不一致：前者无截断，后者有截断 | Context 组装行为不确定，依赖 embedding_scheduler 是否存在 | 统一在 `build` 函数返回前截断，而非在匹配函数内部 |
| AUDIT-005 | P2 | Architecture | `src/application/reply_processor.rs:126-258` | `ReplyProcessor::process` 函数过长（~130行），承担了过多职责 | 可维护性下降，单测困难 | 拆分为更小的函数或使用 Pipeline 模式 |
| AUDIT-006 | P2 | Architecture | `src/application/plugin_api.rs` | `PluginApi` 包含大量 Core 内部引用，Plugin API 实质上暴露了 Core 内部结构 | 未来重构 Core 会破坏 Plugin API，Phase 9 WebUI 也面临同样问题 | Plugin API 应只通过窄接口与 Core 交互，不直接传递 repos |

---

## 4. API / Interface Issues

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-007 | P2 | API | `src/domain/repository.rs:186-197` | `MemoryRepository::search_by_keywords` 默认实现返回空向量而非错误 | 测试桩不会在运行时暴露问题 | 默认实现返回错误或添加 `#[track_caller]` |
| AUDIT-008 | P2 | API | `src/application/cognition.rs:72` | `LlmRequest::metadata` 使用 `serde_json::Value`，但调用方传入的 metadata 结构不一致 | API 语义不清晰 | 引入类型化的 Metadata 结构体 |
| AUDIT-009 | P3 | API | `src/application/llm_scheduler.rs` | `EmbeddingScheduler` 和 `LlmScheduler` 是两个独立 trait，但实现者 `DefaultLlmScheduler` 同时实现两者 | 未来可能需要拆分 | 不强制修改 |

---

## 5. Logic / Runtime Issues

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-010 | P1 | Logic | `src/adapters/onebot/mod.rs:345` | `MessageReceivedEvent.message_id` 在入站处理时被设置为 0（占位） | 无法通过 message_id 做消息去重或幂等处理 | 使用 OneBot 原生的 message_id |
| AUDIT-011 | P2 | Logic | `src/application/event_processor.rs:35-107` | `process` 函数对所有事件类型都只记录 debug 日志，没有实际处理逻辑 | `ResponseGenerated` 等事件发布后没有任何实际作用 | 为关键事件添加处理逻辑或删除这些事件 |
| AUDIT-012 | P2 | Logic | `src/application/reply_processor.rs:158-161` | `cross_reply_enabled` 检查依赖 `get_my_participant_id` 查询，查询失败会静默跳过 | 群聊多 Bot 场景下 cross_reply_enabled 可能无法正确生效 | 添加警告日志并考虑 fallback 行为 |
| AUDIT-013 | P2 | Logic | `src/application/proactive.rs:61-67` | `ProactiveDriver::run` 是无限循环，没有收到关停信号时退出的机制 | 系统关闭时 ProactiveDriver 可能无法优雅退出 | 添加 shutdown 信号监听 |
| AUDIT-014 | P2 | Behavior | `src/application/behavior_engine.rs:139-140` | `deterministic_roll` 使用 FNV-1a 哈希，同一条消息在不同 character_id 下产生不同的 roll 值 | 确认是否是预期设计 | 确认设计意图 |
| AUDIT-015 | P2 | Behavior | `src/application/behavior_engine.rs:339-340` | `decide_proactive` 的 roll 基于 `{character_id}|{conversation_id}|{hour_bucket}`，如果角色同时在多个会话中，跨会话的主动行为决策互不相关 | 符合"每个会话独立决策"的语义 | 确认设计意图 |
| AUDIT-016 | P2 | Memory | `src/application/context.rs:250-266` | 记忆合并逻辑中去重基于 `id == 0` 判断，两条已持久化但内容相同的记忆都会被保留 | 重复记忆可能进入上下文 | 考虑基于内容 hash 去重 |
| AUDIT-017 | P2 | Memory | `src/application/memory_service.rs:36-39` | `MIN_IMPORTANCE = 0.5` 硬编码，无法通过配置调整 | Alpha 测试时无法动态调整记忆提取激进程度 | 移至配置文件 |
| AUDIT-018 | P2 | LLM | `src/application/cognition.rs:74` | `temperature: Some(0.8)` 硬编码，无法通过配置调整 | Alpha 测试时无法实验不同 temperature | 移至 llm.toml 配置 |
| AUDIT-019 | P2 | LLM | `src/application/context.rs` | `ContextLimits` 的默认值硬编码（context_limit=20, memory_limit=10, lorebook_limit=5），无配置化 | Alpha 测试时无法动态调整 context 大小 | 移至配置文件或 llm.toml |
| AUDIT-020 | P2 | LLM | `src/infrastructure/llm/openai_compatible.rs` | 未检查 max_tokens 是否有限制，防止 LLM 输出过长 | 可能有 token 爆炸风险 | 确认并添加 max_tokens 限制 |

---

## 6. Persistence / Database Issues

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-021 | P2 | Database | `src/infrastructure/storage/migrations.rs:48-69` | 会话唯一索引在检测到脏数据时只是 `warn` 并跳过，不解决实际问题 | 脏数据仍然存在 | 在 startup 时提供明确警告或自动清理机制 |
| AUDIT-022 | P2 | Database | `src/infrastructure/storage/migrations.rs` | 迁移 004 创建了 `semantic_memories` 表，但 embedding 生成可能失败 | 语义记忆可能部分写入失败 | 添加事务或补偿机制 |
| AUDIT-023 | P3 | Database | `src/infrastructure/storage/migrations.rs:200-201` | `idx_messages_conversation` 索引可能不是最优覆盖索引 | 对于频繁查询的最近消息可能不是最优 | 考虑覆盖索引 |
| AUDIT-024 | P3 | Database | `src/infrastructure/storage/migrations.rs:216-217` | `idx_memories_character` 没有 `last_accessed` 排序，检索时需要额外排序 | 记忆检索性能可能随数据量增长而下降 | 考虑按时间索引 |

---

## 7. Concurrency / Async Issues

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-025 | P1 | Concurrency | `src/application/proactive.rs:61-67` | `ProactiveDriver::run` 无限循环没有 shutdown 机制 | tokio task 可能成为孤儿任务 | 添加 shutdown channel 监听 |
| AUDIT-026 | P2 | Concurrency | `src/adapters/onebot/connection.rs:160-214` | `run_connection_loop` 中 shutdown 信号的传播可能不够及时 | 确认 shutdown 信号的传播是否正确 | 确认逻辑 |
| AUDIT-027 | P2 | Concurrency | `src/application/event_bus.rs:44-46` | `EventBus::publish` 事件满时静默丢弃（broadcast 语义） | 慢消费者可能导致事件丢失 | 添加 metrics 或在丢事件时记录警告日志 |
| AUDIT-028 | P2 | Concurrency | `src/application/reply_processor.rs:261` | `pending_replies.shuffle(&mut rand::thread_rng())` 在异步上下文中可能不是最佳选择 | 轻微的性能问题 | 使用 `rand::rngs::SmallRng` |
| AUDIT-029 | P2 | Concurrency | `src/adapters/onebot/mod.rs:112` | `started: AtomicBool` 使用 `swap` 方法检查是否已启动 | 当前实现正确，但语义上 `compare_exchange` 更清晰 | 可接受 |
| AUDIT-030 | P2 | Concurrency | `src/adapters/onebot/connection.rs:538-593` | `inbound_message_forwarded_and_reconnect_occurs` 测试有 30 秒超时，在高负载系统上可能不稳定 | 测试偶发失败 | 减少超时时间或使用更可靠的同步机制 |

---

## 8. WebUI / UX Issues

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-031 | P2 | WebUI | `webui/src/main.rs:112,142` | `CorsLayer::new().allow_origin(Any)` 允许任何来源的 CORS 请求 | 安全风险（但目前 WebUI 仅供本地访问） | 在生产环境改为限制来源 |
| AUDIT-032 | P2 | WebUI | `webui/src/main.rs:34-37` | 使用 `include_str!` 嵌入 dist 文件，如果 `../dist/` 目录不存在，编译失败 | 开发者环境必须先构建前端才能编译 WebUI | 添加检查 |
| AUDIT-033 | P2 | WebUI | `webui/src/main.rs:156` | WebUI 读取 `webui.toml` 加载配置，但配置路径是硬编码的 | 部署时配置位置不灵活 | 确认配置路径是否可通过环境变量覆盖 |
| AUDIT-034 | P2 | WebUI | `webui/src/main.rs:194-196` | WebUI HTTP 服务没有优雅关闭机制 | 重启时可能有正在处理的请求被强制中断 | 添加 graceful shutdown |
| AUDIT-035 | P2 | WebUI | Phase 9 尚未实施 | Phase 9 文档在 `docs/14-webui-v2.md`，设计已完成但未实现 | 路线图尚未到达 WebUI 阶段 | 继续按路线图执行 |

---

## 9. Security Issues

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-036 | P2 | Security | `src/application/plugin_api.rs` | `PluginApi` 暴露了大量 Core 内部能力 | 恶意或低质量插件可能破坏 Core 数据或滥用 LLM | Plugin API 应使用更严格的权限模型 |
| AUDIT-037 | P2 | Security | `src/adapters/onebot/mod.rs:280-286` | JSON 解析错误只记录 `warn` 日志，继续处理下一条消息 | 恶意或畸形的 OneBot 消息可能导致事件丢失 | 添加错误计数 metrics |
| AUDIT-038 | P2 | Security | `src/application/config.rs:71-84` | `load_toml` 在文件不存在时返回 `Ok(None)`（使用默认值），但解析错误返回错误 | 配置错误会导致启动失败，这可能是预期行为 | 确认是否需要在配置错误时 fallback 到默认值 |
| AUDIT-039 | P3 | Security | `src/infrastructure/llm/openai_compatible.rs` | API key 通过配置传入，但未确认是否写入日志或暴露到 WebUI | API key 可能泄露 | 确认日志系统是否过滤了敏感配置字段 |

---

## 10. Performance Issues

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-040 | P2 | Performance | `src/application/context.rs:250-266` | `merge_memories` 每次构建 context 都执行 O(n log n) 排序和去重 | Context 组装可能成为 LLM 响应的瓶颈 | 考虑在存储层预先排序和去重，或添加缓存 |
| AUDIT-041 | P2 | Performance | `src/application/memory_service.rs:167-175` | `extract_keywords` 对每条消息的每个触发词列表都做 `to_lowercase()`，存在重复处理 | 轻微的 CPU 浪费 | 预计算或缓存 lowercase 版本 |
| AUDIT-042 | P2 | Performance | `src/application/context.rs:298-320` | `match_lorebook_by_keywords` 如果 lorebook 很大可能很慢 | Lorebook 匹配性能可能随角色卡复杂度线性增长 | 考虑构建 inverted index |
| AUDIT-043 | P2 | Performance | `src/application/reply_processor.rs:261` | `pending_replies.shuffle` 每次回复都要执行，即使只有 1 个回复也执行 shuffle | 轻微的 CPU 浪费 | 只在有多个回复时才 shuffle |

---

## 11. Testing Issues

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-044 | P1 | Testing | `src/application/context.rs:942-960` | `lorebook_limit_truncates` 测试失败，代码 bug 导致 `lorebook_limit` 不生效 | Context 可能包含过多 lorebook 条目，token 消耗不可控 | 修复 bug |
| AUDIT-045 | P2 | Testing | 多个模块 | 各模块的测试大量使用内存桩，部分桩实现不完整 | 测试可能无法发现逻辑问题 | 统一桩实现规范 |
| AUDIT-046 | P2 | Testing | `src/adapters/onebot/connection.rs:596-654` | `inbound_message_forwarded_and_reconnect_occurs` 测试在高负载下可能不稳定（30秒超时） | CI 可能偶发失败 | 减少超时时间或使用更可靠的同步机制 |

---

## 12. Developer Experience Issues

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-047 | P2 | DevEx | `src/application/reply_processor.rs:94` | `#[allow(clippy::too_many_arguments)]`，`ReplyProcessor::new` 有 11 个参数 | 创建时容易出错 | 考虑使用 Builder 模式 |
| AUDIT-048 | P2 | DevEx | `src/application/plugin_api.rs:1272-1282` | `Harness` 结构体有 2 个未使用字段（dead code） | 代码可读性下降 | 删除或使用这两个字段 |
| AUDIT-049 | P3 | DevEx | `src/main.rs` | `run_runtime` 函数非常长（~300行） | 可读性差 | 拆分为更小的函数 |
| AUDIT-050 | P3 | DevEx | `src/application/plugin_api.rs` | `PluginApi` 有 2100+ 行，整个文件过大 | 可读性差 | 考虑拆分为多个模块 |

---

## 13. Deployment / Operations Issues

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-051 | P2 | Deployment | `src/main.rs:136-138` | 配置加载时使用 `unwrap_or_default()`，配置不存在时静默使用默认值 | 部署时可能因为配置文件缺失或格式错误而使用意外默认配置 | 添加日志提示使用了哪些默认配置 |
| AUDIT-052 | P2 | Deployment | `src/main.rs:422-425` | `shutdown_timeout_secs` 配置被读取但从未使用，关停时只等待 `ctrl_c` 信号，没有超时机制 | 如果某个后台任务挂死，系统可能无法优雅关闭 | 实现 shutdown_timeout_secs 超时机制 |
| AUDIT-053 | P2 | Deployment | `src/infrastructure/storage/migrations.rs:60-68` | 脏数据（同一会话多角色绑定）只在 startup 时 warn，不阻止运行但行为不确定 | 脏数据可能导致用户困惑的运行行为 | 提供 CLI 工具检测和清理脏数据 |
| AUDIT-054 | P3 | Deployment | `Cargo.toml` | 没有指定 `rust-version` | 无法确认最低 Rust 版本 | 添加 `rust-version` 字段 |
| AUDIT-055 | P3 | Deployment | 项目根目录 | 没有 Dockerfile 或 docker-compose.yml | 无法使用容器化部署 | Phase 9 或后续添加 |

---

## 14. Modernization Opportunities

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-056 | P3 | Modernization | `Cargo.toml` | 使用 `chrono` 处理时间，但 Rust 生态更现代的选择是 `time` crate | chrono 维护状态稳定，但不是最新 | 不强制升级 |
| AUDIT-057 | P3 | Modernization | 多个模块 | 日志使用 `tracing` 但没有结构化日志字段标准化 | 可观测性不足 | 考虑标准化结构化日志字段 |
| AUDIT-058 | P3 | Modernization | `src/infrastructure/storage/migrations.rs` | 使用手写 SQL 迁移，没有使用 sqlx 的迁移工具 | 迁移管理可能随时间变得复杂 | 不强制升级 |

---

## 15. Future Technical Debt

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-059 | P2 | TechDebt | `src/application/plugin_api.rs` | Plugin API 暴露 Core 内部结构，如果 Core 重构，Plugin API 会断裂 | Phase 6 插件系统与 Core 耦合过紧 | Phase 9 WebUI 阶段应重点关注 |
| AUDIT-060 | P2 | TechDebt | `src/application/reply_processor.rs` | ReplyProcessor 承担了完整的消息处理链路，如果未来需要支持更多消息类型，可能需要大规模重构 | 可扩展性受限 | 考虑使用 Pipeline 模式 |
| AUDIT-061 | P2 | TechDebt | `src/domain/repository.rs` | Repository trait 使用 `i64` 作为 ID 类型，没有 newtype 封装 | 类型安全不足，可能传入错误的 i64 | 考虑引入 newtype |
| AUDIT-062 | P2 | TechDebt | Phase 9 尚未实施 | WebUI 与 Core 的集成方式意味着 WebUI 可以访问 Plugin API 的全部能力 | WebUI 可以绕过业务逻辑直接操作 Core 状态 | Phase 9 实施时重新设计 WebUI API |
| AUDIT-063 | P3 | TechDebt | `src/application/context.rs` | `ContextBuilder` 组装 context 时多次访问数据库（5次） | 延迟敏感时可能需要优化 | 考虑批量查询或添加查询缓存 |
| AUDIT-064 | P3 | TechDebt | `src/application/behavior_engine.rs:359-384` | `base_params` 函数硬编码了 ReplyMode 对应的阈值和延迟 | 行为参数不可配置 | 移至 runtime.toml 或 character 卡配置 |

---

## 16. 问题汇总表

| ID | Severity | Category | Location | Problem | Impact | Recommended Action |
|---|---|---|---|---|---|---|
| AUDIT-001 | P1 | Testing/Logic | `src/application/context.rs:942-960` | lorebook_limit_truncates 测试失败，lorebook_limit 在纯关键词匹配路径不生效 | Context 可能包含过多 lorebook 条目 | 修复截断逻辑 |
| AUDIT-002 | P1 | Build | `src/application/cognition_driver.rs:399,409` | clippy unwrap_or_default 警告 | CI 失败 | 改用 or_default() |
| AUDIT-003 | P1 | Build | `src/application/plugin_api.rs:1280-1281` | dead code: binding_repo, message_repo 未使用 | CI 失败 | 删除或使用 |
| AUDIT-004 | P2 | Architecture | `src/application/context.rs:241-247` | 纯关键词与混合匹配行为不一致 | Context 行为不确定 | 统一截断逻辑 |
| AUDIT-005 | P2 | Architecture | `src/application/reply_processor.rs:126-258` | ReplyProcessor::process 过长 | 可维护性差 | 拆分函数 |
| AUDIT-006 | P2 | Architecture | `src/application/plugin_api.rs` | Plugin API 暴露 Core 内部结构 | 重构困难，Phase 9 风险 | 窄接口隔离 |
| AUDIT-007 | P2 | API | `src/domain/repository.rs:186-197` | search_by_keywords 默认返回空而非错误 | 测试覆盖不足 | 明确必须覆盖 |
| AUDIT-008 | P2 | API | `src/application/cognition.rs:72` | metadata 使用 Value 类型，语义不清 | 下游解析困难 | 类型化 Metadata |
| AUDIT-010 | P1 | Logic | `src/adapters/onebot/mod.rs:345` | message_id 始终为 0 | 无法去重 | 使用原生 ID |
| AUDIT-011 | P2 | Logic | `src/application/event_processor.rs:35-107` | 大部分事件只记日志无实际处理 | 事件系统不完整 | 添加处理或删除事件 |
| AUDIT-012 | P2 | Logic | `src/adapters/onebot/mod.rs:280-286` | JSON 解析错误静默丢弃 | 畸形消息丢事件 | 错误计数 |
| AUDIT-013 | P2 | Logic | `src/application/proactive.rs:61-67` | ProactiveDriver 无 shutdown 机制 | 孤儿任务 | 添加 shutdown 监听 |
| AUDIT-014 | P2 | Behavior | `src/application/behavior_engine.rs:139-140` | deterministic_roll 包含 character_id | 跨角色行为不同 | 确认设计意图 |
| AUDIT-015 | P2 | Behavior | `src/application/behavior_engine.rs:339-340` | 主动行为跨会话独立决策 | 主动行为不协调 | 确认设计意图 |
| AUDIT-016 | P2 | Memory | `src/application/context.rs:250-266` | 记忆去重基于 id，内容相同但不同 id 都会保留 | 重复记忆 | 考虑内容 hash 去重 |
| AUDIT-017 | P2 | Memory | `src/application/memory_service.rs:36-39` | MIN_IMPORTANCE 硬编码 | 无法动态调整 | 配置文件化 |
| AUDIT-018 | P2 | LLM | `src/application/cognition.rs:74` | temperature 硬编码 | 无法实验 | 配置化 |
| AUDIT-019 | P2 | LLM | `src/application/context.rs` | ContextLimits 硬编码 | 无法动态调整 | 配置化 |
| AUDIT-020 | P2 | LLM | `src/infrastructure/llm/openai_compatible.rs` | max_tokens 未限制 | token 爆炸风险 | 确认并添加 |
| AUDIT-021 | P2 | Database | `src/infrastructure/storage/migrations.rs:48-69` | 脏数据只 warn 不处理 | 脏数据遗留 | 提供清理工具 |
| AUDIT-022 | P2 | Database | `src/infrastructure/storage/migrations.rs` | semantic memory 写入可能失败 | 数据不一致 | 事务或补偿 |
| AUDIT-023 | P3 | Database | `src/infrastructure/storage/migrations.rs:200-201` | 索引不够优化 | 消息查询可能慢 | 覆盖索引 |
| AUDIT-024 | P3 | Database | `src/infrastructure/storage/migrations.rs:216-217` | 记忆索引缺少时间排序 | 检索可能需要额外排序 | 时间索引 |
| AUDIT-025 | P1 | Concurrency | `src/application/proactive.rs:61-67` | ProactiveDriver 无 shutdown | 孤儿任务 | 添加 shutdown |
| AUDIT-026 | P2 | Concurrency | `src/adapters/onebot/connection.rs:160-214` | shutdown 信号传播需确认 | 关停可能不及时 | 确认逻辑 |
| AUDIT-027 | P2 | Concurrency | `src/application/event_bus.rs:44-46` | 事件满时静默丢弃 | 慢消费者丢事件无感知 | 添加 metrics |
| AUDIT-028 | P2 | Concurrency | `src/application/reply_processor.rs:261` | thread_rng 在异步中使用 | 轻微性能问题 | SmallRng |
| AUDIT-029 | P2 | Concurrency | `src/adapters/onebot/mod.rs:112` | AtomicBool swap 语义 | 当前正确但语义不清 | 可接受 |
| AUDIT-030 | P2 | Concurrency | `src/adapters/onebot/connection.rs:538-593` | 30秒超时测试可能不稳定 | CI 偶发 | 优化同步 |
| AUDIT-031 | P2 | WebUI | `webui/src/main.rs:112,142` | CORS allow_origin(Any) | 安全风险（本地可接受） | 生产环境限制来源 |
| AUDIT-032 | P2 | WebUI | `webui/src/main.rs:34-37` | include_str! 依赖 dist 存在 | 编译前置条件 | 添加检查 |
| AUDIT-033 | P2 | WebUI | `webui/src/main.rs:156` | 配置路径硬编码 | 不灵活 | 环境变量覆盖 |
| AUDIT-034 | P2 | WebUI | `webui/src/main.rs:194-196` | 无 graceful shutdown | 请求中断 | 添加 shutdown |
| AUDIT-035 | P2 | WebUI | Phase 9 | 尚未实施 | 无法验证集成 | 按路线图执行 |
| AUDIT-036 | P2 | Security | `src/application/plugin_api.rs` | 插件可访问大量 Core 能力 | 恶意插件风险 | 严格权限模型 |
| AUDIT-037 | P2 | Security | `src/adapters/onebot/mod.rs:280-286` | JSON 解析错误静默丢弃 | 畸形消息丢事件 | 错误计数 |
| AUDIT-038 | P2 | Security | `src/application/config.rs:71-84` | 配置错误是否 fallback 不明确 | 部署行为不确定 | 明确行为 |
| AUDIT-039 | P3 | Security | `src/infrastructure/llm/openai_compatible.rs` | API key 是否泄露未确认 | 潜在泄露 | 确认日志过滤 |
| AUDIT-040 | P2 | Performance | `src/application/context.rs:250-266` | merge_memories O(n log n) 排序 | 大数据量时慢 | 预排序或缓存 |
| AUDIT-041 | P2 | Performance | `src/application/memory_service.rs:167-175` | 重复 to_lowercase | CPU 浪费 | 预计算缓存 |
| AUDIT-042 | P2 | Performance | `src/application/context.rs:298-320` | lorebook 关键词匹配无索引 | 大 lorebook 慢 | inverted index |
| AUDIT-043 | P2 | Performance | `src/application/reply_processor.rs:261` | 单回复也 shuffle | 轻微浪费 | 条件执行 |
| AUDIT-044 | P1 | Testing | `src/application/context.rs:942-960` | lorebook_limit_truncates 测试失败 | Context 可能包含过多条目 | 修复 bug |
| AUDIT-045 | P2 | Testing | 多个 MemRepo | 桩实现不一致 | 测试覆盖不足 | 统一桩规范 |
| AUDIT-046 | P2 | Testing | `src/adapters/onebot/connection.rs:596-654` | 30秒超时测试可能不稳定 | CI 偶发 | 优化同步 |
| AUDIT-047 | P2 | DevEx | `src/application/reply_processor.rs:94` | too_many_arguments | API 难用 | Builder 模式 |
| AUDIT-048 | P2 | DevEx | `src/application/plugin_api.rs:1272-1282` | Harness 有 dead code | 可读性差 | 清理 |
| AUDIT-049 | P2 | DevEx | `src/main.rs` | run_runtime 过长 | 可读性差 | 拆分为小函数 |
| AUDIT-050 | P3 | DevEx | `src/application/plugin_api.rs` | 2100+ 行文件过大 | 可读性差 | 拆分模块 |
| AUDIT-051 | P2 | Deployment | `src/main.rs:136-138` | 配置缺失静默 fallback | 部署行为不确定 | 添加日志提示 |
| AUDIT-052 | P2 | Deployment | `src/main.rs:422-425` | shutdown_timeout_secs 未使用 | 无法优雅关闭 | 实现超时机制 |
| AUDIT-053 | P2 | Deployment | `src/infrastructure/storage/migrations.rs:60-68` | 脏数据只 warn | 脏数据遗留 | 提供清理工具 |
| AUDIT-054 | P3 | Deployment | `Cargo.toml` | 无 rust-version | 编译器版本不明确 | 添加字段 |
| AUDIT-055 | P3 | Deployment | 项目根目录 | 无 Dockerfile | 无法容器部署 | 后续添加 |
| AUDIT-056 | P3 | Modernization | `Cargo.toml` | chrono 而非 time | 非最新但稳定 | 不强制升级 |
| AUDIT-057 | P3 | Modernization | 多个模块 | 日志字段不标准 | 可观测性不足 | 标准化字段 |
| AUDIT-058 | P3 | Modernization | `src/infrastructure/storage/migrations.rs` | 手写 SQL | 迁移可能复杂 | 不强制升级 |
| AUDIT-059 | P2 | TechDebt | `src/application/plugin_api.rs` | Core 内部暴露 | 重构困难 | 窄接口 |
| AUDIT-060 | P2 | TechDebt | `src/application/reply_processor.rs` | 单一职责过长 | 扩展困难 | Pipeline 模式 |
| AUDIT-061 | P2 | TechDebt | `src/domain/repository.rs` | i64 类型安全不足 | 错误 ID 风险 | newtype 封装 |
| AUDIT-062 | P2 | TechDebt | Phase 9 | WebUI 可直接操作 Core 状态 | 安全性风险 | 重设计 WebUI API |
| AUDIT-063 | P3 | TechDebt | `src/application/context.rs` | 5次数据库查询 | 延迟敏感时瓶颈 | 批量查询/缓存 |
| AUDIT-064 | P3 | TechDebt | `src/application/behavior_engine.rs:359-384` | 行为参数硬编码 | 不可配置 | 配置文件化 |

---

## 17. 按优先级执行的建议

### 立即修复（Alpha 前必须修）

1. **AUDIT-001** - lorebook_limit_truncates 测试失败（代码 bug）
2. **AUDIT-002** - clippy -D warnings 失败（dead code + or_insert_with）
3. **AUDIT-003** - dead code in Harness
4. **AUDIT-010** - message_id 始终为 0（影响事件追踪）
5. **AUDIT-013** - ProactiveDriver 无 shutdown 机制（资源泄漏）
6. **AUDIT-025** - ProactiveDriver 无 shutdown 机制（资源泄漏）
7. **AUDIT-027** - EventBus 满时静默丢弃（可观测性缺失）

### Alpha 前修复（影响核心功能）

8. **AUDIT-006** - Plugin API 暴露 Core 内部结构（Phase 9 重构风险）
9. **AUDIT-011** - 事件系统不完整（大部分事件无实际处理）
10. **AUDIT-012** - cross_reply_enabled 静默失败
11. **AUDIT-021** - 脏数据只 warn 不处理
12. **AUDIT-036** - 插件权限模型不够细致
13. **AUDIT-051** - 配置缺失静默 fallback
14. **AUDIT-052** - shutdown_timeout_secs 未实现

### Alpha 后修复（不影响路线图）

15. **AUDIT-004** - lorebook_limit 行为不一致
16. **AUDIT-005** - ReplyProcessor 过长
17. **AUDIT-016** - 记忆去重不完整
18. **AUDIT-017** - MIN_IMPORTANCE 硬编码
19. **AUDIT-018** - temperature 硬编码
20. **AUDIT-019** - ContextLimits 硬编码
21. **AUDIT-040** - merge_memories 性能
22. **AUDIT-042** - lorebook 匹配性能
23. **AUDIT-059** - Core 重构困难
24. **AUDIT-060** - ReplyProcessor 扩展性
25. **AUDIT-061** - i64 类型安全

### 接受但不关注（当前阶段可接受）

26. **AUDIT-009** - trait 同时实现两个 Scheduler
27. **AUDIT-014** - deterministic_roll 包含 character_id
28. **AUDIT-015** - 主动行为跨会话独立
29. **AUDIT-023** - 消息索引不够优化
30. **AUDIT-024** - 记忆索引缺少时间排序
31. **AUDIT-029** - AtomicBool 语义
32. **AUDIT-054** - 无 rust-version
33. **AUDIT-055** - 无 Dockerfile
34. **AUDIT-056** - chrono 而非 time
35. **AUDIT-057** - 日志字段不标准
36. **AUDIT-058** - 手写 SQL

---

## 18. 最终结论

**项目当前状态：健康，但有阻塞性问题**

项目架构设计合理，分层清晰，Repository 抽象正确，事件驱动设计合理，Plugin 系统和 LLM Scheduler 设计良好。四命令验证有 2 个阻塞问题（P1）和 1 个测试失败（P1）需要立即修复。

**继续路线图：是，但先修 P1**

**Alpha 阻塞问题：无（修完 P1 后）**

**最大架构风险：**
1. Phase 9 WebUI 设计与 Core 的耦合（Plugin API 暴露 Core 内部）
2. Memory token 爆炸风险（无 context 长度硬限制）
3. Plugin 权限模型不够细致

**建议维护优先级：**
- 立即：修 P1（3 个 build/compile 问题 + 2 个 shutdown 问题）
- Alpha 前：修 P1 影响核心功能的 8 个问题
- Alpha 后：修剩余的 16 个 P2/P3 问题
- 接受：9 个当前阶段可接受的问题
