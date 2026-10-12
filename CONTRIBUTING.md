# 贡献规范

先阅读[架构](docs/architecture.md)和[项目现状](docs/status.md)，理解当前调用链及已知缺口。修改集中解决一个具体问题，实质行为或配置变化同步更新相应文档。

## 代码与文件约定

- Rust 使用项目现有分层，业务类型与仓储接口优先放在 Domain，具体 IO 留在基础设施或适配器。
- 注释、面向用户的消息和文档使用简体中文；代码、模块、包及 API 名称保持英文。
- 提交配置示例时使用 Markdown 代码块和占位值。TOML、运行数据、日志、NapCat 数据、角色卡素材与 Agent 记忆属于本地内容；根 `Cargo.toml` 是例外。
- 不通过修改测试预期、跳过测试或扩大忽略规则掩盖失败。与当前任务无关的源码问题记录到现状文档，留待独立修复。

## 本地检查

```bash
cargo build --locked --lib --bins --examples
cargo test --locked --lib --bins

# 当前存在缺口，分别检查并报告实际结果。
cargo test --locked --all-targets
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
```

测试需要支持 Unix socket 和子进程；受限沙箱中的权限失败与代码失败应分别记录。测试使用隔离数据，不连接真实 QQ 或调用外部模型作为 CI 前提。文档变更检查本地链接、命令参数和示例，避免未经验证的功能声明。

## CI 与 PR

[CI 工作流](.github/workflows/ci.yml)在 Linux 和 stable Rust 上运行，Cargo 检查使用锁文件。

| 检查 | 当前门槛 |
| --- | --- |
| 库、二进制及示例构建 | 必须通过 |
| 库和二进制测试 | 必须通过 |
| 文档本地文件链接 | 必须通过 |
| 全目标测试 | 非阻断，保留真实结果 |
| Rust 格式 | 非阻断，保留真实结果 |
| 严格 Clippy | 非阻断，保留真实结果 |

非阻断作业使用 `continue-on-error`，整体工作流成功不代表所有检查通过，需查看各作业步骤。现存问题修复后，在同一个 PR 中将对应作业转为必过；不要长期保留已无必要的例外。本地 Cargo 检查通过也不代表 Linux 作业已经执行。

PR 说明包括具体问题、修改后的行为、验证命令和结果、相关文档及剩余限制。涉及数据修改时说明迁移或恢复方式。反馈问题使用仓库模板，提供脱敏日志与最小复现。

上传前检查 `git status` 与实际 diff，确认本次文件范围；本项目协作要求每次 GitHub push 前取得维护者确认。
