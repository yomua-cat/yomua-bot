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
cargo test --locked --all-targets
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --lib infrastructure::plugin::transport::tests -- --ignored --test-threads=1
```

测试需要支持 Unix socket 和子进程；受限沙箱中的权限失败与代码失败应分别记录。测试使用隔离数据，不连接真实 QQ 或调用外部模型作为 CI 前提。文档变更检查本地链接、命令参数和示例，避免未经验证的功能声明。

## CI 与 PR

[CI 工作流](.github/workflows/ci.yml)在 Linux、stable Rust 和固定的 Rust 1.88 最低版本上运行，Cargo 检查使用锁文件。

| 检查 | 门槛 |
| --- | --- |
| 库、二进制及示例构建 | 必须通过 |
| 库和二进制测试 | 必须通过 |
| 文档本地文件链接 | 必须通过 |
| 全目标测试 | 必须通过 |
| Rust 格式 | 必须通过 |
| 严格 Clippy | 必须通过 |
| 插件 Unix socket ignored 测试 | 必须通过 |
| Rust 1.88 最低版本构建 | 必须通过 |

Linux CI 使用锁文件运行上述检查；矩阵检查会在步骤摘要中保留命令和结果，失败时直接阻断工作流。本地 Cargo 检查通过也不代表 Linux 作业已经执行。

PR 说明包括具体问题、修改后的行为、验证命令和结果、相关文档及剩余限制。涉及数据修改时说明迁移或恢复方式。反馈问题使用仓库模板，提供脱敏日志与最小复现。

## 版本与上传

项目在确认可以投入生产前不使用 `v1.0.0`。版本采用 `大版本.功能.补丁`：功能轮次完成后功能号加 1，修复单个问题或补丁时补丁号加 1；多个补丁可以累积后一次上传。当前基线为 `0.1.0`，与 `Cargo.toml` 一致。

每完成一轮可验证的修改就创建提交并上传 GitHub。上传前检查 `git status`、暂存区和敏感文件扫描结果，确认配置、运行数据、日志、NapCat、角色素材和 Agent 记忆没有进入提交。任何版本标签都必须与 Cargo 元数据一致，生产确认前不得创建 `v1.0.0`。
