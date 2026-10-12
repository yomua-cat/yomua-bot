# yomua-bot

yomua-bot 是用 Rust 编写的角色运行时：把角色定义、会话、状态、关系、记忆与回复决策组合起来，通过 OneBot 11 接入 QQ，通过兼容 Chat Completions 的 HTTP 服务调用语言模型。

**项目仍在开发中。** 当前已有主要运行链路和测试，但集成测试、消息可靠性、生命周期和部分管理功能仍有缺口。部署前请阅读[项目现状](docs/status.md)，库测试通过不能代表真实消息收发或长期运行已经验收。

## 快速开始

目标部署环境为 Linux，开发验证环境为 macOS。需要 Rust stable 工具链；连接 QQ 时还需要独立运行的 OneBot 11 WebSocket 服务。最低 Rust 版本尚未验证，工具链与检查结果见[项目现状](docs/status.md)。

```bash
cargo build --locked --lib --bins --examples
cargo test --locked --lib --bins

# 首次运行生成配置模板并以退出码 1 退出。
mkdir -p local-config
cargo run --locked --bin yomua-bot -- --config-dir local-config
```

编辑生成的 `local-config/runtime.toml`，填写非空 `admin_users`；编辑 `onebot.toml`，设置 WebSocket 地址及访问令牌。`llm.toml` 默认关闭 LLM。启用模型、配置上下文窗口与多模型路由的方法见[配置参考](docs/configuration.md)。相对 `data_dir` 从进程工作目录解析。

先导入角色卡，并显式绑定目标会话。以下路径和群号需要替换：

```bash
cargo run --locked --bin yomua-bot -- import-card /path/to/character.json \
  --config-dir local-config --conversation 123456 --group

cargo run --locked --bin yomua-bot -- --config-dir local-config
```

另开终端查看状态或请求关停（默认数据目录为当前工作目录下的 `data/`）：

```bash
cargo run --locked --bin yomua-bot-ctl -- --data-dir data status --raw
cargo run --locked --bin yomua-bot-ctl -- --data-dir data shutdown
```

首次配置、导入和控制命令的本地流程已经验证；真实 QQ 回复仍需单独验收。当前自动绑定、提及处理及角色参与者链路的限制见[项目现状](docs/status.md)。

## 仓库结构

```text
src/             领域、应用、基础设施、适配器及程序入口
tests/           独立集成测试
examples/        插件示例
docs/            当前架构、使用指南和项目现状
.github/         CI 与协作模板
Cargo.toml       包元数据与依赖声明
Cargo.lock       应用依赖锁定
```

配置文件、运行数据、日志、NapCat 数据、角色卡素材和 Agent 记忆属于本地内容。提交前检查实际文件范围。

## 文档与贡献

- [文档索引](docs/README.md)
- [架构](docs/architecture.md)
- [配置](docs/configuration.md)
- [部署运维](docs/deployment.md)
- [CLI](docs/cli.md)
- [插件](docs/plugins.md)
- [项目现状与后续整顿](docs/status.md)
- [贡献规范](CONTRIBUTING.md)

CI 的构建、库及二进制测试、文档链接检查是必过项；全目标测试、格式和严格 Clippy 检查当前作为独立的非阻断检查展示。完整检查状态以具体作业结果为准。

## 许可

[MIT](LICENSE)。
