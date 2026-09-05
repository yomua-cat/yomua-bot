//! yomua-bot — Character Runtime 库。
//!
//! 架构：
//! - `domain` — 纯领域模型，不依赖任何基础设施
//! - `application` — 编排、运行时、行为引擎
//! - `infrastructure` — 存储、LLM 提供商、插件宿主
//! - `adapters` — 平台适配器（例如 OneBot）

pub mod adapters;
pub mod application;
pub mod domain;
pub mod error;
pub mod infrastructure;
