//! 基础设施层 — 存储、LLM 提供商、插件宿主。
//!
//! 本层实现 `domain::repository` 中定义的 trait，
//! 并为外部系统提供具体实现。

pub mod character_card;
pub mod llm;
pub mod plugin;
pub mod storage;
