//! 应用层 —— 编排、运行时、事件处理。
//!
//! 本层协调领域模型与基础设施。
//! 它不依赖 SQLite、OneBot 或任何特定的 LLM SDK。

pub mod action;
pub mod behavior_engine;
pub mod binding;
pub mod character_import;
pub mod clock;
pub mod cognition;
pub mod cognition_driver;
pub mod command;
pub mod config;
pub mod context;
pub mod conversation;
pub mod emotion_service;
pub mod event_bus;
pub mod event_processor;
pub mod llm_scheduler;
pub mod memory_service;
pub mod message_persistence;
pub mod plugin_api;
pub mod proactive;
pub mod relationship_service;
pub mod reply_processor;
pub mod runtime;
pub mod scheduler;
