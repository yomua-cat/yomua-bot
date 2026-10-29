//! 应用层 —— 编排、运行时、事件处理。
//!
//! 本层协调领域模型与基础设施。
//! 它不依赖 SQLite、OneBot 或任何特定的 LLM SDK。

pub mod action;
pub mod behavior_engine;
pub mod binding;
pub mod character_card_scan;
pub mod character_history;
pub mod character_import;
pub mod character_service;
pub mod clock;
pub mod cognition;
pub mod cognition_driver;
pub mod command;
pub mod config;
pub mod context;
pub mod context_assembler;
pub mod control;
pub mod control_schema;
pub mod conversation;
pub mod decision_orchestrator;
pub mod decision_pipeline;
pub mod event_bus;
pub mod llm_scheduler;
pub mod memory_maintenance;
pub mod memory_service;
pub mod message_acceptance;
pub mod mood_service;
pub mod outbox_dispatcher;
pub mod outbox_processor;
#[cfg(test)]
pub mod outbox_tests;
pub mod plugin_api;
pub mod proactive;
pub mod rate_limiter;
pub mod relationship_service;
pub mod reply_processor;
pub mod runtime;
pub mod session_service;
