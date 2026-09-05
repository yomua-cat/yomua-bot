//! 领域层——无基础设施依赖的纯业务模型。
//!
//! 本模块包含：
//! - `character` — Character、CharacterDefinition、CharacterState、CharacterBinding
//! - `conversation` — Conversation、Participant、ConversationType
//! - `message` — Message、MessageContent
//! - `emotion` — EmotionState、EmotionEvent、情绪计算
//! - `relationship` — Character 与 Participant 之间的关系
//! - `memory` — Memory、MemoryType
//! - `behavior` — BehaviorDecision、Action、BehaviorEngine

pub mod behavior;
pub mod character;
pub mod conversation;
pub mod emotion;
pub mod event;
pub mod memory;
pub mod message;
pub mod mute;
pub mod relationship;
pub mod repository;

#[cfg(test)]
mod behavior_tests;
#[cfg(test)]
mod emotion_tests;
#[cfg(test)]
mod event_tests;
#[cfg(test)]
mod mute_tests;
#[cfg(test)]
mod relationship_tests;
#[cfg(test)]
mod tests;
