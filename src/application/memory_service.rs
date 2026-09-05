//! 记忆服务 —— 长期记忆的提取与持久化。
//!
//! 遵循 docs/05 的记忆生命周期：
//! `消息 → MemoryCandidate → 重要度评估 → 持久化记忆`。
//!
//! MVP 使用**确定性启发式**提取：不要求每条消息都成为记忆，
//! 只有重要度达到阈值的消息才落库。未来可在认知层引入 LLM 协助提取，
//! 但 LLM 必须经调度器、且与这里是可选叠加关系。

use std::sync::Arc;

use crate::domain::memory::{Memory, MemoryType};
use crate::domain::repository::MemoryRepository;
use crate::error::RuntimeError;

/// 一条待评估的记忆候选。
#[derive(Debug, Clone)]
pub struct MemoryCandidate {
    /// 记忆内容（已清洗的消息文本）。
    pub content: String,
    /// 记忆类型。
    pub memory_type: MemoryType,
    /// 重要度评分（0.0 - 1.0）。
    pub importance: f64,
    /// 用于后续检索的关键词。
    pub keywords: Vec<String>,
    /// 来源参与者（消息发送者）。
    pub participant_id: Option<i64>,
}

/// 记忆服务 —— 负责确定性提取与持久化。
pub struct MemoryService {
    memory_repo: Arc<dyn MemoryRepository>,
}

/// 触发记忆提取的最小区分信息量（字符数）。
const MIN_MESSAGE_CHARS: usize = 12;
/// 记忆落库的重要度阈值。低于它不持久化（避免每条消息都成记忆）。
const MIN_IMPORTANCE: f64 = 0.5;

/// 关系类触发词（表达与他人关系的陈述 → Relationship 记忆）。
const REL_TRIGGERS: &[&str] = &[
    "我朋友",
    "我弟弟",
    "我妹妹",
    "我哥哥",
    "我姐姐",
    "我家人",
    "我爸妈",
    "我爸",
    "我妈",
    "我老公",
    "我老婆",
    "我对象",
    "我室友",
    "我和",
    "我们",
];
/// 自我描述类触发词（表达偏好 / 身份 / 长期事实 → Semantic 记忆）。
const SEM_TRIGGERS: &[&str] = &[
    "我喜欢",
    "我爱",
    "我讨厌",
    "我不喜欢",
    "我住在",
    "我是",
    "我的名字",
    "我养",
    "我家",
    "我生日",
    "我今年",
    "我会",
    "我能",
    "我想要",
];
/// 事件 / 经历类触发词（表达发生过的事 → Episodic 记忆）。
const EPI_TRIGGERS: &[&str] = &[
    "今天",
    "昨天",
    "上周",
    "明天",
    "我考试",
    "我去了",
    "我做了",
    "我买了",
    "我吃完",
    "我记得",
];

impl MemoryService {
    /// 创建一个记忆服务。
    pub fn new(memory_repo: Arc<dyn MemoryRepository>) -> Self {
        Self { memory_repo }
    }

    /// 从一条用户消息中提取记忆并对重要度达标的候选落库。
    ///
    /// 返回本次落库的记忆条数。不重要（低于阈值）的消息会被忽略。
    pub async fn extract_and_store(
        &self,
        character_id: i64,
        conversation_id: Option<i64>,
        participant_id: i64,
        message_text: &str,
    ) -> Result<usize, RuntimeError> {
        let Some(candidate) = self.evaluate(message_text, participant_id) else {
            return Ok(0);
        };

        let memory = Memory::new(
            character_id,
            conversation_id,
            candidate.memory_type,
            candidate.content,
            candidate.importance,
        );
        let _ = self.memory_repo.insert(&memory).await?;
        Ok(1)
    }

    /// 对消息做确定性重要度评估，返回候选；不足阈值时返回 `None`。
    ///
    /// 这是纯函数，便于单测。规则：
    /// - 消息过短且无触发词 → 不记录；
    /// - 重要度低于阈值 → 不记录；
    /// - 类型判定：关系 > 自我描述 > 事件（默认 episodic）。
    fn evaluate(&self, message_text: &str, participant_id: i64) -> Option<MemoryCandidate> {
        let text = message_text.trim();
        if text.is_empty() {
            return None;
        }

        let lower = text.to_lowercase();
        let has_rel = trigger_hit(&lower, REL_TRIGGERS);
        let has_sem = trigger_hit(&lower, SEM_TRIGGERS);
        let has_epi = trigger_hit(&lower, EPI_TRIGGERS);

        // 重要的只有：消息较长，或含有至少一个触发词。
        let char_count = text.chars().count();
        let trigger_count = [has_rel, has_sem, has_epi].iter().filter(|&&b| b).count() as f64;

        let mut importance = 0.4 + trigger_count * 0.2;
        if char_count >= 40 {
            importance += 0.2;
        } else if char_count >= 20 {
            importance += 0.1;
        }
        importance = importance.clamp(0.0, 1.0);

        // 信息量不足且无触发词 → 不记录。
        if char_count < MIN_MESSAGE_CHARS && trigger_count == 0.0 {
            return None;
        }
        if importance < MIN_IMPORTANCE {
            return None;
        }

        // 类型判定优先级：关系 > 自我描述 > 事件（默认 episodic）。
        let memory_type = if has_rel {
            MemoryType::Relationship
        } else if has_sem {
            MemoryType::Semantic
        } else {
            MemoryType::Episodic
        };

        let mut keywords: Vec<String> = Vec::new();
        for kw in [REL_TRIGGERS, SEM_TRIGGERS, EPI_TRIGGERS]
            .iter()
            .flat_map(|list| list.iter())
        {
            if lower.contains(kw) && !keywords.iter().any(|k| k == kw) {
                keywords.push(kw.to_string());
            }
        }

        Some(MemoryCandidate {
            content: text.to_string(),
            memory_type,
            importance,
            keywords,
            participant_id: Some(participant_id),
        })
    }
}

/// 触发词命中检测（任一命中即返回 true）。
fn trigger_hit(text: &str, triggers: &[&str]) -> bool {
    triggers.iter().any(|t| text.contains(t))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::memory::Memory;
    use crate::domain::repository::MemoryRepository;
    use crate::error::RepositoryError;
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// 内存版记忆仓储，用于隔离测试。
    struct MemMemoryRepo {
        items: Mutex<Vec<Memory>>,
    }

    #[async_trait]
    impl MemoryRepository for MemMemoryRepo {
        async fn find_by_character_id(
            &self,
            character_id: i64,
            memory_type: Option<MemoryType>,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(self
                .items
                .lock()
                .unwrap()
                .iter()
                .filter(|m| {
                    m.character_id == character_id && memory_type.is_none_or(|t| m.memory_type == t)
                })
                .cloned()
                .collect())
        }
        async fn insert(&self, m: &Memory) -> Result<i64, RepositoryError> {
            let mut items = self.items.lock().unwrap();
            let id = items.len() as i64 + 1;
            let mut m = m.clone();
            m.id = id;
            items.push(m);
            Ok(id)
        }
        async fn update(&self, _m: &Memory) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete(&self, _id: i64) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    fn service() -> (MemoryService, Arc<MemMemoryRepo>) {
        let repo = Arc::new(MemMemoryRepo {
            items: Mutex::new(vec![]),
        });
        (MemoryService::new(repo.clone()), repo)
    }

    #[test]
    fn short_boring_message_is_skipped() {
        let (svc, _repo) = service();
        // 过短且无触发词 → 不产生候选。
        assert!(svc.evaluate("嗯 好", 1).is_none());
        assert!(svc.evaluate("哈哈", 1).is_none());
    }

    #[test]
    fn self_description_classified_semantic() {
        let (svc, _repo) = service();
        let c = svc.evaluate("我喜欢喝咖啡，每天都要来一杯", 1);
        let c = c.expect("应产生候选");
        assert_eq!(c.memory_type, MemoryType::Semantic);
        assert!(c.importance >= crate::application::memory_service::MIN_IMPORTANCE);
    }

    #[test]
    fn relationship_message_classified_relationship() {
        let (svc, _repo) = service();
        let c = svc
            .evaluate("我妹妹下周要结婚了，我要去参加", 1)
            .expect("应产生候选");
        assert_eq!(c.memory_type, MemoryType::Relationship);
    }

    #[tokio::test]
    async fn extract_stores_worthy_message_and_skips_trivial() {
        let (svc, repo) = service();
        // 值得记忆的消息 → 落库 1 条。
        let n = svc
            .extract_and_store(1, Some(10), 99, "我今天考试考砸了，心情不太好")
            .await
            .expect("提取应成功");
        assert_eq!(n, 1);
        // 无关紧要的短消息 → 不落库。
        let n = svc
            .extract_and_store(1, Some(10), 99, "哈哈")
            .await
            .expect("提取应成功");
        assert_eq!(n, 0);

        let stored = repo
            .items
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.character_id == 1)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].memory_type, MemoryType::Episodic);
    }
}
