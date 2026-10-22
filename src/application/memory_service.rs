//! 记忆服务 —— 长期记忆的提取与持久化。
//!
//! 候选记忆的基本生命周期：
//! `消息 → MemoryCandidate → 重要度评估 → 持久化记忆`。
//!
//! 入站规则只生成带来源的候选；后台决策模型提出审核意见，程序验证后才会确认。

use std::sync::Arc;

use chrono::{DateTime, Utc};

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
    /// 正文是否因长度限制被截断。
    pub truncated: bool,
}

/// 一次记忆提取的完整来源身份与时间。
#[derive(Debug, Clone)]
pub struct MemoryExtractionInput<'a> {
    pub character_id: i64,
    pub conversation_id: Option<i64>,
    pub participant_id: i64,
    pub user_id: Option<i64>,
    pub message_text: &'a str,
    pub source_message_id: i64,
    pub source_occurred_at: DateTime<Utc>,
}

/// 记忆服务 —— 负责确定性提取与持久化。
pub struct MemoryService {
    memory_repo: Arc<dyn MemoryRepository>,
}

/// 触发记忆提取的最小区分信息量（字符数）。
const MIN_MESSAGE_CHARS: usize = 12;
/// 记忆落库的重要度阈值。低于它不持久化（避免每条消息都成记忆）。
const MIN_IMPORTANCE: f64 = 0.5;
/// 单个全域用户每天最多生成的候选记忆数量。
const MAX_DAILY_CANDIDATES: i64 = 20;
/// 候选记忆正文的最大字符数。
const MAX_CANDIDATE_CONTENT_CHARS: usize = 200;

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
];
/// 自我描述类触发词（表达偏好 / 身份 / 长期事实 → Semantic 记忆）。
const SEM_TRIGGERS: &[&str] = &[
    "我喜欢",
    "我爱",
    "我讨厌",
    "我不喜欢",
    "我住在",
    "我的名字",
    "我养",
    "我家",
    "我生日",
    "我今年",
    "我想要",
];
/// 事件 / 经历类触发词（表达发生过的事 → Episodic 记忆）。
const EPI_TRIGGERS: &[&str] = &[
    "昨天",
    "上周",
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
    /// 写入候选记忆时同时传入会话参与者和全域用户身份，禁止通过 metadata 隐式反查。
    pub async fn extract_and_store(
        &self,
        input: MemoryExtractionInput<'_>,
    ) -> Result<usize, RuntimeError> {
        let Some(candidate) = self.evaluate(input.message_text, input.participant_id) else {
            return Ok(0);
        };

        let now = Utc::now();
        let day_start = now
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("午夜时间有效")
            .and_utc();
        let mut memory = Memory::new(
            input.character_id,
            input.conversation_id,
            candidate.memory_type,
            candidate.content,
            candidate.importance,
        );
        memory.user_id = input.user_id;
        memory.metadata = serde_json::json!({
            "participant_ids": [candidate.participant_id],
            "source_message_id": input.source_message_id,
            "source_participant_id": input.participant_id,
            "source_user_id": input.user_id,
            "source_conversation_id": input.conversation_id,
            "source_occurred_at": input.source_occurred_at,
            "keywords": candidate.keywords,
            "kind": "statement",
            "status": "candidate",
            "verification": "pending",
            "candidate_reason": "确定性规则命中，等待后台确认",
            "candidate_created_at": now,
            "truncated": candidate.truncated,
            // 直接陈述类候选才允许走程序自动确认；事件类信息仍需更严格的
            // 后台语义审核，避免把随口一说固化成长期事实。
            "visibility": if candidate.memory_type == MemoryType::Episodic { "conversation" } else { "shared" },
            "shared": candidate.memory_type != MemoryType::Episodic,
        });
        Ok(self
            .memory_repo
            .insert_candidate_with_daily_limit(&memory, day_start, MAX_DAILY_CANDIDATES)
            .await?
            .map(|_| 1)
            .unwrap_or(0))
    }

    /// 对消息做确定性重要度评估，返回候选；不足阈值时返回 `None`。
    ///
    /// 这是纯函数，便于单测。规则：
    /// - 必须命中明确触发词；
    /// - 疑问句和明显否定转折 → 不记录；
    /// - 重要度低于阈值 → 不记录；
    /// - 类型判定：关系 > 自我描述 > 事件（默认 episodic）。
    fn evaluate(&self, message_text: &str, participant_id: i64) -> Option<MemoryCandidate> {
        let text = message_text.trim();
        if text.is_empty() {
            return None;
        }

        if is_question_or_negated_turn(text) {
            return None;
        }

        let lower = text.to_lowercase();
        let has_rel = trigger_hit(&lower, REL_TRIGGERS);
        let has_identity = identity_trigger_hit(text);
        let has_sem = trigger_hit(&lower, SEM_TRIGGERS) || has_identity;
        let has_epi = trigger_hit(&lower, EPI_TRIGGERS);

        let char_count = text.chars().count();
        let trigger_count = [has_rel, has_sem, has_epi].iter().filter(|&&b| b).count() as f64;

        // 只有明确触发词才进入候选，长度本身不能绕过筛选。
        if trigger_count == 0.0 {
            return None;
        }

        let mut importance = 0.4 + trigger_count * 0.2;
        if char_count >= 40 {
            importance += 0.2;
        } else if char_count >= 20 {
            importance += 0.1;
        }
        importance = importance.clamp(0.0, 1.0);

        if char_count < MIN_MESSAGE_CHARS && !has_identity {
            return None;
        }
        if importance < MIN_IMPORTANCE {
            return None;
        }

        // 发生过的事情优先按经历处理。像“我们昨天吵架了”同时包含
        // 关系词和时间词，不能因为“我们”命中就被误判为可跨会话共享的关系事实。
        let memory_type = if has_epi {
            MemoryType::Episodic
        } else if has_rel {
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
        if has_identity {
            keywords.push("身份陈述".to_string());
        }

        let truncated = char_count > MAX_CANDIDATE_CONTENT_CHARS;
        let content = text.chars().take(MAX_CANDIDATE_CONTENT_CHARS).collect();

        Some(MemoryCandidate {
            content,
            memory_type,
            importance,
            keywords,
            participant_id: Some(participant_id),
            truncated,
        })
    }
}

/// 触发词命中检测（任一命中即返回 true）。
fn trigger_hit(text: &str, triggers: &[&str]) -> bool {
    triggers.iter().any(|t| text.contains(t))
}

/// 只在句首或标点分隔后的句首识别受限身份陈述，避免恢复裸子串匹配。
fn identity_trigger_hit(text: &str) -> bool {
    text.split(|ch| {
        matches!(
            ch,
            '，' | ',' | '。' | '.' | '！' | '!' | '？' | '?' | '；' | ';' | '\n'
        )
    })
    .map(str::trim)
    .any(|segment| {
        if let Some(rest) = segment.strip_prefix("我叫") {
            return identity_value_is_valid(rest);
        }
        if let Some(rest) = segment.strip_prefix("我是") {
            return identity_value_is_valid(rest)
                && !rest.starts_with("不是")
                && !rest.starts_with('说');
        }
        if let Some(rest) = segment.strip_prefix("我的职业是") {
            return rest.chars().count() >= 2;
        }
        let Some(rest) = segment.strip_prefix("我在") else {
            return false;
        };
        !rest.is_empty()
            && ["工作", "上学", "读"]
                .iter()
                .any(|verb| rest.contains(verb))
    })
}

fn identity_value_is_valid(value: &str) -> bool {
    value.chars().count() >= 2
        && !value.starts_with("吗")
        && !value.starts_with("呢")
        && !value.starts_with('？')
        && !value.starts_with('?')
}

/// 排除疑问句和把否定转折当作稳定陈述的常见情况。
fn is_question_or_negated_turn(text: &str) -> bool {
    text.contains('吗')
        || text.contains('呢')
        || text.ends_with('？')
        || text.ends_with('?')
        || text.starts_with("不是")
        || text.starts_with("并不是")
        || text.contains("但是")
        || text.contains("不过")
        || text.contains("然而")
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
        async fn find_by_id(&self, id: i64) -> Result<Option<Memory>, RepositoryError> {
            Ok(self
                .items
                .lock()
                .unwrap()
                .iter()
                .find(|m| m.id == id)
                .cloned())
        }
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
        async fn search_by_keywords(
            &self,
            character_id: i64,
            keywords: &[String],
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(self
                .items
                .lock()
                .unwrap()
                .iter()
                .filter(|m| {
                    m.character_id == character_id
                        && keywords
                            .iter()
                            .any(|kw| m.content.to_lowercase().contains(&kw.to_lowercase()))
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
        async fn find_all(&self, _limit: i64) -> Result<Vec<Memory>, RepositoryError> {
            Ok(Vec::new())
        }
        async fn find_by_conversation_id(
            &self,
            _conversation_id: i64,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(Vec::new())
        }
        async fn find_filtered(
            &self,
            _character_id: Option<i64>,
            _conversation_id: Option<i64>,
            _memory_type: Option<crate::domain::memory::MemoryType>,
            _limit: i64,
        ) -> Result<Vec<Memory>, RepositoryError> {
            Ok(Vec::new())
        }

        async fn count_candidates_for_user_since(
            &self,
            user_id: i64,
            since: DateTime<Utc>,
        ) -> Result<i64, RepositoryError> {
            Ok(self
                .items
                .lock()
                .unwrap()
                .iter()
                .filter(|memory| {
                    memory.user_id == Some(user_id)
                        && memory.created_at >= since
                        && memory.metadata.get("candidate_created_at").is_some()
                })
                .count() as i64)
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
    fn evaluate_filters_noise_questions_and_negated_turns() {
        let (svc, _repo) = service();
        let cases = [
            ("今天好累啊，先躺一会儿", false),
            ("这周工作特别忙，回家只想睡觉", false),
            ("我喜欢喝咖啡，也喜欢自己在家手冲", true),
            ("我喜欢喝咖啡吗", false),
            ("我喜欢喝咖啡？", false),
            ("我喜欢喝咖啡，但是最近戒了", false),
        ];
        for (message, expected) in cases {
            assert_eq!(
                svc.evaluate(message, 1).is_some(),
                expected,
                "消息：{message}"
            );
        }
    }

    #[test]
    fn identity_triggers_are_restricted_and_table_driven() {
        let (svc, _repo) = service();
        let cases = [
            ("我是大二学生", true),
            ("我叫小明", true),
            ("我在上海工作", true),
            ("我是不是该睡了", false),
            ("你是谁", false),
            ("我是说今天好累", false),
            ("我会不会迟到", false),
            ("我们走吧", false),
        ];
        for (message, expected) in cases {
            assert_eq!(
                svc.evaluate(message, 1).is_some(),
                expected,
                "消息：{message}"
            );
        }
    }

    #[test]
    fn long_candidate_is_truncated_and_marked() {
        let (svc, _repo) = service();
        let message = format!("我喜欢{}", "很".repeat(MAX_CANDIDATE_CONTENT_CHARS));
        let candidate = svc.evaluate(&message, 1).expect("明确自我陈述应产生候选");
        assert_eq!(
            candidate.content.chars().count(),
            MAX_CANDIDATE_CONTENT_CHARS
        );
        assert!(candidate.truncated);
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

    #[test]
    fn dated_relationship_event_stays_episodic() {
        let (svc, _repo) = service();
        let c = svc
            .evaluate("我们昨天吵架了，后来谁都没有再说话", 1)
            .expect("应产生候选");
        assert_eq!(c.memory_type, MemoryType::Episodic);
    }

    #[tokio::test]
    async fn extract_stores_worthy_message_and_skips_trivial() {
        let (svc, repo) = service();
        // 值得记忆的消息 → 落库 1 条。
        let n = svc
            .extract_and_store(MemoryExtractionInput {
                character_id: 1,
                conversation_id: Some(10),
                participant_id: 99,
                user_id: Some(500),
                message_text: "我考试考砸了，心情不太好",
                source_message_id: 100,
                source_occurred_at: Utc::now(),
            })
            .await
            .expect("提取应成功");
        assert_eq!(n, 1);
        // 无关紧要的短消息 → 不落库。
        let n = svc
            .extract_and_store(MemoryExtractionInput {
                character_id: 1,
                conversation_id: Some(10),
                participant_id: 99,
                user_id: Some(500),
                message_text: "哈哈",
                source_message_id: 101,
                source_occurred_at: Utc::now(),
            })
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
        assert_eq!(stored[0].user_id, Some(500));
        assert_eq!(stored[0].metadata["truncated"], false);
    }

    #[tokio::test]
    async fn daily_candidate_limit_is_enforced_per_user() {
        let (svc, repo) = service();
        for source_message_id in 0..MAX_DAILY_CANDIDATES {
            let count = svc
                .extract_and_store(MemoryExtractionInput {
                    character_id: 1,
                    conversation_id: Some(10),
                    participant_id: 99,
                    user_id: Some(500),
                    message_text: "我喜欢喝咖啡，每天都要来一杯",
                    source_message_id,
                    source_occurred_at: Utc::now(),
                })
                .await
                .expect("候选提取应成功");
            assert_eq!(count, 1);
        }
        let count = svc
            .extract_and_store(MemoryExtractionInput {
                character_id: 1,
                conversation_id: Some(10),
                participant_id: 99,
                user_id: Some(500),
                message_text: "我喜欢喝咖啡，每天都要来一杯",
                source_message_id: 1000,
                source_occurred_at: Utc::now(),
            })
            .await
            .expect("超限应静默跳过");
        assert_eq!(count, 0);
        assert_eq!(
            repo.items.lock().unwrap().len(),
            MAX_DAILY_CANDIDATES as usize
        );
    }
}
