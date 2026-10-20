//! 决策系统——Beta C2 Decision Pipeline 核心类型。
//!
//! 设计原则：
//! - Evaluation Input Snapshot：Repository 数据一次性加载后 freeze，后续所有阶段使用同一份数据
//! - 不要求 ACID Snapshot，只保证 sequential reads → freeze → immutable downstream
//! - Built-in Decision 完整保留 6 字段，LLM 全部失败时精确恢复
//! - Decision LLM 是 advisory override，不是主行为引擎

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::application::llm_scheduler::LlmScheduler;

use crate::domain::behavior::{BehaviorAction, BehaviorDecision};
use crate::domain::character::{CharacterState, ConversationState};
use crate::domain::emotion::Mood;
use crate::domain::relationship::Relationship;
use crate::error::DomainError;

// ============================================================================
// DecisionEvaluator trait
// ============================================================================

/// 行为决策评估 trait——比 BehaviorEngine 更丰富的评估接口。
///
/// 用途：
/// - DecisionOrchestrator 需要同时获取 BehaviorDecision 和 DecisionContext
/// - 不修改 BehaviorEngine trait，保持向后兼容
///
/// BehaviorEngine 实现者（RuleBehaviorEngine）同时实现此 trait。
#[async_trait::async_trait]
pub trait DecisionEvaluator: Send + Sync {
    /// 执行一次完整的行为评估，返回决策结果和决策依据快照。
    async fn evaluate_response(
        &self,
        character_id: i64,
        conversation_id: i64,
        message_content: &str,
        is_mentioned: bool,
        participant_id: Option<i64>,
    ) -> Result<RuleBehaviorEngineOutput, DomainError>;

    /// 执行一次完整的主动行为评估。
    async fn evaluate_proactive(
        &self,
        character_id: i64,
        conversation_id: i64,
    ) -> Result<RuleBehaviorEngineOutput, DomainError>;
}

// ============================================================================
// Decision Situation
// ============================================================================

/// 决策情境——区分 Response 和 Proactive 两种入口。
///
/// 两者共享 Pipeline，但输入事实不同。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DecisionSituation {
    /// 被动回复决策。
    Response {
        message_content: String,
        is_mentioned: bool,
        participant_id: Option<i64>,
    },
    /// 主动行为决策。
    #[allow(dead_code)]
    Proactive,
}

// ============================================================================
// RuleDecisionInput — Frozen Evaluation Input
// ============================================================================

/// 一次性构造的决策输入快照。
///
/// 在构造完成后不可变，用于驱动 Built-in Decision 和 LLM DecisionContext。
///
/// **注意**：当前 Repository 层不提供 ACID Snapshot Isolation。
/// `build_decision_input()` 顺序执行多个 repository 查询，
/// 如果查询之间状态发生变化，两个派生结果都会一致地反映最新状态。
/// 这是 practical limitation，不在 C2 范围内修复。
#[derive(Debug, Clone)]
pub struct RuleDecisionInput {
    /// 角色绑定。
    pub binding: Option<crate::domain::character::CharacterBinding>,
    /// 情绪状态。
    #[allow(dead_code)]
    pub mood: Option<Mood>,
    /// 关系状态（仅当前 participant）。
    pub relationship: Option<Relationship>,
    /// 当前角色的所有关系（用于 proactive 决策计算最佳亲密度）。
    pub all_character_relationships: Vec<Relationship>,
    /// 角色全局状态。
    pub character_state: Option<CharacterState>,
    /// 会话级状态。
    pub conversation_state: Option<ConversationState>,
    /// 行为状态。
    #[allow(dead_code)]
    pub behavior_state: Option<crate::domain::character::BehaviorState>,
    /// 调用参数快照。
    pub character_id: i64,
    pub conversation_id: i64,
    pub message_content: String,
    pub is_mentioned: bool,
    pub participant_id: Option<i64>,
    /// 规则引擎已经计算出的硬静默状态，决策模型不能覆盖。
    pub in_mute_window: bool,
    /// 决策时刻。
    pub evaluated_at: DateTime<Utc>,
}

// ============================================================================
// RuleBehaviorEngineOutput
// ============================================================================

/// RuleBehaviorEngine 的完整输出。
///
/// 包含 Built-in Decision 和原始决策输入快照。
#[derive(Debug, Clone)]
pub struct RuleBehaviorEngineOutput {
    /// 完整决策结果（6 字段）。
    pub decision: BehaviorDecision,
    /// 决策依据快照（供 LLM DecisionContext 使用）。
    pub input: RuleDecisionInput,
}

// ============================================================================
// BuiltInDecision — 完整原始决策 wrapper
// ============================================================================

/// Built-in Decision Model 的原始决策（完整 6 字段）。
///
/// 当 LLM 全部失败时，final == original（字段完全相等）。
/// 不是 subset，是完整 BehaviorDecision 的 owned clone。
#[derive(Debug, Clone)]
pub struct BuiltInDecision {
    /// 完整 BehaviorDecision。
    pub behavior: BehaviorDecision,
}

impl BuiltInDecision {
    pub fn from_behavior(behavior: BehaviorDecision) -> Self {
        Self { behavior }
    }

    /// 从 RuleBehaviorEngineOutput 构造。
    pub fn from_output(output: &RuleBehaviorEngineOutput) -> Self {
        Self {
            behavior: output.decision.clone(),
        }
    }
}

// ============================================================================
// DecisionContext — LLM-facing DTO
// ============================================================================

/// BehaviorBinding 中与决策直接相关的配置子集。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BehaviorConfig {
    /// 当前会话是否已绑定该角色。
    pub has_binding: bool,
    pub reply_mode: crate::domain::character::ReplyMode,
    pub proactive_enabled: bool,
    /// 是否处于静默时段。
    pub in_mute_window: bool,
    pub cross_reply_enabled: bool,
}

/// 关系的确定性 derived state。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipState {
    /// 亲密度（综合值）。
    pub closeness: f64,
    /// 是否为疏远关系（closeness < 0.25）。
    pub is_distant: bool,
    /// 是否为亲近关系（closeness > 0.6）。
    pub is_close: bool,
    /// 厌烦度 > 0.6。
    pub is_annoyed: bool,
    /// 高好感（affection > 0.7）。
    pub high_affection: bool,
    /// 原始值（供 LLM 参考）。
    pub familiarity: f64,
    pub affection: f64,
    pub trust: f64,
    pub intimacy: f64,
    pub annoyance: f64,
}

/// 角色全局状态快照。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CharacterStateSnapshot {
    /// 精力（0-100）。
    pub energy: f64,
    /// 压力（0-100）。
    pub stress: f64,
    pub is_low_energy: bool,
    pub is_high_stress: bool,
}

/// 会话级状态快照。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationStateSnapshot {
    pub energy: f64,
    pub stress: f64,
}

/// 供 Decision LLM 使用的完整上下文。
///
/// 由 RuleDecisionInput 转换而来，只暴露 LLM 需要知道的窄字段。
/// 不持有 Repository / Service / Runtime / Provider。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionContext {
    pub situation: DecisionSituation,
    pub character_id: i64,
    pub conversation_id: i64,
    pub behavior_config: BehaviorConfig,
    pub relationship_state: RelationshipState,
    pub character_state: CharacterStateSnapshot,
    pub conversation_state: ConversationStateSnapshot,
}

impl DecisionContext {
    /// 从 RuleDecisionInput 构造。
    ///
    /// 在 RuleBehaviorEngine::evaluate_response() 返回 RuleBehaviorEngineOutput 后，
    /// 由 DecisionOrchestrator 调用此方法将 input 转换为 LLM-facing context。
    pub fn from_input(input: &RuleDecisionInput, situation: DecisionSituation) -> Self {
        let closeness = input
            .relationship
            .as_ref()
            .map(|r| (r.familiarity + r.affection + r.trust + r.intimacy) / 4.0)
            .unwrap_or(0.0);

        let global_energy = input
            .character_state
            .as_ref()
            .map(|s| s.energy)
            .unwrap_or(CharacterState::default().energy);
        let scoped_energy = input
            .conversation_state
            .as_ref()
            .map(|s| s.energy)
            .unwrap_or(ConversationState::default().energy);
        let energy = global_energy.min(scoped_energy);

        let global_stress = input
            .character_state
            .as_ref()
            .map(|s| s.stress)
            .unwrap_or(CharacterState::default().stress);
        let scoped_stress = input
            .conversation_state
            .as_ref()
            .map(|s| s.stress)
            .unwrap_or(ConversationState::default().stress);
        let stress = global_stress.max(scoped_stress);

        let reply_mode = input
            .binding
            .as_ref()
            .map(|b| b.reply_mode)
            .unwrap_or(crate::domain::character::ReplyMode::MentionOnly);

        DecisionContext {
            situation,
            character_id: input.character_id,
            conversation_id: input.conversation_id,
            behavior_config: BehaviorConfig {
                has_binding: input.binding.is_some(),
                reply_mode,
                proactive_enabled: input
                    .binding
                    .as_ref()
                    .map(|b| b.proactive_enabled)
                    .unwrap_or(false),
                in_mute_window: input.in_mute_window,
                cross_reply_enabled: input
                    .binding
                    .as_ref()
                    .map(|b| b.cross_reply_enabled)
                    .unwrap_or(false),
            },
            relationship_state: RelationshipState {
                closeness,
                is_distant: closeness < 0.25,
                is_close: closeness > 0.6,
                is_annoyed: input
                    .relationship
                    .as_ref()
                    .map(|r| r.annoyance > 0.6)
                    .unwrap_or(false),
                high_affection: input
                    .relationship
                    .as_ref()
                    .map(|r| r.affection > 0.7)
                    .unwrap_or(false),
                familiarity: input
                    .relationship
                    .as_ref()
                    .map(|r| r.familiarity)
                    .unwrap_or(0.0),
                affection: input
                    .relationship
                    .as_ref()
                    .map(|r| r.affection)
                    .unwrap_or(0.0),
                trust: input.relationship.as_ref().map(|r| r.trust).unwrap_or(0.0),
                intimacy: input
                    .relationship
                    .as_ref()
                    .map(|r| r.intimacy)
                    .unwrap_or(0.0),
                annoyance: input
                    .relationship
                    .as_ref()
                    .map(|r| r.annoyance)
                    .unwrap_or(0.0),
            },
            character_state: CharacterStateSnapshot {
                energy,
                stress,
                is_low_energy: energy < 30.0,
                is_high_stress: stress > 60.0,
            },
            conversation_state: ConversationStateSnapshot {
                energy: scoped_energy,
                stress: scoped_stress,
            },
        }
    }
}

// ============================================================================
// LlmAssistance
// ============================================================================

/// LLM 协助需求。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LlmAssistance {
    NotRequired,
    Required { reason: String },
}

/// LLM 协助触发器 trait。
///
/// 输入：DecisionContext + BuiltInDecision
/// 输出：LlmAssistance
///
/// 设计原则：
/// - 必须是确定性函数（相同输入 → 相同输出）
/// - 可以是 NoOp（始终 NotRequired），用于 MVP / 测试
pub trait LlmAssistanceTrigger: Send + Sync {
    fn evaluate(&self, context: &DecisionContext, built_in: &BuiltInDecision) -> LlmAssistance;
}

/// 默认实现：始终 NotRequired（MVP 和测试用）。
#[derive(Debug, Clone, Default)]
pub struct NoOpTrigger;

impl LlmAssistanceTrigger for NoOpTrigger {
    fn evaluate(&self, _: &DecisionContext, _: &BuiltInDecision) -> LlmAssistance {
        LlmAssistance::NotRequired
    }
}

/// 配置启用决策模型时使用的低成本触发器。
///
/// 明确提及已经是程序确定的高优先级请求，不重复调用决策模型；
/// 普通消息和规则处于边界的消息才交给决策模型做 advisory 判断。
#[derive(Debug, Clone, Copy, Default)]
pub struct HeuristicTrigger;

impl LlmAssistanceTrigger for HeuristicTrigger {
    fn evaluate(&self, context: &DecisionContext, built_in: &BuiltInDecision) -> LlmAssistance {
        match &context.situation {
            DecisionSituation::Response {
                is_mentioned: true, ..
            } => LlmAssistance::NotRequired,
            _ if !context.behavior_config.has_binding => LlmAssistance::NotRequired,
            DecisionSituation::Response { .. } => {
                let borderline = built_in.behavior.action == BehaviorAction::Ignore
                    || context.relationship_state.is_distant
                    || context.character_state.is_low_energy
                    || context.character_state.is_high_stress;
                if borderline {
                    LlmAssistance::Required {
                        reason: "普通消息处于参与边界，需要决策模型复核".to_string(),
                    }
                } else {
                    LlmAssistance::NotRequired
                }
            }
            DecisionSituation::Proactive
                if built_in.behavior.action == BehaviorAction::InitiateProactive =>
            {
                LlmAssistance::Required {
                    reason: "规则引擎允许主动行为，需要决策模型复核".to_string(),
                }
            }
            DecisionSituation::Proactive => LlmAssistance::NotRequired,
        }
    }
}

// ============================================================================
// DecisionResult / DecisionAction
// ============================================================================

/// Decision LLM 返回的结构化建议。
///
/// 只包含 LLM 可以合法 override 的窄字段。
/// 其他字段由 BuiltInDecision 完整保留。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionResult {
    /// 建议的动作 override。
    pub action: DecisionAction,
    /// 每个 typed question 的原始答案，供审计和回放使用。
    pub questions: Vec<DecisionQuestionResult>,
    /// 聚合后的动作概率。
    pub probabilities: BTreeMap<String, f64>,
    /// 所有必需问题通过后的保守置信度。
    pub confidence: Option<f64>,
    /// Provider 返回的模型版本。
    pub model_version: String,
}

/// TypeSafe 问题的结构化答案。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionQuestionResult {
    pub name: String,
    pub question_type: String,
    pub value: serde_json::Value,
    #[serde(default)]
    pub probabilities: BTreeMap<String, f64>,
    pub confidence: f64,
}

/// Decision LLM 可以建议的动作 override。
///
/// 这是窄边界 enum，不允许任意字符串或 backend command。
/// 语义：advisory override。Built-in Decision 是 baseline。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DecisionAction {
    /// 保持 Built-in 决策（无 override）。
    Keep,
    /// 建议 Reply。
    SuggestReply,
    /// 建议 Ignore。
    SuggestIgnore,
    /// 建议在原始 delay 基础上增加 delta_ms。
    AdjustDelay { delta_ms: i64 },
}

impl DecisionResult {
    pub fn keep() -> Self {
        Self {
            action: DecisionAction::Keep,
            questions: Vec::new(),
            probabilities: BTreeMap::new(),
            confidence: Some(1.0),
            model_version: "builtin".to_string(),
        }
    }
}

// ============================================================================
// DecisionFailure
// ============================================================================

/// 单次 LLM 调用的失败类型。
#[derive(Debug, Clone)]
pub enum DecisionFailure {
    /// 网络/传输错误。
    Transport(String),
    /// Provider 返回非 200 状态码。
    ProviderError(u16, String),
    /// 所有 Provider 超时。
    Timeout,
    /// 响应无法解析为有效 JSON。
    JsonParse(String),
    /// JSON 解析成功但缺少必需字段或类型错误。
    SchemaValidation(String),
    /// JSON 解析成功但语义验证失败（由 Validator 返回）。
    SemanticValidation(Vec<ValidationError>),
    /// 所有 Provider 均失败。
    AllProvidersFailed,
}

impl std::fmt::Display for DecisionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(s) => write!(f, "transport: {s}"),
            Self::ProviderError(code, msg) => write!(f, "provider_error({code}): {msg}"),
            Self::Timeout => write!(f, "timeout"),
            Self::JsonParse(s) => write!(f, "json_parse: {s}"),
            Self::SchemaValidation(s) => write!(f, "schema_validation: {s}"),
            Self::SemanticValidation(errs) => {
                write!(f, "semantic_validation({} errors)", errs.len())
            }
            Self::AllProvidersFailed => write!(f, "all_providers_failed"),
        }
    }
}

// ============================================================================
// ValidationError
// ============================================================================

/// 验证错误详情。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationError {
    pub field: String,
    pub expected: String,
    pub actual: String,
}

// ============================================================================
// DecisionResultValidator
// ============================================================================

/// DecisionResult 验证器。
///
/// 强制 LLM Result 的权限边界。
/// 不重新计算行为逻辑，不复制 Hard Constraint Engine。
#[derive(Debug)]
pub struct DecisionResultValidator {
    /// 最大允许的 delay delta 绝对值（毫秒）。
    max_delay_delta: i64,
    /// 最大允许的 final delay（毫秒）。
    max_delay: u64,
    /// 最大允许的 confidence。
    max_confidence: f64,
    min_confidence: f64,
}

impl DecisionResultValidator {
    pub fn new(max_delay_delta: i64, max_delay: u64) -> Self {
        Self {
            max_delay_delta,
            max_delay,
            max_confidence: 1.0,
            min_confidence: 0.65,
        }
    }

    /// 验证 DecisionResult 在当前上下文中是否可执行。
    ///
    /// 返回 Ok(()) 或错误列表。
    pub fn validate(
        &self,
        result: &DecisionResult,
        context: &DecisionContext,
        original: &BuiltInDecision,
        policy: &dyn DecisionOverridePolicy,
    ) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();

        // 1. delay_delta 范围
        if let DecisionAction::AdjustDelay { delta_ms } = &result.action {
            if delta_ms.unsigned_abs() > self.max_delay_delta.max(0) as u64 {
                errors.push(ValidationError {
                    field: "delta_ms".to_string(),
                    expected: format!("|delta| ≤ {}", self.max_delay_delta),
                    actual: delta_ms.to_string(),
                });
            }
            let final_delay = original.behavior.delay_ms as i128 + *delta_ms as i128;
            if final_delay < 0 || final_delay > self.max_delay as i128 {
                errors.push(ValidationError {
                    field: "final_delay".to_string(),
                    expected: format!("0 ≤ final ≤ {}", self.max_delay),
                    actual: final_delay.to_string(),
                });
            }
        }

        // 2. situation 兼容性
        if let (DecisionAction::SuggestReply, DecisionSituation::Proactive) =
            (&result.action, &context.situation)
        {
            errors.push(ValidationError {
                field: "action".to_string(),
                expected: "Proactive 情境不允许 SuggestReply".to_string(),
                actual: "SuggestReply".to_string(),
            });
        }
        if let (DecisionAction::AdjustDelay { .. }, DecisionSituation::Proactive) =
            (&result.action, &context.situation)
        {
            errors.push(ValidationError {
                field: "action".to_string(),
                expected: "Proactive 情境只允许 Keep 或 SuggestIgnore".to_string(),
                actual: "AdjustDelay".to_string(),
            });
        }

        if let DecisionAction::SuggestReply = result.action {
            let allowed = match context.situation {
                DecisionSituation::Proactive => false,
                DecisionSituation::Response { is_mentioned, .. } => {
                    context.behavior_config.has_binding
                        && !context.behavior_config.in_mute_window
                        && (is_mentioned
                            || context.behavior_config.reply_mode
                                != crate::domain::character::ReplyMode::MentionOnly)
                }
            };
            if !allowed {
                errors.push(ValidationError {
                    field: "action".to_string(),
                    expected: "reply must satisfy binding, mute and reply-mode constraints"
                        .to_string(),
                    actual: "SuggestReply".to_string(),
                });
            }
        }
        if let DecisionAction::SuggestIgnore = result.action {
            if matches!(
                context.situation,
                DecisionSituation::Response {
                    is_mentioned: true,
                    ..
                }
            ) {
                errors.push(ValidationError {
                    field: "action".to_string(),
                    expected: "explicitly mentioned messages cannot be ignored".to_string(),
                    actual: "SuggestIgnore".to_string(),
                });
            }
        }

        // 3. confidence 与 typed question 完整性
        if result
            .confidence
            .is_some_and(|confidence| !(0.0..=self.max_confidence).contains(&confidence))
        {
            errors.push(ValidationError {
                field: "confidence".to_string(),
                expected: format!("0.0 ≤ confidence ≤ {}", self.max_confidence),
                actual: result.confidence.unwrap_or_default().to_string(),
            });
        }
        if !matches!(result.action, DecisionAction::Keep)
            && result.confidence.unwrap_or_default() < self.min_confidence
        {
            errors.push(ValidationError {
                field: "confidence".to_string(),
                expected: format!("confidence ≥ {}", self.min_confidence),
                actual: result.confidence.unwrap_or_default().to_string(),
            });
        }
        if !matches!(result.action, DecisionAction::Keep) {
            let constraints_probability = result
                .questions
                .iter()
                .find(|question| question.name == "hard_constraints")
                .and_then(|question| question.value.as_f64())
                .unwrap_or(0.0);
            if constraints_probability < 0.9 {
                errors.push(ValidationError {
                    field: "hard_constraints".to_string(),
                    expected: "noul probability ≥ 0.9".to_string(),
                    actual: constraints_probability.to_string(),
                });
            }
        }

        // 4. override policy
        if !policy.allow_action_override(&original.behavior.action, &result.action) {
            errors.push(ValidationError {
                field: "action".to_string(),
                expected: "override policy denied".to_string(),
                actual: format!("{:?}", result.action),
            });
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

// ============================================================================
// DecisionOverridePolicy
// ============================================================================

/// Decision Override 策略 trait。
///
/// 决定 LLM 建议的 action override 是否被允许。
pub trait DecisionOverridePolicy: Send + Sync {
    fn allow_action_override(&self, original: &BehaviorAction, suggested: &DecisionAction) -> bool;
}

/// 默认策略：允许所有已定义的 override。
///
/// 这是当前 Beta/C2 的 policy choice，不是因为 LLM 天生拥有完全行为控制权。
/// 未来可以替换为更严格的 policy 而无需重构整个 Pipeline。
#[derive(Debug, Clone, Default)]
pub struct DefaultOverridePolicy;

impl DecisionOverridePolicy for DefaultOverridePolicy {
    fn allow_action_override(
        &self,
        _original: &BehaviorAction,
        _suggested: &DecisionAction,
    ) -> bool {
        // 当前 Beta：允许所有已定义 override
        true
    }
}

// ============================================================================
// DecisionLlmService — C2-2 实现
// ============================================================================

/// Decision LLM 服务 trait。
///
/// 将 DecisionContext 和 BuiltInDecision 发送给 LLM 并解析结构化 DecisionResult。
/// 不负责 Primary/Backup 路由（由 Pipeline 负责）。
#[async_trait::async_trait]
pub trait DecisionLlmService: Send + Sync {
    async fn call(
        &self,
        ctx: &DecisionContext,
        built_in: &BuiltInDecision,
    ) -> Result<DecisionResult, DecisionFailure>;
    fn name(&self) -> &str;
}

/// Decision LLM 服务实现——通过 LlmScheduler 发送请求。
///
/// 使用 `metadata["llm_role"] = "decision"` 进行 role-based 路由。
pub struct DecisionLlmServiceImpl {
    scheduler: Arc<crate::application::llm_scheduler::DefaultLlmScheduler>,
}

impl DecisionLlmServiceImpl {
    #[allow(dead_code)]
    pub fn new(scheduler: Arc<crate::application::llm_scheduler::DefaultLlmScheduler>) -> Self {
        Self { scheduler }
    }

    /// 构造 Decision LLM 的 system prompt。
    fn build_system_prompt(&self) -> String {
        r#"You are a behavioral decision advisor for a character runtime system.

## Your Role
You analyze the current situation and provide an ADVISORY decision override to the built-in decision model. You do NOT execute actions, generate chat responses, or modify system state.

## Available Override Actions
- "keep": Accept the built-in decision without any change.
- "suggest_reply": Override built-in's action to Reply (if built-in was Ignore).
- "suggest_ignore": Override built-in's action to Ignore (if built-in was Reply).
- "adjust_delay": Adjust the built-in's delay by adding delta_ms (can be negative to reduce delay).

## Output Format (JSON)
The provider receives typed questions and must answer them structurally. Never emit a chat completion object or free-form explanation.

## Decision Rules
- "keep" should be used when the built-in decision is already optimal.
- "suggest_reply" / "suggest_ignore" should be used sparingly and with strong justification.
- "adjust_delay" delay_delta_ms must be between -5000 and +5000 milliseconds.
- When the situation is "Proactive", only "keep" and "suggest_ignore" are allowed. Do not suggest a reply or delay change.
- NEVER suggest actions that would violate hard constraints (no binding, muted context, etc.).

## Strict Prohibitions
- Do NOT generate any chat response content.
- Do NOT call tools or external functions.
- Do NOT modify relationship, mood, or state directly.
- Do NOT override hard constraints encoded in the situation.
- Do NOT respond with any text outside the JSON object."#.to_string()
    }

    /// 构造 user message——包含 DecisionContext 和 BuiltInDecision。
    fn build_user_message(&self, ctx: &DecisionContext, built_in: &BuiltInDecision) -> String {
        let ctx_json = serde_json::to_string_pretty(ctx).unwrap_or_else(|_| "{}".to_string());
        let built_in_json =
            serde_json::to_string_pretty(&built_in.behavior).unwrap_or_else(|_| "{}".to_string());
        format!(
            "## Current Situation\n{}\n\n## Built-in Decision (baseline)\n{}\n\nProvide your advisory override as JSON.",
            ctx_json, built_in_json
        )
    }

    /// 解析 LLM 返回的 JSON 响应。
    fn parse_response(&self, content: &str) -> Result<DecisionResult, DecisionFailure> {
        Self::parse_response_content(content)
    }

    fn parse_response_content(content: &str) -> Result<DecisionResult, DecisionFailure> {
        let raw: serde_json::Value =
            serde_json::from_str(content).map_err(|e| DecisionFailure::JsonParse(e.to_string()))?;
        let model_version = raw
            .get("model")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string();
        let answers = raw
            .get("answers")
            .and_then(|value| value.as_object())
            .ok_or_else(|| DecisionFailure::SchemaValidation("Jev 响应缺少 answers 对象".into()))?;
        let mut questions = Vec::with_capacity(answers.len());
        let mut probabilities = BTreeMap::new();
        let mut confidence = 1.0_f64;
        for (name, answer) in answers {
            let question_type = answer
                .get("type")
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    DecisionFailure::SchemaValidation(format!("answers.{name} 缺少 type"))
                })?;
            let value = match question_type {
                "choice" => answer.get("choice").cloned().ok_or_else(|| {
                    DecisionFailure::SchemaValidation(format!("answers.{name} 缺少 choice"))
                })?,
                "score" => answer.get("score").cloned().ok_or_else(|| {
                    DecisionFailure::SchemaValidation(format!("answers.{name} 缺少 score"))
                })?,
                "noul" => answer
                    .get("noul")
                    .or_else(|| answer.get("allowed"))
                    .cloned()
                    .unwrap_or(serde_json::Value::Number(serde_json::Number::from(0))),
                other => {
                    return Err(DecisionFailure::SchemaValidation(format!(
                        "answers.{name} 使用不支持的问题类型 {other}"
                    )))
                }
            };
            let question_confidence = answer
                .get("confidence")
                .and_then(|value| value.as_f64())
                .unwrap_or(1.0);
            confidence = confidence.min(question_confidence);
            if let Some(raw_probabilities) = answer
                .get("probabilities")
                .and_then(|value| value.as_object())
            {
                for (key, probability) in raw_probabilities {
                    if let Some(probability) = probability.as_f64() {
                        probabilities.insert(format!("{name}:{key}"), probability);
                    }
                }
            }
            questions.push(DecisionQuestionResult {
                name: name.clone(),
                question_type: question_type.to_string(),
                value,
                probabilities: answer
                    .get("probabilities")
                    .and_then(|value| serde_json::from_value(value.clone()).ok())
                    .unwrap_or_default(),
                confidence: question_confidence,
            });
        }
        let action_str = answers
            .get("action")
            .and_then(|answer| answer.get("choice"))
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                DecisionFailure::SchemaValidation("answers.action.choice 缺失".into())
            })?;

        let action = match action_str {
            "keep" => DecisionAction::Keep,
            "suggest_reply" => DecisionAction::SuggestReply,
            "suggest_ignore" => DecisionAction::SuggestIgnore,
            "adjust_delay" => {
                let score = answers
                    .get("delay_delta_ms")
                    .and_then(|answer| answer.get("score"))
                    .ok_or_else(|| {
                        DecisionFailure::SchemaValidation(
                            "adjust_delay requires score question delay_delta_ms".to_string(),
                        )
                    })?;
                let score = score.as_f64().ok_or_else(|| {
                    DecisionFailure::SchemaValidation("delay_delta_ms.score 必须是数字".to_string())
                })?;
                if !(0.0..=10.0).contains(&score) {
                    return Err(DecisionFailure::SchemaValidation(
                        "delay_delta_ms.score 必须在 0 到 10 之间".to_string(),
                    ));
                }
                let delta = ((score - 5.0) * 1_000.0).round() as i64;
                DecisionAction::AdjustDelay { delta_ms: delta }
            }
            other => {
                return Err(DecisionFailure::SchemaValidation(format!(
                    "unknown action '{other}': must be one of: keep, suggest_reply, suggest_ignore, adjust_delay"
                )));
            }
        };

        Ok(DecisionResult {
            action,
            questions,
            probabilities,
            confidence: Some(confidence),
            model_version,
        })
    }

    fn build_typesafe_questions(is_proactive: bool) -> serde_json::Map<String, serde_json::Value> {
        let mut criteria = serde_json::Map::from_iter([
            ("keep".into(), serde_json::json!("保留规则引擎的决定。")),
            (
                "suggest_reply".into(),
                serde_json::json!("建议回复当前消息；仅在程序硬规则允许时生效。"),
            ),
            (
                "suggest_ignore".into(),
                serde_json::json!("建议忽略当前消息或取消本次主动行为。"),
            ),
        ]);
        if is_proactive {
            criteria.remove("suggest_reply");
        } else {
            criteria.insert(
                "adjust_delay".into(),
                serde_json::json!(
                    "只调整规则决定的等待时间；变化范围由 delay_delta_ms score 问题限定。"
                ),
            );
        }

        let mut questions = serde_json::Map::from_iter([
            (
                "action".into(),
                serde_json::json!({
                    "type": "choice",
                    "instructions": if is_proactive {
                        "规则允许主动行为。请选择保留本次主动行为，或取消本次主动行为。"
                    } else {
                        "请在不违反程序硬约束的前提下，选择保留规则决定、建议回复、建议忽略或调整等待时间。"
                    },
                    "criteria": criteria,
                }),
            ),
            (
                "hard_constraints".into(),
                serde_json::json!({
                    "type": "noul",
                    "instructions": "判断当前建议是否满足程序硬约束。",
                    "criteria": {
                        "true": "建议不违反绑定、静默、提及和主动行为限制。",
                        "false": "建议至少违反一项程序硬约束。"
                    }
                }),
            ),
        ]);
        if !is_proactive {
            questions.insert(
                "delay_delta_ms".into(),
                serde_json::json!({
                    "type": "score",
                    "instructions": "评估规则决定需要调整等待时间的方向和程度。分数 0 到 10 分别代表减少 5000 毫秒到增加 5000 毫秒；若 action 不是 adjust_delay，请选择 5。",
                    "criteria": [
                        "减少 5000 毫秒",
                        "减少 4000 毫秒",
                        "减少 3000 毫秒",
                        "减少 2000 毫秒",
                        "减少 1000 毫秒",
                        "不调整等待时间",
                        "增加 1000 毫秒",
                        "增加 2000 毫秒",
                        "增加 3000 毫秒",
                        "增加 4000 毫秒",
                        "增加 5000 毫秒"
                    ]
                }),
            );
        }
        questions
    }
}

#[async_trait::async_trait]
impl DecisionLlmService for DecisionLlmServiceImpl {
    async fn call(
        &self,
        ctx: &DecisionContext,
        built_in: &BuiltInDecision,
    ) -> Result<DecisionResult, DecisionFailure> {
        let system = self.build_system_prompt();
        let user = self.build_user_message(ctx, built_in);

        let situation = serde_json::to_value(ctx).unwrap_or_else(|_| serde_json::json!({}));
        let baseline =
            serde_json::to_value(&built_in.behavior).unwrap_or_else(|_| serde_json::json!({}));
        let state = serde_json::json!({
            "situation": situation,
            "built_in_decision": baseline,
        });
        let is_proactive = matches!(ctx.situation, DecisionSituation::Proactive);
        let questions = Self::build_typesafe_questions(is_proactive);
        let typesafe_request = serde_json::json!({
            "state": state,
            "model": "jev-latest",
            "questions": questions,
        });
        let request = crate::infrastructure::llm::LlmRequest {
            system: Some(system),
            messages: vec![crate::infrastructure::llm::LlmMessage {
                role: crate::infrastructure::llm::LlmRole::User,
                content: user,
            }],
            model: None,
            temperature: Some(0.1), // 低温度以获得确定性 JSON
            max_tokens: Some(512),
            priority: 1,
            metadata: serde_json::json!({
                "llm_role": "decision",
                "typesafe_request": typesafe_request,
            }),
        };

        let response = self
            .scheduler
            .submit(request)
            .await
            .map_err(|error| match error {
                crate::error::RuntimeError::HttpError(message) => {
                    DecisionFailure::Transport(message)
                }
                crate::error::RuntimeError::RateLimited { message, .. } => {
                    DecisionFailure::Transport(message)
                }
                crate::error::RuntimeError::Llm(message) => {
                    DecisionFailure::ProviderError(0, message)
                }
                crate::error::RuntimeError::LlmResponseInvalid(message) => {
                    DecisionFailure::ProviderError(0, message)
                }
                crate::error::RuntimeError::Config(message) => {
                    DecisionFailure::SchemaValidation(message)
                }
                other => DecisionFailure::Transport(other.to_string()),
            })?;

        self.parse_response(&response.content)
    }

    fn name(&self) -> &str {
        "DecisionLlmServiceImpl"
    }
}

#[cfg(test)]
mod tests {
    use super::{DecisionAction, DecisionLlmServiceImpl};

    #[test]
    fn builds_typesafe_questions_with_protocol_shapes() {
        let response = DecisionLlmServiceImpl::build_typesafe_questions(false);
        assert_eq!(response["action"]["type"], "choice");
        assert!(response["action"]["criteria"].get("adjust_delay").is_some());
        assert_eq!(response["hard_constraints"]["type"], "noul");
        assert!(response["hard_constraints"]["criteria"]
            .get("true")
            .is_some());
        assert_eq!(response["delay_delta_ms"]["type"], "score");
        assert_eq!(
            response["delay_delta_ms"]["criteria"]
                .as_array()
                .unwrap()
                .len(),
            11
        );

        let proactive = DecisionLlmServiceImpl::build_typesafe_questions(true);
        assert!(proactive["action"]["criteria"]
            .get("suggest_reply")
            .is_none());
        assert!(proactive.get("delay_delta_ms").is_none());
    }

    #[test]
    fn parses_jev_choice_answer() {
        let result = DecisionLlmServiceImpl::parse_response_content(
            r#"{"model":"jev-1.13.0","answers":{"action":{"type":"choice","choice":"suggest_ignore","probabilities":{"keep":0.1,"suggest_ignore":0.9},"confidence":0.8},"hard_constraints":{"type":"noul","noul":0.99},"delay_delta_ms":{"type":"score","score":0,"confidence":1.0}},"usage":{"input_tokens":10,"output_tokens":4}}"#,
        )
        .unwrap();
        assert_eq!(result.action, DecisionAction::SuggestIgnore);
        assert_eq!(result.confidence, Some(0.8));
        assert_eq!(result.model_version, "jev-1.13.0");
        assert_eq!(result.probabilities["action:suggest_ignore"], 0.9);
        assert_eq!(result.questions.len(), 3);
        assert!(result
            .questions
            .iter()
            .find(|question| question.name == "hard_constraints")
            .and_then(|question| question.value.as_f64())
            .is_some_and(|probability| probability >= 0.9));
    }

    #[test]
    fn rejects_jev_reply_choice_for_proactive_at_validation_layer() {
        let result = DecisionLlmServiceImpl::parse_response_content(
            r#"{"answers":{"action":{"type":"choice","choice":"suggest_reply","confidence":0.9}}}"#,
        )
        .unwrap();
        assert_eq!(result.action, DecisionAction::SuggestReply);
    }

    #[test]
    fn maps_typesafe_score_to_delay_delta_milliseconds() {
        let result = DecisionLlmServiceImpl::parse_response_content(
            r#"{"answers":{"action":{"type":"choice","choice":"adjust_delay"},"hard_constraints":{"type":"noul","noul":1.0},"delay_delta_ms":{"type":"score","score":7.5}}}"#,
        )
        .unwrap();
        assert_eq!(
            result.action,
            DecisionAction::AdjustDelay { delta_ms: 2500 }
        );
    }
}
