//! Decision Orchestrator——Beta C2 Decision Pipeline 核心编排器。
//!
//! 负责：
//! - Built-in Decision 和 DecisionContext 来自同一次 evaluate_response() 调用
//! - LlmAssistanceTrigger 判断
//! - DecisionLlmPipeline 执行（Primary → Backup → Built-in fallback）
//! - 最终返回 BehaviorDecision

use std::sync::Arc;

use crate::domain::behavior::BehaviorDecision;
// Re-export for use by application layers (main, reply_processor)
pub use crate::application::decision_pipeline::DecisionLlmPipeline;
pub use crate::domain::decision::{
    DecisionEvaluator, DecisionLlmService, DecisionResult, DecisionResultValidator, LlmAssistance,
    LlmAssistanceTrigger, NoOpTrigger,
};

#[cfg(test)]
use crate::domain::decision::{DecisionFailure, RuleBehaviorEngineOutput};

use crate::domain::decision::{
    BuiltInDecision, DecisionAction, DecisionContext, DecisionSituation,
};
use crate::error::DomainError;

// ============================================================================
// DecisionOrchestrator
// ============================================================================

/// 决策编排器——协调 Built-in Decision、Trigger、Pipeline。
///
/// 保证：
/// - Built-in Decision 和 DecisionContext 来自同一次 evaluate_response() 调用
/// - LLM 全部失败时，final == original（字段完全相等）
#[derive(Clone)]
pub struct DecisionOrchestrator {
    evaluator: Arc<dyn DecisionEvaluator>,
    trigger: Arc<dyn LlmAssistanceTrigger>,
    pipeline: Option<Arc<DecisionLlmPipeline>>,
}

impl DecisionOrchestrator {
    pub fn new(
        evaluator: Arc<dyn DecisionEvaluator>,
        trigger: Arc<dyn LlmAssistanceTrigger>,
        pipeline: Option<Arc<DecisionLlmPipeline>>,
    ) -> Self {
        Self {
            evaluator,
            trigger,
            pipeline,
        }
    }

    /// 评估 Response 决策。
    ///
    /// 1. 调用 RuleBehaviorEngine.evaluate_response()，同时获取 decision + input
    /// 2. 构造 BuiltInDecision（立即 clone）
    /// 3. 从 input 构造 DecisionContext
    /// 4. 触发 LlmAssistanceTrigger
    /// 5. NotRequired → 返回原始 behavior
    /// 6. Required → 调用 Pipeline
    /// 7. Pipeline 失败 → 返回 built_in.behavior
    /// 8. Pipeline 成功 → 应用 override
    pub async fn evaluate_response(
        &self,
        character_id: i64,
        conversation_id: i64,
        message_content: &str,
        is_mentioned: bool,
        participant_id: Option<i64>,
    ) -> Result<BehaviorDecision, DomainError> {
        // 1. 一次调用，同时获取 decision + context
        let output = self
            .evaluator
            .evaluate_response(
                character_id,
                conversation_id,
                message_content,
                is_mentioned,
                participant_id,
            )
            .await?;

        // 2. 立即构造 BuiltInDecision（clone）
        let built_in = BuiltInDecision::from_behavior(output.decision.clone());

        // 3. 从 output.input 构造 DecisionContext
        let ctx = DecisionContext::from_input(
            &output.input,
            DecisionSituation::Response {
                message_content: message_content.to_string(),
                is_mentioned,
                participant_id,
            },
        );

        // 4. Trigger 判断
        let assistance = self.trigger.evaluate(&ctx, &built_in);

        match assistance {
            LlmAssistance::NotRequired => {
                // No LLM → Alpha 等价路径
                tracing::debug!(
                    target = "decision",
                    "LlmAssistance=NotRequired，直接返回 Built-in"
                );
                Ok(built_in.behavior)
            }
            LlmAssistance::Required { reason: _ } => {
                // 需要 LLM
                if let Some(pipeline) = &self.pipeline {
                    match pipeline.execute(&ctx, &built_in).await {
                        Ok(result) => {
                            // LLM 成功 → 应用 override
                            Ok(self.apply_override(result, built_in, &ctx))
                        }
                        Err(_) => {
                            // LLM 全部失败 → 返回原始
                            tracing::warn!(target = "decision", "LLM 全部失败，使用 Built-in");
                            Ok(built_in.behavior)
                        }
                    }
                } else {
                    // Pipeline 未配置 → 返回原始
                    Ok(built_in.behavior)
                }
            }
        }
    }

    /// 将 DecisionResult override 应用到 BuiltInDecision.behavior。
    fn apply_override(
        &self,
        result: DecisionResult,
        built_in: BuiltInDecision,
        context: &DecisionContext,
    ) -> BehaviorDecision {
        use DecisionAction::*;
        let mut behavior = built_in.behavior.clone();

        match result.action {
            Keep => {
                // 无 override
            }
            SuggestReply => {
                if !context.behavior_config.in_mute_window {
                    behavior.action = crate::domain::behavior::BehaviorAction::Reply;
                }
            }
            SuggestIgnore => {
                let explicitly_mentioned = matches!(
                    context.situation,
                    DecisionSituation::Response {
                        is_mentioned: true,
                        ..
                    }
                );
                if !explicitly_mentioned {
                    behavior.action = crate::domain::behavior::BehaviorAction::Ignore;
                }
            }
            AdjustDelay { delta_ms } => {
                let new_delay = (behavior.delay_ms as i64 + delta_ms).max(0) as u64;
                behavior.delay_ms = new_delay;
            }
        }

        tracing::info!(
            target = "decision",
            original_action = ?built_in.behavior.action,
            final_action = ?behavior.action,
            original_delay = built_in.behavior.delay_ms,
            final_delay = behavior.delay_ms,
            "Decision LLM override applied"
        );

        behavior
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::behavior::{BehaviorAction, CognitionLevel, Priority};
    use crate::domain::character::{CharacterBinding, ReplyMode};
    use crate::domain::relationship::Relationship;
    use async_trait::async_trait;
    use chrono::Utc;

    /// 假的 AlwaysRequiredTrigger——强制走 LLM path。
    pub struct AlwaysRequiredTrigger;
    impl LlmAssistanceTrigger for AlwaysRequiredTrigger {
        fn evaluate(&self, _: &DecisionContext, _: &BuiltInDecision) -> LlmAssistance {
            LlmAssistance::Required {
                reason: "test".to_string(),
            }
        }
    }

    /// Mock DecisionEvaluator——返回预设的 RuleBehaviorEngineOutput。
    pub struct MockDecisionEvaluator {
        pub output: RuleBehaviorEngineOutput,
    }

    #[async_trait]
    impl DecisionEvaluator for MockDecisionEvaluator {
        async fn evaluate_response(
            &self,
            _: i64,
            _: i64,
            _: &str,
            _: bool,
            _: Option<i64>,
        ) -> Result<RuleBehaviorEngineOutput, DomainError> {
            Ok(self.output.clone())
        }

        async fn evaluate_proactive(
            &self,
            _: i64,
            _: i64,
        ) -> Result<RuleBehaviorEngineOutput, DomainError> {
            Ok(self.output.clone())
        }
    }

    /// Mock DecisionLlmService——总是返回错误。
    pub struct FailingDecisionLlmService;
    #[async_trait]
    impl DecisionLlmService for FailingDecisionLlmService {
        async fn call(
            &self,
            _: &DecisionContext,
            _: &BuiltInDecision,
        ) -> Result<DecisionResult, DecisionFailure> {
            Err(DecisionFailure::Transport("test failure".to_string()))
        }
        fn name(&self) -> &str {
            "failing"
        }
    }

    /// Mock DecisionLlmService——总是返回成功的 Keep result。
    pub struct AlwaysKeepDecisionLlmService;
    #[async_trait]
    impl DecisionLlmService for AlwaysKeepDecisionLlmService {
        async fn call(
            &self,
            _: &DecisionContext,
            _: &BuiltInDecision,
        ) -> Result<DecisionResult, DecisionFailure> {
            Ok(DecisionResult::keep())
        }
        fn name(&self) -> &str {
            "always_keep"
        }
    }

    fn sample_output() -> RuleBehaviorEngineOutput {
        RuleBehaviorEngineOutput {
            decision: BehaviorDecision {
                action: BehaviorAction::Reply,
                priority: Priority::Realtime,
                cognition_level: CognitionLevel::Light,
                delay_ms: 1400,
                reason: "mentioned=true".to_string(),
                decided_at: Utc::now(),
            },
            input: crate::domain::decision::RuleDecisionInput {
                binding: Some(CharacterBinding {
                    id: 1,
                    character_id: 1,
                    conversation_id: 10,
                    reply_mode: ReplyMode::Natural,
                    proactive_enabled: false,
                    mute_schedule: None,
                    behavior_overrides: serde_json::json!({}),
                    context_policy: serde_json::json!({}),
                    switched_at: None,
                    cross_reply_enabled: false,
                    created_at: Utc::now(),
                }),
                mood: None,
                relationship: Some(Relationship {
                    character_id: 1,
                    participant_id: 1,
                    familiarity: 0.5,
                    affection: 0.6,
                    trust: 0.4,
                    respect: 0.3,
                    annoyance: 0.1,
                    intimacy: 0.3,
                    interaction_count: 10,
                    last_interaction: Utc::now(),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                }),
                all_character_relationships: Vec::new(),
                character_state: Some(crate::domain::character::CharacterState::default()),
                conversation_state: Some(crate::domain::character::ConversationState::default()),
                behavior_state: None,
                character_id: 1,
                conversation_id: 10,
                message_content: "hello".to_string(),
                is_mentioned: true,
                participant_id: Some(1),
                in_mute_window: false,
                evaluated_at: Utc::now(),
            },
        }
    }

    // -------------------------------------------------------------------------
    // A. NotRequired path: final == original (all 6 fields)
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn not_required_preserves_all_six_fields() {
        let output = sample_output();
        let evaluator = Arc::new(MockDecisionEvaluator { output });
        let trigger = Arc::new(NoOpTrigger); // Always NotRequired
        let orchestrator = DecisionOrchestrator::new(evaluator, trigger, None);

        let final_decision = orchestrator
            .evaluate_response(1, 10, "hello", true, Some(1))
            .await
            .unwrap();

        let original = &sample_output().decision;
        assert_eq!(final_decision.action, original.action);
        assert_eq!(final_decision.priority, original.priority);
        assert_eq!(final_decision.cognition_level, original.cognition_level);
        assert_eq!(final_decision.delay_ms, original.delay_ms);
        assert_eq!(final_decision.reason, original.reason);
    }

    #[tokio::test]
    async fn not_required_does_not_call_llm() {
        // Pipeline is Some but should never be called
        let output = sample_output();
        let evaluator = Arc::new(MockDecisionEvaluator { output });
        let trigger = Arc::new(NoOpTrigger);
        let failing_service = Arc::new(FailingDecisionLlmService);
        let validator = DecisionResultValidator::new(2000, 60_000);
        let policy = Arc::new(crate::domain::decision::DefaultOverridePolicy);
        let pipeline = Arc::new(DecisionLlmPipeline::new(
            failing_service,
            None,
            validator,
            policy,
        ));
        let orchestrator = DecisionOrchestrator::new(evaluator, trigger, Some(pipeline));

        // Should return original without calling LLM
        let result = orchestrator
            .evaluate_response(1, 10, "hello", true, Some(1))
            .await
            .unwrap();

        // Should succeed with original behavior
        assert_eq!(result.action, BehaviorAction::Reply);
    }

    // -------------------------------------------------------------------------
    // B. Primary success: override applied
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn primary_success_with_keep_returns_original() {
        let output = sample_output();
        let evaluator = Arc::new(MockDecisionEvaluator { output });
        let trigger = Arc::new(AlwaysRequiredTrigger);
        let keep_service = Arc::new(AlwaysKeepDecisionLlmService);
        let validator = DecisionResultValidator::new(2000, 60_000);
        let policy = Arc::new(crate::domain::decision::DefaultOverridePolicy);
        let pipeline = Arc::new(DecisionLlmPipeline::new(
            keep_service,
            None,
            validator,
            policy,
        ));
        let orchestrator = DecisionOrchestrator::new(evaluator, trigger, Some(pipeline));

        let result = orchestrator
            .evaluate_response(1, 10, "hello", true, Some(1))
            .await
            .unwrap();

        // Keep action: should return original
        assert_eq!(result.action, BehaviorAction::Reply);
        assert_eq!(result.delay_ms, 1400);
    }

    #[tokio::test]
    async fn primary_success_suggest_reply_overrides_ignore() {
        // Original is Reply for an explicitly mentioned message; LLM cannot suppress it.
        let output = sample_output();
        let evaluator = Arc::new(MockDecisionEvaluator { output });
        let trigger = Arc::new(AlwaysRequiredTrigger);

        // C2-1：使用 override_result 注入预设结果，验证 Pipeline success → Validator → override contract
        let validator = DecisionResultValidator::new(2000, 60_000);
        let policy = Arc::new(crate::domain::decision::DefaultOverridePolicy);
        let pipeline = Arc::new(
            DecisionLlmPipeline::new(Arc::new(FailingDecisionLlmService), None, validator, policy)
                .with_override_result(DecisionResult {
                    action: DecisionAction::SuggestIgnore,
                    reasoning: None,
                    confidence: None,
                }),
        );
        let orchestrator = DecisionOrchestrator::new(evaluator, trigger, Some(pipeline));

        let result = orchestrator
            .evaluate_response(1, 10, "hello", true, Some(1))
            .await
            .unwrap();

        // Hard mention constraint wins over the advisory override.
        assert_eq!(result.action, BehaviorAction::Reply);
    }

    // -------------------------------------------------------------------------
    // C. Primary failure + Backup success
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn primary_failure_triggers_backup() {
        let output = sample_output();
        let evaluator = Arc::new(MockDecisionEvaluator { output });
        let trigger = Arc::new(AlwaysRequiredTrigger);

        struct FailingThenKeepService;
        #[async_trait]
        impl DecisionLlmService for FailingThenKeepService {
            fn name(&self) -> &str {
                "failing_then_keep"
            }
            async fn call(
                &self,
                _: &DecisionContext,
                _: &BuiltInDecision,
            ) -> Result<DecisionResult, DecisionFailure> {
                Err(DecisionFailure::Transport("primary failed".to_string()))
            }
        }

        let primary = Arc::new(FailingThenKeepService);
        let backup = Arc::new(AlwaysKeepDecisionLlmService);
        let validator = DecisionResultValidator::new(2000, 60_000);
        let policy = Arc::new(crate::domain::decision::DefaultOverridePolicy);
        let pipeline = Arc::new(DecisionLlmPipeline::new(
            primary,
            Some(backup),
            validator,
            policy,
        ));
        let orchestrator = DecisionOrchestrator::new(evaluator, trigger, Some(pipeline));

        let result = orchestrator
            .evaluate_response(1, 10, "hello", true, Some(1))
            .await
            .unwrap();

        // Backup succeeded with Keep, should return original
        assert_eq!(result.action, BehaviorAction::Reply);
    }

    // -------------------------------------------------------------------------
    // D. Primary + Backup both fail: final == original (exact)
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn both_llms_fail_returns_exact_original() {
        let output = sample_output();
        let evaluator = Arc::new(MockDecisionEvaluator { output });
        let trigger = Arc::new(AlwaysRequiredTrigger);

        let primary = Arc::new(FailingDecisionLlmService);
        let backup = Arc::new(FailingDecisionLlmService);
        let validator = DecisionResultValidator::new(2000, 60_000);
        let policy = Arc::new(crate::domain::decision::DefaultOverridePolicy);
        let pipeline = Arc::new(DecisionLlmPipeline::new(
            primary,
            Some(backup),
            validator,
            policy,
        ));
        let orchestrator = DecisionOrchestrator::new(evaluator, trigger, Some(pipeline));

        let result = orchestrator
            .evaluate_response(1, 10, "hello", true, Some(1))
            .await
            .unwrap();

        // Both failed → should return exact original
        assert_eq!(result.action, BehaviorAction::Reply);
        assert_eq!(result.priority, Priority::Realtime);
        assert_eq!(result.cognition_level, CognitionLevel::Light);
        assert_eq!(result.delay_ms, 1400);
        assert_eq!(result.reason, "mentioned=true");
    }

    // -------------------------------------------------------------------------
    // E. Invalid DecisionResult: falls through to Built-in
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn invalid_result_falls_through_to_builtin() {
        let output = sample_output();
        let evaluator = Arc::new(MockDecisionEvaluator { output });
        let trigger = Arc::new(AlwaysRequiredTrigger);

        // A service that returns delta > max
        struct LargeDeltaService;
        #[async_trait]
        impl DecisionLlmService for LargeDeltaService {
            async fn call(
                &self,
                _: &DecisionContext,
                _: &BuiltInDecision,
            ) -> Result<DecisionResult, DecisionFailure> {
                Ok(DecisionResult {
                    action: DecisionAction::AdjustDelay {
                        delta_ms: 1_000_000, // way over max
                    },
                    reasoning: None,
                    confidence: None,
                })
            }
            fn name(&self) -> &str {
                "large_delta"
            }
        }

        let service = Arc::new(LargeDeltaService);
        let validator = DecisionResultValidator::new(2000, 60_000); // max_delta = 2000
        let policy = Arc::new(crate::domain::decision::DefaultOverridePolicy);
        let pipeline = Arc::new(DecisionLlmPipeline::new(service, None, validator, policy));
        let orchestrator = DecisionOrchestrator::new(evaluator, trigger, Some(pipeline));

        let result = orchestrator
            .evaluate_response(1, 10, "hello", true, Some(1))
            .await
            .unwrap();

        // Validator rejects delta → falls through to Built-in
        assert_eq!(result.action, BehaviorAction::Reply);
        assert_eq!(result.delay_ms, 1400); // original delay preserved
    }

    // -------------------------------------------------------------------------
    // F. AdjustDelay override
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn adjust_delay_override_applies_delta() {
        let output = sample_output();
        let evaluator = Arc::new(MockDecisionEvaluator { output });
        let trigger = Arc::new(AlwaysRequiredTrigger);

        // C2-1：使用 override_result 注入预设结果，验证 AdjustDelay override contract
        let validator = DecisionResultValidator::new(2000, 60_000);
        let policy = Arc::new(crate::domain::decision::DefaultOverridePolicy);
        let pipeline = Arc::new(
            DecisionLlmPipeline::new(Arc::new(FailingDecisionLlmService), None, validator, policy)
                .with_override_result(DecisionResult {
                    action: DecisionAction::AdjustDelay { delta_ms: 500 },
                    reasoning: Some("increase delay".to_string()),
                    confidence: Some(0.8),
                }),
        );
        let orchestrator = DecisionOrchestrator::new(evaluator, trigger, Some(pipeline));

        let result = orchestrator
            .evaluate_response(1, 10, "hello", true, Some(1))
            .await
            .unwrap();

        // Original delay = 1400, delta = +500 → 1900
        assert_eq!(result.delay_ms, 1900);
        // action stays Reply
        assert_eq!(result.action, BehaviorAction::Reply);
    }

    // -------------------------------------------------------------------------
    // G. Validator: SuggestReply on Proactive situation is rejected
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn validator_rejects_suggest_reply_on_proactive() {
        let validator = DecisionResultValidator::new(2000, 60_000);
        let policy = Arc::new(crate::domain::decision::DefaultOverridePolicy);

        let result = DecisionResult {
            action: DecisionAction::SuggestReply,
            reasoning: None,
            confidence: None,
        };

        let ctx = DecisionContext::from_input(
            &sample_output().input,
            crate::domain::decision::DecisionSituation::Proactive,
        );

        let built_in = BuiltInDecision::from_behavior(sample_output().decision);

        let errors = validator
            .validate(&result, &ctx, &built_in, policy.as_ref())
            .unwrap_err();

        assert!(
            errors.iter().any(|e| e.field == "action"),
            "Should have action error"
        );
    }

    // -------------------------------------------------------------------------
    // H. confidence validation
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn out_of_range_confidence_rejected() {
        let output = sample_output();
        let evaluator = Arc::new(MockDecisionEvaluator { output });
        let trigger = Arc::new(AlwaysRequiredTrigger);

        struct BadConfidenceService;
        #[async_trait]
        impl DecisionLlmService for BadConfidenceService {
            async fn call(
                &self,
                _: &DecisionContext,
                _: &BuiltInDecision,
            ) -> Result<DecisionResult, DecisionFailure> {
                Ok(DecisionResult {
                    action: DecisionAction::Keep,
                    reasoning: None,
                    confidence: Some(1.5), // out of range
                })
            }
            fn name(&self) -> &str {
                "bad_confidence"
            }
        }

        let service = Arc::new(BadConfidenceService);
        let validator = DecisionResultValidator::new(2000, 60_000);
        let policy = Arc::new(crate::domain::decision::DefaultOverridePolicy);
        let pipeline = Arc::new(DecisionLlmPipeline::new(service, None, validator, policy));
        let orchestrator = DecisionOrchestrator::new(evaluator, trigger, Some(pipeline));

        let result = orchestrator
            .evaluate_response(1, 10, "hello", true, Some(1))
            .await
            .unwrap();

        // Validator rejects out-of-range confidence → Built-in
        assert_eq!(result.action, BehaviorAction::Reply);
    }

    // -------------------------------------------------------------------------
    // Routing isolation tests
    // -------------------------------------------------------------------------

    /// 验证 LlmAssistance::Required 时，Pipeline.execute() 被调用。
    ///
    /// 注意：由于 stub Pipeline 直接返回 override_result，不实际调用 service，
    /// 此测试验证 LLM path 确实被触发（而非 NotRequired path）。
    #[tokio::test]
    async fn required_triggers_llm_path() {
        let output = sample_output();
        let evaluator = Arc::new(MockDecisionEvaluator { output });
        let trigger = Arc::new(AlwaysRequiredTrigger);

        // 使用 override_result 模拟 LLM 返回 Keep
        let validator = DecisionResultValidator::new(2000, 60_000);
        let policy = Arc::new(crate::domain::decision::DefaultOverridePolicy);
        let pipeline = Arc::new(
            DecisionLlmPipeline::new(Arc::new(FailingDecisionLlmService), None, validator, policy)
                .with_override_result(DecisionResult::keep()),
        );
        let orchestrator = DecisionOrchestrator::new(evaluator, trigger, Some(pipeline));

        let result = orchestrator
            .evaluate_response(1, 10, "hello", true, Some(1))
            .await
            .unwrap();

        // LLM path 被触发，override = Keep → 返回原始 Reply
        assert_eq!(result.action, BehaviorAction::Reply);
        assert_eq!(result.delay_ms, 1400);
    }

    /// 验证 LlmAssistance::NotRequired 时，Pipeline 不被调用（NoOpTrigger）。
    #[tokio::test]
    async fn not_required_skips_llm_completely() {
        let output = sample_output();
        let evaluator = Arc::new(MockDecisionEvaluator { output });
        let trigger = Arc::new(NoOpTrigger); // Always NotRequired

        // 即使注入了 override_result，NoOpTrigger 也应直接返回 Built-in
        let validator = DecisionResultValidator::new(2000, 60_000);
        let policy = Arc::new(crate::domain::decision::DefaultOverridePolicy);
        let pipeline = Arc::new(
            DecisionLlmPipeline::new(Arc::new(FailingDecisionLlmService), None, validator, policy)
                .with_override_result(DecisionResult {
                    action: DecisionAction::SuggestIgnore,
                    reasoning: None,
                    confidence: None,
                }),
        );
        let orchestrator = DecisionOrchestrator::new(evaluator, trigger, Some(pipeline));

        let result = orchestrator
            .evaluate_response(1, 10, "hello", true, Some(1))
            .await
            .unwrap();

        // NoOpTrigger → NotRequired → 直接返回 Built-in，不走 LLM path
        assert_eq!(result.action, BehaviorAction::Reply);
        assert_eq!(result.delay_ms, 1400);
    }

    /// 验证 Pipeline 失败时，Built-in fallback 返回精确原始结果（含所有 6 字段）。
    /// 关键：使用同一次 sample_output() 调用确保 decided_at 完全相等。
    #[tokio::test]
    async fn pipeline_failure_returns_exact_builtin_all_fields() {
        let output = sample_output(); // 一次调用，确保 timestamp 一致
        let evaluator = Arc::new(MockDecisionEvaluator {
            output: output.clone(),
        });
        let trigger = Arc::new(AlwaysRequiredTrigger);

        let service = Arc::new(FailingDecisionLlmService);
        let validator = DecisionResultValidator::new(2000, 60_000);
        let policy = Arc::new(crate::domain::decision::DefaultOverridePolicy);
        let pipeline = Arc::new(DecisionLlmPipeline::new(
            service, None, // 无 backup
            validator, policy,
        ));
        let orchestrator = DecisionOrchestrator::new(evaluator, trigger, Some(pipeline));

        let result = orchestrator
            .evaluate_response(1, 10, "hello", true, Some(1))
            .await
            .unwrap();

        // 所有 6 字段必须精确相等
        assert_eq!(result.action, output.decision.action);
        assert_eq!(result.priority, output.decision.priority);
        assert_eq!(result.cognition_level, output.decision.cognition_level);
        assert_eq!(result.delay_ms, output.decision.delay_ms);
        assert_eq!(result.reason, output.decision.reason);
        assert_eq!(result.decided_at, output.decision.decided_at);
    }
}
