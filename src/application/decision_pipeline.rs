//! Decision LLM Pipeline——Beta C2 Decision Pipeline 执行器。
//!
//! 负责：
//! - Primary LLM → Backup LLM → Built-in Fallback 链式调用
//! - Validator + OverridePolicy 验证
//! - 将验证后的 DecisionResult 返回给 Orchestrator

use std::sync::Arc;

use crate::domain::decision::{
    BuiltInDecision, DecisionContext, DecisionFailure, DecisionLlmService, DecisionOverridePolicy,
    DecisionResult, DecisionResultValidator,
};

/// Decision LLM Pipeline——执行 Primary → Backup → Built-in Fallback 逻辑。
///
/// 所有错误（transport / timeout / provider error / validation failure）都触发 fallback，
/// 最终 Pipeline 成功返回时一定是经过 Validator 验证的 DecisionResult。
pub struct DecisionLlmPipeline {
    primary: Arc<dyn DecisionLlmService>,
    backup: Option<Arc<dyn DecisionLlmService>>,
    validator: DecisionResultValidator,
    policy: Arc<dyn DecisionOverridePolicy>,
    #[cfg(test)]
    override_result: Option<DecisionResult>,
}

impl std::fmt::Debug for DecisionLlmPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecisionLlmPipeline")
            .field("primary", &self.primary.name())
            .field("backup", &self.backup.as_ref().map(|s| s.name()))
            .field("validator", &self.validator)
            .finish()
    }
}

impl DecisionLlmPipeline {
    pub fn new(
        primary: Arc<dyn DecisionLlmService>,
        backup: Option<Arc<dyn DecisionLlmService>>,
        validator: DecisionResultValidator,
        policy: Arc<dyn DecisionOverridePolicy>,
    ) -> Self {
        Self {
            primary,
            backup,
            validator,
            policy,
            #[cfg(test)]
            override_result: None,
        }
    }

    #[cfg(test)]
    pub fn with_override_result(mut self, result: DecisionResult) -> Self {
        self.override_result = Some(result);
        self
    }

    /// 执行 Pipeline：Primary → Backup → Built-in Fallback。
    ///
    /// 成功返回时一定是经过验证的 DecisionResult。
    /// 失败时 Orchestrator 使用 Built-in Decision。
    pub async fn execute(
        &self,
        ctx: &DecisionContext,
        built_in: &BuiltInDecision,
    ) -> Result<DecisionResult, DecisionFailure> {
        #[cfg(test)]
        if let Some(result) = &self.override_result {
            return Ok(result.clone());
        }
        // 1. 尝试 Primary
        match self.primary.call(ctx, built_in).await {
            Ok(result) => {
                // Primary 成功 → 验证
                match self
                    .validator
                    .validate(&result, ctx, built_in, self.policy.as_ref())
                {
                    Ok(()) => {
                        tracing::debug!(
                            target = "decision",
                            primary = %self.primary.name(),
                            action = ?result.action,
                            "Primary LLM succeeded and validated"
                        );
                        Ok(result)
                    }
                    Err(errors) => {
                        // Primary 结果验证失败 → 尝试 Backup
                        tracing::warn!(
                            target = "decision",
                            primary = %self.primary.name(),
                            errors = ?errors,
                            "Primary LLM result validation failed, trying backup"
                        );
                        self.try_backup(ctx, built_in).await
                    }
                }
            }
            Err(primary_err) => {
                // Primary 失败 → 尝试 Backup
                tracing::warn!(
                    target = "decision",
                    primary = %self.primary.name(),
                    error = %primary_err,
                    "Primary LLM failed, trying backup"
                );
                self.try_backup(ctx, built_in).await
            }
        }
    }

    /// 尝试 Backup（如果配置了）。
    async fn try_backup(
        &self,
        ctx: &DecisionContext,
        built_in: &BuiltInDecision,
    ) -> Result<DecisionResult, DecisionFailure> {
        let Some(backup) = &self.backup else {
            tracing::warn!(target = "decision", "No backup configured, pipeline fails");
            return Err(DecisionFailure::AllProvidersFailed);
        };

        match backup.call(ctx, built_in).await {
            Ok(result) => {
                // Backup 成功 → 验证
                match self
                    .validator
                    .validate(&result, ctx, built_in, self.policy.as_ref())
                {
                    Ok(()) => {
                        tracing::debug!(
                            target = "decision",
                            backup = %backup.name(),
                            action = ?result.action,
                            "Backup LLM succeeded and validated"
                        );
                        Ok(result)
                    }
                    Err(errors) => {
                        // Backup 结果验证失败 → pipeline 失败
                        tracing::warn!(
                            target = "decision",
                            backup = %backup.name(),
                            errors = ?errors,
                            "Backup LLM result validation failed, pipeline fails"
                        );
                        Err(DecisionFailure::SemanticValidation(errors))
                    }
                }
            }
            Err(backup_err) => {
                // Backup 也失败 → pipeline 失败
                tracing::warn!(
                    target = "decision",
                    backup = %backup.name(),
                    error = %backup_err,
                    "Backup LLM also failed, pipeline fails"
                );
                Err(backup_err)
            }
        }
    }
}
