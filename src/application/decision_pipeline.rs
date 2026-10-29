//! Decision LLM Pipeline——Beta C2 Decision Pipeline 执行器。
//!
//! 负责：
//! - Primary LLM → Backup LLM → Built-in Fallback 链式调用
//! - Validator + OverridePolicy 验证
//! - 将验证后的 DecisionResult 返回给 Orchestrator

use std::sync::Arc;

use crate::domain::decision::{
    BuiltInDecision, DecisionContext, DecisionFailure, DecisionLlmService, DecisionOverridePolicy,
    DecisionQuestionResult, DecisionResult, DecisionResultValidator, ValidationError,
};

/// 将 typed question 压缩为可安全写入审计日志的摘要。
///
/// provider 返回的 value 不是可信日志字段，字符串只记录长度，复杂值只记录形状。
pub(crate) fn audit_question_results(questions: &[DecisionQuestionResult]) -> Vec<String> {
    questions
        .iter()
        .map(|question| {
            let name = audit_text(&question.name);
            let question_type = audit_text(&question.question_type);
            let value = match &question.value {
                serde_json::Value::Null => "null".to_string(),
                serde_json::Value::Bool(value) => format!("bool={value}"),
                serde_json::Value::Number(value) => format!("number={value}"),
                serde_json::Value::String(value) => {
                    format!("string_len={}", value.chars().count())
                }
                serde_json::Value::Array(values) => format!("array_len={}", values.len()),
                serde_json::Value::Object(values) => format!("object_keys={}", values.len()),
            };
            format!(
                "{}:{}:{}:confidence={:.3}",
                name, question_type, value, question.confidence
            )
        })
        .collect()
}

/// 仅保留概率键和数值，避免将 provider 原始结构写入日志。
pub(crate) fn audit_probabilities(
    probabilities: &std::collections::BTreeMap<String, f64>,
) -> Vec<String> {
    probabilities
        .iter()
        .map(|(key, value)| format!("{}={value:.3}", audit_text(key)))
        .collect()
}

pub(crate) fn audit_model_version(model_version: &str) -> String {
    let value = audit_text(model_version);
    let lowercase = value.to_ascii_lowercase();
    if lowercase.starts_with("sk-")
        || lowercase.contains("api_key")
        || lowercase.contains("secret")
        || lowercase.contains("token")
    {
        "redacted".to_string()
    } else {
        value
    }
}

fn audit_text(value: &str) -> String {
    value
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_' | ':' | '/')
        })
        .take(64)
        .collect()
}

pub(crate) fn audit_validation_fields(errors: &[ValidationError]) -> Vec<&str> {
    errors.iter().map(|error| error.field.as_str()).collect()
}

pub(crate) fn decision_failure_kind(error: &DecisionFailure) -> &'static str {
    match error {
        DecisionFailure::Transport(_) => "transport",
        DecisionFailure::ProviderError(_, _) => "provider_error",
        DecisionFailure::Timeout => "timeout",
        DecisionFailure::JsonParse(_) => "json_parse",
        DecisionFailure::SchemaValidation(_) => "schema_validation",
        DecisionFailure::SemanticValidation(_) => "semantic_validation",
        DecisionFailure::AllProvidersFailed => "all_providers_failed",
    }
}

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
                            model_version = %audit_model_version(&result.model_version),
                            confidence = ?result.confidence,
                            questions = ?audit_question_results(&result.questions),
                            probabilities = ?audit_probabilities(&result.probabilities),
                            validation = "passed",
                            "Primary LLM succeeded and validated"
                        );
                        Ok(result)
                    }
                    Err(errors) => {
                        // Primary 结果验证失败 → 尝试 Backup
                        tracing::warn!(
                            target = "decision",
                            primary = %self.primary.name(),
                            validation_errors = errors.len(),
                            validation_fields = ?audit_validation_fields(&errors),
                            model_version = %audit_model_version(&result.model_version),
                            confidence = ?result.confidence,
                            questions = ?audit_question_results(&result.questions),
                            probabilities = ?audit_probabilities(&result.probabilities),
                            validation = "rejected",
                            fallback = "backup_or_builtin",
                            "Primary LLM result validation failed, trying backup"
                        );
                        self.try_backup(ctx, built_in, DecisionFailure::SemanticValidation(errors))
                            .await
                    }
                }
            }
            Err(primary_err) => {
                // Primary 失败 → 尝试 Backup
                tracing::warn!(
                    target = "decision",
                    primary = %self.primary.name(),
                    failure = decision_failure_kind(&primary_err),
                    validation = "unavailable",
                    fallback = "backup_or_builtin",
                    "Primary LLM failed, trying backup"
                );
                self.try_backup(ctx, built_in, primary_err).await
            }
        }
    }

    /// 尝试 Backup（如果配置了）。
    async fn try_backup(
        &self,
        ctx: &DecisionContext,
        built_in: &BuiltInDecision,
        primary_failure: DecisionFailure,
    ) -> Result<DecisionResult, DecisionFailure> {
        let Some(backup) = &self.backup else {
            tracing::warn!(
                target = "decision",
                failure = "no_backup",
                validation = "unavailable",
                fallback = "builtin",
                "No backup configured, pipeline fails"
            );
            return Err(primary_failure);
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
                            model_version = %audit_model_version(&result.model_version),
                            confidence = ?result.confidence,
                            questions = ?audit_question_results(&result.questions),
                            probabilities = ?audit_probabilities(&result.probabilities),
                            validation = "passed",
                            "Backup LLM succeeded and validated"
                        );
                        Ok(result)
                    }
                    Err(errors) => {
                        // Backup 结果验证失败 → pipeline 失败
                        tracing::warn!(
                            target = "decision",
                            backup = %backup.name(),
                            validation_errors = errors.len(),
                            validation_fields = ?audit_validation_fields(&errors),
                            model_version = %audit_model_version(&result.model_version),
                            confidence = ?result.confidence,
                            questions = ?audit_question_results(&result.questions),
                            probabilities = ?audit_probabilities(&result.probabilities),
                            validation = "rejected",
                            fallback = "builtin",
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
                    failure = decision_failure_kind(&backup_err),
                    validation = "unavailable",
                    fallback = "builtin",
                    "Backup LLM also failed, pipeline fails"
                );
                Err(backup_err)
            }
        }
    }
}
