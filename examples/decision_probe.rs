//! 单次真实决策模型探针。
//!
//! 这个示例只读取 decision.toml 并调用一次 TypeSafe/Jev：
//! - 不启动主运行时、OneBot 或插件；
//! - 不读取或写入数据库；
//! - 不发送消息；
//! - 不输出原始请求、原始响应或 API Key。
//!
//! 用法：
//! `cargo run --example decision_probe -- --config-dir config --mode response`

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use yomua_bot::application::config::{
    load_decision, load_runtime, validate_runtime, DecisionConfig, LlmModelInstanceConfig,
};
use yomua_bot::application::decision_orchestrator::DecisionOrchestrator;
use yomua_bot::application::decision_pipeline::DecisionLlmPipeline;
use yomua_bot::application::llm_scheduler::{DefaultLlmScheduler, LlmRouteConfig};
use yomua_bot::domain::behavior::{BehaviorAction, BehaviorDecision, CognitionLevel, Priority};
use yomua_bot::domain::character::ReplyMode;
use yomua_bot::domain::decision::{
    BehaviorConfig, BuiltInDecision, CharacterStateSnapshot, ConversationStateSnapshot,
    DecisionContext, DecisionFailure, DecisionLlmServiceImpl, DecisionResult,
    DecisionResultValidator, DecisionSituation, DefaultOverridePolicy, RelationshipState,
};
use yomua_bot::infrastructure::llm::typesafe::TypeSafeProvider;
use yomua_bot::infrastructure::llm::LlmProvider;

#[derive(Debug, Clone, Copy)]
enum ProbeMode {
    Response,
    Proactive,
}

fn main_args() -> (String, ProbeMode) {
    let mut config_dir = "config".to_string();
    let mut mode = ProbeMode::Response;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config-dir" => {
                config_dir = args.next().unwrap_or_else(|| {
                    eprintln!("--config-dir 需要一个目录路径");
                    std::process::exit(2);
                });
            }
            "--mode" => {
                mode = match args.next().as_deref() {
                    Some("response") => ProbeMode::Response,
                    Some("proactive") => ProbeMode::Proactive,
                    Some(value) => {
                        eprintln!("不支持的 --mode：{value}，可选 response 或 proactive");
                        std::process::exit(2);
                    }
                    None => {
                        eprintln!("--mode 需要 response 或 proactive");
                        std::process::exit(2);
                    }
                };
            }
            "--help" | "-h" => {
                println!("用法：cargo run --example decision_probe -- [--config-dir config] [--mode response|proactive]");
                std::process::exit(0);
            }
            value => {
                eprintln!("未知参数：{value}；使用 --help 查看用法");
                std::process::exit(2);
            }
        }
    }
    (config_dir, mode)
}

fn build_context(mode: ProbeMode) -> (DecisionContext, BuiltInDecision) {
    let situation = match mode {
        ProbeMode::Response => DecisionSituation::Response {
            message_content: "这是一次决策模型连通性探针，不需要生成回复文本。".to_string(),
            is_mentioned: false,
            participant_id: Some(10001),
        },
        ProbeMode::Proactive => DecisionSituation::Proactive,
    };
    let behavior = BehaviorDecision {
        action: match mode {
            ProbeMode::Response => BehaviorAction::Ignore,
            ProbeMode::Proactive => BehaviorAction::InitiateProactive,
        },
        priority: Priority::Normal,
        cognition_level: CognitionLevel::Light,
        delay_ms: 1_000,
        reason: "decision_probe_baseline".to_string(),
        decided_at: Utc::now(),
    };
    let context = DecisionContext {
        situation,
        character_id: 1,
        conversation_id: 1,
        behavior_config: BehaviorConfig {
            has_binding: true,
            reply_mode: ReplyMode::Natural,
            proactive_enabled: matches!(mode, ProbeMode::Proactive),
            in_mute_window: false,
            cross_reply_enabled: false,
        },
        relationship_state: RelationshipState {
            closeness: 0.5,
            is_distant: false,
            is_close: false,
            is_annoyed: false,
            high_affection: false,
            familiarity: 0.5,
            affection: 0.5,
            trust: 0.5,
            intimacy: 0.5,
            annoyance: 0.1,
        },
        character_state: CharacterStateSnapshot {
            energy: 80.0,
            stress: 10.0,
            is_low_energy: false,
            is_high_stress: false,
        },
        conversation_state: ConversationStateSnapshot {
            energy: 80.0,
            stress: 10.0,
        },
    };
    (context, BuiltInDecision::from_behavior(behavior))
}

fn build_scheduler(
    config: &DecisionConfig,
    proxy: Option<&str>,
) -> Result<Arc<DefaultLlmScheduler>, String> {
    let primary = config
        .primary
        .as_ref()
        .ok_or_else(|| "decision.toml 缺少 [primary]".to_string())?;
    let models = std::iter::once(primary)
        .chain(config.fallbacks.iter())
        .map(|model| LlmModelInstanceConfig {
            provider: model.provider.clone(),
            model: model.model.clone(),
            context_window: model.context_window,
            max_output_tokens: model.max_output_tokens,
        })
        .collect::<Vec<_>>();
    let mut providers = HashMap::new();
    for model in &models {
        let provider_config = config
            .providers
            .get(&model.provider)
            .ok_or_else(|| format!("模型引用了未注册的 Provider：{}", model.provider))?;
        if provider_config.provider_type != "typesafe" {
            return Err(format!(
                "决策探针只接受 TypeSafe Provider，当前 {} 的类型为：{}",
                model.provider, provider_config.provider_type
            ));
        }
        if !providers.contains_key(&model.provider) {
            let provider = TypeSafeProvider::new(
                provider_config.base_url.clone(),
                provider_config.api_key.clone(),
                Duration::from_secs(provider_config.timeout_secs.unwrap_or(60)),
                proxy,
            )
            .map_err(|error| error.to_string())?;
            providers.insert(
                model.provider.clone(),
                Arc::new(provider) as Arc<dyn LlmProvider>,
            );
        }
    }
    let provider = providers
        .get(&primary.provider)
        .cloned()
        .ok_or_else(|| format!("primary 引用了未注册的 Provider：{}", primary.provider))?;
    let route = LlmRouteConfig {
        models,
        max_attempts: config
            .max_attempts
            .unwrap_or_else(|| config.fallbacks.len() + 1),
        budget_tokens: config.budget_tokens,
    };
    let mut routes = HashMap::new();
    routes.insert("decision".to_string(), route);
    Ok(Arc::new(DefaultLlmScheduler::with_provider_routes(
        provider, 1, 1, providers, routes,
    )))
}

fn value_summary(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(value) => format!("bool={value}"),
        serde_json::Value::Number(value) => format!("number={value}"),
        serde_json::Value::String(value) => format!("string_len={}", value.chars().count()),
        serde_json::Value::Array(values) => format!("array_len={}", values.len()),
        serde_json::Value::Object(values) => format!("object_keys={}", values.len()),
    }
}

fn print_result(result: &DecisionResult, built_in: &BuiltInDecision, context: &DecisionContext) {
    let final_behavior =
        DecisionOrchestrator::apply_override(result.clone(), built_in.clone(), context);
    println!("request=success");
    println!("model_version={}", result.model_version);
    println!("confidence={:?}", result.confidence);
    println!("suggested_action={:?}", result.action);
    println!("baseline_action={:?}", built_in.behavior.action);
    println!("final_action={:?}", final_behavior.action);
    println!("baseline_delay_ms={}", built_in.behavior.delay_ms);
    println!("final_delay_ms={}", final_behavior.delay_ms);
    println!("questions={}", result.questions.len());
    for question in &result.questions {
        println!(
            "question name={} type={} value={} confidence={:.3} probabilities={}",
            question.name,
            question.question_type,
            value_summary(&question.value),
            question.confidence,
            format_probabilities(&question.probabilities)
        );
    }
    println!(
        "aggregated_probabilities={}",
        format_probabilities(&result.probabilities)
    );
    println!("validation=passed");
    println!(
        "adopted={}",
        final_behavior.action != built_in.behavior.action
            || final_behavior.delay_ms != built_in.behavior.delay_ms
    );
}

fn format_probabilities(probabilities: &BTreeMap<String, f64>) -> String {
    probabilities
        .iter()
        .map(|(key, value)| format!("{key}={value:.3}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn failure_kind(error: &DecisionFailure) -> &'static str {
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

#[tokio::main]
async fn main() {
    let (config_dir, mode) = main_args();
    let runtime_path = std::path::Path::new(&config_dir).join("runtime.toml");
    let runtime = match load_runtime(&runtime_path.display().to_string()) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("config_error={error}");
            std::process::exit(1);
        }
    };
    if let Some(error) = validate_runtime(&runtime).into_iter().next() {
        eprintln!("config_error={error}");
        std::process::exit(1);
    }
    let path = std::path::Path::new(&config_dir).join("decision.toml");
    let config = match load_decision(&path.display().to_string()) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("config_error={error}");
            std::process::exit(1);
        }
    };
    let scheduler = match build_scheduler(&config, runtime.proxy.as_deref()) {
        Ok(scheduler) => scheduler,
        Err(error) => {
            eprintln!("config_error={error}");
            std::process::exit(1);
        }
    };
    let service = Arc::new(DecisionLlmServiceImpl::new(scheduler));
    let pipeline = DecisionLlmPipeline::new(
        service,
        None,
        DecisionResultValidator::new(5_000, 30_000),
        Arc::new(DefaultOverridePolicy),
    );
    let (context, built_in) = build_context(mode);
    println!(
        "mode={}",
        match mode {
            ProbeMode::Response => "response",
            ProbeMode::Proactive => "proactive",
        }
    );
    println!("config_enabled={}", config.enabled);
    println!("baseline_action={:?}", built_in.behavior.action);
    match pipeline.execute(&context, &built_in).await {
        Ok(result) => print_result(&result, &built_in, &context),
        Err(error) => {
            println!("request=failed");
            println!("failure_kind={}", failure_kind(&error));
            println!("validation=fallback_to_builtin");
            println!("final_action={:?}", built_in.behavior.action);
            println!("final_delay_ms={}", built_in.behavior.delay_ms);
            eprintln!("decision_probe_error={error}");
            std::process::exit(1);
        }
    }
}
