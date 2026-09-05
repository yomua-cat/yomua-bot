//! Character Card 导入器 —— 将外部 SillyTavern 卡转换为内部规范模型。
//!
//! 采用文档 docs/10 的「Importer → Canonical」模式：
//! 本模块负责识别卡版本、解析 JSON / PNG 元数据，
//! 并把 V1 / V2 / V3 字段映射到 domain 的 [`CharacterDefinition`]。
//! domain 层不依赖任何外部 schema；外部格式的解析能力全部集中在此适配层。
//!
//! 解析策略是「宽松解析」：字段缺失 / 为 null 时尽力容忍，
//! 只有完全无法识别为角色卡时才返回错误。

mod error;
mod png_card;

pub use error::CardImportError;

use serde_json::Value;

use crate::domain::character::{CharacterDefinition, LorebookEntry};

/// 识别的卡版本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CardVersion {
    V1,
    V2,
    V3,
}

/// 从 JSON 文本解析一张角色卡，返回内部规范模型。
pub fn parse_character_card(json: &str) -> Result<CharacterDefinition, CardImportError> {
    let value: Value =
        serde_json::from_str(json).map_err(|e| CardImportError::Json(e.to_string()))?;
    value_to_definition(&value)
}

/// 从 PNG 字节解析一张角色卡（读取 tEXt chunk 中 key 为 `chara` 的 JSON）。
pub fn parse_png_character_card(bytes: &[u8]) -> Result<CharacterDefinition, CardImportError> {
    let text = png_card::extract_chara_text(bytes)?;
    parse_character_card(&text)
}

/// 将解析出的 JSON 值转换为 [`CharacterDefinition`]。
///
/// 这是 V1 / V2 / V3 的统一入口：识别版本后提取数据对象，
/// 再对每个规范字段做宽松映射。
fn value_to_definition(value: &Value) -> Result<CharacterDefinition, CardImportError> {
    if !value.is_object() {
        return Err(CardImportError::NotRecognized);
    }

    let (version, data) = extract_source(value);

    // 判断是否为「可识别」的角色卡：数据对象中至少应包含一个已知字段。
    if !looks_like_card(&data) {
        return Err(CardImportError::NotRecognized);
    }

    let metadata = build_metadata(value, version, &data);

    Ok(CharacterDefinition {
        name: get_string(&data, &["name"]).unwrap_or_default(),
        description: get_string(&data, &["description"]),
        personality: get_string(&data, &["personality"]),
        scenario: get_string(&data, &["scenario"]),
        style: get_string(&data, &["style"]),
        background: get_string(&data, &["background"]),
        greetings: extract_greetings(&data),
        example_messages: extract_example_messages(&data),
        system_prompt: get_string(&data, &["system_prompt", "system_instruction"]),
        post_history_instructions: get_string(&data, &["post_history_instructions"]),
        lorebook: extract_lorebook(&data),
        metadata,
    })
}

/// 识别版本并提取承载字段的数据对象。
///
/// - V3：`spec_version` 标记，或仅 `spec: chara_card_v3`（无 `spec_version` 也识别为 V3）。
/// - V2：`spec` 标记为 `chara_card_v2`。
/// - V1：无标记（或 `spec: chara_card_v1`），数据在顶层（若存在 `data` 也兼容）。
fn extract_source(value: &Value) -> (CardVersion, Value) {
    let spec_version = value.get("spec_version").and_then(|v| v.as_str());
    let spec = value.get("spec").and_then(|v| v.as_str());

    let version = if is_v3(spec_version) || is_v3(spec) {
        CardVersion::V3
    } else if is_v2(spec) {
        CardVersion::V2
    } else {
        CardVersion::V1
    };

    // V2/V3 数据在 `data`；V1 数据通常在顶层（若有 `data` 也偏好使用它）。
    let data = value
        .get("data")
        .filter(|d| d.is_object())
        .cloned()
        .unwrap_or_else(|| value.clone());

    (version, data)
}

/// 判断标记字符串是否声明为 V3（`chara_card_v3`）。
fn is_v3(mark: Option<&str>) -> bool {
    mark.is_some_and(|s| s.contains("v3"))
}

/// 判断标记字符串是否声明为 V2（`chara_card_v2`）。
fn is_v2(mark: Option<&str>) -> bool {
    mark.is_some_and(|s| s.contains("v2"))
}

/// 判断数据对象是否包含至少一个已知的角色卡字段。
fn looks_like_card(data: &Value) -> bool {
    const KNOWN: &[&str] = &[
        "name",
        "description",
        "personality",
        "scenario",
        "first_mes",
        "mes_example",
        "system_prompt",
        "system_instruction",
        "character_book",
        "lorebook",
        "style",
        "background",
    ];
    KNOWN
        .iter()
        .any(|k| data.get(*k).is_some_and(|v| !v.is_null()))
}

/// 在数据对象中按候选 key 顺序取第一个字符串字段。
fn get_string(data: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|k| data.get(*k).and_then(|v| v.as_str()).map(String::from))
}

/// 提取示例消息。
///
/// - V2/V3：`mes_example` 为 ChatItem（`{name, content}`）数组 → `"name: content"`。
/// - V1：`mes_example` 为字符串 → 按行拆分。
/// - V1：`examples` 也可能存在（V1 旧字段数组），一并兼容。
fn extract_example_messages(data: &Value) -> Vec<String> {
    let mut out = Vec::new();

    if let Some(example) = data.get("mes_example") {
        match example {
            Value::Array(items) => {
                for item in items {
                    if let Some(content) = item.get("content").and_then(|c| c.as_str()) {
                        let name = item
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or_default();
                        let text = if name.is_empty() {
                            content.to_string()
                        } else {
                            format!("{name}: {content}")
                        };
                        out.push(text);
                    }
                }
            }
            Value::String(s) => {
                for line in s.lines() {
                    let trimmed = line.trim();
                    if !trimmed.is_empty() {
                        out.push(trimmed.to_string());
                    }
                }
            }
            _ => {}
        }
    }

    // 兼容 V1 旧式 `examples`（数组）。
    if out.is_empty() {
        if let Some(Value::Array(items)) = data.get("examples") {
            for item in items {
                if let Some(s) = item.as_str() {
                    out.push(s.to_string());
                }
            }
        }
    }

    out
}

/// 提取默认问候语：`first_mes` 加上 `alternate_greetings`。
fn extract_greetings(data: &Value) -> Vec<String> {
    let mut greetings = Vec::new();

    if let Some(first) = data.get("first_mes").and_then(|v| v.as_str()) {
        if !first.trim().is_empty() {
            greetings.push(first.to_string());
        }
    }

    if let Some(Value::Array(alts)) = data.get("alternate_greetings") {
        for alt in alts {
            if let Some(s) = alt.as_str() {
                if !s.trim().is_empty() {
                    greetings.push(s.to_string());
                }
            }
        }
    }

    greetings
}

/// 提取 lorebook 条目（`character_book` 或 `lorebook` 对象下的 `entries` 数组）。
fn extract_lorebook(data: &Value) -> Vec<LorebookEntry> {
    let book = data
        .get("character_book")
        .or_else(|| data.get("lorebook"))
        .and_then(|v| v.as_object());

    let Some(book) = book else {
        return Vec::new();
    };

    let entries = match book.get("entries") {
        Some(Value::Array(entries)) => entries,
        _ => return Vec::new(),
    };

    entries.iter().filter_map(parse_lorebook_entry).collect()
}

/// 解析单个 lorebook 条目（宽松）。
fn parse_lorebook_entry(entry: &Value) -> Option<LorebookEntry> {
    let content = entry.get("content")?.as_str()?;
    let keywords = entry
        .get("keys")
        .and_then(|k| k.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|k| k.as_str())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();

    // disabled 与 enabled 二选一：优先 enabled，否则取 disabled 的反。
    let enabled = entry
        .get("enabled")
        .and_then(|e| e.as_bool())
        .or_else(|| entry.get("disabled").and_then(|d| d.as_bool()).map(|d| !d))
        .unwrap_or(true);

    let priority = entry
        .get("insertion_order")
        .and_then(|o| o.as_i64())
        .map(|o| o as i32)
        .unwrap_or(0);

    Some(LorebookEntry {
        keywords,
        content: content.to_string(),
        enabled,
        priority,
    })
}

/// 构建 metadata：记录原始 spec / spec_version 及少量来源字段。
fn build_metadata(root: &Value, version: CardVersion, data: &Value) -> Value {
    let mut metadata = serde_json::Map::new();

    let version_str = match version {
        CardVersion::V1 => "chara_card_v1",
        CardVersion::V2 => "chara_card_v2",
        CardVersion::V3 => "chara_card_v3",
    };
    metadata.insert(
        "spec_version".to_string(),
        Value::String(version_str.to_string()),
    );

    // 保留原始标记（若存在）。
    for key in ["spec", "spec_version"] {
        if let Some(Value::String(s)) = root.get(key) {
            metadata.insert(key.to_string(), Value::String(s.clone()));
        }
    }

    // 常见来源字段。
    for key in ["creator", "character_version"] {
        if let Some(Value::String(s)) = data.get(key) {
            metadata.insert(key.to_string(), Value::String(s.clone()));
        }
    }

    Value::Object(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    const V1_CARD: &str = r#"{
        "name": "Alice",
        "description": "A friendly girl",
        "personality": "Cheerful, curious",
        "scenario": "In a café",
        "first_mes": "Hello there!",
        "mes_example": "User: Hi\nAlice: Welcome!",
        "system_prompt": "You are Alice."
    }"#;

    const V2_CARD: &str = r#"{
        "spec": "chara_card_v2",
        "data": {
            "name": "Bob",
            "description": "A mysterious traveler",
            "personality": "Calm and observant",
            "scenario": "At a crossroads",
            "first_mes": "Greetings, traveler.",
            "alternate_greetings": ["Hello again.", "Nice to meet you."],
            "mes_example": [
                {"name": "User", "content": "Where are you headed?"},
                {"name": "Bob", "content": "I follow the wind."}
            ],
            "system_prompt": "You are Bob.",
            "post_history_instructions": "Always speak in a low voice.",
            "character_book": {
                "entries": [
                    {
                        "keys": ["wind", "travel"],
                        "content": "Bob is guided by the wind.",
                        "enabled": true,
                        "insertion_order": 5,
                        "selective": false
                    }
                ]
            },
            "creator": "SomeAuthor",
            "character_version": "1.0"
        }
    }"#;

    const V3_CARD: &str = r#"{
        "spec": "chara_card_v3",
        "spec_version": "chara_card_v3",
        "data": {
            "name": "Carol",
            "description": "A cheerful inventor. Here is a longer description\nwith multiple lines",
            "personality": "Inventive, energetic",
            "scenario": "In her workshop",
            "first_mes": "Hey! Welcome to my lab!",
            "mes_example": [
                {"name": "User", "content": "What is that machine?"},
                {"name": "Carol", "content": "It turns tea into stars!"}
            ],
            "system_instruction": "You are Carol, an inventor.",
            "lorebook": {
                "entries": [
                    {
                        "keys": ["machine", "stars"],
                        "content": "The star machine is Carol's proudest creation.",
                        "enabled": true,
                        "insertion_order": 2
                    },
                    {
                        "keys": ["tea"],
                        "content": "Carol drinks tea every morning.",
                        "disabled": true,
                        "insertion_order": 1
                    }
                ]
            }
        }
    }"#;

    #[test]
    fn parses_v1_card() {
        let def = parse_character_card(V1_CARD).expect("V1 应解析成功");
        assert_eq!(def.name, "Alice");
        assert_eq!(def.description.as_deref(), Some("A friendly girl"));
        assert_eq!(def.personality.as_deref(), Some("Cheerful, curious"));
        assert_eq!(def.scenario.as_deref(), Some("In a café"));
        assert_eq!(def.system_prompt.as_deref(), Some("You are Alice."));
        assert_eq!(def.greetings, vec!["Hello there!".to_string()]);
        assert_eq!(def.metadata["spec_version"], "chara_card_v1");
    }

    #[test]
    fn parses_v2_card_and_maps_fields() {
        let def = parse_character_card(V2_CARD).expect("V2 应解析成功");
        assert_eq!(def.name, "Bob");
        assert_eq!(def.description.as_deref(), Some("A mysterious traveler"));
        assert_eq!(
            def.post_history_instructions.as_deref(),
            Some("Always speak in a low voice.")
        );
        // first_mes + alternate_greetings
        assert_eq!(
            def.greetings,
            vec![
                "Greetings, traveler.".to_string(),
                "Hello again.".to_string(),
                "Nice to meet you.".to_string()
            ]
        );
        // ChatItem → "name: content"
        assert_eq!(
            def.example_messages,
            vec![
                "User: Where are you headed?".to_string(),
                "Bob: I follow the wind.".to_string()
            ]
        );
        assert_eq!(def.metadata["spec_version"], "chara_card_v2");
        assert_eq!(def.metadata["creator"], "SomeAuthor");
        assert_eq!(def.metadata["character_version"], "1.0");

        assert_eq!(def.lorebook.len(), 1);
        let entry = &def.lorebook[0];
        assert_eq!(
            entry.keywords,
            vec!["wind".to_string(), "travel".to_string()]
        );
        assert_eq!(entry.content, "Bob is guided by the wind.");
        assert!(entry.enabled);
        assert_eq!(entry.priority, 5);
    }

    #[test]
    fn parses_v3_card_and_knows_spec_version() {
        let def = parse_character_card(V3_CARD).expect("V3 应解析成功");
        assert_eq!(def.name, "Carol");
        // system_instruction → system_prompt
        assert_eq!(
            def.system_prompt.as_deref(),
            Some("You are Carol, an inventor.")
        );
        assert_eq!(def.metadata["spec_version"], "chara_card_v3");

        assert_eq!(def.lorebook.len(), 2);
        // disabled:true → enabled:false
        assert!(def.lorebook[0].enabled);
        assert!(!def.lorebook[1].enabled);
    }

    #[test]
    fn tolerates_missing_fields() {
        let json = r#"{
            "name": "Loose",
            "description": null,
            "personality": null,
            "system_prompt": null
        }"#;
        let def = parse_character_card(json).expect("缺失字段应被容忍");
        assert_eq!(def.name, "Loose");
        assert_eq!(def.description, None);
        assert_eq!(def.system_prompt, None);
        assert_eq!(def.greetings, Vec::<String>::new());
        assert!(def.lorebook.is_empty());
    }

    #[test]
    fn rejects_non_card_json() {
        // 空对象无法识别为卡片
        assert!(parse_character_card(r#"{}"#).is_err());
        // 数组无法识别
        assert!(parse_character_card(r#"[1, 2, 3]"#).is_err());
        // 非法 JSON
        assert!(parse_character_card(r#"not json"#).is_err());
    }

    #[test]
    fn v1_mes_example_splits_lines() {
        let json = r#"{
            "name": "Liney",
            "first_mes": "Hi",
            "mes_example": "User: hello\nLiney: hi there\nUser: how are you"
        }"#;
        let def = parse_character_card(json).unwrap();
        assert_eq!(
            def.example_messages,
            vec![
                "User: hello".to_string(),
                "Liney: hi there".to_string(),
                "User: how are you".to_string()
            ]
        );
    }

    #[test]
    fn parse_png_card_roundtrip() {
        let png_bytes = png_card::build_test_png(V2_CARD);
        let def = parse_png_character_card(&png_bytes).expect("PNG 卡应解析成功");
        assert_eq!(def.name, "Bob");
        assert_eq!(def.metadata["spec_version"], "chara_card_v2");
    }

    #[test]
    fn parse_png_card_roundtrip_with_non_ascii_text() {
        // 含中文等非 ASCII 文本的 V2 卡：验证 `chara` 文本不会被 Latin-1 重解码损坏。
        let json = r#"{
            "spec": "chara_card_v2",
            "data": {
                "name": "小琳",
                "description": "一位来自东方的旅人",
                "first_mes": "你好，欢迎来到我的小店！",
                "alternate_greetings": ["再次见面，幸会。"]
            }
        }"#;
        let png_bytes = png_card::build_test_png(json);
        let def = parse_png_character_card(&png_bytes).expect("含中文的 PNG 卡应解析成功");
        assert_eq!(def.name, "小琳");
        assert_eq!(def.description.as_deref(), Some("一位来自东方的旅人"));
        assert_eq!(
            def.greetings,
            vec![
                "你好，欢迎来到我的小店！".to_string(),
                "再次见面，幸会。".to_string()
            ]
        );
        assert_eq!(def.metadata["spec_version"], "chara_card_v2");
    }

    #[test]
    fn parse_png_without_card_returns_error() {
        // 一张不含任何 tEXt `chara` chunk 的最小 PNG，应触发 NoCardData。
        let plain_png = png_card::build_plain_png();
        let result = parse_png_character_card(&plain_png);
        assert!(result.is_err());
    }

    #[test]
    fn spec_only_v3_without_spec_version_is_v3() {
        // 只有 `spec: "chara_card_v3"`、无 `spec_version` 的卡应判定为 V3，而非 V1。
        let json = r#"{
            "spec": "chara_card_v3",
            "data": {
                "name": "Diana",
                "first_mes": "Hi"
            }
        }"#;
        let def = parse_character_card(json).expect("应解析成功");
        assert_eq!(def.metadata["spec_version"], "chara_card_v3");
    }
}
