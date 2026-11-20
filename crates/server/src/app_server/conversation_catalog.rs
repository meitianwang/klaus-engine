//! Conversation choices projected from complete source inventories.

use serde_json::{json, Map, Value};

/// Apply purpose filtering after a source read so old snapshots follow the same
/// rules as live catalogs, without removing metadata needed by dedicated calls.
pub(super) fn response(source: &str, models: Vec<Value>) -> Value {
    json!({"models":models.into_iter().filter(|model| is_conversation_model(source, model) && !curated_conversation_exclusion(source, &model_identity(model))).collect::<Vec<_>>()})
}

/// The lowercase identity a catalog row is matched by, mirroring the field
/// precedence every projection already uses.
fn model_identity(model: &Value) -> String {
    model
        .get("key")
        .or_else(|| model.get("id"))
        .or_else(|| model.get("serverModelName"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

/// Product curation: aged or small-context lineups kept out of the offered
/// conversation list. These rows are genuine chat models, so inventory
/// metadata keeps describing them as conversation-capable and an explicit
/// selection keeps working; only the picker narrows. Exact catalog identities
/// only, never name patterns, so a future lineup is offered until it is
/// deliberately added here.
const CURATED_ANTIGRAVITY: &[&str] = &["gpt-oss-120b-medium"];
const CURATED_MINIMAX: &[&str] = &[
    "minimax-m2",
    "minimax-m2.1",
    "minimax-m2.1-highspeed",
    "minimax-m2.5",
    "minimax-m2.5-highspeed",
    "minimax-m2.7",
    "minimax-m2.7-highspeed",
];
const CURATED_WORKBUDDY: &[&str] = &[
    "kimi-k2-instruct-taiji",
    "deepseek-r1-0528",
    "deepseek-r1-0528-lkeap",
    "deepseek-v3-0324",
    "deepseek-v3-0324-lkeap",
    "deepseek-v3-1",
    "deepseek-v3-1-lkeap",
    "deepseek-v3-1-volc",
    "deepseek-v3-2-volc",
    "glm-4.6v",
    "glm-4.6",
    "hunyuan-2.0-instruct",
    "hunyuan-chat",
    "hy3",
    "hy3-b",
    "hy3-c",
    "hy3-x",
    "completion-gf",
    "default-1.1",
    "default-1.2",
    "glm-5.0-turbo",
    "minimax-m2.5",
    "minimax-m2.7",
];
const CURATED_ZHIPU: &[&str] = &[
    "glm-4.5",
    "glm-4.5-air",
    "glm-4.6",
    "glm-4.7",
    "glm-5",
    "glm-5-turbo",
    "glm-5.1",
];

fn curated_conversation_exclusion(source: &str, key: &str) -> bool {
    let list = match source {
        "antigravity" => CURATED_ANTIGRAVITY,
        "minimax" => CURATED_MINIMAX,
        "workbuddy" => CURATED_WORKBUDDY,
        "zhipu" => CURATED_ZHIPU,
        _ => return false,
    };
    list.contains(&key)
}

pub(super) fn is_conversation_model(source: &str, model: &Value) -> bool {
    let key = model_identity(model);
    !dedicated_identity(source, &key)
        && [
            Some(model),
            model.get("providerMetadata"),
            model.get("capabilities"),
        ]
        .into_iter()
        .flatten()
        .all(conversation_declaration)
}

/// Keep the purpose decision when normalizing a source row. This marker stays
/// in the runtime inventory and is excluded from conversation responses.
pub(super) fn mark_purpose(source: &str, raw: &Value, entry: &mut Map<String, Value>) {
    let key = entry
        .get("key")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let key = if key.is_empty() {
        model_identity(raw)
    } else {
        key
    };
    if dedicated_identity(source, &key) || !is_conversation_model(source, raw) {
        entry.insert("conversationModel".into(), json!(false));
    }
}

/// Map non-chat Antigravity routing and restricted-placeholder aliases onto concrete chat models.
pub(super) fn canonical_antigravity_model_id(model: &str) -> &str {
    match model.trim() {
        "gemini-pro-orchestrator" => "gemini-4-carbon-high",
        "gemini-pro-orchestrator-argon" => "gemini-4-argon-high",
        "gemini-flash-worker-agy" => "gemini-4-hydrogen-xhigh",
        other => other,
    }
}

/// Keep placeholders distinct from explicit model choices until the account catalog is known.
pub(super) fn antigravity_placeholder_family(model: &str) -> Option<&str> {
    let family = model.strip_suffix("-disabled")?;
    (family
        .strip_prefix("gemini-")
        .or_else(|| family.strip_prefix("claude-"))
        .is_some_and(|name| !name.is_empty()))
    .then_some(family)
}

/// Rewrite legacy Antigravity harness aliases inside per-source tier maps before lookup.
pub(super) fn canonical_antigravity_tiers(tiers: &Map<String, Value>) -> Map<String, Value> {
    tiers
        .iter()
        .map(|(tier, value)| {
            let rewritten = value.as_str().map(|raw| {
                let trimmed = raw.trim();
                let prefixed = trimmed.starts_with("antigravity:");
                let bare = trimmed.strip_prefix("antigravity:").unwrap_or(trimmed);
                let canonical = canonical_antigravity_model_id(bare);
                if canonical == bare {
                    value.clone()
                } else if prefixed {
                    json!(format!("antigravity:{canonical}"))
                } else {
                    json!(canonical)
                }
            });
            (tier.clone(), rewritten.unwrap_or_else(|| value.clone()))
        })
        .collect()
}

fn conversation_declaration(model: &Value) -> bool {
    if model.get("conversationModel").and_then(Value::as_bool) == Some(false)
        || ["isInternal", "isInternalOnly"]
            .iter()
            .any(|field| model.get(*field).and_then(Value::as_bool) == Some(true))
        || model
            .get("visibility")
            .and_then(Value::as_str)
            .is_some_and(|visibility| !matches!(visibility, "list" | "default"))
    {
        return false;
    }
    let dedicated = [
        "image",
        "video",
        "audio",
        "music",
        "transcription",
        "search",
        "completion",
        "code-completion",
        "code-rewrite",
        "code-navigation",
        "embedding",
        "rerank",
        "image-generation",
        "video-generation",
        "text-to-image",
        "image-to-image",
        "text-to-video",
        "image-to-video",
        "text-to-speech",
        "speech-to-text",
    ];
    let task_tags = [
        "code-completion",
        "code-rewrite",
        "code-navigation",
        "image-generation",
        "video-generation",
        "text-to-image",
        "image-to-image",
        "text-to-video",
        "image-to-video",
        "text-to-speech",
        "speech-to-text",
    ];
    if ["purpose", "taskType", "task_type"]
        .iter()
        .filter_map(|field| model.get(*field).and_then(Value::as_str))
        .any(|purpose| dedicated.contains(&purpose))
        || model
            .get("tags")
            .and_then(Value::as_array)
            .is_some_and(|tags| {
                tags.iter()
                    .filter_map(Value::as_str)
                    .any(|tag| task_tags.contains(&tag))
            })
        || model.get("videoGeneration").is_some_and(Value::is_object)
    {
        return false;
    }
    // Input image, audio and video support describes a multimodal conversation.
    // Only an explicit output declaration without text establishes another task.
    ["outputModalities", "output_modalities"]
        .iter()
        .all(|field| {
            model
                .get(*field)
                .and_then(Value::as_array)
                .is_none_or(|modalities| {
                    modalities.is_empty()
                        || !modalities.iter().all(|modality| {
                            modality.as_str().is_some_and(|value| {
                                ["image", "audio", "video", "embedding", "embeddings"]
                                    .contains(&value)
                            })
                        })
                })
        })
}

/// Sparse source catalogs carry identities without task declarations. Match
/// declared dedicated families and exact editor-task routes, never chat names,
/// availability flags, tool eligibility or input capabilities.
fn dedicated_identity(source: &str, key: &str) -> bool {
    let wan = key
        .strip_prefix("wan")
        .is_some_and(|version| version.as_bytes().first().is_some_and(u8::is_ascii_digit));
    if wan
        || [
            "gpt-image-",
            "qwen-image-",
            "doubao-seedance-",
            "doubao-seedream-",
        ]
        .iter()
        .any(|prefix| key.starts_with(prefix))
    {
        return true;
    }
    match source {
        "antigravity" => {
            key.starts_with("gemini-pro-orchestrator")
                || key == "gemini-flash-worker-agy"
                || antigravity_placeholder_family(key).is_some()
        }
        "kiro" => key == "auto" || key.starts_with("auto::effort="),
        "bailian" => {
            key == "decision-model-preview"
                || (key.starts_with("qwen-audio-")
                    && ["asr", "tts", "realtime"]
                        .iter()
                        .any(|task| key.split('-').any(|component| component == *task)))
        }
        "minimax" => {
            key == "image-01" || key.starts_with("speech-") || key.starts_with("minimax-h3")
        }
        "zhipu" => key.starts_with("glm-tts") || key.starts_with("glm-asr"),
        "qwenwork" => [
            "open-image-",
            "banana-image-",
            "seedance-",
            "happy-horse-",
            "minimax-music-",
        ]
        .iter()
        .any(|prefix| key.starts_with(prefix)),
        "workbuddy" => {
            ["seedance-", "kling-", "hunyuan-image-", "hunyuan-video-"]
                .iter()
                .any(|prefix| key.starts_with(prefix))
                || [
                    "gemini-3.0-pro-image",
                    "gemini-3.1-flash-image",
                    "gemini-2.5-flash-image",
                    "codewise-completions",
                    "codewise-rewrite",
                    "codewise-nes-a4-027-aide",
                    "codewise-jump",
                    "deepseek-v3-0324-taco-completion",
                ]
                .contains(&key)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_exclusions_are_scoped_to_their_source() {
        for (source, key) in [
            ("antigravity", "gemini-pro-orchestrator"),
            ("antigravity", "gemini-pro-orchestrator-argon"),
            ("antigravity", "gemini-flash-worker-agy"),
            ("bailian", "decision-model-preview"),
            ("kiro", "auto"),
            ("kiro", "auto::effort=max"),
        ] {
            let model = json!({"key":key});
            assert!(response(source, vec![model.clone()])["models"]
                .as_array()
                .unwrap()
                .is_empty());
            assert_eq!(
                response("custom", vec![model.clone()])["models"],
                json!([model])
            );
        }
    }

    #[test]
    fn text_output_and_multimodal_input_remain_conversation_choices() {
        let rows = vec![
            json!({"key":"image-aware-chat","outputModalities":["text","audio"],"supports_video_in":true,"tags":["image","video","audio","search"]}),
            json!({"key":"future-chat","type":"model","supportsToolCall":false,"disabled":true}),
            json!({"key":"unknown-output-chat","outputModalities":["custom-text"]}),
            json!({"key":"native-generation","purpose":"image"}),
            json!({"key":"private-chat","providerMetadata":{"isInternalOnly":true}}),
        ];
        let result = response("custom", rows.clone());
        assert_eq!(result["models"], json!(&rows[..3]));
    }

    #[test]
    fn curated_exclusions_hide_every_listed_identity() {
        for (source, list) in [
            ("antigravity", CURATED_ANTIGRAVITY),
            ("minimax", CURATED_MINIMAX),
            ("workbuddy", CURATED_WORKBUDDY),
            ("zhipu", CURATED_ZHIPU),
        ] {
            for key in list {
                // A genuine chat declaration must not save a curated identity.
                let model = json!({"key":key,"supportsToolCall":true});
                assert!(
                    response(source, vec![model.clone()])["models"]
                        .as_array()
                        .unwrap()
                        .is_empty(),
                    "{source}/{key} must stay out of the conversation list"
                );
                // Inventory metadata stays truthful: curation is not a purpose
                // claim, so the row is never marked non-conversation.
                let mut entry = Map::new();
                entry.insert("key".into(), json!(key));
                mark_purpose(source, &model, &mut entry);
                assert!(
                    !entry.contains_key("conversationModel"),
                    "{source}/{key} must not gain a purpose marker"
                );
                // The exclusion is scoped to its own source.
                assert_eq!(
                    response("custom", vec![model.clone()])["models"],
                    json!([model])
                );
            }
        }
    }

    #[test]
    fn curated_exclusions_keep_neighbouring_identities() {
        for (source, key) in [
            ("antigravity", "claude-sonnet-4-6"),
            ("antigravity", "claude-opus-4-6-thinking"),
            ("antigravity", "gpt-oss-120b"),
            ("minimax", "minimax-m3"),
            ("minimax", "minimax-m3.1-flash"),
            ("minimax", "minimax-m3.1-flash-preview"),
            ("workbuddy", "kimi-k2-thinking"),
            ("workbuddy", "kimi-k2.5"),
            ("workbuddy", "glm-5.2"),
            ("zhipu", "glm-5.2"),
            ("zhipu", "glm-5.3"),
            ("zhipu", "glm-5.3-flash"),
            ("zhipu", "glm-5.3-flashx"),
            ("zhipu", "glm-5.3-flashx-legal"),
        ] {
            let model = json!({"key":key});
            assert_eq!(
                response(source, vec![model.clone()])["models"],
                json!([model]),
                "{source}/{key} must remain a conversation choice"
            );
        }
    }
}
