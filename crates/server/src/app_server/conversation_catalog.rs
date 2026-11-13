//! Conversation choices projected from complete source inventories.

use serde_json::{json, Map, Value};

/// Apply purpose filtering after a source read so old snapshots follow the same
/// rules as live catalogs, without removing metadata needed by dedicated calls.
pub(super) fn response(source: &str, models: Vec<Value>) -> Value {
    json!({"models":models.into_iter().filter(|model| is_conversation_model(source, model)).collect::<Vec<_>>()})
}

pub(super) fn is_conversation_model(source: &str, model: &Value) -> bool {
    let key = model
        .get("key")
        .or_else(|| model.get("id"))
        .or_else(|| model.get("serverModelName"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
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
    if !is_conversation_model(source, raw) {
        entry.insert("conversationModel".into(), json!(false));
    }
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
}
