use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::schemas::content::{ImageBlock, OutputBlock, TextBlock};
use crate::schemas::message::{Message, ToolResultMessage};
use crate::tools::result_storage::{
    apply_per_message_budget, maybe_spill_result, output_blocks_text, SpillOptions,
};

const SCENARIO: &str = "tool-result-shaping-and-truncation";

pub async fn tool_result_trace() -> Result<RuntimeJson, RuntimeCodecError> {
    let base_dir = std::env::temp_dir().join("klaus-tool-result-trace");
    let _ = std::fs::remove_dir_all(&base_dir);
    std::fs::create_dir_all(&base_dir).unwrap();

    let mixed_content = vec![
        text_out("alpha"),
        OutputBlock::Image(ImageBlock {
            mime_type: "image/png".into(),
            base64: "iVBORw0KGgo=".into(),
            detail: None,
            source_path: None,
        }),
        text_out("omega"),
    ];
    let small = maybe_spill_result(
        mixed_content.clone(),
        SpillOptions {
            max_chars: 100.0,
            dir: runtime_path(&base_dir.join("spill")),
            id: "small".into(),
        },
        legacy_spill_text,
    )
    .await;

    let oversized_text = "A😀B".repeat(40);
    let oversized = maybe_spill_result(
        vec![text_out(&oversized_text)],
        SpillOptions {
            max_chars: 7.0,
            dir: runtime_path(&base_dir.join("spill")),
            id: "oversized".into(),
        },
        legacy_spill_text,
    )
    .await;
    let oversized_disk_text = match &oversized.spilled_path {
        Some(_) => std::fs::read_to_string(base_dir.join("spill/oversized.txt")).unwrap(),
        None => String::new(),
    };

    std::fs::write(base_dir.join("blocker"), "x").unwrap();
    let failed_spill = maybe_spill_result(
        vec![text_out(&oversized_text)],
        SpillOptions {
            max_chars: 7.0,
            dir: runtime_path(&base_dir.join("blocker").join("nested")),
            id: "blocked".into(),
        },
        legacy_spill_text,
    )
    .await;

    let budgeted = apply_per_message_budget(
        vec![
            msg("tu_large", &"L".repeat(500)),
            msg("tu_small", "tiny"),
            msg("tu_medium", &"M".repeat(420)),
        ],
        520,
        legacy_trimmed_text,
    );
    let budgeted_values: Vec<RuntimeJson> = budgeted
        .into_iter()
        .map(|message| runtime_codec::encode_value(&Message::ToolResult(message)))
        .collect::<Result<_, _>>()?;

    let mut oversized_value = spill_value(oversized.content, oversized.spilled_path)?;
    {
        let object = oversized_value.as_object_mut().unwrap();
        object.insert(
            "diskTextUnits",
            RuntimeJson::from(oversized_disk_text.encode_utf16().count()),
        );
        object.insert(
            "diskTextPrefix",
            RuntimeJson::String(RuntimeString::from(&oversized_disk_text).slice_utf16(..7)),
        );
    }

    let mut trace = RuntimeObject::new();
    trace.insert("scenario", SCENARIO.into());
    trace.insert(
        "outputBlocksText",
        RuntimeJson::String(output_blocks_text(&mixed_content)),
    );
    trace.insert(
        "smallSpill",
        spill_value(small.content, small.spilled_path)?,
    );
    trace.insert("oversizedSpill", oversized_value);
    trace.insert(
        "failedSpill",
        spill_value(failed_spill.content, failed_spill.spilled_path)?,
    );
    trace.insert("perMessageBudget", RuntimeJson::Array(budgeted_values));
    let mut trace = RuntimeJson::Object(trace);
    normalize_paths(
        &mut trace,
        base_dir.to_str().expect("test path is valid Unicode"),
    );
    Ok(trace)
}

fn spill_value(
    content: Vec<OutputBlock>,
    spilled_path: Option<RuntimePath>,
) -> Result<RuntimeJson, RuntimeCodecError> {
    let mut object = RuntimeObject::new();
    object.insert("content", runtime_codec::encode_value(&content)?);
    if let Some(path) = spilled_path {
        object.insert(
            "spilledPath",
            RuntimeJson::String(path.into_runtime_string()),
        );
    }
    Ok(RuntimeJson::Object(object))
}

fn msg(tool_use_id: &str, text: &str) -> ToolResultMessage {
    ToolResultMessage {
        uuid: format!("uuid_{tool_use_id}").into(),
        tool_use_id: tool_use_id.into(),
        content: vec![text_out(text)],
        is_error: false,
        tool_kind: None,
        native_tool_output: None,
        output: None,
        extra_user_blocks: None,
    }
}

fn text_out(text: &str) -> OutputBlock {
    OutputBlock::Text(TextBlock {
        text: text.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}

fn legacy_spill_text(
    preview: &RuntimeString,
    full_chars: usize,
    path: &RuntimePath,
) -> RuntimeString {
    preview
        .concat(&format!("\n\n… [output truncated; full {full_chars} chars saved to ").into())
        .concat(path.as_runtime_string())
        .concat(&"] …".into())
}

fn legacy_trimmed_text(kept_text: &RuntimeString, omitted_chars: usize) -> RuntimeString {
    kept_text
        .concat(&format!("\n… [trimmed {omitted_chars} chars to fit the context budget] …").into())
}

fn runtime_path(path: &std::path::Path) -> RuntimePath {
    RuntimePath::from(path.to_str().expect("test path is valid Unicode"))
}

/// Erase the temp root so one committed golden covers every host.
///
/// Both spellings are erased: `base_dir` arrives in the host dialect, while the paths the
/// trace reports are POSIX-rendered, which is the shape the engine hands the model.
fn normalize_paths(value: &mut RuntimeJson, base_dir: &str) {
    match value {
        RuntimeJson::String(text) => {
            *text = text
                .replace(base_dir, "<TMP>")
                .replace(&base_dir.replace('\\', "/"), "<TMP>")
        }
        RuntimeJson::Array(items) => {
            for item in items {
                normalize_paths(item, base_dir);
            }
        }
        RuntimeJson::Object(object) => {
            for item in object.values_mut() {
                normalize_paths(item, base_dir);
            }
        }
        RuntimeJson::Null | RuntimeJson::Bool(_) | RuntimeJson::Number(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::behavior::{
        render_tool_result_content, resolve_provider_behavior_profile,
    };
    use crate::schemas::content::HarnessEnvelope;
    use crate::types::provider::ProviderToolCallKind;

    #[tokio::test]
    async fn trace_matches_the_shared_tool_result_golden() {
        let expected = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            include_str!("../tools/fixtures/tool_result_trace.json"),
        ))
        .unwrap();

        assert_eq!(tool_result_trace().await.unwrap(), expected);
    }

    #[test]
    fn persisted_tool_result_consumes_harness_marker_and_omits_transient_input_format() {
        let profile = resolve_provider_behavior_profile(None, &"gpt-5".into());
        let content = render_tool_result_content(
            vec![OutputBlock::Text(TextBlock {
                text: "Use the discovered tool.".into(),
                citations: None,
                harness_envelope: Some(HarnessEnvelope::SystemReminder),
                message_id: None,
                phase: None,
            })],
            &"read".into(),
            false,
            &profile,
        );
        assert_eq!(
            content,
            vec![OutputBlock::Text(TextBlock {
                text: "# System Reminder\n\nUse the discovered tool.".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })]
        );

        let value = runtime_codec::encode_value(&Message::ToolResult(ToolResultMessage {
            uuid: "result-custom".into(),
            tool_use_id: "call-custom".into(),
            content,
            is_error: false,
            tool_kind: Some(ProviderToolCallKind::Custom),
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        }))
        .unwrap();
        assert_eq!(value["toolKind"], RuntimeJson::from("custom"));
        assert!(value.pointer("/content/0/harnessEnvelope").is_none());
        assert!(value.get("inputFormat").is_none());
    }
}
