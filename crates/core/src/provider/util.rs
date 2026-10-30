//! Small helpers shared verbatim by the provider adapters.

use crate::provider::classify::{classify_error, RawProviderError};
use crate::provider::retry::RetryError;
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::schemas::content::{ContentBlock, OutputBlock};
use crate::schemas::message::ToolResultMessage;
use crate::types::errors::ProviderError;
use crate::types::provider::ToolSpec;

/// Project output images before existing attachments for text-only tool wires.
/// Keep the stored result intact for adapters that support images inside results.
pub(crate) fn tool_result_user_blocks(result: &ToolResultMessage) -> Vec<ContentBlock> {
    result
        .content
        .iter()
        .filter_map(|block| match block {
            OutputBlock::Image(image) => Some(ContentBlock::Image(image.clone())),
            OutputBlock::Text(_) => None,
        })
        .chain(result.extra_user_blocks.iter().flatten().cloned())
        .collect()
}

/// Injective ASCII rendering used only for scalar diagnostics: every code
/// point renders as itself, and a lone surrogate renders as `\uXXXX`.
pub(crate) fn diagnostic_string(value: &RuntimeString) -> String {
    let mut output = String::new();
    for point in value.code_points() {
        if let Some(character) = char::from_u32(point) {
            output.push(character);
        } else {
            use std::fmt::Write as _;
            write!(&mut output, "\\u{point:04x}").expect("writing to a String cannot fail");
        }
    }
    output
}

/// ASCII-only lowercasing over the exact UTF-16 carrier (non-ASCII units pass
/// through untouched), for provider/model classification keys.
pub(crate) fn ascii_lowercase(value: &RuntimeString) -> RuntimeString {
    RuntimeString::from_utf16(
        value
            .as_utf16()
            .iter()
            .copied()
            .map(|unit| {
                if (u16::from(b'A')..=u16::from(b'Z')).contains(&unit) {
                    unit + u16::from(b'a' - b'A')
                } else {
                    unit
                }
            })
            .collect::<Vec<_>>(),
    )
}

/// Fold an auth-source failure back into raw signals so the retry classifier
/// re-reads it as any other establishment failure (only its status and message
/// are load-bearing signals; the classifier re-derives the rest).
pub(crate) fn raw_from_provider_error(err: ProviderError) -> RawProviderError {
    RawProviderError {
        status: err.status,
        message: diagnostic_string(&err.message),
        ..Default::default()
    }
}

/// Collapse the retry outcome onto a single classified error. An abort never
/// reaches a surfaced error event (the caller checks the signal first), so it
/// maps to a benign placeholder.
pub(crate) fn retry_err_to_provider(err: RetryError) -> ProviderError {
    match err {
        RetryError::Provider(pe) => pe,
        RetryError::Aborted => classify_error(&RawProviderError {
            message: "aborted".to_string(),
            ..Default::default()
        }),
    }
}

/// Make a replayed tool-call input acceptable to a wire that requires an object.
///
/// Freeform tools (apply_patch and friends) carry their input as a raw TEXT
/// string, which is exactly right on the Responses wire where it rides a
/// `custom_tool_call.input` field. On object-shaped wires (Chat Completions
/// `function.arguments`, Anthropic `tool_use.input`) the same history replays
/// as a quoted string literal, and strict gateways reject the whole request
/// with a "function.arguments must be JSON" style 400 — so a session that ran
/// a freeform call under one wire dies on every later turn under another.
///
/// The repair wraps the raw text into the single string property the tool's
/// own declared schema requires (apply_patch declares `patch`), so the object
/// both parses and validates. Objects pass through untouched, and a schema
/// without exactly one required string property falls back to `value`.
pub(crate) fn objectified_tool_input(input: &RuntimeJson, tools: &[ToolSpec]) -> RuntimeJson {
    if input.is_object() {
        return input.clone();
    }
    let mut object = RuntimeObject::new();
    object.insert(single_string_property(tools), input.clone());
    RuntimeJson::Object(object)
}

fn single_string_property(tools: &[ToolSpec]) -> RuntimeString {
    let mut candidate: Option<RuntimeString> = None;
    for tool in tools {
        let Some(properties) = tool
            .input_schema
            .get("properties")
            .and_then(|v| v.as_object())
        else {
            continue;
        };
        let mut strings = properties.entries().filter_map(|(key, schema)| {
            schema
                .get("type")
                .and_then(|t| t.as_str())
                .filter(|t| *t == "string")
                .map(|_| key)
        });
        let Some(first) = strings.next() else {
            continue;
        };
        if strings.next().is_some() {
            continue;
        }
        candidate = Some(first);
        break;
    }
    candidate.unwrap_or_else(|| RuntimeString::from("value"))
}
