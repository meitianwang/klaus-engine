//! Tool construction helpers: the fail-closed defaults a tool inherits for its
//! commonly-stubbed predicates, plus a convenience for plain-text results.
//!
//! A tool that does not care to override a predicate delegates to the matching
//! default here, so callers never need a `?.() ?? x` dance.

use crate::runtime_values::RuntimeString;
use crate::schemas::content::{HarnessEnvelope, OutputBlock, TextBlock};
use crate::types::json::Json;
use crate::types::tool::{ToolResult, ToolResultSerializable};

/// Tools are enabled unless they say otherwise.
pub const DEFAULT_IS_ENABLED: bool = true;
/// Fail closed: assume NOT parallel-safe until a tool opts in.
pub const DEFAULT_IS_CONCURRENCY_SAFE: bool = false;
/// Fail closed: assume the tool writes until it declares itself read-only.
pub const DEFAULT_IS_READ_ONLY: bool = false;
/// Fail closed: assume non-destructive until a tool says otherwise.
pub const DEFAULT_IS_DESTRUCTIVE: Option<bool> = Some(false);

/// Convenience: a plain text tool result.
pub fn text_result(output: Json, text: impl Into<RuntimeString>, is_error: bool) -> ToolResult {
    ToolResult {
        serializable: ToolResultSerializable {
            output,
            model_content: vec![OutputBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: Some(is_error),
            mcp_meta: None,
            extra_user_blocks: None,
        },
        side_effects: None,
    }
}

pub fn text_result_with_envelope(
    output: Json,
    text: impl Into<RuntimeString>,
    is_error: bool,
    envelope: HarnessEnvelope,
) -> ToolResult {
    ToolResult {
        serializable: ToolResultSerializable {
            output,
            model_content: vec![OutputBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: Some(envelope),
                message_id: None,
                phase: None,
            })],
            is_error: Some(is_error),
            mcp_meta: None,
            extra_user_blocks: None,
        },
        side_effects: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_json, RuntimeString};

    fn exact_json(source: &str) -> Json {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact runtime JSON")
    }

    #[test]
    fn text_result_wraps_output_in_a_single_text_block() {
        let r = text_result(
            exact_json(r#"{"ok":true}"#),
            RuntimeString::from("hello"),
            false,
        );
        assert_eq!(r.serializable.output, exact_json(r#"{"ok":true}"#));
        assert_eq!(r.serializable.is_error, Some(false));
        assert_eq!(
            r.serializable.model_content,
            vec![OutputBlock::Text(TextBlock {
                text: "hello".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })]
        );
        assert!(r.serializable.mcp_meta.is_none());
        assert!(r.serializable.extra_user_blocks.is_none());
        assert!(r.side_effects.is_none());
    }

    #[test]
    fn text_result_marks_errors() {
        let r = text_result(Json::Null, RuntimeString::from("boom"), true);
        assert_eq!(r.serializable.is_error, Some(true));
    }

    #[test]
    fn text_result_with_envelope_keeps_a_typed_hint() {
        let r = text_result_with_envelope(
            exact_json(r#"{"ok":true}"#),
            RuntimeString::from("advisory"),
            false,
            HarnessEnvelope::SystemReminder,
        );
        assert_eq!(
            r.serializable.model_content,
            vec![OutputBlock::Text(TextBlock {
                text: "advisory".into(),
                citations: None,
                harness_envelope: Some(HarnessEnvelope::SystemReminder),
                message_id: None,
                phase: None,
            })]
        );
    }

    #[test]
    fn defaults_are_fail_closed() {
        let defaults = (
            DEFAULT_IS_ENABLED,
            DEFAULT_IS_CONCURRENCY_SAFE,
            DEFAULT_IS_READ_ONLY,
            DEFAULT_IS_DESTRUCTIVE,
        );
        assert_eq!(defaults, (true, false, false, Some(false)));
    }
}
