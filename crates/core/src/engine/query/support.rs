//! Small shared helpers: text/JSON utilities, notification emission, hook
//! input scaffolding, and queue durability error mapping.

// ---------------------------------------------------------------------------
// Small pure helpers.
// ---------------------------------------------------------------------------
use super::EventQueue;
use crate::hooks::types::HookEvent;
use crate::hooks::types::HookInput;
use crate::runtime_process::NodePathDialect;
use crate::runtime_values::runtime_json;
use crate::runtime_values::RuntimeObject;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
use crate::schemas::content::ContentBlock;
use crate::schemas::content::OutputBlock;
use crate::schemas::content::TextBlock;
use crate::schemas::message::Message;
use crate::types::errors::EngineError;
use crate::types::errors::EngineErrorCode;
use crate::types::events::EngineEvent;
use crate::types::events::NotificationLevel;
use crate::types::json::Json;
use crate::types::tool::Tool;
use std::sync::Arc;
#[cfg(test)]
pub(super) fn text_out(text: String) -> OutputBlock {
    OutputBlock::Text(TextBlock {
        text: text.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}

pub(super) fn text_out_exact(text: RuntimeString) -> OutputBlock {
    OutputBlock::Text(TextBlock {
        text,
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}

pub(super) fn permission_denial_message(message: &str) -> RuntimeString {
    RuntimeString::from("Permission denied: ").concat(&message.into())
}

pub(super) fn unknown_tool_message(tool_name: &RuntimeString) -> RuntimeString {
    RuntimeString::from("Unknown tool: ").concat(tool_name)
}

pub(super) fn notify(q: &EventQueue, level: NotificationLevel, message: impl Into<RuntimeString>) {
    q.push(EngineEvent::Notification {
        level,
        message: message.into(),
    });
}

pub(super) fn count_mcp_tools(tools: &[Arc<dyn Tool>]) -> usize {
    tools.iter().filter(|t| t.mcp_server().is_some()).count()
}

pub(super) fn safe_runtime_json_parse(input: &RuntimeString) -> Json {
    if input.is_empty() {
        return Json::Object(RuntimeObject::new());
    }
    runtime_json::parse(input).unwrap_or_else(|_| Json::Object(RuntimeObject::new()))
}

/// Close an argument JSON that was cut off mid-document by the output ceiling.
///
/// The cut is made at the last COMPLETE VALUE, not at the last non-whitespace
/// character. That distinction is the whole design: a truncation usually lands
/// inside a string that was still being written, and everything before it — the
/// path, the earlier fields — is worth keeping, while a cut placed anywhere else
/// either loses that string's content or invents a value that never arrived.
///
/// Concretely, a checkpoint is recorded each time a value finishes (a string
/// closes in value position, a number or literal ends, a container closes), and
/// the repair is `prefix + closing quote if the cut was inside a string + the
/// closers for whatever containers are still open`. So:
///
/// - `{"path":"a.txt","content":"line one` keeps BOTH fields, with the partial
///   content — exactly what the write append protocol then finishes;
/// - `{"a":1,` and `{"a":1,"b":` both fall back to `{"a":1}`, because a comma or
///   a colon introduces a member that never arrived;
/// - a cut inside an escape sequence drops back to before the backslash, since a
///   half-written `\u00` has no valid completion.
///
/// The repaired text is parsed by the runtime's OWN parser, so unescaping is the
/// parser's rather than an approximation here: recovered file content cannot be
/// silently mangled. `None` means no checkpoint existed or the parser still
/// rejects the result, and the caller then leaves the call dropped rather than
/// acting on a guess.
///
/// NOTE: this only makes the text parseable. Whether the recovered VALUES are safe
/// to act on is the caller's question, and it is a different question per tool: a
/// value cut short mid-string is fine for a file's contents (the rest can be
/// appended) and NOT fine for a shell command.
pub(super) fn repair_truncated_json(
    raw: &RuntimeString,
    required_partial_property: Option<&str>,
) -> Option<RuntimeString> {
    // A document that already parses needs no repair, and repairing it anyway
    // would only risk changing it.
    if runtime_json::parse(raw).is_ok() {
        return None;
    }
    let units = raw.as_utf16();
    let quote = u16::from(b'"');
    let backslash = u16::from(b'\\');

    /// One open container. `after_colon` tells a string in object position apart
    /// from a value: a key completing is NOT a place to cut, because closing
    /// there would leave a key with no value.
    #[derive(Clone, Copy)]
    enum Frame {
        Object { after_colon: bool },
        Array,
    }

    fn closers_for(frames: &[Frame]) -> Vec<u16> {
        frames
            .iter()
            .rev()
            .map(|frame| match frame {
                Frame::Object { .. } => u16::from(b'}'),
                Frame::Array => u16::from(b']'),
            })
            .collect()
    }

    let mut frames: Vec<Frame> = Vec::new();
    let mut in_string = false;
    let mut string_is_key = false;
    let mut string_start = 0usize;
    let mut root_key: Option<RuntimeString> = None;
    let mut escaped = false;
    // Hex digits still owed by a `\uXXXX` escape in progress.
    let mut unicode_remaining: u8 = 0;
    // Index of the backslash that opened the escape in progress.
    let mut escape_start = 0usize;
    let mut literal_active = false;
    // `(cut index, cut was inside a string, closers to append)`.
    let mut checkpoint: Option<(usize, bool, Vec<u16>)> = None;

    for (index, unit) in units.iter().enumerate() {
        let unit = *unit;
        if in_string {
            if unicode_remaining > 0 {
                unicode_remaining -= 1;
                continue;
            }
            if escaped {
                escaped = false;
                if unit == u16::from(b'u') {
                    unicode_remaining = 4;
                }
                continue;
            }
            if unit == backslash {
                escaped = true;
                escape_start = index;
                continue;
            }
            if unit == quote {
                in_string = false;
                if string_is_key && frames.len() == 1 {
                    let key = RuntimeString::from_utf16(units[string_start..=index].to_vec());
                    root_key = runtime_json::parse(&key).ok()?.as_string().cloned();
                } else if !string_is_key {
                    checkpoint = Some((index + 1, false, closers_for(&frames)));
                    if let Some(Frame::Object { after_colon }) = frames.last_mut() {
                        *after_colon = false;
                    }
                }
            }
            continue;
        }
        // Outside a string: any character that cannot continue a literal ends it,
        // and a finished literal is a finished value.
        if literal_active && !is_json_literal_char(unit) {
            literal_active = false;
            checkpoint = Some((index, false, closers_for(&frames)));
            if let Some(Frame::Object { after_colon }) = frames.last_mut() {
                *after_colon = false;
            }
        }
        if unit == quote {
            in_string = true;
            string_start = index;
            string_is_key = matches!(frames.last(), Some(Frame::Object { after_colon: false }));
        } else if unit == u16::from(b'{') {
            frames.push(Frame::Object { after_colon: false });
        } else if unit == u16::from(b'[') {
            frames.push(Frame::Array);
        } else if unit == u16::from(b'}') || unit == u16::from(b']') {
            frames.pop();
            // A closed container is itself a complete value in its parent.
            checkpoint = Some((index + 1, false, closers_for(&frames)));
            if let Some(Frame::Object { after_colon }) = frames.last_mut() {
                *after_colon = false;
            }
        } else if unit == u16::from(b':') {
            if let Some(Frame::Object { after_colon }) = frames.last_mut() {
                *after_colon = true;
            }
        } else if unit == u16::from(b',') {
            if let Some(Frame::Object { after_colon }) = frames.last_mut() {
                *after_colon = false;
            }
        } else if is_json_literal_char(unit) {
            literal_active = true;
        }
    }
    if let Some(required) = required_partial_property {
        // A write may recover only its unfinished top-level content string.
        // Repairing a partial path or a trailing option could change the target
        // file or the write mode, so those calls must be reissued in full.
        if !in_string
            || string_is_key
            || frames.len() != 1
            || root_key.as_ref().is_none_or(|key| key != required)
        {
            return None;
        }
    }
    if literal_active {
        checkpoint = Some((units.len(), false, closers_for(&frames)));
    }
    if in_string && !string_is_key {
        // The string was still being written when the budget ran out — the common
        // case, and the one the write append protocol exists to finish. Keep what
        // arrived, unless the cut landed inside an escape sequence: a half-written
        // `\u00` or a dangling backslash has no valid completion, so fall back to
        // before it. This checkpoint is always longer than an earlier one, so it
        // wins.
        let end = if escaped || unicode_remaining > 0 {
            escape_start
        } else {
            units.len()
        };
        if end > 0 {
            checkpoint = Some((end, true, closers_for(&frames)));
        }
    }

    let (end, close_string, closers) = checkpoint?;
    let mut repaired = RuntimeString::from_utf16(units[..end].to_vec());
    if close_string {
        repaired = repaired.concat(&RuntimeString::from_utf16(vec![quote]));
    }
    if !closers.is_empty() {
        repaired = repaired.concat(&RuntimeString::from_utf16(closers));
    }
    // The parser is the authority: if the repair does not hold up, say so rather
    // than handing the caller text it would have to re-validate anyway.
    runtime_json::parse(&repaired).ok()?;
    Some(repaired)
}

/// A character that can appear in a JSON number or in `true` / `false` / `null`.
fn is_json_literal_char(unit: u16) -> bool {
    let digit = (u16::from(b'0'))..=(u16::from(b'9'));
    let lower = (u16::from(b'a'))..=(u16::from(b'z'));
    let upper = (u16::from(b'A'))..=(u16::from(b'Z'));
    digit.contains(&unit)
        || lower.contains(&unit)
        || upper.contains(&unit)
        || unit == u16::from(b'-')
        || unit == u16::from(b'+')
        || unit == u16::from(b'.')
}

pub(super) fn trim_ecmascript_whitespace(value: &RuntimeString) -> RuntimeString {
    fn is_whitespace(unit: u16) -> bool {
        matches!(
            unit,
            0x0009..=0x000d
                | 0x0020
                | 0x00a0
                | 0x1680
                | 0x2000..=0x200a
                | 0x2028
                | 0x2029
                | 0x202f
                | 0x205f
                | 0x3000
                | 0xfeff
        )
    }

    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_whitespace(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !is_whitespace(*unit))
        .map(|index| index + 1)
        .unwrap_or(start);
    value.slice_utf16(start..end)
}

pub(super) fn is_valid_runtime_json(value: &RuntimeString) -> bool {
    runtime_json::parse(&trim_ecmascript_whitespace(value)).is_ok()
}

/// Exact runtime JSON is already safe for a tool-output payload.
pub(super) fn to_json_safe(v: &Json) -> Json {
    v.clone()
}

/// Pretty-print a value for a tool-output text block. Never throws.
pub(super) fn safe_json_text(v: &Json) -> RuntimeString {
    runtime_json::stringify_pretty(v)
}

/// The concatenated user text of the most recent user turn, or `None`.
pub(super) fn last_user_text(messages: &[Message]) -> Option<RuntimeString> {
    for m in messages.iter().rev() {
        if let Message::User(u) = m {
            let text = u
                .content
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::Text(t) => Some(t.text.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let text = join_runtime_strings(&text, "\n");
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
    None
}

pub(super) fn invoked_tool_name_to_registry_host(name: &RuntimeString) -> String {
    name.try_to_utf8()
        .expect("legacy ToolError formatter accepts only scalar registered tool names")
}

pub(super) fn find_runtime_tool(
    tools: &[Arc<dyn Tool>],
    name: &RuntimeString,
) -> Option<Arc<dyn Tool>> {
    tools
        .iter()
        .find(|tool| tool.name() == *name)
        .map(Arc::clone)
}

pub(super) fn query_path_dialect() -> NodePathDialect {
    if cfg!(windows) {
        NodePathDialect::Win32
    } else {
        NodePathDialect::Posix
    }
}

pub(super) fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut joined = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            joined = joined.concat(&separator);
        }
        joined = joined.concat(value);
    }
    joined
}

pub(super) fn concat_runtime_strings(parts: &[RuntimeString]) -> RuntimeString {
    parts
        .iter()
        .fold(RuntimeString::default(), |joined, part| joined.concat(part))
}

pub(super) fn queue_durability_engine_error(
    stage: &'static str,
    error: RuntimeString,
) -> EngineError {
    EngineError {
        code: EngineErrorCode::Internal,
        message: concat_runtime_strings(&[
            "Steering queue durability failed while ".into(),
            stage.into(),
            ": ".into(),
            error,
        ]),
        cause: None,
        request_id: None,
        provider_kind: None,
        retry_after_ms: None,
    }
}

pub(super) fn base_hook_input(
    event: HookEvent,
    cwd: &RuntimePath,
    session_id: &RuntimeString,
) -> HookInput {
    HookInput {
        event,
        cwd: cwd.as_runtime_string().clone(),
        session_id: session_id.clone(),
        tool_name: None,
        tool_input: None,
        tool_result: None,
        error: None,
        is_interrupt: None,
        prompt: None,
        message: None,
        agent_type: None,
        agent_id: None,
        trigger: None,
    }
}

#[cfg(test)]
mod repair_truncated_json_tests {
    use super::*;
    use crate::runtime_values::RuntimeJson;

    fn parse(text: &str) -> Option<RuntimeString> {
        repair_truncated_json(&RuntimeString::from(text), None)
    }

    fn value(text: &str, path: &str) -> Option<String> {
        let repaired = parse(text)?;
        let parsed = runtime_json::parse(&repaired).ok()?;
        parsed
            .get(path)
            .and_then(RuntimeJson::as_string)
            .map(|value| String::from_utf8_lossy(&value.to_node_utf8()).into_owned())
    }

    /// The shape a truncated `write` actually produces: the content string was
    /// still being written when the budget ran out. Everything the model
    /// completed survives, including the partial content itself.
    #[test]
    fn closes_a_string_cut_off_mid_value() {
        assert_eq!(
            value(r#"{"path":"a.txt","content":"line one\nline two"#, "path").as_deref(),
            Some("a.txt")
        );
        assert_eq!(
            value(
                r#"{"path":"a.txt","content":"line one\nline two"#,
                "content"
            )
            .as_deref(),
            Some("line one\nline two"),
            "the partial content is recovered, with its escapes decoded by the parser"
        );
    }

    /// Unescaping must be the parser's, not an approximation here: a recovered
    /// file cannot come back mangled.
    #[test]
    fn recovers_escaped_content_faithfully() {
        assert_eq!(
            value(r#"{"content":"quote:\" tab:\t back:\\ end"#, "content").as_deref(),
            Some("quote:\" tab:\t back:\\ end")
        );
    }

    /// Open containers are closed innermost-first.
    #[test]
    fn closes_open_containers() {
        let repaired = parse(r#"{"a":{"b":[1,2"#).expect("repairable");
        let parsed = runtime_json::parse(&repaired).expect("valid");
        assert_eq!(
            parsed
                .get("a")
                .and_then(|a| a.get("b"))
                .and_then(RuntimeJson::as_array)
                .map(|items| items.len()),
            Some(2)
        );
    }

    /// A trailing comma or colon introduces a member that never arrived. The cut
    /// falls back to the last COMPLETED value, so `"b":2` — which did arrive whole
    /// — is kept, while the dangling comma introduces nothing and adds nothing.
    #[test]
    fn drops_a_trailing_comma_or_colon() {
        assert_eq!(
            repaired_text(r#"{"a":1,"b":2,"#).as_deref(),
            Some(r#"{"a":1,"b":2}"#),
            "the completed b survives; only the value the comma was introducing is absent"
        );
        assert_eq!(
            repaired_text(r#"{"a":"x","#).as_deref(),
            Some(r#"{"a":"x"}"#)
        );
        assert_eq!(
            repaired_text(r#"{"a":"x","b":"#).as_deref(),
            Some(r#"{"a":"x"}"#),
            "a colon with no value yet is not a place to cut"
        );
    }

    /// Cut off after a backslash: closing there would make `\"` an escaped quote
    /// and leave the string open, so the cut falls back to before the backslash.
    #[test]
    fn drops_a_dangling_escape() {
        assert_eq!(
            repaired_text(r#"{"content":"abc\"#).as_deref(),
            Some(r#"{"content":"abc"}"#),
            "the dangling backslash is dropped, the text before it is kept"
        );
    }

    /// A partial escape sequence has no valid completion, so the cut falls back to
    /// before the backslash rather than emitting a half escape the parser rejects.
    #[test]
    fn a_partial_escape_falls_back_to_before_it() {
        assert_eq!(
            repaired_text(r#"{"content":"abc\u00"#).as_deref(),
            Some(r#"{"content":"abc"}"#)
        );
    }

    /// A document that already parses is left alone: repairing it could only
    /// change it.
    #[test]
    fn leaves_valid_json_alone() {
        assert_eq!(parse(r#"{"a":1}"#), None);
        assert_eq!(parse("{}"), None);
    }

    /// With nothing open there is nothing to close, so a document that still
    /// fails to parse is not guessable.
    #[test]
    fn refuses_when_nothing_is_open() {
        assert_eq!(parse("not json at all"), None);
        assert_eq!(parse(""), None);
    }

    /// The repairer is only about parseability. Whether the recovered VALUES are
    /// safe to act on is the caller's question, so a non-write tool's arguments
    /// are not salvaged by this — documented here because it is the difference
    /// between appending half a file and running half a shell command.
    #[test]
    fn recovers_any_shape_leaving_safety_to_the_caller() {
        assert!(parse(r#"{"command":"rm -rf /tmp/x"#).is_some());
    }

    /// The cut is placed at the last COMPLETE VALUE, so a truncation inside a
    /// later string keeps the earlier fields intact rather than discarding them.
    /// This is the case the write append protocol depends on.
    #[test]
    fn keeps_completed_fields_when_a_later_string_is_cut() {
        let repaired = parse(r#"{"path":"a.txt","content":"partial"#).expect("repairable");
        assert_eq!(
            repaired.try_to_utf8().ok().as_deref(),
            Some(r#"{"path":"a.txt","content":"partial"}"#)
        );
    }

    /// A completed container is itself a complete value, so cutting after it keeps
    /// the whole thing rather than dropping back to the previous field.
    #[test]
    fn a_closed_container_is_a_checkpoint() {
        assert_eq!(value(r#"{"a":{"b":1},"c":"x"#, "c").as_deref(), Some("x"));
        assert_eq!(
            repaired_text(r#"{"a":{"b":1},"c":"x"#).as_deref(),
            Some(r#"{"a":{"b":1},"c":"x"}"#)
        );
    }

    /// A cut inside a KEY is not a checkpoint — closing there would produce a key
    /// with no value — so the repair falls back to the last completed member.
    #[test]
    fn a_key_in_progress_is_not_a_checkpoint() {
        assert_eq!(
            repaired_text(r#"{"a":1,"b"#).as_deref(),
            Some(r#"{"a":1}"#),
            "the half-written key is dropped, not closed into a valueless member"
        );
    }

    /// The repaired document, as text, for asserting the exact cut point.
    fn repaired_text(input: &str) -> Option<String> {
        parse(input).map(|repaired| String::from_utf8_lossy(&repaired.to_node_utf8()).into_owned())
    }
}
