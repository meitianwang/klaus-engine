//! Parse a plugin's `hooks/hooks.json` into engine [`Hook`]s.
//!
//! The file maps each lifecycle event to a list of `{ matcher?, hooks: [...] }`
//! groups. Each per-hook entry is a lenient object with a string `type`; only
//! `command` entries are mapped to an engine hook. A non-command hook
//! (`prompt`/`agent`/`http`) is SKIPPED rather than dropping the whole file — its
//! sibling `command` hooks still load. `timeout` is expressed in SECONDS and
//! converts to the engine's milliseconds. Unknown events (not in
//! [`HOOK_EVENTS`], the engine's single authority) are skipped.

use crate::hooks::types::{Hook, HookEvent, HookHandler};
use crate::runtime_values::{
    runtime_codec, runtime_json, NodeBufferUtf8, RuntimeCodecError, RuntimeCodecErrorKind,
    RuntimeDecode, RuntimeJson, RuntimeString,
};

/// A lenient per-hook entry: any object with a string `type`. Only `command` is
/// mapped to an engine hook; `prompt`/`agent`/`http` still validate (so they do
/// not fail the file) but are skipped when mapped. Extra keys are ignored.
struct HookEntry {
    type_: RuntimeString,
    command: Option<RuntimeString>,
    timeout: Option<f64>,
    is_async: Option<bool>,
}

impl RuntimeDecode for HookEntry {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                type_: object.required("type", &[])?,
                command: object.optional("command", &[])?,
                timeout: object.optional("timeout", &[])?,
                is_async: object.optional("async", &[])?,
            })
        })
    }
}

/// A matcher-keyed group of hook entries for one event.
struct HookGroup {
    matcher: Option<RuntimeString>,
    hooks: Vec<HookEntry>,
}

impl RuntimeDecode for HookGroup {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                matcher: object.optional("matcher", &[])?,
                hooks: object.required("hooks", &[])?,
            })
        })
    }
}

/// The whole hooks file: a map of event name → list of groups. Deserialized
/// through a hand-written visitor so the top-level entries keep their document
/// order (the order they map into the resulting hook list).
struct HooksFile(Vec<(RuntimeString, Vec<HookGroup>)>);

impl RuntimeDecode for HooksFile {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let RuntimeJson::Object(object) = value else {
            return Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::ExpectedObject,
            ));
        };
        let mut entries = Vec::with_capacity(object.len());
        for (key, value) in object.entries() {
            let groups = Vec::<HookGroup>::decode_runtime(value)
                .map_err(|error| error.prepend(key.clone()))?;
            entries.push((key, groups));
        }
        Ok(Self(entries))
    }
}

/// Resolve an event name to a known [`HookEvent`], or `None` when it is not one
/// of the engine's dispatched events.
fn hook_event_from_runtime(name: &RuntimeString) -> Option<HookEvent> {
    HookEvent::decode_runtime(&RuntimeJson::String(name.clone())).ok()
}

fn node_utf8_hook_handler_boundary(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8())
        .expect("Node UTF-8 hook handler projection is valid UTF-8")
}

/// Parse already-read hooks.json text into engine hooks (unknown events +
/// non-command hooks skipped). Invalid JSON or a malformed shape yields `[]`.
pub fn parse_hooks_json(text: &str) -> Vec<Hook> {
    parse_hooks_runtime_json(&RuntimeString::from(text))
}

pub(crate) fn parse_hooks_runtime_json(text: &RuntimeString) -> Vec<Hook> {
    let value = match runtime_json::parse(text) {
        Ok(value) => value,
        Err(_) => return Vec::new(),
    };
    let file = match HooksFile::decode_runtime(&value) {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };

    let mut result: Vec<Hook> = Vec::new();
    for (event_name, groups) in file.0 {
        let event = match hook_event_from_runtime(&event_name) {
            Some(e) => e,
            None => continue,
        };
        for group in &groups {
            for h in &group.hooks {
                // The runtime executes command handlers only; prompt/agent/http are
                // skipped (their presence does not invalidate the command hooks in
                // the same group).
                if h.type_ != "command" {
                    continue;
                }
                let command = match &h.command {
                    Some(command) => node_utf8_hook_handler_boundary(command),
                    None => continue,
                };
                result.push(Hook {
                    event,
                    matcher: group.matcher.as_ref().map(node_utf8_hook_handler_boundary),
                    handler: HookHandler::Command {
                        command,
                        timeout_ms: h.timeout.map(|t| (t * 1000.0) as u64),
                    },
                    is_async: h.is_async == Some(true),
                });
            }
        }
    }
    result
}

/// Read + parse a `hooks/hooks.json` file. A missing/invalid file yields `[]`.
pub async fn load_hooks_json(path: &str) -> Vec<Hook> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(_) => return Vec::new(),
    };
    parse_hooks_runtime_json(&NodeBufferUtf8::decode(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_timeout_seconds_to_ms_carries_matcher_and_async_defaults_the_rest() {
        let hooks = parse_hooks_json(
            r#"{"PostToolUse":[{"matcher":"Edit","hooks":[{"type":"command","command":"fmt","timeout":5,"async":true}]},{"hooks":[{"type":"command","command":"lint"}]}]}"#,
        );
        assert_eq!(hooks.len(), 2);

        let h0 = &hooks[0];
        assert_eq!(h0.event, HookEvent::PostToolUse);
        assert_eq!(h0.matcher.as_deref(), Some("Edit"));
        assert!(h0.is_async);
        match &h0.handler {
            HookHandler::Command {
                command,
                timeout_ms,
            } => {
                assert_eq!(command, "fmt");
                assert_eq!(*timeout_ms, Some(5000));
            }
            _ => panic!("expected command handler"),
        }

        let h1 = &hooks[1];
        assert_eq!(h1.event, HookEvent::PostToolUse);
        assert_eq!(h1.matcher, None);
        assert!(!h1.is_async);
        match &h1.handler {
            HookHandler::Command {
                command,
                timeout_ms,
            } => {
                assert_eq!(command, "lint");
                assert_eq!(*timeout_ms, None);
            }
            _ => panic!("expected command handler"),
        }
    }

    #[test]
    fn skips_unknown_events_but_keeps_known_ones_from_the_same_file() {
        let hooks = parse_hooks_json(
            r#"{"NotARealEvent":[{"hooks":[{"type":"command","command":"x"}]}],"SessionStart":[{"hooks":[{"type":"command","command":"y"}]}]}"#,
        );
        let events: Vec<HookEvent> = hooks.iter().map(|h| h.event).collect();
        assert_eq!(events, vec![HookEvent::SessionStart]);
    }

    #[test]
    fn returns_empty_for_invalid_json_or_a_malformed_shape() {
        assert!(parse_hooks_json("{not json").is_empty());
        assert!(parse_hooks_json(r#"{"PreToolUse":"nope"}"#).is_empty());
    }
}
