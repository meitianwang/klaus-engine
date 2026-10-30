//! One standardized fact: an extension point did not run.
//!
//! Hooks, plugins and skills fail in three unrelated places — a hook command
//! exits non-zero, a plugin manifest fails to load, a skill is skipped — but
//! they produce the same fact for the user: something they installed is not
//! working, and nothing in the conversation will say so. Three separate event
//! shapes would mean three consumers doing the same triage, so this is one
//! event with a `kind`.
//!
//! Deliberately NOT folded into the existing runtime snapshots
//! (`PluginRuntimeSnapshot` load diagnostics, `McpRuntimeResult` states): those
//! are polled replies that describe the current graph. A failure is a moment,
//! and a surface that only polls cannot tell "still broken" from "just broke".

use std::sync::Arc;

use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

/// Which extension point failed. The kind decides how a host offers to fix it
/// (re-run vs reinstall vs reconfigure), so it is part of the wire shape rather
/// than something a consumer parses back out of the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionFailureKind {
    /// A configured hook command failed or could not be spawned.
    Hook,
    /// A plugin could not be loaded from its manifest.
    Plugin,
    /// A skill was skipped or could not be read.
    Skill,
}

impl ExtensionFailureKind {
    const WIRE: [&'static str; 3] = ["hook", "plugin", "skill"];

    fn wire(self) -> &'static str {
        match self {
            Self::Hook => Self::WIRE[0],
            Self::Plugin => Self::WIRE[1],
            Self::Skill => Self::WIRE[2],
        }
    }
}

/// A single failure, identified well enough that a host can list it, dedupe it
/// and offer the right repair.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtensionFailure {
    pub kind: ExtensionFailureKind,
    /// What failed: the hook's event name, the plugin name, the skill name.
    /// Never empty — an unidentifiable failure cannot be listed or deduped, so
    /// the emitter substitutes the kind rather than sending a blank row.
    pub id: RuntimeString,
    /// Why, in the emitter's own words. Not localized: it carries a command's
    /// stderr or a loader's reason, which the host shows verbatim.
    pub message: RuntimeString,
    /// Where the failing definition lives, when the emitter knows (`global` /
    /// `project`). Absent rather than guessed.
    pub scope: Option<RuntimeString>,
}

impl ExtensionFailure {
    pub fn new(
        kind: ExtensionFailureKind,
        id: impl Into<RuntimeString>,
        message: impl Into<RuntimeString>,
    ) -> Self {
        let id: RuntimeString = id.into();
        let id = if id.is_empty() {
            RuntimeString::from(kind.wire())
        } else {
            id
        };
        Self {
            kind,
            id,
            message: message.into(),
            scope: None,
        }
    }

    pub fn with_scope(mut self, scope: impl Into<RuntimeString>) -> Self {
        let scope: RuntimeString = scope.into();
        if !scope.is_empty() {
            self.scope = Some(scope);
        }
        self
    }
}

/// Host-provided delivery. The engine never decides what a failure looks like on
/// screen; it only reports that one happened.
pub type ExtensionFailureSink = Arc<dyn Fn(ExtensionFailure) + Send + Sync + 'static>;

impl RuntimeEncode for ExtensionFailure {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("kind", &RuntimeString::from(self.kind.wire()))?;
        object.field("id", &self.id)?;
        object.field("message", &self.message)?;
        // Omitted when unknown, so a consumer can tell "no scope reported" from
        // "reported as empty".
        if let Some(scope) = &self.scope {
            object.field("scope", scope)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ExtensionFailure {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let kind: RuntimeString = object.required("kind", &[])?;
            let kind = match kind.try_to_utf8().unwrap_or_default().as_str() {
                "hook" => ExtensionFailureKind::Hook,
                "plugin" => ExtensionFailureKind::Plugin,
                "skill" => ExtensionFailureKind::Skill,
                // An unknown kind is rejected rather than bucketed into one of
                // the three: a host routes its repair action off this field, so
                // guessing would offer the wrong fix.
                _ => {
                    return Err(RuntimeCodecError::new(
                        RuntimeCodecErrorKind::InvalidEnumShape,
                    ))
                }
            };
            Ok(Self {
                kind,
                id: object.required("id", &[])?,
                message: object.required("message", &[])?,
                scope: object.optional("scope", &[])?,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_round_trip_and_never_ship_an_unidentifiable_row() {
        let failure = ExtensionFailure::new(ExtensionFailureKind::Hook, "PreToolUse", "exit 1")
            .with_scope("project");
        let encoded = runtime_codec::encode_value(&failure).expect("encodes");
        let decoded = ExtensionFailure::decode_runtime(&encoded).expect("decodes");
        assert_eq!(decoded, failure);

        // An empty id would produce a row a host cannot list or dedupe, so the
        // kind stands in for it.
        let blank = ExtensionFailure::new(ExtensionFailureKind::Plugin, "", "manifest unreadable");
        assert_eq!(blank.id.try_to_utf8().unwrap(), "plugin");

        // Scope is absent rather than empty when the emitter does not know it.
        assert!(blank.scope.is_none());
        let empty_scope =
            ExtensionFailure::new(ExtensionFailureKind::Skill, "s", "m").with_scope("");
        assert!(empty_scope.scope.is_none(), "an empty scope is not a scope");
    }

    #[test]
    fn an_unknown_kind_is_rejected_rather_than_silently_bucketed() {
        let raw = runtime_json_object(&[("kind", "whatever"), ("id", "x"), ("message", "m")]);
        assert!(ExtensionFailure::decode_runtime(&raw).is_err());
    }

    fn runtime_json_object(fields: &[(&str, &str)]) -> RuntimeJson {
        let mut object = crate::runtime_values::RuntimeObject::new();
        for (key, value) in fields {
            object.insert(*key, RuntimeJson::String(RuntimeString::from(*value)));
        }
        RuntimeJson::Object(object)
    }
}
