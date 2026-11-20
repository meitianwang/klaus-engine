//! Endpoint/model-scoped learning for native tool protocol refusals.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};

use crate::runtime_values::RuntimeString;

type LearnedNativeTools = Arc<Mutex<HashSet<(String, RuntimeString, String)>>>;

#[derive(Clone)]
pub(crate) struct NativeToolSupport(LearnedNativeTools);

impl Default for NativeToolSupport {
    fn default() -> Self {
        static SHARED: OnceLock<LearnedNativeTools> = OnceLock::new();
        Self(
            SHARED
                .get_or_init(|| Arc::new(Mutex::new(HashSet::new())))
                .clone(),
        )
    }
}

impl NativeToolSupport {
    pub(crate) fn disabled(&self, endpoint: &str, model: &RuntimeString) -> bool {
        self.disabled_for(endpoint, model, "")
    }

    pub(crate) fn disable(&self, endpoint: &str, model: &RuntimeString) {
        self.disable_for(endpoint, model, "")
    }

    /// Capability-scoped learning: refusing one native tool protocol (say
    /// deferred loading) must not disable an unrelated one (say inline tool
    /// changes) on the same endpoint and model. The empty capability is the
    /// legacy scope and keeps its exact pre-existing semantics.
    pub(crate) fn disabled_for(
        &self,
        endpoint: &str,
        model: &RuntimeString,
        capability: &str,
    ) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(&(endpoint.to_owned(), model.clone(), capability.to_owned()))
    }

    pub(crate) fn disable_for(&self, endpoint: &str, model: &RuntimeString, capability: &str) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert((endpoint.to_owned(), model.clone(), capability.to_owned()));
    }
}

/// Only schema/feature refusals can trigger this local one-shot downgrade.
/// Authentication, quota, context limits and transient failures retain their
/// existing recovery paths. Callers must also prove native fields were sent.
pub(crate) fn unsupported_native_tools(status: Option<u16>, message: &str) -> bool {
    if !matches!(status, Some(400 | 422)) {
        return false;
    }
    let message = message.to_ascii_lowercase();
    let validation = [
        "not permitted",
        "not allowed",
        "unsupported",
        "not supported",
        "unknown",
        "unrecognized",
        "invalid",
        "extra inputs",
        "extra_forbidden",
    ]
    .iter()
    .any(|needle| message.contains(needle));
    let system_role_refusal = message.contains("system")
        && (message.contains("role")
            || message.contains("messages")
            || message.contains("first")
            || message.contains("middle"));
    validation
        && (system_role_refusal
            || [
                "defer_loading",
                "tool_search",
                "tool_reference",
                "execution",
                "additional_tools",
                "tool_addition",
                "tool_removal",
                "tool_definition",
                "namespace",
            ]
            .iter()
            .any(|needle| message.contains(needle))
            || message.contains("extra inputs are not permitted"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn learning_is_scoped_to_endpoint_and_exact_model() {
        let learned = NativeToolSupport::default();
        learned.disable("https://gateway.example/v1/responses", &"model-a".into());
        assert!(NativeToolSupport::default()
            .disabled("https://gateway.example/v1/responses", &"model-a".into()));
        assert!(learned.disabled("https://gateway.example/v1/responses", &"model-a".into()));
        assert!(!learned.disabled("https://gateway.example/v1/responses", &"model-b".into()));
        assert!(!learned.disabled("https://other.example/v1/responses", &"model-a".into()));
    }

    #[test]
    fn capability_scopes_learn_independently() {
        let learned = NativeToolSupport::default();
        learned.disable_for(
            "https://gateway.example/cap-scope",
            &"model-c".into(),
            "inline-tools",
        );
        assert!(learned.disabled_for(
            "https://gateway.example/cap-scope",
            &"model-c".into(),
            "inline-tools"
        ));
        // A refusal of one protocol leaves the legacy scope and other
        // capabilities on the same endpoint/model untouched.
        assert!(!learned.disabled("https://gateway.example/cap-scope", &"model-c".into()));
        assert!(!learned.disabled_for(
            "https://gateway.example/cap-scope",
            &"model-c".into(),
            "deferred-tools"
        ));
        learned.disable("https://gateway.example/cap-legacy", &"model-c".into());
        assert!(learned.disabled("https://gateway.example/cap-legacy", &"model-c".into()));
        assert!(!learned.disabled_for(
            "https://gateway.example/cap-legacy",
            &"model-c".into(),
            "inline-tools"
        ));
    }

    #[test]
    fn inline_tool_block_refusals_downgrade() {
        assert!(unsupported_native_tools(
            Some(400),
            "unknown block type: tool_addition"
        ));
        assert!(unsupported_native_tools(
            Some(422),
            "tool_removal is not permitted in messages"
        ));
        assert!(unsupported_native_tools(
            Some(400),
            "additional_tools is not supported for this model"
        ));
        assert!(unsupported_native_tools(
            Some(400),
            "tools.0.type: 'namespace' is not supported"
        ));
        assert!(unsupported_native_tools(
            Some(400),
            "Invalid value: 'namespace'. Supported values are: 'function' and 'custom'."
        ));
        // Same needles without a validation phrase stay ordinary errors.
        assert!(!unsupported_native_tools(
            Some(400),
            "tool_addition quota exceeded"
        ));
    }

    #[test]
    fn only_native_schema_refusals_downgrade() {
        assert!(unsupported_native_tools(
            Some(422),
            "Extra Inputs are not permitted"
        ));
        assert!(unsupported_native_tools(
            Some(400),
            "Unsupported parameter: tools[0].defer_loading"
        ));
        assert!(unsupported_native_tools(
            Some(400),
            "tool_search is not supported for this model"
        ));
        assert!(!unsupported_native_tools(
            Some(401),
            "tool_search is not permitted"
        ));
        assert!(!unsupported_native_tools(
            Some(429),
            "tool_search quota exceeded"
        ));
        assert!(!unsupported_native_tools(
            Some(400),
            "invalid image payload"
        ));
        assert!(!unsupported_native_tools(
            Some(500),
            "Extra Inputs are not permitted"
        ));
    }
}
