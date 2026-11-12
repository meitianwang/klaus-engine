//! Endpoint/model-scoped learning for native tool protocol refusals.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};

use crate::runtime_values::RuntimeString;

type LearnedNativeTools = Arc<Mutex<HashSet<(String, RuntimeString)>>>;

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
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(&(endpoint.to_owned(), model.clone()))
    }

    pub(crate) fn disable(&self, endpoint: &str, model: &RuntimeString) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert((endpoint.to_owned(), model.clone()));
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
    validation
        && ([
            "defer_loading",
            "tool_search",
            "execution",
            "additional_tools",
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
