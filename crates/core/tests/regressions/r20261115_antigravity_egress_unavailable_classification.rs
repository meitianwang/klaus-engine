//! Defect: missing or unlaunchable local Antigravity egress relay lacked a
//! dedicated terminal error classification when direct fallback was disabled.
//! Root cause: `classify_error` did not map `antigravity_egress_unavailable` to
//! a non-retryable `InvalidRequest` verdict.
//! Work item: 20261115 Antigravity egress fail-closed classification.

use klaus_agent_core::provider::classify::{classify_error, RawProviderError};
use klaus_agent_core::provider::retry::is_retryable;
use klaus_agent_core::types::errors::ProviderErrorKind;

#[test]
fn antigravity_egress_unavailable_classifies_as_terminal_invalid_request() {
    let classified = classify_error(&RawProviderError {
        code: Some("antigravity_egress_unavailable".into()),
        message: "antigravity_egress_unavailable".into(),
        ..Default::default()
    });
    assert_eq!(classified.kind, ProviderErrorKind::InvalidRequest);
    assert_eq!(
        classified.code.as_ref().unwrap(),
        "antigravity_egress_unavailable"
    );
    assert!(!is_retryable(&classified));
}
