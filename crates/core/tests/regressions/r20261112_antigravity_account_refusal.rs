//! Defect: account service restrictions were presented as expired sign-in.
//! Root cause: unrecognized source verdicts fell back to the HTTP 403 auth kind.
//! Work item: 20261112 Antigravity account refusal reporting.

use klaus_agent_core::provider::classify::{classify_error, RawProviderError};
use klaus_agent_core::provider::retry::is_retryable;
use klaus_agent_core::types::errors::ProviderErrorKind;

#[test]
fn account_refusals_remain_terminal_without_becoming_auth_or_network_failures() {
    for code in [
        "antigravity_account_disabled",
        "antigravity_account_restricted",
        "antigravity_permission_denied",
    ] {
        let error = classify_error(&RawProviderError {
            status: Some(403),
            code: Some(code.into()),
            message: format!("{code}: account service refusal"),
            ..Default::default()
        });
        assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(error.code.as_ref().unwrap(), code);
        assert!(!is_retryable(&error));
    }
}
