//! Defect: upstream missing-credential hop drops and OAuth renewal verdicts on
//! Antigravity collapsed into terminal `antigravity_auth_required` failures, and
//! verification-required refusals lacked an explicit classification signal.
//! Root cause: `classify_error` only mapped `antigravity_auth_required` to `Auth`
//! without covering `antigravity_login_expired`, `antigravity_not_signed_in`,
//! bounded `antigravity_refresh_unavailable`, or `antigravity_verification_required`.
//! Work item: 20261114 Antigravity auth renewal and error classification.

use klaus_agent_core::provider::classify::{classify_error, RawProviderError};
use klaus_agent_core::provider::retry::is_retryable;
use klaus_agent_core::types::errors::ProviderErrorKind;

#[test]
fn antigravity_auth_renewal_verification_and_hop_drop_signals_classify_distinctly() {
    let hop_drop = classify_error(&RawProviderError {
        status: Some(401),
        code: Some("antigravity_service_unavailable".into()),
        error_type: Some("UNAUTHENTICATED".into()),
        message: "antigravity_service_unavailable [UNAUTHENTICATED]: Request is missing required authentication credential.".into(),
        ..Default::default()
    });
    assert_eq!(hop_drop.kind, ProviderErrorKind::Overloaded);
    assert_eq!(
        hop_drop.code.as_ref().unwrap(),
        "antigravity_service_unavailable"
    );
    assert!(is_retryable(&hop_drop));

    for code in ["antigravity_login_expired", "antigravity_not_signed_in"] {
        let terminal_auth = classify_error(&RawProviderError {
            code: Some(code.into()),
            message: code.into(),
            ..Default::default()
        });
        assert_eq!(terminal_auth.kind, ProviderErrorKind::Auth);
        assert_eq!(terminal_auth.code.as_ref().unwrap(), code);
        assert!(!is_retryable(&terminal_auth));
    }

    let refresh_blip = classify_error(&RawProviderError {
        code: Some("antigravity_refresh_unavailable".into()),
        message: "antigravity_refresh_unavailable".into(),
        ..Default::default()
    });
    assert_eq!(refresh_blip.kind, ProviderErrorKind::Timeout);
    assert_eq!(
        refresh_blip.code.as_ref().unwrap(),
        "antigravity_refresh_unavailable"
    );
    assert!(is_retryable(&refresh_blip));

    let verification = classify_error(&RawProviderError {
        status: Some(403),
        code: Some("antigravity_verification_required".into()),
        error_type: Some("PERMISSION_DENIED".into()),
        message: "antigravity_verification_required [PERMISSION_DENIED]: Verify your account to continue.".into(),
        ..Default::default()
    });
    assert_eq!(verification.kind, ProviderErrorKind::InvalidRequest);
    assert_eq!(
        verification.code.as_ref().unwrap(),
        "antigravity_verification_required"
    );
    assert!(!is_retryable(&verification));
}
