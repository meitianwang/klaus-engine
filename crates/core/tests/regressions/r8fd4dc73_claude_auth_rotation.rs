//! Defect: a revoked subscription account stopped requests despite spare accounts.
//! Root cause: credential rotation excluded authentication failures after renewal.
//! Fixing commit: 8fd4dc73.

use super::rbb313dbb_claude_account_rotation::{assert_success, AuthMode, Fixture, Reply};
use klaus_agent_core::types::errors::TerminalReason;

#[tokio::test]
async fn revoked_account_yields_to_the_next_subscription_account() {
    let fixture = Fixture::new(AuthMode::Multi(&["a", "b"])).await;
    fixture.plan(&[("a", Reply::Unauthorized), ("b", Reply::Success)]);
    assert_success(&fixture.run().await);
    fixture.assert_trace(&["a", "b"]);
    fixture.assert_order(&["b", "a"]);
    fixture.shutdown().await;
}

#[tokio::test]
async fn renewal_failure_before_inference_does_not_block_a_healthy_account() {
    let fixture = Fixture::new(AuthMode::MultiExpired(&["a", "b"])).await;
    fixture.plan(&[("b", Reply::Success)]);
    assert_success(&fixture.run().await);
    fixture.assert_trace(&["b"]);
    fixture.assert_order(&["b", "a"]);
    assert_eq!(
        fixture.refresh_requests(),
        2,
        "initial and forced renewal are bounded"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn authentication_and_quota_refusals_share_one_bounded_rotation_cycle() {
    let fixture = Fixture::new(AuthMode::Multi(&["a", "b", "c"])).await;
    fixture.plan(&[
        ("a", Reply::Unauthorized),
        ("b", Reply::RateLimited),
        ("c", Reply::Success),
    ]);
    assert_success(&fixture.run().await);
    fixture.assert_trace(&["a", "b", "c"]);
    fixture.assert_order(&["c", "a", "b"]);
    fixture.shutdown().await;
}

#[tokio::test]
async fn all_revoked_accounts_stop_with_a_localizable_auth_failure_and_can_retry() {
    let fixture = Fixture::new(AuthMode::Multi(&["a", "b"])).await;
    fixture.plan(&[
        ("a", Reply::Unauthorized),
        ("b", Reply::Unauthorized),
        ("a", Reply::Unauthorized),
    ]);
    let result = fixture.run().await;
    assert_eq!(result.stop_reason, TerminalReason::AuthFailed);
    assert!(result.final_text.is_empty());
    fixture.assert_trace(&["a", "b", "a"]);
    fixture.assert_order(&["a", "b"]);
    assert_eq!(fixture.auth_failure_kinds(), ["auth"]);
    fixture.plan(&[("a", Reply::Unauthorized), ("b", Reply::Success)]);
    assert_success(&fixture.run().await);
    fixture.assert_trace(&["a", "b", "a", "a", "b"]);
    fixture.assert_order(&["b", "a"]);
    fixture.shutdown().await;
}

#[tokio::test]
async fn one_revoked_account_still_returns_an_authentication_failure() {
    let fixture = Fixture::new(AuthMode::SingleOAuth).await;
    fixture.plan(&[("a", Reply::Unauthorized)]);
    assert_eq!(fixture.run().await.stop_reason, TerminalReason::AuthFailed);
    fixture.assert_trace(&["a"]);
    assert_eq!(fixture.auth_failure_kinds(), ["auth"]);
    fixture.shutdown().await;
}
