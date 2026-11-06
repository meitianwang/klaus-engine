//! Defect: establishment and stream recovery disagreed about the failed account.
//! Root cause: queue-wide recovery state and pre-stream account capture crossed requests.
//! Tracking: f4a2eb9d request-local credential recovery.

use super::rbb313dbb_claude_account_rotation::AuthPause;
use super::rbb313dbb_claude_account_rotation::{assert_success, AuthMode, Fixture, Reply};
use klaus_agent_core::types::provider::ModelStreamEvent;
use std::sync::Arc;

#[tokio::test]
async fn stream_refusal_after_establishment_rotation_advances_the_actual_account() {
    let fixture = Fixture::new(AuthMode::Multi(&["a", "b", "c"])).await;
    fixture.plan(&[
        ("a", Reply::RateLimited),
        ("b", Reply::StreamRateLimited),
        ("c", Reply::Success),
    ]);
    assert_success(&fixture.run().await);
    fixture.assert_trace(&["a", "b", "c"]);
    fixture.shutdown().await;
}

#[tokio::test]
async fn repeated_stream_refusals_reach_the_untried_healthy_account() {
    let fixture = Fixture::new(AuthMode::Multi(&["a", "b", "c", "d"])).await;
    fixture.plan(&[
        ("a", Reply::RateLimited),
        ("b", Reply::StreamRateLimited),
        ("c", Reply::StreamRateLimited),
        ("d", Reply::Success),
    ]);
    assert_success(&fixture.run().await);
    fixture.assert_trace(&["a", "b", "c", "d"]);
    fixture.shutdown().await;
}

#[tokio::test]
async fn every_stream_refusal_still_terminates_and_a_new_turn_gets_a_fresh_cycle() {
    let fixture = Fixture::new(AuthMode::Multi(&["a", "b", "c"])).await;
    fixture.plan(&[
        ("a", Reply::StreamRateLimited),
        ("b", Reply::StreamRateLimited),
        ("c", Reply::StreamRateLimited),
    ]);
    let result = fixture.run().await;
    let error = result.error.as_ref().unwrap().provider_error().unwrap();
    assert_eq!(
        error.kind,
        klaus_agent_core::types::errors::ProviderErrorKind::RateLimit
    );
    assert_eq!(
        error.status, None,
        "an SSE error has no HTTP refusal status"
    );
    fixture.plan(&[("a", Reply::RateLimited), ("b", Reply::Success)]);
    assert_success(&fixture.run().await);
    fixture.assert_trace(&["a", "b", "c", "a", "b"]);
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_exhausted_concurrent_request_cannot_stop_an_in_flight_request() {
    let pause = Arc::new(AuthPause::default());
    let fixture =
        Fixture::new_with_auth_pause(AuthMode::Multi(&["a", "b", "c"]), Some(pause.clone())).await;
    fixture.plan(&[
        ("a", Reply::RateLimited),
        ("b", Reply::RateLimited),
        ("c", Reply::RateLimited),
        ("a", Reply::RateLimited),
        ("b", Reply::RateLimited),
        ("b", Reply::Success),
    ]);
    let competing = async {
        pause.paused.notified().await;
        let result = fixture.run_standalone().await;
        pause.resume.notify_one();
        result
    };
    let (first, second) = tokio::join!(fixture.run(), competing);
    assert!(second
        .iter()
        .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
    assert_success(&first);
    fixture.assert_trace(&["a", "b", "c", "a", "b", "b"]);
    fixture.shutdown().await;
}
