//! Regression 91f6a179: a malformed native response ended an otherwise valid turn.
//! Root cause: response shape failures were classified as invalid caller requests.
//! Exercise bounded resampling through the public session facade; retain strict
//! certification and ensure invalid responses never publish executable calls.

#[tokio::test]
async fn cursor_malformed_response_recovers_without_executing_uncertified_calls() {
    super::r20261016_cursor_source::run_malformed_response(false).await;
}

#[tokio::test]
async fn cursor_persistent_malformed_response_stops_at_the_retry_limit() {
    super::r20261016_cursor_source::run_malformed_response(true).await;
}

#[tokio::test]
async fn cursor_malformed_tool_certificate_recovers_then_executes_each_call_once() {
    super::r20261016_cursor_source::run_malformed_tool_response().await;
}
