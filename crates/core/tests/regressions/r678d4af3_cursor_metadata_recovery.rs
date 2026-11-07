//! Regression r678d4af3: invalid discovery selectors terminated otherwise usable
//! sessions. Resample only rejected query arguments, never execution ownership
//! or certified history, within the session's existing retry budget.

#[tokio::test]
async fn mixed_metadata_query_discards_held_calls_then_executes_each_tool_once() {
    super::r20261016_cursor_source::run_metadata_tool_response().await;
}

#[tokio::test]
async fn native_metadata_query_recovers_without_exposing_uncertified_calls() {
    super::r20261016_cursor_source::run_metadata_response(false, false).await;
}

#[tokio::test]
async fn helper_metadata_query_recovers_without_exposing_uncertified_calls() {
    super::r20261016_cursor_source::run_metadata_response(true, false).await;
}

#[tokio::test]
async fn persistent_native_metadata_query_stops_at_the_retry_budget() {
    super::r20261016_cursor_source::run_metadata_response(false, true).await;
}

#[tokio::test]
async fn persistent_helper_metadata_query_stops_at_the_retry_budget() {
    super::r20261016_cursor_source::run_metadata_response(true, true).await;
}
