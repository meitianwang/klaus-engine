//! Regression 96e85d7c: an earlier discovery checkpoint aborted a later tool batch.
//! Root cause: readiness checked batch counts without checking checkpoint call IDs.
//! Exercise the public session facade and preserve real local execution and replay.

#[tokio::test]
async fn cursor_delayed_checkpoint_preserves_local_execution_and_signed_continuation() {
    super::r20261016_cursor_source::run_delayed_checkpoint_inventory().await;
}
