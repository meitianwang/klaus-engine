//! Regression work item 20261018: pattern-only metadata discovery stopped local turns.
//! Root cause: helper selectors required a server and completed metadata batches
//! were reset only at HTTP chunk boundaries. Exercise the named session facade,
//! whose provider adapter and integration fixtures belong to this server crate.

#[tokio::test]
async fn cursor_pattern_discovery_preserves_local_execution_and_signed_continuation() {
    super::r20261016_cursor_source::run_default_inventory(false, true).await;
}
