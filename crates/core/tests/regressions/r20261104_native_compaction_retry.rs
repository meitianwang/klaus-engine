//! Defect: one DNS failure during native compaction ended the whole turn.
//! Root cause: native checkpoint requests bypassed the compactor's retry budget.
//! Work item 20261104: reconnect native compaction with the existing retry policy.

use super::r5dea2d9b_compaction_migration_contract::{spec, succeeded, Fixture, Spec};

#[tokio::test]
async fn transient_native_compaction_failures_retry_before_normal_inference() {
    let fixture = Fixture::new(
        Spec {
            remote: true,
            native_network_failures: 2,
            max_retries: 5,
            ..spec("old", "same-model", Some("v1"))
        },
        false,
    );
    fixture.select(Spec {
        max_retries: 5,
        ..spec("new", "same-model", Some("v2"))
    });

    let events = fixture.run().await;

    assert!(succeeded(&events), "{events:?}");
    assert_eq!(
        fixture.routes(),
        [
            ("old".into(), "native"),
            ("old".into(), "native"),
            ("old".into(), "native"),
            ("new".into(), "normal")
        ]
    );
    fixture.close().await;
}
