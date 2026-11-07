//! Defect: child stream progress was indistinguishable from actual parent notifications.
//! Root cause: successful queue/mailbox delivery had no separate receiver-owned event.
//! Fixed by: 2026-09-06 agent delivery receipt work item.

#[tokio::test]
async fn actual_background_notification_arrives_once_after_child_streaming() {
    super::r20260906_subagent_live_stream::assert_live_child(true, false).await;
}

#[tokio::test]
async fn actual_child_mailbox_notification_is_live_and_durable_without_parent_notice() {
    super::r20260906_subagent_live_stream::assert_live_child_with_mailbox(false, false, true).await;
}
