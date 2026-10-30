//! Defect: automatic memory maintenance appeared as failed general-purpose sub-agents.
//! Root cause: hidden memory forks skipped live observation but still entered the public lifecycle.
//! Fixed for the 2026-09-16 internal-memory lifecycle work item.

#[tokio::test]
async fn automatic_memory_maintenance_stays_out_of_the_public_subagent_lifecycle() {
    super::r8399e9e_memory_fork_approval::assert_internal_memory_maintenance_is_not_a_user_subagent()
        .await;
}
