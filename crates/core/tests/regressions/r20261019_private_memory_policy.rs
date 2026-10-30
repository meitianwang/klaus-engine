//! Defect: private memory writes inherited an unrelated interactive read-only policy.
//! Root cause: maintenance lost its pinned filesystem capability at context creation.
//! Work item: 20261019 private maintenance policy and independent parent authority.

#[tokio::test]
async fn readonly_parent_allows_private_memory_and_denies_unrelated_writes_without_prompts() {
    super::r8399e9e_memory_fork_approval::assert_readonly_parent_keeps_private_memory_writes_scoped().await;
}
