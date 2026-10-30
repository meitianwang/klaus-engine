//! Defect: a completed built-in Explore could not reuse its search context.
//! Root cause: exploration shared the terminal planning role's one-shot policy.
//! Work item: 20261019 bounded explicit exploration follow-ups.

#[tokio::test]
async fn completed_explore_accepts_task_resume_with_its_original_identity_and_role() {
    super::r20261010_subagent_message_delivery::completed_child_resumes(Some("Explore")).await;
}
