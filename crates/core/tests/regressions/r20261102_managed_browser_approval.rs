//! Defect: browser selection and tab claiming failed after full access was narrowed.
//! Root cause: model-required review replaced the sandbox and reviewer but retained never.
//! Work item: 20261102 managed browser approval admission.

#![cfg(unix)]

use super::r20261018_automatic_approval_review::managed_browser_outcome;
use klaus_agent_core::types::approval_review::ApprovalReviewStatus;
use klaus_agent_core::types::permission::{ApprovalPolicy, ApprovalsReviewer, SandboxMode};

#[tokio::test]
async fn narrowed_full_access_reviews_browser_selection_and_tab_claiming() {
    for tool in ["browser_select_browser", "browser_claim_tab"] {
        let outcome = managed_browser_outcome(tool, true, false).await;
        assert_eq!(
            outcome.executions, 1,
            "{tool} must reach the browser runtime"
        );
        assert!(!outcome.tool_error, "{tool} must finish successfully");
        assert_eq!(outcome.human_prompts, 0);
        assert_eq!(outcome.reviews, 1);
        assert_eq!(outcome.status, Some(ApprovalReviewStatus::Approved));
        assert_eq!(
            outcome.policy.approval_policy,
            Some(ApprovalPolicy::OnRequest)
        );
        assert_eq!(
            outcome.policy.sandbox_mode,
            Some(SandboxMode::WorkspaceWrite)
        );
        assert_eq!(
            outcome.policy.approvals_reviewer,
            Some(ApprovalsReviewer::Automatic)
        );
        assert!(!outcome.policy.network_allowed);
    }
}

#[tokio::test]
async fn narrowed_full_access_preserves_a_managed_browser_denial() {
    let outcome = managed_browser_outcome("browser_claim_tab", true, true).await;
    assert_eq!(
        outcome.reviews, 1,
        "required review must run before rejecting the action"
    );
    assert_eq!(outcome.status, Some(ApprovalReviewStatus::Denied));
    assert_eq!(outcome.executions, 0);
    assert_eq!(outcome.human_prompts, 0);
    assert!(outcome.tool_error);
}

#[tokio::test]
async fn explicitly_sandboxed_never_still_rejects_browser_mutations() {
    let outcome = managed_browser_outcome("browser_claim_tab", false, false).await;
    assert_eq!(outcome.policy.approval_policy, Some(ApprovalPolicy::Never));
    assert_eq!(
        outcome.policy.sandbox_mode,
        Some(SandboxMode::WorkspaceWrite)
    );
    assert_eq!(outcome.executions, 0);
    assert_eq!(outcome.human_prompts, 0);
    assert_eq!(outcome.reviews, 0);
    assert!(outcome.tool_error);
}
