use super::{
    ClassifierError, ClassifierScore, ModelReviewPolicy, ReviewMode, ReviewScope, ScoreProgress,
    ToolObservation, UnscoredAction,
};
use std::time::SystemTime;

pub struct ObservationInput<'a> {
    pub action: ToolObservation<'a>,
    pub default_namespace: bool,
    pub tool_name: &'a str,
    /// Resolved from host-owned registration metadata, including connector identity.
    pub scope: Option<ReviewScope>,
    pub explicitly_escalated: bool,
    pub with_additional_permissions: bool,
    /// True only for the registered `js` computer-use execution tool.
    pub computer_use_js_execution: bool,
    pub full_access: bool,
    pub user_reviewer: bool,
    pub synchronous_review_required_for_model: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationDisposition {
    Ignored,
    Unscored { index: usize },
    Classify { index: usize },
}

pub fn observe_action<A>(
    progress: &ScoreProgress<A>,
    policy: &ModelReviewPolicy,
    input: ObservationInput<'_>,
) -> ObservationDisposition {
    if input.default_namespace && input.tool_name == "wait" {
        return ObservationDisposition::Ignored;
    }
    if input.scope.is_none()
        && input.default_namespace
        && input.tool_name == "exec"
        && input.action.is_code_wrapper
        && input.action.origin == super::ToolOrigin::Direct
        && policy.other_tools == ReviewMode::Disabled
    {
        return ObservationDisposition::Ignored;
    }
    if !policy.scores_tool(
        input.scope,
        input.default_namespace,
        input.tool_name,
        input.explicitly_escalated,
    ) {
        return match policy.unscored_action {
            UnscoredAction::Ignore => ObservationDisposition::Ignored,
            UnscoredAction::AgeScore => {
                let index = progress.observe(input.action);
                if input.default_namespace
                    && input.tool_name == "exec_command"
                    && input.with_additional_permissions
                {
                    progress.invalidate(index);
                }
                ObservationDisposition::Unscored { index }
            }
            UnscoredAction::InvalidateScore => {
                let index = progress.observe(input.action);
                progress.invalidate(index);
                ObservationDisposition::Unscored { index }
            }
        };
    }
    if input.computer_use_js_execution {
        progress.observe_js_execution();
    }
    let index = progress.observe(input.action);
    if input.full_access || input.user_reviewer {
        progress.invalidate(index);
        return ObservationDisposition::Unscored { index };
    }
    if input.synchronous_review_required_for_model
        && !(input.scope == Some(ReviewScope::ComputerUse) && policy.allows_initial_computer_use())
    {
        progress.clear_score();
        return ObservationDisposition::Unscored { index };
    }
    ObservationDisposition::Classify { index }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassificationPublication {
    Published,
    Superseded,
    Failed,
}

/// Classification is binary evidence. Only the blocking reviewer chooses an
/// allow/deny verdict; an elevated score requests a fresh blocking assessment.
pub fn publish_classification<A>(
    progress: &ScoreProgress<A>,
    output: Result<&str, &ClassifierError>,
    authorization: A,
    sampled_at: SystemTime,
    index: usize,
    authorization_is_current: bool,
    owning_generation_is_current: bool,
) -> ClassificationPublication {
    let risk = match output {
        Ok("low") => 0.0,
        Ok("high") => 1.0,
        Err(ClassifierError::Superseded) => return ClassificationPublication::Superseded,
        _ => {
            progress.fail_closed(sampled_at);
            return ClassificationPublication::Failed;
        }
    };
    if !authorization_is_current || !owning_generation_is_current {
        return ClassificationPublication::Superseded;
    }
    if progress.publish(
        ClassifierScore {
            action_risk: risk,
            sampled_at,
        },
        authorization,
        index,
    ) {
        ClassificationPublication::Published
    } else {
        ClassificationPublication::Superseded
    }
}
