//! Request-local instruction authority and live permission facts.

use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, TextBlock};
use crate::schemas::message::{Message, UserMessage};
use crate::types::permission::SandboxMode;
use crate::types::provider::SystemPromptPart;
use crate::types::sandbox::SandboxKind;
use crate::types::tool::ToolContext;

/// Synthetic workspace policy belongs to the request view, never the transcript.
pub(crate) fn is_workspace_instruction_projection(message: &Message) -> bool {
    matches!(message, Message::User(user) if user.is_meta == Some(true)
        && user.uuid.starts_with("request-workspace-instructions-"))
}

/// Keep workspace instructions at user authority without modifying stored history
/// or the cache identity of the remaining stable system fragments. The cached
/// `memory` slot is the instruction-file loader; runtime agent memory uses an
/// uncached slot with the same name and must remain a separate policy fragment.
pub fn project_workspace_instructions(
    parts: &[SystemPromptPart],
    history: &[Message],
) -> (Vec<SystemPromptPart>, Vec<Message>) {
    let mut system = Vec::with_capacity(parts.len());
    let mut messages = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        if part.name == "memory" && part.cacheable == Some(true) {
            if !part.text.is_empty() {
                messages.push(Message::User(UserMessage {
                    question_answer: None,
                    uuid: format!("request-workspace-instructions-{index}").into(),
                    content: vec![ContentBlock::Text(TextBlock {
                        text: part.text.clone(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })],
                    is_meta: Some(true),
                }));
            }
        } else {
            system.push(part.clone());
        }
    }
    // Also repair request views built from histories saved before projections
    // were kept ephemeral. Current sources replace every stale injected copy.
    messages.extend(
        history
            .iter()
            .filter(|message| !is_workspace_instruction_projection(message))
            .cloned(),
    );
    (system, messages)
}

fn append_paths(
    text: &mut RuntimeString,
    label: &str,
    paths: &[crate::runtime_values::RuntimePath],
) {
    if paths.is_empty() {
        return;
    }
    *text = text.concat(&RuntimeString::from(label));
    for path in paths {
        *text = text
            .concat(&RuntimeString::from("\n- "))
            .concat(path.as_runtime_string());
    }
}

/// Describe the actual tool context captured for this request. Backend identity
/// comes from the live sandbox, never from an unresolved backend preference.
pub fn permission_instructions(
    context: &ToolContext,
    approval_handler_available: bool,
) -> SystemPromptPart {
    let sandbox_mode = context.sandbox_mode.unwrap_or(SandboxMode::ReadOnly);
    let sandbox_name = match sandbox_mode {
        SandboxMode::ReadOnly => "read-only",
        SandboxMode::WorkspaceWrite => "workspace-write",
        SandboxMode::DangerFullAccess => "danger-full-access",
    };
    let backend_kind = context.sandbox.kind();
    let backend = match backend_kind {
        SandboxKind::Process => "process",
        SandboxKind::Seatbelt => "seatbelt",
        SandboxKind::Bubblewrap => "bubblewrap",
        SandboxKind::NativeHelper => "native-helper",
        SandboxKind::WindowsRestrictedToken => "windows-restricted-token",
        SandboxKind::WindowsMxc => "windows-mxc",
    };
    let can_approve = approval_handler_available
        && context.approval_policy != Some(crate::types::permission::ApprovalPolicy::Never);
    let baseline = if context.planning_read_only {
        "Planning is active. Research and read-only actions are allowed; do not implement changes. The bound plan document and engine-owned planning state follow their declared tool contracts."
    } else {
        match sandbox_mode {
            SandboxMode::ReadOnly => "The filesystem sandbox is read-only. Shell research may run inside it; writes require an eligible explicit escalation approval. The network policy is independent.",
            SandboxMode::WorkspaceWrite => "Shell commands and ordinary edits may run within the workspace and declared writable roots. Commands that exceed that filesystem or network boundary require escalation approval. Explicit rules and security checks remain active.",
            SandboxMode::DangerFullAccess => "Filesystem access is unrestricted. File targets within this policy do not need path-based approval. Explicit deny and ask rules, security checks, task scope, and the user's requested scope remain active.",
        }
    };
    let network = if context.network_allowed.unwrap_or(false) {
        "allowed by the shell policy"
    } else {
        "denied by the shell policy"
    };
    let mut text = RuntimeString::from(format!(
        "# Current permissions\n\nFilesystem policy: {sandbox_name}. {baseline}\n\nActive shell sandbox backend: {backend}. Shell network access is {network}. These facts describe shell execution; other tools retain their own network and permission checks."
    ));
    if let Some(policy) = context.approval_policy {
        text = text.concat(&RuntimeString::from(match policy {
            crate::types::permission::ApprovalPolicy::UnlessTrusted => " Approval policy: untrusted. Shell commands require approval unless an explicit command policy rule permits them.",
            crate::types::permission::ApprovalPolicy::Granular(_) => " Approval policy: granular. Disabled approval categories are rejected instead of prompting the user.",
            crate::types::permission::ApprovalPolicy::OnRequest => " Approval policy: on-request. Operations that exceed the sandbox may ask for approval.",
            crate::types::permission::ApprovalPolicy::Never => " Approval policy: never. Approval requests and sandbox escalation are disabled; a blocked action remains blocked.",
        }));
    }
    if let Some(crate::types::permission::ApprovalPolicy::Granular(config)) =
        context.approval_policy
    {
        let categories = [
            (config.allows_sandbox_approval(), "sandbox_approval"),
            (config.allows_rules_approval(), "rules"),
            (config.allows_skill_approval(), "skill_approval"),
            (config.allows_request_permissions(), "request_permissions"),
            (config.allows_mcp_elicitations(), "mcp_elicitations"),
        ];
        for (allowed, category) in categories {
            text = text.concat(&RuntimeString::from(format!(
                " {category}: {}.",
                if allowed {
                    "may prompt"
                } else {
                    "automatically rejected"
                }
            )));
        }
    }
    if let Some(reviewer) = context.approvals_reviewer {
        text = text.concat(&RuntimeString::from(match reviewer {
            crate::types::permission::ApprovalsReviewer::User => " Approval reviewer: user.",
            crate::types::permission::ApprovalsReviewer::Automatic => " Approval reviewer: automatic. A separate risk review resolves eligible requests and may ask the user when necessary.",
        }));
    }
    if backend_kind == SandboxKind::Process {
        text = text.concat(&RuntimeString::from(" The process backend does not provide filesystem or network isolation; permission checks and any task scope remain independent."));
    }
    text = text
        .concat(&RuntimeString::from("\n\nCurrent working directory: "))
        .concat(context.cwd.as_runtime_string());
    append_paths(
        &mut text,
        "\nAdditional writable roots:",
        &context.permission.additional_writable_roots,
    );
    if let Some(scope) = context.sandbox.task_resource_scope() {
        text = text.concat(&RuntimeString::from(
            "\nAn owner-approved task resource scope is enforced; listed roots do not expand it.",
        ));
        append_paths(&mut text, "\nTask-readable roots:", &scope.readable_roots());
    }
    text = text.concat(&RuntimeString::from(if can_approve {
        "\n\nA host approval flow is available for actions that require it. Use the actual action tool; a question-tool answer does not grant runtime permission."
    } else {
        "\n\nNo interactive approval flow is available for this request. A denied action remains denied; continue any unaffected work and explain a relevant limitation."
    }));
    let retry = can_approve
        && context
            .approval_policy
            .unwrap_or_default()
            .allows_prompt(crate::types::permission::ApprovalPromptKind::Sandbox)
        && context.sandbox_retry_on_denial.unwrap_or(false);
    text = text.concat(&RuntimeString::from(if retry {
        " The shell tool may request host approval to retry a sandbox denial outside the sandbox. Do not imitate that approval through a question tool."
    } else {
        " Sandbox-denial retries outside the sandbox are disabled. Do not invent escalation parameters or try another tool to evade the denial."
    }));
    SystemPromptPart {
        name: "permissions".into(),
        text,
        cacheable: Some(false),
        cache_scope: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approval_availability_uses_the_real_handler_and_canonical_policy() {
        let mut context = crate::tools::builtins::test_ctx::make_ctx("/workspace");
        context.permission.should_avoid_prompts = true;
        context.sandbox_retry_on_denial = Some(true);
        let available = permission_instructions(&context, true);
        assert!(available.text.contains("A host approval flow is available"));
        assert!(available
            .text
            .contains("The shell tool may request host approval"));
        assert!(available.text.contains("Filesystem policy: read-only"));
        let unavailable = permission_instructions(&context, false);
        assert!(unavailable
            .text
            .contains("No interactive approval flow is available"));
        assert!(unavailable
            .text
            .contains("Sandbox-denial retries outside the sandbox are disabled"));
    }
}
