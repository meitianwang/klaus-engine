//! Helpers shared by the shell tool family (`bash`, `bash_output`,
//! `exec_command`/`write_stdin`, `kill_shell`): model-facing output slicing
//! and the common output budget.

use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::types::errors::ToolError;
use crate::types::json::Json;
use crate::types::permission::{ApprovalPromptKind, SandboxMode};
use crate::types::tool::ToolContext;

/// The shared default cap on captured shell output presented to the model.
pub(crate) const DEFAULT_MAX_OUTPUT_BYTES: usize = 256 * 1024;

/// An explicit request also covers network failures that have no reliable denial marker.
pub(crate) async fn request_shell_escalation(
    input: &Json,
    ctx: &ToolContext,
) -> Result<bool, ToolError> {
    match input.get("sandboxPermissions") {
        None => return Ok(false),
        Some(RuntimeJson::String(value)) if value == "use_default" => return Ok(false),
        Some(RuntimeJson::String(value)) if value == "require_escalated" => {}
        _ => {
            return Err(ToolError::new(
                "sandboxPermissions must be use_default or require_escalated.",
            ))
        }
    }
    if ctx.planning_read_only {
        return Err(ToolError::new(
            "Plan mode cannot run an escalated shell command.",
        ));
    }
    if ctx.sandbox_mode == Some(SandboxMode::DangerFullAccess) {
        return Ok(false);
    }
    if !ctx
        .approval_policy
        .unwrap_or_default()
        .allows_prompt(ApprovalPromptKind::Sandbox)
    {
        return Err(ToolError::new(
            "The current approval policy does not permit an escalated shell command.",
        ));
    }
    if ctx.sandbox.task_resource_scope().is_some() {
        return Err(ToolError::new(
            "The current execution scope does not permit sandbox escalation.",
        ));
    }
    let justification = input.get("justification").and_then(RuntimeJson::as_str)
        .filter(|value| !value.is_empty() && value.len_utf16() <= 2048)
        .ok_or_else(|| ToolError::new("An escalated command requires a nonempty justification of at most 2048 characters."))?;
    let approve = ctx.request_approval.as_ref().ok_or_else(|| {
        ToolError::new("Sandbox escalation requires an available approval reviewer.")
    })?;
    let message = RuntimeString::from(
        "Allow this command to run outside the sandbox for this operation?\n\n",
    )
    .concat(justification);
    if !approve(message).await || ctx.signal.is_cancelled() {
        return Err(ToolError::new(
            "The escalated shell command was not approved.",
        ));
    }
    Ok(true)
}

/// The first `n` JavaScript UTF-16 code units of `value`.
pub(crate) fn first_chars(value: &RuntimeString, n: usize) -> RuntimeString {
    value.slice_utf16(..n)
}

/// The model-facing exit-code token: the number, or `null` when the process was
/// signal-terminated (mirrors JS string interpolation of `number | null`).
pub(crate) fn code_str(code: Option<i32>) -> RuntimeString {
    match code {
        Some(code) => RuntimeString::from_utf8(&code.to_string()),
        None => RuntimeString::from("null"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_chars_slices_utf16_code_units() {
        let value = RuntimeString::from_utf16(vec![0xd83d, 0xde00, b'x' as u16]);
        assert_eq!(first_chars(&value, 1).as_utf16(), &[0xd83d]);
    }

    #[test]
    fn code_str_renders_number_or_null() {
        assert_eq!(code_str(Some(0)), RuntimeString::from("0"));
        assert_eq!(code_str(Some(-1)), RuntimeString::from("-1"));
        assert_eq!(code_str(None), RuntimeString::from("null"));
    }
}
