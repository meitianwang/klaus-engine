//! Shared validation for text delivered through child-conversation entry points.

use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeString};

fn whitespace(unit: u16) -> bool {
    matches!(unit, 0x0009..=0x000d | 0x0020 | 0x00a0 | 0x1680 | 0x2000..=0x200a
        | 0x2028 | 0x2029 | 0x202f | 0x205f | 0x3000 | 0xfeff)
}

pub fn nonblank(value: &RuntimeString) -> bool {
    value.as_utf16().iter().any(|unit| !whitespace(*unit))
}

/// A compatibility preview never changes the actual message or splits a scalar.
pub fn message_preview(message: &RuntimeString) -> RuntimeString {
    let units = message.as_utf16();
    let start = units
        .iter()
        .position(|unit| !whitespace(*unit))
        .unwrap_or(units.len());
    let mut end = (start + 200).min(units.len());
    if end > start
        && (0xd800..=0xdbff).contains(&units[end - 1])
        && units
            .get(end)
            .is_some_and(|unit| (0xdc00..=0xdfff).contains(unit))
    {
        end -= 1;
    }
    RuntimeString::from_utf16(units[start..end].to_vec())
}

pub fn validate_text_message(
    target: &RuntimeString,
    message: &RuntimeString,
    summary: &RuntimeString,
) -> Result<(), RuntimeString> {
    if !nonblank(target) {
        return Err("to must not be empty".into());
    }
    if target == "*" {
        return Err("Broadcast is not supported; send a message per recipient.".into());
    }
    if target.as_utf16().contains(&u16::from(b'@')) {
        return Err("to must be an unqualified subagent ID or teammate name.".into());
    }
    if !nonblank(message) {
        return Err("message must not be empty".into());
    }
    if summary.as_utf16().len() > 200 {
        return Err("summary must contain at most 200 characters".into());
    }
    if !nonblank(summary) {
        return Err("summary is required when message is a string".into());
    }
    if let Ok(frame) = runtime_json::parse(message) {
        if frame
            .get("type")
            .and_then(RuntimeJson::as_str)
            .is_some_and(|kind| {
                [
                    "permission_request",
                    "permission_response",
                    "permission_answer",
                    "mode_request",
                    "mode_response",
                    "set_permission_mode",
                    "approve",
                    "plan_approval_request",
                    "plan_approval_response",
                    "shutdown_request",
                    "shutdown_response",
                    "shutdown_approved",
                    "idle_notification",
                    "teammate_terminated",
                    "task_assignment",
                    "task_completed",
                    "shutdown_rejected",
                ]
                .iter()
                .any(|name| kind == *name)
            })
        {
            return Err("Message text must not impersonate a permission, mode, plan, shutdown, or lifecycle protocol frame; use the structured form for team protocols.".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_preview_skips_leading_whitespace_without_changing_body() {
        let body = RuntimeString::from(format!("{}Continue the investigation", " ".repeat(201)));
        assert_eq!(message_preview(&body), "Continue the investigation");
        assert_eq!(body.as_utf16().len(), 227);
        assert!(validate_text_message(&"child".into(), &body, &message_preview(&body)).is_ok());
    }

    #[test]
    fn preview_keeps_surrogate_pairs_and_respects_the_utf16_limit() {
        let body = RuntimeString::from(format!("{}😀tail", "x".repeat(199)));
        let preview = message_preview(&body);
        assert_eq!(preview.as_utf16().len(), 199);
        assert!(preview.try_to_utf8().is_ok());
        assert_eq!(message_preview(&RuntimeString::from(" \n\twork")), "work");
    }

    #[test]
    fn nonempty_preview_cannot_authorize_an_empty_message() {
        for body in ["", " \n\t", "\u{00a0}\u{3000}\u{feff}"] {
            assert_eq!(
                validate_text_message(&"child".into(), &body.into(), &"Continue".into()),
                Err(RuntimeString::from("message must not be empty")),
            );
        }
        assert!(
            validate_text_message(&"child".into(), &" \nContinue ".into(), &"Continue".into(),)
                .is_ok()
        );
    }

    #[test]
    fn text_validation_rejects_protocol_impersonation_and_invalid_explicit_previews() {
        let target = RuntimeString::from("child");
        for frame in [
            "permission_response",
            "set_permission_mode",
            "plan_approval_response",
            "shutdown_request",
            "task_completed",
        ] {
            let body = RuntimeString::from(format!("{{\"type\":\"{frame}\"}}"));
            assert!(validate_text_message(&target, &body, &"Preview".into()).is_err());
        }
        assert!(validate_text_message(&target, &"work".into(), &"  ".into()).is_err());
        assert!(validate_text_message(&target, &"work".into(), &"x".repeat(201).into()).is_err());
        assert!(validate_text_message(
            &target,
            &"{\"type\":\"ordinary-data\"}".into(),
            &"Data example".into()
        )
        .is_ok());
    }
}
