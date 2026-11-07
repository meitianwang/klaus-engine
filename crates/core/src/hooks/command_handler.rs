//! Command hook execution: spawn a shell command, feed it the hook input on
//! stdin, and translate its stdout JSON into a hook outcome.

use crate::hooks::types::{HookDecision, HookInput, HookOutcome, HookResult};
use crate::runtime_process::{RuntimeProcessContext, RuntimeSpawnRequest, RuntimeStdio};
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson as Json, RuntimeObject, RuntimeString, WhatwgUtf8,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Run a command hook: the HookInput JSON is written to the command's stdin, and
/// its stdout (if JSON) is translated into a HookOutcome. Tolerant — a failing or
/// non-JSON command degrades to `failed_continue` rather than erroring.
pub async fn run_command_hook(
    process_context: &RuntimeProcessContext,
    command: &str,
    input: &HookInput,
    timeout_ms: Option<u64>,
) -> HookOutcome {
    let timeout_ms = timeout_ms.unwrap_or(30_000);
    let failed_continue = || HookOutcome {
        result: Some(HookResult::FailedContinue),
        ..HookOutcome::default()
    };

    let spawner = process_context.spawner();
    let request = RuntimeSpawnRequest::new(RuntimeString::from("bash"))
        .with_args(vec![
            RuntimeString::from("-c"),
            RuntimeString::from(command),
        ])
        .with_stdio(RuntimeStdio::Pipe, RuntimeStdio::Pipe, RuntimeStdio::Pipe)
        .with_hard_kill_on_drop(true);
    let mut child = match spawner.spawn(request).await {
        Ok(child) => child,
        // Spawn failure (no shell, resource limits) → degrade.
        Err(_) => return failed_continue(),
    };

    let payload = match runtime_codec::encode_bytes(input) {
        Ok(payload) => payload,
        Err(_) => return failed_continue(),
    };
    let stdin = child.take_stdin();
    let mut stdout = child.take_stdout();
    let mut stderr = child.take_stderr();

    // Feed stdin and collect stdout concurrently (so neither pipe can stall the
    // other), then wait for the exit status. Stdin write errors are tolerated —
    // a hook that exits without reading its input still gets judged on its exit
    // code and stdout.
    let work = async {
        let write = async {
            if let Some(mut stdin) = stdin {
                let _ = stdin.write_all(&payload).await;
                let _ = stdin.shutdown().await;
            }
        };
        let read = async {
            let mut buf = Vec::new();
            if let Some(stdout) = stdout.as_mut() {
                let _ = stdout.read_to_end(&mut buf).await;
            }
            buf
        };
        let drain_stderr = async {
            let mut discarded = Vec::new();
            if let Some(stderr) = stderr.as_mut() {
                let _ = stderr.read_to_end(&mut discarded).await;
            }
        };
        let ((), out, ()) = tokio::join!(write, read, drain_stderr);
        let status = child.wait().await;
        (out, status)
    };
    let raced = tokio::time::timeout(Duration::from_millis(timeout_ms), work).await;
    let (out, status) = match raced {
        Ok(result) => result,
        Err(_) => {
            // Dropping the owned runtime child executes its closed hard-kill policy.
            drop(child);
            return failed_continue();
        }
    };
    let status = match status {
        Ok(status) => status,
        Err(_) => return failed_continue(),
    };

    // Base result comes from the exit code (a signal-killed hook has no code →
    // failed_continue); a parseable JSON object on stdout refines it.
    let base_result = if status.code() == Some(0) {
        HookResult::Success
    } else {
        HookResult::FailedContinue
    };
    let mut decoder = WhatwgUtf8::new();
    let out = decoder.feed(&out).concat(&decoder.finish());
    match runtime_json::parse(&out) {
        Ok(parsed) if parsed.as_object().is_some() => {
            let mut outcome = translate_hook_wire_output(
                parsed
                    .as_object()
                    .expect("object presence was checked by the match guard"),
            );
            // An explicit result in the wire output wins; otherwise the exit code decides.
            if outcome.result.is_none() {
                outcome.result = Some(base_result);
            }
            outcome
        }
        _ => HookOutcome {
            result: Some(base_result),
            ..HookOutcome::default()
        },
    }
}

/// The first non-empty string among the candidates, or `None`.
fn first_string(candidates: &[Option<&Json>]) -> Option<RuntimeString> {
    for candidate in candidates {
        if let Some(Json::String(s)) = candidate {
            if !s.is_empty() {
                return Some(s.clone());
            }
        }
    }
    None
}

/// Translate a hook command's stdout JSON into a HookOutcome. The documented hook
/// wire format differs field-for-field from the engine's HookOutcome, so a blind merge
/// silently dropped most of it. We map that wire shape AND still accept the engine's own
/// field names (a fn-hook can share this shape), so both formats work:
///
///  - `continue: false`            → `result: 'failed_abort'` (halt the turn), with
///    `stopReason` surfaced as the system message.
///  - top-level `decision`         → `approve` / `block` (legacy PreToolUse form).
///  - `hookSpecificOutput.permissionDecision` `allow`/`deny` → `approve`/`block`.
///  - `hookSpecificOutput.additionalContext` (or top-level) → `additionalContext`.
///  - `hookSpecificOutput.updatedInput` (or top-level)      → `updatedInput`.
///  - top-level `updatedToolOutput` (PostToolUse)        → `updatedToolOutput`.
///  - `permissionDecisionReason` / legacy `reason` (an ask/block explanation) → `permissionReason`,
///    a channel distinct from `additionalContext` so the reason
///    drives the prompt / block message without being appended twice.
pub fn translate_hook_wire_output(parsed: &RuntimeObject) -> HookOutcome {
    let empty = RuntimeObject::new();
    let hso = parsed
        .get("hookSpecificOutput")
        .and_then(Json::as_object)
        .unwrap_or(&empty);
    let mut out = HookOutcome::default();

    // result: an explicit engine value wins; otherwise `continue:false` halts the turn.
    let continue_false = parsed.get("continue") == Some(&Json::Bool(false));
    match parsed.get("result").and_then(Json::as_str) {
        Some(value) if value == "success" => out.result = Some(HookResult::Success),
        Some(value) if value == "failed_continue" => out.result = Some(HookResult::FailedContinue),
        Some(value) if value == "failed_abort" => out.result = Some(HookResult::FailedAbort),
        _ => {
            if continue_false {
                out.result = Some(HookResult::FailedAbort);
            }
        }
    }

    // decision: top-level approve/block (legacy + engine) or hso.permissionDecision.
    // hso `ask` ESCALATES to the user prompt; the reason flows via additionalContext below.
    // (`defer` is print-mode-only with no engine surface → left as a no-op passthrough.)
    match parsed.get("decision").and_then(Json::as_str) {
        Some(value) if value == "approve" => out.decision = Some(HookDecision::Approve),
        Some(value) if value == "block" => out.decision = Some(HookDecision::Block),
        _ => match hso.get("permissionDecision").and_then(Json::as_str) {
            Some(value) if value == "allow" => out.decision = Some(HookDecision::Approve),
            Some(value) if value == "deny" => out.decision = Some(HookDecision::Block),
            Some(value) if value == "ask" => out.decision = Some(HookDecision::Ask),
            _ => {}
        },
    }

    // updatedInput: nested (PreToolUse) or top-level (engine); a null nested
    // value falls through to the top-level one.
    let updated_input = match hso.get("updatedInput") {
        Some(v) if !v.is_null() => Some(v),
        _ => parsed.get("updatedInput"),
    };
    if let Some(updated_input) = updated_input {
        out.updated_input = Some(updated_input.clone());
    }

    // updatedToolOutput: top-level (PostToolUse + engine).
    if let Some(updated_tool_output) = parsed.get("updatedToolOutput") {
        out.updated_tool_output = Some(updated_tool_output.clone());
    }

    // additionalContext: nested or top-level (engine) — model-visible appended context ONLY.
    if let Some(additional_context) = first_string(&[
        hso.get("additionalContext"),
        parsed.get("additionalContext"),
    ]) {
        out.additional_context = Some(additional_context);
    }

    // The ask/block REASON (`permissionDecisionReason`, or a legacy top-level `reason`) is a
    // SEPARATE channel from additionalContext — it drives the prompt / block message, and is NOT
    // appended as context, so the reason is never surfaced twice.
    if let Some(permission_reason) =
        first_string(&[hso.get("permissionDecisionReason"), parsed.get("reason")])
    {
        out.permission_reason = Some(permission_reason);
    }

    // systemMessage: top-level; `stopReason` (the halt explanation) is surfaced here too.
    let system_message = first_string(&[
        parsed.get("systemMessage"),
        if continue_false {
            parsed.get("stopReason")
        } else {
            None
        },
    ]);
    if let Some(system_message) = system_message {
        out.system_message = Some(system_message);
    }

    if let Some(worktree_path) = parsed.get("worktreePath").and_then(Json::as_str) {
        out.worktree_path = Some(worktree_path.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::types::HookEvent;

    fn exact_json(source: &str) -> Json {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact runtime JSON")
    }

    fn exact_object(source: &str) -> RuntimeObject {
        exact_json(source)
            .as_object()
            .expect("expected a JSON object")
            .clone()
    }

    #[test]
    fn maps_continue_false_to_a_turn_halting_result_with_the_stop_reason_message() {
        assert_eq!(
            translate_hook_wire_output(&exact_object(
                r#"{"continue":false,"stopReason":"policy violation"}"#,
            )),
            HookOutcome {
                result: Some(HookResult::FailedAbort),
                system_message: Some(RuntimeString::from("policy violation")),
                ..HookOutcome::default()
            }
        );
    }

    #[test]
    fn unwraps_hook_specific_output_additional_context_and_updated_input() {
        assert_eq!(
            translate_hook_wire_output(&exact_object(
                r#"{"hookSpecificOutput":{"hookEventName":"PreToolUse","additionalContext":"note","updatedInput":{"path":"x"}}}"#,
            )),
            HookOutcome {
                additional_context: Some(RuntimeString::from("note")),
                updated_input: Some(exact_json(r#"{"path":"x"}"#)),
                ..HookOutcome::default()
            }
        );
    }

    #[test]
    fn maps_hso_permission_decision_allow_deny_ask_to_approve_block_ask() {
        assert_eq!(
            translate_hook_wire_output(&exact_object(
                r#"{"hookSpecificOutput":{"permissionDecision":"allow"}}"#,
            ))
            .decision,
            Some(HookDecision::Approve)
        );
        assert_eq!(
            translate_hook_wire_output(&exact_object(
                r#"{"hookSpecificOutput":{"permissionDecision":"deny"}}"#,
            ))
            .decision,
            Some(HookDecision::Block)
        );
        assert_eq!(
            translate_hook_wire_output(&exact_object(
                r#"{"hookSpecificOutput":{"permissionDecision":"ask"}}"#,
            ))
            .decision,
            Some(HookDecision::Ask)
        );
    }

    #[test]
    fn carries_an_ask_reason_into_the_separate_permission_reason_channel() {
        assert_eq!(
            translate_hook_wire_output(&exact_object(
                r#"{"hookSpecificOutput":{"permissionDecision":"ask","permissionDecisionReason":"confirm?"}}"#,
            )),
            HookOutcome {
                decision: Some(HookDecision::Ask),
                permission_reason: Some(RuntimeString::from("confirm?")),
                ..HookOutcome::default()
            }
        );
    }

    #[test]
    fn leaves_cc_defer_as_a_no_op_no_engine_surface() {
        assert_eq!(
            translate_hook_wire_output(&exact_object(
                r#"{"hookSpecificOutput":{"permissionDecision":"defer"}}"#,
            ))
            .decision,
            None
        );
    }

    #[test]
    fn reads_top_level_updated_tool_output_post_tool_use() {
        assert_eq!(
            translate_hook_wire_output(&exact_object(r#"{"updatedToolOutput":"replaced"}"#))
                .updated_tool_output,
            Some(exact_json(r#""replaced""#))
        );
    }

    #[test]
    fn honors_the_legacy_top_level_decision_plus_reason_form() {
        assert_eq!(
            translate_hook_wire_output(&exact_object(r#"{"decision":"block","reason":"no"}"#)),
            HookOutcome {
                decision: Some(HookDecision::Block),
                permission_reason: Some(RuntimeString::from("no")),
                ..HookOutcome::default()
            }
        );
    }

    #[test]
    fn routes_permission_decision_reason_to_the_permission_reason_channel_not_additional_context() {
        assert_eq!(
            translate_hook_wire_output(&exact_object(
                r#"{"hookSpecificOutput":{"permissionDecision":"deny","permissionDecisionReason":"blocked path"}}"#,
            )),
            HookOutcome {
                decision: Some(HookDecision::Block),
                permission_reason: Some(RuntimeString::from("blocked path")),
                ..HookOutcome::default()
            }
        );
    }

    #[test]
    fn keeps_additional_context_and_permission_decision_reason_as_distinct_channels() {
        assert_eq!(
            translate_hook_wire_output(&exact_object(
                r#"{"hookSpecificOutput":{"permissionDecision":"ask","permissionDecisionReason":"why","additionalContext":"note"}}"#,
            )),
            HookOutcome {
                decision: Some(HookDecision::Ask),
                additional_context: Some(RuntimeString::from("note")),
                permission_reason: Some(RuntimeString::from("why")),
                ..HookOutcome::default()
            }
        );
    }

    #[test]
    fn still_accepts_direct_engine_hook_outcome_fields() {
        assert_eq!(
            translate_hook_wire_output(&exact_object(
                r#"{"result":"failed_abort","additionalContext":"x","systemMessage":"m"}"#,
            )),
            HookOutcome {
                result: Some(HookResult::FailedAbort),
                additional_context: Some(RuntimeString::from("x")),
                system_message: Some(RuntimeString::from("m")),
                ..HookOutcome::default()
            }
        );
    }

    #[test]
    fn returns_an_empty_outcome_for_an_empty_object() {
        assert_eq!(
            translate_hook_wire_output(&RuntimeObject::new()),
            HookOutcome::default()
        );
    }

    fn input() -> HookInput {
        HookInput {
            event: HookEvent::PreToolUse,
            cwd: RuntimeString::from("/x"),
            session_id: RuntimeString::from("s"),
            tool_name: None,
            tool_input: None,
            tool_result: None,
            error: None,
            is_interrupt: None,
            prompt: None,
            message: None,
            agent_type: None,
            agent_id: None,
            trigger: None,
        }
    }

    #[tokio::test]
    async fn translates_a_cc_shaped_stdout_into_a_hook_outcome() {
        let json = r#"{"hookSpecificOutput":{"permissionDecision":"deny","permissionDecisionReason":"nope"}}"#;
        let r = run_command_hook(
            &crate::runtime_process::test_runtime_process_context(),
            &format!("cat >/dev/null; echo '{json}'"),
            &input(),
            None,
        )
        .await;
        assert_eq!(r.decision, Some(HookDecision::Block));
        assert_eq!(
            r.permission_reason.as_ref(),
            Some(&RuntimeString::from("nope"))
        );
        assert_eq!(r.result, Some(HookResult::Success));
    }

    #[tokio::test]
    async fn halts_the_turn_on_a_cc_continue_false() {
        let json = r#"{"continue":false,"stopReason":"stop now"}"#;
        let r = run_command_hook(
            &crate::runtime_process::test_runtime_process_context(),
            &format!("cat >/dev/null; echo '{json}'"),
            &input(),
            None,
        )
        .await;
        assert_eq!(r.result, Some(HookResult::FailedAbort));
        assert_eq!(
            r.system_message.as_ref(),
            Some(&RuntimeString::from("stop now"))
        );
    }

    #[tokio::test]
    async fn hook_stdin_uses_javascript_json_stringify_number_projection() {
        let mut hook_input = input();
        hook_input.tool_input = Some(
            runtime_json::parse(&RuntimeString::from(
                r#"{"unsafe":9007199254740993,"negativeZero":-0,"overflow":1e400}"#,
            ))
            .expect("valid runtime hook input"),
        );
        let outcome = run_command_hook(
            &crate::runtime_process::test_runtime_process_context(),
            r#"IFS= read -r payload; printf '{"updatedInput":%s}\n' "$payload""#,
            &hook_input,
            None,
        )
        .await;

        assert_eq!(
            outcome.updated_input.expect("echoed hook input")["toolInput"],
            exact_json(r#"{"unsafe":9007199254740992,"negativeZero":0,"overflow":null}"#,)
        );
    }

    #[tokio::test]
    async fn hook_stdout_json_parse_uses_javascript_number_semantics() {
        let outcome = run_command_hook(
            &crate::runtime_process::test_runtime_process_context(),
            r#"printf '%s\n' '{"updatedInput":{"unsafe":9007199254740993,"negativeZero":-0,"overflow":1e400}}'"#,
            &input(),
            None,
        )
        .await;
        let updated = outcome.updated_input.expect("trusted hook updated input");

        assert_eq!(updated["unsafe"], exact_json("9007199254740992"));
        let negative_zero = updated["negativeZero"].as_f64().unwrap();
        assert_eq!(negative_zero, 0.0);
        assert!(negative_zero.is_sign_negative());
        assert!(updated["overflow"].is_number());
        assert_eq!(updated["overflow"].as_f64(), Some(f64::INFINITY));
        // JSON.stringify(Infinity) renders "null" in JavaScript.
        assert_eq!(
            runtime_json::stringify(&updated["overflow"]),
            RuntimeString::from("null")
        );
    }

    #[tokio::test]
    async fn falls_back_to_the_exit_code_result_on_non_json_output() {
        assert_eq!(
            run_command_hook(
                &crate::runtime_process::test_runtime_process_context(),
                "echo not-json",
                &input(),
                None,
            )
            .await
            .result,
            Some(HookResult::Success)
        );
        assert_eq!(
            run_command_hook(
                &crate::runtime_process::test_runtime_process_context(),
                "echo not-json; exit 1",
                &input(),
                None,
            )
            .await
            .result,
            Some(HookResult::FailedContinue)
        );
    }
}
