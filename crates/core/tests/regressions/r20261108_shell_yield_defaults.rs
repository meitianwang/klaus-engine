//! Defect: the interactive shell tools ignored their documented wait defaults
//! and the documented EOF input. `exec_command` returned after a hard-coded
//! 250 ms regardless of the documented default window, so a command finishing
//! in under a second was still handed back as a live session; `write_stdin`
//! with input returned without waiting at all, so the model received
//! "(no new output)" and burned an extra polling round-trip; empty polls
//! honored no minimum window; and sending "\u{4}" to a piped session wrote the
//! raw control byte instead of closing stdin, so the documented EOF never
//! reached the child.
//! Root cause: the registry-backed session rewrite dropped the yield defaults,
//! the wait-after-write phase, and the pipe-EOF handling.
//! Work item: 20261108 interactive shell yield defaults. The fix collects
//! output for the full window with documented defaults and clamps (exec 10 s
//! default within 250 ms-30 s; input writes 250 ms default capped at 30 s;
//! empty polls 5 s-300 s) and closes piped stdin on EOT.

#![cfg(unix)]

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::tools::builtins::interactive_shell::{
    create_exec_command_tool, create_write_stdin_tool,
};
use klaus_agent_core::types::tool::ToolResult;

use super::r20261015_browser_diagnostics_and_targets::make_ctx;
use super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor;

fn input(value: serde_json::Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()
}

fn text_of(result: &ToolResult) -> String {
    result
        .serializable
        .model_content
        .iter()
        .map(|block| match block {
            OutputBlock::Text(text) => {
                String::from_utf8(text.text.to_node_utf8()).unwrap_or_default()
            }
            _ => String::new(),
        })
        .collect()
}

fn workspace(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "klaus-shell-yield-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn session_id_of(result: &ToolResult, context: &str) -> String {
    let session_id = result
        .serializable
        .output
        .get("sessionId")
        .and_then(RuntimeJson::as_str)
        .cloned()
        .unwrap_or_default();
    let text = String::from_utf8(session_id.to_node_utf8()).unwrap_or_default();
    assert!(!text.is_empty(), "{context}: {}", text_of(result));
    text
}

/// The documented default exec window covers a command that finishes in well
/// under a second, so the tool reports the completed process instead of
/// handing back a session that already exited.
#[tokio::test]
async fn exec_default_window_waits_for_a_short_command_to_finish() {
    let dir = workspace("exec-default");
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let ctx = make_ctx(dir.to_str().unwrap(), process);
    let result = create_exec_command_tool()
        .call(
            input(serde_json::json!({
                "command": "sleep 0.6; printf default-window-done"
            })),
            &ctx,
            None,
        )
        .await
        .unwrap();
    let text = text_of(&result);
    let json_true = runtime_json::parse(&RuntimeString::from("true")).unwrap();
    assert_eq!(
        result.serializable.output.get("exited"),
        Some(&json_true),
        "the documented default window must cover a 0.6 s command: {text}"
    );
    assert!(text.contains("default-window-done"), "text: {text}");
    assert!(text.contains("Process exited with code 0"), "text: {text}");
    std::fs::remove_dir_all(&dir).ok();
}

/// Sending input without an explicit wait still observes the child's echo in
/// the same call, instead of returning "(no new output)" immediately and
/// forcing the model to spend another round-trip on a poll.
#[tokio::test]
async fn write_with_input_default_window_captures_the_echo() {
    let dir = workspace("write-default");
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let ctx = make_ctx(dir.to_str().unwrap(), process);
    let exec = create_exec_command_tool()
        .call(
            input(serde_json::json!({ "command": "cat", "yieldMs": 300 })),
            &ctx,
            None,
        )
        .await
        .unwrap();
    let session_id = session_id_of(&exec, "cat must be handed back as a session");
    let sent = create_write_stdin_tool()
        .call(
            input(serde_json::json!({
                "sessionId": session_id,
                "input": "regression-echo\n"
            })),
            &ctx,
            None,
        )
        .await
        .unwrap();
    let text = text_of(&sent);
    assert!(
        text.contains("regression-echo"),
        "the default input window must capture the echo: {text}"
    );
    ctx.background_processes
        .kill(&RuntimeString::from(session_id.as_str()));
    std::fs::remove_dir_all(&dir).ok();
}

/// An empty poll never waits less than the documented floor, so output that
/// arrives hundreds of milliseconds later is still delivered by the same call;
/// the window still ends early once the process exits.
#[tokio::test]
async fn empty_poll_floor_waits_for_delayed_output_until_exit() {
    let dir = workspace("poll-floor");
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let ctx = make_ctx(dir.to_str().unwrap(), process);
    let exec = create_exec_command_tool()
        .call(
            input(serde_json::json!({
                "command": "sleep 0.7; printf poll-floor-done",
                "yieldMs": 300
            })),
            &ctx,
            None,
        )
        .await
        .unwrap();
    let session_id = session_id_of(&exec, "the 0.7 s command must outlive the 300 ms window");
    let poll = create_write_stdin_tool()
        .call(
            input(serde_json::json!({ "sessionId": session_id, "yieldMs": 50 })),
            &ctx,
            None,
        )
        .await
        .unwrap();
    let text = text_of(&poll);
    assert!(
        text.contains("poll-floor-done"),
        "the empty-poll floor must cover the delayed output: {text}"
    );
    assert!(text.contains("Process exited with code 0"), "text: {text}");
    std::fs::remove_dir_all(&dir).ok();
}

/// The documented EOT input closes a piped session's stdin so the child
/// observes EOF and terminates, instead of receiving a raw control byte that
/// leaves the session running forever.
#[tokio::test]
async fn eot_input_closes_piped_stdin_and_ends_the_session() {
    let dir = workspace("eot");
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let ctx = make_ctx(dir.to_str().unwrap(), process);
    let exec = create_exec_command_tool()
        .call(
            input(serde_json::json!({ "command": "cat", "yieldMs": 300 })),
            &ctx,
            None,
        )
        .await
        .unwrap();
    let session_id = session_id_of(&exec, "cat must be handed back as a session");
    let sent = create_write_stdin_tool()
        .call(
            input(serde_json::json!({
                "sessionId": session_id,
                "input": "\u{4}",
                "yieldMs": 2000
            })),
            &ctx,
            None,
        )
        .await
        .unwrap();
    let text = text_of(&sent);
    assert!(
        text.contains("Process exited with code 0"),
        "EOT must close piped stdin so cat exits: {text}"
    );
    std::fs::remove_dir_all(&dir).ok();
}
