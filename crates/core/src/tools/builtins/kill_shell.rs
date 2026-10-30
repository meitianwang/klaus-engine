//! The `kill_shell` [`Tool`]: terminate a background shell (from `bash` run in
//! the background) OR an interactive session (from `exec_command`) and its whole
//! process group by id.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::tools::shell::Signal;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

use super::shell_common::first_chars;

fn parse_signal(signal: Option<&RuntimeString>) -> Result<Signal, RuntimeString> {
    let signal = signal
        .cloned()
        .unwrap_or_else(|| RuntimeString::from("SIGTERM"));
    if signal == "SIGTERM" {
        Ok(Signal::Term)
    } else if signal == "SIGKILL" {
        Ok(Signal::Kill)
    } else {
        Err(RuntimeString::from("Unsupported signal ")
            .concat(&signal)
            .concat(&RuntimeString::from(". Use SIGTERM or SIGKILL.")))
    }
}

fn signal_label(signal: Signal) -> &'static str {
    match signal {
        Signal::Term => "SIGTERM",
        Signal::Kill => "SIGKILL",
        Signal::Int => "SIGINT",
        Signal::Hup => "SIGHUP",
        Signal::Quit => "SIGQUIT",
        Signal::Usr1 => "SIGUSR1",
        Signal::Usr2 => "SIGUSR2",
    }
}

/// Build the `kill_shell` input JSON Schema.
fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "shellId": {
                "type": "string",
                "description": "The id returned by bash (background) or exec_command (session)"
            },
            "signal": {
                "type": "string",
                "enum": ["SIGTERM", "SIGKILL"],
                "description": "Signal to send; defaults to SIGTERM with SIGKILL fallback"
            }
        },
        "required": ["shellId"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap()
}

/// Kill a background shell or interactive session by id.
pub struct KillShellTool {
    input_schema: JsonSchema,
}

impl KillShellTool {
    fn new() -> Self {
        KillShellTool {
            input_schema: build_input_schema(),
        }
    }
}

/// Construct the `kill_shell` tool.
pub fn create_kill_shell_tool() -> Arc<dyn Tool> {
    Arc::new(KillShellTool::new())
}

#[async_trait]
impl Tool for KillShellTool {
    fn static_name(&self) -> &str {
        "kill_shell"
    }

    fn static_description(&self) -> &str {
        "Kill a background shell (from bash) or an interactive session (from exec_command) by id, terminating its whole process group."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let shell_id = input
            .get("shellId")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let signal = parse_signal(input.get("signal").and_then(RuntimeJson::as_str))
            .map(signal_label)
            .unwrap_or("signal");
        Some(
            RuntimeString::from("Killing shell ")
                .concat(&first_chars(&shell_id, 8))
                .concat(&RuntimeString::from(" with "))
                .concat(&RuntimeString::from(signal)),
        )
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let shell_id = input
            .get("shellId")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let signal = match parse_signal(input.get("signal").and_then(RuntimeJson::as_str)) {
            Ok(signal) => signal,
            Err(message) => {
                let mut output = RuntimeObject::new();
                output.insert("killed", RuntimeJson::Bool(false));
                return Ok(text_result(RuntimeJson::Object(output), message, true));
            }
        };
        let killed = ctx
            .background_processes
            .stop_with_signal_and_wait(&shell_id, signal)
            .await
            .map_err(ToolError::new)?
            .accepted;
        if !killed {
            let mut output = RuntimeObject::new();
            output.insert("killed", RuntimeJson::Bool(false));
            return Ok(text_result(
                RuntimeJson::Object(output),
                RuntimeString::from("No shell or session with id ")
                    .concat(&shell_id)
                    .concat(&RuntimeString::from(".")),
                true,
            ));
        }
        let mut output = RuntimeObject::new();
        output.insert("killed", RuntimeJson::Bool(true));
        Ok(text_result(
            RuntimeJson::Object(output),
            RuntimeString::from("Sent ")
                .concat(&RuntimeString::from(signal_label(signal)))
                .concat(&RuntimeString::from(" to "))
                .concat(&shell_id)
                .concat(&RuntimeString::from(".")),
            false,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::{ParentEnvSnapshot, RuntimeEnv};
    use crate::runtime_values::RuntimeNumber;
    #[cfg(unix)]
    use crate::runtime_values::RuntimePath;
    use crate::schemas::content::OutputBlock;
    #[cfg(unix)]
    use crate::tools::background_shells::StartShellOptions;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use serde_json::Value;
    #[cfg(unix)]
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(unix)]
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn runtime_from_serde(value: Value) -> RuntimeJson {
        match value {
            Value::Null => RuntimeJson::Null,
            Value::Bool(value) => RuntimeJson::Bool(value),
            Value::Number(value) => {
                RuntimeJson::Number(RuntimeNumber::from_f64(value.as_f64().unwrap()))
            }
            Value::String(value) => RuntimeJson::String(RuntimeString::from(value)),
            Value::Array(values) => {
                RuntimeJson::Array(values.into_iter().map(runtime_from_serde).collect())
            }
            Value::Object(values) => {
                let mut object = RuntimeObject::new();
                for (key, value) in values {
                    object.insert(key, runtime_from_serde(value));
                }
                RuntimeJson::Object(object)
            }
        }
    }

    macro_rules! rjson {
        ($($json:tt)+) => {
            runtime_from_serde(serde_json::json!($($json)+))
        };
    }

    fn rs(value: impl Into<RuntimeString>) -> RuntimeString {
        value.into()
    }

    struct IdSandbox;
    impl SandboxBackend for IdSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            let file = req.argv.first().cloned().unwrap_or_default();
            let args = req.argv.iter().skip(1).cloned().collect();
            let env: RuntimeEnv = req.env.clone().unwrap_or_else(ParentEnvSnapshot::capture);
            SpawnSpec {
                file,
                args,
                cwd: req.cwd.clone(),
                env,
            }
        }
    }

    fn ctx() -> ToolContext {
        let mut ctx = make_ctx(&std::env::temp_dir().to_string_lossy());
        ctx.sandbox = Arc::new(IdSandbox);
        ctx
    }

    fn ui_string(value: &RuntimeString) -> String {
        String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 UI projection is valid UTF-8")
    }

    fn text_of(res: &ToolResult) -> String {
        res.serializable
            .model_content
            .iter()
            .map(|b| match b {
                OutputBlock::Text(t) => ui_string(&t.text),
                _ => String::new(),
            })
            .collect()
    }

    #[cfg(unix)]
    fn unique_cwd() -> String {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "ae-killshell-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    #[test]
    fn exposes_expected_metadata_and_schema() {
        let tool = create_kill_shell_tool();
        assert_eq!(tool.name(), "kill_shell");
        let input = rjson!({ "shellId": "abcdefghijkl", "signal": "SIGTERM" });
        assert!(!tool.is_read_only(&input));
        assert!(!tool.is_concurrency_safe(&input));
        assert_eq!(
            tool.summarize(&input),
            Some(rs("Killing shell abcdefgh with SIGTERM"))
        );
        let schema = Value::Object(tool.static_input_schema().clone());
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": {
                    "shellId": {
                        "type": "string",
                        "description": "The id returned by bash (background) or exec_command (session)"
                    },
                    "signal": {
                        "type": "string",
                        "enum": ["SIGTERM", "SIGKILL"],
                        "description": "Signal to send; defaults to SIGTERM with SIGKILL fallback"
                    }
                },
                "required": ["shellId"],
                "additionalProperties": false
            })
        );
    }

    #[test]
    fn parses_supported_kill_signals_like_pty_control_frames() {
        assert_eq!(parse_signal(None), Ok(Signal::Term));
        assert_eq!(parse_signal(Some(&rs("SIGTERM"))), Ok(Signal::Term));
        assert_eq!(parse_signal(Some(&rs("SIGKILL"))), Ok(Signal::Kill));
        assert!(parse_signal(Some(&rs("SIGINT"))).is_err());
        assert!(parse_signal(Some(&rs("SIGHUP"))).is_err());
        assert!(parse_signal(Some(&rs("SIGQUIT"))).is_err());
        assert!(parse_signal(Some(&rs("SIGUSR1"))).is_err());
    }

    #[tokio::test]
    async fn reports_an_unknown_id() {
        let res = create_kill_shell_tool()
            .call(rjson!({ "shellId": "nope" }), &ctx(), None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(res.serializable.output, rjson!({ "killed": false }));
        assert_eq!(text_of(&res), "No shell or session with id nope.");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn kills_a_live_background_shell() {
        let cwd = unique_cwd();
        let ctx = ctx();
        let id = ctx
            .background_processes
            .start(
                crate::runtime_process::test_runtime_process_context(),
                StartShellOptions {
                    command: rs("sleep 30"),
                    cwd: RuntimePath::from(cwd.as_str()),
                    sandbox: Arc::new(IdSandbox),
                    env: None,
                    max_output_bytes: 1024,
                    writable_roots: None,
                    unconfined_writes: false,
                    network_allowed: None,
                    managed_network: None,
                    on_complete: None,
                },
            )
            .await
            .unwrap();
        tokio::task::yield_now().await;
        let res = create_kill_shell_tool()
            .call(rjson!({ "shellId": ui_string(&id) }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(res.serializable.output, rjson!({ "killed": true }));
        assert_eq!(
            text_of(&res),
            format!("Sent SIGTERM to {}.", ui_string(&id))
        );
        ctx.background_processes.remove(&id);
        let _ = std::fs::remove_dir_all(&cwd);
    }
}

#[cfg(test)]
mod exact_tests {
    use super::*;

    #[test]
    fn signal_parser_keeps_exact_error_identity() {
        let unsupported = RuntimeString::from_utf16(vec![b'S' as u16, 0xd800]);
        let error = parse_signal(Some(&unsupported)).expect_err("unsupported signal");
        assert!(error.as_utf16().contains(&0xd800));
    }
}
