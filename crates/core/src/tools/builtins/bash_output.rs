//! The `bash_output` [`Tool`]: drain the output a background shell has produced
//! since the last poll, optionally filtered to lines matching a regex, together
//! with its status and exit code. Pairs with `bash` run in the background and
//! `kill_shell`.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::background_shells::BackgroundShellStatus;
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

/// The lowercase status token surfaced to the model.
fn status_str(s: BackgroundShellStatus) -> &'static str {
    match s {
        BackgroundShellStatus::Running => "running",
        BackgroundShellStatus::Exited => "exited",
        BackgroundShellStatus::Killed => "killed",
    }
}

use super::shell_common::first_chars;

fn split_lines(value: &RuntimeString) -> Vec<RuntimeString> {
    let mut lines = Vec::new();
    let mut start = 0;
    for (index, unit) in value.as_utf16().iter().copied().enumerate() {
        if unit == u16::from(b'\n') {
            lines.push(RuntimeString::from_utf16(
                value.as_utf16()[start..index].to_vec(),
            ));
            start = index + 1;
        }
    }
    lines.push(RuntimeString::from_utf16(
        value.as_utf16()[start..].to_vec(),
    ));
    lines
}

fn join_lines(lines: &[RuntimeString]) -> RuntimeString {
    let mut joined = RuntimeString::default();
    for (index, line) in lines.iter().enumerate() {
        if index > 0 {
            joined = joined.concat(&RuntimeString::from("\n"));
        }
        joined = joined.concat(line);
    }
    joined
}

fn v8_string(scope: &v8::PinScope<'_, '_>, value: v8::Local<'_, v8::String>) -> RuntimeString {
    let mut units = vec![0_u16; value.length()];
    value.write_v2(scope, 0, &mut units, v8::WriteFlags::default());
    RuntimeString::from_utf16(units)
}

fn filter_lines_exact(
    output: &RuntimeString,
    pattern: &RuntimeString,
) -> Result<RuntimeString, RuntimeString> {
    crate::v8_platform::initialize_v8().map_err(RuntimeString::from)?;
    let isolate = &mut v8::Isolate::new(v8::CreateParams::default());
    v8::scope!(let scope, isolate);
    let context = v8::Context::new(scope, Default::default());
    let scope = &mut v8::ContextScope::new(scope, context);
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let tc = tc.init();
    let pattern = v8::String::new_from_two_byte(&tc, pattern.as_utf16(), v8::NewStringType::Normal)
        .ok_or_else(|| RuntimeString::from("failed to allocate filter regex"))?;
    let regex =
        v8::RegExp::new(&tc, pattern, v8::RegExpCreationFlags::empty()).ok_or_else(|| {
            tc.exception()
                .and_then(|value| value.to_string(&tc))
                .map(|value| v8_string(&tc, value))
                .unwrap_or_else(|| RuntimeString::from("invalid filter regex"))
        })?;
    let mut retained = Vec::new();
    for line in split_lines(output) {
        let subject =
            v8::String::new_from_two_byte(&tc, line.as_utf16(), v8::NewStringType::Normal)
                .ok_or_else(|| RuntimeString::from("failed to allocate filter subject"))?;
        // `RegExp::exec` surfaces the JavaScript result object: on "no match"
        // it yields the JS `null` VALUE (not an empty handle), so the presence
        // of a handle alone must not be read as a match.
        if regex
            .exec(&tc, subject)
            .is_some_and(|result| !result.is_null_or_undefined())
        {
            retained.push(line);
        }
    }
    Ok(join_lines(&retained))
}

/// Build the `bash_output` input JSON Schema.
fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "shellId": {
                "type": "string",
                "description": "The id returned by bash when run in the background"
            },
            "yieldMs": {
                "description": "Observation window in milliseconds. Empty polls wait at least 5000 ms; the default maximum is 300000 ms. Expiry does not stop the process.",
                "type": "integer",
                "minimum": 0,
                "maximum": 9007199254740991_i64
            },
            "filter": {
                "description": "Optional JS regex; only output lines matching it are returned",
                "type": "string"
            }
        },
        "required": ["shellId"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap()
}

/// Return new output from a background shell since the last poll.
pub struct BashOutputTool {
    input_schema: JsonSchema,
}

impl BashOutputTool {
    fn new() -> Self {
        BashOutputTool {
            input_schema: build_input_schema(),
        }
    }
}

/// Construct the `bash_output` tool.
pub fn create_bash_output_tool() -> Arc<dyn Tool> {
    Arc::new(BashOutputTool::new())
}

#[async_trait]
impl Tool for BashOutputTool {
    fn static_name(&self) -> &str {
        "bash_output"
    }

    fn static_description(&self) -> &str {
        "Wait for background shell output until the observation window expires or the process finishes, then return its new output, status and exit code. A live process keeps the same shellId. Optionally filter the collected lines by regex."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let shell_id = input
            .get("shellId")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Reading background shell ").concat(&first_chars(&shell_id, 8)))
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
        let wait_ms = ctx
            .background_processes
            .poll_wait_ms(input.get("yieldMs").and_then(RuntimeJson::as_u64));
        let res = ctx
            .background_processes
            .read_new_wait(&shell_id, wait_ms, &ctx.signal, None)
            .await
            .map_err(ToolError::new)?;
        if !res.found {
            let mut output = RuntimeObject::new();
            output.insert("found", RuntimeJson::Bool(false));
            output.insert("status", RuntimeJson::from("exited"));
            output.insert("exitCode", RuntimeJson::Null);
            output.insert("hasNewOutput", RuntimeJson::Bool(false));
            return Ok(text_result(
                RuntimeJson::Object(output),
                RuntimeString::from("No background shell with id ")
                    .concat(&shell_id)
                    .concat(&RuntimeString::from(".")),
                true,
            ));
        }

        let mut output = res.output.clone();
        let mut filter_error: Option<RuntimeString> = None;
        let filter = input.get("filter").and_then(RuntimeJson::as_str);
        if let Some(filter) = filter {
            if !output.is_empty() {
                match filter_lines_exact(&output, filter) {
                    Ok(filtered) => output = filtered,
                    Err(error) => {
                        filter_error = Some(
                            RuntimeString::from("\n[invalid filter regex: ")
                                .concat(&error)
                                .concat(&RuntimeString::from("]")),
                        );
                    }
                }
            }
        }

        let status_line = RuntimeString::from("[status: ")
            .concat(&RuntimeString::from(status_str(res.status)))
            .concat(&match res.exit_code {
                Some(code) => RuntimeString::from_utf8(&format!(", exit code {code}")),
                None => RuntimeString::default(),
            })
            .concat(&RuntimeString::from("]"));
        let body = if output.is_empty() {
            RuntimeString::from("(no new output)")
        } else {
            output
        };
        let text = body
            .concat(&RuntimeString::from("\n"))
            .concat(&status_line)
            .concat(&filter_error.unwrap_or_default());

        let mut result = RuntimeObject::new();
        result.insert("found", RuntimeJson::Bool(true));
        result.insert("status", RuntimeJson::from(status_str(res.status)));
        result.insert(
            "shellId",
            if res.status == BackgroundShellStatus::Running {
                RuntimeJson::String(shell_id)
            } else {
                RuntimeJson::Null
            },
        );
        result.insert(
            "exitCode",
            res.exit_code.map_or(RuntimeJson::Null, RuntimeJson::from),
        );
        result.insert("hasNewOutput", RuntimeJson::Bool(!res.output.is_empty()));

        Ok(text_result(RuntimeJson::Object(result), text, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::ParentEnvSnapshot;
    #[cfg(unix)]
    use crate::runtime_values::RuntimePath;
    use crate::schemas::content::OutputBlock;
    #[cfg(unix)]
    use crate::tools::background_shells::StartShellOptions;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use crate::types::tool::ToolContext;
    use serde_json::Value;
    #[cfg(unix)]
    use std::sync::atomic::{AtomicU64, Ordering};
    #[cfg(unix)]
    use std::time::Duration;

    #[cfg(unix)]
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn runtime_from_serde(value: Value) -> RuntimeJson {
        match value {
            Value::Null => RuntimeJson::Null,
            Value::Bool(value) => RuntimeJson::Bool(value),
            Value::Number(value) => RuntimeJson::from(value.as_f64().unwrap_or_default()),
            Value::String(value) => RuntimeJson::String(RuntimeString::from(value.as_str())),
            Value::Array(values) => {
                RuntimeJson::Array(values.into_iter().map(runtime_from_serde).collect())
            }
            Value::Object(entries) => {
                let mut object = RuntimeObject::new();
                for (key, value) in entries {
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
            let env = req.env.clone().unwrap_or_else(ParentEnvSnapshot::capture);
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
            "ae-bashoutput-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    #[test]
    fn exposes_expected_metadata_and_schema() {
        let tool = create_bash_output_tool();
        assert_eq!(tool.name(), "bash_output");
        let input = rjson!({ "shellId": "abcdefghijkl" });
        assert!(tool.is_read_only(&input));
        assert!(tool.is_concurrency_safe(&input));
        assert_eq!(
            tool.summarize(&input),
            Some(rs("Reading background shell abcdefgh"))
        );
        let schema = Value::Object(tool.static_input_schema().clone());
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": {
                    "shellId": {
                        "type": "string",
                        "description": "The id returned by bash when run in the background"
                    },
                    "yieldMs": {
                "description": "Observation window in milliseconds. Empty polls wait at least 5000 ms; the default maximum is 300000 ms. Expiry does not stop the process.",
                "type": "integer",
                "minimum": 0,
                "maximum": 9007199254740991_i64
            },
            "filter": {
                        "description": "Optional JS regex; only output lines matching it are returned",
                        "type": "string"
                    }
                },
                "required": ["shellId"],
                "additionalProperties": false
            })
        );
    }

    #[tokio::test]
    async fn reports_an_unknown_shell_id() {
        let res = create_bash_output_tool()
            .call(rjson!({ "shellId": "nope" }), &ctx(), None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            res.serializable.output,
            rjson!({ "found": false, "status": "exited", "exitCode": null, "hasNewOutput": false })
        );
        assert_eq!(text_of(&res), "No background shell with id nope.");
    }

    /// Start a background shell on the shared singleton, wait for it to drain, and
    /// return its id and cwd dir for cleanup.
    #[cfg(unix)]
    async fn start_lines(lines: &str) -> (RuntimeString, String, ToolContext) {
        let cwd = unique_cwd();
        let ctx = ctx();
        let id = ctx
            .background_processes
            .start(
                crate::runtime_process::test_runtime_process_context(),
                StartShellOptions {
                    command: rs(format!("printf '{lines}'").as_str()),
                    cwd: RuntimePath::from(cwd.as_str()),
                    sandbox: Arc::new(IdSandbox),
                    env: None,
                    max_output_bytes: 64 * 1024,
                    writable_roots: None,
                    unconfined_writes: false,
                    network_allowed: None,
                    managed_network: None,
                    on_complete: None,
                },
            )
            .await
            .unwrap();
        // Let the shell finish and its pumps drain.
        tokio::time::sleep(Duration::from_millis(300)).await;
        (id, cwd, ctx)
    }

    /// `shellId` is a `RuntimeString`, which serde's `json!` cannot serialize, so the
    /// input object is built directly rather than through `rjson!`.
    #[cfg(unix)]
    fn shell_input(id: &RuntimeString, filter: &str) -> RuntimeJson {
        let mut object = RuntimeObject::new();
        object.insert("shellId", RuntimeJson::String(id.clone()));
        object.insert("filter", RuntimeJson::String(rs(filter)));
        RuntimeJson::Object(object)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn drains_output_and_applies_a_filter() {
        let (id, cwd, ctx) = start_lines("alpha\\nbeta\\ngamma\\n").await;
        let res = create_bash_output_tool()
            .call(shell_input(&id, "^a"), &ctx, None)
            .await
            .unwrap();
        let text = text_of(&res);
        assert!(text.contains("alpha"), "text: {text}");
        assert!(!text.contains("beta"), "text: {text}");
        assert!(!text.contains("gamma"), "text: {text}");
        assert!(text.contains("[status: exited"), "text: {text}");
        assert_eq!(
            res.serializable.output.get("hasNewOutput"),
            Some(&rjson!(true))
        );
        ctx.background_processes.remove(&id);
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reports_an_invalid_filter_regex_without_failing() {
        let (id, cwd, ctx) = start_lines("one\\ntwo\\n").await;
        // An unbalanced group is an invalid regex; the tool notes it and still
        // returns the (unfiltered) output rather than erroring.
        let res = create_bash_output_tool()
            .call(shell_input(&id, "("), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        let text = text_of(&res);
        assert!(text.contains("[invalid filter regex:"), "text: {text}");
        assert!(text.contains("one") && text.contains("two"), "text: {text}");
        ctx.background_processes.remove(&id);
        let _ = std::fs::remove_dir_all(&cwd);
    }
}

#[cfg(test)]
mod exact_tests {
    use super::*;

    #[test]
    fn javascript_regex_filter_preserves_lone_surrogate_lines() {
        let output =
            RuntimeString::from_utf16(vec![b'a' as u16, 0xd800, b'\n' as u16, b'b' as u16]);
        let filtered = filter_lines_exact(&output, &RuntimeString::from("^a.$"))
            .expect("valid JavaScript regex");
        assert_eq!(filtered.as_utf16(), &[b'a' as u16, 0xd800]);
    }
}
