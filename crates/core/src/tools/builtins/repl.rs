//! `REPL` — execute JavaScript with programmatic tool access. State persists
//! across calls in one session-scoped context: top-level `await`,
//! `let`/`const` persistence, and last-expression results are provided by the
//! persistent in-process V8 runtime
//! ([`crate::engine::code_mode::runtime::repl_session`]). Every other tool is
//! exposed inside the context as an async function routed through the full
//! engine pipeline (`invoke_tool`), so permissions and hooks still apply to
//! nested calls.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::engine::code_mode::runtime::repl_session::{
    ReplExecOutcome, ReplExecRequest, ReplToolBinding, REPL_DEFAULT_TIMEOUT_MS, REPL_MAX_TIMEOUT_MS,
};
use crate::engine::code_mode::session::{CodeModeSessionProvider, ReplSessionHandle};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::permission::{PermissionReason, PermissionResult};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

/// A name → identifier-safe key for the `tools` bridge (most engine tool names
/// already are): every unit outside `[A-Za-z0-9_$]` becomes `_`.
fn normalize_tool_name(name: &RuntimeString) -> RuntimeString {
    RuntimeString::from_utf16(
        name.as_utf16()
            .iter()
            .map(|unit| {
                let ascii = u8::try_from(*unit).ok();
                if ascii.is_some_and(|value| {
                    value.is_ascii_alphanumeric() || value == b'_' || value == b'$'
                }) {
                    *unit
                } else {
                    u16::from(b'_')
                }
            })
            .collect::<Vec<_>>(),
    )
}

const REPL_DESCRIPTION: &str = "Execute JavaScript code with access to the session's tools.

REPL is your programming interface to the engine's tools. Use it to loop, branch, and compose tool calls with code.

## How to Use

Write JavaScript that calls the tools available in this session as async functions. Replace the example path with a relevant file:
```javascript
const path = 'src/example.ts'
const content = await read({ path })
if (String(content).includes('oldName')) {
  await edit({ path, oldString: 'oldName', newString: 'newName', replaceAll: true })
}
```

Nested tool calls return their text content. When that text is valid JSON, it is returned as a parsed value; otherwise it is a string.

Batch independent operations in one REPL call when useful. Keep dependent steps sequential, inspect their results, and adapt before continuing.

## Available Tools

All tools work as async functions: `read`, `write`, `edit`, `glob`, `grep`, `bash`, etc. MCP tools are callable by their full name (e.g. `await mcp__slack__slack_send_message({...})`).

## Tips
- `import`/`require` don't work here — the context is sealed. For filesystem access use `read`/`write`/`glob`; for shell use `bash`.
- Use `Promise.all()` for parallel operations
- Variables persist across REPL calls
- Last expression is returned as the result
- `console.log` output is captured as stdout, `console.error` as stderr";

fn repl_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "code": {
                "type": "string",
                "description": "JavaScript code to execute. Supports top-level await. State persists across calls."
            },
            "description": {
                "type": "string",
                "description": "Clear, concise description of what this script does in active voice (5-10 words). E.g. \"Trace upgrade message to its GrowthBook flag\""
            },
            "timeout": {
                "type": "number",
                "description": "Optional timeout in milliseconds (default 30000, max 600000)"
            }
        },
        "required": ["code"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// Assemble the model-visible text for one exec outcome, mirroring the
/// reference result mapping: a bare formatted result when it is the only
/// payload, else labeled sections joined by blank lines.
pub(crate) fn render_repl_text(outcome: &ReplExecOutcome) -> RuntimeString {
    if outcome.stdout.is_empty()
        && outcome.stderr.is_empty()
        && outcome.error.is_none()
        && outcome.result_defined
    {
        return outcome.result_text.clone();
    }
    let mut sections: Vec<RuntimeString> = Vec::new();
    if !outcome.stdout.is_empty() {
        sections.push(RuntimeString::from("stdout:\n").concat(&outcome.stdout));
    }
    if !outcome.stderr.is_empty() {
        sections.push(RuntimeString::from("stderr:\n").concat(&outcome.stderr));
    }
    if let Some(error) = &outcome.error {
        sections.push(RuntimeString::from("error: ").concat(error));
    }
    if outcome.result_defined {
        sections.push(RuntimeString::from("result: ").concat(&outcome.result_text));
    }
    let mut text = RuntimeString::default();
    for (index, section) in sections.iter().enumerate() {
        if index > 0 {
            text = text.concat(&RuntimeString::from("\n\n"));
        }
        text = text.concat(section);
    }
    text
}

/// The structured tool output: `{ code, result, stdout, stderr, error? }`.
fn repl_output(code: &RuntimeString, outcome: &ReplExecOutcome) -> Json {
    let mut output = RuntimeObject::new();
    output.insert("code", RuntimeJson::String(code.clone()));
    output.insert("result", outcome.result.clone());
    output.insert("stdout", RuntimeJson::String(outcome.stdout.clone()));
    output.insert("stderr", RuntimeJson::String(outcome.stderr.clone()));
    if let Some(error) = &outcome.error {
        output.insert("error", RuntimeJson::String(error.clone()));
    }
    RuntimeJson::Object(output)
}

/// First line of the code, bounded at 50 UTF-16 units with an ellipsis
/// (the tool-use summary).
fn code_summary(code: &RuntimeString) -> Option<RuntimeString> {
    let units = code.as_utf16();
    let line_end = units
        .iter()
        .position(|unit| *unit == u16::from(b'\n'))
        .unwrap_or(units.len());
    let mut start = 0;
    while start < line_end && units[start] == u16::from(b' ') {
        start += 1;
    }
    let first_line = code.slice_utf16(start..line_end);
    if first_line.is_empty() {
        return None;
    }
    if first_line.as_utf16().len() > 50 {
        return Some(
            first_line
                .slice_utf16(..49)
                .concat(&RuntimeString::from("\u{2026}")),
        );
    }
    Some(first_line)
}

/// The tool bindings exposed inside the context: every cataloged tool except
/// `REPL` itself, under identifier-safe global names.
fn tool_bindings(ctx: &ToolContext) -> Vec<ReplToolBinding> {
    let Some(catalog) = &ctx.tool_catalog else {
        return Vec::new();
    };
    catalog()
        .into_iter()
        .filter(|entry| entry.name != "REPL")
        .map(|entry| ReplToolBinding {
            key: normalize_tool_name(&entry.name),
            name: entry.name,
        })
        .collect()
}

pub struct ReplTool {
    provider: Arc<dyn CodeModeSessionProvider>,
    handle: tokio::sync::Mutex<Option<Arc<dyn ReplSessionHandle>>>,
    input_schema: JsonSchema,
}

impl ReplTool {
    /// The session's persistent context, created lazily on first use. A
    /// carrier that cannot serve the session fails here as an infrastructure
    /// error instead of silently switching carriers.
    async fn handle(&self) -> Result<Arc<dyn ReplSessionHandle>, String> {
        let mut slot = self.handle.lock().await;
        if let Some(handle) = slot.as_ref() {
            return Ok(handle.clone());
        }
        let handle = self
            .provider
            .create_repl_session()
            .await
            .map_err(|error| String::from_utf8_lossy(&error.to_node_utf8()).into_owned())?;
        *slot = Some(handle.clone());
        Ok(handle)
    }
}

/// Construct the `REPL` tool (one persistent JS context per tool instance,
/// i.e. per session toolset) on the carrier the provider selects.
pub fn create_repl_tool(provider: Arc<dyn CodeModeSessionProvider>) -> Arc<dyn Tool> {
    Arc::new(ReplTool {
        provider,
        handle: tokio::sync::Mutex::new(None),
        input_schema: repl_input_schema(),
    })
}

#[async_trait]
impl Tool for ReplTool {
    fn static_name(&self) -> &str {
        "REPL"
    }

    fn static_description(&self) -> &str {
        REPL_DESCRIPTION
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path
            .is_empty()
            .then_some(&["code", "description", "timeout"])
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("execute JavaScript with programmatic tool access")
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        Some(100_000)
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

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn check_permissions(
        &self,
        _input: &Json,
        _ctx: &ToolContext,
    ) -> Option<PermissionResult> {
        // The script itself is always allowed to run; every nested tool call
        // is permission-checked individually through `invoke_tool`.
        Some(PermissionResult::Allow {
            updated_input: None,
            reason: PermissionReason::ToolSelfCheck,
        })
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let code = input.get("code").and_then(RuntimeJson::as_string)?;
        code_summary(code)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let Some(code) = input.get("code").and_then(RuntimeJson::as_string).cloned() else {
            return Ok(text_result(
                RuntimeJson::Object(RuntimeObject::new()),
                "code is required for REPL",
                true,
            ));
        };
        let timeout_ms = input
            .get("timeout")
            .and_then(RuntimeJson::as_f64)
            .map(|timeout| {
                if timeout.is_finite() && timeout > 0.0 {
                    (timeout as u64).min(REPL_MAX_TIMEOUT_MS)
                } else {
                    0
                }
            })
            .unwrap_or(REPL_DEFAULT_TIMEOUT_MS);
        let handle = self.handle().await.map_err(ToolError::new)?;
        let outcome = handle
            .execute(
                ReplExecRequest {
                    code: code.clone(),
                    timeout_ms,
                    tools: tool_bindings(ctx),
                },
                ctx.invoke_tool.clone(),
                &ctx.signal,
            )
            .await
            .map_err(ToolError::new)?;
        let text = render_repl_text(&outcome);
        let is_error = outcome.error.is_some();
        Ok(text_result(repl_output(&code, &outcome), text, is_error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::tool::{
        BoxFuture, InvokeToolFn, InvokeToolResult, ToolCatalogEntry, ToolExposure,
    };
    use std::sync::Mutex;

    macro_rules! rjson {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    fn rs(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    fn outcome(
        result_defined: bool,
        result_text: &str,
        stdout: &str,
        stderr: &str,
        error: Option<&str>,
    ) -> ReplExecOutcome {
        ReplExecOutcome {
            result: RuntimeJson::Null,
            result_defined,
            result_text: rs(result_text),
            stdout: rs(stdout),
            stderr: rs(stderr),
            error: error.map(rs),
        }
    }

    fn recording_invoke(
        calls: std::sync::Arc<Mutex<Vec<(RuntimeString, RuntimeJson)>>>,
    ) -> InvokeToolFn {
        Arc::new(move |name: RuntimeString, arg: RuntimeJson| {
            let calls = calls.clone();
            Box::pin(async move {
                calls.lock().unwrap().push((name.clone(), arg.clone()));
                if name == "boom" {
                    return Ok(InvokeToolResult {
                        is_error: true,
                        output: None,
                        content: vec![OutputBlock::Text(crate::schemas::content::TextBlock {
                            text: rs("nested tool exploded"),
                            citations: None,
                            harness_envelope: None,
                            message_id: None,
                            phase: None,
                        })],
                    });
                }
                let mut output = RuntimeObject::new();
                output.insert("tool", RuntimeJson::String(name));
                output.insert("arg", arg);
                Ok(InvokeToolResult {
                    is_error: false,
                    output: None,
                    content: vec![OutputBlock::Text(crate::schemas::content::TextBlock {
                        text: runtime_json::stringify(&RuntimeJson::Object(output)),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })],
                })
            }) as BoxFuture<'static, Result<InvokeToolResult, ToolError>>
        })
    }

    fn repl_ctx(calls: std::sync::Arc<Mutex<Vec<(RuntimeString, RuntimeJson)>>>) -> ToolContext {
        let mut ctx = make_ctx(".");
        ctx.invoke_tool = Some(recording_invoke(calls));
        ctx.tool_catalog = Some(Box::new(|| {
            ["read-file", "boom", "REPL"]
                .into_iter()
                .map(|name| ToolCatalogEntry {
                    name: rs(name),
                    description: rs("d"),
                    search_hint: None,
                    exposure: ToolExposure::Direct,
                    spec: None,
                })
                .collect()
        }));
        ctx
    }

    fn browser_ctx(calls: std::sync::Arc<Mutex<Vec<(RuntimeString, RuntimeJson)>>>) -> ToolContext {
        let mut ctx = make_ctx(".");
        ctx.invoke_tool = Some(recording_invoke(calls));
        ctx.tool_catalog = Some(Box::new(|| {
            [
                "mcp__klaus-browser__ax_state",
                "mcp__klaus-browser__ax_action",
                "mcp__klaus-browser__page_text",
                "REPL",
            ]
            .into_iter()
            .map(|name| ToolCatalogEntry {
                name: rs(name),
                description: rs("d"),
                search_hint: None,
                exposure: ToolExposure::Direct,
                spec: None,
            })
            .collect()
        }));
        ctx
    }

    // ---- metadata + schema -----------------------------------------------------

    #[test]
    fn tool_exposes_expected_metadata_and_flags() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        assert_eq!(tool.name(), "REPL");
        assert_eq!(
            tool.search_hint(),
            Some(rs("execute JavaScript with programmatic tool access"))
        );
        assert_eq!(tool.max_result_size_chars(), Some(100_000));
        assert!(!tool.is_read_only(&rjson!({})));
        assert!(!tool.is_concurrency_safe(&rjson!({})));
        assert_eq!(tool.is_destructive(&rjson!({})), Some(false));
    }

    #[test]
    fn schema_matches_exactly() {
        let schema = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ))
        .input_schema();
        assert_eq!(
            schema,
            rjson!({
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "JavaScript code to execute. Supports top-level await. State persists across calls."
                    },
                    "description": {
                        "type": "string",
                        "description": "Clear, concise description of what this script does in active voice (5-10 words). E.g. \"Trace upgrade message to its GrowthBook flag\""
                    },
                    "timeout": {
                        "type": "number",
                        "description": "Optional timeout in milliseconds (default 30000, max 600000)"
                    }
                },
                "required": ["code"],
                "additionalProperties": false
            })
        );
    }

    #[tokio::test]
    async fn permission_is_always_allow() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = make_ctx(".");
        assert_eq!(
            tool.check_permissions(&rjson!({ "code": "1" }), &ctx).await,
            Some(PermissionResult::Allow {
                updated_input: None,
                reason: PermissionReason::ToolSelfCheck,
            })
        );
    }

    #[test]
    fn summarize_takes_the_first_line_bounded_at_50_units() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        assert_eq!(
            tool.summarize(&rjson!({ "code": "const x = 1\nmore" })),
            Some(rs("const x = 1"))
        );
        let long = format!("{}\nrest", "x".repeat(60));
        let mut input = RuntimeObject::new();
        input.insert("code", RuntimeJson::String(rs(&long)));
        let summary = tool.summarize(&RuntimeJson::Object(input)).unwrap();
        assert_eq!(summary.as_utf16().len(), 50);
        assert_eq!(summary, rs(&format!("{}\u{2026}", "x".repeat(49))));
        assert_eq!(tool.summarize(&rjson!({ "code": "" })), None);
    }

    // ---- text mapping ------------------------------------------------------------

    #[test]
    fn bare_result_renders_without_sections() {
        assert_eq!(
            render_repl_text(&outcome(true, "42", "", "", None)),
            rs("42")
        );
    }

    #[test]
    fn sections_render_in_reference_order() {
        assert_eq!(
            render_repl_text(&outcome(true, "null", "out", "warn", Some("boom"))),
            rs("stdout:\nout\n\nstderr:\nwarn\n\nerror: boom\n\nresult: null")
        );
        assert_eq!(
            render_repl_text(&outcome(false, "", "hi", "", None)),
            rs("stdout:\nhi")
        );
        // Undefined result with no output at all renders as an empty string.
        assert_eq!(render_repl_text(&outcome(false, "", "", "", None)), rs(""));
    }

    // ---- execution (requires a Node interpreter) ---------------------------------

    #[tokio::test]
    async fn state_persists_and_last_expression_is_returned() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = repl_ctx(Default::default());
        let first = tool
            .call(rjson!({ "code": "let counter = 41; counter" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(first.serializable.is_error, Some(false));
        assert_eq!(text_of(&first), "41");
        let second = tool
            .call(rjson!({ "code": "counter + 1" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(text_of(&second), "42");
        assert_eq!(
            second.serializable.output.get("result"),
            Some(&RuntimeJson::from(42.0))
        );
    }

    #[tokio::test]
    async fn top_level_await_and_console_capture_work() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = repl_ctx(Default::default());
        let res = tool
            .call(
                rjson!({ "code": "console.log('working'); console.error('careful'); await Promise.resolve(7)" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            text_of(&res),
            "stdout:\nworking\n\nstderr:\ncareful\n\nresult: 7"
        );
        assert_eq!(res.serializable.is_error, Some(false));
    }

    #[tokio::test]
    async fn errors_are_reported_with_is_error() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = repl_ctx(Default::default());
        let res = tool
            .call(rjson!({ "code": "throw new Error('kaboom')" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        let text = String::from_utf8(text_of(&res).to_node_utf8()).unwrap();
        assert!(text.contains("error: Error: kaboom"), "{text}");
        assert!(text.contains("result: null"), "{text}");
    }

    #[tokio::test]
    async fn tools_are_callable_and_route_through_invoke_tool() {
        let calls: std::sync::Arc<Mutex<Vec<(RuntimeString, RuntimeJson)>>> = Default::default();
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = repl_ctx(calls.clone());
        let res = tool
            .call(
                rjson!({ "code": "const r = await read_file({ path: 'x' }); r.tool" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&res), "read-file");
        let recorded = calls.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, rs("read-file"));
        assert_eq!(recorded[0].1, rjson!({ "path": "x" }));
    }

    // ---- browser prelude ---------------------------------------------------------

    #[tokio::test]
    async fn browser_prelude_routes_helpers_to_bound_host_tools() {
        let calls: std::sync::Arc<Mutex<Vec<(RuntimeString, RuntimeJson)>>> = Default::default();
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = browser_ctx(calls.clone());
        let res = tool
            .call(
                rjson!({ "code": "const r = await browser.getState(); r.tool" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        let text = String::from_utf8(text_of(&res).to_node_utf8()).unwrap();
        assert!(
            text.contains("mcp__klaus-browser__ax_state"),
            "the helper routes to the bound host tool: {text}"
        );
        assert!(
            text.contains("browser.docs()"),
            "first use prints the API pointer: {text}"
        );
        let recorded = calls.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, rs("mcp__klaus-browser__ax_state"));
        assert_eq!(recorded[0].1, rjson!({}));
    }

    #[tokio::test]
    async fn browser_tab_handles_inject_tab_id_and_map_ax_actions() {
        let calls: std::sync::Arc<Mutex<Vec<(RuntimeString, RuntimeJson)>>> = Default::default();
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = browser_ctx(calls.clone());
        let res = tool
            .call(
                rjson!({ "code": "await browser.getTab(7).ax.click(3, { mouseButton: 'right' }); 'done'" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        let recorded = calls.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, rs("mcp__klaus-browser__ax_action"));
        assert_eq!(
            recorded[0].1,
            rjson!({ "action": "click", "elementIndex": 3, "mouseButton": "right", "tabId": 7 })
        );
    }

    #[tokio::test]
    async fn browser_helpers_refuse_clearly_when_host_tools_are_not_bound() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = repl_ctx(Default::default());
        let res = tool
            .call(rjson!({ "code": "await browser.getState()" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        let text = String::from_utf8(text_of(&res).to_node_utf8()).unwrap();
        assert!(
            text.contains("not bound in this REPL session"),
            "the refusal names the recovery path: {text}"
        );
    }

    #[tokio::test]
    async fn browser_docs_cover_the_ax_workflow_and_the_note_prints_once() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = browser_ctx(Default::default());
        let docs = tool
            .call(rjson!({ "code": "browser.docs()" }), &ctx, None)
            .await
            .unwrap();
        let text = String::from_utf8(text_of(&docs).to_node_utf8()).unwrap();
        assert!(text.contains("tab.ax.click(index"), "{text}");
        assert!(
            text.contains("call getState() again"),
            "the docs teach index re-derivation: {text}"
        );
        let second = tool
            .call(
                rjson!({ "code": "await browser.getState(); await browser.getState(); 'ok'" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8(text_of(&second).to_node_utf8()).unwrap(),
            "ok",
            "the first-use note does not repeat on later execs"
        );
    }

    #[tokio::test]
    async fn nested_tool_errors_are_aggregated_into_the_error_text() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = repl_ctx(Default::default());
        let res = tool
            .call(rjson!({ "code": "await boom({})" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        let text = String::from_utf8(text_of(&res).to_node_utf8()).unwrap();
        assert!(
            text.contains("Inner tool errors (likely root cause):\n- boom: nested tool exploded"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn pending_tool_calls_at_script_end_warn_on_stderr() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = repl_ctx(Default::default());
        let res = tool
            .call(
                rjson!({ "code": "read_file({ path: 'x' }); 'done'" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let text = String::from_utf8(text_of(&res).to_node_utf8()).unwrap();
        assert!(
            text.contains(
                "\u{26a0} 1 tool call(s) still pending at script end \u{2014} results discarded: read-file. Add 'await'."
            ),
            "{text}"
        );
    }

    #[tokio::test]
    async fn async_timeout_returns_the_script_time_message_and_keeps_the_session() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = repl_ctx(Default::default());
        // Seed state, then hit an unbounded await with a tiny budget.
        tool.call(rjson!({ "code": "let kept = 'alive'" }), &ctx, None)
            .await
            .unwrap();
        let res = tool
            .call(
                rjson!({ "code": "await new Promise(() => {})", "timeout": 200 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        let text = String::from_utf8(text_of(&res).to_node_utf8()).unwrap();
        assert!(
            text.contains(
                "REPL execution timed out after 200ms of script time (inner tool calls excluded)."
            ),
            "{text}"
        );
        // The context survived the async timeout.
        let after = tool
            .call(rjson!({ "code": "kept" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(text_of(&after), "alive");
    }

    #[tokio::test]
    async fn script_budget_pauses_while_nested_tool_calls_are_in_flight() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let calls: std::sync::Arc<Mutex<Vec<(RuntimeString, RuntimeJson)>>> = Default::default();
        let mut ctx = repl_ctx(calls.clone());
        // The nested call (900ms) outlives the script budget (500ms): tool
        // time must not be charged against the budget, so the exec succeeds.
        let slow_calls = calls.clone();
        ctx.invoke_tool = Some(Arc::new(move |name: RuntimeString, arg: RuntimeJson| {
            let calls = slow_calls.clone();
            Box::pin(async move {
                calls.lock().unwrap().push((name.clone(), arg.clone()));
                tokio::time::sleep(std::time::Duration::from_millis(900)).await;
                let mut output = RuntimeObject::new();
                output.insert("tool", RuntimeJson::String(name));
                Ok(InvokeToolResult {
                    is_error: false,
                    output: None,
                    content: vec![OutputBlock::Text(crate::schemas::content::TextBlock {
                        text: runtime_json::stringify(&RuntimeJson::Object(output)),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })],
                })
            }) as BoxFuture<'static, Result<InvokeToolResult, ToolError>>
        }));
        let res = tool
            .call(
                rjson!({ "code": "const r = await read_file({ path: 'x' }); r.tool", "timeout": 500 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(text_of(&res), "read-file");
        let recorded = calls.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, rs("read-file"));
    }

    #[tokio::test]
    async fn sync_hang_is_interrupted_and_the_context_survives() {
        let tool = ReplTool {
            provider: Arc::new(crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider),
            handle: tokio::sync::Mutex::new(None),
            input_schema: repl_input_schema(),
        };
        let ctx = repl_ctx(Default::default());
        let before = tool
            .call(rjson!({ "code": "const kept = 7; kept" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(text_of(&before), "7");
        let res = tool
            .call(rjson!({ "code": "for(;;) {}", "timeout": 200 }), &ctx, None)
            .await
            .unwrap();
        // The isolate watchdog interrupts the synchronous loop and the exec
        // surfaces the script-time budget message.
        assert_eq!(res.serializable.is_error, Some(true));
        let text = String::from_utf8(text_of(&res).to_node_utf8()).unwrap();
        assert!(
            text.contains("REPL execution timed out after 200ms of script time"),
            "{text}"
        );
        // The persistent context survives the watchdog: state declared before
        // the hang is still visible and later calls evaluate normally.
        let after = tool
            .call(rjson!({ "code": "kept + 1" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(text_of(&after), "8");
    }

    #[tokio::test]
    async fn abort_signal_interrupts_execution() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = repl_ctx(Default::default());
        ctx.signal.cancel();
        let res = tool
            .call(
                rjson!({ "code": "await new Promise(() => {})" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        let text = String::from_utf8(text_of(&res).to_node_utf8()).unwrap();
        assert!(text.contains("REPL execution interrupted"), "{text}");
    }

    #[tokio::test]
    async fn top_level_await_persists_declarations_and_returns_last_expression() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = repl_ctx(Default::default());
        let first = tool
            .call(
                rjson!({ "code": "const base = await Promise.resolve(40); base + 2" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&first), "42");
        let second = tool
            .call(rjson!({ "code": "base" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(text_of(&second), "40");
    }

    #[tokio::test]
    async fn console_capture_splits_stdout_and_stderr() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = repl_ctx(Default::default());
        let res = tool
            .call(
                rjson!({ "code": "console.log('hi', 2); console.error('bad'); 1" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let text = String::from_utf8(text_of(&res).to_node_utf8()).unwrap();
        assert!(text.contains("stdout:\nhi 2"), "{text}");
        assert!(text.contains("stderr:\nbad"), "{text}");
        assert!(text.contains("result: 1"), "{text}");
    }

    #[tokio::test]
    async fn result_formatting_matches_the_inspect_contract() {
        let tool = create_repl_tool(Arc::new(
            crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider,
        ));
        let ctx = repl_ctx(Default::default());
        let nested = tool
            .call(rjson!({ "code": "({ a: { b: [1, 2] } })" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(text_of(&nested), "{ a: { b: [ 1, 2 ] } }");
        let plain = tool
            .call(rjson!({ "code": "({ only: 'strings' })" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(text_of(&plain), "only:\nstrings");
        let cycle = tool
            .call(
                rjson!({ "code": "const o = {}; o.self = o; o" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let text = String::from_utf8(text_of(&cycle).to_node_utf8()).unwrap();
        assert!(text.contains("[Circular *1]"), "{text}");
    }
}
