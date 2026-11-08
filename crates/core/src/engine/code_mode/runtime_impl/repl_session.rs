//! Persistent REPL session runtime on the in-process V8 carrier.
//!
//! The `REPL` tool evaluates model-authored JavaScript with state that
//! persists across calls in one session-scoped V8 context: top-level `await`,
//! `let`/`const` persistence, and last-expression results. Sources without
//! top-level await run as plain scripts (the V8 completion value is the last
//! expression and global lexical declarations persist in the context);
//! sources with top-level await are transformed by the vendored parser into
//! an async wrapper whose declarations are hoisted onto the context global
//! once the wrapper settles. Every other tool is exposed inside the context
//! as an async function routed through the caller's `invoke_tool` bridge, so
//! permissions and hooks still apply to nested calls.
//!
//! ## Timeouts
//!
//! The caller enforces a script-time budget (default 30s, capped 600s) that
//! pauses while nested tool calls are in flight, plus a hard 600s wall clock
//! and abort-signal interruption. Expiry terminates the running evaluation
//! through the isolate watchdog; the persistent context survives, captured
//! console output is preserved, and later calls continue in the same context.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::time::Duration;

use futures::future::{BoxFuture, Either};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::runtime::Handle;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::Mutex as TokioMutex;
use tokio_util::sync::CancellationToken;

use crate::engine::code_mode::protocol::{CodeModeToolKind, NestedToolMetadata};
use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimeString};
use crate::schemas::content::OutputBlock;
use crate::types::errors::{CodeModeToolRejection, ToolError};
use crate::types::tool::{InvokeToolFn, InvokeToolResult};
use crate::v8_platform::initialize_v8;

use super::isolate::value::{
    json_to_v8, runtime_string_to_v8, v8_value_to_json, v8_value_to_runtime_string,
    value_to_error_text,
};

/// Default script-time budget when the model omits `timeout`.
pub(crate) const REPL_DEFAULT_TIMEOUT_MS: u64 = 30_000;
/// Upper clamp for the script-time budget AND the hard wall-clock limit.
pub(crate) const REPL_MAX_TIMEOUT_MS: u64 = 600_000;

const INTERRUPTED_MESSAGE: &str = "REPL execution interrupted";
pub(crate) const BRIDGE_UNAVAILABLE: &str = "REPL tool bridge is unavailable";

fn timeout_message(budget_ms: u64) -> String {
    format!(
        "REPL execution timed out after {budget_ms}ms of script time (inner tool calls excluded). \
Script may still be running \u{2014} avoid unbounded awaits."
    )
}

fn hard_limit_message() -> String {
    format!(
        "REPL execution exceeded hard wall-clock limit of {REPL_MAX_TIMEOUT_MS}ms. An inner tool \
call may be hung \u{2014} try a shorter timeout on the tool itself, or split the work."
    )
}

// Vendored parser: acorn 8.16.0 ESM distribution bundle, byte-identical to
// the official dist (MIT License; full text in vendor/LICENSE-acorn). Used
// only by the top-level-await transform to parse model-authored sources.
const ACORN_SOURCE: &str = include_str!("vendor/acorn.js");
// Project-authored companions: an inspect-equivalent result formatter and
// the top-level-await transform built on the vendored parser.
const FORMAT_SOURCE: &str = include_str!("vendor/format.js");
const TRANSFORM_SOURCE: &str = include_str!("vendor/transform.js");
// Project-authored companion: a browser control surface layered over the
// bound host browser tools. Its helpers resolve the target function at call
// time, so per-exec binding changes are picked up without a reinstall and
// sessions without browser tools pay nothing beyond one small script.
const BROWSER_PRELUDE_SOURCE: &str = include_str!("vendor/browser-prelude.js");
// Project-authored companion: a unified computer-use surface layering the
// bound native app tools and the browser channel into one hierarchical API
// (cross-surface inventory, app and desktop handles, tab handles with mention
// resolution). Binding lookup and failure semantics match the browser
// prelude: helpers resolve their target at call time and fail with an
// actionable message instead of a TypeError.
const COMPUTER_PRELUDE_SOURCE: &str = include_str!("vendor/computer-prelude.js");

/// Captured console, the result formatter entry point and the context seal.
const REPL_BOOTSTRAP_SOURCE: &str = r#"
(function () {
  for (const name of ['Atomics', 'SharedArrayBuffer', 'WebAssembly']) {
    try { delete globalThis[name]; } catch {}
  }
  const capture = { out: [], err: [] };
  function line(stream, args) {
    stream.push(Array.prototype.map.call(args, function (a) {
      return typeof a === 'string' ? a : __klausFormat(a);
    }).join(' '));
  }
  const consoleObject = {
    log: function () { line(capture.out, arguments); },
    info: function () { line(capture.out, arguments); },
    debug: function () { line(capture.out, arguments); },
    error: function () { line(capture.err, arguments); },
    warn: function () { line(capture.err, arguments); },
    trace: function () { line(capture.err, arguments); },
  };
  function defineHidden(name, value) {
    Object.defineProperty(globalThis, name, {
      value: value, configurable: true, writable: true, enumerable: false,
    });
  }
  defineHidden('console', consoleObject);
  defineHidden('__klausCapture', capture);
  defineHidden('__klausResetCapture', function () {
    capture.out.length = 0;
    capture.err.length = 0;
  });
  const RESERVED = new Set(['stdout', 'stderr', 'error', 'result']);
  function plainStringObject(value) {
    try {
      if (value === null || typeof value !== 'object' || Array.isArray(value)
        || value.constructor?.name !== 'Object') return undefined;
      const entries = Object.entries(value);
      if (entries.length === 0
        || entries.some(([k, v]) => typeof v !== 'string' || RESERVED.has(k))) return undefined;
      return entries.map(([k, v]) => `${k}:\n${v}`).join('\n\n');
    } catch { return undefined; }
  }
  defineHidden('__klausFormatResult', function (value) {
    if (typeof value === 'string' && value.trim() !== '') return value;
    const special = plainStringObject(value);
    return special === undefined ? __klausFormat(value) : special;
  });
})();
"#;

/// One tool exposed inside the REPL context: `key` is the identifier-safe
/// global name, `name` the routing name for the `invoke_tool` bridge.
pub struct ReplToolBinding {
    pub key: RuntimeString,
    pub name: RuntimeString,
}

/// One requested evaluation: the script source, its script-time budget, and
/// the tool bindings to expose inside the context.
pub struct ReplExecRequest {
    pub code: RuntimeString,
    pub timeout_ms: u64,
    pub tools: Vec<ReplToolBinding>,
}

/// The settled outcome of one evaluation.
pub struct ReplExecOutcome {
    pub result: RuntimeJson,
    pub result_defined: bool,
    /// Inspect-equivalent formatted completion value.
    pub result_text: RuntimeString,
    pub stdout: RuntimeString,
    pub stderr: RuntimeString,
    pub error: Option<RuntimeString>,
}

enum ReplCommand {
    ToolResponse {
        id: u64,
        result: RuntimeJson,
    },
    ToolError {
        id: u64,
        rejection: CodeModeToolRejection,
    },
    TimeoutFired {
        id: u64,
    },
    /// Carries the exec id so a terminate aimed at an already-finished exec
    /// cannot leak into the next one.
    Terminate {
        exec_id: u64,
    },
}

enum ReplEvent {
    ToolCall {
        id: u64,
        name: RuntimeString,
        input: RuntimeJson,
    },
    Done(ReplDone),
    ThreadPanicked,
}

struct ReplDone {
    stdout: RuntimeString,
    stderr: RuntimeString,
    result: RuntimeJson,
    result_defined: bool,
    result_text: RuntimeString,
    error: Option<RuntimeString>,
    terminated: bool,
}

struct ReplEvalRequest {
    source: RuntimeString,
    tools: Vec<NestedToolMetadata>,
    exec_id: u64,
}

struct ScheduledTimeout {
    callback: v8::Global<v8::Function>,
    token: CancellationToken,
    interval_ms: Option<u64>,
}

struct ReplRuntimeState {
    command_tx: std_mpsc::Sender<ReplCommand>,
    event_tx: tokio_mpsc::UnboundedSender<ReplEvent>,
    pending_tool_calls: HashMap<u64, v8::Global<v8::PromiseResolver>>,
    pending_tool_order: Vec<(u64, RuntimeString)>,
    /// Tool calls that rejected during the current eval, in call order:
    /// aggregated into the error text as the likely root cause.
    failed_tool_calls: Vec<(RuntimeString, RuntimeString)>,
    pending_timeouts: HashMap<u64, ScheduledTimeout>,
    enabled_tools: Vec<NestedToolMetadata>,
    next_tool_call_id: u64,
    next_timeout_id: u64,
    timer_handle: Handle,
}

// ---- runtime thread ---------------------------------------------------------

fn run_repl_runtime(
    eval_rx: std_mpsc::Receiver<ReplEvalRequest>,
    event_tx: tokio_mpsc::UnboundedSender<ReplEvent>,
    command_tx: std_mpsc::Sender<ReplCommand>,
    command_rx: std_mpsc::Receiver<ReplCommand>,
    timer_handle: Handle,
    isolate_handle_tx: std_mpsc::SyncSender<v8::IsolateHandle>,
) {
    let isolate = &mut v8::Isolate::new(v8::CreateParams::default());
    let isolate_handle = isolate.thread_safe_handle();
    if isolate_handle_tx.send(isolate_handle).is_err() {
        return;
    }
    v8::scope!(let scope, isolate);
    let context = v8::Context::new(scope, Default::default());
    let scope = &mut v8::ContextScope::new(scope, context);
    scope.set_slot(ReplRuntimeState {
        command_tx,
        event_tx: event_tx.clone(),
        pending_tool_calls: HashMap::new(),
        pending_tool_order: Vec::new(),
        failed_tool_calls: Vec::new(),
        pending_timeouts: HashMap::new(),
        enabled_tools: Vec::new(),
        next_tool_call_id: 1,
        next_timeout_id: 1,
        timer_handle,
    });

    // A terminate racing with startup would otherwise kill the fresh
    // session. The termination flag can be consumed by the failing call
    // itself, so retry unconditionally a bounded number of times: a
    // genuine bootstrap failure repeats and is then reported, while a
    // one-shot terminate lets the retry succeed so the queued eval can
    // report the interruption through the normal terminated path.
    let mut bootstrap_attempts = 0;
    loop {
        match bootstrap(scope) {
            Ok(()) => break,
            Err(error) => {
                scope.cancel_terminate_execution();
                if bootstrap_attempts < 3 {
                    bootstrap_attempts += 1;
                    continue;
                }
                send_done(
                    &event_tx,
                    ReplDone {
                        stdout: RuntimeString::default(),
                        stderr: RuntimeString::default(),
                        result: RuntimeJson::Null,
                        result_defined: true,
                        result_text: RuntimeString::from("null"),
                        error: Some(error),
                        terminated: false,
                    },
                );
                return;
            }
        }
    }

    while let Ok(request) = eval_rx.recv() {
        run_eval(scope, &command_rx, &event_tx, request);
        scope.cancel_terminate_execution();
        // Drop commands that arrived after the eval settled so a stale
        // terminate or tool response cannot leak into the next call.
        while command_rx.try_recv().is_ok() {}
    }
}

fn bootstrap(scope: &mut v8::PinScope<'_, '_>) -> Result<(), RuntimeString> {
    eval_acorn(scope)?;
    for source in [
        FORMAT_SOURCE,
        TRANSFORM_SOURCE,
        REPL_BOOTSTRAP_SOURCE,
        BROWSER_PRELUDE_SOURCE,
        COMPUTER_PRELUDE_SOURCE,
    ] {
        eval_plain_script(scope, source)?;
    }
    install_timers(scope)?;
    Ok(())
}

/// Evaluate the vendored parser as a module and publish its `parse` entry
/// point for the transform bootstrap (the parser ships as an ES module).
fn eval_acorn(scope: &mut v8::PinScope<'_, '_>) -> Result<(), RuntimeString> {
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();
    let source = runtime_string_to_v8(&tc, &RuntimeString::from(ACORN_SOURCE))
        .ok_or_else(|| RuntimeString::from("failed to allocate parser source"))?;
    // Module compilation requires a script origin flagged as a module.
    let resource_name = v8::String::new(&tc, "klaus_repl_parser.mjs")
        .ok_or_else(|| RuntimeString::from("failed to allocate script origin"))?;
    let origin = v8::ScriptOrigin::new(
        &tc,
        resource_name.into(),
        0,
        0,
        true,
        0,
        None,
        true,
        false,
        true,
        None,
    );
    let mut module_source = v8::script_compiler::Source::new(source, Some(&origin));
    let module = v8::script_compiler::compile_module(&tc, &mut module_source)
        .ok_or_else(|| RuntimeString::from("failed to compile the vendored parser"))?;
    fn resolve<'s>(
        _context: v8::Local<'s, v8::Context>,
        _specifier: v8::Local<'s, v8::String>,
        _import_attributes: v8::Local<'s, v8::FixedArray>,
        _referrer: v8::Local<'s, v8::Module>,
    ) -> Option<v8::Local<'s, v8::Module>> {
        None
    }
    if module.instantiate_module(&tc, resolve) != Some(true) {
        return Err(RuntimeString::from(
            "failed to instantiate the vendored parser",
        ));
    }
    module
        .evaluate(&tc)
        .ok_or_else(|| RuntimeString::from("failed to evaluate the vendored parser"))?;
    tc.perform_microtask_checkpoint();
    let namespace = module.get_module_namespace();
    let namespace = v8::Local::<v8::Object>::try_from(namespace)
        .map_err(|_| RuntimeString::from("parser namespace is not an object"))?;
    let parse_key = v8::String::new(&tc, "parse")
        .ok_or_else(|| RuntimeString::from("failed to allocate parser export name"))?;
    let parse = namespace
        .get(&tc, parse_key.into())
        .ok_or_else(|| RuntimeString::from("parser export missing"))?;
    let global = tc.get_current_context().global(&tc);
    let key = v8::String::new(&tc, "__acornParse")
        .ok_or_else(|| RuntimeString::from("failed to allocate parser global name"))?;
    if global.set(&tc, key.into(), parse) != Some(true) {
        return Err(RuntimeString::from(
            "failed to publish the parser entry point",
        ));
    }
    Ok(())
}

fn eval_plain_script(scope: &mut v8::PinScope<'_, '_>, source: &str) -> Result<(), RuntimeString> {
    let source = runtime_string_to_v8(scope, &RuntimeString::from(source))
        .ok_or_else(|| RuntimeString::from("failed to allocate bootstrap source"))?;
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();
    let script = v8::Script::compile(&tc, source, None)
        .ok_or_else(|| RuntimeString::from("failed to compile bootstrap source"))?;
    if script.run(&tc).is_none() || tc.has_caught() {
        return Err(tc
            .exception()
            .map(|exception| value_to_error_text(&mut tc, exception))
            .unwrap_or_else(|| RuntimeString::from("bootstrap evaluation failed")));
    }
    Ok(())
}

struct TlaPlan {
    wrapped: RuntimeString,
    decl_names: Vec<RuntimeString>,
}

fn run_eval(
    scope: &mut v8::PinScope<'_, '_>,
    command_rx: &std_mpsc::Receiver<ReplCommand>,
    event_tx: &tokio_mpsc::UnboundedSender<ReplEvent>,
    request: ReplEvalRequest,
) {
    // A terminate for THIS exec that arrived before the eval started must
    // interrupt it deterministically; stale commands are dropped.
    let mut pre_terminated = false;
    while let Ok(command) = command_rx.try_recv() {
        if matches!(command, ReplCommand::Terminate { exec_id } if exec_id == request.exec_id) {
            pre_terminated = true;
        }
    }
    if pre_terminated {
        call_void_global(scope, "__klausResetCapture");
        send_done(event_tx, terminated_done(scope));
        return;
    }
    call_void_global(scope, "__klausResetCapture");
    if let Some(state) = scope.get_slot_mut::<ReplRuntimeState>() {
        for (_, timeout) in state.pending_timeouts.drain() {
            timeout.token.cancel();
        }
        state.pending_tool_calls.clear();
        state.pending_tool_order.clear();
        state.failed_tool_calls.clear();
        state.enabled_tools = request.tools.clone();
    }
    if let Err(error) = install_tools(scope) {
        send_done(event_tx, error_done(error));
        return;
    }
    let Some(source) = runtime_string_to_v8(scope, &request.source) else {
        send_done(
            event_tx,
            error_done(RuntimeString::from("failed to allocate REPL source")),
        );
        return;
    };

    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();

    let mut plan: Option<TlaPlan> = None;
    let script = match v8::Script::compile(&tc, source, None) {
        Some(script) => script,
        None => {
            let original = tc
                .exception()
                .map(|exception| v8::Global::new(&tc, exception));
            // Clear the stale compile error so the transformed script runs
            // with a clean catch state; the original stays available for the
            // fallback error report.
            tc.reset();
            match transform_source(&mut tc, &request.source) {
                Ok(Some(transformed)) => {
                    let Some(wrapped) = runtime_string_to_v8(&tc, &transformed.wrapped) else {
                        send_done(event_tx, error_done_via(&mut tc, original));
                        return;
                    };
                    match v8::Script::compile(&tc, wrapped, None) {
                        Some(script) => {
                            plan = Some(transformed);
                            script
                        }
                        None => {
                            send_done(event_tx, error_done_via(&mut tc, original));
                            return;
                        }
                    }
                }
                _ => {
                    send_done(event_tx, error_done_via(&mut tc, original));
                    return;
                }
            }
        }
    };

    let completion = script.run(&tc);
    if tc.has_terminated() {
        send_done(event_tx, terminated_done(&mut tc));
        return;
    }
    if tc.has_caught() {
        let exception = tc
            .exception()
            .map(|exception| v8::Global::new(&tc, exception));
        send_done(event_tx, error_done_via(&mut tc, exception));
        return;
    }
    let mut completion = completion.unwrap_or_else(|| v8::undefined(&tc).into());

    let mut terminated = false;
    if let Ok(promise) = v8::Local::<v8::Promise>::try_from(completion) {
        let promise = v8::Global::new(&tc, promise);
        loop {
            tc.perform_microtask_checkpoint();
            if tc.has_terminated() {
                terminated = true;
                break;
            }
            let state = v8::Local::new(&tc, &promise).state();
            if state != v8::PromiseState::Pending {
                break;
            }
            match command_rx.recv() {
                Ok(ReplCommand::ToolResponse { id, result }) => {
                    resolve_pending(&mut tc, id, Ok(result));
                }
                Ok(ReplCommand::ToolError { id, rejection }) => {
                    resolve_pending(&mut tc, id, Err(rejection));
                }
                Ok(ReplCommand::TimeoutFired { id }) => invoke_timeout(&mut tc, id),
                Ok(ReplCommand::Terminate { exec_id }) => {
                    if exec_id == request.exec_id {
                        tc.thread_safe_handle().terminate_execution();
                        terminated = true;
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        if !terminated {
            let local = v8::Local::new(&tc, &promise);
            match local.state() {
                v8::PromiseState::Fulfilled => completion = local.result(&tc),
                v8::PromiseState::Rejected => {
                    let rejected_with = local.result(&tc);
                    let reason = v8::Global::new(&tc, rejected_with);
                    send_done(event_tx, error_done_via(&mut tc, Some(reason)));
                    return;
                }
                v8::PromiseState::Pending => {}
            }
        }
    }
    if terminated {
        send_done(event_tx, terminated_done(&mut tc));
        return;
    }

    let mut result_value = completion;
    if let Some(plan) = plan {
        if let Ok(pack) = v8::Local::<v8::Object>::try_from(completion) {
            result_value =
                get_field(&mut tc, pack, "last").unwrap_or_else(|| v8::undefined(&tc).into());
            if let Some(decls) = get_field(&mut tc, pack, "decls")
                .and_then(|value| v8::Local::<v8::Object>::try_from(value).ok())
            {
                let global = tc.get_current_context().global(&tc);
                for name in &plan.decl_names {
                    let (Some(key), Some(value)) = (
                        runtime_string_to_v8(&tc, name),
                        get_field_rs(&mut tc, decls, name),
                    ) else {
                        continue;
                    };
                    let _ = global.set(&tc, key.into(), value);
                }
            }
        }
    }

    let pending_names = take_pending_tool_names(&mut tc);
    let mut stderr = read_capture(&mut tc, "err");
    if !pending_names.is_empty() {
        let warning = RuntimeString::from(format!(
            "\u{26A0} {} tool call(s) still pending at script end \u{2014} results discarded: {}. Add 'await'.",
            pending_names.len(),
            pending_names.join(", ")
        ));
        stderr = if stderr.as_utf16().is_empty() {
            warning
        } else {
            stderr.concat(&RuntimeString::from("\n")).concat(&warning)
        };
    }
    let stdout = read_capture(&mut tc, "out");
    if let Some(state) = tc.get_slot_mut::<ReplRuntimeState>() {
        for (_, timeout) in state.pending_timeouts.drain() {
            timeout.token.cancel();
        }
    }
    let result_text = format_result(&mut tc, result_value);
    let result_defined = !result_value.is_undefined();
    let result = v8_value_to_json(&mut tc, result_value)
        .unwrap_or(None)
        .unwrap_or(RuntimeJson::Null);
    send_done(
        event_tx,
        ReplDone {
            stdout,
            stderr,
            result,
            result_defined,
            result_text,
            error: None,
            terminated: false,
        },
    );
}

// ---- isolate helpers --------------------------------------------------------

fn transform_source(
    scope: &mut v8::PinScope<'_, '_>,
    source: &RuntimeString,
) -> Result<Option<TlaPlan>, ()> {
    let global = scope.get_current_context().global(scope);
    let key = v8::String::new(scope, "__klausTransform").ok_or(())?;
    let transform = global.get(scope, key.into()).ok_or(())?;
    let transform = v8::Local::<v8::Function>::try_from(transform).map_err(|_| ())?;
    let source = runtime_string_to_v8(scope, source).ok_or(())?;
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();
    let value = transform
        .call(&tc, global.into(), &[source.into()])
        .ok_or(())?;
    if tc.has_caught() {
        return Ok(None);
    }
    let pack = v8::Local::<v8::Object>::try_from(value).map_err(|_| ())?;
    let wrapped = get_field(&mut tc, pack, "wrapped")
        .and_then(|value| v8::Local::<v8::String>::try_from(value).ok())
        .and_then(|value| v8_value_to_runtime_string(&tc, value.into()).ok())
        .ok_or(())?;
    let mut decl_names = Vec::new();
    if let Some(names) = get_field(&mut tc, pack, "declNames")
        .and_then(|value| v8::Local::<v8::Array>::try_from(value).ok())
    {
        for index in 0..names.length() {
            if let Some(name) = names.get_index(&tc, index) {
                if let Ok(name) = v8_value_to_runtime_string(&tc, name) {
                    decl_names.push(name);
                }
            }
        }
    }
    Ok(Some(TlaPlan {
        wrapped,
        decl_names,
    }))
}

fn get_field<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    key: &str,
) -> Option<v8::Local<'s, v8::Value>> {
    let key = v8::String::new(scope, key)?;
    object.get(scope, key.into())
}

fn get_field_rs<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    key: &RuntimeString,
) -> Option<v8::Local<'s, v8::Value>> {
    let key = runtime_string_to_v8(scope, key)?;
    object.get(scope, key.into())
}

fn call_void_global(scope: &mut v8::PinScope<'_, '_>, name: &str) {
    let global = scope.get_current_context().global(scope);
    let Some(key) = v8::String::new(scope, name) else {
        return;
    };
    let Some(value) = global.get(scope, key.into()) else {
        return;
    };
    let Ok(function) = v8::Local::<v8::Function>::try_from(value) else {
        return;
    };
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let tc = tc.init();
    let _ = function.call(&tc, global.into(), &[]);
}

fn call_formatter(
    scope: &mut v8::PinScope<'_, '_>,
    name: &str,
    value: v8::Local<v8::Value>,
) -> Option<RuntimeString> {
    let global = scope.get_current_context().global(scope);
    let key = v8::String::new(scope, name)?;
    let function = global.get(scope, key.into())?;
    let function = v8::Local::<v8::Function>::try_from(function).ok()?;
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let tc = tc.init();
    let result = function.call(&tc, global.into(), &[value])?;
    v8_value_to_runtime_string(&tc, result).ok()
}

fn format_result(scope: &mut v8::PinScope<'_, '_>, value: v8::Local<v8::Value>) -> RuntimeString {
    call_formatter(scope, "__klausFormatResult", value)
        .unwrap_or_else(|| v8_value_to_runtime_string(scope, value).unwrap_or_default())
}

fn format_error(
    scope: &mut v8::PinScope<'_, '_>,
    exception: v8::Global<v8::Value>,
) -> RuntimeString {
    let local = v8::Local::new(scope, &exception);
    call_formatter(scope, "__klausFormatError", local)
        .unwrap_or_else(|| value_to_error_text(scope, local))
}

fn read_capture(scope: &mut v8::PinScope<'_, '_>, key: &str) -> RuntimeString {
    let global = scope.get_current_context().global(scope);
    let Some(capture_key) = v8::String::new(scope, "__klausCapture") else {
        return RuntimeString::default();
    };
    let Some(capture) = global.get(scope, capture_key.into()) else {
        return RuntimeString::default();
    };
    let Ok(capture) = v8::Local::<v8::Object>::try_from(capture) else {
        return RuntimeString::default();
    };
    let Some(stream) = get_field(scope, capture, key) else {
        return RuntimeString::default();
    };
    let Ok(lines) = v8::Local::<v8::Array>::try_from(stream) else {
        return RuntimeString::default();
    };
    let mut text = RuntimeString::default();
    for index in 0..lines.length() {
        let Some(line) = lines.get_index(scope, index) else {
            continue;
        };
        let Ok(line) = v8_value_to_runtime_string(scope, line) else {
            continue;
        };
        if index > 0 {
            text = text.concat(&RuntimeString::from("\n"));
        }
        text = text.concat(&line);
    }
    text
}

fn install_tools(scope: &mut v8::PinScope<'_, '_>) -> Result<(), RuntimeString> {
    let enabled_tools = scope
        .get_slot::<ReplRuntimeState>()
        .map(|state| state.enabled_tools.clone())
        .unwrap_or_default();
    // Tools are bare context globals under their identifier-safe key, so a
    // script calls `read_file({...})` directly.
    let global = scope.get_current_context().global(scope);
    for (index, tool) in enabled_tools.iter().enumerate() {
        let name = runtime_string_to_v8(scope, &tool.global_name)
            .ok_or_else(|| RuntimeString::from("failed to allocate tool name"))?;
        let data = v8::String::new(scope, &index.to_string())
            .ok_or_else(|| RuntimeString::from("failed to allocate tool callback data"))?;
        let template = v8::FunctionTemplate::builder(repl_tool_callback)
            .data(data.into())
            .build(scope);
        let function = template
            .get_function(scope)
            .ok_or_else(|| RuntimeString::from("failed to create tool function"))?;
        if global.set(scope, name.into(), function.into()) != Some(true) {
            return Err(RuntimeString::from("failed to install tool global"));
        }
    }
    Ok(())
}

fn repl_tool_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let tool_index = match v8_value_to_runtime_string(scope, args.data())
        .ok()
        .and_then(|value| value.try_to_utf8().ok())
        .and_then(|value| value.parse::<usize>().ok())
    {
        Some(index) => index,
        None => return,
    };
    let input = if args.length() == 0 {
        RuntimeJson::Object(RuntimeObject::new())
    } else {
        v8_value_to_json(scope, args.get(0))
            .ok()
            .flatten()
            .unwrap_or_else(|| RuntimeJson::Object(RuntimeObject::new()))
    };
    let Some(resolver) = v8::PromiseResolver::new(scope) else {
        return;
    };
    let promise = resolver.get_promise(scope);
    let resolver = v8::Global::new(scope, resolver);
    let (tool_name, event_tx, id) = {
        let Some(state) = scope.get_slot_mut::<ReplRuntimeState>() else {
            return;
        };
        let Some(tool) = state.enabled_tools.get(tool_index) else {
            return;
        };
        let id = state.next_tool_call_id;
        state.next_tool_call_id = state.next_tool_call_id.saturating_add(1);
        state.pending_tool_calls.insert(id, resolver);
        state.pending_tool_order.push((id, tool.tool_name.clone()));
        (tool.tool_name.clone(), state.event_tx.clone(), id)
    };
    let _ = event_tx.send(ReplEvent::ToolCall {
        id,
        name: tool_name,
        input,
    });
    retval.set(promise.into());
}

fn resolve_pending(
    scope: &mut v8::PinScope<'_, '_>,
    id: u64,
    reply: Result<RuntimeJson, CodeModeToolRejection>,
) {
    let Some(resolver) = scope
        .get_slot_mut::<ReplRuntimeState>()
        .and_then(|state| state.pending_tool_calls.remove(&id))
    else {
        return;
    };
    let tool_name = scope.get_slot::<ReplRuntimeState>().and_then(|state| {
        state
            .pending_tool_order
            .iter()
            .find(|(pending, _)| *pending == id)
            .map(|(_, name)| name.clone())
    });
    if let Some(state) = scope.get_slot_mut::<ReplRuntimeState>() {
        state
            .pending_tool_order
            .retain(|(pending, _)| *pending != id);
        if let (Err(rejection), Some(name)) = (&reply, tool_name) {
            state
                .failed_tool_calls
                .push((name, rejection.message.clone()));
        }
    }
    let resolver = v8::Local::new(scope, &resolver);
    match reply {
        Ok(result) => {
            let value = json_to_v8(scope, &result).unwrap_or_else(|| v8::undefined(scope).into());
            let _ = resolver.resolve(scope, value);
        }
        Err(rejection) => {
            let Some(message) = runtime_string_to_v8(scope, &rejection.message) else {
                let _ = resolver.reject(scope, v8::undefined(scope).into());
                return;
            };
            let error = v8::Exception::error(scope, message);
            if let Ok(object) = v8::Local::<v8::Object>::try_from(error) {
                if let (Some(key), Some(name)) = (
                    v8::String::new(scope, "name"),
                    v8::String::new(scope, rejection.name),
                ) {
                    let _ = object.set(scope, key.into(), name.into());
                }
            }
            let _ = resolver.reject(scope, error);
        }
    }
}

fn take_pending_tool_names(scope: &mut v8::PinScope<'_, '_>) -> Vec<String> {
    let order = scope
        .get_slot_mut::<ReplRuntimeState>()
        .map(|state| {
            state.pending_tool_calls.clear();
            std::mem::take(&mut state.pending_tool_order)
        })
        .unwrap_or_default();
    order
        .into_iter()
        .map(|(_, name)| String::from_utf8_lossy(&name.to_node_utf8()).into_owned())
        .collect()
}

// ---- timers -----------------------------------------------------------------

fn schedule_timeout(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    interval_ms: Option<u64>,
) -> Option<u64> {
    let callback = args.get(0);
    let callback = v8::Local::<v8::Function>::try_from(callback).ok()?;
    let delay_ms = args
        .get(1)
        .number_value(scope)
        .map(|delay| {
            if delay.is_finite() && delay > 0.0 {
                delay.trunc().min(u64::MAX as f64) as u64
            } else {
                0
            }
        })
        .unwrap_or(0);
    let callback = v8::Global::new(scope, callback);
    let state = scope.get_slot_mut::<ReplRuntimeState>()?;
    let id = state.next_timeout_id;
    state.next_timeout_id = state.next_timeout_id.saturating_add(1);
    let command_tx = state.command_tx.clone();
    let timer_handle = state.timer_handle.clone();
    let token = CancellationToken::new();
    state.pending_timeouts.insert(
        id,
        ScheduledTimeout {
            callback,
            token: token.clone(),
            interval_ms,
        },
    );
    timer_handle.spawn(async move {
        tokio::select! {
            _ = token.cancelled() => {}
            _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => {
                let _ = command_tx.send(ReplCommand::TimeoutFired { id });
            }
        }
    });
    Some(id)
}

fn set_timeout_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let id = schedule_timeout(scope, args, None).unwrap_or(0);
    retval.set(v8::Number::new(scope, id as f64).into());
}

fn set_interval_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let delay = args.get(1).number_value(scope).unwrap_or(0.0);
    let interval_ms = if delay.is_finite() && delay > 0.0 {
        delay.trunc().min(u64::MAX as f64) as u64
    } else {
        0
    };
    let id = schedule_timeout(scope, args, Some(interval_ms)).unwrap_or(0);
    retval.set(v8::Number::new(scope, id as f64).into());
}

fn clear_timeout_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    _retval: v8::ReturnValue<v8::Value>,
) {
    let Some(id) = args.get(0).number_value(scope) else {
        return;
    };
    if !id.is_finite() || id <= 0.0 {
        return;
    }
    let id = id.trunc().min(u64::MAX as f64) as u64;
    if let Some(state) = scope.get_slot_mut::<ReplRuntimeState>() {
        if let Some(timeout) = state.pending_timeouts.remove(&id) {
            timeout.token.cancel();
        }
    }
}

fn install_timers(scope: &mut v8::PinScope<'_, '_>) -> Result<(), RuntimeString> {
    fn install(
        scope: &mut v8::PinScope<'_, '_>,
        name: &str,
        callback: impl v8::MapFnTo<v8::FunctionCallback>,
    ) -> Result<(), RuntimeString> {
        let template = v8::FunctionTemplate::builder(callback).build(scope);
        let function = template
            .get_function(scope)
            .ok_or_else(|| RuntimeString::from("failed to create timer helper"))?;
        let global = scope.get_current_context().global(scope);
        let key = v8::String::new(scope, name)
            .ok_or_else(|| RuntimeString::from("failed to allocate timer helper name"))?;
        if global.set(scope, key.into(), function.into()) == Some(true) {
            Ok(())
        } else {
            Err(RuntimeString::from("failed to set timer helper"))
        }
    }
    install(scope, "setTimeout", set_timeout_callback)?;
    install(scope, "setInterval", set_interval_callback)?;
    install(scope, "clearTimeout", clear_timeout_callback)?;
    install(scope, "clearInterval", clear_timeout_callback)?;
    Ok(())
}

fn invoke_timeout(scope: &mut v8::PinScope<'_, '_>, id: u64) {
    let entry = scope
        .get_slot_mut::<ReplRuntimeState>()
        .and_then(|state| state.pending_timeouts.remove(&id));
    let Some(entry) = entry else {
        return;
    };
    if let Some(interval_ms) = entry.interval_ms {
        let callback = entry.callback.clone();
        let command_tx = scope
            .get_slot::<ReplRuntimeState>()
            .map(|state| state.command_tx.clone());
        let timer_handle = scope
            .get_slot::<ReplRuntimeState>()
            .map(|state| state.timer_handle.clone());
        if let (Some(command_tx), Some(timer_handle)) = (command_tx, timer_handle) {
            if let Some(state) = scope.get_slot_mut::<ReplRuntimeState>() {
                let new_id = state.next_timeout_id;
                state.next_timeout_id = state.next_timeout_id.saturating_add(1);
                let token = CancellationToken::new();
                state.pending_timeouts.insert(
                    new_id,
                    ScheduledTimeout {
                        callback,
                        token: token.clone(),
                        interval_ms: Some(interval_ms),
                    },
                );
                timer_handle.spawn(async move {
                    tokio::select! {
                        _ = token.cancelled() => {}
                        _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {
                            let _ = command_tx.send(ReplCommand::TimeoutFired { id: new_id });
                        }
                    }
                });
            }
        }
    }
    let callback = v8::Local::new(scope, &entry.callback);
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let tc = tc.init();
    let receiver = v8::undefined(&tc).into();
    let _ = callback.call(&tc, receiver, &[]);
}

// ---- outcomes ---------------------------------------------------------------

fn send_done(event_tx: &tokio_mpsc::UnboundedSender<ReplEvent>, done: ReplDone) {
    let _ = event_tx.send(ReplEvent::Done(done));
}

fn error_done(error: RuntimeString) -> ReplDone {
    ReplDone {
        stdout: RuntimeString::default(),
        stderr: RuntimeString::default(),
        result: RuntimeJson::Null,
        result_defined: true,
        result_text: RuntimeString::from("null"),
        error: Some(error),
        terminated: false,
    }
}

fn error_done_via(
    scope: &mut v8::PinScope<'_, '_>,
    exception: Option<v8::Global<v8::Value>>,
) -> ReplDone {
    let mut error = match exception {
        Some(exception) => format_error(scope, exception),
        None => RuntimeString::from("unknown REPL exception"),
    };
    // Surface rejected inner tool calls as the likely root cause, matching
    // the aggregated error contract of the tool.
    let failed = scope
        .get_slot_mut::<ReplRuntimeState>()
        .map(|state| std::mem::take(&mut state.failed_tool_calls))
        .unwrap_or_default();
    if !failed.is_empty() {
        error = error.concat(&RuntimeString::from(
            "\n\nInner tool errors (likely root cause):",
        ));
        for (name, message) in &failed {
            error = error
                .concat(&RuntimeString::from("\n- "))
                .concat(name)
                .concat(&RuntimeString::from(": "))
                .concat(message);
        }
    }
    ReplDone {
        stdout: read_capture(scope, "out"),
        stderr: read_capture(scope, "err"),
        result: RuntimeJson::Null,
        result_defined: true,
        result_text: RuntimeString::from("null"),
        error: Some(error),
        terminated: false,
    }
}

fn terminated_done(scope: &mut v8::PinScope<'_, '_>) -> ReplDone {
    if let Some(state) = scope.get_slot_mut::<ReplRuntimeState>() {
        state.pending_tool_calls.clear();
        state.pending_tool_order.clear();
        for (_, timeout) in state.pending_timeouts.drain() {
            timeout.token.cancel();
        }
    }
    ReplDone {
        stdout: read_capture(scope, "out"),
        stderr: read_capture(scope, "err"),
        result: RuntimeJson::Null,
        result_defined: true,
        result_text: RuntimeString::from("null"),
        error: None,
        terminated: true,
    }
}

// ---- caller-side adapter ----------------------------------------------------

/// The script-time budget: counts down only while NO inner tool call is in
/// flight (inner tool time is excluded).
struct ScriptBudget {
    remaining: Duration,
    running_since: Option<tokio::time::Instant>,
    inflight: usize,
}

impl ScriptBudget {
    fn new(budget_ms: u64) -> Self {
        Self {
            remaining: Duration::from_millis(budget_ms),
            running_since: Some(tokio::time::Instant::now()),
            inflight: 0,
        }
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        self.running_since.map(|since| since + self.remaining)
    }

    fn on_tool_start(&mut self) {
        self.inflight += 1;
        if let Some(since) = self.running_since.take() {
            self.remaining = self.remaining.saturating_sub(since.elapsed());
        }
    }

    fn on_tool_end(&mut self) {
        self.inflight = self.inflight.saturating_sub(1);
        if self.inflight == 0 && self.running_since.is_none() {
            self.running_since = Some(tokio::time::Instant::now());
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum TerminateReason {
    Budget,
    Wall,
    Interrupt,
}

struct ReplActor {
    eval_tx: std_mpsc::Sender<ReplEvalRequest>,
    command_tx: std_mpsc::Sender<ReplCommand>,
    isolate_handle: v8::IsolateHandle,
    events: TokioMutex<tokio_mpsc::UnboundedReceiver<ReplEvent>>,
}

/// A persistent per-session REPL runtime. Execs are serialized by the session
/// (the tool is not concurrency-safe).
pub(crate) struct ReplSession {
    actor: TokioMutex<Option<ReplActor>>,
    next_exec_id: AtomicU64,
}

impl Default for ReplSession {
    fn default() -> Self {
        Self {
            actor: TokioMutex::new(None),
            next_exec_id: AtomicU64::new(1),
        }
    }
}

fn spawn_actor() -> Result<ReplActor, String> {
    initialize_v8()?;
    let (eval_tx, eval_rx) = std_mpsc::channel();
    let (command_tx, command_rx) = std_mpsc::channel();
    let (event_tx, event_rx) = tokio_mpsc::unbounded_channel();
    let (handle_tx, handle_rx) = std_mpsc::sync_channel(1);
    let timer_handle =
        Handle::try_current().map_err(|error| format!("REPL timers require Tokio: {error}"))?;
    let thread_event_tx = event_tx.clone();
    let actor_command_tx = command_tx.clone();
    std::thread::spawn(move || {
        let runtime = move || {
            run_repl_runtime(
                eval_rx,
                thread_event_tx,
                command_tx,
                command_rx,
                timer_handle,
                handle_tx,
            );
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(runtime)).is_err() {
            let _ = event_tx.send(ReplEvent::ThreadPanicked);
        }
    });
    let isolate_handle = handle_rx
        .recv()
        .map_err(|_| "failed to initialize REPL runtime".to_string())?;
    Ok(ReplActor {
        eval_tx,
        command_tx: actor_command_tx,
        isolate_handle,
        events: TokioMutex::new(event_rx),
    })
}

/// The bridge form the runtime dispatches nested tool calls through: it
/// resolves with the JSON value the script sees, or rejects with the
/// tool-facing reason.
pub(crate) type ReplInvokeFn = Arc<
    dyn Fn(
            RuntimeString,
            RuntimeJson,
        ) -> BoxFuture<'static, Result<RuntimeJson, CodeModeToolRejection>>
        + Send
        + Sync,
>;

/// Project one reentrant tool call into the value a script's tool function
/// resolves to: a failing call rejects with its text (or a synthesized
/// message when it produced none), and text that parses as JSON resolves to
/// that value rather than to a string.
pub(crate) fn project_invoke_result(
    name: &RuntimeString,
    result: Result<InvokeToolResult, ToolError>,
) -> Result<RuntimeJson, CodeModeToolRejection> {
    let result = result.map_err(CodeModeToolRejection::tool)?;
    let text = text_of_blocks(&result.content);
    if result.is_error {
        let message = if text.as_utf16().is_empty() {
            RuntimeString::from("tool ")
                .concat(name)
                .concat(&RuntimeString::from(" failed"))
        } else {
            text
        };
        return Err(CodeModeToolRejection::plain(message));
    }
    Ok(runtime_json::parse(&text).unwrap_or(RuntimeJson::String(text)))
}

/// Adapt the caller's reentrant `invoke_tool` bridge onto [`ReplInvokeFn`].
pub(crate) fn tool_fn_invoke(invoke_tool: Option<InvokeToolFn>) -> Option<ReplInvokeFn> {
    invoke_tool.map(|invoke_tool| {
        Arc::new(move |name: RuntimeString, input: RuntimeJson| {
            let invoke_tool = Arc::clone(&invoke_tool);
            let projection_name = name.clone();
            let future: BoxFuture<'static, Result<RuntimeJson, CodeModeToolRejection>> =
                Box::pin(async move {
                    project_invoke_result(&projection_name, invoke_tool(name, input).await)
                });
            future
        }) as ReplInvokeFn
    })
}

fn dispatch_invoke(
    invoke: Option<ReplInvokeFn>,
    rpc_id: u64,
    name: RuntimeString,
    input: RuntimeJson,
) -> BoxFuture<'static, (u64, Result<RuntimeJson, CodeModeToolRejection>)> {
    Box::pin(async move {
        let Some(invoke) = invoke else {
            return (
                rpc_id,
                Err(CodeModeToolRejection::plain(BRIDGE_UNAVAILABLE)),
            );
        };
        (rpc_id, invoke(name, input).await)
    })
}

fn text_of_blocks(content: &[OutputBlock]) -> RuntimeString {
    let mut text = RuntimeString::default();
    let mut first = true;
    for block in content {
        if let OutputBlock::Text(block) = block {
            if !first {
                text = text.concat(&RuntimeString::from("\n"));
            }
            text = text.concat(&block.text);
            first = false;
        }
    }
    text
}

impl ReplSession {
    /// Run one evaluation against an explicit nested-tool bridge and interrupt
    /// signal. This is the carrier-neutral entry point: the in-process tool
    /// path adapts its `ToolContext`, and the host carrier adapts the
    /// connection's delegate channel onto the same shape.
    pub(crate) async fn execute_with(
        &self,
        request: ReplExecRequest,
        invoke: Option<ReplInvokeFn>,
        signal: &CancellationToken,
    ) -> Result<ReplExecOutcome, String> {
        let mut slot = self.actor.lock().await;
        let actor = match slot.take() {
            Some(actor) => actor,
            None => spawn_actor()?,
        };
        let budget_ms = request.timeout_ms;
        let tools = request
            .tools
            .iter()
            .map(|binding| NestedToolMetadata {
                tool_name: binding.name.clone(),
                global_name: binding.key.clone(),
                description: RuntimeString::default(),
                kind: CodeModeToolKind::Function,
            })
            .collect();
        let exec_id = self.next_exec_id.fetch_add(1, Ordering::Relaxed);
        let result = if actor
            .eval_tx
            .send(ReplEvalRequest {
                source: request.code,
                tools,
                exec_id,
            })
            .is_err()
        {
            Err("REPL runtime unavailable".to_string())
        } else {
            run_exec_loop(&actor, invoke, signal, budget_ms, exec_id).await
        };
        if result.is_ok() {
            *slot = Some(actor);
        }
        result
    }
}

async fn run_exec_loop(
    actor: &ReplActor,
    invoke: Option<ReplInvokeFn>,
    signal: &CancellationToken,
    budget_ms: u64,
    exec_id: u64,
) -> Result<ReplExecOutcome, String> {
    let mut events = actor.events.lock().await;
    let mut budget = ScriptBudget::new(budget_ms);
    let hard_deadline = tokio::time::Instant::now() + Duration::from_millis(REPL_MAX_TIMEOUT_MS);
    let mut terminate_reason: Option<TerminateReason> = None;
    let mut pending: FuturesUnordered<
        BoxFuture<'static, (u64, Result<RuntimeJson, CodeModeToolRejection>)>,
    > = FuturesUnordered::new();

    loop {
        let deadline = match terminate_reason {
            Some(_) => None,
            None => Some(
                budget
                    .deadline()
                    .map_or(hard_deadline, |deadline| deadline.min(hard_deadline)),
            ),
        };
        let sleep = match deadline {
            Some(deadline) => Either::Left(tokio::time::sleep_until(deadline)),
            None => Either::Right(futures::future::pending()),
        };
        tokio::select! {
            event = events.recv() => {
                match event {
                    Some(ReplEvent::ToolCall { id, name, input }) => {
                        if terminate_reason.is_some() {
                            let _ = actor.command_tx.send(ReplCommand::ToolError {
                                id,
                                rejection: CodeModeToolRejection::plain(INTERRUPTED_MESSAGE),
                            });
                            continue;
                        }
                        budget.on_tool_start();
                        pending.push(dispatch_invoke(invoke.clone(), id, name, input));
                    }
                    Some(ReplEvent::Done(done)) => {
                        return Ok(compose_outcome(done, terminate_reason, budget_ms));
                    }
                    Some(ReplEvent::ThreadPanicked) => {
                        return Err("REPL runtime thread panicked".to_string());
                    }
                    None => return Err("REPL runtime unavailable".to_string()),
                }
            }
            Some((rpc_id, reply)) = pending.next(), if !pending.is_empty() => {
                budget.on_tool_end();
                let command = match reply {
                    Ok(result) => ReplCommand::ToolResponse { id: rpc_id, result },
                    Err(rejection) => ReplCommand::ToolError { id: rpc_id, rejection },
                };
                if actor.command_tx.send(command).is_err() {
                    return Err("REPL runtime unavailable".to_string());
                }
            }
            _ = sleep => {
                let reason = if deadline == Some(hard_deadline) {
                    TerminateReason::Wall
                } else {
                    TerminateReason::Budget
                };
                terminate_reason = Some(reason);
                actor.isolate_handle.terminate_execution();
                let _ = actor.command_tx.send(ReplCommand::Terminate { exec_id });
            }
            _ = signal.cancelled(), if terminate_reason.is_none() => {
                terminate_reason = Some(TerminateReason::Interrupt);
                actor.isolate_handle.terminate_execution();
                let _ = actor.command_tx.send(ReplCommand::Terminate { exec_id });
            }
        }
    }
}

fn compose_outcome(
    done: ReplDone,
    reason: Option<TerminateReason>,
    budget_ms: u64,
) -> ReplExecOutcome {
    let error = if done.terminated {
        Some(RuntimeString::from(match reason {
            Some(TerminateReason::Budget) => timeout_message(budget_ms),
            Some(TerminateReason::Wall) => hard_limit_message(),
            Some(TerminateReason::Interrupt) | None => INTERRUPTED_MESSAGE.to_string(),
        }))
    } else {
        done.error
    };
    ReplExecOutcome {
        result: done.result,
        result_defined: done.result_defined,
        result_text: done.result_text,
        stdout: done.stdout,
        stderr: done.stderr,
        error,
    }
}
