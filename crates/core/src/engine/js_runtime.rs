//! JavaScript script runners backed by the host Node/V8 runtime.
//!
//! This is the production path for JS-language workflow programs:
//! model-authored scripts run as real JavaScript in Node's `vm` module, while
//! every effect still crosses the Rust sandbox seam.

use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use crate::engine::flow::FlowWorkflowScriptRunner;
use crate::engine::workflow::runtime::{
    WorkflowSandbox, WorkflowScriptContext, WorkflowScriptRunner, PER_CALL_ITEM_CAP,
};
use crate::engine::workflow::WorkflowError;
use crate::runtime_process::{
    parent_runtime_env_value, RuntimeProcessContext, RuntimeSpawnRequest, RuntimeStdio,
};
use crate::runtime_values::{
    runtime_codec, NodeChunkUtf8, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder,
    RuntimeJson, RuntimeObject, RuntimeObjectEncoder, RuntimeString, StrictScalarUtf8,
    Utf8BomPolicy,
};
use crate::types::json::Json;
use crate::types::tool::BoxFuture;

/// Workflow runner that executes the wrapped script as JavaScript in Node/V8.
pub struct NodeJsWorkflowScriptRunner;

/// Backwards-compatible workflow runner: native JSON Flow remains supported,
/// while ordinary JS workflow scripts run in Node/V8.
pub struct HybridWorkflowScriptRunner;

const NODE_BRIDGE: &str = r#"
const readline = require('node:readline');
const vm = require('node:vm');

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
let booted = false;
let nextId = 1;
const pending = new Map();

function send(msg) {
  process.stdout.write(JSON.stringify(msg) + '\n');
}

function messageOf(err) {
  if (err && typeof err.message === 'string') return err.message;
  return String(err);
}

function toJson(value, inArray = false) {
  if (value === null) return null;
  if (value === undefined) return inArray ? null : undefined;
  const t = typeof value;
  if (t === 'boolean' || t === 'string') return value;
  if (t === 'number') return Number.isFinite(value) ? value : null;
  if (t === 'bigint') return null;
  if (t === 'function' || t === 'symbol') return inArray ? null : undefined;
  if (Array.isArray(value)) return value.map((v) => toJson(v, true));
  const out = {};
  for (const key of Object.keys(value)) {
    const v = toJson(value[key], false);
    if (v !== undefined) out[key] = v;
  }
  return out;
}

function rpc(method, params) {
  const id = nextId++;
  send({ type: 'call', id, method, params });
  return new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
}

function fire(method, params) {
  send({ type: 'notify', method, params });
}

function finish(msg) {
  send(msg);
  setImmediate(() => process.exit(0));
}

function installResponseHandler() {
  rl.on('line', (line) => {
    let msg;
    try {
      msg = JSON.parse(line);
    } catch (_) {
      return;
    }
    if (msg.type !== 'response') return;
    const slot = pending.get(msg.id);
    if (!slot) return;
    pending.delete(msg.id);
    if (msg.ok) {
      slot.resolve(msg.value);
    } else {
      slot.reject(new Error(msg.message || 'host call failed'));
    }
  });
}

function makeWorkflowSandbox(payload) {
  let budgetSpent = 0;
  const budgetTotal = payload.budgetTotal;
  const budget = {
    total: budgetTotal === undefined ? null : budgetTotal,
    spent: () => budgetSpent,
    remaining: () => budgetTotal === null || budgetTotal === undefined ? Infinity : Math.max(0, budgetTotal - budgetSpent),
  };
  function absorbBudget(value) {
    if (value && typeof value.budgetSpent === 'number') budgetSpent = value.budgetSpent;
    return value ? value.value : null;
  }
  return {
    args: payload.args,
    budget,
    agent: async (prompt, opts = {}) => absorbBudget(await rpc('agent', { prompt: toJson(prompt), opts: toJson(opts ?? {}) })),
    workflow: async (nameOrRef, childArgs = null) => absorbBudget(await rpc('workflow', { nameOrRef: toJson(nameOrRef), args: toJson(childArgs) })),
    phase: (value) => fire('phase', { value: String(value) }),
    log: (value) => fire('log', { value: String(value) }),
    parallel: (thunks) => {
      if (!Array.isArray(thunks)) throw new TypeError('parallel(thunks) expects an array of functions');
      if (thunks.length > payload.maxItems) throw new Error(`parallel: ${thunks.length} items exceeds the ${payload.maxItems} cap`);
      return Promise.all(thunks.map((thunk) => {
        if (typeof thunk !== 'function') return Promise.resolve(null);
        try {
          return Promise.resolve(thunk()).catch(() => null);
        } catch (_) {
          return Promise.resolve(null);
        }
      }));
    },
    pipeline: (items, ...stages) => {
      if (!Array.isArray(items)) throw new TypeError('pipeline(items, ...stages) expects an array of items');
      if (items.length > payload.maxItems) throw new Error(`pipeline: ${items.length} items exceeds the ${payload.maxItems} cap`);
      const fns = stages.filter((stage) => typeof stage === 'function');
      return Promise.all(items.map(async (item, index) => {
        let cur = item;
        for (const stage of fns) {
          try {
            cur = await stage(cur, item, index);
          } catch (_) {
            return null;
          }
        }
        return cur;
      }));
    },
  };
}

async function runPayload(payload) {
  installResponseHandler();
  const sandbox = makeWorkflowSandbox(payload);
  const context = vm.createContext(sandbox);
  if (payload.prelude) new vm.Script(payload.prelude, { filename: 'klaus-prelude.js' }).runInContext(context);
  let value;
  try {
    value = new vm.Script(payload.wrappedSource, { filename: 'workflow.js' }).runInContext(context);
  } catch (err) {
    finish({ type: 'done', ok: false, message: messageOf(err) });
    return;
  }
  try {
    const fn = value;
    const result = await fn();
    const json = toJson(result);
    finish({ type: 'done', ok: true, value: json === undefined ? null : json });
  } catch (err) {
    finish({ type: 'done', ok: false, message: messageOf(err) });
  }
}

rl.once('line', (line) => {
  let payload;
  try {
    payload = JSON.parse(line);
  } catch (err) {
    finish({ type: 'done', ok: false, message: messageOf(err) });
    return;
  }
  runPayload(payload).catch((err) => {
    finish({ type: 'done', ok: false, message: messageOf(err) });
  });
});
"#;

struct NodePayload {
    wrapped_source: RuntimeString,
    args: RuntimeJson,
    budget_total: Option<i64>,
    max_items: usize,
    prelude: &'static RuntimeString,
}

impl RuntimeEncode for NodePayload {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("wrappedSource", &self.wrapped_source)?;
        object.field("args", &self.args)?;
        object.field("budgetTotal", &self.budget_total)?;
        object.field("maxItems", &self.max_items)?;
        object.field("prelude", &self.prelude)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

#[derive(Debug)]
enum ChildMessage {
    Call {
        id: u64,
        method: RuntimeString,
        params: RuntimeJson,
    },
    Notify {
        method: RuntimeString,
        params: RuntimeJson,
    },
    Done {
        ok: bool,
        value: Option<RuntimeJson>,
        message: Option<RuntimeString>,
    },
}

impl RuntimeDecode for ChildMessage {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                runtime_codec::RuntimeEnumVariant::new("call", &[]),
                runtime_codec::RuntimeEnumVariant::new("notify", &[]),
                runtime_codec::RuntimeEnumVariant::new("done", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Call {
                    id: object.required("id", &[])?,
                    method: object.required("method", &[])?,
                    params: object.required("params", &[])?,
                }),
                1 => Ok(Self::Notify {
                    method: object.required("method", &[])?,
                    params: object.required("params", &[])?,
                }),
                2 => Ok(Self::Done {
                    ok: object.required("ok", &[])?,
                    value: object.optional("value", &[])?,
                    message: object.optional("message", &[])?,
                }),
                _ => unreachable!("variant index is resolved above"),
            },
        )
    }
}

struct HostResponse {
    kind: &'static RuntimeString,
    id: u64,
    ok: bool,
    value: Option<RuntimeJson>,
    message: Option<RuntimeString>,
}

impl RuntimeEncode for HostResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("type", &self.kind)?;
        object.field("id", &self.id)?;
        object.field("ok", &self.ok)?;
        if let Some(value) = &self.value {
            object.field("value", value)?;
        }
        if let Some(message) = &self.message {
            object.field("message", message)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

struct RpcReply {
    ok: bool,
    value: Option<Json>,
    message: Option<RuntimeString>,
}

impl RpcReply {
    fn ok(value: Json) -> Self {
        Self {
            ok: true,
            value: Some(value),
            message: None,
        }
    }

    fn err(message: impl Into<RuntimeString>) -> Self {
        Self {
            ok: false,
            value: None,
            message: Some(message.into()),
        }
    }
}

struct DoneMessage {
    ok: bool,
    value: Option<Json>,
    message: Option<RuntimeString>,
}

static WORKFLOW_PRELUDE: LazyLock<RuntimeString> =
    LazyLock::new(|| RuntimeString::from(crate::engine::workflow::runtime::DETERMINISM_PRELUDE));
static RESPONSE_KIND: LazyLock<RuntimeString> = LazyLock::new(|| RuntimeString::from("response"));

pub async fn node_available(process_context: &RuntimeProcessContext) -> bool {
    let spawner = process_context.spawner();
    let request = RuntimeSpawnRequest::new(node_binary())
        .with_args(vec![RuntimeString::from("--version")])
        .with_stdio(
            RuntimeStdio::Inherit,
            RuntimeStdio::Null,
            RuntimeStdio::Null,
        );
    let Ok(mut child) = spawner.spawn(request).await else {
        return false;
    };
    child.wait().await.is_ok_and(|status| status.success())
}

pub(crate) fn node_binary() -> RuntimeString {
    parent_runtime_env_value("KLAUS_NODE").unwrap_or_else(|| RuntimeString::from("node"))
}

/// Compose an actionable diagnostic for a failed Node bridge launch.
///
/// The workflow runner executes native JSON flow programs in-process and only
/// falls back to a Node subprocess for scripts authored as raw JavaScript.
/// When that subprocess cannot be launched the raw spawn error (typically
/// `No such file or directory`) is opaque, so surface the resolved
/// interpreter, the `KLAUS_NODE` override, and the native-flow alternative.
pub(crate) fn node_unavailable_message(error: &impl std::fmt::Display) -> String {
    let binary = String::from_utf8_lossy(&node_binary().to_node_utf8()).into_owned();
    let source = if parent_runtime_env_value("KLAUS_NODE").is_some() {
        "the KLAUS_NODE environment variable"
    } else {
        "the PATH-resolved `node` executable"
    };
    format!(
        "JavaScript runtime unavailable: could not launch `{binary}` (resolved from {source}): \
{error}. Raw-JavaScript workflow scripts require a Node.js interpreter; \
install Node.js or point KLAUS_NODE at its binary. Scripts written as native JSON flow \
programs run in-process and do not need Node.js."
    )
}

async fn run_node_loop<'a, N, D>(
    process_context: &RuntimeProcessContext,
    payload: NodePayload,
    mut notify: N,
    mut dispatch: D,
) -> Result<DoneMessage, String>
where
    N: FnMut(RuntimeString, RuntimeJson) -> Result<(), String>,
    D: FnMut(u64, RuntimeString, RuntimeJson) -> BoxFuture<'a, (u64, RpcReply)>,
{
    let spawner = process_context.spawner();
    let request = RuntimeSpawnRequest::new(node_binary())
        .with_args(vec![
            RuntimeString::from("--experimental-vm-modules"),
            RuntimeString::from("-e"),
            RuntimeString::from(NODE_BRIDGE),
        ])
        .with_stdio(RuntimeStdio::Pipe, RuntimeStdio::Pipe, RuntimeStdio::Pipe)
        .with_hard_kill_on_drop(true);
    let mut child = spawner
        .spawn(request)
        .await
        .map_err(|error| node_unavailable_message(&error))?;
    let mut stdin = child
        .take_stdin()
        .ok_or_else(|| "JavaScript runtime stdin unavailable".to_string())?;
    let stdout = child
        .take_stdout()
        .ok_or_else(|| "JavaScript runtime stdout unavailable".to_string())?;
    let mut stderr = child.take_stderr();
    let stderr_task = tokio::spawn(async move {
        let mut text = RuntimeString::default();
        let mut buffer = [0_u8; 8 * 1024];
        if let Some(stderr) = stderr.as_mut() {
            loop {
                match stderr.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => {
                        text = text.concat(&NodeChunkUtf8::decode_chunk(&buffer[..read]));
                    }
                }
            }
        }
        text
    });

    let mut payload_line = runtime_codec::encode_bytes(&payload)
        .map_err(|error| format!("encode JS payload: {error}"))?;
    payload_line.push(b'\n');
    stdin
        .write_all(&payload_line)
        .await
        .map_err(|e| format!("write JS payload: {e}"))?;

    let mut stdout = BufReader::new(stdout);
    let mut line_bytes = Vec::new();
    let mut pending: FuturesUnordered<BoxFuture<'a, (u64, RpcReply)>> = FuturesUnordered::new();
    loop {
        line_bytes.clear();
        tokio::select! {
            read = stdout.read_until(b'\n', &mut line_bytes) => {
                let read = read.map_err(|e| format!("read JS runtime output: {e}"))?;
                if read == 0 {
                    let status = child.wait().await.map_err(|e| format!("wait JS runtime: {e}"))?;
                    let stderr = stderr_task.await.unwrap_or_default();
                    return Err(format!("JavaScript runtime exited before completion ({status:?}); {stderr:?}"));
                }
                if line_bytes.last() == Some(&b'\n') {
                    line_bytes.pop();
                    if line_bytes.last() == Some(&b'\r') {
                        line_bytes.pop();
                    }
                }
                let line = StrictScalarUtf8::decode(&line_bytes, Utf8BomPolicy::Forbid)
                    .map_err(|error| format!("decode JS runtime UTF-8: {error}"))?;
                let msg = runtime_codec::decode_text::<ChildMessage>(&line)
                    .map_err(|error| format!("decode JS runtime message: {error}: {line:?}"))?;
                match msg {
                    ChildMessage::Call { id, method, params } => {
                        pending.push(dispatch(id, method, params));
                    }
                    ChildMessage::Notify { method, params } => {
                        notify(method, params)?;
                    }
                    ChildMessage::Done { ok, value, message } => {
                        drop(stdin);
                        let _ = child.wait().await;
                        let _ = stderr_task.await;
                        return Ok(DoneMessage { ok, value, message });
                    }
                }
            }
            Some((id, reply)) = pending.next(), if !pending.is_empty() => {
                let mut response = runtime_codec::encode_bytes(&HostResponse {
                    kind: &RESPONSE_KIND,
                    id,
                    ok: reply.ok,
                    value: reply.value,
                    message: reply.message,
                }).map_err(|error| format!("encode JS host response: {error}"))?;
                response.push(b'\n');
                stdin.write_all(&response).await
                    .map_err(|e| format!("write JS host response: {e}"))?;
            }
        }
    }
}

fn runtime_object<const N: usize>(entries: [(&'static str, RuntimeJson); N]) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (name, value) in entries {
        object.insert(name, value);
    }
    RuntimeJson::Object(object)
}

fn runtime_message_to_host_string(value: RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 projection is valid UTF-8")
}

#[async_trait]
impl WorkflowScriptRunner for NodeJsWorkflowScriptRunner {
    async fn run(&self, ctx: WorkflowScriptContext) -> Result<Json, WorkflowError> {
        let payload = NodePayload {
            wrapped_source: ctx.wrapped_source,
            args: ctx.args,
            budget_total: ctx.sandbox.budget_total(),
            max_items: PER_CALL_ITEM_CAP,
            prelude: &WORKFLOW_PRELUDE,
        };
        let sandbox = Arc::clone(&ctx.sandbox);
        let done = run_node_loop(
            &ctx.process_context,
            payload,
            {
                let sandbox = Arc::clone(&sandbox);
                move |method, params| workflow_notify(&sandbox, method, params)
            },
            move |id, method, params| workflow_dispatch(Arc::clone(&sandbox), id, method, params),
        )
        .await
        .map_err(WorkflowError::new)?;
        if done.ok {
            Ok(done.value.unwrap_or(RuntimeJson::Null))
        } else {
            Err(WorkflowError::new(
                done.message
                    .map(runtime_message_to_host_string)
                    .unwrap_or_else(|| "workflow failed".to_string()),
            ))
        }
    }
}

#[async_trait]
impl WorkflowScriptRunner for HybridWorkflowScriptRunner {
    async fn run(&self, ctx: WorkflowScriptContext) -> Result<Json, WorkflowError> {
        match FlowWorkflowScriptRunner.run(ctx.clone()).await {
            Ok(value) => Ok(value),
            Err(e)
                if e.message
                    .starts_with("this engine executes JSON workflow programs") =>
            {
                NodeJsWorkflowScriptRunner.run(ctx).await
            }
            Err(e) => Err(e),
        }
    }
}

fn workflow_notify(
    sandbox: &Arc<WorkflowSandbox>,
    method: RuntimeString,
    params: Json,
) -> Result<(), String> {
    let value = params.get("value").cloned().unwrap_or(RuntimeJson::Null);
    if method == "phase" {
        sandbox.phase(&value);
    } else if method == "log" {
        sandbox.log(&value);
    } else {
        return Err(format!("unknown workflow notification: {method:?}"));
    }
    Ok(())
}

fn workflow_dispatch(
    sandbox: Arc<WorkflowSandbox>,
    id: u64,
    method: RuntimeString,
    params: Json,
) -> BoxFuture<'static, (u64, RpcReply)> {
    Box::pin(async move {
        let reply = if method == "agent" {
            let prompt = params.get("prompt").cloned().unwrap_or(RuntimeJson::Null);
            let opts = params
                .get("opts")
                .cloned()
                .unwrap_or_else(|| runtime_object([]));
            match sandbox.agent(prompt, opts).await {
                Ok(value) => RpcReply::ok(runtime_object([
                    ("value", value),
                    ("budgetSpent", RuntimeJson::from(sandbox.budget_spent())),
                ])),
                Err(e) => RpcReply::err(e.message),
            }
        } else if method == "workflow" {
            let name = params
                .get("nameOrRef")
                .cloned()
                .unwrap_or(RuntimeJson::Null);
            let args = params.get("args").cloned().unwrap_or(RuntimeJson::Null);
            match sandbox.workflow(name, args).await {
                Ok(value) => RpcReply::ok(runtime_object([
                    ("value", value),
                    ("budgetSpent", RuntimeJson::from(sandbox.budget_spent())),
                ])),
                Err(e) => RpcReply::err(e.message),
            }
        } else {
            RpcReply::err(format!("unknown workflow host call: {method:?}"))
        };
        (id, reply)
    })
}

#[cfg(test)]
mod tests {
    use crate::engine::workflow::runtime::{run_workflow, WorkflowRunInput, WorkflowRuntimeDeps};
    use crate::runtime_values::{RuntimeJson, RuntimeString};
    use crate::testing::make_usage;
    use crate::types::abort::AbortSignal;
    use crate::types::errors::TerminalReason;
    use crate::types::events::{EngineEvent, ThreadItem, ToolDomainEvent};
    use crate::types::tool::{EngineEventStream, SpawnSubAgent, SpawnSubAgentOptions};
    use std::sync::Arc;

    fn rs(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn runtime(source: &str) -> RuntimeJson {
        crate::runtime_values::runtime_json::parse(&rs(source)).expect("valid exact JSON fixture")
    }

    fn spawn_echo() -> SpawnSubAgent {
        Arc::new(|opts: SpawnSubAgentOptions| {
            let events = vec![
                EngineEvent::ItemCompleted {
                    inference: None,
                    item: ThreadItem::AgentMessage {
                        phase: None,
                        id: rs("m"),
                        text: rs("echo: ").concat(&opts.prompt),
                    },
                },
                EngineEvent::TurnCompleted {
                    turn_id: "js".into(),
                    usage: Some(make_usage(None, Some(3))),
                    stop_reason: TerminalReason::Completed,
                },
            ];
            Box::pin(futures::stream::iter(events)) as EngineEventStream
        })
    }

    #[tokio::test]
    async fn node_workflow_runs_js_closures_parallel_and_to_json_like_ts() {
        let _external = crate::test_support::external_process_lock().await;
        if !super::node_available(&crate::runtime_process::test_runtime_process_context()).await {
            return;
        }
        let script = r#"
            export const meta = { name: "js-demo", description: "d" };
            const mk = (x) => async () => {
              const r = await agent(`p${x}`, { label: `l${x}` });
              return `${r}:${Object.getPrototypeOf([]) === Array.prototype}`;
            };
            const values = await parallel([mk(1), mk(2), 7, () => { throw new Error("drop"); }]);
            return { values, undef: undefined, nan: NaN, nested: { kept: 1, dropped: undefined } };
        "#;
        let result = run_workflow(
            WorkflowRunInput {
                script: rs(script),
                args: None,
                resume_from_run_id: None,
                budget_tokens: Some(20),
            },
            WorkflowRuntimeDeps {
                process_context: crate::runtime_process::test_runtime_process_context(),
                spawn_workflow_agent: spawn_echo(),
                journal: None,
                emit: Some(Arc::new(|_ev: ToolDomainEvent| {})),
                resolve_workflow: None,
                signal: AbortSignal::new(),
                max_concurrency: Some(2),
            },
            Arc::new(super::NodeJsWorkflowScriptRunner),
        )
        .await
        .expect("workflow runs");
        assert_eq!(
            result.result,
            runtime(
                r#"{"values":["echo: p1:true","echo: p2:true",null,null],"nan":null,"nested":{"kept":1}}"#,
            )
        );
        assert_eq!(result.output_tokens_spent, 6);
    }

    #[test]
    fn node_unavailable_message_is_actionable() {
        // Branch-independent invariants: whatever resolves `node`, the message
        // must name the runtime requirement, the KLAUS_NODE override, the
        // native JSON-flow alternative, and carry the underlying spawn error.
        let message = super::node_unavailable_message(&"No such file or directory (os error 2)");
        assert!(message.starts_with("JavaScript runtime unavailable:"));
        assert!(message.contains("os error 2"));
        assert!(message.contains("KLAUS_NODE"));
        assert!(message.contains("Node.js"));
        assert!(message.contains("native JSON flow"));
    }
}
