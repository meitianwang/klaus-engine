//! Run a workflow script to completion, returning its `return` value.
//!
//! ## The script-execution seam
//!
//! A workflow script is executed in a host-provided scripting VM. That VM is
//! NOT portable, so it is modeled as the injected [`WorkflowScriptRunner`]
//! trait: given a [`WorkflowScriptContext`] (the parsed meta, the wrapped source +
//! line offset, the `args` value, and a [`WorkflowSandbox`] of orchestration
//! primitives), the host runs the script — calling back into the sandbox's
//! `agent` / `parallel` / `pipeline` / `phase` / `log` / `workflow` primitives
//! and returning the script's value. Determinism enforcement (stripping the
//! non-reproducible clock/RNG from the script's view) is the host runner's job;
//! the exact prelude it should apply is exposed verbatim as
//! [`DETERMINISM_PRELUDE`].
//!
//! Everything AROUND that seam — budget ceilings, the resume journal cache, the
//! agent / concurrency caps, sub-agent spawn + token accounting, deterministic
//! call hashing, the parallel barrier, and pipeline chains — is implemented
//! here and exercised through the sandbox.
//!
//! Ordering contract the host runner must honor: the sandbox's `agent` records
//! its deterministic launch index SYNCHRONOUSLY when constructed (before the
//! returned future is awaited), and `parallel` invokes each thunk in array order
//! synchronously, so each thunk's first `agent` launches deterministically. A
//! thunk's `Thunk::Fn` closure must therefore run its own synchronous prefix
//! (including that first `agent(...)`) when CALLED, returning a future for the
//! remainder.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::future::join_all;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::engine::workflow::meta::{parse_workflow, WorkflowMeta};
use crate::engine::workflow::WorkflowError;
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::{runtime_json, RuntimeNumber, RuntimeObject, RuntimeString};
use crate::types::abort::AbortSignal;
use crate::types::errors::TerminalReason;
use crate::types::events::{EngineEvent, ThreadItem, ToolDomainEvent};
use crate::types::json::Json;
use crate::types::tool::{
    BoxFuture, Isolation, SpawnSubAgent, SpawnSubAgentOptions, WorkflowJournal,
    WorkflowJournalEntry,
};

/// Hard cap on the total sub-agents one run may spawn.
pub const TOTAL_AGENT_CAP: u64 = 1000;
/// Hard cap on the items passed to one `parallel` / `pipeline` call.
pub const PER_CALL_ITEM_CAP: usize = 4096;

/// The determinism prelude a host script runner must apply to a workflow
/// context before running the wrapped source: it strips the non-reproducible
/// clock/RNG from the script's view. This is a COOPERATIVE guarantee
/// (accident-containment, not a hard boundary) — a script that bypasses it only
/// breaks its own resume.
pub const DETERMINISM_PRELUDE: &str = r#"
'use strict';
Math.random = function random() {
  throw new Error('Math.random() is unavailable in workflow scripts (results must be reproducible for resume).');
};
globalThis.Date = (function (RealDate) {
  function WorkflowDate(...a) {
    if (!(this instanceof WorkflowDate)) {
      throw new Error('Date() as a function is unavailable in workflow scripts.');
    }
    if (a.length === 0) {
      throw new Error('new Date() with no arguments is unavailable in workflow scripts (non-deterministic).');
    }
    return new RealDate(...a);
  }
  WorkflowDate.now = function () {
    throw new Error('Date.now() is unavailable in workflow scripts (non-deterministic).');
  };
  WorkflowDate.parse = RealDate.parse;
  WorkflowDate.UTC = RealDate.UTC;
  WorkflowDate.prototype = RealDate.prototype;
  return WorkflowDate;
})(Date);
"#;

/// Default concurrency: leave headroom for the host, never exceed 16.
pub fn default_concurrency() -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1) as i64;
    (cpus - 2).clamp(1, 16) as usize
}

/// Resolve a named/ref workflow to its script source (host-supplied).
pub type ResolveWorkflowFn = Arc<dyn Fn(&RuntimeString) -> Option<RuntimeString> + Send + Sync>;

/// Host capabilities the runtime needs.
pub struct WorkflowRuntimeDeps {
    /// Process namespace shared by the owning engine and every workflow spawn.
    pub process_context: RuntimeProcessContext,
    /// Spawn one workflow agent (depth-bounded, workflow-governed concurrency).
    pub spawn_workflow_agent: SpawnSubAgent,
    /// Persisted result journal for deterministic resume (optional).
    pub journal: Option<Arc<dyn WorkflowJournal>>,
    /// Structured progress sink (phase / log / agent lifecycle).
    pub emit: Option<Arc<dyn Fn(ToolDomainEvent) + Send + Sync>>,
    /// Resolve a named/ref workflow to its script source (host-supplied).
    pub resolve_workflow: Option<ResolveWorkflowFn>,
    /// Abort signal for the whole workflow.
    pub signal: AbortSignal,
    /// Override the concurrency cap (tests). `None` = [`default_concurrency`].
    pub max_concurrency: Option<usize>,
}

/// Input to a workflow run.
#[derive(Debug, Clone)]
pub struct WorkflowRunInput {
    pub script: RuntimeString,
    /// The `args` value exposed to the script (`None` → `null`).
    pub args: Option<Json>,
    /// Resume: reuse the journal under this run id; new/changed calls run live.
    pub resume_from_run_id: Option<RuntimeString>,
    /// Output-token ceiling; `None` = unlimited.
    pub budget_tokens: Option<i64>,
}

/// The outcome of a completed workflow run.
#[derive(Debug, Clone)]
pub struct WorkflowRunResult {
    pub run_id: RuntimeString,
    pub result: Json,
    pub meta: WorkflowMeta,
    pub agents_spawned: u64,
    pub output_tokens_spent: u64,
}

/// The context a [`WorkflowScriptRunner`] receives for one script.
#[derive(Clone)]
pub struct WorkflowScriptContext {
    pub process_context: RuntimeProcessContext,
    /// The parsed meta of the script being run.
    pub meta: WorkflowMeta,
    /// The script body wrapped in an async function expression, ready to
    /// compile.
    pub wrapped_source: RuntimeString,
    /// Lines prepended by the wrapper, so a thrown error's line can be
    /// corrected.
    pub line_offset: i64,
    /// The `args` global exposed to the script.
    pub args: Json,
    /// The orchestration primitives exposed to the script as globals.
    pub sandbox: Arc<WorkflowSandbox>,
}

/// The injected script-execution seam: run one wrapped workflow source against
/// its [`WorkflowSandbox`] and return the script's value (or its thrown error).
#[async_trait]
pub trait WorkflowScriptRunner: Send + Sync {
    async fn run(&self, ctx: WorkflowScriptContext) -> Result<Json, WorkflowError>;
}

/// Mutable run-wide state, shared with nested `workflow()` calls (which share
/// everything but their nesting depth).
struct SharedState {
    run_id: RuntimeString,
    limit: Semaphore,
    deps: WorkflowRuntimeDeps,
    budget_total: Option<i64>,
    /// Cached prior results keyed by deterministic launch index.
    cache: HashMap<u64, WorkflowJournalEntry>,
    launch_counter: AtomicU64,
    total_agents: AtomicU64,
    spent: AtomicU64,
    /// Once a call diverges from the cached prefix, it + all later calls run
    /// live.
    prefix_broken: AtomicBool,
    /// Checkpoint failures survive script-level catches and branch error values.
    journal_failures: Mutex<Vec<WorkflowError>>,
    operation_tasks: Arc<crate::engine::query::RetainedToolTasks>,
    operation_signal: AbortSignal,
}

struct WorkflowRunOwner(AbortSignal);
impl Drop for WorkflowRunOwner {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// A thunk future produced by a `parallel` element.
pub type ThunkFuture = BoxFuture<'static, Result<Json, WorkflowError>>;

/// One element of a `parallel(thunks)` array. A non-function element resolves to
/// null verbatim; a function is invoked with no args and may throw/reject
/// (modeled as an immediately-`Err` future).
pub enum Thunk {
    Fn(Box<dyn FnOnce() -> ThunkFuture + Send>),
    NonFunction,
}

/// One `pipeline` stage: `(cur, item, index) -> value`. A throw returns an error
/// value for that item's chain.
pub type Stage =
    Box<dyn Fn(Json, Json, usize) -> BoxFuture<'static, Result<Json, WorkflowError>> + Send + Sync>;

/// The globals exposed to a workflow script. The host script runner calls these
/// directly. `parallel` / `pipeline` receive `None` when the corresponding
/// script argument was NOT an array (so the runtime can raise the exact
/// type-error); their pre-filtered function stages/thunks arrive as the
/// [`Thunk`] / [`Stage`] handles.
pub struct WorkflowSandbox {
    shared: Arc<SharedState>,
    /// Workflow nesting depth (0 = top level; nesting is one level only).
    depth: u32,
    args: Json,
    runner: Arc<dyn WorkflowScriptRunner>,
}

impl WorkflowSandbox {
    /// The `args` global.
    pub fn args(&self) -> &Json {
        &self.args
    }

    /// The nesting depth of this frame (0 = top level).
    pub fn depth(&self) -> u32 {
        self.depth
    }

    /// `budget.total` — the output-token ceiling, or `None` (unlimited).
    pub fn budget_total(&self) -> Option<i64> {
        self.shared.budget_total
    }

    /// `budget.spent()` — output tokens spent so far.
    pub fn budget_spent(&self) -> u64 {
        self.shared.spent.load(Ordering::SeqCst)
    }

    /// `budget.remaining()` — `+Infinity` when unlimited, else `max(0, total -
    /// spent)`.
    pub fn budget_remaining(&self) -> f64 {
        match self.shared.budget_total {
            None => f64::INFINITY,
            Some(total) => {
                let spent = self.shared.spent.load(Ordering::SeqCst) as i64;
                (total - spent).max(0) as f64
            }
        }
    }

    /// `phase(title)` — emit a `phase: <title>` status.
    pub fn phase(&self, title: &Json) {
        emit_status(
            &self.shared.deps,
            RuntimeString::from_utf8("phase: ").concat(&js_string(title)),
        );
    }

    /// `log(message)` — emit a status.
    pub fn log(&self, message: &Json) {
        emit_status(&self.shared.deps, js_string(message));
    }

    /// The single chokepoint: budget, journal cache, caps, spawn, accounting.
    pub fn agent(
        &self,
        prompt: Json,
        opts: Json,
    ) -> BoxFuture<'static, Result<Json, WorkflowError>> {
        if let Some(error) = journal_failure(&self.shared) {
            return ready_err(error);
        }
        let prompt_str = match prompt.as_str() {
            Some(s) => s.clone(),
            None => {
                return ready_err(WorkflowError::type_error(
                    "agent(prompt) expects a string prompt",
                ));
            }
        };
        if self.shared.operation_signal.is_cancelled() {
            return ready_err(WorkflowError::new("workflow was aborted"));
        }
        if let Some(value) = opts.get("effort") {
            if <crate::schemas::effort::EffortValue as crate::runtime_values::RuntimeDecode>::decode_runtime(value).is_err() {
                return ready_err(WorkflowError::type_error("agent effort must be a named effort or integer"));
            }
        }
        if opts
            .get("ultraMode")
            .is_some_and(|value| value.as_bool().is_none())
        {
            return ready_err(WorkflowError::type_error(
                "agent ultraMode must be a boolean",
            ));
        }
        // Budget is a hard ceiling on EVERY call (cached or live): once spend
        // has reached the total, any further agent() throws — checked before the
        // cache lookup so a resume can't silently overspend by replaying cached
        // results.
        if let Some(total) = self.shared.budget_total {
            if (self.shared.spent.load(Ordering::SeqCst) as i64) >= total {
                return ready_err(WorkflowError::new("workflow token budget exhausted"));
            }
        }

        let idx = self.shared.launch_counter.fetch_add(1, Ordering::SeqCst);
        let hash = hash_call(&prompt_str, &opts);

        // Cached prefix: a matching entry returns instantly (no spawn, no fresh
        // spend). A resume intentionally preserves completed results even when
        // the parent's inherited model/effort defaults changed. Those defaults
        // apply to newly launched calls only; explicit opts.effort is part of
        // the hash and changing it invalidates the completed prefix.
        if !self.shared.prefix_broken.load(Ordering::SeqCst) {
            if let Some(cached) = self.shared.cache.get(&idx) {
                if cached.hash == hash {
                    self.shared
                        .spent
                        .fetch_add(cached.output_tokens, Ordering::SeqCst);
                    emit_status(
                        &self.shared.deps,
                        RuntimeString::from(format!("agent#{idx}: cached")),
                    );
                    return ready_ok(cached.result.clone());
                }
            }
            // First divergence (edited/new call) breaks the prefix: this and all
            // subsequent calls run live, even if a later call happens to still
            // match.
            self.shared.prefix_broken.store(true, Ordering::SeqCst);
        }

        if self.shared.total_agents.load(Ordering::SeqCst) >= TOTAL_AGENT_CAP {
            return ready_err(WorkflowError::new(format!(
                "workflow exceeded the {TOTAL_AGENT_CAP}-agent limit"
            )));
        }
        self.shared.total_agents.fetch_add(1, Ordering::SeqCst);

        let shared = Arc::clone(&self.shared);
        Box::pin(async move {
            // The returned future is only a waiter. Once polled, one registered
            // operation owns the child and its checkpoint even if a script
            // returns or throws while this waiter is pending.
            let registration = shared
                .operation_tasks
                .reserve()
                .map_err(|error| WorkflowError::new(error.message))?;
            let (send_result, receive_result) = tokio::sync::oneshot::channel();
            let _task = registration.spawn(async move {
                let mut send_result = Some(send_result);
                let outcome = async {

            let _permit = tokio::select! {
                biased;
                _ = shared.operation_signal.cancelled() => {
                    return Err(WorkflowError::new("Workflow agent execution was interrupted before admission."));
                }
                permit = shared.limit.acquire() => permit
                    .map_err(|_| WorkflowError::new("Workflow concurrency semaphore closed."))?,
            };
            if shared.operation_signal.is_cancelled() {
                return Err(WorkflowError::new("Workflow agent execution was interrupted before admission."));
            }
            if let Some(error) = journal_failure(&shared) {
                return Err(error);
            }
            let spawn_opts = build_spawn_opts(&prompt_str, &opts);
            let label = label_for(&opts, idx);
            emit_status(
                &shared.deps,
                RuntimeString::from_utf8("agent ▸ ").concat(&label),
            );
            let mut final_text = RuntimeString::default();
            let mut failure = None;
            let mut terminal_seen = false;
            let mut out: u64 = 0;
            if shared.operation_signal.is_cancelled() {
                return Err(WorkflowError::new("Workflow agent execution was interrupted before admission."));
            }
            let mut stream = (shared.deps.spawn_workflow_agent)(spawn_opts);
            loop {
                let ev = {
                    tokio::select! {
                        biased;
                        _ = shared.operation_signal.cancelled() => {
                            failure = Some("Workflow agent execution was interrupted.".into());
                            break;
                        }
                        event = stream.next() => match event {
                            Some(event) => event,
                            None => break,
                        },
                    }
                };
                match ev {
                    EngineEvent::ItemCompleted {
                        inference: _,
                        item: ThreadItem::AgentMessage { text, .. },
                    } => {
                        final_text = text;
                    }
                    EngineEvent::TurnFailed { error, .. } => {
                        terminal_seen = true;
                        failure = Some(if error.message.is_empty() {
                            "Workflow agent failed without an error message.".into()
                        } else {
                            error.message
                        });
                    }
                    EngineEvent::TurnCompleted {
                        usage, stop_reason, ..
                    } => {
                        terminal_seen = true;
                        if let Some(usage) = usage {
                            out += usage.output_tokens;
                        }
                        if stop_reason != TerminalReason::Completed && failure.is_none() {
                            failure = crate::tools::subagent_stream::terminal_failure(stop_reason);
                        }
                        // The terminal carries the complete result and usage.
                        // Checkpoint it before any further stream-cleanup await.
                        break;
                    }
                    _ => {}
                }
            }
            shared.spent.fetch_add(out, Ordering::SeqCst);
            if !terminal_seen && failure.is_none() {
                failure = Some("Workflow agent stream ended without a terminal event.".into());
            }
            if let Some(failure) = failure {
                return Err(WorkflowError::new(
                    crate::tools::subagent_stream::incomplete_background_output(
                        &failure,
                        &final_text,
                    ),
                ));
            }
            let schema_present = matches!(opts.get("schema"), Some(v) if !v.is_null());
            let result: Json = if schema_present {
                safe_parse_json(&final_text)
            } else {
                Json::String(final_text)
            };
            if let Some(journal) = &shared.deps.journal {
                let entry = WorkflowJournalEntry {
                    run_id: shared.run_id.clone(),
                    idx,
                    hash,
                    result: result.clone(),
                    output_tokens: out,
                };
                if let Err(error) = journal.append(entry.clone()).await {
                    let failure = journal_append_failure(&entry, error);
                    shared
                        .journal_failures
                        .lock()
                        .expect("workflow journal failures")
                        .push(failure.clone());
                    shared.operation_tasks.record_failure(failure.message.clone());
                    return Err(failure);
                }
            }
            if let Some(send) = send_result.take() { let _ = send.send(Ok(result.clone())); }
            // Keep the concurrency permit until the child's cleanup has run.
            // A finished or abandoned script cancels cleanup without cancelling
            // the completed checkpoint that this operation already owns.
            loop {
                tokio::select! {
                    biased;
                    event = stream.next() => { if event.is_none() { break; } },
                    _ = shared.operation_signal.cancelled() => break,
                }
                if shared.operation_signal.is_cancelled() { break; }
                tokio::task::yield_now().await;
            }
            Ok(result)
            }.await;
            if let Some(send) = send_result.take() { let _ = send.send(outcome); }
            Ok::<(), crate::types::errors::ToolError>(())
            });
            receive_result.await.map_err(|_| {
                WorkflowError::new("Workflow agent owner ended before reporting its result")
            })?
        })
    }

    /// `parallel(thunks)`: barrier; each failed thunk returns an explicit error
    /// value. Thunks are invoked in array order so each thunk's first
    /// `agent()` launches deterministically.
    pub fn parallel(
        &self,
        thunks: Option<Vec<Thunk>>,
    ) -> BoxFuture<'static, Result<Vec<Json>, WorkflowError>> {
        let thunks = match thunks {
            None => {
                return Box::pin(async {
                    Err(WorkflowError::type_error(
                        "parallel(thunks) expects an array of functions",
                    ))
                });
            }
            Some(v) => v,
        };
        if thunks.len() > PER_CALL_ITEM_CAP {
            let n = thunks.len();
            return Box::pin(async move {
                Err(WorkflowError::new(format!(
                    "parallel: {n} items exceeds the {PER_CALL_ITEM_CAP} cap"
                )))
            });
        }
        let futs: Vec<BoxFuture<'static, Json>> = thunks.into_iter().map(invoke_thunk).collect();
        Box::pin(async move { Ok(join_all(futs).await) })
    }

    /// `pipeline(items, ...stages)`: each item flows through all stages
    /// independently (no barrier between stages); a throwing stage returns an
    /// explicit error value for that item.
    pub fn pipeline(
        &self,
        items: Option<Vec<Json>>,
        stages: Vec<Stage>,
    ) -> BoxFuture<'static, Result<Vec<Json>, WorkflowError>> {
        let items = match items {
            None => {
                return Box::pin(async {
                    Err(WorkflowError::type_error(
                        "pipeline(items, ...stages) expects an array of items",
                    ))
                });
            }
            Some(v) => v,
        };
        if items.len() > PER_CALL_ITEM_CAP {
            let n = items.len();
            return Box::pin(async move {
                Err(WorkflowError::new(format!(
                    "pipeline: {n} items exceeds the {PER_CALL_ITEM_CAP} cap"
                )))
            });
        }
        let stages = Arc::new(stages);
        let futs: Vec<_> = items
            .into_iter()
            .enumerate()
            .map(|(i, item)| run_chain(item, i, Arc::clone(&stages)))
            .collect();
        Box::pin(async move { Ok(join_all(futs).await) })
    }

    /// `workflow(nameOrRef, args)`: run a resolved workflow inline; one level of
    /// nesting only. Shares the parent's limiter / counters / budget / journal /
    /// abort.
    pub fn workflow(
        &self,
        name_or_ref: Json,
        child_args: Json,
    ) -> BoxFuture<'static, Result<Json, WorkflowError>> {
        if self.depth >= 1 {
            return Box::pin(async {
                Err(WorkflowError::new("workflow() nesting is one level only"))
            });
        }
        let name = match name_or_ref.as_str() {
            Some(s) => s.clone(),
            None => {
                return Box::pin(async {
                    Err(WorkflowError::type_error(
                        "workflow(nameOrRef) expects a string name or ref",
                    ))
                });
            }
        };
        let child_source = self
            .shared
            .deps
            .resolve_workflow
            .as_ref()
            .and_then(|r| r(&name));
        let child_source = match child_source {
            Some(s) => s,
            None => {
                let name = runtime_diagnostic(&name);
                return Box::pin(async move {
                    Err(WorkflowError::new(format!("unknown workflow: {name}")))
                });
            }
        };
        let shared = Arc::clone(&self.shared);
        let runner = Arc::clone(&self.runner);
        let depth = self.depth + 1;
        Box::pin(async move {
            let (result, _meta) =
                execute_script(&child_source, child_args, shared, depth, runner).await?;
            Ok(result)
        })
    }
}

/// Run a workflow script to completion, returning its `return` value.
pub async fn run_workflow(
    input: WorkflowRunInput,
    deps: WorkflowRuntimeDeps,
    runner: Arc<dyn WorkflowScriptRunner>,
) -> Result<WorkflowRunResult, WorkflowError> {
    let run_id = input
        .resume_from_run_id
        .clone()
        .unwrap_or_else(|| RuntimeString::from(format!("wf_{}", random_uuid())));
    let mut cache: HashMap<u64, WorkflowJournalEntry> = HashMap::new();
    if let (Some(rid), Some(journal)) = (input.resume_from_run_id.as_ref(), deps.journal.as_ref()) {
        // Last write wins per idx, so a re-run's fresh miss supersedes a stale
        // entry.
        for e in journal.prior(rid) {
            cache.insert(e.idx, e);
        }
    }
    let concurrency = deps
        .max_concurrency
        .unwrap_or_else(default_concurrency)
        .max(1);
    let operation_signal = deps.signal.child_token();
    let _run_owner = WorkflowRunOwner(operation_signal.clone());
    let operation_tasks = deps
        .journal
        .as_ref()
        .and_then(|journal| journal.operation_tasks())
        .unwrap_or_else(|| Arc::new(crate::engine::query::RetainedToolTasks::new()));
    let shared = Arc::new(SharedState {
        operation_signal,
        operation_tasks,
        run_id: run_id.clone(),
        limit: Semaphore::new(concurrency),
        deps,
        budget_total: input.budget_tokens,
        cache,
        launch_counter: AtomicU64::new(0),
        total_agents: AtomicU64::new(0),
        spent: AtomicU64::new(0),
        prefix_broken: AtomicBool::new(false),
        journal_failures: Mutex::new(Vec::new()),
    });
    let outcome = execute_script(
        &input.script,
        input.args.clone().unwrap_or(Json::Null),
        Arc::clone(&shared),
        0,
        runner,
    )
    .await;
    if let Some(error) = journal_failure(&shared) {
        return Err(error);
    }
    let (result, meta) = outcome?;
    Ok(WorkflowRunResult {
        run_id,
        result,
        meta,
        agents_spawned: shared.total_agents.load(Ordering::SeqCst),
        output_tokens_spent: shared.spent.load(Ordering::SeqCst),
    })
}

/// Parse + run one script (top-level or a nested `workflow()`), returning its
/// value + meta.
async fn execute_script(
    source: &RuntimeString,
    args: Json,
    shared: Arc<SharedState>,
    depth: u32,
    runner: Arc<dyn WorkflowScriptRunner>,
) -> Result<(Json, WorkflowMeta), WorkflowError> {
    let parsed = parse_workflow(source)?;
    let process_context = shared.deps.process_context.clone();
    let sandbox = Arc::new(WorkflowSandbox {
        shared,
        depth,
        args: args.clone(),
        runner: Arc::clone(&runner),
    });
    let ctx = WorkflowScriptContext {
        process_context,
        meta: parsed.meta.clone(),
        wrapped_source: parsed.wrapped_source,
        line_offset: parsed.line_offset,
        args,
        sandbox,
    };
    let raw = runner.run(ctx).await?;
    Ok((raw, parsed.meta))
}

// ---- primitives + helpers ---------------------------------------------------

fn journal_append_failure(entry: &WorkflowJournalEntry, error: std::io::Error) -> WorkflowError {
    let checkpoint = crate::runtime_values::runtime_codec::encode_value(entry)
        .expect("workflow journal entry must encode");
    WorkflowError::new(
        RuntimeString::from(format!(
            "Workflow agent completed, but its journal checkpoint failed: {error}. \
             Its external effects may already be applied; do not automatically rerun this call. \
             Its replay checkpoint was not confirmed.\nCompleted checkpoint: "
        ))
        .concat(&runtime_json::stringify(&checkpoint)),
    )
}

fn journal_failure(shared: &SharedState) -> Option<WorkflowError> {
    let failures = shared
        .journal_failures
        .lock()
        .expect("workflow journal failures");
    let mut failures = failures.iter();
    let mut message = failures.next()?.message.clone();
    for failure in failures {
        message = message
            .concat(&RuntimeString::from("\n\n"))
            .concat(&failure.message);
    }
    Some(WorkflowError::new(message))
}

fn ready_ok(v: Json) -> BoxFuture<'static, Result<Json, WorkflowError>> {
    Box::pin(async move { Ok(v) })
}

fn ready_err(e: WorkflowError) -> BoxFuture<'static, Result<Json, WorkflowError>> {
    Box::pin(async move { Err(e) })
}

fn workflow_error_result(error: WorkflowError) -> Json {
    let mut object = RuntimeObject::new();
    object.insert("ok", Json::Bool(false));
    object.insert("error", Json::String(error.message));
    Json::Object(object)
}

/// A single `parallel` element preserves a failed thunk's reason.
fn invoke_thunk(t: Thunk) -> BoxFuture<'static, Json> {
    match t {
        Thunk::NonFunction => Box::pin(async { Json::Null }),
        Thunk::Fn(f) => {
            // Invoke synchronously so the thunk's first agent() records its
            // launch index in array order.
            let fut = f();
            Box::pin(async move {
                match fut.await {
                    Ok(v) => v,
                    Err(error) => workflow_error_result(error),
                }
            })
        }
    }
}

async fn run_chain(item: Json, index: usize, stages: Arc<Vec<Stage>>) -> Json {
    let mut cur = item.clone();
    for stage in stages.iter() {
        match stage(cur, item.clone(), index).await {
            Ok(v) => cur = v,
            Err(error) => return workflow_error_result(error),
        }
    }
    cur
}

fn build_spawn_opts(prompt: &RuntimeString, opts: &Json) -> SpawnSubAgentOptions {
    let mut o = SpawnSubAgentOptions {
        prompt: prompt.clone(),
        ..Default::default()
    };
    if let Some(s) = opts.get("agentType").and_then(Json::as_string) {
        o.agent_type = Some(s.clone());
    }
    if let Some(s) = opts.get("model").and_then(Json::as_string) {
        o.model = Some(s.clone());
    }
    if let Some(effort) = opts.get("effort") {
        o.effort = <crate::schemas::effort::EffortValue as crate::runtime_values::RuntimeDecode>::decode_runtime(effort).ok();
    }
    o.ultra_mode = opts.get("ultraMode").and_then(Json::as_bool);
    crate::schemas::effort::normalize_legacy_ultra(&mut o.effort, &mut o.ultra_mode);
    if let Some(Json::Object(schema)) = opts.get("schema") {
        o.output_schema = Some(schema.clone());
    }
    if opts
        .get("isolation")
        .and_then(Json::as_string)
        .is_some_and(|value| value == "worktree")
    {
        o.isolation = Some(Isolation::Worktree);
    }
    o
}

fn label_for(opts: &Json, idx: u64) -> RuntimeString {
    if let Some(s) = opts.get("label").and_then(Json::as_string) {
        return s.clone();
    }
    if let Some(s) = opts.get("phase").and_then(Json::as_string) {
        return s.clone();
    }
    RuntimeString::from(format!("agent#{idx}"))
}

fn emit_status(deps: &WorkflowRuntimeDeps, message: RuntimeString) {
    if let Some(emit) = &deps.emit {
        emit(ToolDomainEvent::Status { message });
    }
}

fn hash_call(prompt: &RuntimeString, opts: &Json) -> RuntimeString {
    let mut norm = RuntimeObject::new();
    norm.insert(
        "agentType",
        opts.get("agentType")
            .and_then(Json::as_string)
            .cloned()
            .map(Json::String)
            .unwrap_or(Json::Null),
    );
    norm.insert(
        "model",
        opts.get("model")
            .and_then(Json::as_string)
            .cloned()
            .map(Json::String)
            .unwrap_or(Json::Null),
    );
    if let Some(ultra_mode) = opts.get("ultraMode") {
        norm.insert("ultraMode", ultra_mode.clone());
    }
    // Omitted effort retains the historical hash; an explicit effort changes
    // the computation and must invalidate the journal prefix on resume.
    if let Some(effort) = opts.get("effort") {
        norm.insert("effort", effort.clone());
    }
    norm.insert(
        "isolation",
        if opts
            .get("isolation")
            .and_then(Json::as_string)
            .is_some_and(|value| value == "worktree")
        {
            Json::String(RuntimeString::from("worktree"))
        } else {
            Json::Null
        },
    );
    norm.insert(
        "schema",
        match opts.get("schema") {
            Some(v @ Json::Object(_)) => v.clone(),
            _ => Json::Null,
        },
    );
    let norm = Json::Object(norm);
    let mut hasher = Sha256::new();
    // Node's `Hash.update(string)` is the actual byte host boundary for this
    // externally visible call hash. Apply its UTF-8 replacement projection
    // exactly once here; all normalization and JSON writing above remain exact
    // UTF-16.
    hasher.update(prompt.to_node_utf8());
    hasher.update(b" ");
    hasher.update(stable_stringify(&norm).to_node_utf8());
    let digest = hasher.finalize();
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut units = Vec::with_capacity(16);
    for byte in digest.iter().take(8).copied() {
        units.push(u16::from(HEX[usize::from(byte >> 4)]));
        units.push(u16::from(HEX[usize::from(byte & 0x0f)]));
    }
    RuntimeString::from_utf16(units)
}

fn safe_parse_json(text: &RuntimeString) -> Json {
    runtime_json::parse(text).unwrap_or(Json::Null)
}

/// Deterministic JSON for hashing (object keys sorted recursively, by UTF-16
/// code unit order).
fn stable_stringify(value: &Json) -> RuntimeString {
    match value {
        Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_) => {
            runtime_json::stringify(value)
        }
        Json::Array(values) => {
            let mut output = vec![u16::from(b'[')];
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(u16::from(b','));
                }
                output.extend_from_slice(stable_stringify(value).as_utf16());
            }
            output.push(u16::from(b']'));
            RuntimeString::from_utf16(output)
        }
        Json::Object(object) => {
            let mut entries = object.entries().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            let mut output = vec![u16::from(b'{')];
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(u16::from(b','));
                }
                output.extend_from_slice(runtime_json::stringify(&Json::String(key)).as_utf16());
                output.push(u16::from(b':'));
                output.extend_from_slice(stable_stringify(value).as_utf16());
            }
            output.push(u16::from(b'}'));
            RuntimeString::from_utf16(output)
        }
    }
}

/// The string coercion `phase` / `log` apply to their argument.
fn js_string(value: &Json) -> RuntimeString {
    match value {
        Json::Null => RuntimeString::from("null"),
        Json::Bool(b) => {
            if *b {
                RuntimeString::from("true")
            } else {
                RuntimeString::from("false")
            }
        }
        Json::Number(number) => js_number_string(*number),
        Json::String(string) => string.clone(),
        Json::Array(values) => {
            let mut output = Vec::new();
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(u16::from(b','));
                }
                if !value.is_null() {
                    output.extend_from_slice(js_string(value).as_utf16());
                }
            }
            RuntimeString::from_utf16(output)
        }
        Json::Object(_) => RuntimeString::from("[object Object]"),
    }
}

fn js_number_string(number: RuntimeNumber) -> RuntimeString {
    let value = number.as_f64();
    if value.is_nan() {
        RuntimeString::from("NaN")
    } else if value == f64::INFINITY {
        RuntimeString::from("Infinity")
    } else if value == f64::NEG_INFINITY {
        RuntimeString::from("-Infinity")
    } else {
        RuntimeString::from(number.to_json_token())
    }
}

/// Workflow errors remain scalar host diagnostics. Render exact runtime strings
/// injectively instead of applying Display or a replacement conversion.
fn runtime_diagnostic(value: &RuntimeString) -> String {
    let mut output = String::new();
    let units = value.as_utf16();
    let mut index = 0;
    while index < units.len() {
        let unit = units[index];
        if (0xd800..=0xdbff).contains(&unit)
            && units
                .get(index + 1)
                .is_some_and(|low| (0xdc00..=0xdfff).contains(low))
        {
            let low = units[index + 1];
            let point = 0x10000 + ((u32::from(unit) - 0xd800) << 10) + (u32::from(low) - 0xdc00);
            output.push(char::from_u32(point).expect("valid surrogate pair"));
            index += 2;
            continue;
        }
        if (0xd800..=0xdfff).contains(&unit) {
            use std::fmt::Write as _;
            write!(&mut output, "\\u{unit:04x}").expect("writing to String cannot fail");
        } else {
            output.push(char::from_u32(u32::from(unit)).expect("non-surrogate UTF-16 unit"));
        }
        index += 1;
    }
    output
}

fn random_uuid() -> String {
    use rand::RngCore;
    let mut b = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // variant
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0],
        b[1],
        b[2],
        b[3],
        b[4],
        b[5],
        b[6],
        b[7],
        b[8],
        b[9],
        b[10],
        b[11],
        b[12],
        b[13],
        b[14],
        b[15]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::make_usage;
    use crate::types::errors::{EngineError, EngineErrorCode, TerminalReason};
    use crate::types::tool::EngineEventStream;
    use std::sync::Mutex;

    const VALID_SCRIPT: &str =
        "export const meta = { name: 'demo', description: 'd' }\nreturn null";

    fn exact_json(source: &str) -> Json {
        runtime_json::parse(&RuntimeString::from_utf8(source))
            .unwrap_or_else(|error| panic!("invalid exact test JSON {source:?}: {error}"))
    }

    macro_rules! exact_json {
        ($($json:tt)+) => {
            exact_json(stringify!($($json)+))
        };
    }

    fn runtime_object<const N: usize>(entries: [(&'static str, Json); N]) -> Json {
        let mut object = RuntimeObject::new();
        for (key, value) in entries {
            object.insert(key, value);
        }
        Json::Object(object)
    }

    type RunnerFn = Arc<
        dyn Fn(Arc<WorkflowSandbox>) -> BoxFuture<'static, Result<Json, WorkflowError>>
            + Send
            + Sync,
    >;

    struct MockRunner(RunnerFn);

    #[async_trait]
    impl WorkflowScriptRunner for MockRunner {
        async fn run(&self, ctx: WorkflowScriptContext) -> Result<Json, WorkflowError> {
            (self.0)(ctx.sandbox).await
        }
    }

    fn mock(f: RunnerFn) -> Arc<dyn WorkflowScriptRunner> {
        Arc::new(MockRunner(f))
    }

    struct SourceCaptureRunner(Arc<Mutex<Option<RuntimeString>>>);

    #[async_trait]
    impl WorkflowScriptRunner for SourceCaptureRunner {
        async fn run(&self, ctx: WorkflowScriptContext) -> Result<Json, WorkflowError> {
            *self.0.lock().unwrap() = Some(ctx.wrapped_source);
            Ok(Json::Null)
        }
    }

    #[derive(Default)]
    struct MemJournal {
        entries: Mutex<Vec<WorkflowJournalEntry>>,
    }

    #[async_trait]
    impl WorkflowJournal for MemJournal {
        async fn append(&self, e: WorkflowJournalEntry) -> std::io::Result<()> {
            self.entries.lock().unwrap().push(e);
            Ok(())
        }
        fn prior(&self, run_id: &RuntimeString) -> Vec<WorkflowJournalEntry> {
            self.entries
                .lock()
                .unwrap()
                .iter()
                .filter(|e| &e.run_id == run_id)
                .cloned()
                .collect()
        }
    }

    /// A spawner that yields a fixed agent_message (or a turn.failed) plus a
    /// turn.completed carrying `out` output tokens.
    fn spawn_yielding(text: &'static str, out: u64, fail: bool) -> SpawnSubAgent {
        Arc::new(move |_opts: SpawnSubAgentOptions| {
            let mut events: Vec<EngineEvent> = Vec::new();
            if fail {
                events.push(EngineEvent::TurnFailed {
                    inference: None,
                    error: EngineError {
                        code: EngineErrorCode::Internal,
                        message: "boom".into(),
                        cause: None,
                        request_id: None,
                        provider_kind: None,
                        retry_after_ms: None,
                    },
                });
            } else {
                events.push(EngineEvent::ItemCompleted {
                    inference: None,
                    item: ThreadItem::AgentMessage {
                        phase: None,
                        id: "m".into(),
                        text: text.into(),
                    },
                });
            }
            events.push(EngineEvent::TurnCompleted {
                turn_id: "wf".into(),
                usage: Some(make_usage(None, Some(out))),
                stop_reason: TerminalReason::Completed,
            });
            Box::pin(futures::stream::iter(events)) as EngineEventStream
        })
    }

    fn empty_spawn() -> SpawnSubAgent {
        Arc::new(|_opts: SpawnSubAgentOptions| {
            Box::pin(futures::stream::empty::<EngineEvent>()) as EngineEventStream
        })
    }

    fn deps(
        spawn: SpawnSubAgent,
        journal: Option<Arc<dyn WorkflowJournal>>,
        signal: AbortSignal,
        resolve: Option<ResolveWorkflowFn>,
    ) -> WorkflowRuntimeDeps {
        WorkflowRuntimeDeps {
            process_context: crate::runtime_process::test_runtime_process_context(),
            spawn_workflow_agent: spawn,
            journal,
            emit: None,
            resolve_workflow: resolve,
            signal,
            max_concurrency: Some(4),
        }
    }

    fn input(script: &str, budget: Option<i64>, resume: Option<&str>) -> WorkflowRunInput {
        WorkflowRunInput {
            script: script.into(),
            args: None,
            resume_from_run_id: resume.map(RuntimeString::from),
            budget_tokens: budget,
        }
    }

    #[tokio::test]
    async fn runs_agents_and_accounts_output_tokens() {
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move {
                let a = sb.agent(exact_json!("p1"), exact_json!({})).await?;
                let b = sb.agent(exact_json!("p2"), exact_json!({})).await?;
                Ok(Json::Array(vec![a, b]))
            })
        }));
        let res = run_workflow(
            input(VALID_SCRIPT, None, None),
            deps(
                spawn_yielding("hello", 7, false),
                None,
                AbortSignal::new(),
                None,
            ),
            runner,
        )
        .await
        .unwrap();
        assert_eq!(res.agents_spawned, 2);
        assert_eq!(res.output_tokens_spent, 14);
        assert_eq!(res.result, exact_json!(["hello", "hello"]));
        assert_eq!(res.meta.name, "demo");
        assert!(res.run_id.starts_with("wf_"));
    }

    #[tokio::test]
    async fn schema_option_parses_agent_json() {
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move {
                sb.agent(
                    exact_json!("p"),
                    exact_json!({ "schema": { "type": "object" } }),
                )
                .await
            })
        }));
        let res = run_workflow(
            input(VALID_SCRIPT, None, None),
            deps(
                spawn_yielding("{\"k\":1}", 2, false),
                None,
                AbortSignal::new(),
                None,
            ),
            runner,
        )
        .await
        .unwrap();
        assert_eq!(res.result, exact_json!({ "k": 1 }));
    }

    #[tokio::test]
    async fn budget_ceiling_rejects_further_agents() {
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move {
                let _ = sb.agent(exact_json!("p1"), exact_json!({})).await?; // spends 10
                sb.agent(exact_json!("p2"), exact_json!({})).await?; // budget now exhausted
                Ok(Json::Null)
            })
        }));
        let err = run_workflow(
            input(VALID_SCRIPT, Some(10), None),
            deps(
                spawn_yielding("x", 10, false),
                None,
                AbortSignal::new(),
                None,
            ),
            runner,
        )
        .await
        .unwrap_err();
        assert_eq!(err.message, "workflow token budget exhausted");
    }

    #[tokio::test]
    async fn resume_returns_cached_result_without_spawning() {
        let hash = hash_call(&RuntimeString::from("p1"), &exact_json!({}));
        let journal = Arc::new(MemJournal::default());
        journal
            .append(WorkflowJournalEntry {
                run_id: "wf_resume".into(),
                idx: 0,
                hash,
                result: exact_json!("cached-value"),
                output_tokens: 5,
            })
            .await
            .unwrap();
        // A spawner that panics if ever called (a cache hit must not spawn).
        let spawn: SpawnSubAgent = Arc::new(|_o: SpawnSubAgentOptions| {
            panic!("cache hit must not spawn");
        });
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move { sb.agent(exact_json!("p1"), exact_json!({})).await })
        }));
        let res = run_workflow(
            input(VALID_SCRIPT, None, Some("wf_resume")),
            deps(spawn, Some(journal), AbortSignal::new(), None),
            runner,
        )
        .await
        .unwrap();
        assert_eq!(res.result, exact_json!("cached-value"));
        assert_eq!(res.output_tokens_spent, 5);
        assert_eq!(res.agents_spawned, 0);
        assert_eq!(res.run_id, "wf_resume");
    }

    #[tokio::test]
    async fn parallel_preserves_failed_agent_reason_and_non_function_null() {
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move {
                let sb2 = sb.clone();
                let thunks = vec![
                    Thunk::Fn(Box::new(move || {
                        sb2.agent(exact_json!("a"), exact_json!({}))
                    })),
                    Thunk::NonFunction,
                ];
                let out = sb.parallel(Some(thunks)).await?;
                Ok(Json::Array(out))
            })
        }));
        let res = run_workflow(
            input(VALID_SCRIPT, None, None),
            deps(
                spawn_yielding("ok", 3, true),
                None,
                AbortSignal::new(),
                None,
            ),
            runner,
        )
        .await
        .unwrap();
        assert_eq!(
            res.result,
            exact_json!([{"ok":false,"error":"[failed] boom"}, null])
        );
        assert_eq!(res.agents_spawned, 1);
        assert_eq!(res.output_tokens_spent, 3);
    }

    #[tokio::test]
    async fn parallel_non_array_rejects() {
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move { sb.parallel(None).await.map(Json::Array) })
        }));
        let err = run_workflow(
            input(VALID_SCRIPT, None, None),
            deps(empty_spawn(), None, AbortSignal::new(), None),
            runner,
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.message,
            "parallel(thunks) expects an array of functions"
        );
    }

    #[tokio::test]
    async fn parallel_over_cap_rejects() {
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move {
                let thunks: Vec<Thunk> = (0..PER_CALL_ITEM_CAP + 1)
                    .map(|_| Thunk::NonFunction)
                    .collect();
                sb.parallel(Some(thunks)).await.map(Json::Array)
            })
        }));
        let err = run_workflow(
            input(VALID_SCRIPT, None, None),
            deps(empty_spawn(), None, AbortSignal::new(), None),
            runner,
        )
        .await
        .unwrap_err();
        assert_eq!(err.message, "parallel: 4097 items exceeds the 4096 cap");
    }

    #[tokio::test]
    async fn pipeline_flows_items_and_preserves_thrown_stage_reason() {
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move {
                let items = vec![exact_json!(1), exact_json!(2), exact_json!(3)];
                let stage1: Stage = Box::new(|cur: Json, _item: Json, _i: usize| {
                    let n = cur.as_i64().unwrap_or(0);
                    Box::pin(async move { Ok(Json::from(n * 2)) })
                        as BoxFuture<'static, Result<Json, WorkflowError>>
                });
                let stage2: Stage = Box::new(|cur: Json, _item: Json, _i: usize| {
                    let n = cur.as_i64().unwrap_or(0);
                    Box::pin(async move {
                        if n == 4 {
                            Err(WorkflowError::new("drop"))
                        } else {
                            Ok(Json::from(n + 1))
                        }
                    }) as BoxFuture<'static, Result<Json, WorkflowError>>
                });
                let out = sb.pipeline(Some(items), vec![stage1, stage2]).await?;
                Ok(Json::Array(out))
            })
        }));
        let res = run_workflow(
            input(VALID_SCRIPT, None, None),
            deps(empty_spawn(), None, AbortSignal::new(), None),
            runner,
        )
        .await
        .unwrap();
        assert_eq!(res.result, exact_json!([3, {"ok":false,"error":"drop"}, 7]));
    }

    #[tokio::test]
    async fn aborted_signal_rejects_agent() {
        let signal = AbortSignal::new();
        signal.cancel();
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move {
                sb.agent(exact_json!("p"), exact_json!({})).await?;
                Ok(Json::Null)
            })
        }));
        let err = run_workflow(
            input(VALID_SCRIPT, None, None),
            deps(empty_spawn(), None, signal, None),
            runner,
        )
        .await
        .unwrap_err();
        assert_eq!(err.message, "workflow was aborted");
    }

    #[tokio::test]
    async fn nested_workflow_shares_counters() {
        let child = "export const meta = { name: 'child', description: 'c' }\nreturn null";
        let resolve: ResolveWorkflowFn = Arc::new(move |name: &RuntimeString| {
            if name == "child" {
                Some(child.into())
            } else {
                None
            }
        });
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move {
                if sb.depth() == 0 {
                    let a = sb.agent(exact_json!("p"), exact_json!({})).await?;
                    let c = sb.workflow(exact_json!("child"), Json::Null).await?;
                    Ok(runtime_object([("a", a), ("c", c)]))
                } else {
                    sb.agent(exact_json!("cp"), exact_json!({})).await
                }
            })
        }));
        let res = run_workflow(
            input(
                "export const meta = { name: 'p', description: 'd' }\nreturn null",
                None,
                None,
            ),
            deps(
                spawn_yielding("r", 2, false),
                None,
                AbortSignal::new(),
                Some(resolve),
            ),
            runner,
        )
        .await
        .unwrap();
        assert_eq!(res.agents_spawned, 2);
        assert_eq!(res.output_tokens_spent, 4);
        assert_eq!(res.result, exact_json!({ "a": "r", "c": "r" }));
    }

    #[tokio::test]
    async fn workflow_nesting_beyond_one_level_errors() {
        let child = "export const meta = { name: 'child', description: 'c' }\nreturn null";
        let resolve: ResolveWorkflowFn = Arc::new(move |name: &RuntimeString| {
            if name == "child" {
                Some(child.into())
            } else {
                None
            }
        });
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move {
                sb.workflow(exact_json!("child"), Json::Null).await?;
                Ok(Json::Null)
            })
        }));
        let err = run_workflow(
            input(
                "export const meta = { name: 'p', description: 'd' }\nreturn null",
                None,
                None,
            ),
            deps(empty_spawn(), None, AbortSignal::new(), Some(resolve)),
            runner,
        )
        .await
        .unwrap_err();
        assert_eq!(err.message, "workflow() nesting is one level only");
    }

    #[tokio::test]
    async fn unknown_nested_workflow_errors() {
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move { sb.workflow(exact_json!("missing"), Json::Null).await })
        }));
        let resolve: ResolveWorkflowFn = Arc::new(|_n: &RuntimeString| None);
        let err = run_workflow(
            input(VALID_SCRIPT, None, None),
            deps(empty_spawn(), None, AbortSignal::new(), Some(resolve)),
            runner,
        )
        .await
        .unwrap_err();
        assert_eq!(err.message, "unknown workflow: missing");
    }

    #[tokio::test]
    async fn non_string_prompt_rejects() {
        let runner = mock(Arc::new(|sb: Arc<WorkflowSandbox>| {
            Box::pin(async move { sb.agent(exact_json!(123), exact_json!({})).await })
        }));
        let err = run_workflow(
            input(VALID_SCRIPT, None, None),
            deps(empty_spawn(), None, AbortSignal::new(), None),
            runner,
        )
        .await
        .unwrap_err();
        assert_eq!(err.message, "agent(prompt) expects a string prompt");
    }

    #[test]
    fn exact_json_helpers_preserve_surrogates_and_ecmascript_order() {
        let parsed = safe_parse_json(&RuntimeString::from_utf8(
            r#"{"b":"\ud800","2":"two","1":"one","a":"\udc00","b":"\ud801"}"#,
        ));
        let object = parsed.as_object().expect("parsed object");
        let keys = object
            .keys()
            .map(|key| key.as_utf16().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                vec![u16::from(b'1')],
                vec![u16::from(b'2')],
                vec![u16::from(b'b')],
                vec![u16::from(b'a')],
            ]
        );
        assert_eq!(
            object
                .get("b")
                .and_then(Json::as_string)
                .expect("last duplicate value")
                .as_utf16(),
            &[0xd801]
        );
        assert_eq!(
            object
                .get("a")
                .and_then(Json::as_string)
                .expect("lone-low value")
                .as_utf16(),
            &[0xdc00]
        );

        // Stable hashing sorts keys by unsigned UTF-16, while the exact writer
        // escapes rather than replaces every unpaired surrogate.
        assert_eq!(
            stable_stringify(&parsed),
            RuntimeString::from_utf8(r#"{"1":"one","2":"two","a":"\udc00","b":"\ud801"}"#)
        );

        let coerced = js_string(&Json::Array(vec![
            Json::String(RuntimeString::from_utf16(vec![0xd800])),
            Json::Null,
            Json::String(RuntimeString::from_utf16(vec![0xdc00])),
        ]));
        assert_eq!(
            coerced.as_utf16(),
            &[0xd800, u16::from(b','), u16::from(b','), 0xdc00]
        );
        assert_eq!(
            js_string(&Json::Number(RuntimeNumber::from_f64(f64::NAN))),
            "NaN"
        );
        assert_eq!(
            js_string(&Json::Number(RuntimeNumber::from_f64(f64::INFINITY))),
            "Infinity"
        );
    }

    #[test]
    fn workflow_explicit_effort_is_applied_and_invalidates_resume_hash() {
        use crate::schemas::effort::{EffortLevel, EffortValue};
        let prompt = RuntimeString::from("inspect independently");
        let ultra = exact_json!({"effort":"ultra"});
        let low = exact_json!({"effort":"low"});
        let xhigh = exact_json!({"effort":"xhigh"});
        assert_eq!(
            build_spawn_opts(&prompt, &ultra).effort,
            Some(EffortValue::Level(EffortLevel::Ultra))
        );
        assert_eq!(
            build_spawn_opts(&prompt, &low).effort,
            Some(EffortValue::Level(EffortLevel::Low))
        );
        assert_eq!(build_spawn_opts(&prompt, &ultra).ultra_mode, Some(true));
        let explicit = exact_json!({"effort":"ultra", "ultraMode":false});
        assert_eq!(
            build_spawn_opts(&prompt, &explicit).effort,
            Some(EffortValue::Level(EffortLevel::Ultra))
        );
        assert_eq!(build_spawn_opts(&prompt, &explicit).ultra_mode, Some(false));
        assert_ne!(hash_call(&prompt, &ultra), hash_call(&prompt, &explicit));
        assert_ne!(hash_call(&prompt, &ultra), hash_call(&prompt, &low));
        assert_ne!(hash_call(&prompt, &ultra), hash_call(&prompt, &xhigh));
        assert_ne!(
            hash_call(&prompt, &ultra),
            hash_call(&prompt, &exact_json!({}))
        );
    }
    #[test]
    fn spawn_options_and_hash_keep_exact_runtime_carriers() {
        let prompt = RuntimeString::from_utf16(vec![u16::from(b'p'), 0xd800]);
        let agent_type = RuntimeString::from_utf16(vec![0xd801]);
        let model = RuntimeString::from_utf16(vec![0xdc01]);
        let label = RuntimeString::from_utf16(vec![u16::from(b'L'), 0xdc02]);

        let mut schema = RuntimeObject::new();
        schema.insert("9", Json::from(9));
        schema.insert("1", Json::from(1));
        schema.insert(
            RuntimeString::from_utf16(vec![0xd802]),
            Json::String(RuntimeString::from_utf16(vec![0xdc03])),
        );
        let mut opts = RuntimeObject::new();
        opts.insert("label", Json::String(label.clone()));
        opts.insert("agentType", Json::String(agent_type.clone()));
        opts.insert("model", Json::String(model.clone()));
        opts.insert("schema", Json::Object(schema));
        opts.insert("isolation", Json::String(RuntimeString::from("worktree")));
        let opts = Json::Object(opts);

        assert_eq!(label_for(&opts, 17), label);
        let built = build_spawn_opts(&prompt, &opts);
        assert_eq!(built.prompt, prompt);
        assert_eq!(built.agent_type, Some(agent_type));
        assert_eq!(built.model, Some(model));
        assert_eq!(built.isolation, Some(Isolation::Worktree));
        let schema = built.output_schema.expect("exact schema");
        assert_eq!(
            schema
                .keys()
                .map(|key| key.as_utf16().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![u16::from(b'1')], vec![u16::from(b'9')], vec![0xd802],]
        );
        assert_eq!(
            schema
                .get(RuntimeString::from_utf16(vec![0xd802]))
                .and_then(Json::as_string)
                .expect("surrogate schema property")
                .as_utf16(),
            &[0xdc03]
        );

        let mut high_schema = RuntimeObject::new();
        high_schema.insert(
            "value",
            Json::String(RuntimeString::from_utf16(vec![0xd800])),
        );
        let mut replacement_schema = RuntimeObject::new();
        replacement_schema.insert("value", Json::String(RuntimeString::from_utf8("�")));
        let high_opts = runtime_object([("schema", Json::Object(high_schema))]);
        let replacement_opts = runtime_object([("schema", Json::Object(replacement_schema))]);
        assert_ne!(
            hash_call(&RuntimeString::from("p"), &high_opts),
            hash_call(&RuntimeString::from("p"), &replacement_opts),
            "the exact stable writer must distinguish a surrogate from U+FFFD"
        );
    }

    #[tokio::test]
    async fn agent_status_result_and_resume_id_remain_exact_utf16() {
        type CapturedSpawn = (RuntimeString, Option<RuntimeString>, Option<RuntimeString>);
        let captured: Arc<Mutex<Option<CapturedSpawn>>> = Arc::new(Mutex::new(None));
        let captured_for_spawn = Arc::clone(&captured);
        let final_text = RuntimeString::from_utf16(vec![u16::from(b'r'), 0xdc04]);
        let final_text_for_spawn = final_text.clone();
        let spawn: SpawnSubAgent = Arc::new(move |opts: SpawnSubAgentOptions| {
            *captured_for_spawn.lock().unwrap() =
                Some((opts.prompt, opts.agent_type.clone(), opts.model.clone()));
            let events = vec![
                EngineEvent::ItemCompleted {
                    inference: None,
                    item: ThreadItem::AgentMessage {
                        phase: None,
                        id: "m".into(),
                        text: final_text_for_spawn.clone(),
                    },
                },
                EngineEvent::TurnCompleted {
                    turn_id: "wf".into(),
                    usage: None,
                    stop_reason: TerminalReason::Completed,
                },
            ];
            Box::pin(futures::stream::iter(events)) as EngineEventStream
        });

        let emitted = Arc::new(Mutex::new(Vec::<RuntimeString>::new()));
        let emitted_for_deps = Arc::clone(&emitted);
        let emit: Arc<dyn Fn(ToolDomainEvent) + Send + Sync> = Arc::new(move |event| {
            if let ToolDomainEvent::Status { message } = event {
                emitted_for_deps.lock().unwrap().push(message);
            }
        });

        let prompt = RuntimeString::from_utf16(vec![u16::from(b'p'), 0xd800]);
        let agent_type = RuntimeString::from_utf16(vec![0xd801]);
        let model = RuntimeString::from_utf16(vec![0xdc01]);
        let label = RuntimeString::from_utf16(vec![u16::from(b'L'), 0xdc02]);
        let prompt_for_runner = prompt.clone();
        let agent_type_for_runner = agent_type.clone();
        let model_for_runner = model.clone();
        let label_for_runner = label.clone();
        let runner = mock(Arc::new(move |sandbox: Arc<WorkflowSandbox>| {
            let prompt = prompt_for_runner.clone();
            let agent_type = agent_type_for_runner.clone();
            let model = model_for_runner.clone();
            let label = label_for_runner.clone();
            Box::pin(async move {
                sandbox.phase(&Json::String(prompt.clone()));
                let mut opts = RuntimeObject::new();
                opts.insert("agentType", Json::String(agent_type));
                opts.insert("model", Json::String(model));
                opts.insert("label", Json::String(label));
                sandbox
                    .agent(Json::String(prompt), Json::Object(opts))
                    .await
            })
        }));
        let resume = RuntimeString::from_utf16(vec![u16::from(b'w'), 0xd803]);
        let mut runtime_deps = deps(spawn, None, AbortSignal::new(), None);
        runtime_deps.emit = Some(emit);
        let result = run_workflow(
            WorkflowRunInput {
                script: VALID_SCRIPT.into(),
                args: None,
                resume_from_run_id: Some(resume.clone()),
                budget_tokens: None,
            },
            runtime_deps,
            runner,
        )
        .await
        .unwrap();

        assert_eq!(result.run_id, resume);
        assert_eq!(result.result, Json::String(final_text));
        assert_eq!(
            captured.lock().unwrap().as_ref(),
            Some(&(prompt.clone(), Some(agent_type), Some(model)))
        );
        let messages = emitted.lock().unwrap();
        assert_eq!(
            messages[0],
            RuntimeString::from_utf8("phase: ").concat(&prompt)
        );
        assert_eq!(
            messages[1],
            RuntimeString::from_utf8("agent ▸ ").concat(&label)
        );
    }

    #[tokio::test]
    async fn workflow_script_source_reaches_runner_without_utf8_projection() {
        let raw_meta = RuntimeString::from_utf16(vec![0xd800]);
        let raw_body = RuntimeString::from_utf16(vec![0xdc00]);
        let source = RuntimeString::from_utf8("export const meta = { name: 'n")
            .concat(&raw_meta)
            .concat(&RuntimeString::from_utf8(
                "', description: 'd' }\nreturn 'body",
            ))
            .concat(&raw_body)
            .concat(&RuntimeString::from_utf8("'\n"));
        let captured = Arc::new(Mutex::new(None));
        let result = run_workflow(
            WorkflowRunInput {
                script: source,
                args: None,
                resume_from_run_id: None,
                budget_tokens: None,
            },
            deps(empty_spawn(), None, AbortSignal::new(), None),
            Arc::new(SourceCaptureRunner(Arc::clone(&captured))),
        )
        .await
        .unwrap();
        assert_eq!(result.meta.name.as_utf16(), &[u16::from(b'n'), 0xd800]);
        let wrapped = captured.lock().unwrap().clone().unwrap();
        assert!(wrapped.as_utf16().contains(&0xd800));
        assert!(wrapped.as_utf16().contains(&0xdc00));
        assert!(!wrapped.as_utf16().contains(&0xfffd));
    }

    #[tokio::test]
    async fn unknown_workflow_surrogate_is_injective_not_replacement() {
        let name = RuntimeString::from_utf16(vec![u16::from(b'x'), 0xdc00]);
        let seen = Arc::new(Mutex::new(None::<RuntimeString>));
        let seen_for_resolver = Arc::clone(&seen);
        let resolve: ResolveWorkflowFn = Arc::new(move |name: &RuntimeString| {
            *seen_for_resolver.lock().unwrap() = Some(name.clone());
            None
        });
        let name_for_runner = name.clone();
        let runner = mock(Arc::new(move |sandbox: Arc<WorkflowSandbox>| {
            let name = name_for_runner.clone();
            Box::pin(async move { sandbox.workflow(Json::String(name), Json::Null).await })
        }));
        let error = run_workflow(
            input(VALID_SCRIPT, None, None),
            deps(empty_spawn(), None, AbortSignal::new(), Some(resolve)),
            runner,
        )
        .await
        .unwrap_err();
        assert_eq!(seen.lock().unwrap().as_ref(), Some(&name));
        assert_eq!(error.message, "unknown workflow: x\\udc00");
        assert!(!error.message.contains("�"));
    }

    #[test]
    fn hash_is_stable_across_opt_key_order() {
        let prompt = RuntimeString::from("p");
        let a = hash_call(&prompt, &exact_json!({ "model": "m", "agentType": "r" }));
        let b = hash_call(&prompt, &exact_json!({ "agentType": "r", "model": "m" }));
        assert_eq!(a, b);
        assert_eq!(a.len_utf16(), 16);
        // A different prompt yields a different hash.
        assert_ne!(
            a,
            hash_call(
                &RuntimeString::from("q"),
                &exact_json!({ "model": "m", "agentType": "r" }),
            )
        );
    }

    #[test]
    fn default_concurrency_is_bounded() {
        let c = default_concurrency();
        assert!((1..=16).contains(&c));
    }
}
