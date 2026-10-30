//! The flow-program interpreter, and the production script runner that plugs
//! it into the workflow host seam.
//!
//! The interpreter owns ONLY control flow and variable scoping. Every effect —
//! sub-agent spawn, nested workflow, phase/log — is delegated to the host
//! sandbox it was handed ([`WorkflowSandbox`]), so budget ceilings, the
//! resume journal cache, the concurrency limiter, the agent/lifetime caps,
//! result hashing, output caps, and the wall-clock race all stay in the host.
//!
//! ## Semantics highlights (the full format contract lives in the module docs
//! of [`crate::engine::flow`])
//!
//! - Scoping is lexical with block scopes: every nested body (`then`/`else`,
//!   `case`/`default` arms, loop bodies, `try`/`catch`/`finally`, parallel
//!   branches, map bodies, pipeline stages, function bodies) is a child
//!   scope. `set` (and an `as` binding) updates the nearest enclosing scope
//!   that already has the name, else declares in the current scope.
//! - `parallel` is a barrier: branches run concurrently (agent calls inside
//!   flow through the host's concurrency cap); a branch that throws resolves
//!   to null — the step itself never throws. `return` inside a branch supplies
//!   that branch's value.
//! - `map` mode `seq` runs items in order (a throw propagates); mode
//!   `parallel` mirrors `parallel` (a throwing item resolves to null); mode
//!   `pipeline` runs each item's stage chain independently with NO barrier
//!   between stages — stage bindings are `prev` (previous stage's value; the
//!   item for stage 0), the item under `var`, and the position under `index`
//!   (default name `index`); a throwing stage drops that item to null and
//!   skips its remaining stages.
//! - `parallel` and `map` in modes `parallel`/`pipeline` enforce the host's
//!   documented per-call item cap with the host's exact error message; `seq`
//!   iteration is uncapped like any loop.
//! - Errors are values with a `message`: a `throw`, a failed expression, or a
//!   rejected host call all unwind to the nearest `try` (the catch variable
//!   binds `{"message": string}`); uncaught they abort the program with that
//!   message.
//! - Function calls are depth-capped at [`MAX_CALL_DEPTH`] (a deterministic,
//!   catchable throw), which bounds unbounded recursion.

use crate::sync_ext::MutexExt;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cel_interpreter::Context;
use futures::future::join_all;

use crate::engine::flow::eval::{
    build_root_context, eval_vexpr, flow_string, truthy, BudgetSource,
};
use crate::engine::flow::program::{
    validate_program, AgentStep, Branch, FlowError, FlowProgram, MapMode, MapStep, Step, VExpr,
};
use crate::engine::workflow::runtime::{
    WorkflowSandbox, WorkflowScriptContext, WorkflowScriptRunner, PER_CALL_ITEM_CAP,
};
use crate::engine::workflow::WorkflowError;
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::types::json::Json;
use crate::types::tool::BoxFuture;

/// Cap on nested `call` depth, so unbounded recursion is a deterministic,
/// catchable throw instead of resource exhaustion.
pub const MAX_CALL_DEPTH: usize = 128;

/// Flow programs are allowed to express CPU-only loops. The interpreter still
/// needs to hand control back to the async host often enough for wall-clock
/// races and abort propagation around host calls to remain observable.
const COOPERATIVE_YIELD_EVERY_STEPS: u64 = 1024;

// ---- environment ------------------------------------------------------------------

/// A lexical scope chain. Scopes are shared (parallel branches may update
/// enclosing variables concurrently), so each frame is internally locked.
#[derive(Clone)]
struct Env(Arc<Scope>);

struct Scope {
    vars: Mutex<HashMap<String, Json>>,
    parent: Option<Env>,
}

impl Env {
    fn root() -> Env {
        Env(Arc::new(Scope {
            vars: Mutex::new(HashMap::new()),
            parent: None,
        }))
    }

    fn child(&self) -> Env {
        Env(Arc::new(Scope {
            vars: Mutex::new(HashMap::new()),
            parent: Some(self.clone()),
        }))
    }

    /// Introduce a binding in THIS scope (loop variables, parameters, …).
    fn declare(&self, name: &str, value: Json) {
        self.0.vars.lock_recover().insert(name.to_string(), value);
    }

    /// Declare-or-update: assign to the nearest enclosing scope that already
    /// binds `name`, else declare here.
    fn assign(&self, name: &str, value: Json) {
        if !self.try_update(name, &value) {
            self.declare(name, value);
        }
    }

    fn try_update(&self, name: &str, value: &Json) -> bool {
        {
            let mut vars = self.0.vars.lock_recover();
            if let Some(slot) = vars.get_mut(name) {
                *slot = value.clone();
                return true;
            }
        }
        match &self.0.parent {
            Some(parent) => parent.try_update(name, value),
            None => false,
        }
    }

    /// Flatten the chain for one expression evaluation (inner shadows outer).
    fn snapshot(&self) -> HashMap<String, Json> {
        let mut scopes: Vec<&Env> = Vec::new();
        let mut cur = Some(self);
        while let Some(env) = cur {
            scopes.push(env);
            cur = env.0.parent.as_ref();
        }
        let mut out = HashMap::new();
        for env in scopes.into_iter().rev() {
            for (k, v) in env.0.vars.lock_recover().iter() {
                out.insert(k.clone(), v.clone());
            }
        }
        out
    }
}

// ---- interpreter ------------------------------------------------------------------

/// Step-level control flow.
enum Control {
    Normal,
    Break,
    Continue,
    Return(Json),
}

struct Exec<'e> {
    program: &'e FlowProgram,
    sandbox: Arc<WorkflowSandbox>,
    cel_root: Context<'static>,
    root_env: Env,
    steps: AtomicU64,
}

/// The message a thrown value carries: a string throws itself; any other value
/// throws its compact JSON form.
fn throw_message(v: &Json) -> RuntimeString {
    flow_string(v)
}

fn runtime_value_type(value: &RuntimeJson) -> &'static str {
    match value {
        RuntimeJson::Null => "null",
        RuntimeJson::Bool(_) => "boolean",
        RuntimeJson::Number(_) => "number",
        RuntimeJson::String(_) => "string",
        RuntimeJson::Array(_) => "array",
        RuntimeJson::Object(_) => "object",
    }
}

impl<'e> Exec<'e> {
    async fn tick(&self) {
        let step = self
            .steps
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .wrapping_add(1);
        if step.is_multiple_of(COOPERATIVE_YIELD_EVERY_STEPS) {
            tokio::task::yield_now().await;
        }
    }

    fn eval(&self, v: &VExpr, env: &Env) -> Result<Json, FlowError> {
        eval_vexpr(v, &self.cel_root, &env.snapshot())
    }

    fn eval_truthy(&self, v: &VExpr, env: &Env) -> Result<bool, FlowError> {
        Ok(truthy(&self.eval(v, env)?))
    }

    fn run_steps<'a>(
        &'a self,
        steps: &'a [Step],
        env: Env,
        depth: usize,
    ) -> BoxFuture<'a, Result<Control, FlowError>> {
        Box::pin(async move {
            for step in steps {
                match self.run_step(step, &env, depth).await? {
                    Control::Normal => {}
                    other => return Ok(other),
                }
            }
            Ok(Control::Normal)
        })
    }

    /// Dispatch one step. Effect-free arms run inline; every arm that awaits
    /// is a separate small `async fn`, keeping each recursion level's poll
    /// frame small (deep recursion through `call` would otherwise exhaust the
    /// stack in unoptimized builds).
    fn run_step<'a>(
        &'a self,
        step: &'a Step,
        env: &'a Env,
        depth: usize,
    ) -> BoxFuture<'a, Result<Control, FlowError>> {
        // Keep recursive function calls out of the large all-variants dispatch
        // future. In debug builds, polling that future recursively at the
        // documented depth cap can exhaust Tokio's default worker stack before
        // the deterministic depth error is reached.
        if let Step::Call { name, args, bind } = step {
            return Box::pin(async move {
                self.tick().await;
                self.exec_call(name, args, bind.as_deref(), env, depth)
                    .await
            });
        }

        Box::pin(async move {
            self.tick().await;
            match step {
                Step::Set { var, value } => {
                    let v = self.eval(value, env)?;
                    env.assign(var, v);
                    Ok(Control::Normal)
                }
                Step::If { cond, then, els } => {
                    self.exec_if(cond, then, els.as_deref(), env, depth).await
                }
                Step::Switch { cases, default } => {
                    self.exec_switch(cases, default.as_deref(), env, depth)
                        .await
                }
                Step::While { cond, body } => self.exec_while(cond, body, env, depth).await,
                Step::Break => Ok(Control::Break),
                Step::Continue => Ok(Control::Continue),
                Step::For {
                    var,
                    index,
                    list,
                    body,
                } => {
                    self.exec_for(var, index.as_deref(), list, body, env, depth)
                        .await
                }
                Step::Parallel { branches, bind } => {
                    self.exec_parallel(branches, bind.as_deref(), env, depth)
                        .await
                }
                Step::Map(m) => self.run_map(m, env, depth).await,
                Step::Agent(a) => self.exec_agent(a.as_ref(), env).await,
                Step::Workflow { target, args, bind } => {
                    self.exec_workflow(target, args.as_ref(), bind.as_deref(), env)
                        .await
                }
                Step::Phase(v) => {
                    let v = self.eval(v, env)?;
                    self.sandbox.phase(&v);
                    Ok(Control::Normal)
                }
                Step::Log(v) => {
                    let v = self.eval(v, env)?;
                    self.sandbox.log(&v);
                    Ok(Control::Normal)
                }
                Step::Try {
                    body,
                    catch,
                    finally,
                } => {
                    self.exec_try(body, catch.as_ref(), finally.as_deref(), env, depth)
                        .await
                }
                Step::Throw(v) => {
                    let v = self.eval(v, env)?;
                    Err(FlowError::new(throw_message(&v)))
                }
                Step::Return(v) => Ok(Control::Return(self.eval(v, env)?)),
                Step::Call { .. } => unreachable!("call steps use the compact dispatch future"),
            }
        })
    }

    async fn exec_if(
        &self,
        cond: &VExpr,
        then: &[Step],
        els: Option<&[Step]>,
        env: &Env,
        depth: usize,
    ) -> Result<Control, FlowError> {
        if self.eval_truthy(cond, env)? {
            self.run_steps(then, env.child(), depth).await
        } else if let Some(els) = els {
            self.run_steps(els, env.child(), depth).await
        } else {
            Ok(Control::Normal)
        }
    }

    async fn exec_switch(
        &self,
        cases: &[(VExpr, Vec<Step>)],
        default: Option<&[Step]>,
        env: &Env,
        depth: usize,
    ) -> Result<Control, FlowError> {
        for (cond, body) in cases {
            if self.eval_truthy(cond, env)? {
                return self.run_steps(body, env.child(), depth).await;
            }
        }
        match default {
            Some(body) => self.run_steps(body, env.child(), depth).await,
            None => Ok(Control::Normal),
        }
    }

    async fn exec_while(
        &self,
        cond: &VExpr,
        body: &[Step],
        env: &Env,
        depth: usize,
    ) -> Result<Control, FlowError> {
        loop {
            self.tick().await;
            if !self.eval_truthy(cond, env)? {
                break;
            }
            match self.run_steps(body, env.child(), depth).await? {
                Control::Normal | Control::Continue => {}
                Control::Break => break,
                other => return Ok(other),
            }
        }
        Ok(Control::Normal)
    }

    async fn exec_for(
        &self,
        var: &str,
        index: Option<&str>,
        list: &VExpr,
        body: &[Step],
        env: &Env,
        depth: usize,
    ) -> Result<Control, FlowError> {
        let list = self.eval(list, env)?;
        let items = as_items("for", &list)?;
        for (i, item) in items.iter().enumerate() {
            self.tick().await;
            let scope = env.child();
            scope.declare(var, item.clone());
            if let Some(ix) = index {
                scope.declare(ix, Json::from(i as i64));
            }
            match self.run_steps(body, scope, depth).await? {
                Control::Normal | Control::Continue => {}
                Control::Break => break,
                other => return Ok(other),
            }
        }
        Ok(Control::Normal)
    }

    async fn exec_parallel(
        &self,
        branches: &[Branch],
        bind: Option<&str>,
        env: &Env,
        depth: usize,
    ) -> Result<Control, FlowError> {
        if branches.len() > PER_CALL_ITEM_CAP {
            return Err(FlowError::new(format!(
                "parallel: {} items exceeds the {PER_CALL_ITEM_CAP} cap",
                branches.len()
            )));
        }
        // Branch futures are created (and later polled) in array order, so
        // each branch's first agent launch is deterministic.
        let futs: Vec<_> = branches
            .iter()
            .map(|b| self.run_branch(b, env.child(), depth))
            .collect();
        let outcomes = join_all(futs).await;
        if let Some(bind) = bind {
            env.assign(bind, Json::Array(outcomes));
        }
        Ok(Control::Normal)
    }

    async fn exec_agent(&self, a: &AgentStep, env: &Env) -> Result<Control, FlowError> {
        let sandbox = Arc::clone(&self.sandbox);
        let prompt = self.eval(&a.prompt, env)?;
        let mut opts = RuntimeObject::new();
        for (key, expr) in [
            ("label", &a.label),
            ("phase", &a.phase),
            ("model", &a.model),
            ("isolation", &a.isolation),
            ("agentType", &a.agent_type),
        ] {
            if let Some(expr) = expr {
                opts.insert(key, self.eval(expr, env)?);
            }
        }
        if let Some(schema) = &a.schema {
            opts.insert("schema", RuntimeJson::Object(schema.clone()));
        }
        let result = sandbox
            .agent(prompt, Json::Object(opts))
            .await
            .map_err(|e| FlowError::new(e.message))?;
        if let Some(bind) = &a.bind {
            env.assign(bind, result);
        }
        Ok(Control::Normal)
    }

    async fn exec_workflow(
        &self,
        target: &VExpr,
        args: Option<&VExpr>,
        bind: Option<&str>,
        env: &Env,
    ) -> Result<Control, FlowError> {
        let sandbox = Arc::clone(&self.sandbox);
        let target = self.eval(target, env)?;
        let child_args = match args {
            Some(v) => self.eval(v, env)?,
            None => Json::Null,
        };
        let result = sandbox
            .workflow(target, child_args)
            .await
            .map_err(|e| FlowError::new(e.message))?;
        if let Some(bind) = bind {
            env.assign(bind, result);
        }
        Ok(Control::Normal)
    }

    async fn exec_try(
        &self,
        body: &[Step],
        catch: Option<&(Option<String>, Vec<Step>)>,
        finally: Option<&[Step]>,
        env: &Env,
        depth: usize,
    ) -> Result<Control, FlowError> {
        let mut outcome = self.run_steps(body, env.child(), depth).await;
        if let (Err(thrown), Some((var, handler))) = (&outcome, catch) {
            let scope = env.child();
            if let Some(var) = var {
                let mut error = RuntimeObject::new();
                error.insert("message", RuntimeJson::String(thrown.message.clone()));
                scope.declare(var, RuntimeJson::Object(error));
            }
            outcome = self.run_steps(handler, scope, depth).await;
        }
        if let Some(cleanup) = finally {
            // An abrupt completion in `finally` overrides the try/catch
            // outcome.
            match self.run_steps(cleanup, env.child(), depth).await? {
                Control::Normal => {}
                abrupt => return Ok(abrupt),
            }
        }
        outcome
    }

    async fn exec_call(
        &self,
        name: &str,
        args: &[VExpr],
        bind: Option<&str>,
        env: &Env,
        depth: usize,
    ) -> Result<Control, FlowError> {
        if depth >= MAX_CALL_DEPTH {
            return Err(FlowError::new(format!(
                "maximum function call depth ({MAX_CALL_DEPTH}) exceeded"
            )));
        }
        let func = self
            .program
            .funcs
            .get(name)
            .ok_or_else(|| FlowError::new(format!("internal: unknown function \"{name}\"")))?;
        let mut values = Vec::with_capacity(args.len());
        for arg in args {
            values.push(self.eval(arg, env)?);
        }
        // Functions close over the program scope (not the caller's).
        let scope = self.root_env.child();
        for (i, param) in func.params.iter().enumerate() {
            scope.declare(param, values.get(i).cloned().unwrap_or(Json::Null));
        }
        let result = match self.run_steps(&func.body, scope, depth + 1).await? {
            Control::Return(v) => v,
            _ => Json::Null,
        };
        if let Some(bind) = bind {
            env.assign(bind, result);
        }
        Ok(Control::Normal)
    }

    /// One `parallel` branch: a throw (anywhere in `do` or in `value`)
    /// resolves the branch to null; `return` supplies the branch value.
    async fn run_branch(&self, branch: &Branch, scope: Env, depth: usize) -> Json {
        match self.run_steps(&branch.body, scope.clone(), depth).await {
            Err(_) => Json::Null,
            Ok(Control::Return(v)) => v,
            Ok(_) => branch
                .value
                .as_ref()
                .and_then(|v| self.eval(v, &scope).ok())
                .unwrap_or(Json::Null),
        }
    }

    async fn run_map<'a>(
        &'a self,
        m: &'a MapStep,
        env: &Env,
        depth: usize,
    ) -> Result<Control, FlowError> {
        let list = self.eval(&m.list, env)?;
        let items = as_items("map", &list)?;
        let results: Vec<Json> = match m.mode {
            MapMode::Seq => {
                let mut out = Vec::with_capacity(items.len());
                for (i, item) in items.iter().enumerate() {
                    self.tick().await;
                    let scope = self.item_scope(m, env, item, i);
                    if let Control::Return(v) =
                        self.run_steps(&m.body, scope.clone(), depth).await?
                    {
                        out.push(v);
                        continue;
                    }
                    out.push(match &m.value {
                        Some(v) => self.eval(v, &scope)?,
                        None => Json::Null,
                    });
                }
                out
            }
            MapMode::Parallel => {
                if items.len() > PER_CALL_ITEM_CAP {
                    return Err(FlowError::new(format!(
                        "parallel: {} items exceeds the {PER_CALL_ITEM_CAP} cap",
                        items.len()
                    )));
                }
                let futs: Vec<_> = items
                    .iter()
                    .enumerate()
                    .map(|(i, item)| self.run_map_item(m, env, item, i, depth))
                    .collect();
                join_all(futs).await
            }
            MapMode::Pipeline => {
                if items.len() > PER_CALL_ITEM_CAP {
                    return Err(FlowError::new(format!(
                        "pipeline: {} items exceeds the {PER_CALL_ITEM_CAP} cap",
                        items.len()
                    )));
                }
                let futs: Vec<_> = items
                    .iter()
                    .enumerate()
                    .map(|(i, item)| self.run_chain(m, env, item, i, depth))
                    .collect();
                join_all(futs).await
            }
        };
        if let Some(bind) = &m.bind {
            env.assign(bind, Json::Array(results));
        }
        Ok(Control::Normal)
    }

    fn item_scope(&self, m: &MapStep, env: &Env, item: &Json, i: usize) -> Env {
        let scope = env.child();
        scope.declare(&m.var, item.clone());
        if let Some(ix) = &m.index {
            scope.declare(ix, Json::from(i as i64));
        }
        scope
    }

    /// One `map`-parallel item: same throw→null semantics as a branch.
    async fn run_map_item(
        &self,
        m: &MapStep,
        env: &Env,
        item: &Json,
        i: usize,
        depth: usize,
    ) -> Json {
        let scope = self.item_scope(m, env, item, i);
        match self.run_steps(&m.body, scope.clone(), depth).await {
            Err(_) => Json::Null,
            Ok(Control::Return(v)) => v,
            Ok(_) => m
                .value
                .as_ref()
                .and_then(|v| self.eval(v, &scope).ok())
                .unwrap_or(Json::Null),
        }
    }

    /// One pipeline item's stage chain: no barrier between stages; a throwing
    /// stage drops the item to null and skips its remaining stages.
    async fn run_chain(&self, m: &MapStep, env: &Env, item: &Json, i: usize, depth: usize) -> Json {
        let mut prev = item.clone();
        for stage in &m.stages {
            let scope = env.child();
            scope.declare("prev", prev.clone());
            scope.declare(&m.var, item.clone());
            scope.declare(m.index.as_deref().unwrap_or("index"), Json::from(i as i64));
            match self.run_steps(&stage.body, scope.clone(), depth).await {
                Err(_) => return Json::Null,
                Ok(Control::Return(v)) => {
                    prev = v;
                    continue;
                }
                Ok(_) => {}
            }
            prev = match &stage.value {
                Some(v) => match self.eval(v, &scope) {
                    Ok(v) => v,
                    Err(_) => return Json::Null,
                },
                None => Json::Null,
            };
        }
        prev
    }
}

fn as_items<'v>(step: &str, list: &'v Json) -> Result<&'v Vec<Json>, FlowError> {
    list.as_array().ok_or_else(|| {
        FlowError::new(format!(
            "{step}: `in` must evaluate to an array (got {})",
            runtime_value_type(list)
        ))
    })
}

/// Run one validated program to completion against the workflow sandbox.
/// The result is the program's `return` value (null when it falls off the
/// end).
pub(crate) async fn run_flow(
    program: &FlowProgram,
    sandbox: Arc<WorkflowSandbox>,
    args: Json,
) -> Result<Json, FlowError> {
    let budget = BudgetSource::Workflow(Arc::clone(&sandbox));
    let cel_root = build_root_context(&args, &budget)?;
    let root_env = Env::root();
    for (name, value) in &program.vars {
        let v = eval_vexpr(value, &cel_root, &root_env.snapshot())?;
        root_env.declare(name, v);
    }
    let exec = Exec {
        program,
        sandbox,
        cel_root,
        root_env: root_env.clone(),
        steps: AtomicU64::new(0),
    };
    match exec.run_steps(&program.body, root_env, 0).await? {
        Control::Return(v) => Ok(v),
        _ => Ok(Json::Null),
    }
}

// ---- the workflow-seam runner --------------------------------------------------------

/// The wrapper the workflow runtime puts around a parsed script body.
const WORKFLOW_WRAP_PREFIX: &str = "(async function __workflow(){\n";
const WORKFLOW_WRAP_SUFFIX: &str = "\n})";

/// The model-facing message for a script that is not a flow program.
const NOT_A_FLOW_PROGRAM: &str = "this engine executes JSON workflow programs: a JSON object with \"meta\" (name, description) and \"body\" (an array of steps). The supplied script is not one — author the workflow as a flow program.";

fn is_ecmascript_whitespace(unit: u16) -> bool {
    matches!(
        unit,
        0x0009 | 0x000a | 0x000b | 0x000c | 0x000d | 0x0020 | 0x00a0 | 0x1680 | 0x2000
            ..=0x200a | 0x2028 | 0x2029 | 0x202f | 0x205f | 0x3000 | 0xfeff
    )
}

fn trim_runtime_start(value: &RuntimeString, trim_semicolons: bool) -> RuntimeString {
    let start = value
        .as_utf16()
        .iter()
        .position(|unit| {
            !(is_ecmascript_whitespace(*unit) || trim_semicolons && *unit == u16::from(b';'))
        })
        .unwrap_or(value.len_utf16());
    value.slice_utf16(start..)
}

/// Find the end (exclusive) of the exact JSON object starting at UTF-16 unit 0.
fn json_object_end(s: &RuntimeString) -> Option<usize> {
    let units = s.as_utf16();
    if units.first() != Some(&u16::from(b'{')) {
        return None;
    }
    let mut depth: i64 = 0;
    let mut in_string = false;
    let mut i = 0;
    while i < units.len() {
        let c = units[i];
        if in_string {
            if c == u16::from(b'\\') {
                i += 1;
            } else if c == u16::from(b'"') {
                in_string = false;
            }
        } else {
            match c {
                unit if unit == u16::from(b'"') => in_string = true,
                unit if unit == u16::from(b'{') => depth += 1,
                unit if unit == u16::from(b'}') => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i + 1);
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

/// Recover the flow-program JSON from the wrapped source the workflow runtime
/// hands its script runner. Composed sources (see
/// [`crate::engine::flow::program::compose_workflow_script`]) carry a
/// `const meta = {…}` binding followed by the program document.
fn recover_workflow_flow_source(wrapped: &RuntimeString) -> Result<RuntimeString, WorkflowError> {
    if !wrapped.starts_with(WORKFLOW_WRAP_PREFIX) || !wrapped.ends_with(WORKFLOW_WRAP_SUFFIX) {
        return Err(WorkflowError::new(NOT_A_FLOW_PROGRAM));
    }
    let prefix_len = WORKFLOW_WRAP_PREFIX.encode_utf16().count();
    let suffix_len = WORKFLOW_WRAP_SUFFIX.encode_utf16().count();
    let body = wrapped.slice_utf16(prefix_len..wrapped.len_utf16().saturating_sub(suffix_len));
    let mut rest = trim_runtime_start(&body, false);
    if !rest.starts_with("const meta") {
        return Err(WorkflowError::new(NOT_A_FLOW_PROGRAM));
    }
    rest = trim_runtime_start(
        &rest.slice_utf16("const meta".encode_utf16().count()..),
        false,
    );
    if !rest.starts_with("=") {
        return Err(WorkflowError::new(NOT_A_FLOW_PROGRAM));
    }
    rest = trim_runtime_start(&rest.slice_utf16(1..), false);
    let end = json_object_end(&rest).ok_or_else(|| WorkflowError::new(NOT_A_FLOW_PROGRAM))?;
    let after = trim_runtime_start(&rest.slice_utf16(end..), true);
    // The program document must follow the meta binding; anything else (for
    // example a classic script body) is not a flow program.
    if !after.starts_with("{") {
        return Err(WorkflowError::new(NOT_A_FLOW_PROGRAM));
    }
    Ok(after)
}

/// The production workflow script runner: interprets a flow program against
/// the [`WorkflowSandbox`], so every agent / nested-workflow / phase / log /
/// budget effect flows through the host runtime unchanged.
pub struct FlowWorkflowScriptRunner;

#[async_trait]
impl WorkflowScriptRunner for FlowWorkflowScriptRunner {
    async fn run(&self, ctx: WorkflowScriptContext) -> Result<Json, WorkflowError> {
        let source = recover_workflow_flow_source(&ctx.wrapped_source)?;
        let program = validate_program(&source).map_err(|e| WorkflowError::new(e.message))?;
        run_flow(&program, Arc::clone(&ctx.sandbox), ctx.args.clone())
            .await
            .map_err(|e| WorkflowError::new(e.message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::flow::program::compose_workflow_script;
    use crate::engine::workflow::runtime::{
        run_workflow, ResolveWorkflowFn, WorkflowRunInput, WorkflowRunResult, WorkflowRuntimeDeps,
    };
    use crate::testing::make_usage;
    use crate::types::abort::AbortSignal;
    use crate::types::errors::{EngineError, EngineErrorCode, TerminalReason};
    use crate::types::events::{EngineEvent, ThreadItem, ToolDomainEvent};
    use crate::types::tool::{
        EngineEventStream, SpawnSubAgent, SpawnSubAgentOptions, WorkflowJournal,
        WorkflowJournalEntry,
    };
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Duration;

    macro_rules! exact_json {
        (@array [$($elements:expr,)*]) => { vec![$($elements,)*] };
        (@array [$($elements:expr),*]) => { vec![$($elements),*] };
        (@array [$($elements:expr,)*] null $($rest:tt)*) => {
            exact_json!(@array [$($elements,)* RuntimeJson::Null] $($rest)*)
        };
        (@array [$($elements:expr,)*] true $($rest:tt)*) => {
            exact_json!(@array [$($elements,)* RuntimeJson::Bool(true)] $($rest)*)
        };
        (@array [$($elements:expr,)*] false $($rest:tt)*) => {
            exact_json!(@array [$($elements,)* RuntimeJson::Bool(false)] $($rest)*)
        };
        (@array [$($elements:expr,)*] [$($array:tt)*] $($rest:tt)*) => {
            exact_json!(@array [$($elements,)* exact_json!([$($array)*])] $($rest)*)
        };
        (@array [$($elements:expr,)*] {$($object:tt)*} $($rest:tt)*) => {
            exact_json!(@array [$($elements,)* exact_json!({$($object)*})] $($rest)*)
        };
        (@array [$($elements:expr,)*] $next:expr, $($rest:tt)*) => {
            exact_json!(@array [$($elements,)* exact_json!($next),] $($rest)*)
        };
        (@array [$($elements:expr,)*] $last:expr) => {
            exact_json!(@array [$($elements,)* exact_json!($last)])
        };
        (@array [$($elements:expr),*] , $($rest:tt)*) => {
            exact_json!(@array [$($elements,)*] $($rest)*)
        };

        (@object $object:ident) => {};
        (@object $object:ident ,) => {};
        (@object $object:ident $key:literal : null, $($rest:tt)*) => {{
            $object.insert($key, RuntimeJson::Null);
            exact_json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : null) => {{
            $object.insert($key, RuntimeJson::Null);
        }};
        (@object $object:ident $key:literal : true, $($rest:tt)*) => {{
            $object.insert($key, RuntimeJson::Bool(true));
            exact_json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : true) => {{
            $object.insert($key, RuntimeJson::Bool(true));
        }};
        (@object $object:ident $key:literal : false, $($rest:tt)*) => {{
            $object.insert($key, RuntimeJson::Bool(false));
            exact_json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : false) => {{
            $object.insert($key, RuntimeJson::Bool(false));
        }};
        (@object $object:ident $key:literal : [$($array:tt)*], $($rest:tt)*) => {{
            $object.insert($key, exact_json!([$($array)*]));
            exact_json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : [$($array:tt)*]) => {{
            $object.insert($key, exact_json!([$($array)*]));
        }};
        (@object $object:ident $key:literal : {$($nested:tt)*}, $($rest:tt)*) => {{
            $object.insert($key, exact_json!({$($nested)*}));
            exact_json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : {$($nested:tt)*}) => {{
            $object.insert($key, exact_json!({$($nested)*}));
        }};
        (@object $object:ident $key:literal : $value:expr, $($rest:tt)*) => {{
            $object.insert($key, exact_json!($value));
            exact_json!(@object $object $($rest)*);
        }};
        (@object $object:ident $key:literal : $value:expr) => {{
            $object.insert($key, exact_json!($value));
        }};

        (null) => { RuntimeJson::Null };
        (true) => { RuntimeJson::Bool(true) };
        (false) => { RuntimeJson::Bool(false) };
        ([]) => { RuntimeJson::Array(Vec::new()) };
        ([$($tokens:tt)+]) => {
            RuntimeJson::Array(exact_json!(@array [] $($tokens)+))
        };
        ({}) => { RuntimeJson::Object(RuntimeObject::new()) };
        ({$($tokens:tt)+}) => {{
            let mut object = RuntimeObject::new();
            exact_json!(@object object $($tokens)+);
            RuntimeJson::Object(object)
        }};
        ($value:expr) => {
            crate::runtime_values::runtime_codec::encode_value(&$value)
                .expect("test exact JSON expression must encode")
        };
    }

    type StatusRecorder = (
        Arc<dyn Fn(ToolDomainEvent) + Send + Sync>,
        Arc<Mutex<Vec<RuntimeString>>>,
    );

    // ---- workflow-context scaffolding ------------------------------------------

    fn meta() -> Json {
        exact_json!({ "name": "demo", "description": "d" })
    }

    fn program(body: Json) -> Json {
        exact_json!({ "meta": meta(), "body": body })
    }

    fn script(program: &Json) -> RuntimeString {
        compose_workflow_script(&crate::runtime_values::runtime_json::stringify(program))
            .expect("composes")
    }

    /// A spawner that yields a fixed agent message (or a failed turn) plus a
    /// completed turn carrying `out` output tokens.
    fn spawn_yielding(text: &'static str, out: u64, fail: bool) -> SpawnSubAgent {
        Arc::new(move |_opts: SpawnSubAgentOptions| {
            let mut events: Vec<EngineEvent> = Vec::new();
            if fail {
                events.push(EngineEvent::TurnFailed {
                    inference: None,
                    error: EngineError {
                        code: EngineErrorCode::Internal,
                        message: RuntimeString::from("boom"),
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
                        id: RuntimeString::from("m"),
                        text: RuntimeString::from(text),
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

    /// A spawner that records each prompt (in spawn order) and echoes it back.
    fn recording_spawn(prompts: Arc<Mutex<Vec<RuntimeString>>>, out: u64) -> SpawnSubAgent {
        Arc::new(move |opts: SpawnSubAgentOptions| {
            prompts.lock().unwrap().push(opts.prompt.clone());
            let events = vec![
                EngineEvent::ItemCompleted {
                    inference: None,
                    item: ThreadItem::AgentMessage {
                        phase: None,
                        id: RuntimeString::from("m"),
                        text: opts.prompt,
                    },
                },
                EngineEvent::TurnCompleted {
                    turn_id: "wf".into(),
                    usage: Some(make_usage(None, Some(out))),
                    stop_reason: TerminalReason::Completed,
                },
            ];
            Box::pin(futures::stream::iter(events)) as EngineEventStream
        })
    }

    fn empty_spawn() -> SpawnSubAgent {
        Arc::new(|_opts: SpawnSubAgentOptions| {
            Box::pin(futures::stream::empty::<EngineEvent>()) as EngineEventStream
        })
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

    struct DepsCfg {
        spawn: SpawnSubAgent,
        journal: Option<Arc<dyn WorkflowJournal>>,
        signal: AbortSignal,
        resolve: Option<ResolveWorkflowFn>,
        emit: Option<Arc<dyn Fn(ToolDomainEvent) + Send + Sync>>,
        max_concurrency: usize,
    }

    impl Default for DepsCfg {
        fn default() -> Self {
            DepsCfg {
                spawn: empty_spawn(),
                journal: None,
                signal: AbortSignal::new(),
                resolve: None,
                emit: None,
                max_concurrency: 4,
            }
        }
    }

    fn deps(cfg: DepsCfg) -> WorkflowRuntimeDeps {
        WorkflowRuntimeDeps {
            process_context: crate::runtime_process::test_runtime_process_context(),
            spawn_workflow_agent: cfg.spawn,
            journal: cfg.journal,
            emit: cfg.emit,
            resolve_workflow: cfg.resolve,
            signal: cfg.signal,
            max_concurrency: Some(cfg.max_concurrency),
        }
    }

    async fn run_prog(
        prog: &Json,
        args: Option<Json>,
        budget: Option<i64>,
        resume: Option<&RuntimeString>,
        cfg: DepsCfg,
    ) -> Result<WorkflowRunResult, WorkflowError> {
        run_workflow(
            WorkflowRunInput {
                script: script(prog),
                args,
                resume_from_run_id: resume.cloned(),
                budget_tokens: budget,
            },
            deps(cfg),
            Arc::new(FlowWorkflowScriptRunner),
        )
        .await
    }

    async fn run_ok(prog: &Json, cfg: DepsCfg) -> WorkflowRunResult {
        run_prog(prog, None, None, None, cfg)
            .await
            .expect("run succeeds")
    }

    /// An emit sink that records every status message.
    fn status_recorder() -> StatusRecorder {
        let seen: Arc<Mutex<Vec<RuntimeString>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let emit: Arc<dyn Fn(ToolDomainEvent) + Send + Sync> = Arc::new(move |ev| {
            if let ToolDomainEvent::Status { message } = ev {
                sink.lock().unwrap().push(message);
            }
        });
        (emit, seen)
    }

    // ---- basics -------------------------------------------------------------------

    #[tokio::test]
    async fn returns_the_program_value_and_meta() {
        let prog = program(exact_json!([ { "return": { "x": { "$": "1 + 1" } } } ]));
        let res = run_ok(&prog, DepsCfg::default()).await;
        assert_eq!(res.result, exact_json!({ "x": 2 }));
        assert_eq!(res.meta.name, "demo");
        assert!(res.run_id.starts_with("wf_"));
        assert_eq!(res.agents_spawned, 0);
    }

    #[tokio::test]
    async fn falls_off_the_end_to_null() {
        let prog = program(exact_json!([ { "set": { "var": "x", "value": 1 } } ]));
        let res = run_ok(&prog, DepsCfg::default()).await;
        assert_eq!(res.result, exact_json!(null));
    }

    #[tokio::test]
    async fn vars_evaluate_in_order_and_args_are_exposed() {
        let prog = exact_json!({
            "meta": meta(),
            "vars": [
                { "var": "base", "value": { "$": "args.q * 10" } },
                { "var": "msg", "value": "base=${base}" }
            ],
            "body": [ { "return": { "$": "[base, msg]" } } ]
        });
        let res = run_prog(
            &prog,
            Some(exact_json!({ "q": 7 })),
            None,
            None,
            DepsCfg::default(),
        )
        .await
        .unwrap();
        assert_eq!(res.result, exact_json!([70, "base=70"]));
    }

    #[tokio::test]
    async fn control_flow_if_switch_while_for() {
        let prog = exact_json!({
            "meta": meta(),
            "vars": [ { "var": "out", "value": [] } ],
            "body": [
                { "if": { "$": "1 > 2" },
                  "then": [ { "set": { "var": "out", "value": { "$": "out + ['then']" } } } ],
                  "else": [ { "set": { "var": "out", "value": { "$": "out + ['else']" } } } ] },
                { "switch": [
                    { "case": { "$": "false" }, "do": [ { "set": { "var": "out", "value": { "$": "out + ['a']" } } } ] },
                    { "case": { "$": "size(out) == 1" }, "do": [ { "set": { "var": "out", "value": { "$": "out + ['b']" } } } ] }
                  ],
                  "default": [ { "set": { "var": "out", "value": { "$": "out + ['d']" } } } ] },
                { "set": { "var": "i", "value": 0 } },
                { "while": { "$": "i < 5" }, "do": [
                    { "set": { "var": "i", "value": { "$": "i + 1" } } },
                    { "if": { "$": "i == 2" }, "then": [ { "continue": null } ] },
                    { "if": { "$": "i == 4" }, "then": [ { "break": null } ] },
                    { "set": { "var": "out", "value": { "$": "out + [i]" } } }
                ] },
                { "for": { "var": "x", "index": "ix", "in": ["p", "q"], "do": [
                    { "set": { "var": "out", "value": { "$": "out + [x + string(ix)]" } } }
                ] } },
                { "return": { "$": "out" } }
            ]
        });
        let res = run_ok(&prog, DepsCfg::default()).await;
        assert_eq!(res.result, exact_json!(["else", "b", 1, 3, "p0", "q1"]));
    }

    #[tokio::test]
    async fn switch_falls_through_to_default() {
        let prog = program(exact_json!([
            { "switch": [ { "case": false, "do": [ { "return": "case" } ] } ],
              "default": [ { "return": "default" } ] }
        ]));
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!("default")
        );
    }

    #[tokio::test]
    async fn for_over_a_non_array_throws_a_catchable_type_error() {
        let prog = program(exact_json!([
            { "try": [ { "for": { "var": "x", "in": "nope", "do": [] } } ],
              "catch": { "var": "e", "do": [ { "return": { "$": "e.message" } } ] } }
        ]));
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!("for: `in` must evaluate to an array (got string)")
        );
    }

    // ---- Turing-complete imperative core (parity requirement 1) ---------------------

    #[tokio::test]
    async fn loop_until_dry_with_set_like_dedup() {
        // Expand n → {2n, 3n} (bounded by 20) from a frontier, deduplicating
        // via `x in seen` — the loop-until-dry shape.
        let prog = exact_json!({
            "meta": meta(),
            "vars": [
                { "var": "queue", "value": [1] },
                { "var": "seen", "value": [] }
            ],
            "body": [
                { "while": { "$": "size(queue) > 0" }, "do": [
                    { "set": { "var": "x", "value": { "$": "queue[0]" } } },
                    { "set": { "var": "queue", "value": { "$": "slice(queue, 1)" } } },
                    { "if": { "$": "!(x in seen)" }, "then": [
                        { "set": { "var": "seen", "value": { "$": "seen + [x]" } } },
                        { "if": { "$": "x * 2 <= 20" }, "then": [
                            { "set": { "var": "queue", "value": { "$": "queue + [x * 2]" } } } ] },
                        { "if": { "$": "x * 3 <= 20" }, "then": [
                            { "set": { "var": "queue", "value": { "$": "queue + [x * 3]" } } } ] }
                    ] }
                ] },
                { "return": { "$": "sort(seen)" } }
            ]
        });
        let res = run_ok(&prog, DepsCfg::default()).await;
        assert_eq!(res.result, exact_json!([1, 2, 3, 4, 6, 8, 9, 12, 16, 18]));
    }

    #[tokio::test]
    async fn recursive_functions_compute_fib() {
        let prog = exact_json!({
            "meta": meta(),
            "funcs": {
                "fib": { "params": ["n"], "body": [
                    { "if": { "$": "n < 2" }, "then": [ { "return": { "$": "n" } } ] },
                    { "call": { "fn": "fib", "args": [ { "$": "n - 1" } ] }, "as": "a" },
                    { "call": { "fn": "fib", "args": [ { "$": "n - 2" } ] }, "as": "b" },
                    { "return": { "$": "a + b" } }
                ] }
            },
            "body": [
                { "call": { "fn": "fib", "args": [10] }, "as": "out" },
                { "return": { "$": "out" } }
            ]
        });
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!(55)
        );
    }

    #[tokio::test]
    async fn unbounded_recursion_is_a_catchable_depth_error() {
        let prog = exact_json!({
            "meta": meta(),
            "funcs": { "spin": { "params": [], "body": [ { "call": { "fn": "spin" } } ] } },
            "body": [
                { "try": [ { "call": { "fn": "spin" } } ],
                  "catch": { "var": "e", "do": [ { "return": { "$": "e.message" } } ] } }
            ]
        });
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!(RuntimeString::from(format!(
                "maximum function call depth ({MAX_CALL_DEPTH}) exceeded"
            )))
        );
    }

    #[tokio::test]
    async fn missing_args_bind_null_and_extra_args_are_ignored() {
        let prog = exact_json!({
            "meta": meta(),
            "funcs": { "f": { "params": ["a", "b"], "body": [ { "return": { "$": "[a, b]" } } ] } },
            "body": [
                { "call": { "fn": "f", "args": [1, 2, 3] }, "as": "x" },
                { "call": { "fn": "f", "args": [1] }, "as": "y" },
                { "return": { "$": "[x, y]" } }
            ]
        });
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!([[1, 2], [1, null]])
        );
    }

    // ---- scoping ---------------------------------------------------------------------

    #[tokio::test]
    async fn set_updates_enclosing_scopes_and_blocks_are_scoped() {
        let prog = exact_json!({
            "meta": meta(),
            "vars": [ { "var": "outer", "value": 1 } ],
            "body": [
                { "if": true, "then": [
                    { "set": { "var": "outer", "value": 2 } },
                    { "set": { "var": "inner", "value": 9 } }
                ] },
                // `inner` was declared in the then-block, so it is gone here.
                { "try": [ { "return": { "$": "inner" } } ],
                  "catch": { "do": [ { "return": { "$": "outer" } } ] } }
            ]
        });
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!(2)
        );
    }

    #[tokio::test]
    async fn functions_close_over_program_scope_not_the_caller() {
        // `local` lives in the caller's block scope, so `f` cannot see it —
        // functions see the program scope (`g` and top-level bindings) only.
        let prog = exact_json!({
            "meta": meta(),
            "vars": [ { "var": "g", "value": "global" }, { "var": "out", "value": null } ],
            "funcs": { "f": { "params": [], "body": [
                { "try": [ { "return": { "$": "local" } } ],
                  "catch": { "do": [ { "return": { "$": "g" } } ] } }
            ] } },
            "body": [
                { "if": true, "then": [
                    { "set": { "var": "local", "value": "caller" } },
                    { "call": { "fn": "f" }, "as": "out" }
                ] },
                { "return": { "$": "out" } }
            ]
        });
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!("global")
        );
    }

    // ---- try / throw --------------------------------------------------------------------

    #[tokio::test]
    async fn try_catch_finally_ordering_and_binding() {
        let prog = exact_json!({
            "meta": meta(),
            "vars": [ { "var": "trace", "value": [] } ],
            "body": [
                { "try": [
                    { "set": { "var": "trace", "value": { "$": "trace + ['try']" } } },
                    { "throw": "kaboom" },
                    { "set": { "var": "trace", "value": { "$": "trace + ['unreached']" } } }
                  ],
                  "catch": { "var": "e", "do": [
                    { "set": { "var": "trace", "value": { "$": "trace + ['caught:' + e.message]" } } }
                  ] },
                  "finally": [
                    { "set": { "var": "trace", "value": { "$": "trace + ['finally']" } } }
                  ] },
                { "return": { "$": "trace" } }
            ]
        });
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!(["try", "caught:kaboom", "finally"])
        );
    }

    #[tokio::test]
    async fn finally_abrupt_completion_overrides() {
        let prog = program(exact_json!([
            { "try": [ { "throw": "lost" } ],
              "catch": { "do": [ { "return": "from-catch" } ] },
              "finally": [ { "return": "from-finally" } ] }
        ]));
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!("from-finally")
        );
    }

    #[tokio::test]
    async fn uncaught_throw_aborts_with_its_message() {
        let prog = program(exact_json!([ { "throw": "top-level failure" } ]));
        let err = run_prog(&prog, None, None, None, DepsCfg::default())
            .await
            .unwrap_err();
        assert_eq!(err.message, "top-level failure");
    }

    #[tokio::test]
    async fn thrown_non_strings_stringify_to_the_message() {
        let prog = program(exact_json!([
            { "try": [ { "throw": { "code": 5 } } ],
              "catch": { "var": "e", "do": [ { "return": { "$": "e.message" } } ] } }
        ]));
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!("{\"code\":5}")
        );
    }

    #[tokio::test]
    async fn expression_failures_are_catchable_throws() {
        let prog = program(exact_json!([
            { "try": [ { "set": { "var": "x", "value": { "$": "1 / 0" } } } ],
              "catch": { "var": "e", "do": [ { "return": { "$": "e.message" } } ] } }
        ]));
        let res = run_ok(&prog, DepsCfg::default()).await;
        let msg = res.result.as_str().expect("string message");
        assert!(msg.starts_with("expression `1 / 0` failed:"), "{msg:?}");
    }

    // ---- agents (host effects, budget, journaling) ------------------------------------------

    #[tokio::test]
    async fn agents_run_and_account_output_tokens() {
        let prog = program(exact_json!([
            { "agent": { "prompt": "p1" }, "as": "a" },
            { "agent": { "prompt": "p2" }, "as": "b" },
            { "return": { "$": "[a, b]" } }
        ]));
        let res = run_ok(
            &prog,
            DepsCfg {
                spawn: spawn_yielding("hello", 7, false),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(res.agents_spawned, 2);
        assert_eq!(res.output_tokens_spent, 14);
        assert_eq!(res.result, exact_json!(["hello", "hello"]));
    }

    #[tokio::test]
    async fn agent_schema_option_parses_json_and_opts_flow_through() {
        let prompts = Arc::new(Mutex::new(Vec::new()));
        let prog = program(exact_json!([
            { "agent": { "prompt": "analyze ${args.q}", "schema": { "type": "object" },
                         "label": "L", "model": "m", "agentType": "researcher" }, "as": "r" },
            { "return": { "$": "r" } }
        ]));
        // The recording spawner echoes the prompt as the agent text, so a
        // schema'd call must fail JSON parsing unless the prompt IS json.
        let res = run_prog(
            &program(exact_json!([
                { "agent": { "prompt": "{\"k\": 1}", "schema": { "type": "object" } }, "as": "r" },
                { "return": { "$": "r" } }
            ])),
            None,
            None,
            None,
            DepsCfg {
                spawn: recording_spawn(prompts.clone(), 2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(res.result, exact_json!({ "k": 1 }));

        // Interpolated prompts reach the spawner resolved.
        let res2 = run_prog(
            &prog,
            Some(exact_json!({ "q": 7 })),
            None,
            None,
            DepsCfg {
                spawn: recording_spawn(prompts.clone(), 2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(res2.result, exact_json!(null)); // "analyze 7" is not JSON → schema parse → null
        assert_eq!(
            prompts.lock().unwrap().as_slice(),
            ["{\"k\": 1}", "analyze 7"]
        );
    }

    #[tokio::test]
    async fn non_string_prompt_is_the_host_type_error() {
        let prog = program(exact_json!([ { "agent": { "prompt": 123 } } ]));
        let err = run_prog(&prog, None, None, None, DepsCfg::default())
            .await
            .unwrap_err();
        assert_eq!(err.message, "agent(prompt) expects a string prompt");
    }

    #[tokio::test]
    async fn budget_exhaustion_throws_from_the_host_and_is_catchable() {
        // Uncaught: the run fails with the host's message.
        let prog = program(exact_json!([
            { "agent": { "prompt": "p1" } },
            { "agent": { "prompt": "p2" } }
        ]));
        let err = run_prog(
            &prog,
            None,
            Some(10),
            None,
            DepsCfg {
                spawn: spawn_yielding("x", 10, false),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.message, "workflow token budget exhausted");

        // Caught: the program can react to it.
        let prog = program(exact_json!([
            { "agent": { "prompt": "p1" } },
            { "try": [ { "agent": { "prompt": "p2" } } ],
              "catch": { "var": "e", "do": [ { "return": { "$": "e.message" } } ] } }
        ]));
        let res = run_prog(
            &prog,
            None,
            Some(10),
            None,
            DepsCfg {
                spawn: spawn_yielding("x", 10, false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(res.result, exact_json!("workflow token budget exhausted"));
    }

    #[tokio::test]
    async fn budget_loop_reads_live_spend() {
        // Budget 100, 30 tokens per agent: remaining after each run is
        // 70 → 40, so the `> 50` guard admits exactly two iterations.
        let prog = exact_json!({
            "meta": meta(),
            "vars": [ { "var": "runs", "value": 0 } ],
            "body": [
                { "while": { "$": "budget_total != null && budget_remaining() > 50" }, "do": [
                    { "agent": { "prompt": "chunk ${runs}" } },
                    { "set": { "var": "runs", "value": { "$": "runs + 1" } } }
                ] },
                { "return": { "$": "[runs, budget_spent(), budget_total]" } }
            ]
        });
        let res = run_prog(
            &prog,
            None,
            Some(100),
            None,
            DepsCfg {
                spawn: spawn_yielding("ok", 30, false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(res.agents_spawned, 2);
        assert_eq!(res.result, exact_json!([2, 60, 100]));
    }

    #[tokio::test]
    async fn resume_replays_cached_agent_results_without_spawning() {
        // First run: live, journaled.
        let journal: Arc<MemJournal> = Arc::new(MemJournal::default());
        let prog = program(exact_json!([
            { "agent": { "prompt": "expensive question" }, "as": "a" },
            { "return": { "$": "a" } }
        ]));
        let first = run_prog(
            &prog,
            None,
            None,
            None,
            DepsCfg {
                spawn: spawn_yielding("expensive answer", 5, false),
                journal: Some(journal.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(first.result, exact_json!("expensive answer"));
        assert_eq!(first.agents_spawned, 1);
        assert_eq!(journal.entries.lock().unwrap().len(), 1);

        // Second run of the UNCHANGED program under the same run id: the
        // journal cache satisfies the call — a spawn would panic.
        let panicking: SpawnSubAgent = Arc::new(|_o: SpawnSubAgentOptions| {
            panic!("a cached call must not spawn");
        });
        let second = run_prog(
            &prog,
            None,
            None,
            Some(&first.run_id),
            DepsCfg {
                spawn: panicking,
                journal: Some(journal.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(second.result, exact_json!("expensive answer"));
        assert_eq!(second.agents_spawned, 0);
        assert_eq!(second.output_tokens_spent, 5);
        assert_eq!(second.run_id, first.run_id);
    }

    #[tokio::test]
    async fn resume_with_a_changed_call_breaks_the_prefix_and_runs_live() {
        let journal: Arc<MemJournal> = Arc::new(MemJournal::default());
        let prog1 = program(exact_json!([
            { "agent": { "prompt": "original" }, "as": "a" },
            { "return": { "$": "a" } }
        ]));
        let first = run_prog(
            &prog1,
            None,
            None,
            None,
            DepsCfg {
                spawn: spawn_yielding("v1", 5, false),
                journal: Some(journal.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Same run id, EDITED prompt: the cache must not serve the stale value.
        let prog2 = program(exact_json!([
            { "agent": { "prompt": "edited" }, "as": "a" },
            { "return": { "$": "a" } }
        ]));
        let second = run_prog(
            &prog2,
            None,
            None,
            Some(&first.run_id),
            DepsCfg {
                spawn: spawn_yielding("v2", 5, false),
                journal: Some(journal.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(second.result, exact_json!("v2"));
        assert_eq!(second.agents_spawned, 1);
    }

    // ---- parallel (parity requirement 2) ---------------------------------------------------

    #[tokio::test]
    async fn parallel_maps_throws_and_failures_to_null_and_never_rejects() {
        let prog = program(exact_json!([
            { "parallel": { "branches": [
                { "do": [ { "throw": "branch blew up" } ] },
                { "do": [ { "agent": { "prompt": "p" }, "as": "a" } ], "value": { "$": "a" } },
                { "do": [], "value": 42 },
                { "do": [ { "return": 7 } ], "value": "ignored" }
            ] }, "as": "out" },
            { "return": { "$": "out" } }
        ]));
        // The spawner fails the turn, so the host resolves that agent to null.
        let res = run_ok(
            &prog,
            DepsCfg {
                spawn: spawn_yielding("ok", 3, true),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(res.result, exact_json!([null, null, 42, 7]));
        // The failed agent still spawned and its output tokens still accrued.
        assert_eq!(res.agents_spawned, 1);
        assert_eq!(res.output_tokens_spent, 3);
    }

    #[tokio::test]
    async fn parallel_over_the_item_cap_rejects_with_the_host_message() {
        let branches: Vec<Json> = (0..PER_CALL_ITEM_CAP + 1)
            .map(|_| exact_json!({ "do": [] }))
            .collect();
        let prog = program(exact_json!([
            { "parallel": { "branches": branches } }
        ]));
        let err = run_prog(&prog, None, None, None, DepsCfg::default())
            .await
            .unwrap_err();
        assert_eq!(err.message, "parallel: 4097 items exceeds the 4096 cap");
    }

    #[tokio::test]
    async fn parallel_agents_flow_through_the_host_concurrency_cap() {
        let active = Arc::new(AtomicI64::new(0));
        let max_seen = Arc::new(AtomicI64::new(0));
        let spawn: SpawnSubAgent = {
            let active = active.clone();
            let max_seen = max_seen.clone();
            Arc::new(move |_o: SpawnSubAgentOptions| {
                let active = active.clone();
                let max_seen = max_seen.clone();
                let s = async_stream::stream! {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    yield EngineEvent::ItemCompleted { inference: None,
                        item: ThreadItem::AgentMessage { phase: None,
                            id: RuntimeString::from("m"),
                            text: RuntimeString::from("ok"),
                        },
                    };
                    yield EngineEvent::TurnCompleted {
                        turn_id: "wf".into(),
                        usage: Some(make_usage(None, Some(1))),
                        stop_reason: TerminalReason::Completed,
                    };
                    active.fetch_sub(1, Ordering::SeqCst);
                };
                Box::pin(s) as EngineEventStream
            })
        };
        let prog = program(exact_json!([
            { "parallel": { "branches": [
                { "do": [ { "agent": { "prompt": "a" } } ] },
                { "do": [ { "agent": { "prompt": "b" } } ] },
                { "do": [ { "agent": { "prompt": "c" } } ] }
            ] } }
        ]));
        let res = run_ok(
            &prog,
            DepsCfg {
                spawn,
                max_concurrency: 1,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(res.agents_spawned, 3);
        // The host's semaphore (capacity 1) serialized the streams.
        assert_eq!(max_seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn parallel_branch_agents_launch_in_array_order() {
        let prompts = Arc::new(Mutex::new(Vec::new()));
        let prog = program(exact_json!([
            { "parallel": { "branches": [
                { "do": [ { "agent": { "prompt": "p0" } } ] },
                { "do": [ { "agent": { "prompt": "p1" } } ] },
                { "do": [ { "agent": { "prompt": "p2" } } ] }
            ] } }
        ]));
        run_ok(
            &prog,
            DepsCfg {
                spawn: recording_spawn(prompts.clone(), 1),
                max_concurrency: 1,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(prompts.lock().unwrap().as_slice(), ["p0", "p1", "p2"]);
    }

    #[tokio::test]
    async fn parallel_and_map_bind_via_the_canonical_inner_as() {
        let prog = program(exact_json!([
            { "parallel": { "branches": [ { "do": [], "value": 1 }, { "do": [], "value": 2 } ],
                            "as": "pair" } },
            { "map": { "var": "n", "in": { "$": "pair" }, "mode": "seq",
                       "value": { "$": "n * 10" }, "as": "tens" } },
            { "return": { "$": "tens" } }
        ]));
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!([10, 20])
        );
    }

    #[tokio::test]
    async fn parallel_branches_share_enclosing_state() {
        let prog = exact_json!({
            "meta": meta(),
            "vars": [ { "var": "total", "value": 0 } ],
            "body": [
                { "parallel": { "branches": [
                    { "do": [ { "set": { "var": "total", "value": { "$": "total + 1" } } } ] },
                    { "do": [ { "set": { "var": "total", "value": { "$": "total + 1" } } } ] },
                    { "do": [ { "set": { "var": "total", "value": { "$": "total + 1" } } } ] }
                ] } },
                { "return": { "$": "total" } }
            ]
        });
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!(3)
        );
    }

    // ---- map / pipeline (parity requirement 3) -------------------------------------------------

    #[tokio::test]
    async fn map_seq_transforms_in_order_with_index() {
        let prog = program(exact_json!([
            { "map": { "var": "item", "index": "i", "in": [10, 20], "mode": "seq",
                       "do": [ { "set": { "var": "y", "value": { "$": "item + i" } } } ],
                       "value": { "$": "y" } }, "as": "out" },
            { "return": { "$": "out" } }
        ]));
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!([10, 21])
        );
    }

    #[tokio::test]
    async fn map_seq_propagates_throws() {
        let prog = program(exact_json!([
            { "map": { "var": "item", "in": [1, 2, 3], "mode": "seq",
                       "do": [ { "if": { "$": "item == 2" }, "then": [ { "throw": "seq stop" } ] } ],
                       "value": { "$": "item" } } }
        ]));
        let err = run_prog(&prog, None, None, None, DepsCfg::default())
            .await
            .unwrap_err();
        assert_eq!(err.message, "seq stop");
    }

    #[tokio::test]
    async fn map_parallel_resolves_throwing_items_to_null() {
        let prog = program(exact_json!([
            { "map": { "var": "item", "in": [1, 2, 3], "mode": "parallel",
                       "do": [ { "if": { "$": "item == 2" }, "then": [ { "throw": "drop" } ] } ],
                       "value": { "$": "item * 2" } }, "as": "out" },
            { "return": { "$": "out" } }
        ]));
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!([2, null, 6])
        );
    }

    #[tokio::test]
    async fn pipeline_flows_items_and_drops_thrown_stages() {
        // The host-runtime pipeline semantics, expressed as a flow program:
        // stage 1 doubles; stage 2 throws for 4, else +1 → [3, null, 7].
        let prog = program(exact_json!([
            { "map": { "var": "item", "in": [1, 2, 3], "mode": "pipeline", "stages": [
                { "value": { "$": "prev * 2" } },
                { "do": [ { "if": { "$": "prev == 4" }, "then": [ { "throw": "drop" } ] } ],
                  "value": { "$": "prev + 1" } }
            ] }, "as": "out" },
            { "return": { "$": "out" } }
        ]));
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!([3, null, 7])
        );
    }

    #[tokio::test]
    async fn pipeline_stage_bindings_prev_item_index() {
        // Default binding names: prev / <var> / index.
        let prog = program(exact_json!([
            { "map": { "var": "item", "in": ["a", "b"], "mode": "pipeline", "stages": [
                { "value": { "$": "[prev, item, index]" } }
            ] }, "as": "out" },
            { "return": { "$": "out" } }
        ]));
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!([["a", "a", 0], ["b", "b", 1]])
        );

        // Renamed bindings via var/index; prev stays fixed.
        let prog = program(exact_json!([
            { "map": { "var": "it", "index": "ix", "in": [5], "mode": "pipeline", "stages": [
                { "value": { "$": "prev + 1" } },
                { "value": { "$": "[prev, it, ix]" } }
            ] }, "as": "out" },
            { "return": { "$": "out" } }
        ]));
        assert_eq!(
            run_ok(&prog, DepsCfg::default()).await.result,
            exact_json!([[6, 5, 0]])
        );
    }

    #[tokio::test]
    async fn pipeline_has_no_barrier_between_stages() {
        // With no awaits inside the stages, each item's chain runs to
        // completion before the next chain starts — item 1 reaches stage 2
        // before item 2 has run stage 1, which a stage barrier would forbid.
        let (emit, seen) = status_recorder();
        let prog = program(exact_json!([
            { "map": { "var": "item", "in": [1, 2], "mode": "pipeline", "stages": [
                { "do": [ { "log": "s1-${item}" } ], "value": { "$": "prev" } },
                { "do": [ { "log": "s2-${item}" } ], "value": { "$": "prev" } }
            ] } }
        ]));
        run_ok(
            &prog,
            DepsCfg {
                emit: Some(emit),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            ["s1-1", "s2-1", "s1-2", "s2-2"]
        );
    }

    #[tokio::test]
    async fn map_parallel_and_pipeline_enforce_the_item_caps() {
        let prog = program(exact_json!([
            { "map": { "var": "x", "in": { "$": "range(4097)" }, "mode": "parallel", "value": 1 } }
        ]));
        let err = run_prog(&prog, None, None, None, DepsCfg::default())
            .await
            .unwrap_err();
        assert_eq!(err.message, "parallel: 4097 items exceeds the 4096 cap");

        let prog = program(exact_json!([
            { "map": { "var": "x", "in": { "$": "range(4097)" }, "mode": "pipeline",
                       "stages": [ { "value": 1 } ] } }
        ]));
        let err = run_prog(&prog, None, None, None, DepsCfg::default())
            .await
            .unwrap_err();
        assert_eq!(err.message, "pipeline: 4097 items exceeds the 4096 cap");
    }

    #[tokio::test]
    async fn map_over_a_non_array_throws() {
        let prog = program(exact_json!([
            { "map": { "var": "x", "in": 42, "mode": "seq", "value": 1 } }
        ]));
        let err = run_prog(&prog, None, None, None, DepsCfg::default())
            .await
            .unwrap_err();
        assert_eq!(
            err.message,
            "map: `in` must evaluate to an array (got number)"
        );
    }

    // ---- nested workflows -------------------------------------------------------------------------

    fn child_resolver(child: Json) -> ResolveWorkflowFn {
        let composed = script(&child);
        Arc::new(move |name: &RuntimeString| {
            if name == "child" {
                Some(composed.clone())
            } else {
                None
            }
        })
    }

    #[tokio::test]
    async fn nested_workflow_runs_inline_and_shares_counters() {
        let child = exact_json!({
            "meta": { "name": "child", "description": "c" },
            "body": [
                { "agent": { "prompt": "child of ${args.parent}" }, "as": "r" },
                { "return": { "$": "r" } }
            ]
        });
        let prog = program(exact_json!([
            { "agent": { "prompt": "top" }, "as": "a" },
            { "workflow": { "name": "child", "args": { "parent": "top" } }, "as": "c" },
            { "return": { "a": { "$": "a" }, "c": { "$": "c" } } }
        ]));
        let prompts = Arc::new(Mutex::new(Vec::new()));
        let res = run_prog(
            &prog,
            None,
            None,
            None,
            DepsCfg {
                spawn: recording_spawn(prompts.clone(), 2),
                resolve: Some(child_resolver(child)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(res.agents_spawned, 2);
        assert_eq!(res.output_tokens_spent, 4);
        assert_eq!(res.result, exact_json!({ "a": "top", "c": "child of top" }));
        assert_eq!(prompts.lock().unwrap().as_slice(), ["top", "child of top"]);
    }

    #[tokio::test]
    async fn nesting_beyond_one_level_is_the_host_error() {
        // The child itself tries to run a workflow — the host rejects depth 2.
        let child = exact_json!({
            "meta": { "name": "child", "description": "c" },
            "body": [ { "workflow": { "name": "child" } } ]
        });
        let prog = program(exact_json!([ { "workflow": { "name": "child" } } ]));
        let err = run_prog(
            &prog,
            None,
            None,
            None,
            DepsCfg {
                resolve: Some(child_resolver(child)),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.message, "workflow() nesting is one level only");
    }

    #[tokio::test]
    async fn unknown_nested_workflow_is_the_host_error_and_catchable() {
        let prog = program(exact_json!([
            { "try": [ { "workflow": { "name": "missing" } } ],
              "catch": { "var": "e", "do": [ { "return": { "$": "e.message" } } ] } }
        ]));
        let resolve: ResolveWorkflowFn = Arc::new(|_n: &RuntimeString| None);
        let res = run_prog(
            &prog,
            None,
            None,
            None,
            DepsCfg {
                resolve: Some(resolve),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(res.result, exact_json!("unknown workflow: missing"));
    }

    // ---- phase / log / abort ------------------------------------------------------------------------

    #[tokio::test]
    async fn phase_and_log_emit_host_statuses() {
        let (emit, seen) = status_recorder();
        let prog = program(exact_json!([
            { "phase": "Research" },
            { "log": "n=${1 + 1}" },
            { "log": { "an": "object" } }
        ]));
        run_ok(
            &prog,
            DepsCfg {
                emit: Some(emit),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            ["phase: Research", "n=2", "[object Object]"]
        );
    }

    #[tokio::test]
    async fn aborted_signal_rejects_agent_calls() {
        let signal = AbortSignal::new();
        signal.cancel();
        let prog = program(exact_json!([ { "agent": { "prompt": "p" } } ]));
        let err = run_prog(
            &prog,
            None,
            None,
            None,
            DepsCfg {
                signal,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.message, "workflow was aborted");
    }

    // ---- runner-level diagnostics -----------------------------------------------------------------------

    #[tokio::test]
    async fn classic_scripts_report_the_flow_program_requirement() {
        let err = run_workflow(
            WorkflowRunInput {
                script: RuntimeString::from(
                    "export const meta = { name: 'demo', description: 'd' }\nreturn null",
                ),
                args: None,
                resume_from_run_id: None,
                budget_tokens: None,
            },
            deps(DepsCfg::default()),
            Arc::new(FlowWorkflowScriptRunner),
        )
        .await
        .unwrap_err();
        assert_eq!(err.message, NOT_A_FLOW_PROGRAM);
    }

    #[tokio::test]
    async fn body_validation_errors_surface_with_paths() {
        let prog = program(exact_json!([ { "definitely_not_a_step": 1 } ]));
        let err = run_prog(&prog, None, None, None, DepsCfg::default())
            .await
            .unwrap_err();
        assert!(
            err.message
                .starts_with("body[0]: no step tag found among keys [definitely_not_a_step]"),
            "{:?}",
            err.message
        );
    }

    #[test]
    fn recover_survives_multi_line_meta_headers() {
        // A hand-composed script may format the meta binding across lines.
        let wrapped = RuntimeString::from(format!(
            "{WORKFLOW_WRAP_PREFIX}const meta = {{\n  \"name\": \"n{{\",\n  \"description\": \"}}d\"\n}};\n{{\"meta\":{{}},\"body\":[]}}{WORKFLOW_WRAP_SUFFIX}"
        ));
        let recovered = recover_workflow_flow_source(&wrapped).expect("recovers");
        assert_eq!(recovered, "{\"meta\":{},\"body\":[]}");
    }

    #[test]
    fn recover_rejects_classic_bodies() {
        let wrapped = RuntimeString::from(format!(
            "{WORKFLOW_WRAP_PREFIX}const x = 1{WORKFLOW_WRAP_SUFFIX}"
        ));
        assert_eq!(
            recover_workflow_flow_source(&wrapped).unwrap_err().message,
            NOT_A_FLOW_PROGRAM
        );
    }
}
