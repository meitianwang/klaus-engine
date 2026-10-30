//! Flow programs — the engine's native workflow/orchestration program format:
//! a JSON document of steps with embedded pure expressions, interpreted
//! in-process. Flow programs are the production implementation of the
//! `workflow` tool's [`crate::engine::workflow::runtime::WorkflowScriptRunner`]
//! seam. The interpreter drives every effect through the host sandbox it is
//! handed, so budget, journaling/resume, concurrency and lifetime caps,
//! output caps, and timeouts all remain host-owned.
//!
//! # The format (canonical specification)
//!
//! A program is one JSON object:
//!
//! ```json
//! { "meta":  { "name": "…", "description": "…", "whenToUse": "…",
//!              "phases": [ { "title": "…", "detail": "…", "model": "…" } ] },
//!   "vars":  [ { "var": "name", "value": VEXPR }, … ],
//!   "funcs": { "fname": { "params": ["a", "b"], "body": [ STEP… ] }, … },
//!   "body":  [ STEP… ] }
//! ```
//!
//! `meta` is a PURE literal (strings only, no expressions) so it can be read
//! without executing anything — [`parse_flow_meta`] does exactly that. `vars`
//! are evaluated in order into the program scope (later entries may reference
//! earlier ones). `funcs` are callable from anywhere via the `call` step;
//! recursion is allowed (bounded by [`interp::MAX_CALL_DEPTH`]). The program's
//! result is the value of the first `return` reached in `body` (null when the
//! body falls off the end).
//!
//! ## Value expressions (VEXPR)
//!
//! Anywhere a value is needed, any JSON literal may appear; it is evaluated
//! DEEPLY with two escape hatches:
//!
//! - An object exactly of the form `{"$": "<expression>"}` evaluates the
//!   expression (at any depth inside arrays/objects). `{"$": <non-string>}`
//!   stays a literal `"$"` key (its value is still evaluated deeply). To write
//!   a literal object with a `"$"` string key, use the key `"$$"` — it emits
//!   the key `"$"` verbatim.
//! - A string containing `${…}` is interpolated: each `${expr}` is evaluated
//!   and string-coerced (strings stay themselves; other values print their
//!   compact JSON), then concatenated. Escape a literal `${` as `$${`.
//!   Strings without `${` are literal.
//!
//! Expressions use the Common Expression Language: variables in scope are
//! every flow variable plus `args` (the run input) and `budget_total` (null
//! when no budget target), plus loop/branch/stage bindings. Expressions are
//! pure, synchronous, and deterministic — there is no clock and no
//! randomness. Evaluation failures are flow throws (catchable by `try`). See
//! [`eval`] for the registered standard library and what the expression
//! engine provides natively.
//!
//! ## Steps
//!
//! Each step is a JSON object carrying exactly ONE step tag plus that tag's
//! companion keys (`then`/`else` for `if`, `do` for `while`, `catch`/`finally`
//! for `try`, `default` for `switch`, and `as` on value-producing steps to
//! bind the result declare-or-update into the current scope):
//!
//! - `{"set": {"var": "x", "value": VEXPR}}` — declare-or-update `x` (lexical
//!   scoping; every nested body is a block scope).
//! - `{"if": VEXPR, "then": [STEP…], "else": [STEP…]}` — `else` optional.
//! - `{"switch": [{"case": VEXPR, "do": [STEP…]}…], "default": [STEP…]}` —
//!   the first truthy case runs. Truthiness: null/false/0/"" are false;
//!   everything else (including empty arrays/objects) is true.
//! - `{"while": VEXPR, "do": [STEP…]}` with `{"break": null}` /
//!   `{"continue": null}` inside loop bodies.
//! - `{"for": {"var": "x", "index": "i", "in": VEXPR, "do": [STEP…]}}` —
//!   sequential foreach over an array (`index` optional).
//! - `{"parallel": {"branches": [{"do": [STEP…], "value": VEXPR}…], "as": "x"}}`
//!   — barrier: branches run concurrently (through the host's concurrency
//!   cap); a branch that throws resolves to null (the step never throws); the
//!   result is the array of branch values (`value` evaluated after `do`,
//!   default null; a `return` inside a branch supplies the value directly).
//!   For `parallel` and `map`, `"as"` lives inside the tag value (the sibling
//!   position is also accepted, but not both).
//! - `{"map": {"var": "item", "index": "i", "in": VEXPR, "mode": "seq" |
//!   "parallel" | "pipeline", "do": [STEP…], "value": VEXPR,
//!   "stages": [{"do": [STEP…], "value": VEXPR}…], "as": "x"}}` — per-item
//!   `do`+`value` for seq/parallel (seq propagates throws; parallel maps a
//!   throwing item to null). Pipeline mode runs each item through the stages
//!   with NO barrier between stages; stage bindings are `prev` (previous
//!   stage's value; the item for stage 0), the item under `var`, and the
//!   position under `index` (default name `index`); a throwing stage drops
//!   that item to null and skips its remaining stages. `parallel`/`pipeline`
//!   modes enforce the host's per-call item cap.
//! - `{"agent": {"prompt": VEXPR, "label"?, "phase"?, "schema"?: <literal
//!   schema object>, "model"?, "isolation"?, "agentType"?}, "as": "x"}` —
//!   spawns one sub-agent through the host (its null-on-failure result
//!   mapping, journal cache, caps, and budget-exhausted throw apply
//!   unchanged).
//! - `{"workflow": {"name" | "scriptPath": VEXPR, "args": VEXPR}, "as": "x"}`
//!   — runs a registered workflow inline (the host enforces one-level nesting
//!   and shares counters/budget/journal).
//! - `{"phase": VEXPR}` / `{"log": VEXPR}` — progress statuses.
//! - `{"try": [STEP…], "catch": {"var": "e", "do": [STEP…]}, "finally":
//!   [STEP…]}` — `e` binds `{"message": string}`; `finally` always runs and
//!   its abrupt completion overrides.
//! - `{"throw": VEXPR}` — throw (a string throws itself; other values throw
//!   their compact JSON).
//! - `{"return": VEXPR}` — return from the enclosing function, or the program.
//! - `{"call": {"fn": "name", "args": [VEXPR…]}, "as": "x"}` — call a user
//!   function (missing arguments bind null; extra arguments are ignored;
//!   functions close over the program scope).
//!
//! ## Validation
//!
//! [`program::validate_program`] parses, validates, and pre-compiles a
//! program; unknown step tags, malformed escapes, and non-literal `meta` are
//! rejected with path-precise messages. [`program::FLOW_PROGRAM_SCHEMA`] is
//! the format's JSON Schema.
//!
//! ## Plugging into the workflow runtime
//!
//! The workflow runtime parses scripts that begin with a
//! `export const meta = {…}` header. [`program::compose_workflow_script`]
//! wraps a flow program in that form (a one-line JSON meta header followed by
//! the unmodified program document); [`interp::FlowWorkflowScriptRunner`]
//! recovers the document from the wrapped source and interprets it.

pub mod eval;
pub mod interp;
pub mod program;

pub use interp::{FlowWorkflowScriptRunner, MAX_CALL_DEPTH};
pub use program::{
    compose_workflow_script, is_flow_source, parse_flow_meta, validate_program, FlowError,
    FlowMeta, FlowPhase, FlowProgram, FLOW_PROGRAM_SCHEMA,
};
