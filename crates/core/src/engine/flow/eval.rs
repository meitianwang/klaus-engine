//! The flow expression environment: JSON ⇄ expression-value conversion, the
//! registered standard library, live budget reads, and value-expression
//! (VEXPR) evaluation.
//!
//! Expressions are PURE and synchronous — every effect happens at step level.
//! Determinism: no clock and no randomness are exposed (there is no `now()` /
//! `random()`), so an expression's value depends only on the variables in
//! scope. Everything registered here is deterministic.
//!
//! ## What the expression engine provides natively
//!
//! Comprehension macros `map` / `filter` / `exists` / `all` / `exists_one`,
//! `has(...)`, the `in` operator, the ternary `cond ? a : b`, arithmetic on
//! same-typed numbers, string/list concatenation with `+`, comparisons
//! (including across numeric types), short-circuit `&&` / `||`, plus the
//! functions `size`, `contains`, `startsWith`, `endsWith`, `matches` (regex),
//! `string`, `int`, `uint`, `double`, `bytes`. Note: arithmetic between an
//! integer and a double is a type error — convert with `double(x)` / `int(x)`.
//!
//! ## What this module registers on top
//!
//! `keys`, `values`, `has_key`, `flatten`, `unique`, `sort`, `reverse`,
//! `join`, `split`, `trim`, `upper`, `lower`, `replace`, `starts_with`,
//! `ends_with`, `find_all`, `sum`, `min`, `max` (list-aware), `range`,
//! `slice`, `index_of`, `to_json`, `from_json`, `coalesce`, and the live
//! budget reads `budget_spent()` / `budget_remaining()`.
//!
//! `keys` / `values` return entries sorted by key so map iteration is
//! deterministic (prefer them over comprehension macros applied directly to a
//! map, whose iteration order is unspecified).

use std::collections::HashMap;
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use cel_interpreter::extractors::{Arguments, This};
use cel_interpreter::objects::{Key, Map as CelMap};
use cel_interpreter::{Context, ExecutionError, FunctionContext, Value as Cel};

use crate::engine::flow::program::{CelExpr, FlowError, InterpPart, VExpr};
use crate::engine::workflow::runtime::WorkflowSandbox;
use crate::runtime_values::{
    runtime_json, RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString,
};
use crate::types::json::Json;

/// Cap on `range(n)` so an expression cannot allocate unbounded memory.
const RANGE_CAP: i64 = 1_000_000;

/// Where the live budget reads come from. Workflow programs read the run's
/// shared spend counters through the sandbox (thread-safe, LIVE — not a
/// snapshot); expression-level tests run without a sandbox (`budget_total` is
/// null, nothing is spent, remaining is unbounded).
#[derive(Clone)]
pub(crate) enum BudgetSource {
    Workflow(Arc<WorkflowSandbox>),
    #[cfg(test)]
    Unlimited,
}

impl BudgetSource {
    fn total(&self) -> Option<i64> {
        match self {
            BudgetSource::Workflow(sb) => sb.budget_total(),
            #[cfg(test)]
            BudgetSource::Unlimited => None,
        }
    }

    fn spent(&self) -> u64 {
        match self {
            BudgetSource::Workflow(sb) => sb.budget_spent(),
            #[cfg(test)]
            BudgetSource::Unlimited => 0,
        }
    }

    fn remaining(&self) -> f64 {
        match self {
            BudgetSource::Workflow(sb) => sb.budget_remaining(),
            #[cfg(test)]
            BudgetSource::Unlimited => f64::INFINITY,
        }
    }
}

// ---- JSON ⇄ expression-value conversion -----------------------------------------

/// JSON → expression value. Integer-valued JSON numbers become `Int` whenever
/// they fit in `i64` (so literal and variable integers share one type and mix
/// in arithmetic); other numbers become `Float`.
pub(crate) fn json_to_cel(v: &Json) -> Result<Cel, String> {
    match v {
        RuntimeJson::Null => Ok(Cel::Null),
        RuntimeJson::Bool(b) => Ok(Cel::Bool(*b)),
        RuntimeJson::Number(n) => {
            let value = n.as_f64();
            if value.is_finite()
                && value.fract() == 0.0
                && value >= i64::MIN as f64
                && value <= i64::MAX as f64
            {
                Ok(Cel::Int(value as i64))
            } else if value.is_finite()
                && value.fract() == 0.0
                && value >= 0.0
                && value <= u64::MAX as f64
            {
                Ok(Cel::UInt(value as u64))
            } else {
                Ok(Cel::Float(value))
            }
        }
        RuntimeJson::String(s) => {
            Ok(Cel::String(Arc::new(s.try_to_utf8().map_err(|error| {
                format!("CEL string boundary rejected {error}")
            })?)))
        }
        RuntimeJson::Array(items) => Ok(Cel::List(Arc::new(
            items
                .iter()
                .map(json_to_cel)
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        RuntimeJson::Object(object) => {
            let mut map = HashMap::with_capacity(object.len());
            for (key, value) in object.entries() {
                let key = key
                    .try_to_utf8()
                    .map_err(|error| format!("CEL object-key boundary rejected {error}"))?;
                map.insert(Key::String(Arc::new(key)), json_to_cel(value)?);
            }
            Ok(Cel::Map(CelMap { map: Arc::new(map) }))
        }
    }
}

/// Expression value → JSON. Non-finite floats become null (they have no JSON
/// representation); non-JSON expression types (functions, …) are an error.
pub(crate) fn cel_to_json(v: &Cel) -> Result<Json, String> {
    match v {
        Cel::Null => Ok(RuntimeJson::Null),
        Cel::Bool(value) => Ok(RuntimeJson::Bool(*value)),
        Cel::Int(value) => Ok(RuntimeJson::Number(RuntimeNumber::from(*value))),
        Cel::UInt(value) => Ok(RuntimeJson::Number(RuntimeNumber::from(*value))),
        Cel::Float(value) if value.is_finite() => {
            Ok(RuntimeJson::Number(RuntimeNumber::from_f64(*value)))
        }
        Cel::Float(_) => Ok(RuntimeJson::Null),
        Cel::String(value) => Ok(RuntimeJson::String(RuntimeString::from(value.as_str()))),
        Cel::Bytes(value) => Ok(RuntimeJson::String(RuntimeString::from(
            BASE64_STANDARD.encode(value.as_slice()),
        ))),
        Cel::List(values) => Ok(RuntimeJson::Array(
            values
                .iter()
                .map(cel_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Cel::Map(map) => {
            let mut entries = map.map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| key_order(key));
            let mut object = RuntimeObject::new();
            for (key, value) in entries {
                object.insert(RuntimeString::from(key.to_string()), cel_to_json(value)?);
            }
            Ok(RuntimeJson::Object(object))
        }
        Cel::Duration(value) => value
            .num_nanoseconds()
            .map(|value| RuntimeJson::Number(RuntimeNumber::from(value)))
            .ok_or_else(|| "CEL duration overflows JSON nanoseconds".to_string()),
        Cel::Timestamp(value) => Ok(RuntimeJson::String(RuntimeString::from(value.to_rfc3339()))),
        Cel::Function(_, _) => Err("CEL function is not a JSON value".to_string()),
    }
}

/// The deterministic string coercion applied by `${...}` interpolation and by
/// `throw` messages: strings stay themselves; null/booleans/numbers print
/// their JSON form; arrays and objects serialize to compact JSON.
pub(crate) fn flow_string(v: &Json) -> RuntimeString {
    match v {
        RuntimeJson::String(s) => s.clone(),
        other => runtime_json::stringify(other),
    }
}

/// Flow truthiness (for `if` / `while` / `switch` conditions): `null`, `false`,
/// `0`, and `""` are false; everything else — including empty arrays and
/// objects — is true.
pub(crate) fn truthy(v: &Json) -> bool {
    match v {
        RuntimeJson::Null => false,
        RuntimeJson::Bool(b) => *b,
        RuntimeJson::Number(n) => n.as_f64() != 0.0,
        RuntimeJson::String(s) => !s.is_empty(),
        RuntimeJson::Array(_) | RuntimeJson::Object(_) => true,
    }
}

// ---- environment ------------------------------------------------------------------

/// Build the root expression context for one program run: the standard
/// library, the live budget functions, and the fixed globals `args` and
/// `budget_total`. Per-evaluation variables layer on top as a child context.
pub(crate) fn build_root_context(
    args: &Json,
    budget: &BudgetSource,
) -> Result<Context<'static>, FlowError> {
    let mut ctx = Context::default();
    register_stdlib(&mut ctx);

    let b = budget.clone();
    ctx.add_function("budget_spent", move || -> Result<Cel, ExecutionError> {
        Ok(Cel::Int(b.spent().min(i64::MAX as u64) as i64))
    });
    let b = budget.clone();
    ctx.add_function(
        "budget_remaining",
        move || -> Result<Cel, ExecutionError> { Ok(Cel::Float(b.remaining())) },
    );

    ctx.add_variable_from_value(
        "args",
        json_to_cel(args).map_err(|error| FlowError::new(format!("args: {error}")))?,
    );
    ctx.add_variable_from_value(
        "budget_total",
        match budget.total() {
            Some(t) => Cel::Int(t),
            None => Cel::Null,
        },
    );
    Ok(ctx)
}

/// Evaluate one compiled expression against the root context plus the given
/// variable snapshot. Failures become flow throws (catchable by `try`).
pub(crate) fn eval_cel(
    expr: &CelExpr,
    root: &Context<'static>,
    vars: &HashMap<String, Json>,
) -> Result<Json, FlowError> {
    let mut child = root.new_inner_scope();
    for (name, value) in vars {
        child.add_variable_from_value(
            name.clone(),
            json_to_cel(value)
                .map_err(|error| FlowError::new(format!("variable `{name}`: {error}")))?,
        );
    }
    // Evaluation is pure, so an engine panic leaves no state behind; guard it
    // so any such failure surfaces as a normal (catchable) flow throw.
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        expr.program.execute(&child)
    }))
    .map_err(|_| {
        FlowError::new(
            RuntimeString::from("expression `")
                .concat(&expr.source)
                .concat(&RuntimeString::from("` failed: internal evaluation panic")),
        )
    })?
    .map_err(|error| {
        FlowError::new(
            RuntimeString::from("expression `")
                .concat(&expr.source)
                .concat(&RuntimeString::from("` failed: "))
                .concat(&RuntimeString::from(error.to_string())),
        )
    })?;
    cel_to_json(&out).map_err(|e| {
        FlowError::new(
            RuntimeString::from("expression `")
                .concat(&expr.source)
                .concat(&RuntimeString::from("` produced a non-JSON value: "))
                .concat(&RuntimeString::from(e)),
        )
    })
}

/// Evaluate a value expression: literals evaluate to themselves (deeply),
/// `{"$": "expr"}` escapes evaluate the expression, and strings interpolate
/// their `${...}` segments (each result coerced with [`flow_string`]).
pub(crate) fn eval_vexpr(
    v: &VExpr,
    root: &Context<'static>,
    vars: &HashMap<String, Json>,
) -> Result<Json, FlowError> {
    match v {
        VExpr::Null => Ok(RuntimeJson::Null),
        VExpr::Bool(b) => Ok(RuntimeJson::Bool(*b)),
        VExpr::Number(n) => Ok(RuntimeJson::Number(*n)),
        VExpr::LitString(s) => Ok(RuntimeJson::String(s.clone())),
        VExpr::Interp(parts) => {
            let mut out = RuntimeString::default();
            for part in parts {
                match part {
                    InterpPart::Lit(s) => out = out.concat(s),
                    InterpPart::Expr(e) => {
                        out = out.concat(&flow_string(&eval_cel(e, root, vars)?));
                    }
                }
            }
            Ok(RuntimeJson::String(out))
        }
        VExpr::Cel(e) => eval_cel(e, root, vars),
        VExpr::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(eval_vexpr(item, root, vars)?);
            }
            Ok(RuntimeJson::Array(out))
        }
        VExpr::Object(fields) => {
            let mut out = RuntimeObject::new();
            for (k, fv) in fields {
                out.insert(k.clone(), eval_vexpr(fv, root, vars)?);
            }
            Ok(RuntimeJson::Object(out))
        }
    }
}

// ---- standard library ---------------------------------------------------------------

fn func_err(name: &str, msg: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::function_error(name, msg.to_string())
}

/// A deterministic ordering key across the map key types, for sorted
/// `keys()` / `values()` output.
fn key_order(k: &Key) -> (u8, String) {
    match k {
        Key::Int(i) => (0, format!("{i:020}")),
        Key::Uint(u) => (0, format!("{u:020}")),
        Key::Bool(b) => (1, b.to_string()),
        Key::String(s) => (2, s.to_string()),
    }
}

fn key_to_cel(k: &Key) -> Cel {
    match k {
        Key::Int(i) => Cel::Int(*i),
        Key::Uint(u) => Cel::UInt(*u),
        Key::Bool(b) => Cel::Bool(*b),
        Key::String(s) => Cel::String(s.clone()),
    }
}

fn sorted_entries(map: &CelMap) -> Vec<(&Key, &Cel)> {
    let mut entries: Vec<(&Key, &Cel)> = map.map.iter().collect();
    entries.sort_by_key(|(k, _)| key_order(k));
    entries
}

/// A comparable scalar for `sort` / `min` / `max`.
enum Comparable {
    Num(f64),
    Str(Arc<String>),
}

fn comparable(name: &str, v: &Cel) -> Result<Comparable, ExecutionError> {
    match v {
        Cel::Int(i) => Ok(Comparable::Num(*i as f64)),
        Cel::UInt(u) => Ok(Comparable::Num(*u as f64)),
        Cel::Float(f) => Ok(Comparable::Num(*f)),
        Cel::String(s) => Ok(Comparable::Str(s.clone())),
        other => Err(func_err(
            name,
            format!("requires numbers or strings, got {other:?}"),
        )),
    }
}

fn compare_values(name: &str, a: &Cel, b: &Cel) -> Result<std::cmp::Ordering, ExecutionError> {
    use std::cmp::Ordering;
    match (comparable(name, a)?, comparable(name, b)?) {
        (Comparable::Num(x), Comparable::Num(y)) => {
            Ok(x.partial_cmp(&y).unwrap_or(Ordering::Equal))
        }
        (Comparable::Str(x), Comparable::Str(y)) => Ok(x.cmp(&y)),
        _ => Err(func_err(
            name,
            "cannot compare a number with a string (the list must be all numbers or all strings)",
        )),
    }
}

fn as_list<'v>(name: &str, v: &'v Cel) -> Result<&'v Arc<Vec<Cel>>, ExecutionError> {
    match v {
        Cel::List(items) => Ok(items),
        other => Err(func_err(name, format!("requires a list, got {other:?}"))),
    }
}

fn as_string<'v>(name: &str, v: &'v Cel) -> Result<&'v Arc<String>, ExecutionError> {
    match v {
        Cel::String(s) => Ok(s),
        other => Err(func_err(name, format!("requires a string, got {other:?}"))),
    }
}

fn as_int(name: &str, v: &Cel) -> Result<i64, ExecutionError> {
    match v {
        Cel::Int(i) => Ok(*i),
        Cel::UInt(u) => Ok((*u).min(i64::MAX as u64) as i64),
        other => Err(func_err(
            name,
            format!("requires an integer, got {other:?}"),
        )),
    }
}

/// Resolve one `slice` bound: a negative index counts from the end; the
/// result is clamped to `[0, len]`.
fn slice_bound(raw: i64, len: usize) -> usize {
    let len = len as i64;
    let idx = if raw < 0 { len + raw } else { raw };
    idx.clamp(0, len) as usize
}

/// The variadic-or-single-list argument convention of `min` / `max` /
/// `coalesce`: one list argument spreads to its elements.
fn spread_args(args: &[Cel]) -> Vec<Cel> {
    match args {
        [Cel::List(items)] => items.as_ref().clone(),
        other => other.to_vec(),
    }
}

fn register_stdlib(ctx: &mut Context<'static>) {
    // -- maps ------------------------------------------------------------------
    ctx.add_function(
        "keys",
        |This(this): This<Cel>| -> Result<Cel, ExecutionError> {
            match &this {
                Cel::Map(m) => Ok(Cel::List(Arc::new(
                    sorted_entries(m)
                        .iter()
                        .map(|(k, _)| key_to_cel(k))
                        .collect(),
                ))),
                other => Err(func_err("keys", format!("requires a map, got {other:?}"))),
            }
        },
    );
    ctx.add_function(
        "values",
        |This(this): This<Cel>| -> Result<Cel, ExecutionError> {
            match &this {
                Cel::Map(m) => Ok(Cel::List(Arc::new(
                    sorted_entries(m)
                        .iter()
                        .map(|(_, v)| (*v).clone())
                        .collect(),
                ))),
                other => Err(func_err("values", format!("requires a map, got {other:?}"))),
            }
        },
    );
    ctx.add_function(
        "has_key",
        |This(this): This<Cel>, key: Cel| -> Result<Cel, ExecutionError> {
            let map = match &this {
                Cel::Map(m) => m,
                other => {
                    return Err(func_err(
                        "has_key",
                        format!("requires a map, got {other:?}"),
                    ))
                }
            };
            let key = match key {
                Cel::String(s) => Key::String(s),
                Cel::Int(i) => Key::Int(i),
                Cel::UInt(u) => Key::Uint(u),
                Cel::Bool(b) => Key::Bool(b),
                other => return Err(func_err("has_key", format!("invalid key type {other:?}"))),
            };
            Ok(Cel::Bool(map.map.contains_key(&key)))
        },
    );

    // -- lists -----------------------------------------------------------------
    ctx.add_function(
        "flatten",
        |This(this): This<Cel>| -> Result<Cel, ExecutionError> {
            let items = as_list("flatten", &this)?;
            let mut out: Vec<Cel> = Vec::new();
            for item in items.iter() {
                match item {
                    Cel::List(inner) => out.extend(inner.iter().cloned()),
                    other => out.push(other.clone()),
                }
            }
            Ok(Cel::List(Arc::new(out)))
        },
    );
    ctx.add_function(
        "unique",
        |This(this): This<Cel>| -> Result<Cel, ExecutionError> {
            let items = as_list("unique", &this)?;
            let mut out: Vec<Cel> = Vec::new();
            for item in items.iter() {
                if !out.iter().any(|seen| seen == item) {
                    out.push(item.clone());
                }
            }
            Ok(Cel::List(Arc::new(out)))
        },
    );
    ctx.add_function(
        "sort",
        |This(this): This<Cel>| -> Result<Cel, ExecutionError> {
            let items = as_list("sort", &this)?;
            let mut out = items.as_ref().clone();
            let mut err: Option<ExecutionError> = None;
            out.sort_by(|a, b| match compare_values("sort", a, b) {
                Ok(ord) => ord,
                Err(e) => {
                    err.get_or_insert(e);
                    std::cmp::Ordering::Equal
                }
            });
            match err {
                Some(e) => Err(e),
                None => Ok(Cel::List(Arc::new(out))),
            }
        },
    );
    ctx.add_function(
        "reverse",
        |This(this): This<Cel>| -> Result<Cel, ExecutionError> {
            match &this {
                Cel::List(items) => Ok(Cel::List(Arc::new(items.iter().rev().cloned().collect()))),
                Cel::String(s) => Ok(Cel::String(Arc::new(s.chars().rev().collect()))),
                other => Err(func_err(
                    "reverse",
                    format!("requires a list or a string, got {other:?}"),
                )),
            }
        },
    );
    ctx.add_function(
        "join",
        |ftx: &FunctionContext, This(this): This<Cel>| -> Result<Cel, ExecutionError> {
            let items = as_list("join", &this)?;
            // Arguments not consumed by the target/`this` extraction.
            let rest = &ftx.args[ftx.arg_idx..];
            let sep = match rest {
                [] => String::new(),
                [sep] => as_string("join", &ftx.resolve(sep.clone())?)?.to_string(),
                more => {
                    return Err(func_err(
                        "join",
                        format!(
                            "takes a list and an optional separator, got {} extra arguments",
                            more.len()
                        ),
                    ))
                }
            };
            let mut parts: Vec<String> = Vec::with_capacity(items.len());
            for item in items.iter() {
                let json = cel_to_json(item).map_err(|error| func_err("join", error))?;
                let part = flow_string(&json)
                    .try_to_utf8()
                    .map_err(|error| func_err("join", error))?;
                parts.push(part);
            }
            Ok(Cel::String(Arc::new(parts.join(&sep))))
        },
    );
    ctx.add_function(
        "sum",
        |This(this): This<Cel>| -> Result<Cel, ExecutionError> {
            let items = as_list("sum", &this)?;
            let mut int_sum: i64 = 0;
            let mut float_sum: f64 = 0.0;
            let mut is_float = false;
            for item in items.iter() {
                match item {
                    Cel::Int(i) => {
                        int_sum = int_sum
                            .checked_add(*i)
                            .ok_or_else(|| func_err("sum", "integer overflow"))?;
                        float_sum += *i as f64;
                    }
                    Cel::UInt(u) => {
                        let i =
                            i64::try_from(*u).map_err(|_| func_err("sum", "integer overflow"))?;
                        int_sum = int_sum
                            .checked_add(i)
                            .ok_or_else(|| func_err("sum", "integer overflow"))?;
                        float_sum += i as f64;
                    }
                    Cel::Float(f) => {
                        is_float = true;
                        float_sum += f;
                    }
                    other => {
                        return Err(func_err(
                            "sum",
                            format!("requires a list of numbers, got {other:?}"),
                        ))
                    }
                }
            }
            Ok(if is_float {
                Cel::Float(float_sum)
            } else {
                Cel::Int(int_sum)
            })
        },
    );
    // List-aware min/max replace the built-ins of the same names: a single
    // list argument spreads to its elements.
    ctx.add_function(
        "min",
        |Arguments(args): Arguments| -> Result<Cel, ExecutionError> {
            extremum("min", &spread_args(&args), std::cmp::Ordering::Less)
        },
    );
    ctx.add_function(
        "max",
        |Arguments(args): Arguments| -> Result<Cel, ExecutionError> {
            extremum("max", &spread_args(&args), std::cmp::Ordering::Greater)
        },
    );
    ctx.add_function("range", |n: Cel| -> Result<Cel, ExecutionError> {
        let n = as_int("range", &n)?;
        if n > RANGE_CAP {
            return Err(func_err(
                "range",
                format!("{n} exceeds the {RANGE_CAP} cap"),
            ));
        }
        Ok(Cel::List(Arc::new((0..n.max(0)).map(Cel::Int).collect())))
    });
    ctx.add_function(
        "slice",
        |ftx: &FunctionContext, This(this): This<Cel>| -> Result<Cel, ExecutionError> {
            // Arguments not consumed by the target/`this` extraction.
            let rest = &ftx.args[ftx.arg_idx..];
            let (start, end) = match rest {
                [s] => (as_int("slice", &ftx.resolve(s.clone())?)?, None),
                [s, e] => (
                    as_int("slice", &ftx.resolve(s.clone())?)?,
                    Some(as_int("slice", &ftx.resolve(e.clone())?)?),
                ),
                other => {
                    return Err(func_err(
                        "slice",
                        format!(
                            "takes (value, start, end?), got {} extra arguments",
                            other.len()
                        ),
                    ))
                }
            };
            match &this {
                Cel::List(items) => {
                    let s = slice_bound(start, items.len());
                    let e = slice_bound(end.unwrap_or(items.len() as i64), items.len());
                    let out: Vec<Cel> = if s < e {
                        items[s..e].to_vec()
                    } else {
                        Vec::new()
                    };
                    Ok(Cel::List(Arc::new(out)))
                }
                Cel::String(text) => {
                    let chars: Vec<char> = text.chars().collect();
                    let s = slice_bound(start, chars.len());
                    let e = slice_bound(end.unwrap_or(chars.len() as i64), chars.len());
                    let out: String = if s < e {
                        chars[s..e].iter().collect()
                    } else {
                        String::new()
                    };
                    Ok(Cel::String(Arc::new(out)))
                }
                other => Err(func_err(
                    "slice",
                    format!("requires a list or a string, got {other:?}"),
                )),
            }
        },
    );
    ctx.add_function(
        "index_of",
        |This(this): This<Cel>, needle: Cel| -> Result<Cel, ExecutionError> {
            match &this {
                Cel::List(items) => Ok(Cel::Int(
                    items
                        .iter()
                        .position(|v| v == &needle)
                        .map(|i| i as i64)
                        .unwrap_or(-1),
                )),
                Cel::String(text) => {
                    let needle = as_string("index_of", &needle)?;
                    match text.find(needle.as_str()) {
                        // Report the position in characters, matching slice().
                        Some(byte) => Ok(Cel::Int(text[..byte].chars().count() as i64)),
                        None => Ok(Cel::Int(-1)),
                    }
                }
                other => Err(func_err(
                    "index_of",
                    format!("requires a list or a string, got {other:?}"),
                )),
            }
        },
    );

    // -- strings -----------------------------------------------------------------
    ctx.add_function("trim", |This(this): This<Arc<String>>| -> String {
        this.trim().to_string()
    });
    ctx.add_function("upper", |This(this): This<Arc<String>>| -> String {
        this.to_uppercase()
    });
    ctx.add_function("lower", |This(this): This<Arc<String>>| -> String {
        this.to_lowercase()
    });
    ctx.add_function(
        "replace",
        |This(this): This<Arc<String>>, from: Arc<String>, to: Arc<String>| -> String {
            this.replace(from.as_str(), to.as_str())
        },
    );
    ctx.add_function(
        "split",
        |This(this): This<Arc<String>>, sep: Arc<String>| -> Result<Cel, ExecutionError> {
            let parts: Vec<Cel> = if sep.is_empty() {
                this.chars()
                    .map(|c| Cel::String(Arc::new(c.to_string())))
                    .collect()
            } else {
                this.split(sep.as_str())
                    .map(|p| Cel::String(Arc::new(p.to_string())))
                    .collect()
            };
            Ok(Cel::List(Arc::new(parts)))
        },
    );
    // Snake-case aliases of the native predicates, so the documented stdlib
    // names always resolve.
    ctx.add_function("starts_with", cel_interpreter::functions::starts_with);
    ctx.add_function("ends_with", cel_interpreter::functions::ends_with);
    ctx.add_function(
        "find_all",
        |This(this): This<Arc<String>>, pattern: Arc<String>| -> Result<Cel, ExecutionError> {
            let re = regex::Regex::new(&pattern)
                .map_err(|e| func_err("find_all", format!("invalid pattern: {e}")))?;
            Ok(Cel::List(Arc::new(
                re.find_iter(&this)
                    .map(|m| Cel::String(Arc::new(m.as_str().to_string())))
                    .collect(),
            )))
        },
    );

    // -- JSON ----------------------------------------------------------------------
    ctx.add_function("to_json", |v: Cel| -> Result<Cel, ExecutionError> {
        let json = cel_to_json(&v).map_err(|error| func_err("to_json", error))?;
        let encoded = runtime_json::stringify(&json)
            .try_to_utf8()
            .map_err(|error| func_err("to_json", error))?;
        Ok(Cel::String(Arc::new(encoded)))
    });
    ctx.add_function(
        "from_json",
        |s: Arc<String>| -> Result<Cel, ExecutionError> {
            let parsed = runtime_json::parse(&RuntimeString::from(s.as_str()))
                .map_err(|e| func_err("from_json", format!("invalid JSON: {e}")))?;
            json_to_cel(&parsed).map_err(|error| func_err("from_json", error))
        },
    );
    ctx.add_function(
        "coalesce",
        |Arguments(args): Arguments| -> Result<Cel, ExecutionError> {
            Ok(args
                .iter()
                .find(|v| !matches!(v, Cel::Null))
                .cloned()
                .unwrap_or(Cel::Null))
        },
    );
}

fn extremum(name: &str, items: &[Cel], keep: std::cmp::Ordering) -> Result<Cel, ExecutionError> {
    let mut best: Option<&Cel> = None;
    for item in items {
        best = Some(match best {
            None => item,
            Some(cur) => {
                if compare_values(name, item, cur)? == keep {
                    item
                } else {
                    cur
                }
            }
        });
    }
    best.cloned()
        .ok_or_else(|| func_err(name, "requires at least one value (got an empty list)"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::flow::program::{validate_program, Step};

    macro_rules! exact_json {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    /// Compile a single expression by wrapping it in a one-step program.
    fn compile(expr: &str) -> CelExpr {
        let mut cel = RuntimeObject::new();
        cel.insert("$", RuntimeJson::String(RuntimeString::from(expr)));
        let mut return_step = RuntimeObject::new();
        return_step.insert("return", RuntimeJson::Object(cel));
        let mut meta = RuntimeObject::new();
        meta.insert("name", RuntimeJson::String(RuntimeString::from("t")));
        meta.insert("description", RuntimeJson::String(RuntimeString::from("t")));
        let mut source = RuntimeObject::new();
        source.insert("meta", RuntimeJson::Object(meta));
        source.insert(
            "body",
            RuntimeJson::Array(vec![RuntimeJson::Object(return_step)]),
        );
        let program = validate_program(&runtime_json::stringify(&RuntimeJson::Object(source)))
            .expect("compiles");
        match program.body.into_iter().next() {
            Some(Step::Return(VExpr::Cel(e))) => e,
            _ => panic!("expected a compiled expression"),
        }
    }

    fn eval(expr: &str) -> Result<Json, FlowError> {
        eval_with(expr, &HashMap::new())
    }

    fn eval_with(expr: &str, vars: &HashMap<String, Json>) -> Result<Json, FlowError> {
        let root = build_root_context(
            &exact_json!({ "n": 3, "xs": [3, 1, 2] }),
            &BudgetSource::Unlimited,
        )?;
        eval_cel(&compile(expr), &root, vars)
    }

    #[test]
    fn native_operators_and_macros_are_available() {
        assert_eq!(eval("1 + 1").unwrap(), exact_json!(2));
        assert_eq!(eval("'a' + 'b'").unwrap(), exact_json!("ab"));
        assert_eq!(eval("[1] + [2]").unwrap(), exact_json!([1, 2]));
        assert_eq!(eval("1 > 0 ? 'y' : 'n'").unwrap(), exact_json!("y"));
        assert_eq!(eval("2 in [1, 2]").unwrap(), exact_json!(true));
        assert_eq!(
            eval("[1,2,3].map(x, x * 2)").unwrap(),
            exact_json!([2, 4, 6])
        );
        assert_eq!(
            eval("[1,2,3].filter(x, x > 1)").unwrap(),
            exact_json!([2, 3])
        );
        assert_eq!(
            eval("[1,2,3].exists(x, x == 2)").unwrap(),
            exact_json!(true)
        );
        assert_eq!(eval("[1,2,3].all(x, x > 0)").unwrap(), exact_json!(true));
        assert_eq!(eval("has(args.n)").unwrap(), exact_json!(true));
        assert_eq!(eval("has(args.missing)").unwrap(), exact_json!(false));
        assert_eq!(eval("size('abc')").unwrap(), exact_json!(3));
        assert_eq!(eval("'abc'.contains('b')").unwrap(), exact_json!(true));
        assert_eq!(eval("'a1'.matches('[0-9]')").unwrap(), exact_json!(true));
        assert_eq!(eval("false && (1 / 0 > 0)").unwrap(), exact_json!(false));
    }

    #[test]
    fn args_integers_mix_with_literal_integers() {
        // JSON integers must convert to Int so `args.n + 1` works.
        assert_eq!(eval("args.n + 1").unwrap(), exact_json!(4));
        assert_eq!(eval("args.xs[0] * 2").unwrap(), exact_json!(6));
    }

    #[test]
    fn evaluation_errors_become_flow_errors() {
        let e = eval("1 / 0").unwrap_err();
        assert!(e.message.starts_with("expression `1 / 0` failed:"), "{e}");
        let e = eval("nope + 1").unwrap_err();
        assert!(e.message.contains("Undeclared reference to 'nope'"), "{e}");
        // No clock and no randomness.
        assert!(eval("now()").is_err());
        assert!(eval("random()").is_err());
    }

    #[test]
    fn map_helpers() {
        assert_eq!(
            eval("keys({'b': 2, 'a': 1})").unwrap(),
            exact_json!(["a", "b"])
        );
        assert_eq!(
            eval("values({'b': 2, 'a': 1})").unwrap(),
            exact_json!([1, 2])
        );
        assert_eq!(eval("has_key({'a': 1}, 'a')").unwrap(), exact_json!(true));
        assert_eq!(eval("has_key({'a': 1}, 'b')").unwrap(), exact_json!(false));
        assert!(eval("keys([1])")
            .unwrap_err()
            .message
            .contains("requires a map"));
    }

    #[test]
    fn list_helpers() {
        assert_eq!(
            eval("flatten([[1, 2], [3], 4])").unwrap(),
            exact_json!([1, 2, 3, 4])
        );
        assert_eq!(
            eval("unique([1, 2, 1, 3, 2])").unwrap(),
            exact_json!([1, 2, 3])
        );
        assert_eq!(eval("sort([3, 1, 2])").unwrap(), exact_json!([1, 2, 3]));
        assert_eq!(eval("sort(['b', 'a'])").unwrap(), exact_json!(["a", "b"]));
        assert_eq!(eval("sort([2, 1.5])").unwrap(), exact_json!([1.5, 2]));
        assert!(eval("sort([1, 'a'])")
            .unwrap_err()
            .message
            .contains("cannot compare"));
        assert_eq!(eval("reverse([1, 2, 3])").unwrap(), exact_json!([3, 2, 1]));
        assert_eq!(eval("reverse('abc')").unwrap(), exact_json!("cba"));
        assert_eq!(eval("join(['a', 'b'], '-')").unwrap(), exact_json!("a-b"));
        assert_eq!(eval("join(['a', 'b'])").unwrap(), exact_json!("ab"));
        assert_eq!(
            eval("join([1, null, {'k': 1}], '|')").unwrap(),
            exact_json!("1|null|{\"k\":1}")
        );
        assert_eq!(eval("sum([1, 2, 3])").unwrap(), exact_json!(6));
        assert_eq!(eval("sum([1, 2.5])").unwrap(), exact_json!(3.5));
        assert_eq!(eval("sum([])").unwrap(), exact_json!(0));
        assert!(eval("sum(['a'])").is_err());
        assert_eq!(eval("min([3, 1, 2])").unwrap(), exact_json!(1));
        assert_eq!(eval("max([3, 1, 2])").unwrap(), exact_json!(3));
        assert_eq!(eval("min(3, 1, 2)").unwrap(), exact_json!(1));
        assert_eq!(eval("max('a', 'c', 'b')").unwrap(), exact_json!("c"));
        assert!(eval("min([])").unwrap_err().message.contains("empty list"));
        assert_eq!(eval("range(3)").unwrap(), exact_json!([0, 1, 2]));
        assert_eq!(eval("range(-1)").unwrap(), exact_json!([]));
        assert!(eval("range(10000000)").unwrap_err().message.contains("cap"));
        assert_eq!(
            eval("slice([1, 2, 3, 4], 1, 3)").unwrap(),
            exact_json!([2, 3])
        );
        assert_eq!(
            eval("slice([1, 2, 3, 4], -2)").unwrap(),
            exact_json!([3, 4])
        );
        assert_eq!(eval("slice('hello', 1, 3)").unwrap(), exact_json!("el"));
        assert_eq!(eval("slice([1, 2], 5)").unwrap(), exact_json!([]));
        assert_eq!(eval("index_of([1, 2, 3], 2)").unwrap(), exact_json!(1));
        assert_eq!(eval("index_of([1], 9)").unwrap(), exact_json!(-1));
        assert_eq!(eval("index_of('hello', 'll')").unwrap(), exact_json!(2));
        assert_eq!(eval("index_of('hello', 'x')").unwrap(), exact_json!(-1));
    }

    #[test]
    fn string_helpers() {
        assert_eq!(eval("trim('  a  ')").unwrap(), exact_json!("a"));
        assert_eq!(eval("upper('ab')").unwrap(), exact_json!("AB"));
        assert_eq!(eval("lower('AB')").unwrap(), exact_json!("ab"));
        assert_eq!(
            eval("replace('a-b-c', '-', '+')").unwrap(),
            exact_json!("a+b+c")
        );
        assert_eq!(
            eval("split('a,b,c', ',')").unwrap(),
            exact_json!(["a", "b", "c"])
        );
        assert_eq!(eval("split('ab', '')").unwrap(), exact_json!(["a", "b"]));
        assert_eq!(eval("starts_with('abc', 'ab')").unwrap(), exact_json!(true));
        assert_eq!(eval("ends_with('abc', 'bc')").unwrap(), exact_json!(true));
        assert_eq!(eval("'abc'.starts_with('ab')").unwrap(), exact_json!(true));
        assert_eq!(
            eval("find_all('a1 b22 c3', '[0-9]+')").unwrap(),
            exact_json!(["1", "22", "3"])
        );
        assert!(eval("find_all('x', '(')")
            .unwrap_err()
            .message
            .contains("invalid pattern"));
    }

    #[test]
    fn json_helpers() {
        assert_eq!(
            eval("to_json({'a': [1, true]})").unwrap(),
            exact_json!("{\"a\":[1,true]}")
        );
        assert_eq!(eval("from_json('{\"a\": 1}').a").unwrap(), exact_json!(1));
        assert!(eval("from_json('nope')")
            .unwrap_err()
            .message
            .contains("invalid JSON"));
        assert_eq!(eval("coalesce(null, null, 3, 4)").unwrap(), exact_json!(3));
        assert_eq!(eval("coalesce(null, null)").unwrap(), exact_json!(null));

        let surrogate = runtime_json::parse(&RuntimeString::from(r#""\ud800""#)).unwrap();
        let mut vars = HashMap::new();
        vars.insert("surrogate".to_string(), surrogate.clone());
        vars.insert(
            "encoded".to_string(),
            RuntimeJson::String(RuntimeString::from(r#""\ud800""#)),
        );
        assert!(eval_with("to_json(surrogate)", &vars)
            .unwrap_err()
            .message
            .contains("CEL string boundary rejected unpaired UTF-16 surrogate"));
        assert!(eval_with("from_json(encoded)", &vars)
            .unwrap_err()
            .message
            .contains("CEL string boundary rejected unpaired UTF-16 surrogate"));
    }

    #[test]
    fn budget_defaults_without_a_workflow() {
        assert_eq!(eval("budget_total").unwrap(), exact_json!(null));
        assert_eq!(eval("budget_spent()").unwrap(), exact_json!(0));
        assert_eq!(
            eval("budget_remaining() > 50000.0").unwrap(),
            exact_json!(true)
        );
        // The canonical guard short-circuits when there is no budget target.
        assert_eq!(
            eval("budget_total != null && budget_remaining() > 50000").unwrap(),
            exact_json!(false)
        );
    }

    #[test]
    fn scope_variables_shadow_globals_snapshot() {
        let mut vars = HashMap::new();
        vars.insert("x".to_string(), exact_json!([10, 20]));
        assert_eq!(eval_with("x[1] + 1", &vars).unwrap(), exact_json!(21));
        assert_eq!(eval_with("size(x)", &vars).unwrap(), exact_json!(2));
    }

    #[test]
    fn vexpr_literals_escapes_and_interpolation() {
        let root = build_root_context(&exact_json!(null), &BudgetSource::Unlimited)
            .expect("root context builds");
        let vars = HashMap::from([("n".to_string(), exact_json!(7))]);
        let program = validate_program(&runtime_json::stringify(&exact_json!({
            "meta": { "name": "t", "description": "t" },
            "body": [ { "return": {
                "literal": [1, "plain", { "$": "n * 2" }],
                "$$": "kept ${n}",
                "nested": { "deep": { "$": "[n]" } },
                "text": "n=${n}, list=${[1, n]}, obj=${{'k': n}}, esc=$${n}"
            } } ]
        })))
        .expect("compiles");
        let expr = match &program.body[0] {
            Step::Return(v) => v,
            _ => panic!("expected return"),
        };
        let out = eval_vexpr(expr, &root, &vars).unwrap();
        assert_eq!(
            out,
            exact_json!({
                "literal": [1, "plain", 14],
                "$": "kept 7",
                "nested": { "deep": [7] },
                "text": "n=7, list=[1,7], obj={\"k\":7}, esc=${n}"
            })
        );
    }

    #[test]
    fn flow_string_coercions() {
        assert_eq!(flow_string(&exact_json!(null)), "null");
        assert_eq!(flow_string(&exact_json!(true)), "true");
        assert_eq!(flow_string(&exact_json!(3.5)), "3.5");
        assert_eq!(flow_string(&exact_json!("s")), "s");
        assert_eq!(flow_string(&exact_json!([1, "a"])), "[1,\"a\"]");
        assert_eq!(flow_string(&exact_json!({ "k": 1 })), "{\"k\":1}");
    }

    #[test]
    fn truthiness_rules() {
        for v in [
            exact_json!(null),
            exact_json!(false),
            exact_json!(0),
            exact_json!(0.0),
            exact_json!(""),
        ] {
            assert!(!truthy(&v), "{v}");
        }
        for v in [
            exact_json!(true),
            exact_json!(1),
            exact_json!(-0.5),
            exact_json!("x"),
            exact_json!([]),
            exact_json!({}),
        ] {
            assert!(truthy(&v), "{v}");
        }
    }

    #[test]
    fn json_conversion_round_trips() {
        let j = exact_json!({ "a": [1, 2.5, "x", true, null], "b": { "c": -3 } });
        let cel = json_to_cel(&j).expect("exact JSON converts to CEL");
        assert_eq!(cel_to_json(&cel).unwrap(), j);
        // Non-finite floats have no JSON form and become null.
        assert_eq!(
            cel_to_json(&Cel::Float(f64::INFINITY)).unwrap(),
            exact_json!(null)
        );
    }

    #[test]
    fn root_context_is_send_and_sync() {
        fn assert_ss<T: Send + Sync>(_: &T) {}
        let root = build_root_context(&exact_json!(null), &BudgetSource::Unlimited)
            .expect("root context builds");
        assert_ss(&root);
    }
}
