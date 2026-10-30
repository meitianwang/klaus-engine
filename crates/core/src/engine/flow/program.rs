//! Flow-program parsing and validation: the serde-level types a program
//! deserializes into, the compile step that turns raw JSON into an executable
//! [`FlowProgram`] (value expressions pre-compiled, steps tagged), and the
//! machine-readable [`FLOW_PROGRAM_SCHEMA`].
//!
//! Everything here is PURE — no effects, no interpretation. Validation errors
//! are model-facing: each one carries the JSON path of the offending node and a
//! precise, actionable description.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::runtime_values::{
    runtime_json, RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString,
};
use crate::types::json::Json;
use cel_interpreter::Program as CompiledCel;

/// An error raised while parsing, validating, or running a flow program.
/// Carries only the human-facing scalar diagnostic message. Exact runtime
/// strings embedded in diagnostics use [`runtime_diagnostic`], so this host
/// error boundary never applies replacement or a lossy `Display` shim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowError {
    pub message: RuntimeString,
}

impl FlowError {
    pub fn new(message: impl Into<RuntimeString>) -> Self {
        FlowError {
            message: message.into(),
        }
    }

    /// An error located at a JSON path within the program document.
    fn at(path: &str, detail: impl fmt::Display) -> Self {
        FlowError {
            message: format!("{path}: {detail}").into(),
        }
    }
}

impl fmt::Display for FlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let projected = self.message.to_node_utf8();
        f.write_str(std::str::from_utf8(&projected).expect("Node UTF-8 projection is valid"))
    }
}

impl std::error::Error for FlowError {}

/// Injective ASCII rendering used only for scalar diagnostics. Runtime values
/// themselves remain exact UTF-16; unpaired surrogates are never replaced.
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

fn runtime_eq_utf8(value: &RuntimeString, expected: &str) -> bool {
    value.as_utf16().iter().copied().eq(expected.encode_utf16())
}

fn parse_runtime_document(source: &RuntimeString) -> Result<RuntimeJson, FlowError> {
    runtime_json::parse(source)
        .map_err(|error| FlowError::new(format!("flow program is not valid JSON: {error}")))
}

/// Render one exact runtime JSON value without projecting its UTF-16 identity.
fn runtime_json_source(value: &RuntimeJson) -> RuntimeString {
    runtime_json::stringify(value)
}

/// One phase declared in a flow program's `meta.phases`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowPhase {
    pub title: RuntimeString,
    pub detail: Option<RuntimeString>,
    pub model: Option<RuntimeString>,
}

/// The static `meta` header of a flow program: pure literal JSON (no value
/// expressions), extractable without executing anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowMeta {
    pub name: RuntimeString,
    pub description: RuntimeString,
    pub when_to_use: Option<RuntimeString>,
    pub phases: Option<Vec<FlowPhase>>,
}

/// A compiled expression: the original source (for error messages) plus its
/// compiled form.
pub(crate) struct CelExpr {
    pub(crate) source: RuntimeString,
    pub(crate) program: CompiledCel,
}

/// One segment of an interpolated string.
pub(crate) enum InterpPart {
    Lit(RuntimeString),
    Expr(CelExpr),
}

/// A compiled value expression (VEXPR): a JSON literal evaluated deeply, with
/// `{"$": "expr"}` escapes and `${expr}` string interpolation resolved to
/// compiled expressions.
pub(crate) enum VExpr {
    Null,
    Bool(bool),
    Number(RuntimeNumber),
    /// A string with no interpolation.
    LitString(RuntimeString),
    /// A string containing one or more `${...}` segments.
    Interp(Vec<InterpPart>),
    /// A `{"$": "<expression>"}` escape.
    Cel(CelExpr),
    Array(Vec<VExpr>),
    /// Keys are already unescaped (`"$$"` written out as `"$"`).
    Object(Vec<(RuntimeString, VExpr)>),
}

/// One `parallel` branch: run `do`, then evaluate `value` (default null).
pub(crate) struct Branch {
    pub(crate) body: Vec<Step>,
    pub(crate) value: Option<VExpr>,
}

/// One `map`-pipeline stage: run `do`, then evaluate `value` (default null).
pub(crate) struct StageDef {
    pub(crate) body: Vec<Step>,
    pub(crate) value: Option<VExpr>,
}

/// The `map` step's execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MapMode {
    Seq,
    Parallel,
    Pipeline,
}

/// The `map` step.
pub(crate) struct MapStep {
    pub(crate) var: String,
    pub(crate) index: Option<String>,
    pub(crate) list: VExpr,
    pub(crate) mode: MapMode,
    pub(crate) body: Vec<Step>,
    pub(crate) value: Option<VExpr>,
    pub(crate) stages: Vec<StageDef>,
    pub(crate) bind: Option<String>,
}

/// The `agent` step.
pub(crate) struct AgentStep {
    pub(crate) prompt: VExpr,
    pub(crate) label: Option<VExpr>,
    pub(crate) phase: Option<VExpr>,
    /// A literal JSON Schema object (NOT a value expression).
    pub(crate) schema: Option<RuntimeObject>,
    pub(crate) model: Option<VExpr>,
    pub(crate) isolation: Option<VExpr>,
    pub(crate) agent_type: Option<VExpr>,
    pub(crate) bind: Option<String>,
}

/// One executable step.
pub(crate) enum Step {
    Set {
        var: String,
        value: VExpr,
    },
    If {
        cond: VExpr,
        then: Vec<Step>,
        els: Option<Vec<Step>>,
    },
    Switch {
        cases: Vec<(VExpr, Vec<Step>)>,
        default: Option<Vec<Step>>,
    },
    While {
        cond: VExpr,
        body: Vec<Step>,
    },
    Break,
    Continue,
    For {
        var: String,
        index: Option<String>,
        list: VExpr,
        body: Vec<Step>,
    },
    Parallel {
        branches: Vec<Branch>,
        bind: Option<String>,
    },
    Map(MapStep),
    Agent(Box<AgentStep>),
    Workflow {
        target: VExpr,
        args: Option<VExpr>,
        bind: Option<String>,
    },
    Phase(VExpr),
    Log(VExpr),
    Try {
        body: Vec<Step>,
        catch: Option<(Option<String>, Vec<Step>)>,
        finally: Option<Vec<Step>>,
    },
    Throw(VExpr),
    Return(VExpr),
    Call {
        name: String,
        args: Vec<VExpr>,
        bind: Option<String>,
    },
}

/// One user-defined function.
pub(crate) struct FlowFunc {
    pub(crate) params: Vec<String>,
    pub(crate) body: Vec<Step>,
}

/// A validated, compiled flow program, ready to interpret.
///
/// The compiled step/expression internals carry no useful `Debug` shape, so
/// only the meta is shown.
pub struct FlowProgram {
    /// The validated meta header.
    pub meta: FlowMeta,
    /// The meta exactly as written (pure JSON), for embedding in a composed
    /// workflow script header.
    pub raw_meta: Json,
    pub(crate) vars: Vec<(String, VExpr)>,
    pub(crate) funcs: HashMap<String, FlowFunc>,
    pub(crate) body: Vec<Step>,
}

impl fmt::Debug for FlowProgram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlowProgram")
            .field("meta", &self.meta)
            .finish_non_exhaustive()
    }
}

// ---- format constants ---------------------------------------------------------

/// Step tags that carry a host effect (agents, nested workflows, progress
/// statuses).
const WORKFLOW_ONLY_TAGS: [&str; 4] = ["agent", "workflow", "phase", "log"];

/// Step tags shared by every construct of the format.
const SHARED_TAGS: [&str; 13] = [
    "set", "if", "switch", "while", "break", "continue", "for", "parallel", "map", "try", "throw",
    "return", "call",
];

/// Names a program may not bind: the fixed globals plus the expression
/// language's reserved words.
const RESERVED_NAMES: [&str; 25] = [
    "args",
    "budget_total",
    "true",
    "false",
    "null",
    "in",
    "as",
    "break",
    "const",
    "continue",
    "else",
    "for",
    "function",
    "if",
    "import",
    "let",
    "loop",
    "package",
    "namespace",
    "return",
    "var",
    "void",
    "while",
    "size",
    "has",
];

/// The JSON Schema (draft 2020-12 subset) of the flow-program format. This is
/// engine-authored, immutable scalar configuration text; it is not a runtime
/// payload carrier. Step tags restricted to one context are noted in their
/// descriptions; `validate_program` enforces the restriction.
pub const FLOW_PROGRAM_SCHEMA: &str = r##"{
  "$id": "flow-program",
  "title": "Flow program",
  "description": "A JSON orchestration program: a literal meta header, optional vars and funcs, and a body of steps. Values are value expressions (VEXPR): JSON literals evaluated deeply, where {\"$\": \"<expr>\"} evaluates an expression and \"${...}\" interpolates expressions into strings (escape a literal \"${\" as \"$${\"; write a literal \"$\" object key as \"$$\").",
  "type": "object",
  "required": ["meta", "body"],
  "additionalProperties": false,
  "properties": {
    "meta": {
      "type": "object",
      "required": ["name", "description"],
      "additionalProperties": false,
      "properties": {
        "name": { "type": "string", "minLength": 1 },
        "description": { "type": "string", "minLength": 1 },
        "whenToUse": { "type": "string" },
        "phases": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["title"],
            "additionalProperties": false,
            "properties": {
              "title": { "type": "string" },
              "detail": { "type": "string" },
              "model": { "type": "string" }
            }
          }
        }
      }
    },
    "vars": {
      "type": "array",
      "description": "Initial variables, evaluated in order (later entries may reference earlier ones).",
      "items": {
        "type": "object",
        "required": ["var", "value"],
        "additionalProperties": false,
        "properties": {
          "var": { "$ref": "#/$defs/ident" },
          "value": { "$ref": "#/$defs/vexpr" }
        }
      }
    },
    "funcs": {
      "type": "object",
      "description": "User functions callable via the call step; recursion is allowed.",
      "additionalProperties": {
        "type": "object",
        "required": ["params", "body"],
        "additionalProperties": false,
        "properties": {
          "params": { "type": "array", "items": { "$ref": "#/$defs/ident" } },
          "body": { "$ref": "#/$defs/steps" }
        }
      }
    },
    "body": { "$ref": "#/$defs/steps" }
  },
  "$defs": {
    "ident": { "type": "string", "pattern": "^[A-Za-z_][A-Za-z0-9_]*$" },
    "vexpr": { "description": "Any JSON value, evaluated deeply. {\"$\": \"<expr>\"} evaluates an expression; strings interpolate ${...} segments." },
    "steps": { "type": "array", "items": { "$ref": "#/$defs/step" } },
    "branch": {
      "type": "object",
      "required": ["do"],
      "additionalProperties": false,
      "properties": {
        "do": { "$ref": "#/$defs/steps" },
        "value": { "$ref": "#/$defs/vexpr" }
      }
    },
    "stage": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "do": { "$ref": "#/$defs/steps" },
        "value": { "$ref": "#/$defs/vexpr" }
      }
    },
    "step": {
      "type": "object",
      "description": "Exactly one step tag per object, plus that tag's companion keys.",
      "properties": {
        "set": {
          "type": "object",
          "required": ["var", "value"],
          "additionalProperties": false,
          "properties": {
            "var": { "$ref": "#/$defs/ident" },
            "value": { "$ref": "#/$defs/vexpr" }
          }
        },
        "if": { "$ref": "#/$defs/vexpr" },
        "then": { "$ref": "#/$defs/steps" },
        "else": { "$ref": "#/$defs/steps" },
        "switch": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["case", "do"],
            "additionalProperties": false,
            "properties": {
              "case": { "$ref": "#/$defs/vexpr" },
              "do": { "$ref": "#/$defs/steps" }
            }
          }
        },
        "default": { "$ref": "#/$defs/steps" },
        "while": { "$ref": "#/$defs/vexpr" },
        "do": { "$ref": "#/$defs/steps" },
        "break": { "description": "Exit the innermost loop. The tag value is ignored; write null." },
        "continue": { "description": "Skip to the next loop iteration. The tag value is ignored; write null." },
        "for": {
          "type": "object",
          "required": ["var", "in", "do"],
          "additionalProperties": false,
          "properties": {
            "var": { "$ref": "#/$defs/ident" },
            "index": { "$ref": "#/$defs/ident" },
            "in": { "$ref": "#/$defs/vexpr" },
            "do": { "$ref": "#/$defs/steps" }
          }
        },
        "parallel": {
          "type": "object",
          "required": ["branches"],
          "additionalProperties": false,
          "properties": {
            "branches": { "type": "array", "items": { "$ref": "#/$defs/branch" } },
            "as": { "$ref": "#/$defs/ident" }
          }
        },
        "map": {
          "type": "object",
          "required": ["var", "in", "mode"],
          "additionalProperties": false,
          "properties": {
            "var": { "$ref": "#/$defs/ident" },
            "index": { "$ref": "#/$defs/ident" },
            "in": { "$ref": "#/$defs/vexpr" },
            "mode": { "enum": ["seq", "parallel", "pipeline"] },
            "do": { "$ref": "#/$defs/steps" },
            "value": { "$ref": "#/$defs/vexpr" },
            "stages": { "type": "array", "items": { "$ref": "#/$defs/stage" }, "minItems": 1 },
            "as": { "$ref": "#/$defs/ident" }
          }
        },
        "agent": {
          "description": "Spawn one sub-agent and bind its result via as.",
          "type": "object",
          "required": ["prompt"],
          "additionalProperties": false,
          "properties": {
            "prompt": { "$ref": "#/$defs/vexpr" },
            "label": { "$ref": "#/$defs/vexpr" },
            "phase": { "$ref": "#/$defs/vexpr" },
            "schema": { "type": "object", "description": "A literal JSON Schema object (not a value expression)." },
            "model": { "$ref": "#/$defs/vexpr" },
            "isolation": { "$ref": "#/$defs/vexpr" },
            "agentType": { "$ref": "#/$defs/vexpr" }
          }
        },
        "workflow": {
          "description": "Run a registered workflow inline (one nesting level).",
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "name": { "$ref": "#/$defs/vexpr" },
            "scriptPath": { "$ref": "#/$defs/vexpr" },
            "args": { "$ref": "#/$defs/vexpr" }
          }
        },
        "phase": { "description": "Emit a phase-progress status.", "$ref": "#/$defs/vexpr" },
        "log": { "description": "Emit a log status.", "$ref": "#/$defs/vexpr" },
        "try": { "$ref": "#/$defs/steps" },
        "catch": {
          "type": "object",
          "required": ["do"],
          "additionalProperties": false,
          "properties": {
            "var": { "$ref": "#/$defs/ident" },
            "do": { "$ref": "#/$defs/steps" }
          }
        },
        "finally": { "$ref": "#/$defs/steps" },
        "throw": { "$ref": "#/$defs/vexpr" },
        "return": { "$ref": "#/$defs/vexpr" },
        "call": {
          "type": "object",
          "required": ["fn"],
          "additionalProperties": false,
          "properties": {
            "fn": { "$ref": "#/$defs/ident" },
            "args": { "type": "array", "items": { "$ref": "#/$defs/vexpr" } }
          }
        },
        "as": { "$ref": "#/$defs/ident", "description": "Bind the step's result to a variable (declare-or-update)." }
      }
    }
  }
}"##;

// ---- meta ---------------------------------------------------------------------

pub(crate) fn value_type(v: &RuntimeJson) -> &'static str {
    match v {
        RuntimeJson::Null => "null",
        RuntimeJson::Bool(_) => "boolean",
        RuntimeJson::Number(_) => "number",
        RuntimeJson::String(_) => "string",
        RuntimeJson::Array(_) => "array",
        RuntimeJson::Object(_) => "object",
    }
}

fn require_object<'v>(v: &'v Json, path: &str) -> Result<&'v RuntimeObject, FlowError> {
    v.as_object()
        .ok_or_else(|| FlowError::at(path, format!("expected an object, got {}", value_type(v))))
}

fn require_string(v: &RuntimeJson, path: &str) -> Result<RuntimeString, FlowError> {
    match v {
        RuntimeJson::String(s) => Ok(s.clone()),
        other => Err(FlowError::at(
            path,
            format!("expected a string, got {}", value_type(other)),
        )),
    }
}

fn require_nonempty_string(v: &RuntimeJson, path: &str) -> Result<RuntimeString, FlowError> {
    let s = require_string(v, path)?;
    if s.is_empty() {
        return Err(FlowError::at(path, "must not be empty"));
    }
    Ok(s)
}

fn reject_unknown_keys(obj: &RuntimeObject, allowed: &[&str], path: &str) -> Result<(), FlowError> {
    for key in obj.keys() {
        if !allowed.iter().any(|allowed| runtime_eq_utf8(&key, allowed)) {
            return Err(FlowError::at(
                path,
                format!(
                    "unknown key \"{}\" (allowed keys: {})",
                    runtime_diagnostic(&key),
                    allowed.join(", ")
                ),
            ));
        }
    }
    Ok(())
}

/// Validate the literal `meta` header. Every field must be a plain literal
/// (value expressions are not evaluated inside `meta`).
fn validate_meta(raw: &Json, path: &str) -> Result<FlowMeta, FlowError> {
    let obj = require_object(raw, path)?;
    reject_unknown_keys(obj, &["name", "description", "whenToUse", "phases"], path)?;
    let name = match obj.get("name") {
        Some(v) => require_nonempty_string(v, &format!("{path}.name"))?,
        None => return Err(FlowError::at(path, "missing required key \"name\"")),
    };
    let description = match obj.get("description") {
        Some(v) => require_nonempty_string(v, &format!("{path}.description"))?,
        None => return Err(FlowError::at(path, "missing required key \"description\"")),
    };
    let when_to_use = match obj.get("whenToUse") {
        Some(v) => Some(require_string(v, &format!("{path}.whenToUse"))?),
        None => None,
    };
    let phases = match obj.get("phases") {
        None => None,
        Some(RuntimeJson::Array(items)) => {
            let mut out = Vec::new();
            for (i, item) in items.iter().enumerate() {
                let ppath = format!("{path}.phases[{i}]");
                let pobj = require_object(item, &ppath)?;
                reject_unknown_keys(pobj, &["title", "detail", "model"], &ppath)?;
                let title = match pobj.get("title") {
                    Some(v) => require_string(v, &format!("{ppath}.title"))?,
                    None => return Err(FlowError::at(&ppath, "missing required key \"title\"")),
                };
                let detail = match pobj.get("detail") {
                    Some(v) => Some(require_string(v, &format!("{ppath}.detail"))?),
                    None => None,
                };
                let model = match pobj.get("model") {
                    Some(v) => Some(require_string(v, &format!("{ppath}.model"))?),
                    None => None,
                };
                out.push(FlowPhase {
                    title,
                    detail,
                    model,
                });
            }
            Some(out)
        }
        Some(other) => {
            return Err(FlowError::at(
                &format!("{path}.phases"),
                format!("expected an array, got {}", value_type(other)),
            ))
        }
    };
    Ok(FlowMeta {
        name,
        description,
        when_to_use,
        phases,
    })
}

/// Parse only the `meta` header of a flow program — a pure JSON parse, no
/// compilation and no execution. For discovery/permission surfaces that need
/// the name/description without validating the whole body.
pub fn parse_flow_meta(source: &RuntimeString) -> Result<FlowMeta, FlowError> {
    let doc = parse_runtime_document(source)?;
    let obj = require_object(&doc, "program")?;
    let raw = obj
        .get("meta")
        .ok_or_else(|| FlowError::new("program: missing required key \"meta\""))?;
    validate_meta(raw, "meta")
}

/// Whether a script source looks like a flow program (a JSON document) rather
/// than a classic workflow script: flow programs are JSON objects, so the first
/// non-whitespace byte is `{`.
pub fn is_flow_source(source: &RuntimeString) -> bool {
    source
        .as_utf16()
        .iter()
        .copied()
        .find(|unit| !is_ecmascript_whitespace(*unit))
        == Some(u16::from(b'{'))
}

fn is_ecmascript_whitespace(unit: u16) -> bool {
    matches!(
        unit,
        0x0009 | 0x000a | 0x000b | 0x000c | 0x000d | 0x0020 | 0x00a0 | 0x1680 | 0x2000
            ..=0x200a | 0x2028 | 0x2029 | 0x202f | 0x205f | 0x3000 | 0xfeff
    )
}

/// Compose a flow-program source into the workflow-script form the workflow
/// runtime parses: a leading `export const meta = <meta-json>` header (a single
/// line — the meta is re-serialized compactly) followed by the unmodified
/// program JSON. Only the meta is validated here; the body is validated by the
/// script runner with the same precise diagnostics.
pub fn compose_workflow_script(source: &RuntimeString) -> Result<RuntimeString, FlowError> {
    let doc = parse_runtime_document(source)?;
    let obj = require_object(&doc, "program")?;
    let raw = obj
        .get("meta")
        .ok_or_else(|| FlowError::new("program: missing required key \"meta\""))?;
    validate_meta(raw, "meta")?;
    let header = runtime_json_source(raw);
    Ok(RuntimeString::from("export const meta = ")
        .concat(&header)
        .concat(&RuntimeString::from("\n"))
        .concat(source))
}

// ---- value expressions ----------------------------------------------------------

fn compile_cel(src: &RuntimeString, path: &str) -> Result<CelExpr, FlowError> {
    let scalar = src.try_to_utf8().map_err(|error| {
        FlowError::at(
            path,
            format!(
                "expression `{}` contains unpaired surrogate U+{:04X} at UTF-16 unit {}",
                runtime_diagnostic(src),
                error.code_unit(),
                error.index()
            ),
        )
    })?;
    // The expression parser can panic (not error) on some malformed input, so
    // compilation is unwind-guarded to keep every syntax problem a plain
    // validation error.
    let compiled = std::panic::catch_unwind(|| CompiledCel::compile(&scalar));
    match compiled {
        Ok(Ok(program)) => Ok(CelExpr {
            source: src.clone(),
            program,
        }),
        Ok(Err(e)) => Err(FlowError::at(
            path,
            format!("invalid expression `{}`: {e}", runtime_diagnostic(src)),
        )),
        Err(_) => Err(FlowError::at(
            path,
            format!(
                "invalid expression `{}`: unparseable expression syntax",
                runtime_diagnostic(src)
            ),
        )),
    }
}

/// Scan an interpolated string. `"$${"` escapes a literal `"${"`; `"${...}"`
/// wraps an expression (brace-matched, aware of quoted strings inside the
/// expression).
fn compile_template(s: &RuntimeString, path: &str) -> Result<VExpr, FlowError> {
    if !s.contains("${") {
        return Ok(VExpr::LitString(s.clone()));
    }
    let units = s.as_utf16();
    let mut parts: Vec<InterpPart> = Vec::new();
    let mut lit = Vec::<u16>::new();
    let mut i = 0;
    while i < units.len() {
        if units[i..].starts_with(&[u16::from(b'$'), u16::from(b'$'), u16::from(b'{')]) {
            lit.extend([u16::from(b'$'), u16::from(b'{')]);
            i += 3;
        } else if units[i..].starts_with(&[u16::from(b'$'), u16::from(b'{')]) {
            let start = i + 2;
            let end = match expr_end(s, start) {
                Some(end) => end,
                None => return Err(FlowError::at(
                    path,
                    "unterminated \"${\" expression in string (escape a literal \"${\" as \"$${\")",
                )),
            };
            if !lit.is_empty() {
                parts.push(InterpPart::Lit(RuntimeString::from_utf16(std::mem::take(
                    &mut lit,
                ))));
            }
            let src = s.slice_utf16(start..end);
            parts.push(InterpPart::Expr(compile_cel(&src, path)?));
            i = end + 1;
        } else {
            lit.push(units[i]);
            i += 1;
        }
    }
    if !lit.is_empty() {
        parts.push(InterpPart::Lit(RuntimeString::from_utf16(lit)));
    }
    Ok(VExpr::Interp(parts))
}

/// Find the index of the `}` that closes an interpolation opened just before
/// `start`, respecting nested braces and single/double-quoted strings inside
/// the expression.
fn expr_end(s: &RuntimeString, start: usize) -> Option<usize> {
    let units = s.as_utf16();
    let mut depth = 1i64;
    let mut i = start;
    let mut quote: Option<u16> = None;
    while i < units.len() {
        let c = units[i];
        match quote {
            Some(q) => {
                if c == u16::from(b'\\') {
                    i += 1;
                } else if c == q {
                    quote = None;
                }
            }
            None => match c {
                unit if unit == u16::from(b'\'') || unit == u16::from(b'"') => quote = Some(c),
                unit if unit == u16::from(b'{') => depth += 1,
                unit if unit == u16::from(b'}') => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            },
        }
        i += 1;
    }
    None
}

fn compile_vexpr(value: &Json, path: &str) -> Result<VExpr, FlowError> {
    match value {
        RuntimeJson::Null => Ok(VExpr::Null),
        RuntimeJson::Bool(b) => Ok(VExpr::Bool(*b)),
        RuntimeJson::Number(n) => Ok(VExpr::Number(*n)),
        RuntimeJson::String(s) => compile_template(s, path),
        RuntimeJson::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                out.push(compile_vexpr(item, &format!("{path}[{i}]"))?);
            }
            Ok(VExpr::Array(out))
        }
        RuntimeJson::Object(obj) => {
            if let Some(RuntimeJson::String(src)) = obj.get("$") {
                if obj.len() > 1 {
                    return Err(FlowError::at(
                        path,
                        "an expression escape object must have \"$\" as its only key (write a literal \"$\" key as \"$$\")",
                    ));
                }
                return Ok(VExpr::Cel(compile_cel(src, path)?));
            }
            // A "$" key with a non-string value stays a literal key; its value
            // is still evaluated deeply like any other.
            let mut out = Vec::with_capacity(obj.len());
            let mut seen: HashSet<RuntimeString> = HashSet::new();
            for (k, v) in obj {
                let key = if runtime_eq_utf8(&k, "$$") {
                    RuntimeString::from_utf8("$")
                } else {
                    k.clone()
                };
                if !seen.insert(key.clone()) {
                    return Err(FlowError::at(
                        path,
                        format!(
                            "duplicate key \"{}\" after unescaping \"$$\"",
                            runtime_diagnostic(&key)
                        ),
                    ));
                }
                out.push((
                    key,
                    compile_vexpr(v, &format!("{path}.{}", runtime_diagnostic(&k)))?,
                ));
            }
            Ok(VExpr::Object(out))
        }
    }
}

// ---- identifiers ----------------------------------------------------------------

fn is_ident(s: &RuntimeString) -> bool {
    let mut units = s.as_utf16().iter().copied();
    match units.next() {
        Some(unit)
            if (u16::from(b'A')..=u16::from(b'Z')).contains(&unit)
                || (u16::from(b'a')..=u16::from(b'z')).contains(&unit)
                || unit == u16::from(b'_') => {}
        _ => return false,
    }
    units.all(|unit| {
        (u16::from(b'A')..=u16::from(b'Z')).contains(&unit)
            || (u16::from(b'a')..=u16::from(b'z')).contains(&unit)
            || (u16::from(b'0')..=u16::from(b'9')).contains(&unit)
            || unit == u16::from(b'_')
    })
}

/// Validate the schema-declared ASCII identifier domain and only then project
/// it to the host `String` used by CEL variable/function lookup. Every `String`
/// identifier stored below has crossed this closed scalar-only boundary.
fn validate_name(v: &RuntimeJson, path: &str) -> Result<String, FlowError> {
    let s = require_string(v, path)?;
    if !is_ident(&s) {
        return Err(FlowError::at(
            path,
            format!(
                "\"{}\" is not a valid name (use letters, digits, and \"_\", not starting with a digit)",
                runtime_diagnostic(&s)
            ),
        ));
    }
    let scalar = s
        .as_utf16()
        .iter()
        .map(|unit| char::from_u32(u32::from(*unit)).expect("validated ASCII identifier"))
        .collect::<String>();
    if RESERVED_NAMES.contains(&scalar.as_str()) {
        return Err(FlowError::at(
            path,
            format!("\"{scalar}\" is a reserved name"),
        ));
    }
    Ok(scalar)
}

// ---- steps -----------------------------------------------------------------------

/// Static validation context threaded through step compilation.
struct StepCtx<'a> {
    /// Names of the program's user functions (for validating `call`).
    funcs: &'a HashSet<String>,
}

fn all_tags() -> Vec<&'static str> {
    let mut tags: Vec<&'static str> = SHARED_TAGS.to_vec();
    tags.extend(WORKFLOW_ONLY_TAGS);
    tags
}

/// The companion keys each step tag may carry alongside the tag itself.
fn companions(tag: &str) -> &'static [&'static str] {
    match tag {
        "if" => &["then", "else"],
        "switch" => &["default"],
        "while" => &["do"],
        "try" => &["catch", "finally"],
        "parallel" | "map" | "call" | "agent" | "workflow" => &["as"],
        _ => &[],
    }
}

fn compile_steps(
    v: &Json,
    path: &str,
    ctx: &StepCtx<'_>,
    in_loop: bool,
) -> Result<Vec<Step>, FlowError> {
    let items = match v {
        RuntimeJson::Array(items) => items,
        other => {
            return Err(FlowError::at(
                path,
                format!("expected an array of steps, got {}", value_type(other)),
            ))
        }
    };
    let mut out = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        out.push(compile_step(item, &format!("{path}[{i}]"), ctx, in_loop)?);
    }
    Ok(out)
}

/// Read the optional `"as"` binding of a step.
fn bind_of(obj: &RuntimeObject, path: &str) -> Result<Option<String>, FlowError> {
    match obj.get("as") {
        None => Ok(None),
        Some(v) => Ok(Some(validate_name(v, &format!("{path}.as"))?)),
    }
}

/// Read the binding for `parallel` / `map`, whose canonical `"as"` position is
/// INSIDE the tag value; the sibling position is also accepted — but not both.
fn merged_bind(
    inner: &RuntimeObject,
    outer: &RuntimeObject,
    path: &str,
    vpath: &str,
) -> Result<Option<String>, FlowError> {
    let inner_bind = match inner.get("as") {
        None => None,
        Some(v) => Some(validate_name(v, &format!("{vpath}.as"))?),
    };
    let outer_bind = bind_of(outer, path)?;
    match (inner_bind, outer_bind) {
        (Some(_), Some(_)) => Err(FlowError::at(
            path,
            "provide \"as\" once (inside the step object or beside the tag), not both",
        )),
        (a, b) => Ok(a.or(b)),
    }
}

fn compile_step(v: &Json, path: &str, ctx: &StepCtx<'_>, in_loop: bool) -> Result<Step, FlowError> {
    let obj = require_object(v, path)?;

    // Identify the step tag: exactly one primary tag key must be present.
    let my_tags = all_tags();
    let present: Vec<&str> = my_tags
        .iter()
        .copied()
        .filter(|t| obj.contains_key(*t))
        .collect();
    let tag = match present.as_slice() {
        [] => {
            return Err(FlowError::at(
                path,
                format!(
                    "no step tag found among keys [{}]; a step must carry exactly one of: {}",
                    obj.keys()
                        .map(|key| runtime_diagnostic(&key))
                        .collect::<Vec<_>>()
                        .join(", "),
                    my_tags.join(", ")
                ),
            ))
        }
        [one] => *one,
        many => {
            let mut sorted: Vec<&str> = many.to_vec();
            sorted.sort_unstable();
            return Err(FlowError::at(
                path,
                format!(
                    "ambiguous step: found multiple step tags ({}); a step must carry exactly one",
                    sorted.join(", ")
                ),
            ));
        }
    };

    // Reject stray keys not belonging to this tag.
    {
        let mut allowed: Vec<&str> = vec![tag];
        allowed.extend_from_slice(companions(tag));
        for key in obj.keys() {
            if !allowed.iter().any(|allowed| runtime_eq_utf8(&key, allowed)) {
                return Err(FlowError::at(
                    path,
                    format!(
                        "unknown key \"{}\" for step \"{tag}\" (allowed keys: {})",
                        runtime_diagnostic(&key),
                        allowed.join(", ")
                    ),
                ));
            }
        }
    }

    let tag_value = obj
        .get(tag)
        .expect("the selected static step tag was observed above");
    let vpath = format!("{path}.{tag}");
    match tag {
        "set" => {
            let sobj = require_object(tag_value, &vpath)?;
            reject_unknown_keys(sobj, &["var", "value"], &vpath)?;
            let var = match sobj.get("var") {
                Some(v) => validate_name(v, &format!("{vpath}.var"))?,
                None => return Err(FlowError::at(&vpath, "missing required key \"var\"")),
            };
            let value = match sobj.get("value") {
                Some(v) => compile_vexpr(v, &format!("{vpath}.value"))?,
                None => return Err(FlowError::at(&vpath, "missing required key \"value\"")),
            };
            Ok(Step::Set { var, value })
        }
        "if" => {
            let cond = compile_vexpr(tag_value, &vpath)?;
            let then = match obj.get("then") {
                Some(v) => compile_steps(v, &format!("{path}.then"), ctx, in_loop)?,
                None => {
                    return Err(FlowError::at(
                        path,
                        "step \"if\" requires a \"then\" array of steps",
                    ))
                }
            };
            let els = match obj.get("else") {
                Some(v) => Some(compile_steps(v, &format!("{path}.else"), ctx, in_loop)?),
                None => None,
            };
            Ok(Step::If { cond, then, els })
        }
        "switch" => {
            let items = match tag_value {
                RuntimeJson::Array(items) => items,
                other => {
                    return Err(FlowError::at(
                        &vpath,
                        format!(
                            "expected an array of {{case, do}} objects, got {}",
                            value_type(other)
                        ),
                    ))
                }
            };
            let mut cases = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let cpath = format!("{vpath}[{i}]");
                let cobj = require_object(item, &cpath)?;
                reject_unknown_keys(cobj, &["case", "do"], &cpath)?;
                let cond = match cobj.get("case") {
                    Some(v) => compile_vexpr(v, &format!("{cpath}.case"))?,
                    None => return Err(FlowError::at(&cpath, "missing required key \"case\"")),
                };
                let body = match cobj.get("do") {
                    Some(v) => compile_steps(v, &format!("{cpath}.do"), ctx, in_loop)?,
                    None => return Err(FlowError::at(&cpath, "missing required key \"do\"")),
                };
                cases.push((cond, body));
            }
            let default = match obj.get("default") {
                Some(v) => Some(compile_steps(v, &format!("{path}.default"), ctx, in_loop)?),
                None => None,
            };
            Ok(Step::Switch { cases, default })
        }
        "while" => {
            let cond = compile_vexpr(tag_value, &vpath)?;
            let body = match obj.get("do") {
                Some(v) => compile_steps(v, &format!("{path}.do"), ctx, true)?,
                None => {
                    return Err(FlowError::at(
                        path,
                        "step \"while\" requires a \"do\" array of steps",
                    ))
                }
            };
            Ok(Step::While { cond, body })
        }
        "break" | "continue" => {
            if !in_loop {
                return Err(FlowError::at(
                    path,
                    format!("\"{tag}\" is only valid inside a \"while\" or \"for\" loop body (parallel branches, map bodies, pipeline stages, and functions are separate scopes)"),
                ));
            }
            Ok(if tag == "break" {
                Step::Break
            } else {
                Step::Continue
            })
        }
        "for" => {
            let fobj = require_object(tag_value, &vpath)?;
            reject_unknown_keys(fobj, &["var", "index", "in", "do"], &vpath)?;
            let var = match fobj.get("var") {
                Some(v) => validate_name(v, &format!("{vpath}.var"))?,
                None => return Err(FlowError::at(&vpath, "missing required key \"var\"")),
            };
            let index = match fobj.get("index") {
                Some(v) => Some(validate_name(v, &format!("{vpath}.index"))?),
                None => None,
            };
            if index.as_deref() == Some(var.as_str()) {
                return Err(FlowError::at(
                    &format!("{vpath}.index"),
                    "\"index\" must differ from \"var\"",
                ));
            }
            let list = match fobj.get("in") {
                Some(v) => compile_vexpr(v, &format!("{vpath}.in"))?,
                None => return Err(FlowError::at(&vpath, "missing required key \"in\"")),
            };
            let body = match fobj.get("do") {
                Some(v) => compile_steps(v, &format!("{vpath}.do"), ctx, true)?,
                None => return Err(FlowError::at(&vpath, "missing required key \"do\"")),
            };
            Ok(Step::For {
                var,
                index,
                list,
                body,
            })
        }
        "parallel" => {
            let pobj = require_object(tag_value, &vpath)?;
            reject_unknown_keys(pobj, &["branches", "as"], &vpath)?;
            let items = match pobj.get("branches") {
                Some(RuntimeJson::Array(items)) => items,
                Some(other) => {
                    return Err(FlowError::at(
                        &format!("{vpath}.branches"),
                        format!("expected an array of branches, got {}", value_type(other)),
                    ))
                }
                None => return Err(FlowError::at(&vpath, "missing required key \"branches\"")),
            };
            let mut branches = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let bpath = format!("{vpath}.branches[{i}]");
                let bobj = require_object(item, &bpath)?;
                reject_unknown_keys(bobj, &["do", "value"], &bpath)?;
                let body = match bobj.get("do") {
                    Some(v) => compile_steps(v, &format!("{bpath}.do"), ctx, false)?,
                    None => return Err(FlowError::at(&bpath, "missing required key \"do\"")),
                };
                let value = match bobj.get("value") {
                    Some(v) => Some(compile_vexpr(v, &format!("{bpath}.value"))?),
                    None => None,
                };
                branches.push(Branch { body, value });
            }
            Ok(Step::Parallel {
                branches,
                bind: merged_bind(pobj, obj, path, &vpath)?,
            })
        }
        "map" => compile_map(tag_value, obj, path, &vpath, ctx),
        "agent" => compile_agent(tag_value, obj, path, &vpath),
        "workflow" => {
            let wobj = require_object(tag_value, &vpath)?;
            reject_unknown_keys(wobj, &["name", "scriptPath", "args"], &vpath)?;
            let target = match (wobj.get("name"), wobj.get("scriptPath")) {
                (Some(v), None) => compile_vexpr(v, &format!("{vpath}.name"))?,
                (None, Some(v)) => compile_vexpr(v, &format!("{vpath}.scriptPath"))?,
                (Some(_), Some(_)) => {
                    return Err(FlowError::at(
                        &vpath,
                        "provide exactly one of \"name\" or \"scriptPath\", not both",
                    ))
                }
                (None, None) => {
                    return Err(FlowError::at(
                        &vpath,
                        "provide exactly one of \"name\" or \"scriptPath\"",
                    ))
                }
            };
            let args = match wobj.get("args") {
                Some(v) => Some(compile_vexpr(v, &format!("{vpath}.args"))?),
                None => None,
            };
            Ok(Step::Workflow {
                target,
                args,
                bind: bind_of(obj, path)?,
            })
        }
        "phase" => Ok(Step::Phase(compile_vexpr(tag_value, &vpath)?)),
        "log" => Ok(Step::Log(compile_vexpr(tag_value, &vpath)?)),
        "try" => {
            let body = compile_steps(tag_value, &vpath, ctx, in_loop)?;
            let catch = match obj.get("catch") {
                None => None,
                Some(v) => {
                    let cpath = format!("{path}.catch");
                    let cobj = require_object(v, &cpath)?;
                    reject_unknown_keys(cobj, &["var", "do"], &cpath)?;
                    let var = match cobj.get("var") {
                        Some(v) => Some(validate_name(v, &format!("{cpath}.var"))?),
                        None => None,
                    };
                    let steps = match cobj.get("do") {
                        Some(v) => compile_steps(v, &format!("{cpath}.do"), ctx, in_loop)?,
                        None => return Err(FlowError::at(&cpath, "missing required key \"do\"")),
                    };
                    Some((var, steps))
                }
            };
            let finally = match obj.get("finally") {
                Some(v) => Some(compile_steps(v, &format!("{path}.finally"), ctx, in_loop)?),
                None => None,
            };
            if catch.is_none() && finally.is_none() {
                return Err(FlowError::at(
                    path,
                    "step \"try\" requires a \"catch\" and/or a \"finally\"",
                ));
            }
            Ok(Step::Try {
                body,
                catch,
                finally,
            })
        }
        "throw" => Ok(Step::Throw(compile_vexpr(tag_value, &vpath)?)),
        "return" => Ok(Step::Return(compile_vexpr(tag_value, &vpath)?)),
        "call" => {
            let cobj = require_object(tag_value, &vpath)?;
            reject_unknown_keys(cobj, &["fn", "args"], &vpath)?;
            let name = match cobj.get("fn") {
                Some(v) => validate_name(v, &format!("{vpath}.fn"))?,
                None => return Err(FlowError::at(&vpath, "missing required key \"fn\"")),
            };
            if !ctx.funcs.contains(&name) {
                let mut known: Vec<&str> = ctx.funcs.iter().map(String::as_str).collect();
                known.sort_unstable();
                return Err(FlowError::at(
                    &format!("{vpath}.fn"),
                    if known.is_empty() {
                        format!("unknown function \"{name}\" (the program declares no \"funcs\")")
                    } else {
                        format!(
                            "unknown function \"{name}\" (declared functions: {})",
                            known.join(", ")
                        )
                    },
                ));
            }
            let args = match cobj.get("args") {
                None => Vec::new(),
                Some(RuntimeJson::Array(items)) => {
                    let mut out = Vec::with_capacity(items.len());
                    for (i, item) in items.iter().enumerate() {
                        out.push(compile_vexpr(item, &format!("{vpath}.args[{i}]"))?);
                    }
                    out
                }
                Some(other) => {
                    return Err(FlowError::at(
                        &format!("{vpath}.args"),
                        format!("expected an array, got {}", value_type(other)),
                    ))
                }
            };
            Ok(Step::Call {
                name,
                args,
                bind: bind_of(obj, path)?,
            })
        }
        other => Err(FlowError::at(path, format!("unknown step tag \"{other}\""))),
    }
}

fn compile_map(
    tag_value: &Json,
    obj: &RuntimeObject,
    path: &str,
    vpath: &str,
    ctx: &StepCtx<'_>,
) -> Result<Step, FlowError> {
    let mobj = require_object(tag_value, vpath)?;
    reject_unknown_keys(
        mobj,
        &["var", "index", "in", "mode", "do", "value", "stages", "as"],
        vpath,
    )?;
    let var = match mobj.get("var") {
        Some(v) => validate_name(v, &format!("{vpath}.var"))?,
        None => return Err(FlowError::at(vpath, "missing required key \"var\"")),
    };
    let index = match mobj.get("index") {
        Some(v) => Some(validate_name(v, &format!("{vpath}.index"))?),
        None => None,
    };
    if index.as_deref() == Some(var.as_str()) {
        return Err(FlowError::at(
            &format!("{vpath}.index"),
            "\"index\" must differ from \"var\"",
        ));
    }
    let list = match mobj.get("in") {
        Some(v) => compile_vexpr(v, &format!("{vpath}.in"))?,
        None => return Err(FlowError::at(vpath, "missing required key \"in\"")),
    };
    let mode = match mobj.get("mode") {
        Some(RuntimeJson::String(s)) if runtime_eq_utf8(s, "seq") => MapMode::Seq,
        Some(RuntimeJson::String(s)) if runtime_eq_utf8(s, "parallel") => MapMode::Parallel,
        Some(RuntimeJson::String(s)) if runtime_eq_utf8(s, "pipeline") => MapMode::Pipeline,
        Some(RuntimeJson::String(other)) => {
            return Err(FlowError::at(
                &format!("{vpath}.mode"),
                format!(
                    "unknown mode \"{}\" (expected \"seq\", \"parallel\", or \"pipeline\")",
                    runtime_diagnostic(other)
                ),
            ))
        }
        Some(other) => {
            return Err(FlowError::at(
                &format!("{vpath}.mode"),
                format!("expected a string, got {}", value_type(other)),
            ))
        }
        None => return Err(FlowError::at(vpath, "missing required key \"mode\"")),
    };
    let body = match mobj.get("do") {
        Some(v) => compile_steps(v, &format!("{vpath}.do"), ctx, false)?,
        None => Vec::new(),
    };
    let value = match mobj.get("value") {
        Some(v) => Some(compile_vexpr(v, &format!("{vpath}.value"))?),
        None => None,
    };
    let stages_val = mobj.get("stages");
    match mode {
        MapMode::Pipeline => {
            if mobj.contains_key("do") || mobj.contains_key("value") {
                return Err(FlowError::at(
                    vpath,
                    "pipeline mode uses \"stages\", not \"do\"/\"value\" (put them inside each stage)",
                ));
            }
            // `prev` is auto-bound in every stage.
            if var == "prev" || index.as_deref() == Some("prev") {
                return Err(FlowError::at(
                    vpath,
                    "\"prev\" is bound automatically in pipeline stages; pick another name",
                ));
            }
        }
        _ => {
            if stages_val.is_some() {
                return Err(FlowError::at(
                    vpath,
                    "\"stages\" is only valid with mode \"pipeline\" (use \"do\"/\"value\" for seq/parallel)",
                ));
            }
            if !mobj.contains_key("do") && !mobj.contains_key("value") {
                return Err(FlowError::at(
                    vpath,
                    "map requires at least one of \"do\" or \"value\" per item",
                ));
            }
        }
    }
    let stages = match (mode, stages_val) {
        (MapMode::Pipeline, Some(RuntimeJson::Array(items))) => {
            if items.is_empty() {
                return Err(FlowError::at(
                    &format!("{vpath}.stages"),
                    "pipeline mode requires at least one stage",
                ));
            }
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let spath = format!("{vpath}.stages[{i}]");
                let sobj = require_object(item, &spath)?;
                reject_unknown_keys(sobj, &["do", "value"], &spath)?;
                let sbody = match sobj.get("do") {
                    Some(v) => compile_steps(v, &format!("{spath}.do"), ctx, false)?,
                    None => Vec::new(),
                };
                let svalue = match sobj.get("value") {
                    Some(v) => Some(compile_vexpr(v, &format!("{spath}.value"))?),
                    None => None,
                };
                if !sobj.contains_key("do") && !sobj.contains_key("value") {
                    return Err(FlowError::at(
                        &spath,
                        "a stage requires at least one of \"do\" or \"value\"",
                    ));
                }
                out.push(StageDef {
                    body: sbody,
                    value: svalue,
                });
            }
            out
        }
        (MapMode::Pipeline, Some(other)) => {
            return Err(FlowError::at(
                &format!("{vpath}.stages"),
                format!("expected an array of stages, got {}", value_type(other)),
            ))
        }
        (MapMode::Pipeline, None) => {
            return Err(FlowError::at(vpath, "pipeline mode requires \"stages\""))
        }
        _ => Vec::new(),
    };
    Ok(Step::Map(MapStep {
        var,
        index,
        list,
        mode,
        body,
        value,
        stages,
        bind: merged_bind(mobj, obj, path, vpath)?,
    }))
}

fn compile_agent(
    tag_value: &Json,
    obj: &RuntimeObject,
    path: &str,
    vpath: &str,
) -> Result<Step, FlowError> {
    let aobj = require_object(tag_value, vpath)?;
    reject_unknown_keys(
        aobj,
        &[
            "prompt",
            "label",
            "phase",
            "schema",
            "model",
            "isolation",
            "agentType",
        ],
        vpath,
    )?;
    let prompt = match aobj.get("prompt") {
        Some(v) => compile_vexpr(v, &format!("{vpath}.prompt"))?,
        None => return Err(FlowError::at(vpath, "missing required key \"prompt\"")),
    };
    let opt = |key: &str| -> Result<Option<VExpr>, FlowError> {
        match aobj.get(key) {
            Some(v) => Ok(Some(compile_vexpr(v, &format!("{vpath}.{key}"))?)),
            None => Ok(None),
        }
    };
    let schema = match aobj.get("schema") {
        None => None,
        Some(RuntimeJson::Object(o)) => Some(o.clone()),
        Some(other) => {
            return Err(FlowError::at(
                &format!("{vpath}.schema"),
                format!(
                    "expected a literal JSON Schema object (not a value expression), got {}",
                    value_type(other)
                ),
            ))
        }
    };
    Ok(Step::Agent(Box::new(AgentStep {
        prompt,
        label: opt("label")?,
        phase: opt("phase")?,
        schema,
        model: opt("model")?,
        isolation: opt("isolation")?,
        agent_type: opt("agentType")?,
        bind: bind_of(obj, path)?,
    })))
}

// ---- program ---------------------------------------------------------------------

/// Parse, validate, and compile a flow program. All expressions are compiled
/// here, so a syntactically invalid expression is a validation error, not a
/// runtime one.
pub fn validate_program(source: &RuntimeString) -> Result<FlowProgram, FlowError> {
    let doc = parse_runtime_document(source)?;
    let obj = require_object(&doc, "program")?;
    reject_unknown_keys(obj, &["meta", "vars", "funcs", "body"], "program")?;

    let raw_meta = obj
        .get("meta")
        .ok_or_else(|| FlowError::new("program: missing required key \"meta\""))?
        .clone();
    let meta = validate_meta(&raw_meta, "meta")?;

    let mut vars: Vec<(String, VExpr)> = Vec::new();
    if let Some(v) = obj.get("vars") {
        let items = match v {
            RuntimeJson::Array(items) => items,
            other => {
                return Err(FlowError::at(
                    "vars",
                    format!("expected an array, got {}", value_type(other)),
                ))
            }
        };
        let mut seen: HashSet<String> = HashSet::new();
        for (i, item) in items.iter().enumerate() {
            let path = format!("vars[{i}]");
            let vobj = require_object(item, &path)?;
            reject_unknown_keys(vobj, &["var", "value"], &path)?;
            let name = match vobj.get("var") {
                Some(v) => validate_name(v, &format!("{path}.var"))?,
                None => return Err(FlowError::at(&path, "missing required key \"var\"")),
            };
            if !seen.insert(name.clone()) {
                return Err(FlowError::at(
                    &format!("{path}.var"),
                    format!("duplicate variable \"{name}\""),
                ));
            }
            let value = match vobj.get("value") {
                Some(v) => compile_vexpr(v, &format!("{path}.value"))?,
                None => return Err(FlowError::at(&path, "missing required key \"value\"")),
            };
            vars.push((name, value));
        }
    }

    // Collect function names first so bodies (and the main body) can call
    // forward and recursively.
    let mut func_names: HashSet<String> = HashSet::new();
    if let Some(v) = obj.get("funcs") {
        let fobj = require_object(v, "funcs")?;
        for name in fobj.keys() {
            let named = RuntimeJson::String(name.clone());
            let scalar_name =
                validate_name(&named, &format!("funcs.{}", runtime_diagnostic(&name)))?;
            func_names.insert(scalar_name);
        }
    }
    let ctx = StepCtx { funcs: &func_names };

    let mut funcs: HashMap<String, FlowFunc> = HashMap::new();
    if let Some(v) = obj.get("funcs") {
        let fobj = require_object(v, "funcs")?;
        for (name, fv) in fobj {
            let named = RuntimeJson::String(name.clone());
            let scalar_name =
                validate_name(&named, &format!("funcs.{}", runtime_diagnostic(&name)))?;
            let path = format!("funcs.{}", runtime_diagnostic(&name));
            let def = require_object(fv, &path)?;
            reject_unknown_keys(def, &["params", "body"], &path)?;
            let mut params: Vec<String> = Vec::new();
            match def.get("params") {
                None => {}
                Some(RuntimeJson::Array(items)) => {
                    let mut seen: HashSet<String> = HashSet::new();
                    for (i, item) in items.iter().enumerate() {
                        let p = validate_name(item, &format!("{path}.params[{i}]"))?;
                        if !seen.insert(p.clone()) {
                            return Err(FlowError::at(
                                &format!("{path}.params[{i}]"),
                                format!("duplicate parameter \"{p}\""),
                            ));
                        }
                        params.push(p);
                    }
                }
                Some(other) => {
                    return Err(FlowError::at(
                        &format!("{path}.params"),
                        format!("expected an array, got {}", value_type(other)),
                    ))
                }
            }
            let body = match def.get("body") {
                Some(v) => compile_steps(v, &format!("{path}.body"), &ctx, false)?,
                None => return Err(FlowError::at(&path, "missing required key \"body\"")),
            };
            funcs.insert(scalar_name, FlowFunc { params, body });
        }
    }

    let body = match obj.get("body") {
        Some(v) => compile_steps(v, "body", &ctx, false)?,
        None => return Err(FlowError::new("program: missing required key \"body\"")),
    };

    Ok(FlowProgram {
        meta,
        raw_meta,
        vars,
        funcs,
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! exact_json {
        (@array [$($elements:expr,)*]) => {
            vec![$($elements,)*]
        };
        (@array [$($elements:expr),*]) => {
            vec![$($elements),*]
        };
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

    fn wf(v: Json) -> Result<FlowProgram, FlowError> {
        validate_program(&source(&v))
    }

    fn source(value: &Json) -> RuntimeString {
        runtime_json_source(value)
    }

    fn split_newline(value: &RuntimeString) -> Option<(RuntimeString, RuntimeString)> {
        let index = value
            .as_utf16()
            .iter()
            .position(|unit| *unit == u16::from(b'\n'))?;
        Some((
            value.slice_utf16(..index),
            value.slice_utf16(index.saturating_add(1)..),
        ))
    }

    fn meta() -> Json {
        exact_json!({ "name": "demo", "description": "d" })
    }

    #[test]
    fn schema_constant_is_valid_json() {
        let v = runtime_json::parse(&RuntimeString::from_utf8(FLOW_PROGRAM_SCHEMA))
            .expect("static scalar schema parses as exact JSON");
        assert!(v
            .get("title")
            .and_then(RuntimeJson::as_str)
            .is_some_and(|title| runtime_eq_utf8(title, "Flow program")));
        assert!(v.get("$defs").and_then(|d| d.get("step")).is_some());
    }

    #[test]
    fn exact_runtime_flow_preserves_utf16_and_ecmascript_object_order() {
        let source = RuntimeString::from(
            r#"{"meta":{"name":"\ud800","description":"\udc00","phases":[{"title":"\ud800x","detail":"y\udc00"}]},"body":[{"log":{"2":"two","1":"one","\ud800":"\udc00","a":"tail"}},{"log":"pre\ud800${1}post\udc00"},{"agent":{"prompt":"p","schema":{"2":{"const":"\ud800"},"1":{"const":"\udc00"},"a":true}}}]}"#,
        );
        let program = validate_program(&source).expect("exact flow compiles");

        assert_eq!(program.meta.name.as_utf16(), &[0xd800]);
        assert_eq!(program.meta.description.as_utf16(), &[0xdc00]);
        let phase = &program.meta.phases.as_ref().expect("phases")[0];
        assert_eq!(phase.title.as_utf16(), &[0xd800, u16::from(b'x')]);
        assert_eq!(
            phase.detail.as_ref().expect("detail").as_utf16(),
            &[u16::from(b'y'), 0xdc00]
        );

        let Step::Log(VExpr::Object(fields)) = &program.body[0] else {
            panic!("first step must be an exact literal object");
        };
        assert_eq!(
            fields
                .iter()
                .map(|(key, _)| key.as_utf16().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![u16::from(b'1')],
                vec![u16::from(b'2')],
                vec![0xd800],
                vec![u16::from(b'a')],
            ]
        );
        let VExpr::LitString(lone_low) = &fields[2].1 else {
            panic!("surrogate-key value must remain a literal runtime string");
        };
        assert_eq!(lone_low.as_utf16(), &[0xdc00]);

        let Step::Log(VExpr::Interp(parts)) = &program.body[1] else {
            panic!("second step must remain interpolated");
        };
        let [InterpPart::Lit(prefix), InterpPart::Expr(expr), InterpPart::Lit(suffix)] =
            parts.as_slice()
        else {
            panic!("interpolation must retain its three exact segments");
        };
        assert_eq!(
            prefix.as_utf16(),
            &[u16::from(b'p'), u16::from(b'r'), u16::from(b'e'), 0xd800]
        );
        assert_eq!(expr.source, "1");
        assert_eq!(
            suffix.as_utf16(),
            &[
                u16::from(b'p'),
                u16::from(b'o'),
                u16::from(b's'),
                u16::from(b't'),
                0xdc00,
            ]
        );

        let Step::Agent(agent) = &program.body[2] else {
            panic!("third step must be an agent");
        };
        let schema = agent.schema.as_ref().expect("literal schema");
        assert_eq!(
            schema
                .keys()
                .map(|key| key.as_utf16().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![u16::from(b'1')],
                vec![u16::from(b'2')],
                vec![u16::from(b'a')],
            ]
        );

        let raw_meta = runtime_json::stringify(&program.raw_meta)
            .try_to_utf8()
            .expect("writer escapes lone surrogates");
        assert!(raw_meta.contains("\\ud800"));
        assert!(raw_meta.contains("\\udc00"));
        let composed = compose_workflow_script(&source).expect("exact meta composes");
        let (header, rest) = split_newline(&composed).expect("header and original source");
        assert!(header.contains("\\ud800"));
        assert!(header.contains("\\udc00"));
        assert_eq!(rest, source);
    }

    #[test]
    fn cel_scalar_boundary_rejects_lone_surrogates_without_replacement() {
        let error = validate_program(&RuntimeString::from(
            r#"{"meta":{"name":"n","description":"d"},"body":[{"log":"${'\ud800'}"}]}"#,
        ))
        .unwrap_err();
        assert!(error.message.contains("unpaired surrogate U+D800"));
        assert!(error.message.contains("`'\\ud800'`"));
        assert!(!error.message.contains("\u{fffd}"));

        let unknown = validate_program(&RuntimeString::from(
            r#"{"meta":{"name":"n","description":"d"},"body":[{"\udc00":null}]}"#,
        ))
        .unwrap_err();
        assert!(unknown.message.contains("keys [\\udc00]"));
        assert!(!unknown.message.contains("\u{fffd}"));
    }

    #[test]
    fn validates_a_full_workflow_program() {
        let p = wf(exact_json!({
            "meta": {
                "name": "demo",
                "description": "d",
                "whenToUse": "testing",
                "phases": [ { "title": "One" }, { "title": "Two", "detail": "x", "model": "m" } ]
            },
            "vars": [ { "var": "total", "value": 0 } ],
            "funcs": { "twice": { "params": ["n"], "body": [ { "return": { "$": "n * 2" } } ] } },
            "body": [
                { "set": { "var": "x", "value": { "$": "1 + 1" } } },
                { "if": { "$": "x > 1" }, "then": [ { "log": "big" } ], "else": [ { "log": "small" } ] },
                { "while": { "$": "x < 4" }, "do": [ { "set": { "var": "x", "value": { "$": "x + 1" } } }, { "break": null } ] },
                { "for": { "var": "item", "index": "i", "in": [1, 2], "do": [ { "continue": null } ] } },
                { "parallel": { "branches": [ { "do": [], "value": 1 } ] }, "as": "par" },
                { "map": { "var": "item", "in": [1], "mode": "seq", "value": { "$": "item" } }, "as": "mapped" },
                { "map": { "var": "item", "in": [1], "mode": "pipeline", "stages": [ { "value": { "$": "prev" } } ] }, "as": "piped" },
                { "agent": { "prompt": "p", "schema": { "type": "object" } }, "as": "a" },
                { "workflow": { "name": "child", "args": { "q": 1 } }, "as": "w" },
                { "phase": "Phase 1" },
                { "try": [ { "throw": "boom" } ], "catch": { "var": "e", "do": [ { "log": { "$": "e.message" } } ] }, "finally": [ { "log": "done" } ] },
                { "call": { "fn": "twice", "args": [ 21 ] }, "as": "t" },
                { "switch": [ { "case": true, "do": [] } ], "default": [] },
                { "return": { "$": "x" } }
            ]
        }));
        let p = p.expect("valid program");
        assert_eq!(p.meta.name, "demo");
        assert_eq!(p.meta.phases.as_ref().unwrap().len(), 2);
        assert_eq!(p.vars.len(), 1);
        assert!(p.funcs.contains_key("twice"));
        assert_eq!(p.body.len(), 14);
    }

    #[test]
    fn rejects_non_json_and_non_object_sources() {
        let e = validate_program(&RuntimeString::from("not json")).unwrap_err();
        assert!(
            e.message.starts_with("flow program is not valid JSON:"),
            "{e}"
        );
        let e = validate_program(&RuntimeString::from("[1]")).unwrap_err();
        assert_eq!(e.message, "program: expected an object, got array");
    }

    #[test]
    fn rejects_unknown_top_level_and_missing_keys() {
        let e = wf(exact_json!({ "meta": meta(), "body": [], "extra": 1 })).unwrap_err();
        assert_eq!(
            e.message,
            "program: unknown key \"extra\" (allowed keys: meta, vars, funcs, body)"
        );
        let e = wf(exact_json!({ "body": [] })).unwrap_err();
        assert_eq!(e.message, "program: missing required key \"meta\"");
        let e = wf(exact_json!({ "meta": meta() })).unwrap_err();
        assert_eq!(e.message, "program: missing required key \"body\"");
    }

    #[test]
    fn meta_must_be_literal_and_typed() {
        let e = wf(exact_json!({ "meta": { "name": "", "description": "d" }, "body": [] }))
            .unwrap_err();
        assert_eq!(e.message, "meta.name: must not be empty");
        let e = wf(
            exact_json!({ "meta": { "name": { "$": "expr" }, "description": "d" }, "body": [] }),
        )
        .unwrap_err();
        assert_eq!(e.message, "meta.name: expected a string, got object");
        let e = wf(
            exact_json!({ "meta": { "name": "n", "description": "d", "model": "m" }, "body": [] }),
        )
        .unwrap_err();
        assert_eq!(
            e.message,
            "meta: unknown key \"model\" (allowed keys: name, description, whenToUse, phases)"
        );
        let e =
            wf(exact_json!({ "meta": { "name": "n", "description": "d", "phases": [ 3 ] }, "body": [] }))
                .unwrap_err();
        assert_eq!(e.message, "meta.phases[0]: expected an object, got number");
        let e = wf(
            exact_json!({ "meta": { "name": "n", "description": "d", "phases": [ {} ] }, "body": [] }),
        )
        .unwrap_err();
        assert_eq!(e.message, "meta.phases[0]: missing required key \"title\"");
    }

    #[test]
    fn rejects_unknown_step_tags_with_the_full_tag_list() {
        let e = wf(exact_json!({ "meta": meta(), "body": [ { "sett": {} } ] })).unwrap_err();
        assert!(
            e.message
                .starts_with("body[0]: no step tag found among keys [sett]"),
            "{e}"
        );
        assert!(e.message.contains("agent"), "{e}");
        assert!(!e.message.contains("invoke"), "{e}");
    }

    #[test]
    fn rejects_ambiguous_steps() {
        let e = wf(exact_json!({ "meta": meta(), "body": [ { "log": "x", "phase": "y" } ] }))
            .unwrap_err();
        assert_eq!(
            e.message,
            "body[0]: ambiguous step: found multiple step tags (log, phase); a step must carry exactly one"
        );
    }

    #[test]
    fn rejects_steps_outside_the_format() {
        let e = wf(exact_json!({ "meta": meta(), "body": [ { "invoke": { "tool": "read" } } ] }))
            .unwrap_err();
        assert!(
            e.message
                .starts_with("body[0]: no step tag found among keys [invoke]"),
            "{e}"
        );
    }

    #[test]
    fn rejects_stray_companion_keys() {
        let e = wf(exact_json!({ "meta": meta(), "body": [ { "while": true, "then": [] } ] }))
            .unwrap_err();
        assert_eq!(
            e.message,
            "body[0]: unknown key \"then\" for step \"while\" (allowed keys: while, do)"
        );
        let e =
            wf(exact_json!({ "meta": meta(), "body": [ { "log": "x", "as": "y" } ] })).unwrap_err();
        assert_eq!(
            e.message,
            "body[0]: unknown key \"as\" for step \"log\" (allowed keys: log)"
        );
    }

    #[test]
    fn rejects_break_and_continue_outside_loops() {
        let e = wf(exact_json!({ "meta": meta(), "body": [ { "break": null } ] })).unwrap_err();
        assert!(
            e.message.starts_with(
                "body[0]: \"break\" is only valid inside a \"while\" or \"for\" loop body"
            ),
            "{e}"
        );
        // A parallel branch inside a loop is a separate scope.
        let e = wf(exact_json!({ "meta": meta(), "body": [
            { "while": true, "do": [
                { "parallel": { "branches": [ { "do": [ { "continue": null } ] } ] } }
            ] }
        ] }))
        .unwrap_err();
        assert!(
            e.message.starts_with(
                "body[0].do[0].parallel.branches[0].do[0]: \"continue\" is only valid inside"
            ),
            "{e}"
        );
        // A loop inside a branch re-enables them.
        assert!(wf(exact_json!({ "meta": meta(), "body": [
            { "parallel": { "branches": [ { "do": [
                { "while": true, "do": [ { "break": null } ] }
            ] } ] } }
        ] }))
        .is_ok());
    }

    #[test]
    fn rejects_invalid_expression_syntax_at_validation_time() {
        let e =
            wf(exact_json!({ "meta": meta(), "body": [ { "log": { "$": "1 +" } } ] })).unwrap_err();
        assert!(
            e.message
                .starts_with("body[0].log: invalid expression `1 +`:"),
            "{e}"
        );
    }

    #[test]
    fn rejects_malformed_escape_objects() {
        let e = wf(exact_json!({ "meta": meta(), "body": [ { "log": { "$": "1", "x": 2 } } ] }))
            .unwrap_err();
        assert_eq!(
            e.message,
            "body[0].log: an expression escape object must have \"$\" as its only key (write a literal \"$\" key as \"$$\")"
        );
        // "$" with a non-string value stays a literal key.
        assert!(wf(exact_json!({ "meta": meta(), "body": [ { "log": { "$": 5 } } ] })).is_ok());
        // "$$" unescaping into a colliding "$" key is rejected.
        let e = wf(exact_json!({ "meta": meta(), "body": [ { "log": { "$": 5, "$$": 6 } } ] }))
            .unwrap_err();
        assert_eq!(
            e.message,
            "body[0].log: duplicate key \"$\" after unescaping \"$$\""
        );
    }

    #[test]
    fn rejects_unterminated_interpolation() {
        let e =
            wf(exact_json!({ "meta": meta(), "body": [ { "log": "a ${1 + 2" } ] })).unwrap_err();
        assert_eq!(
            e.message,
            "body[0].log: unterminated \"${\" expression in string (escape a literal \"${\" as \"$${\")"
        );
    }

    #[test]
    fn rejects_reserved_and_malformed_names() {
        let e = wf(
            exact_json!({ "meta": meta(), "body": [ { "set": { "var": "args", "value": 1 } } ] }),
        )
        .unwrap_err();
        assert_eq!(e.message, "body[0].set.var: \"args\" is a reserved name");
        let e =
            wf(exact_json!({ "meta": meta(), "body": [ { "set": { "var": "1x", "value": 1 } } ] }))
                .unwrap_err();
        assert!(e.message.contains("not a valid name"), "{e}");
        let e = wf(exact_json!({ "meta": meta(), "vars": [ { "var": "budget_total", "value": 1 } ], "body": [] }))
            .unwrap_err();
        assert_eq!(
            e.message,
            "vars[0].var: \"budget_total\" is a reserved name"
        );
    }

    #[test]
    fn rejects_duplicate_vars_and_params() {
        let e = wf(exact_json!({ "meta": meta(), "vars": [
            { "var": "x", "value": 1 }, { "var": "x", "value": 2 }
        ], "body": [] }))
        .unwrap_err();
        assert_eq!(e.message, "vars[1].var: duplicate variable \"x\"");
        let e = wf(exact_json!({ "meta": meta(), "funcs": {
            "f": { "params": ["a", "a"], "body": [] }
        }, "body": [] }))
        .unwrap_err();
        assert_eq!(e.message, "funcs.f.params[1]: duplicate parameter \"a\"");
    }

    #[test]
    fn rejects_calls_to_unknown_functions() {
        let e = wf(exact_json!({ "meta": meta(), "body": [ { "call": { "fn": "nope" } } ] }))
            .unwrap_err();
        assert_eq!(
            e.message,
            "body[0].call.fn: unknown function \"nope\" (the program declares no \"funcs\")"
        );
        let e = wf(
            exact_json!({ "meta": meta(), "funcs": { "f": { "params": [], "body": [] } },
            "body": [ { "call": { "fn": "nope" } } ] }),
        )
        .unwrap_err();
        assert_eq!(
            e.message,
            "body[0].call.fn: unknown function \"nope\" (declared functions: f)"
        );
    }

    #[test]
    fn functions_can_recurse_and_call_forward() {
        assert!(wf(exact_json!({ "meta": meta(), "funcs": {
            "a": { "params": [], "body": [ { "call": { "fn": "b" } } ] },
            "b": { "params": [], "body": [ { "call": { "fn": "a" } } ] }
        }, "body": [ { "call": { "fn": "a" } } ] }))
        .is_ok());
    }

    #[test]
    fn map_mode_constraints() {
        let e = wf(exact_json!({ "meta": meta(), "body": [
            { "map": { "var": "x", "in": [], "mode": "pipeline", "do": [] } }
        ] }))
        .unwrap_err();
        assert_eq!(
            e.message,
            "body[0].map: pipeline mode uses \"stages\", not \"do\"/\"value\" (put them inside each stage)"
        );
        let e = wf(exact_json!({ "meta": meta(), "body": [
            { "map": { "var": "x", "in": [], "mode": "seq", "stages": [], "do": [] } }
        ] }))
        .unwrap_err();
        assert_eq!(
            e.message,
            "body[0].map: \"stages\" is only valid with mode \"pipeline\" (use \"do\"/\"value\" for seq/parallel)"
        );
        let e = wf(exact_json!({ "meta": meta(), "body": [
            { "map": { "var": "x", "in": [], "mode": "seq" } }
        ] }))
        .unwrap_err();
        assert_eq!(
            e.message,
            "body[0].map: map requires at least one of \"do\" or \"value\" per item"
        );
        let e = wf(exact_json!({ "meta": meta(), "body": [
            { "map": { "var": "x", "in": [], "mode": "pipeline", "stages": [] } }
        ] }))
        .unwrap_err();
        assert_eq!(
            e.message,
            "body[0].map.stages: pipeline mode requires at least one stage"
        );
        let e = wf(exact_json!({ "meta": meta(), "body": [
            { "map": { "var": "x", "in": [], "mode": "sideways", "do": [] } }
        ] }))
        .unwrap_err();
        assert_eq!(
            e.message,
            "body[0].map.mode: unknown mode \"sideways\" (expected \"seq\", \"parallel\", or \"pipeline\")"
        );
    }

    #[test]
    fn workflow_step_requires_exactly_one_target() {
        let e = wf(exact_json!({ "meta": meta(), "body": [ { "workflow": {} } ] })).unwrap_err();
        assert_eq!(
            e.message,
            "body[0].workflow: provide exactly one of \"name\" or \"scriptPath\""
        );
        let e = wf(exact_json!({ "meta": meta(), "body": [
            { "workflow": { "name": "a", "scriptPath": "b" } }
        ] }))
        .unwrap_err();
        assert_eq!(
            e.message,
            "body[0].workflow: provide exactly one of \"name\" or \"scriptPath\", not both"
        );
    }

    #[test]
    fn agent_schema_must_be_a_literal_object() {
        let e = wf(exact_json!({ "meta": meta(), "body": [
            { "agent": { "prompt": "p", "schema": "not-an-object" } }
        ] }))
        .unwrap_err();
        assert_eq!(
            e.message,
            "body[0].agent.schema: expected a literal JSON Schema object (not a value expression), got string"
        );
    }

    #[test]
    fn parallel_and_map_accept_as_in_both_positions_but_not_twice() {
        // Canonical: "as" inside the tag value.
        assert!(wf(exact_json!({ "meta": meta(), "body": [
            { "parallel": { "branches": [ { "do": [] } ], "as": "x" } },
            { "map": { "var": "i", "in": [], "mode": "seq", "value": 1, "as": "y" } }
        ] }))
        .is_ok());
        // Sibling position also accepted.
        assert!(wf(exact_json!({ "meta": meta(), "body": [
            { "parallel": { "branches": [ { "do": [] } ] }, "as": "x" }
        ] }))
        .is_ok());
        // Both at once is ambiguous.
        let e = wf(exact_json!({ "meta": meta(), "body": [
            { "parallel": { "branches": [ { "do": [] } ], "as": "x" }, "as": "y" }
        ] }))
        .unwrap_err();
        assert_eq!(
            e.message,
            "body[0]: provide \"as\" once (inside the step object or beside the tag), not both"
        );
    }

    #[test]
    fn try_requires_catch_or_finally() {
        let e = wf(exact_json!({ "meta": meta(), "body": [ { "try": [] } ] })).unwrap_err();
        assert_eq!(
            e.message,
            "body[0]: step \"try\" requires a \"catch\" and/or a \"finally\""
        );
    }

    #[test]
    fn parse_flow_meta_is_a_pure_parse() {
        let meta = parse_flow_meta(&source(&exact_json!({
            "meta": { "name": "n", "description": "d", "whenToUse": "w",
                      "phases": [ { "title": "T" } ] },
            // The body is NOT validated by meta extraction.
            "body": [ { "definitely-not-a-step": true } ]
        })))
        .expect("meta parses");
        assert_eq!(meta.name, "n");
        assert!(meta
            .when_to_use
            .as_ref()
            .is_some_and(|value| runtime_eq_utf8(value, "w")));
        assert_eq!(meta.phases.as_ref().unwrap()[0].title, "T");
        let e = parse_flow_meta(&RuntimeString::from("nope")).unwrap_err();
        assert!(e.message.starts_with("flow program is not valid JSON:"));
        let e = parse_flow_meta(&RuntimeString::from("{}")).unwrap_err();
        assert_eq!(e.message, "program: missing required key \"meta\"");
    }

    #[test]
    fn compose_prepends_a_single_line_meta_header() {
        let src = source(
            &exact_json!({ "meta": { "name": "n", "description": "d line\ntwo" }, "body": [] }),
        );
        let composed = compose_workflow_script(&src).expect("composes");
        let (header, rest) = split_newline(&composed).expect("two parts");
        assert!(header.starts_with("export const meta = {"), "{header:?}");
        // Newlines inside meta strings stay escaped, keeping the header one line.
        assert!(header.contains("d line\\ntwo"), "{header:?}");
        assert_eq!(rest, src);
        // The composed form parses under the workflow runtime's meta parser.
        let parsed = crate::engine::workflow::meta::parse_workflow(&composed).expect("parses");
        assert_eq!(parsed.meta.name, "n");
        assert_eq!(parsed.meta.description, "d line\ntwo");
    }

    #[test]
    fn compose_rejects_bad_meta_with_flow_diagnostics() {
        let e = compose_workflow_script(&source(
            &exact_json!({ "meta": { "name": "n" }, "body": [] }),
        ))
        .unwrap_err();
        assert_eq!(e.message, "meta: missing required key \"description\"");
    }

    #[test]
    fn is_flow_source_checks_the_first_byte() {
        assert!(is_flow_source(&RuntimeString::from("  { \"meta\": {} }")));
        assert!(!is_flow_source(&RuntimeString::from(
            "export const meta = {}"
        )));
        assert!(!is_flow_source(&RuntimeString::default()));
    }

    #[test]
    fn interpolation_and_escape_forms_compile() {
        // "$${" is a literal "${"; a bare "$" is literal; "${...}" compiles.
        let p = wf(exact_json!({ "meta": meta(), "body": [
            { "log": "cost: $${not evaluated} $5 ${1 + 2}" },
            { "log": "${'nested { braces } and \\'quotes\\''}" }
        ] }));
        assert!(p.is_ok(), "{:?}", p.err());
    }
}
