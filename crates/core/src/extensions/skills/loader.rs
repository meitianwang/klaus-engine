//! Discovery and loading of `SKILL.md` extensions.
//!
//! A skill is a subdirectory that contains a `SKILL.md` file. The file is a
//! markdown document with an optional YAML frontmatter block describing the
//! skill's metadata (name, description, allowed tools, model/effort overrides,
//! conditional `paths`, and so on). Loading is deliberately lenient: unknown
//! frontmatter keys are ignored, a missing/non-scalar description falls back to
//! the markdown body, and a numeric/invalid effort degrades that single field
//! rather than dropping the whole skill. Only a hard type mismatch on a strongly
//! typed field (e.g. a non-string `model`, or a `context` outside
//! `inline`/`fork`) drops the skill — it is skipped with a warning instead of
//! failing discovery.

use crate::extensions::arg_substitution::{parse_argument_names, ArgNamesInput};
use crate::extensions::frontmatter::{
    coerce_description_to_string, extract_description_from_markdown, parse_frontmatter,
    parse_optional_bool_field, parse_tool_list, Frontmatter, ToolListInput,
};
use crate::fs::node_path::{resolve_runtime, runtime_string_from_native};
use crate::fs::paths::config_home;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, NodeBufferUtf8, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode,
    RuntimeEncode, RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimePath, RuntimeString,
};
use crate::schemas::effort::{parse_effort, EffortValue};
use crate::types::json::JsonObject;
use regex::Regex;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::LazyLock;

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

/// How a skill is run when invoked: expanded inline into the current turn, or
/// forked as an isolated sub-agent with its own context window and token budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillContext {
    /// Expand the skill into the parent turn (the default).
    Inline,
    /// Run the skill as an isolated sub-agent.
    Fork,
}

impl RuntimeEncode for SkillContext {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Inline => "inline",
            Self::Fork => "fork",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for SkillContext {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("inline", &[]),
                RuntimeEnumVariant::new("fork", &[]),
            ],
            &[Self::Inline, Self::Fork],
        )
    }
}

/// A loaded skill: the parsed metadata from a `SKILL.md` frontmatter block plus
/// the markdown body and the source path. Runtime-visible strings retain exact
/// JavaScript UTF-16 identity; the handwritten codec emits declaration-ordered
/// camelCase keys matching the engine wire representation.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Skill {
    pub name: RuntimeString,
    pub description: RuntimeString,
    /// Compact catalog blurb. When present the skill listing shows THIS instead of
    /// `description`, so a long description does not have to compete with every
    /// other skill's location for the listing's budget. Absent means the listing
    /// falls back to `description` (plus `when_to_use`) as before.
    pub short_description: Option<RuntimeString>,
    pub when_to_use: Option<RuntimeString>,
    pub argument_hint: Option<RuntimeString>,
    pub allowed_tools: Option<Vec<RuntimeString>>,
    /// Model override: applied to the parent turn when inline, or the sub-agent
    /// when forked.
    pub model: Option<RuntimeString>,
    /// Reasoning-effort override, applied to the parent turn when inline.
    pub effort: Option<EffortValue>,
    /// Optional local orchestration selection.
    pub ultra_mode: Option<bool>,
    /// `inline` (default) runs the skill in the main loop; `fork` runs it as a
    /// sub-agent.
    pub context: Option<SkillContext>,
    /// Sub-agent type to fork as when `context: fork`. Absent means
    /// general-purpose.
    pub agent: Option<RuntimeString>,
    /// Named-argument positions for `$name` substitution.
    pub arguments: Option<Vec<RuntimeString>>,
    /// Exposed as a user slash command. Absent means the default `true`.
    pub user_invocable: Option<bool>,
    /// When true, the model's skill tool cannot invoke it — user slash only.
    /// Absent means `false`.
    pub disable_model_invocation: Option<bool>,
    /// Glob patterns the skill applies to — a context-gating hint for the host.
    pub paths: Option<Vec<RuntimeString>>,
    /// Shell for inline-bash directives in the body.
    pub shell: Option<RuntimeString>,
    /// Plugin namespace when loaded from an enabled plugin (`<namespace>:<name>` in listings).
    pub plugin_namespace: Option<RuntimeString>,
    pub body: RuntimeString,
    pub path: RuntimeString,
}

impl RuntimeEncode for Skill {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("description", &self.description)?;
        if let Some(value) = &self.short_description {
            object.field("shortDescription", value)?;
        }
        if let Some(value) = &self.when_to_use {
            object.field("whenToUse", value)?;
        }
        if let Some(value) = &self.argument_hint {
            object.field("argumentHint", value)?;
        }
        if let Some(value) = &self.allowed_tools {
            object.field("allowedTools", value)?;
        }
        if let Some(value) = &self.model {
            object.field("model", value)?;
        }
        if let Some(value) = &self.ultra_mode {
            object.field("ultraMode", value)?;
        }
        if let Some(value) = &self.effort {
            object.field("effort", value)?;
        }
        if let Some(value) = &self.context {
            object.field("context", value)?;
        }
        if let Some(value) = &self.agent {
            object.field("agent", value)?;
        }
        if let Some(value) = &self.arguments {
            object.field("arguments", value)?;
        }
        if let Some(value) = &self.user_invocable {
            object.field("userInvocable", value)?;
        }
        if let Some(value) = &self.disable_model_invocation {
            object.field("disableModelInvocation", value)?;
        }
        if let Some(value) = &self.paths {
            object.field("paths", value)?;
        }
        if let Some(value) = &self.shell {
            object.field("shell", value)?;
        }
        object.field("body", &self.body)?;
        object.field("path", &self.path)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for Skill {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let mut effort = object.optional("effort", &[])?;
            let mut ultra_mode = object.optional("ultraMode", &[])?;
            crate::schemas::effort::normalize_legacy_ultra(&mut effort, &mut ultra_mode);
            Ok(Self {
                name: object.required("name", &[])?,
                description: object.required("description", &[])?,
                short_description: object.optional("shortDescription", &[])?,
                when_to_use: object.optional("whenToUse", &[])?,
                argument_hint: object.optional("argumentHint", &[])?,
                allowed_tools: object.optional("allowedTools", &[])?,
                model: object.optional("model", &[])?,
                effort,
                ultra_mode,
                context: object.optional("context", &[])?,
                agent: object.optional("agent", &[])?,
                arguments: object.optional("arguments", &[])?,
                user_invocable: object.optional("userInvocable", &[])?,
                disable_model_invocation: object.optional("disableModelInvocation", &[])?,
                paths: object.optional("paths", &[])?,
                shell: object.optional("shell", &[])?,
                plugin_namespace: None,
                body: object.required("body", &[])?,
                path: object.required("path", &[])?,
            })
        })
    }
}

/// Model-facing skill identifier in the `<skill-catalog>` listing.
///
/// Plugin-contributed skills render as `<plugin_namespace>:<name>` unless `name`
/// already starts with that prefix; standalone skills render as `<name>`.
pub fn catalog_skill_name(skill: &Skill) -> RuntimeString {
    if let Some(ns) = &skill.plugin_namespace {
        if !ns.is_empty() {
            let prefix = ns.concat(&RuntimeString::from(":"));
            if !skill
                .name
                .starts_with(&prefix.try_to_utf8().unwrap_or_else(|_| String::from(":")))
            {
                return prefix.concat(&skill.name);
            }
        }
    }
    skill.name.clone()
}

/// Resolve a skill by either its `<plugin_namespace>:<name>` listing identifier,
/// its bare `name`, or a `<plugin@marketplace>:<name>` qualified identifier.
pub fn resolve_skill_by_name<'a>(
    skills: &'a [Skill],
    requested: &RuntimeString,
) -> Option<&'a Skill> {
    if let Some(exact_catalog) = skills.iter().find(|s| catalog_skill_name(s) == *requested) {
        return Some(exact_catalog);
    }
    if let Some(exact_bare) = skills.iter().find(|s| s.name == *requested) {
        return Some(exact_bare);
    }
    let req_utf8 = requested.try_to_utf8().ok()?;
    let (raw_ns, bare_name) = req_utf8.split_once(':')?;
    let normalized_ns = raw_ns
        .split_once('@')
        .map(|(plugin, _)| plugin)
        .unwrap_or(raw_ns)
        .trim();
    let bare_name = bare_name.trim();
    if normalized_ns.is_empty() || bare_name.is_empty() {
        return None;
    }
    let ns_rt = RuntimeString::from(normalized_ns);
    let bare_rt = RuntimeString::from(bare_name);
    skills
        .iter()
        .find(|s| s.plugin_namespace.as_ref() == Some(&ns_rt) && s.name == bare_rt)
        .or_else(|| {
            (normalized_ns == bare_name)
                .then(|| skills.iter().find(|s| s.name == bare_rt))
                .flatten()
        })
}

const SKILL_FILE: &str = "SKILL.md";

/// Whether the MODEL may reach this skill through its `skill` tool.
///
/// The one predicate for that question, used by both places that have to answer
/// it: the listing the model reads, and the lookup that runs a name the model
/// sent. Those two must never disagree — a skill named in the listing but
/// refused at the call site wastes a turn on a tool error, and a skill missing
/// from the listing but still callable is a gate that only looks closed. They
/// were two separate copies of `!disable_model_invocation.unwrap_or(false)`,
/// which is exactly the shape that drifts when one side gets edited.
///
/// `None` means yes: a skill that says nothing about model invocation is
/// model-callable, which is what almost every skill wants.
pub fn is_model_invocable(skill: &Skill) -> bool {
    !skill.disable_model_invocation.unwrap_or(false)
}

/// The subset of `skills` the model may invoke.
pub fn model_invocable(skills: Vec<Skill>) -> Vec<Skill> {
    skills.into_iter().filter(is_model_invocable).collect()
}

// ---- frontmatter validation ------------------------------------------------

/// A frontmatter value that is either a delimited string or a list of strings.
enum StrOrList {
    Str(String),
    List(Vec<String>),
}

/// A frontmatter value that is either a string or a boolean.
enum StrOrBool {
    Str(String),
    Bool(bool),
}

/// The validated, strongly typed subset of the skill frontmatter. Unknown keys
/// are ignored; `description` / `name` / `argument-hint` / `effort` are kept as
/// raw values (they never fail validation — a bad value degrades the field).
struct SkillFm {
    description: Option<Value>,
    name: Option<Value>,
    short_description: Option<String>,
    short_description_hyphen: Option<String>,
    when_to_use: Option<String>,
    when_to_use_hyphen: Option<String>,
    argument_hint: Option<Value>,
    model: Option<String>,
    effort: Option<Value>,
    ultra_mode: Option<bool>,
    allowed_tools: Option<StrOrList>,
    context: Option<SkillContext>,
    agent: Option<String>,
    arguments: Option<StrOrList>,
    user_invocable: Option<StrOrBool>,
    disable_model_invocation: Option<StrOrBool>,
    paths: Option<StrOrList>,
    shell: Option<String>,
}

/// Read an optional string field: absent → `None`, a string → the string,
/// anything else → a validation error (which drops the skill).
fn get_string_field(data: &JsonObject, key: &str) -> Result<Option<String>, String> {
    match data.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(format!("\"{key}\": expected string")),
    }
}

/// Read an optional string-or-string-list field.
fn get_str_or_list_field(data: &JsonObject, key: &str) -> Result<Option<StrOrList>, String> {
    match data.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(StrOrList::Str(s.clone()))),
        Some(Value::Array(arr)) => {
            let mut list = Vec::with_capacity(arr.len());
            for v in arr {
                match v {
                    Value::String(s) => list.push(s.clone()),
                    _ => return Err(format!("\"{key}\": expected string or string[]")),
                }
            }
            Ok(Some(StrOrList::List(list)))
        }
        Some(_) => Err(format!("\"{key}\": expected string or string[]")),
    }
}

/// Read an optional string-or-boolean field.
fn get_str_or_bool_field(data: &JsonObject, key: &str) -> Result<Option<StrOrBool>, String> {
    match data.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(StrOrBool::Str(s.clone()))),
        Some(Value::Bool(b)) => Ok(Some(StrOrBool::Bool(*b))),
        Some(_) => Err(format!("\"{key}\": expected string or boolean")),
    }
}

/// Validate the skill frontmatter shape. A bad strongly typed field fails the
/// whole parse, so the skill is skipped rather than partially loaded. The
/// leniently typed fields (`description`, `name`, `argument-hint`, `effort`) are
/// carried as raw values and coerced/degraded separately.
fn validate_skill_frontmatter(data: &JsonObject) -> Result<SkillFm, String> {
    let short_description = get_string_field(data, "short_description")?;
    let short_description_hyphen = get_string_field(data, "short-description")?;
    let when_to_use = get_string_field(data, "when_to_use")?;
    let when_to_use_hyphen = get_string_field(data, "when-to-use")?;
    let model = get_string_field(data, "model")?;
    let allowed_tools = get_str_or_list_field(data, "allowed-tools")?;
    let context = match data.get("context") {
        None => None,
        Some(Value::String(s)) => match s.as_str() {
            "inline" => Some(SkillContext::Inline),
            "fork" => Some(SkillContext::Fork),
            _ => return Err("\"context\": invalid enum value".to_string()),
        },
        Some(_) => return Err("\"context\": expected string".to_string()),
    };
    let agent = get_string_field(data, "agent")?;
    let arguments = get_str_or_list_field(data, "arguments")?;
    let user_invocable = get_str_or_bool_field(data, "user-invocable")?;
    let disable_model_invocation = get_str_or_bool_field(data, "disable-model-invocation")?;
    let paths = get_str_or_list_field(data, "paths")?;
    let shell = get_string_field(data, "shell")?;
    Ok(SkillFm {
        description: data.get("description").cloned(),
        name: data.get("name").cloned(),
        short_description,
        short_description_hyphen,
        when_to_use,
        when_to_use_hyphen,
        argument_hint: data.get("argument-hint").cloned(),
        model,
        effort: data.get("effort").cloned(),
        ultra_mode: parse_optional_bool_field(data, "ultraMode")?,
        allowed_tools,
        context,
        agent,
        arguments,
        user_invocable,
        disable_model_invocation,
        paths,
        shell,
    })
}

// ---- field coercion helpers ------------------------------------------------

/// Coerce a frontmatter boolean: ONLY a boolean `true` or the literal string
/// `"true"` is true (`"1"`/`"yes"`/`"on"` are NOT). Absent falls back to `dflt`.
fn coerce_bool(value: &Option<StrOrBool>, dflt: bool) -> bool {
    match value {
        None => dflt,
        Some(StrOrBool::Bool(b)) => *b,
        Some(StrOrBool::Str(s)) => s == "true",
    }
}

/// Stringify a frontmatter value the way a scalar coercion would: a string is
/// itself; an array joins its elements with `,` (a null element contributes an
/// empty string); a number/boolean is its textual form. Objects and a bare null
/// map to their conventional textual forms.
fn js_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(a) => a
            .iter()
            .map(|e| match e {
                Value::Null => String::new(),
                other => js_string(other),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_string(),
    }
}

/// Adapt a validated string-or-list into the argument-names input shape.
fn str_or_list_to_arg_names(t: StrOrList) -> ArgNamesInput {
    match t {
        StrOrList::Str(s) => ArgNamesInput::Str(s),
        StrOrList::List(l) => ArgNamesInput::List(l),
    }
}

/// Adapt a validated string-or-list into the tool-list input shape.
fn str_or_list_to_tool_list(t: StrOrList) -> ToolListInput {
    match t {
        StrOrList::Str(s) => ToolListInput::Str(RuntimeString::from(s)),
        StrOrList::List(l) => ToolListInput::List(l.into_iter().map(RuntimeString::from).collect()),
    }
}

// ---- paths normalization ---------------------------------------------------

/// Matches a leading `{a,b}` brace alternation: `prefix{alts}suffix`.
static BRACE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([^{]*)\{([^}]+)\}(.*)$").unwrap());

/// Recursively expand a `{a,b}` brace alternation into separate patterns:
/// `src/*.{ts,tsx}` → `["src/*.ts", "src/*.tsx"]`. A gitignore matcher treats
/// `{…}` literally, so this expansion is what makes a brace `paths` pattern
/// actually match.
fn expand_braces(pattern: &str) -> Vec<String> {
    let caps = match BRACE_RE.captures(pattern) {
        Some(c) => c,
        None => return vec![pattern.to_string()],
    };
    let prefix = caps.get(1).map(|m| m.as_str()).unwrap_or("");
    let group = caps.get(2).map(|m| m.as_str()).unwrap_or("");
    let suffix = caps.get(3).map(|m| m.as_str()).unwrap_or("");
    group
        .split(',')
        .flat_map(|alt| expand_braces(&format!("{}{}{}", prefix, alt.trim(), suffix)))
        .collect()
}

/// Split a single frontmatter path string by COMMA, respecting `{a,b}` brace
/// groups, then expand each brace alternation.
fn split_path_string(input: &str) -> Vec<String> {
    let mut parts: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut brace_depth: i32 = 0;
    for ch in input.chars() {
        if ch == '{' {
            brace_depth += 1;
            current.push(ch);
        } else if ch == '}' {
            brace_depth -= 1;
            current.push(ch);
        } else if ch == ',' && brace_depth == 0 {
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                parts.push(trimmed.to_string());
            }
            current.clear();
        } else {
            current.push(ch);
        }
    }
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        parts.push(trimmed.to_string());
    }
    parts.iter().flat_map(|p| expand_braces(p)).collect()
}

/// Split a frontmatter `paths` value (string or list). A list is flat-mapped
/// over its elements; each element is split as a string.
fn split_path_in_frontmatter(input: StrOrList) -> Vec<String> {
    match input {
        StrOrList::List(list) => list.iter().flat_map(|s| split_path_string(s)).collect(),
        StrOrList::Str(s) => split_path_string(&s),
    }
}

/// Normalize the `paths` field: comma-split (brace-aware), strip a trailing
/// `/**` (a directory and its contents match either way), drop empties, and
/// treat an all-`**` (match-everything) set as "no paths" → `None`. A
/// non-`None` result makes the skill conditional (hidden from the model listing
/// until a matching file is touched).
fn parse_skill_paths(value: Option<StrOrList>) -> Option<Vec<String>> {
    let value = value?;
    let patterns: Vec<String> = split_path_in_frontmatter(value)
        .into_iter()
        .map(|p| {
            if p.ends_with("/**") {
                p[..p.len() - 3].to_string()
            } else {
                p
            }
        })
        .filter(|p| !p.is_empty())
        .collect();
    if patterns.is_empty() || patterns.iter().all(|p| p == "**") {
        return None;
    }
    Some(patterns)
}

// ---- POSIX path join -------------------------------------------------------

/// POSIX `path.join`: concatenate the non-empty segments with `/` and normalize.
fn join_posix(parts: &[&str]) -> String {
    let mut joined: Option<String> = None;
    for part in parts {
        if part.is_empty() {
            continue;
        }
        joined = Some(match joined {
            None => (*part).to_string(),
            Some(j) => format!("{j}/{part}"),
        });
    }
    match joined {
        None => ".".to_string(),
        Some(j) => normalize_posix(&j),
    }
}

/// Normalize a POSIX path: collapse `.`/`..` and duplicate slashes, preserving a
/// leading `/` (absolute) and a trailing `/`.
fn normalize_posix(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let is_abs = path.starts_with('/');
    let trailing = path.ends_with('/');
    let mut out: Vec<&str> = Vec::new();
    for seg in path.split('/') {
        match seg {
            "" | "." => {}
            ".." => match out.last() {
                Some(&last) if last != ".." => {
                    out.pop();
                }
                _ => {
                    if !is_abs {
                        out.push("..");
                    }
                }
            },
            s => out.push(s),
        }
    }
    let mut joined = out.join("/");
    if joined.is_empty() {
        if is_abs {
            return "/".to_string();
        }
        return if trailing {
            "./".to_string()
        } else {
            ".".to_string()
        };
    }
    if trailing {
        joined.push('/');
    }
    if is_abs {
        format!("/{joined}")
    } else {
        joined
    }
}

// ---- loading ---------------------------------------------------------------

/// Read and validate a single `SKILL.md`, returning a [`Skill`] or `None`.
pub async fn load_one_skill(skill_path: &str, default_name: &str) -> Option<Skill> {
    load_one_skill_with_default(skill_path, RuntimeString::from(default_name)).await
}

async fn load_one_skill_with_default(
    skill_path: &str,
    default_name: RuntimeString,
) -> Option<Skill> {
    let raw = match tokio::fs::read(skill_path).await {
        Ok(bytes) => NodeBufferUtf8::decode(&bytes),
        Err(_) => return None,
    };

    let raw = skill_file_text_to_frontmatter_host(&raw);
    let Frontmatter { data, body } = parse_frontmatter(&raw);
    let fm = match validate_skill_frontmatter(&data) {
        Ok(fm) => fm,
        Err(reason) => {
            tracing::warn!(
                "[extensions] skipping invalid skill at {}: {}",
                skill_path,
                reason
            );
            return None;
        }
    };

    // Stringify name (falling back to the directory name when absent).
    let name = match &fm.name {
        Some(v) if !v.is_null() => RuntimeString::from(js_string(v)),
        _ => default_name,
    };
    // Coerce the description, else fall back to the markdown H1/first line (never
    // drops the skill).
    let description = coerce_description_to_string(fm.description.as_ref())
        .unwrap_or_else(|| extract_description_from_markdown(&body, "Skill"));

    let mut skill = Skill {
        name,
        description: RuntimeString::from(description),
        short_description: None,
        when_to_use: None,
        argument_hint: None,
        allowed_tools: None,
        model: None,
        effort: None,
        ultra_mode: None,
        context: None,
        agent: None,
        arguments: None,
        user_invocable: Some(coerce_bool(&fm.user_invocable, true)),
        disable_model_invocation: Some(coerce_bool(&fm.disable_model_invocation, false)),
        paths: None,
        shell: None,
        plugin_namespace: None,
        body: RuntimeString::from(body),
        path: RuntimeString::from(skill_path),
    };

    // `when_to_use` (underscore) is preferred; the hyphen form is accepted too.
    let when_to_use = fm
        .when_to_use
        .clone()
        .or_else(|| fm.when_to_use_hyphen.clone());
    if let Some(w) = when_to_use {
        skill.when_to_use = Some(RuntimeString::from(w));
    }
    // Same two-spelling tolerance as `when_to_use`. An EMPTY short description is
    // treated as absent so the listing falls back to the full description rather
    // than rendering a blank entry.
    let short_description = fm
        .short_description
        .clone()
        .or_else(|| fm.short_description_hyphen.clone())
        .filter(|value| !value.trim().is_empty());
    if let Some(value) = short_description {
        skill.short_description = Some(RuntimeString::from(value));
    }
    if let Some(v) = &fm.argument_hint {
        if !v.is_null() {
            skill.argument_hint = Some(RuntimeString::from(js_string(v)));
        }
    }
    if let Some(tools) = parse_tool_list(fm.allowed_tools.map(str_or_list_to_tool_list)) {
        skill.allowed_tools = Some(tools);
    }
    // `model: inherit` is the sentinel for "use the parent model" → leave unset.
    if let Some(m) = fm.model {
        if m != "inherit" {
            skill.model = Some(RuntimeString::from(m));
        }
    }
    // Route through the effort parser: a numeric/invalid value degrades the
    // FIELD, not the whole skill.
    skill.ultra_mode = fm.ultra_mode;
    if let Some(e) = fm.effort.as_ref().and_then(parse_effort) {
        skill.effort = Some(e);
    }
    crate::schemas::effort::normalize_legacy_ultra(&mut skill.effort, &mut skill.ultra_mode);
    if let Some(c) = fm.context {
        skill.context = Some(c);
    }
    if let Some(a) = fm.agent {
        skill.agent = Some(RuntimeString::from(a));
    }
    let arg_names = parse_argument_names(fm.arguments.map(str_or_list_to_arg_names));
    if !arg_names.is_empty() {
        skill.arguments = Some(arg_names.into_iter().map(RuntimeString::from).collect());
    }
    if let Some(p) = parse_skill_paths(fm.paths) {
        skill.paths = Some(p.into_iter().map(RuntimeString::from).collect());
    }
    if let Some(sh) = fm.shell {
        skill.shell = Some(RuntimeString::from(sh));
    }
    Some(skill)
}

/// Node's complete-buffer UTF-8 decoder has already replaced malformed byte
/// sequences, so the scalar-only frontmatter parser projection is strict and
/// cannot discard an exact runtime code unit.
fn skill_file_text_to_frontmatter_host(value: &RuntimeString) -> String {
    value
        .try_to_utf8()
        .expect("NodeBufferUtf8 skill text is Unicode scalar data")
}

/// Named Node/V8 projection used only to hand a logical directory-entry/path
/// operand to the current scalar filesystem APIs.
fn skill_filesystem_operand_host(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8())
        .expect("Node UTF-8 skill path projection is valid UTF-8")
}

/// Load every skill in `dir`, and report the directories that looked like skills
/// but did not load. A skill is a subdirectory containing a `SKILL.md`. Invalid
/// skills are skipped rather than throwing, so a single malformed skill cannot
/// break discovery — which is correct, and is also why a broken skill is
/// otherwise invisible. A missing `dir` yields empty lists.
pub async fn load_skills_from_dir_reporting(
    dir: &str,
) -> (
    Vec<Skill>,
    Vec<crate::extensions::failure::ExtensionFailure>,
) {
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return (Vec::new(), Vec::new()),
    };
    let mut skills: Vec<Skill> = Vec::new();
    let mut skipped: Vec<crate::extensions::failure::ExtensionFailure> = Vec::new();
    loop {
        let entry = match rd.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(_) => break,
        };
        // Directory check WITHOUT following symlinks (a symlink is not a dir).
        let is_dir = match entry.file_type().await {
            Ok(ft) => ft.is_dir(),
            Err(_) => false,
        };
        if !is_dir {
            continue;
        }
        let runtime_name = runtime_string_from_native(&entry.file_name());
        let name = skill_filesystem_operand_host(&runtime_name);
        let skill_path = join_posix(&[dir, &name, SKILL_FILE]);
        if let Some(skill) = load_one_skill_with_default(&skill_path, runtime_name).await {
            skills.push(skill);
        } else {
            // A directory that looks like a skill but did not load is the case a
            // user cannot see: discovery just returns one fewer skill. Record it
            // so a caller that wants to report it can, without changing what
            // discovery returns.
            skipped.push(crate::extensions::failure::ExtensionFailure::new(
                crate::extensions::failure::ExtensionFailureKind::Skill,
                RuntimeString::from(name.clone()),
                RuntimeString::from("SKILL.md missing or invalid"),
            ));
        }
    }
    (skills, skipped)
}

/// Load every skill in `dir`, discarding the record of what was skipped. Kept as
/// the plain entry point so callers that only want skills are unaffected.
pub async fn load_skills_from_dir(dir: &str) -> Vec<Skill> {
    load_skills_from_dir_reporting(dir).await.0
}

/// The project-scoped config directory a skill root sits under.
const PROJECT_CONFIG_DIR: &str = ".klaus";
/// The directory name holding skill subdirectories under either root.
const SKILLS_DIR: &str = "skills";

/// The standard skill search roots, in precedence order:
///   1. `${cwd}/.klaus/skills`   (project)
///   2. `<config_home>/skills`   (user)
///
/// Discovery and the system prompt's `# Environment` block both read this, so
/// the layout the model is told about is the layout skills are actually loaded
/// from — a second copy of these segments could drift from the loader.
pub fn skill_search_roots(cwd: &str) -> [String; 2] {
    [
        join_posix(&[cwd, PROJECT_CONFIG_DIR, SKILLS_DIR]),
        join_posix(&[&config_home(), SKILLS_DIR]),
    ]
}

/// [`skill_search_roots`] in the runtime path domain, for callers holding a
/// `RuntimePath` cwd that may not project to a host string.
pub fn skill_search_roots_exact(cwd: &RuntimePath) -> [RuntimePath; 2] {
    let user_home = RuntimePath::from(config_home());
    [
        resolve_runtime(
            cwd,
            &[
                cwd.clone(),
                RuntimePath::from(PROJECT_CONFIG_DIR),
                RuntimePath::from(SKILLS_DIR),
            ],
        )
        .expect("an explicit skill cwd cannot fail resolution"),
        resolve_runtime(
            &user_home,
            &[user_home.clone(), RuntimePath::from(SKILLS_DIR)],
        )
        .expect("the configured skill home cannot fail resolution"),
    ]
}

/// Discover skills across the standard search roots ([`skill_search_roots`]),
/// then each of `extra_dirs` (caller-supplied).
///
/// Skills are deduped by `name` with first-wins semantics, so a project skill
/// shadows a user skill of the same name.
pub async fn discover_skills(cwd: &str, extra_dirs: &[String]) -> Vec<Skill> {
    let mut dirs: Vec<String> = Vec::from(skill_search_roots(cwd));
    dirs.extend(extra_dirs.iter().cloned());

    let mut seen: HashSet<RuntimeString> = HashSet::new();
    let mut result: Vec<Skill> = Vec::new();
    for dir in &dirs {
        let skills = load_skills_from_dir(dir).await;
        for skill in skills {
            if seen.contains(&skill.name) {
                continue;
            }
            seen.insert(skill.name.clone());
            result.push(skill);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn skill_frontmatter_preserves_raw_ultra_and_explicit_false() {
        for (extra, expected) in [("", true), ("ultraMode: false\n", false)] {
            let content = format!("---\ndescription: Inspect the task\neffort: ultra\n{extra}---\nReview the workspace.");
            let skill = load_content(&content, "mode").await.unwrap();
            assert_eq!(skill.effort, Some(EffortValue::Level(EffortLevel::Ultra)));
            assert_eq!(skill.ultra_mode, Some(expected));
        }
    }
    use super::*;
    use crate::schemas::effort::EffortLevel;
    use crate::test_support::CONFIG_ENV_LOCK as ENV_LOCK;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The model-invocation gate.
    ///
    /// This is the whole reason a skill can be the USER's to invoke and not the
    /// model's. Nothing else pins it: the two callers hold a closure over
    /// `get_skills`, so a refactor that drops the filter leaves every other test
    /// green while the gate silently opens.
    #[test]
    fn only_disable_model_invocation_closes_the_gate() {
        let named = |name: &str, flag: Option<bool>| Skill {
            name: RuntimeString::from(name),
            disable_model_invocation: flag,
            ..Default::default()
        };

        // Absent means model-callable. Almost every skill is this case, so a
        // predicate that got it backwards would hide every skill from the model.
        assert!(is_model_invocable(&named("pdf", None)));
        assert!(is_model_invocable(&named("pdf", Some(false))));
        assert!(!is_model_invocable(&named("canvas", Some(true))));

        // `user_invocable` is a DIFFERENT axis: it controls the slash menu and
        // must not affect what the model can reach. A skill that is not offered
        // as a slash command is still the model's to call.
        let model_only = Skill {
            user_invocable: Some(false),
            ..named("internal", None)
        };
        assert!(is_model_invocable(&model_only));

        let kept = model_invocable(vec![
            named("pdf", None),
            named("canvas", Some(true)),
            named("chart", Some(false)),
        ]);
        assert_eq!(
            kept.iter().map(|s| s.name.clone()).collect::<Vec<_>>(),
            vec![RuntimeString::from("pdf"), RuntimeString::from("chart")],
            "only the gated skill is withheld, and order is preserved"
        );
    }

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_dir(tag: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let uniq = format!(
            "ae-skills-loader-{tag}-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            n
        );
        let dir = std::env::temp_dir().join(uniq);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_skill(root: &Path, name: &str, content: &str) {
        let d = root.join(name);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("SKILL.md"), content).unwrap();
    }

    fn path_str(path: &Path) -> &str {
        path.to_str().expect("test paths are Unicode scalar data")
    }

    fn skill_md(desc: &str) -> String {
        ["---", &format!("description: {desc}"), "---", "body"].join("\n")
    }

    async fn load_content(content: &str, default_name: &str) -> Option<Skill> {
        let dir = unique_dir("one");
        let sd = dir.join(default_name);
        std::fs::create_dir_all(&sd).unwrap();
        let path = sd.join("SKILL.md");
        std::fs::write(&path, content).unwrap();
        load_one_skill(path_str(&path), default_name).await
    }

    fn runtime_strings(values: &[&str]) -> Vec<RuntimeString> {
        values
            .iter()
            .map(|value| RuntimeString::from(*value))
            .collect()
    }

    struct EnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(value: &str) -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let prev = std::env::var("KLAUS_CONFIG_DIR").ok();
            crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", value);
            EnvGuard { _lock: lock, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => {
                    crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", v)
                }
                None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
            }
        }
    }

    /// The compact catalog blurb is optional and accepts the same two spellings as
    /// `when_to_use`; a blank one is absent so the listing falls back to the full
    /// description instead of rendering an empty entry.
    #[tokio::test]
    async fn short_description_loads_from_either_spelling() {
        for key in ["short_description", "short-description"] {
            let content = [
                "---",
                "description: Does things",
                &format!("{key}: Does things briefly"),
                "---",
                "Body line",
            ]
            .join("\n");
            let s = load_content(&content, "x").await.unwrap();
            assert_eq!(
                s.short_description.as_ref(),
                Some(&RuntimeString::from("Does things briefly")),
                "{key}"
            );
        }

        let content = [
            "---",
            "description: Does things",
            "short_description: '   '",
            "---",
            "Body line",
        ]
        .join("\n");
        let s = load_content(&content, "x").await.unwrap();
        assert_eq!(s.short_description, None);
    }

    #[tokio::test]
    async fn loads_a_full_skill() {
        let content = [
            "---",
            "name: my-skill",
            "description: Does things",
            "short_description: Does things briefly",
            "when_to_use: when needed",
            "argument-hint: <file>",
            "allowed-tools: Read, Write",
            "model: gpt",
            "effort: high",
            "context: fork",
            "agent: general",
            "arguments: foo bar",
            "user-invocable: false",
            "disable-model-invocation: true",
            "paths: src/**",
            "shell: bash",
            "---",
            "Body line",
        ]
        .join("\n");
        let dir = unique_dir("full");
        write_skill(&dir, "my-skill", &content);
        let path = dir.join("my-skill").join("SKILL.md");
        let s = load_one_skill(path_str(&path), "my-skill").await.unwrap();
        assert_eq!(s.name, "my-skill");
        assert_eq!(s.description, "Does things");
        assert_eq!(
            s.short_description.as_ref(),
            Some(&RuntimeString::from("Does things briefly"))
        );
        assert_eq!(
            s.when_to_use.as_ref(),
            Some(&RuntimeString::from("when needed"))
        );
        assert_eq!(
            s.argument_hint.as_ref(),
            Some(&RuntimeString::from("<file>"))
        );
        assert_eq!(s.allowed_tools, Some(runtime_strings(&["Read", "Write"])));
        assert_eq!(s.model.as_ref(), Some(&RuntimeString::from("gpt")));
        assert_eq!(s.effort, Some(EffortValue::Level(EffortLevel::High)));
        assert_eq!(s.context, Some(SkillContext::Fork));
        assert_eq!(s.agent.as_ref(), Some(&RuntimeString::from("general")));
        assert_eq!(s.arguments, Some(runtime_strings(&["foo", "bar"])));
        assert_eq!(s.user_invocable, Some(false));
        assert_eq!(s.disable_model_invocation, Some(true));
        assert_eq!(s.paths, Some(runtime_strings(&["src"])));
        assert_eq!(s.shell.as_ref(), Some(&RuntimeString::from("bash")));
        assert_eq!(s.body, "Body line");
        assert_eq!(s.path, RuntimeString::from(path.to_str().unwrap()));
    }

    #[tokio::test]
    async fn missing_file_returns_none() {
        let s = load_one_skill("/definitely/not/here/SKILL.md", "x").await;
        assert!(s.is_none());
    }

    #[tokio::test]
    async fn skill_file_ingress_uses_node_complete_buffer_utf8_decoding() {
        let dir = unique_dir("invalid-utf8");
        let skill_dir = dir.join("x");
        std::fs::create_dir_all(&skill_dir).unwrap();
        let path = skill_dir.join("SKILL.md");
        std::fs::write(&path, b"left\xffright").unwrap();

        let skill = load_one_skill(path_str(&path), "x").await.unwrap();
        assert_eq!(skill.body, "left\u{fffd}right");
    }

    #[tokio::test]
    async fn invalid_context_drops_the_skill() {
        let content = ["---", "description: d", "context: nope", "---", "b"].join("\n");
        assert!(load_content(&content, "x").await.is_none());
    }

    #[tokio::test]
    async fn non_string_model_drops_the_skill() {
        let content = ["---", "description: d", "model:", "  - a", "---", "b"].join("\n");
        assert!(load_content(&content, "x").await.is_none());
    }

    #[tokio::test]
    async fn unknown_frontmatter_keys_do_not_drop_the_skill() {
        let content = [
            "---",
            "description: d",
            "x-extra: keepable metadata",
            "metadata:",
            "  - a",
            "  - b",
            "---",
            "b",
        ]
        .join("\n");
        let s = load_content(&content, "x").await.unwrap();
        assert_eq!(s.name, "x");
        assert_eq!(s.description, "d");
        assert_eq!(s.body, "b");
    }

    #[tokio::test]
    async fn name_falls_back_to_default() {
        let content = ["---", "description: d", "---", "b"].join("\n");
        let s = load_content(&content, "the-default").await.unwrap();
        assert_eq!(s.name, "the-default");
    }

    #[tokio::test]
    async fn name_is_coerced_from_a_list() {
        let content = [
            "---",
            "name:",
            "  - a",
            "  - b",
            "description: d",
            "---",
            "b",
        ]
        .join("\n");
        let s = load_content(&content, "def").await.unwrap();
        assert_eq!(s.name, "a,b");
    }

    #[tokio::test]
    async fn description_falls_back_to_markdown_header() {
        let content = ["---", "name: x", "---", "# Heading Title", "rest"].join("\n");
        let s = load_content(&content, "x").await.unwrap();
        assert_eq!(s.description, "Heading Title");
    }

    #[tokio::test]
    async fn description_falls_back_to_skill_literal_when_body_blank() {
        let content = ["---", "name: x", "---"].join("\n");
        let s = load_content(&content, "x").await.unwrap();
        assert_eq!(s.description, "Skill");
    }

    #[tokio::test]
    async fn model_inherit_is_unset() {
        let content = ["---", "description: d", "model: inherit", "---", "b"].join("\n");
        let s = load_content(&content, "x").await.unwrap();
        assert_eq!(s.model, None);
    }

    #[tokio::test]
    async fn custom_and_numeric_effort_are_preserved() {
        let content = ["---", "description: d", "effort: notaneffort", "---", "b"].join("\n");
        let s = load_content(&content, "x").await.unwrap();
        assert_eq!(s.effort, Some(EffortValue::Custom("notaneffort".into())));
        let content = ["---", "description: d", "effort: 80", "---", "b"].join("\n");
        let s = load_content(&content, "x").await.unwrap();
        assert_eq!(s.effort, Some(EffortValue::Number(80)));
    }

    #[tokio::test]
    async fn user_invocable_defaults_and_coercion() {
        let s = load_content(&["---", "description: d", "---", "b"].join("\n"), "x")
            .await
            .unwrap();
        assert_eq!(s.user_invocable, Some(true));
        assert_eq!(s.disable_model_invocation, Some(false));

        let s = load_content(
            &[
                "---",
                "description: d",
                "user-invocable: false",
                "disable-model-invocation: true",
                "---",
                "b",
            ]
            .join("\n"),
            "x",
        )
        .await
        .unwrap();
        assert_eq!(s.user_invocable, Some(false));
        assert_eq!(s.disable_model_invocation, Some(true));

        // Only the literal `true` is true; `yes` is false.
        let s = load_content(
            &["---", "description: d", "user-invocable: yes", "---", "b"].join("\n"),
            "x",
        )
        .await
        .unwrap();
        assert_eq!(s.user_invocable, Some(false));
    }

    #[tokio::test]
    async fn paths_all_globstar_is_none() {
        let content = ["---", "description: d", "paths: **", "---", "b"].join("\n");
        let s = load_content(&content, "x").await.unwrap();
        assert_eq!(s.paths, None);
    }

    #[tokio::test]
    async fn paths_brace_expansion_and_globstar_strip() {
        let content = [
            "---",
            "description: d",
            "paths: src/*.{ts,tsx}, docs/**",
            "---",
            "b",
        ]
        .join("\n");
        let s = load_content(&content, "x").await.unwrap();
        assert_eq!(
            s.paths,
            Some(runtime_strings(&["src/*.ts", "src/*.tsx", "docs"]))
        );
    }

    #[tokio::test]
    async fn arguments_drop_numeric_only_names() {
        let content = ["---", "description: d", "arguments: foo 1 bar", "---", "b"].join("\n");
        let s = load_content(&content, "x").await.unwrap();
        assert_eq!(s.arguments, Some(runtime_strings(&["foo", "bar"])));
    }

    #[test]
    fn skill_runtime_codec_uses_declaration_order_and_exact_utf16() {
        let exact = RuntimeString::from_utf16(vec![u16::from(b'x'), 0xd800, u16::from(b'y')]);
        let skill = Skill {
            name: exact.clone(),
            description: exact.clone(),
            short_description: Some(exact.clone()),
            when_to_use: Some(exact.clone()),
            argument_hint: Some(exact.clone()),
            allowed_tools: Some(vec![exact.clone()]),
            model: Some(exact.clone()),
            effort: Some(EffortValue::Level(EffortLevel::High)),
            ultra_mode: None,
            context: Some(SkillContext::Fork),
            agent: Some(exact.clone()),
            arguments: Some(vec![exact.clone()]),
            user_invocable: Some(false),
            disable_model_invocation: Some(true),
            paths: Some(vec![exact.clone()]),
            shell: Some(exact.clone()),
            plugin_namespace: None,
            body: exact.clone(),
            path: exact.clone(),
        };

        let encoded = runtime_codec::encode_value(&skill).unwrap();
        assert_eq!(
            encoded.as_object().unwrap().keys().collect::<Vec<_>>(),
            [
                "name",
                "description",
                "shortDescription",
                "whenToUse",
                "argumentHint",
                "allowedTools",
                "model",
                "effort",
                "context",
                "agent",
                "arguments",
                "userInvocable",
                "disableModelInvocation",
                "paths",
                "shell",
                "body",
                "path",
            ]
            .into_iter()
            .map(RuntimeString::from)
            .collect::<Vec<_>>()
        );
        for field in [
            "name",
            "description",
            "shortDescription",
            "whenToUse",
            "argumentHint",
            "model",
            "agent",
            "shell",
            "body",
            "path",
        ] {
            assert_eq!(
                encoded.get(field).and_then(RuntimeJson::as_str),
                Some(&exact)
            );
        }
        assert_eq!(
            runtime_codec::decode_value::<Skill>(&encoded).unwrap(),
            skill
        );
    }

    #[tokio::test]
    async fn load_skills_from_dir_skips_files_and_invalid() {
        let dir = unique_dir("dir");
        write_skill(&dir, "a", &skill_md("skill-a"));
        write_skill(&dir, "b", &skill_md("skill-b"));
        // A non-directory file at top level → skipped.
        std::fs::write(dir.join("notskill.md"), "hello").unwrap();
        // A directory without SKILL.md → skipped.
        std::fs::create_dir_all(dir.join("empty")).unwrap();
        // A directory with an invalid SKILL.md → skipped.
        write_skill(&dir, "bad", "---\ncontext: nope\n---\nbody");

        let skills = load_skills_from_dir(path_str(&dir)).await;
        let mut names: Vec<RuntimeString> = skills.iter().map(|s| s.name.clone()).collect();
        names.sort();
        assert_eq!(names, runtime_strings(&["a", "b"]));

        // The reporting variant returns the same skills PLUS a record of what was
        // skipped, so a broken skill stops being invisible. A plain file at top
        // level is not a skill directory and is not reported as a failure.
        let (reported_skills, skipped) = load_skills_from_dir_reporting(path_str(&dir)).await;
        assert_eq!(
            reported_skills.len(),
            skills.len(),
            "reporting must not change what loads"
        );
        let mut skipped_ids: Vec<String> = skipped
            .iter()
            .map(|failure| failure.id.try_to_utf8().unwrap())
            .collect();
        skipped_ids.sort();
        assert_eq!(
            skipped_ids,
            vec!["bad".to_string(), "empty".to_string()],
            "both the invalid and the SKILL.md-less directory are reported; a loose file is not"
        );
        assert!(skipped
            .iter()
            .all(|f| f.kind == crate::extensions::failure::ExtensionFailureKind::Skill));
    }

    #[tokio::test]
    async fn load_skills_from_dir_missing_dir_is_empty() {
        let dir = unique_dir("missing");
        let sub = dir.join("does-not-exist");
        let skills = load_skills_from_dir(path_str(&sub)).await;
        assert!(skills.is_empty());
    }

    #[tokio::test]
    async fn discover_skills_precedence_and_dedup() {
        let project = unique_dir("proj");
        let project_skills = project.join(".klaus").join("skills");
        std::fs::create_dir_all(&project_skills).unwrap();
        write_skill(&project_skills, "foo", &skill_md("project-foo"));
        let extra = unique_dir("extra");
        write_skill(&extra, "foo", &skill_md("extra-foo"));
        write_skill(&extra, "bar", &skill_md("extra-bar"));
        // An empty config home (no `skills` subdir) so the user root contributes
        // nothing.
        let cfg = unique_dir("cfg");
        let _g = EnvGuard::set(path_str(&cfg));

        let skills = discover_skills(path_str(&project), &[path_str(&extra).to_string()]).await;
        let mut got: Vec<(RuntimeString, RuntimeString)> = skills
            .iter()
            .map(|s| (s.name.clone(), s.description.clone()))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                (RuntimeString::from("bar"), RuntimeString::from("extra-bar")),
                // The project skill wins over the extra-dir skill of the same name.
                (
                    RuntimeString::from("foo"),
                    RuntimeString::from("project-foo")
                ),
            ]
        );
    }
}
