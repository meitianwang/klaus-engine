//! Discover and load plugin bundles from disk.
//!
//! A plugin is a directory carrying a `plugin.json` manifest (at
//! `.klaus-plugin/plugin.json`, `.codex-plugin/plugin.json`,
//! `.claude-plugin/plugin.json`, or the legacy root `plugin.json`). Its
//! `skills/`, `commands/`, `agents/`, `output-styles/`, and `hooks/hooks.json`
//! subdirectories are auto-detected ONLY when the manifest does not itself list
//! that component; a listed component is loaded from the manifest-declared paths
//! instead. `hooks` are ADDITIVE (the standard `hooks/hooks.json` plus any
//! manifest entries), while `mcpServers` / `lspServers` come only from the
//! manifest. Invalid plugins are skipped (with a warning) rather than aborting
//! discovery. Remote sources (git/npm/tarball) are handled by `remote.rs`.

use crate::extensions::agent_loader::{
    load_agent_from_file, load_agents_from_dir, AgentDefinition,
};
use crate::extensions::command_loader::{
    load_command_from_file, load_commands_from_dir, make_inline_command, CommandLoadOptions,
    InlineCommandMeta,
};
use crate::extensions::commands::Command;
use crate::extensions::hooks_loader::load_hooks_json;
use crate::extensions::output_styles::{
    load_output_style_from_file, load_output_styles_from_dir, OutputStyle,
};
use crate::extensions::skills::loader::{load_skills_from_dir, Skill};
use crate::hooks::types::Hook;
use crate::mcp::config_file::parse_mcp_server_entry;
use crate::mcp::types::McpServerConfig;
use crate::permission::bash::node_path::is_absolute;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, runtime_json, try_to_scalar_json, NodeBufferUtf8, RuntimeCodecError,
    RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeNumber, RuntimeObject, RuntimeObjectEncoder, RuntimeString,
};
use crate::types::json::JsonObject;
use serde_json::Value;
use std::collections::HashSet;

fn decode_provider_unit_enum<T: Clone>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .cloned()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginLoadIssueKind {
    InvalidJson,
    InvalidManifest,
    UnsupportedRuntimeCapability,
    DuplicateMcpServerSuppressed,
}

impl RuntimeEncode for PluginLoadIssueKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::InvalidJson => "invalid_json",
            Self::InvalidManifest => "invalid_manifest",
            Self::UnsupportedRuntimeCapability => "unsupported_runtime_capability",
            Self::DuplicateMcpServerSuppressed => "duplicate_mcp_server_suppressed",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for PluginLoadIssueKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("invalid_json", &[]),
                RuntimeEnumVariant::new("invalid_manifest", &[]),
                RuntimeEnumVariant::new("unsupported_runtime_capability", &[]),
                RuntimeEnumVariant::new("duplicate_mcp_server_suppressed", &[]),
            ],
            &[
                Self::InvalidJson,
                Self::InvalidManifest,
                Self::UnsupportedRuntimeCapability,
                Self::DuplicateMcpServerSuppressed,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginLoadIssue {
    pub kind: PluginLoadIssueKind,
    pub dir: RuntimeString,
    pub message: RuntimeString,
}

impl PluginLoadIssue {
    /// The same fact a failed hook reports: something the user installed is not
    /// working. The loader stays a pure function returning a report — whoever
    /// owns the report decides to announce it — so this is a conversion rather
    /// than a sink plumbed down here.
    ///
    /// The identity is the plugin's directory name, because that is what the user
    /// installed and what they would remove; the full path goes nowhere near a
    /// row label.
    pub fn to_extension_failure(&self) -> crate::extensions::failure::ExtensionFailure {
        use crate::extensions::failure::{ExtensionFailure, ExtensionFailureKind};
        let dir = self.dir.try_to_utf8().unwrap_or_default();
        let name = crate::permission::bash::node_path::basename(&dir);
        ExtensionFailure::new(
            ExtensionFailureKind::Plugin,
            RuntimeString::from(name),
            self.message.clone(),
        )
    }
}

impl RuntimeEncode for PluginLoadIssue {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("kind", &self.kind)?;
        object.field("dir", &self.dir)?;
        object.field("message", &self.message)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PluginLoadIssue {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                kind: object.required("kind", &[])?,
                dir: object.required("dir", &[])?,
                message: object.required("message", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginDisabledEntry {
    pub name: Option<RuntimeString>,
    pub dir: RuntimeString,
    pub reason: RuntimeString,
}

impl RuntimeEncode for PluginDisabledEntry {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("dir", &self.dir)?;
        object.field("reason", &self.reason)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PluginDisabledEntry {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                dir: object.required("dir", &[])?,
                reason: object.required("reason", &[])?,
            })
        })
    }
}

#[derive(Default)]
pub struct PluginLoadReport {
    pub enabled: Vec<LoadedPlugin>,
    pub disabled: Vec<PluginDisabledEntry>,
    pub errors: Vec<PluginLoadIssue>,
    pub warnings: Vec<PluginLoadIssue>,
}

/// A fully loaded plugin: all of its components plus the raw declarations the
/// host decides how to wire.
pub struct LoadedPlugin {
    pub name: String,
    pub version: Option<String>,
    /// UI/model-facing name. Falls back to `name` when absent.
    pub display_name: Option<String>,
    /// Prompt-safe manifest description with whitespace normalized.
    pub description: Option<String>,
    /// Stable namespace used for plugin-owned resources.
    pub plugin_namespace: String,
    /// Discovery keywords from the manifest.
    pub keywords: Vec<String>,
    /// Plugin dependencies declared by the manifest. Resolution is host policy.
    pub dependencies: Vec<String>,
    /// User-configurable option declarations, preserved for host UI/secret storage.
    pub user_config: JsonObject,
    /// Settings requested by the plugin, preserved for a host allowlist/merge policy.
    pub settings: JsonObject,
    pub dir: String,
    pub skills: Vec<Skill>,
    /// Flat-file slash commands from `commands/`.
    pub commands: Vec<Command>,
    /// Sub-agent definitions from `agents/`.
    pub agents: Vec<AgentDefinition>,
    /// Lifecycle hooks from `hooks/hooks.json` plus manifest additions.
    pub hooks: Vec<Hook>,
    /// Output styles from `output-styles/`.
    pub output_styles: Vec<OutputStyle>,
    /// MCP servers declared in the manifest (the host decides whether to connect them).
    pub mcp_servers: Vec<McpServerConfig>,
    /// LSP servers declared in the manifest, surfaced raw for the host to wire.
    pub lsp_servers: JsonObject,
    /// Runtime capabilities declared by the manifest and understood by this engine.
    pub runtime_capabilities: Vec<String>,
    /// Model/UI-facing capability labels declared by the manifest interface.
    pub interface_capabilities: Vec<String>,
    /// App/connector identifiers declared by the manifest.
    pub app_connectors: Vec<String>,
}

const MANIFEST_FILE: &str = "plugin.json";
const MANIFEST_DIR: &str = ".klaus-plugin";
const CODEX_MANIFEST_DIR: &str = ".codex-plugin";
const CLAUDE_MANIFEST_DIR: &str = ".claude-plugin";

// ---- Order-preserving JSON value -------------------------------------------
//
// The manifest is parsed into this recursive value BEFORE validation so that
// object key order (which several manifest fields depend on — the order MCP
// servers connect, the order commands and hook events are emitted) is preserved
// exactly as written. A plain JSON map does not retain insertion order here, so
// order is captured explicitly. Duplicate keys collapse to the FIRST position
// with the LAST value, matching how a JSON parser materializes an object.

#[derive(Debug, Clone)]
enum OrderedJson {
    Null,
    Bool(bool),
    Number(RuntimeNumber),
    Str(RuntimeString),
    Array(Vec<OrderedJson>),
    Object(Vec<(RuntimeString, OrderedJson)>),
}

impl OrderedJson {
    fn from_runtime(value: &RuntimeJson) -> Self {
        match value {
            RuntimeJson::Null => Self::Null,
            RuntimeJson::Bool(value) => Self::Bool(*value),
            RuntimeJson::Number(value) => Self::Number(*value),
            RuntimeJson::String(value) => Self::Str(value.clone()),
            RuntimeJson::Array(values) => {
                Self::Array(values.iter().map(Self::from_runtime).collect())
            }
            RuntimeJson::Object(values) => Self::Object(
                values
                    .entries()
                    .map(|(key, value)| (key, Self::from_runtime(value)))
                    .collect(),
            ),
        }
    }

    fn to_runtime(&self) -> RuntimeJson {
        match self {
            Self::Null => RuntimeJson::Null,
            Self::Bool(value) => RuntimeJson::Bool(*value),
            Self::Number(value) => RuntimeJson::Number(*value),
            Self::Str(value) => RuntimeJson::String(value.clone()),
            Self::Array(values) => {
                RuntimeJson::Array(values.iter().map(Self::to_runtime).collect())
            }
            Self::Object(entries) => {
                let mut object = RuntimeObject::new();
                for (key, value) in entries {
                    object.insert(key.clone(), value.to_runtime());
                }
                RuntimeJson::Object(object)
            }
        }
    }

    /// Project a validated scalar-only plugin configuration subtree into its
    /// legacy serde consumer without routing the manifest parser through serde.
    fn to_scalar_config_value(&self, field: &str) -> Result<Value, String> {
        let runtime = self.to_runtime();
        try_to_scalar_json(&runtime)
            .map_err(|error| format!("\"{field}\": non-scalar configuration JSON: {error}"))?;
        serde_json::from_slice(&runtime_json::stringify(&runtime).to_node_utf8())
            .map_err(|error| format!("\"{field}\": invalid scalar configuration JSON: {error}"))
    }

    /// Serialize back to exact JSON text, preserving ECMAScript property order.
    fn to_json_string(&self) -> RuntimeString {
        runtime_json::stringify(&self.to_runtime())
    }
}

impl RuntimeEncode for OrderedJson {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        out.emit(self.to_runtime())
    }
}

impl RuntimeDecode for OrderedJson {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        Ok(Self::from_runtime(value))
    }
}

// ---- Validated manifest shape ----------------------------------------------

/// A relative path, or an array of them.
enum RelPath {
    One(String),
    Many(Vec<String>),
}

impl RelPath {
    fn as_list(&self) -> Vec<&str> {
        match self {
            RelPath::One(s) => vec![s.as_str()],
            RelPath::Many(v) => v.iter().map(String::as_str).collect(),
        }
    }

    /// Whether this field is "present" for gating auto-detection: a non-empty
    /// string, or any array (even an empty one).
    fn is_truthy(&self) -> bool {
        match self {
            RelPath::One(s) => !s.is_empty(),
            RelPath::Many(_) => true,
        }
    }
}

/// The manifest `commands` field: a path / array of paths, or a NAME → metadata
/// map (the object-map form, order-preserving).
enum CommandsSpec {
    Paths(RelPath),
    Map(Vec<(String, CommandMetaEntry)>),
}

impl CommandsSpec {
    fn is_truthy(&self) -> bool {
        match self {
            CommandsSpec::Paths(rel) => rel.is_truthy(),
            CommandsSpec::Map(_) => true,
        }
    }
}

/// One entry of the `commands` object-map form. Exactly one of
/// `source`/`content` (treating an empty string as absent) is required, checked
/// during validation — a both/neither entry fails the whole manifest.
struct CommandMetaEntry {
    source: Option<String>,
    content: Option<String>,
    description: Option<String>,
    argument_hint: Option<String>,
    allowed_tools: Option<Vec<String>>,
}

/// The manifest `hooks` field: a path to another hooks JSON file, an array of
/// entries (each a path string or an inline event-map), or a bare inline
/// event-map object (the Codex marketplace form, e.g. `"hooks": {}`).
enum HooksSpec {
    Path(String),
    List(Vec<OrderedJson>),
    Inline(OrderedJson),
}

/// The manifest `mcpServers` field: an inline name -> config map, or a path to a
/// servers document inside the plugin (`{mcpServers:{…}}` or a bare map).
enum McpServersSpec {
    Inline(Vec<(String, Value)>),
    Path(String),
}

/// The conventional servers document at a plugin's root. cc-style plugins ship
/// this file and do NOT mention it in the manifest — every bundled marketplace
/// plugin that has MCP servers declares them this way — so a loader that reads
/// only the manifest gives those plugins no servers at all.
const ROOT_MCP_FILE: &str = ".mcp.json";

/// The plugin-root placeholders a servers document may use to name a file it
/// ships. `${CLAUDE_PLUGIN_ROOT}` is the ecosystem spelling bundled marketplace
/// plugins already carry; `${KLAUS_PLUGIN_ROOT}` is ours and says what the value
/// actually is.
const PLUGIN_ROOT_PLACEHOLDERS: [&str; 2] = ["${KLAUS_PLUGIN_ROOT}", "${CLAUDE_PLUGIN_ROOT}"];

/// Replace the plugin-root placeholders with the plugin's own directory.
///
/// Expansion has to happen here rather than where the command is spawned: the
/// plugin directory is known while loading the bundle and lost by the time the
/// config reaches the transport. Only these two names are substituted — a
/// document's other `${VAR}` spellings belong to that server (the terraform one
/// passes `${TFE_TOKEN}` through to `docker run`) and clearing them would turn a
/// working entry into a broken one.
fn expand_plugin_root(value: &Value, plugin_dir: &str) -> Value {
    fn expand_string(text: &str, plugin_dir: &str) -> String {
        let mut expanded = text.to_string();
        for placeholder in PLUGIN_ROOT_PLACEHOLDERS {
            if expanded.contains(placeholder) {
                expanded = expanded.replace(placeholder, plugin_dir);
            }
        }
        expanded
    }
    match value {
        Value::String(text) => Value::String(expand_string(text, plugin_dir)),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| expand_plugin_root(item, plugin_dir))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, item)| (key.clone(), expand_plugin_root(item, plugin_dir)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Read a servers document: the `{mcpServers:{…}}` wrapper or a bare
/// name -> config map, both of which appear in the wild.
async fn read_mcp_servers_document(path: &str) -> Vec<(String, Value)> {
    let Ok(bytes) = tokio::fs::read(path).await else {
        return Vec::new();
    };
    let Ok(parsed) = serde_json::from_slice::<Value>(&bytes) else {
        return Vec::new();
    };
    let map = match parsed.get("mcpServers") {
        Some(Value::Object(map)) => map.clone(),
        _ => match parsed {
            Value::Object(map) => map,
            _ => return Vec::new(),
        },
    };
    map.into_iter().collect()
}

impl HooksSpec {
    fn is_truthy(&self) -> bool {
        match self {
            HooksSpec::Path(s) => !s.is_empty(),
            HooksSpec::List(_) | HooksSpec::Inline(_) => true,
        }
    }
}

/// The validated `plugin.json` manifest.
struct PluginManifest {
    name: String,
    version: Option<String>,
    display_name: Option<String>,
    description: Option<String>,
    keywords: Vec<String>,
    dependencies: Vec<String>,
    user_config: JsonObject,
    settings: JsonObject,
    enabled: bool,
    skills: Option<RelPath>,
    commands: Option<CommandsSpec>,
    agents: Option<RelPath>,
    output_styles: Option<RelPath>,
    hooks: Option<HooksSpec>,
    /// MCP server declarations: either inline in the manifest, or a path to a
    /// servers document inside the plugin.
    mcp_servers: McpServersSpec,
    /// LSP server declarations, surfaced raw.
    lsp_servers: JsonObject,
    runtime_capabilities: Vec<String>,
    interface_capabilities: Vec<String>,
    app_connectors: Vec<String>,
}

// ---- Manifest validation (mirrors the manifest schema) ---------------------

/// A plugin `name` becomes an install directory, so it must not contain path
/// separators, `..`, a leading dot, or be absolute.
fn valid_plugin_name(n: &str) -> bool {
    !n.contains("..")
        && !n.contains('/')
        && !n.contains('\\')
        && !n.starts_with('.')
        && !is_absolute(n)
}

fn plugin_manifest_scalar_text(value: &RuntimeString, field: &str) -> Result<String, String> {
    value
        .try_to_utf8()
        .map_err(|error| format!("\"{field}\": expected Unicode scalar text: {error}"))
}

fn validate_opt_string(v: Option<&OrderedJson>, key: &str) -> Result<Option<String>, String> {
    match v {
        None => Ok(None),
        Some(OrderedJson::Str(s)) => plugin_manifest_scalar_text(s, key).map(Some),
        Some(_) => Err(format!("\"{key}\": expected string")),
    }
}

fn validate_opt_string_array(
    v: Option<&OrderedJson>,
    key: &str,
) -> Result<Option<Vec<String>>, String> {
    match v {
        None => Ok(None),
        Some(OrderedJson::Array(arr)) => {
            let mut list = Vec::with_capacity(arr.len());
            for e in arr {
                match e {
                    OrderedJson::Str(s) => list.push(plugin_manifest_scalar_text(s, key)?),
                    _ => return Err(format!("\"{key}\": expected string[]")),
                }
            }
            Ok(Some(list))
        }
        Some(_) => Err(format!("\"{key}\": expected string[]")),
    }
}

fn validate_opt_object(v: Option<&OrderedJson>, key: &str) -> Result<JsonObject, String> {
    match v {
        None => Ok(JsonObject::new()),
        Some(OrderedJson::Object(entries)) => {
            let mut out = JsonObject::new();
            for (k, v) in entries {
                let key_text = plugin_manifest_scalar_text(k, key)?;
                out.insert(key_text, v.to_scalar_config_value(key)?);
            }
            Ok(out)
        }
        Some(_) => Err(format!("\"{key}\": expected object")),
    }
}

fn prompt_safe_plugin_text(value: Option<String>) -> Option<String> {
    let value = value?.split_whitespace().collect::<Vec<_>>().join(" ");
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn plugin_namespace(name: &str) -> String {
    name.split_once('@')
        .map(|(name, _)| name)
        .unwrap_or(name)
        .to_string()
}

#[derive(Default)]
struct ManifestInterface {
    display_name: Option<String>,
    short_description: Option<String>,
    capabilities: Vec<String>,
}

fn validate_manifest_interface(v: Option<&OrderedJson>) -> Result<ManifestInterface, String> {
    let Some(v) = v else {
        return Ok(ManifestInterface::default());
    };
    let fields = match v {
        OrderedJson::Object(fields) => fields,
        _ => return Err("\"interface\": expected object".to_string()),
    };
    let get = |key: &str| fields.iter().find(|(k, _)| k == key).map(|(_, v)| v);
    Ok(ManifestInterface {
        display_name: validate_opt_string(get("displayName"), "interface.displayName")?,
        short_description: validate_opt_string(
            get("shortDescription"),
            "interface.shortDescription",
        )?,
        capabilities: validate_opt_string_array(get("capabilities"), "interface.capabilities")?
            .unwrap_or_default(),
    })
}

fn validate_opt_relpath(v: Option<&OrderedJson>, key: &str) -> Result<Option<RelPath>, String> {
    match v {
        None => Ok(None),
        Some(OrderedJson::Str(s)) => Ok(Some(RelPath::One(plugin_manifest_scalar_text(s, key)?))),
        Some(OrderedJson::Array(arr)) => {
            let mut list = Vec::with_capacity(arr.len());
            for e in arr {
                match e {
                    OrderedJson::Str(s) => list.push(plugin_manifest_scalar_text(s, key)?),
                    _ => return Err(format!("\"{key}\": expected string or string[]")),
                }
            }
            Ok(Some(RelPath::Many(list)))
        }
        Some(_) => Err(format!("\"{key}\": expected string or string[]")),
    }
}

fn validate_command_meta(v: &OrderedJson) -> Result<CommandMetaEntry, String> {
    let fields = match v {
        OrderedJson::Object(f) => f,
        _ => return Err("command entry must be an object".to_string()),
    };
    let getf = |key: &str| fields.iter().find(|(k, _)| k == key).map(|(_, x)| x);
    let source = validate_opt_string(getf("source"), "source")?;
    let content = validate_opt_string(getf("content"), "content")?;
    let description = validate_opt_string(getf("description"), "description")?;
    let argument_hint = validate_opt_string(getf("argumentHint"), "argumentHint")?;
    // `model` is validated for shape but not otherwise used here.
    let _model = validate_opt_string(getf("model"), "model")?;
    let allowed_tools = validate_opt_string_array(getf("allowedTools"), "allowedTools")?;
    // Exactly one NON-EMPTY of source/content (an empty string counts as absent).
    let has_source = source.as_deref().is_some_and(|s| !s.is_empty());
    let has_content = content.as_deref().is_some_and(|s| !s.is_empty());
    if has_source == has_content {
        return Err(
            "Command must have either \"source\" (file path) or \"content\" (inline markdown), but not both"
                .to_string(),
        );
    }
    Ok(CommandMetaEntry {
        source,
        content,
        description,
        argument_hint,
        allowed_tools,
    })
}

fn validate_commands_spec(v: &OrderedJson) -> Result<CommandsSpec, String> {
    match v {
        OrderedJson::Str(s) => Ok(CommandsSpec::Paths(RelPath::One(
            plugin_manifest_scalar_text(s, "commands")?,
        ))),
        OrderedJson::Array(arr) => {
            let mut list = Vec::with_capacity(arr.len());
            for e in arr {
                match e {
                    OrderedJson::Str(s) => list.push(plugin_manifest_scalar_text(s, "commands")?),
                    _ => {
                        return Err("\"commands\": expected string, string[], or a map".to_string())
                    }
                }
            }
            Ok(CommandsSpec::Paths(RelPath::Many(list)))
        }
        OrderedJson::Object(entries) => {
            let mut out = Vec::with_capacity(entries.len());
            for (k, ev) in entries {
                let entry = validate_command_meta(ev)?;
                out.push((plugin_manifest_scalar_text(k, "commands")?, entry));
            }
            Ok(CommandsSpec::Map(out))
        }
        _ => Err("\"commands\": expected string, string[], or a map".to_string()),
    }
}

fn validate_hooks_spec(v: &OrderedJson) -> Result<HooksSpec, String> {
    match v {
        OrderedJson::Str(s) => Ok(HooksSpec::Path(plugin_manifest_scalar_text(s, "hooks")?)),
        OrderedJson::Array(arr) => Ok(HooksSpec::List(arr.clone())),
        // A bare object is an inline event map; rejecting it failed the whole
        // manifest, i.e. the plugin vanished rather than losing its hooks.
        OrderedJson::Object(_) => Ok(HooksSpec::Inline(v.clone())),
        _ => Err("\"hooks\": expected string, array, or object".to_string()),
    }
}

/// Validate a parsed manifest value against the manifest schema. Any typed-field
/// mismatch, a bad `name`, or a both/neither command entry fails the whole
/// manifest (so the plugin is skipped).
fn validate_manifest(json: &OrderedJson) -> Result<PluginManifest, String> {
    let obj = match json {
        OrderedJson::Object(fields) => fields,
        _ => return Err("manifest must be an object".to_string()),
    };
    let get = |key: &str| obj.iter().find(|(k, _)| k == key).map(|(_, v)| v);

    let name = match get("name") {
        Some(OrderedJson::Str(s)) => plugin_manifest_scalar_text(s, "name")?,
        Some(_) => return Err("\"name\": expected string".to_string()),
        None => return Err("\"name\": required".to_string()),
    };
    if !valid_plugin_name(&name) {
        return Err(
            "plugin name must not contain path separators, \"..\", a leading dot, or be absolute"
                .to_string(),
        );
    }
    let version = validate_opt_string(get("version"), "version")?;
    let description =
        prompt_safe_plugin_text(validate_opt_string(get("description"), "description")?);
    let keywords = validate_opt_string_array(get("keywords"), "keywords")?.unwrap_or_default();
    let dependencies =
        validate_opt_string_array(get("dependencies"), "dependencies")?.unwrap_or_default();
    let user_config = validate_opt_object(get("userConfig"), "userConfig")?;
    let settings = validate_opt_object(get("settings"), "settings")?;
    let interface = validate_manifest_interface(get("interface"))?;
    let display_name = prompt_safe_plugin_text(
        validate_opt_string(get("displayName"), "displayName")?.or(interface.display_name),
    );
    let description = description.or_else(|| prompt_safe_plugin_text(interface.short_description));
    let app_connectors =
        validate_opt_string_array(get("appConnectors"), "appConnectors")?.unwrap_or_default();
    let enabled = match get("enabled") {
        None => true,
        Some(OrderedJson::Bool(value)) => *value,
        Some(_) => return Err("\"enabled\": expected boolean".to_string()),
    };

    let skills = validate_opt_relpath(get("skills"), "skills")?;
    let agents = validate_opt_relpath(get("agents"), "agents")?;
    let output_styles = validate_opt_relpath(get("outputStyles"), "outputStyles")?;
    let runtime_capabilities =
        validate_opt_string_array(get("runtimeCapabilities"), "runtimeCapabilities")?
            .unwrap_or_default();
    let commands = match get("commands") {
        None => None,
        Some(v) => Some(validate_commands_spec(v)?),
    };
    let hooks = match get("hooks") {
        None => None,
        Some(v) => Some(validate_hooks_spec(v)?),
    };
    let mcp_servers = match get("mcpServers") {
        None => McpServersSpec::Inline(Vec::new()),
        // A PATH to a servers document, the same shape `commands` and `hooks`
        // already accept. Plugins in the wild write `"mcpServers": "./.mcp.json"`,
        // and rejecting it failed the WHOLE manifest, i.e. the plugin vanished
        // rather than losing one field.
        Some(OrderedJson::Str(s)) => {
            McpServersSpec::Path(plugin_manifest_scalar_text(s, "mcpServers")?)
        }
        Some(OrderedJson::Object(entries)) => McpServersSpec::Inline(
            entries
                .iter()
                .map(|(k, v)| {
                    Ok((
                        plugin_manifest_scalar_text(k, "mcpServers")?,
                        v.to_scalar_config_value("mcpServers")?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        ),
        Some(_) => return Err("\"mcpServers\": expected string or object".to_string()),
    };
    let lsp_servers = match get("lspServers") {
        None => JsonObject::new(),
        Some(OrderedJson::Object(entries)) => {
            let mut m = JsonObject::new();
            for (k, v) in entries {
                m.insert(
                    plugin_manifest_scalar_text(k, "lspServers")?,
                    v.to_scalar_config_value("lspServers")?,
                );
            }
            m
        }
        Some(_) => return Err("\"lspServers\": expected object".to_string()),
    };

    Ok(PluginManifest {
        name,
        version,
        display_name,
        description,
        keywords,
        dependencies,
        user_config,
        settings,
        enabled,
        skills,
        commands,
        agents,
        output_styles,
        hooks,
        mcp_servers,
        lsp_servers,
        runtime_capabilities,
        interface_capabilities: interface.capabilities,
        app_connectors,
    })
}

// ---- host path helpers -----------------------------------------------------
//
// These address real files on disk (a plugin directory and the manifest-relative paths
// under it), so they follow the host dialect. A POSIX-only copy used to live here: on
// Windows it split `C:\\plugin\\commands` on `/` alone, found no shared prefix with the
// plugin root, and `resolve_under` therefore rejected every manifest path as an escape —
// commands, hooks and MCP servers declared in a plugin manifest all silently vanished.

pub(crate) use crate::permission::bash::node_path::{join as path_join, relative as path_relative};

/// Resolve a manifest-relative path under `plugin_dir`, rejecting any that
/// escapes the plugin root.
fn resolve_under(plugin_dir: &str, rel: &str) -> Option<String> {
    let full = path_join(&[plugin_dir, rel]);
    let r = path_relative(plugin_dir, &full);
    if r.starts_with("..") || is_absolute(&r) {
        return None;
    }
    Some(full)
}

/// Whether a resolved path is a directory (false for a file or a missing path).
async fn is_dir(path: &str) -> bool {
    match tokio::fs::metadata(path).await {
        Ok(m) => m.is_dir(),
        Err(_) => false,
    }
}

/// The realpath of `path`, falling back to `path` itself when it cannot be
/// resolved (used as a stable dedup key for hook files).
async fn realpath_or(path: &str) -> String {
    match tokio::fs::canonicalize(path)
        .await
        .map(crate::fs::node_path::simplify_host)
    {
        Ok(p) => p.to_string_lossy().into_owned(),
        Err(_) => path.to_string(),
    }
}

/// Duplicate command-load options for a call that consumes them by value (the
/// runner handle and vars are cheaply clonable).
fn dup_command_opts(o: &CommandLoadOptions) -> CommandLoadOptions {
    CommandLoadOptions {
        run_bash: o.run_bash.clone(),
        vars: o.vars.clone(),
    }
}

// ---- Component loaders (manifest-listed forms) -----------------------------

async fn load_manifest_skills(plugin_dir: &str, spec: &RelPath) -> Vec<Skill> {
    let mut out = Vec::new();
    for rel in spec.as_list() {
        if let Some(full) = resolve_under(plugin_dir, rel) {
            out.extend(load_skills_from_dir(&full).await);
        }
    }
    out
}

async fn load_manifest_agents(plugin_dir: &str, spec: &RelPath) -> Vec<AgentDefinition> {
    let mut out = Vec::new();
    for rel in spec.as_list() {
        let full = match resolve_under(plugin_dir, rel) {
            Some(f) => f,
            None => continue,
        };
        if is_dir(&full).await {
            out.extend(load_agents_from_dir(&full).await);
        } else if let Some(agent) = load_agent_from_file(&full).await {
            out.push(agent);
        }
    }
    out
}

async fn load_manifest_output_styles(plugin_dir: &str, spec: &RelPath) -> Vec<OutputStyle> {
    let mut out = Vec::new();
    for rel in spec.as_list() {
        let full = match resolve_under(plugin_dir, rel) {
            Some(f) => f,
            None => continue,
        };
        if is_dir(&full).await {
            out.extend(load_output_styles_from_dir(&full).await);
        } else if let Some(style) = load_output_style_from_file(&full).await {
            out.push(style);
        }
    }
    out
}

async fn load_manifest_commands(
    plugin_dir: &str,
    spec: &CommandsSpec,
    opts: &CommandLoadOptions,
) -> Vec<Command> {
    let mut out = Vec::new();
    match spec {
        // Object-map form: { name: { source | content, … } }.
        CommandsSpec::Map(entries) => {
            for (name, meta) in entries {
                if let Some(content) = &meta.content {
                    // Manifest entry's allowed-tools → injected into the inline command's bash gate.
                    let inline_meta = InlineCommandMeta {
                        description: meta.description.clone(),
                        argument_hint: meta.argument_hint.clone(),
                    };
                    if let Some(cmd) = make_inline_command(
                        name,
                        content,
                        inline_meta,
                        opts,
                        meta.allowed_tools.clone(),
                    ) {
                        out.push(cmd);
                    }
                } else if let Some(source) = &meta.source {
                    let full = match resolve_under(plugin_dir, source) {
                        Some(f) => f,
                        None => continue,
                    };
                    // A manifest-level allowed-tools grant REPLACES the file's own list.
                    if let Some(cmd) = load_command_from_file(
                        &full,
                        dup_command_opts(opts),
                        meta.allowed_tools.clone(),
                    )
                    .await
                    {
                        match cmd {
                            Command::Prompt(mut p) => {
                                p.name = name.clone();
                                if let Some(d) = &meta.description {
                                    p.meta.description = Some(d.clone());
                                }
                                if let Some(h) = &meta.argument_hint {
                                    p.meta.argument_hint = Some(h.clone());
                                }
                                out.push(Command::Prompt(p));
                            }
                            Command::Local(_) => {}
                        }
                    }
                }
            }
        }
        // String / array-of-paths form: each is a command file or a directory of command files.
        CommandsSpec::Paths(rel) => {
            for r in rel.as_list() {
                let full = match resolve_under(plugin_dir, r) {
                    Some(f) => f,
                    None => continue,
                };
                if is_dir(&full).await {
                    out.extend(load_commands_from_dir(&full, dup_command_opts(opts)).await);
                } else if let Some(cmd) =
                    load_command_from_file(&full, dup_command_opts(opts), None).await
                {
                    out.push(cmd);
                }
            }
        }
    }
    out
}

/// Pick the events map for an inline hook entry: its `hooks` value when that is
/// an object or array (matching the truthy typeof-object test), else the entry
/// itself.
fn pick_hook_events_map<'a>(
    entry: &'a OrderedJson,
    fields: &'a [(RuntimeString, OrderedJson)],
) -> &'a OrderedJson {
    match fields.iter().rev().find(|(k, _)| k == "hooks") {
        Some((_, v @ OrderedJson::Object(_))) | Some((_, v @ OrderedJson::Array(_))) => v,
        _ => entry,
    }
}

async fn load_manifest_hooks(
    plugin_dir: &str,
    spec: &HooksSpec,
    loaded: &mut HashSet<String>,
) -> Vec<Hook> {
    let mut out = Vec::new();
    match spec {
        HooksSpec::Path(s) => {
            if let Some(full) = resolve_under(plugin_dir, s) {
                let key = realpath_or(&full).await;
                if loaded.insert(key) {
                    out.extend(load_hooks_json(&full).await);
                }
            }
        }
        HooksSpec::Inline(value) => {
            let OrderedJson::Object(fields) = value else {
                return out;
            };
            let events = pick_hook_events_map(value, fields);
            out.extend(crate::extensions::hooks_loader::parse_hooks_runtime_json(
                &events.to_json_string(),
            ));
        }
        HooksSpec::List(list) => {
            for entry in list {
                match entry {
                    OrderedJson::Str(s) => {
                        let Ok(path) = plugin_manifest_scalar_text(s, "hooks") else {
                            continue;
                        };
                        if let Some(full) = resolve_under(plugin_dir, &path) {
                            let key = realpath_or(&full).await;
                            if loaded.insert(key) {
                                out.extend(load_hooks_json(&full).await);
                            }
                        }
                    }
                    OrderedJson::Object(fields) => {
                        let events = pick_hook_events_map(entry, fields);
                        out.extend(crate::extensions::hooks_loader::parse_hooks_runtime_json(
                            &events.to_json_string(),
                        ));
                    }
                    _ => {}
                }
            }
        }
    }
    out
}

// ---- Manifest discovery + plugin load --------------------------------------

/// Locate + read a plugin manifest: `.klaus-plugin/plugin.json` first, then
/// `.codex-plugin`/`.claude-plugin` roots, then the legacy root `plugin.json`.
async fn read_manifest(plugin_dir: &str) -> Option<(String, RuntimeString)> {
    let candidates = [
        path_join(&[plugin_dir, MANIFEST_DIR, MANIFEST_FILE]),
        path_join(&[plugin_dir, CODEX_MANIFEST_DIR, MANIFEST_FILE]),
        path_join(&[plugin_dir, CLAUDE_MANIFEST_DIR, MANIFEST_FILE]),
        path_join(&[plugin_dir, MANIFEST_FILE]),
    ];
    for path in candidates {
        if let Ok(raw) = tokio::fs::read(&path).await {
            return Some((path, NodeBufferUtf8::decode(&raw)));
        }
    }
    None
}

const SUPPORTED_RUNTIME_CAPABILITIES: &[&str] = &["browser"];

#[derive(Default)]
struct PluginLoadOneReport {
    plugin: Option<LoadedPlugin>,
    disabled: Option<PluginDisabledEntry>,
    error: Option<PluginLoadIssue>,
    warnings: Vec<PluginLoadIssue>,
}

fn plugin_issue(
    kind: PluginLoadIssueKind,
    plugin_dir: &str,
    message: impl Into<String>,
) -> PluginLoadIssue {
    PluginLoadIssue {
        kind,
        dir: RuntimeString::from(plugin_dir),
        message: RuntimeString::from(message.into()),
    }
}

fn normalize_runtime_capabilities(
    plugin_dir: &str,
    declared: &[String],
) -> (Vec<String>, Vec<PluginLoadIssue>) {
    let mut capabilities = Vec::new();
    let mut warnings = Vec::new();
    for name in declared {
        if SUPPORTED_RUNTIME_CAPABILITIES.contains(&name.as_str()) {
            if !capabilities.contains(name) {
                capabilities.push(name.clone());
            }
        } else {
            warnings.push(plugin_issue(
                PluginLoadIssueKind::UnsupportedRuntimeCapability,
                plugin_dir,
                format!("unsupported runtime capability \"{name}\""),
            ));
        }
    }
    capabilities.sort();
    (capabilities, warnings)
}

/// Read + validate a single plugin directory, loading all of its components.
/// Returns `None` when there is no manifest, the manifest is invalid JSON, or it
/// fails the manifest schema.
pub async fn load_one_plugin(plugin_dir: &str, opts: CommandLoadOptions) -> Option<LoadedPlugin> {
    load_one_plugin_with_report(plugin_dir, opts).await.plugin
}

/// Read + validate one plugin directory and return the same report shape as
/// bulk plugin discovery. This lets installed-plugin ledgers load exact plugin
/// directories while preserving disabled/error/warning diagnostics.
pub async fn load_one_plugin_report(
    plugin_dir: &str,
    opts: CommandLoadOptions,
) -> PluginLoadReport {
    let one = load_one_plugin_with_report(plugin_dir, opts).await;
    let mut report = PluginLoadReport::default();
    if let Some(plugin) = one.plugin {
        report.enabled.push(plugin);
    }
    if let Some(disabled) = one.disabled {
        report.disabled.push(disabled);
    }
    if let Some(error) = one.error {
        report.errors.push(error);
    }
    report.warnings.extend(one.warnings);
    report
}

async fn load_one_plugin_with_report(
    plugin_dir: &str,
    opts: CommandLoadOptions,
) -> PluginLoadOneReport {
    let Some((manifest_path, raw)) = read_manifest(plugin_dir).await else {
        return PluginLoadOneReport::default();
    };

    let json = match runtime_json::parse(&raw) {
        Ok(json) => OrderedJson::from_runtime(&json),
        Err(_) => {
            tracing::warn!(
                "[extensions] skipping plugin with invalid JSON manifest at {}",
                plugin_dir
            );
            return PluginLoadOneReport {
                error: Some(plugin_issue(
                    PluginLoadIssueKind::InvalidJson,
                    plugin_dir,
                    "invalid JSON manifest",
                )),
                ..PluginLoadOneReport::default()
            };
        }
    };
    let manifest = match validate_manifest(&json) {
        Ok(m) => m,
        Err(reason) => {
            tracing::warn!(
                "[extensions] skipping invalid plugin at {}: {}",
                plugin_dir,
                reason
            );
            return PluginLoadOneReport {
                error: Some(plugin_issue(
                    PluginLoadIssueKind::InvalidManifest,
                    plugin_dir,
                    reason,
                )),
                ..PluginLoadOneReport::default()
            };
        }
    };
    if !manifest.enabled {
        return PluginLoadOneReport {
            disabled: Some(PluginDisabledEntry {
                name: Some(RuntimeString::from(manifest.name)),
                dir: RuntimeString::from(plugin_dir),
                reason: RuntimeString::from("disabled by manifest"),
            }),
            ..PluginLoadOneReport::default()
        };
    }
    let (runtime_capabilities, warnings) =
        normalize_runtime_capabilities(plugin_dir, &manifest.runtime_capabilities);

    // Hooks are ADDITIVE (standard hooks/hooks.json + manifest.hooks), but a
    // manifest entry pointing back at the standard file must not double-load it —
    // seed the dedup set with its realpath first.
    let standard_hooks_path = path_join(&[plugin_dir, "hooks", "hooks.json"]);
    let mut loaded_hook_paths: HashSet<String> = HashSet::new();
    loaded_hook_paths.insert(realpath_or(&standard_hooks_path).await);

    // For commands/agents/skills/outputStyles, auto-detect the conventional subdir
    // ONLY when the manifest does not list that component; a listed component is
    // loaded from its manifest paths instead.
    let computed_namespace = plugin_namespace(&manifest.name);
    let mut skills = match &manifest.skills {
        Some(spec) if spec.is_truthy() => load_manifest_skills(plugin_dir, spec).await,
        _ => load_skills_from_dir(&path_join(&[plugin_dir, "skills"])).await,
    };
    if !computed_namespace.trim().is_empty() {
        let ns_rt = crate::runtime_values::RuntimeString::from(computed_namespace.clone());
        for skill in &mut skills {
            if skill.plugin_namespace.is_none() {
                skill.plugin_namespace = Some(ns_rt.clone());
            }
        }
    }
    let commands = match &manifest.commands {
        Some(spec) if spec.is_truthy() => load_manifest_commands(plugin_dir, spec, &opts).await,
        _ => {
            load_commands_from_dir(
                &path_join(&[plugin_dir, "commands"]),
                dup_command_opts(&opts),
            )
            .await
        }
    };
    let agents = match &manifest.agents {
        Some(spec) if spec.is_truthy() => load_manifest_agents(plugin_dir, spec).await,
        _ => load_agents_from_dir(&path_join(&[plugin_dir, "agents"])).await,
    };
    let output_styles = match &manifest.output_styles {
        Some(spec) if spec.is_truthy() => load_manifest_output_styles(plugin_dir, spec).await,
        _ => load_output_styles_from_dir(&path_join(&[plugin_dir, "output-styles"])).await,
    };
    let standard_hooks = load_hooks_json(&standard_hooks_path).await;
    let manifest_hooks = match &manifest.hooks {
        Some(spec) if spec.is_truthy() => {
            load_manifest_hooks(plugin_dir, spec, &mut loaded_hook_paths).await
        }
        _ => Vec::new(),
    };
    let mut hooks = standard_hooks;
    hooks.extend(manifest_hooks);

    // MCP servers come from the manifest (inline or a path it names) and, by
    // convention, from the plugin root's `.mcp.json`. The manifest wins a name
    // collision; the root file fills in the rest, which is the only way the
    // cc-style plugins that never mention it get their servers at all.
    let (declared, declaration_path) = match &manifest.mcp_servers {
        McpServersSpec::Inline(entries) => (entries.clone(), Some(manifest_path.clone())),
        McpServersSpec::Path(rel) => match resolve_under(plugin_dir, rel) {
            Some(path) => (read_mcp_servers_document(&path).await, Some(path)),
            None => (Vec::new(), None),
        },
    };
    let root_path = path_join(&[plugin_dir, ROOT_MCP_FILE]);
    let root_entries = read_mcp_servers_document(&root_path).await;
    let mut mcp_servers: Vec<McpServerConfig> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for (entries, declaration_path) in [
        (&declared, declaration_path.as_deref()),
        (&root_entries, Some(root_path.as_str())),
    ] {
        for (name, raw_cfg) in entries {
            if seen.iter().any(|taken| taken == name) {
                continue;
            }
            let expanded = expand_plugin_root(raw_cfg, plugin_dir);
            if let Some(mut cfg) = parse_mcp_server_entry(name, &expanded) {
                cfg.options_mut().native_origin = declaration_path.map(|path| {
                    crate::mcp::approval_provenance::NativeMcpOrigin::plugin(
                        crate::fs::paths::config_home().into(),
                        plugin_dir.into(),
                        manifest_path.clone().into(),
                        path.into(),
                        expanded.clone(),
                    )
                });
                seen.push(name.clone());
                mcp_servers.push(cfg);
            }
        }
    }

    PluginLoadOneReport {
        plugin: Some(LoadedPlugin {
            plugin_namespace: plugin_namespace(&manifest.name),
            name: manifest.name,
            version: manifest.version,
            display_name: manifest.display_name,
            description: manifest.description,
            keywords: manifest.keywords,
            dependencies: manifest.dependencies,
            user_config: manifest.user_config,
            settings: manifest.settings,
            dir: plugin_dir.to_string(),
            skills,
            commands,
            agents,
            output_styles,
            hooks,
            mcp_servers,
            lsp_servers: manifest.lsp_servers,
            runtime_capabilities,
            interface_capabilities: manifest.interface_capabilities,
            app_connectors: manifest.app_connectors,
        }),
        warnings,
        ..PluginLoadOneReport::default()
    }
}

/// Load every plugin in `dir`. A plugin is a subdirectory carrying a
/// `plugin.json` manifest (at `.klaus-plugin/plugin.json`,
/// `.codex-plugin/plugin.json`, `.claude-plugin/plugin.json`, or the legacy
/// root).
/// Invalid plugins are skipped; a missing `dir` yields `[]`.
pub async fn load_plugins_from_dir(dir: &str, opts: CommandLoadOptions) -> Vec<LoadedPlugin> {
    load_plugins_report_from_dir(dir, opts).await.enabled
}

/// Load every plugin in `dir`, preserving per-plugin load state for hosts that
/// need to show enabled/disabled plugins plus manifest errors and warnings.
pub async fn load_plugins_report_from_dir(dir: &str, opts: CommandLoadOptions) -> PluginLoadReport {
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return PluginLoadReport::default(),
    };
    let mut paths = Vec::new();
    loop {
        let entry = match rd.next_entry().await {
            Ok(Some(e)) => e,
            _ => break,
        };
        let file_type = match entry.file_type().await {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if !file_type.is_dir() {
            continue;
        }
        paths.push(entry.path().to_string_lossy().into_owned());
    }
    paths.sort();

    let mut report = PluginLoadReport::default();
    for path in paths {
        let outcome = load_one_plugin_with_report(&path, dup_command_opts(&opts)).await;
        if let Some(plugin) = outcome.plugin {
            report.enabled.push(plugin);
        }
        if let Some(disabled) = outcome.disabled {
            report.disabled.push(disabled);
        }
        if let Some(error) = outcome.error {
            report.errors.push(error);
        }
        report.warnings.extend(outcome.warnings);
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::types::HookEvent;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-plugins-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write(dir: &Path, rel: &str, content: &str) {
        let full = dir.join(rel);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&full, content).unwrap();
    }

    fn command_name(c: &Command) -> &str {
        match c {
            Command::Prompt(p) => p.name.as_str(),
            Command::Local(l) => l.name.as_str(),
        }
    }

    #[test]
    fn ordered_json_preserves_first_slot_last_value_and_wire_order() {
        let value = OrderedJson::from_runtime(
            &runtime_json::parse(&RuntimeString::from(
                r#"{"b":1,"a":2,"b":3,"nested":{"z":0,"y":1}}"#,
            ))
            .unwrap(),
        );
        let wire = value.to_json_string();
        assert_eq!(wire, r#"{"b":3,"a":2,"nested":{"z":0,"y":1}}"#);

        let decoded = OrderedJson::from_runtime(&runtime_json::parse(&wire).unwrap());
        assert_eq!(decoded.to_json_string(), wire);
    }

    #[tokio::test]
    async fn loads_a_plugin_and_autodetects_the_conventional_subdirs() {
        let root = temp_dir();
        let plug = root.join("demo");
        write(&plug, ".klaus-plugin/plugin.json", r#"{"name":"demo"}"#);
        write(
            &plug,
            "commands/foo.md",
            "---\ndescription: foo cmd\n---\nBody",
        );
        write(
            &plug,
            "agents/bar.md",
            "---\nname: bar\ndescription: does bar\n---\nprompt",
        );
        write(
            &plug,
            "output-styles/baz.md",
            "---\nname: baz\n---\nBe baz.",
        );
        write(
            &plug,
            "skills/qux/SKILL.md",
            "---\nname: qux\n---\nSkill body",
        );
        write(
            &plug,
            "hooks/hooks.json",
            r#"{"PostToolUse":[{"hooks":[{"type":"command","command":"fmt"}]}]}"#,
        );

        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .expect("plugin loads");
        assert_eq!(plugin.name, "demo");
        assert_eq!(plugin.commands.len(), 1);
        assert_eq!(plugin.agents.len(), 1);
        assert_eq!(plugin.output_styles.len(), 1);
        assert_eq!(plugin.skills.len(), 1);
        assert_eq!(plugin.hooks.len(), 1);
        assert_eq!(plugin.hooks[0].event, HookEvent::PostToolUse);
    }

    #[tokio::test]
    async fn reads_the_legacy_root_manifest_when_the_klaus_plugin_dir_is_absent() {
        let root = temp_dir();
        let plug = root.join("legacy");
        write(&plug, "plugin.json", r#"{"name":"legacy"}"#);
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default()).await;
        assert!(plugin.is_some());
        assert_eq!(plugin.unwrap().name, "legacy");
    }

    #[tokio::test]
    async fn reads_codex_and_claude_plugin_manifests_as_first_class_plugin_roots() {
        let root = temp_dir();
        let codex = root.join("codex");
        let claude = root.join("claude");

        write(
            &codex,
            ".codex-plugin/plugin.json",
            r#"{"name":"codex-demo"}"#,
        );
        write(
            &claude,
            ".claude-plugin/plugin.json",
            r#"{"name":"claude-demo"}"#,
        );

        let codex_plugin =
            load_one_plugin(codex.to_str().unwrap(), CommandLoadOptions::default()).await;
        let claude_plugin =
            load_one_plugin(claude.to_str().unwrap(), CommandLoadOptions::default()).await;

        assert_eq!(codex_plugin.unwrap().name, "codex-demo");
        assert_eq!(claude_plugin.unwrap().name, "claude-demo");
    }

    #[tokio::test]
    async fn skips_a_plugin_with_invalid_json() {
        let root = temp_dir();
        let plug = root.join("bad");
        write(&plug, "plugin.json", "{not json");
        assert!(
            load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn skips_a_plugin_whose_name_has_a_path_separator() {
        let root = temp_dir();
        let plug = root.join("unsafe");
        write(&plug, "plugin.json", r#"{"name":"a/b"}"#);
        assert!(
            load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn missing_manifest_yields_none() {
        let root = temp_dir();
        let plug = root.join("empty");
        std::fs::create_dir_all(&plug).unwrap();
        assert!(
            load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn manifest_commands_object_map_inline_content_form() {
        let root = temp_dir();
        let plug = root.join("p");
        // A listed `commands` map suppresses auto-detection of commands/.
        write(
            &plug,
            "commands/ignored.md",
            "---\ndescription: ignored\n---\nX",
        );
        write(
            &plug,
            "plugin.json",
            r#"{"name":"p","commands":{"greet":{"content":"Hi there"}}}"#,
        );
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();
        assert_eq!(plugin.commands.len(), 1);
        assert_eq!(command_name(&plugin.commands[0]), "greet");
    }

    #[tokio::test]
    async fn manifest_commands_object_map_source_form_applies_name_and_meta_overrides() {
        let root = temp_dir();
        let plug = root.join("p");
        write(
            &plug,
            "cmds/x.md",
            "---\ndescription: original\nargument-hint: [orig]\n---\nBody",
        );
        write(
            &plug,
            "plugin.json",
            r#"{"name":"p","commands":{"custom":{"source":"cmds/x.md","description":"overridden","argumentHint":"[a]"}}}"#,
        );
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();
        assert_eq!(plugin.commands.len(), 1);
        match &plugin.commands[0] {
            Command::Prompt(p) => {
                assert_eq!(p.name, "custom");
                assert_eq!(p.meta.description.as_deref(), Some("overridden"));
                assert_eq!(p.meta.argument_hint.as_deref(), Some("[a]"));
            }
            Command::Local(_) => panic!("plugin markdown commands should load as prompt commands"),
        }
    }

    #[tokio::test]
    async fn a_command_entry_with_both_source_and_content_invalidates_the_whole_manifest() {
        let root = temp_dir();
        let plug = root.join("p");
        write(
            &plug,
            "plugin.json",
            r#"{"name":"p","commands":{"x":{"source":"a.md","content":"inline"}}}"#,
        );
        assert!(
            load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_listed_component_suppresses_autodetection_and_path_escapes_are_rejected() {
        let root = temp_dir();
        let plug = root.join("p");
        // Would be auto-detected if commands were unlisted.
        write(&plug, "commands/foo.md", "---\ndescription: foo\n---\nX");
        write(&plug, "plugin.json", r#"{"name":"p","commands":"../evil"}"#);
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();
        // Listed (truthy) but escapes root → nothing loaded, and autodetect suppressed.
        assert_eq!(plugin.commands.len(), 0);
    }

    #[tokio::test]
    async fn parses_manifest_mcp_servers() {
        let root = temp_dir();
        let plug = root.join("p");
        write(
            &plug,
            "plugin.json",
            r#"{"name":"p","mcpServers":{"srv":{"command":"echo","args":["hi"]}}}"#,
        );
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();
        assert_eq!(plugin.mcp_servers.len(), 1);
    }

    /// The plugin root's `.mcp.json`, with NO manifest mention, is how cc-style
    /// plugins declare MCP servers — every bundled marketplace plugin that has any
    /// does it this way. Reading only the manifest gave those plugins no servers,
    /// so nothing connected them.
    #[tokio::test]
    async fn root_mcp_json_is_picked_up_without_a_manifest_mention() {
        let root = temp_dir();
        let plug = root.join("p");
        write(&plug, "plugin.json", r#"{"name":"p"}"#);
        write(
            &plug,
            ".mcp.json",
            r#"{"mcpServers":{"from-root":{"command":"node","args":["r.js"]}}}"#,
        );
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            plugin
                .mcp_servers
                .iter()
                .map(|s| s.name().to_string())
                .collect::<Vec<_>>(),
            vec!["from-root".to_string()]
        );
    }

    /// The plugin-root placeholders are the only names substituted: a server
    /// naming a file it ships needs the directory resolved, while a document's
    /// own `${VAR}` spellings belong to that server and must survive untouched.
    #[test]
    fn expand_plugin_root_substitutes_only_the_root_placeholders() {
        let doc = serde_json::json!({
            "command": "${KLAUS_PLUGIN_ROOT}/servers/run.py",
            "args": ["--cwd", "${CLAUDE_PLUGIN_ROOT}", "--token", "${TFE_TOKEN}"],
            "env": { "ROOT": "${KLAUS_PLUGIN_ROOT}", "PASSTHROUGH": "${API_KEY}" }
        });
        let expanded = expand_plugin_root(&doc, "/plugins/voice-clone");
        assert_eq!(
            expanded["command"],
            serde_json::json!("/plugins/voice-clone/servers/run.py")
        );
        assert_eq!(
            expanded["args"],
            serde_json::json!(["--cwd", "/plugins/voice-clone", "--token", "${TFE_TOKEN}"]),
            "a server's own variable is passed through, not cleared"
        );
        assert_eq!(
            expanded["env"],
            serde_json::json!({
                "ROOT": "/plugins/voice-clone",
                "PASSTHROUGH": "${API_KEY}"
            })
        );
    }

    /// Both spellings name the same directory, and a value carrying neither is
    /// left exactly as written.
    #[test]
    fn expand_plugin_root_accepts_both_spellings_and_leaves_plain_values_alone() {
        assert_eq!(
            expand_plugin_root(&serde_json::json!("${CLAUDE_PLUGIN_ROOT}/a"), "/p"),
            serde_json::json!("/p/a")
        );
        assert_eq!(
            expand_plugin_root(&serde_json::json!("node"), "/p"),
            serde_json::json!("node")
        );
        // Non-string scalars survive unchanged.
        assert_eq!(
            expand_plugin_root(&serde_json::json!({ "alwaysLoad": true }), "/p"),
            serde_json::json!({ "alwaysLoad": true })
        );
    }

    /// A bundled server that runs a file from its own bundle: the command the
    /// transport spawns is the resolved absolute path, because the plugin
    /// directory is known only while the bundle is being loaded.
    #[tokio::test]
    async fn root_mcp_json_plugin_root_placeholder_reaches_the_spawned_command() {
        let root = temp_dir();
        let plug = root.join("p");
        write(&plug, "plugin.json", r#"{"name":"p"}"#);
        write(
            &plug,
            ".mcp.json",
            r#"{"mcpServers":{"bundled":{"command":"python3","args":["${KLAUS_PLUGIN_ROOT}/servers/run.py"]}}}"#,
        );
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();
        let bundled = plugin
            .mcp_servers
            .iter()
            .find(|s| s.name() == "bundled")
            .expect("the bundled server is loaded");
        match bundled {
            McpServerConfig::Stdio { command, args, .. } => {
                assert_eq!(command, "python3");
                let script = args
                    .as_ref()
                    .and_then(|a| a.first())
                    .expect("the script path survives");
                assert!(
                    is_absolute(script),
                    "the placeholder became an absolute path, got {script}"
                );
                assert!(
                    script.ends_with("servers/run.py"),
                    "the path points at the bundled script, got {script}"
                );
            }
            other => panic!("expected stdio, got {other:?}"),
        }
    }

    /// A plugin directory whose path contains spaces (the usual Windows case:
    /// `C:\Users\First Last\...`). Substitution is a plain string replacement on
    /// an argv entry that is never passed through a shell, so the spaces need no
    /// quoting and must survive byte-for-byte.
    #[tokio::test]
    async fn root_mcp_json_plugin_root_expansion_survives_spaces_in_the_path() {
        let root = temp_dir();
        let spaced = root.join("Program Files (x86)");
        let plug = spaced.join("my plugin");
        write(&plug, "plugin.json", r#"{"name":"p"}"#);
        write(
            &plug,
            ".mcp.json",
            r#"{"mcpServers":{"s":{"command":"python3","args":["${KLAUS_PLUGIN_ROOT}/server/run.py"]}}}"#,
        );
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();
        let server = plugin
            .mcp_servers
            .iter()
            .find(|s| s.name() == "s")
            .expect("the server is loaded");
        match server {
            McpServerConfig::Stdio { args, .. } => {
                let script = args
                    .as_ref()
                    .and_then(|a| a.first())
                    .expect("an arg survives");
                assert_eq!(
                    script.as_str(),
                    format!("{}/server/run.py", plug.to_str().unwrap()).as_str(),
                    "spaces are preserved literally, not quoted or split"
                );
            }
            other => panic!("expected stdio, got {other:?}"),
        }
    }

    /// A placeholder appearing in the MIDDLE of an argument, and twice in one
    /// value, both expand. Bundled plugins write these forms for real — a hook
    /// command like `node "<root>/scripts/x.mjs" SessionStart`, and a server arg
    /// that embeds the root inside a larger string.
    #[tokio::test]
    async fn root_mcp_json_plugin_root_expands_mid_string_and_every_occurrence() {
        let root = temp_dir();
        let plug = root.join("p");
        write(&plug, "plugin.json", r#"{"name":"p"}"#);
        write(
            &plug,
            ".mcp.json",
            r#"{"mcpServers":{"s":{"command":"node","args":["--root=${KLAUS_PLUGIN_ROOT}","${CLAUDE_PLUGIN_ROOT}/a:${KLAUS_PLUGIN_ROOT}/b"]}}}"#,
        );
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();
        let dir = plug.to_str().unwrap().to_string();
        let server = plugin
            .mcp_servers
            .iter()
            .find(|s| s.name() == "s")
            .expect("the server is loaded");
        match server {
            McpServerConfig::Stdio { args, .. } => {
                let args = args.as_ref().expect("args present");
                assert_eq!(args[0], format!("--root={dir}"));
                assert_eq!(
                    args[1],
                    format!("{dir}/a:{dir}/b"),
                    "both spellings, both occurrences"
                );
            }
            other => panic!("expected stdio, got {other:?}"),
        }
    }

    /// A plugin reached through a symlinked directory expands to the path it was
    /// loaded from, not a canonicalised one. Both spawn correctly — the OS
    /// resolves the link — but the value in the config is the literal directory
    /// the loader was handed, which is what a caller reading the config sees.
    #[cfg(unix)]
    #[tokio::test]
    async fn root_mcp_json_plugin_root_expansion_keeps_a_symlinked_path_literal() {
        let root = temp_dir();
        let real = root.join("real");
        let plug = real.join("p");
        write(&plug, "plugin.json", r#"{"name":"p"}"#);
        write(
            &plug,
            ".mcp.json",
            r#"{"mcpServers":{"s":{"command":"python3","args":["${KLAUS_PLUGIN_ROOT}/run.py"]}}}"#,
        );
        let link = root.join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let through_link = link.join("p");
        let plugin = load_one_plugin(
            through_link.to_str().unwrap(),
            CommandLoadOptions::default(),
        )
        .await
        .unwrap();
        let server = plugin
            .mcp_servers
            .iter()
            .find(|s| s.name() == "s")
            .expect("the server is loaded");
        match server {
            McpServerConfig::Stdio { args, .. } => {
                let script = args
                    .as_ref()
                    .and_then(|a| a.first())
                    .expect("an arg survives");
                assert_eq!(
                    script.as_str(),
                    format!("{}/run.py", through_link.to_str().unwrap()).as_str(),
                    "the expanded root is the path it was loaded from, not the realpath"
                );
            }
            other => panic!("expected stdio, got {other:?}"),
        }
    }

    /// `"mcpServers": "./file"` is the PATH form, which `commands` and `hooks`
    /// already accept. It used to fail the whole manifest, so the plugin vanished
    /// entirely rather than losing one field. The manifest's own entry still wins a
    /// name collision with the root file.
    #[tokio::test]
    async fn manifest_mcp_servers_accept_a_path_and_win_over_the_root_file() {
        let root = temp_dir();
        let plug = root.join("p");
        write(
            &plug,
            "plugin.json",
            r#"{"name":"p","mcpServers":"./servers.json"}"#,
        );
        // A bare name -> config map, no `mcpServers` wrapper.
        write(
            &plug,
            "servers.json",
            r#"{"shared":{"command":"node","args":["manifest.js"]},"only-manifest":{"command":"node"}}"#,
        );
        write(
            &plug,
            ".mcp.json",
            r#"{"mcpServers":{"shared":{"command":"node","args":["root.js"]},"only-root":{"command":"node"}}}"#,
        );
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();
        let mut names = plugin
            .mcp_servers
            .iter()
            .map(|s| s.name().to_string())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, vec!["only-manifest", "only-root", "shared"]);
        let shared = plugin
            .mcp_servers
            .iter()
            .find(|s| s.name() == "shared")
            .expect("the colliding name survives once");
        match shared {
            McpServerConfig::Stdio { args, .. } => assert_eq!(
                args.as_ref().map(|a| a.first().map(|s| s.to_string())),
                Some(Some("manifest.js".to_string())),
                "the manifest's entry wins the collision"
            ),
            other => panic!("expected stdio, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn surfaces_lsp_servers_raw() {
        let root = temp_dir();
        let plug = root.join("p");
        write(
            &plug,
            "plugin.json",
            r#"{"name":"p","lspServers":{"rust":{"command":"rust-analyzer"}}}"#,
        );
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();
        assert!(plugin.lsp_servers.contains_key("rust"));
    }

    #[tokio::test]
    async fn manifest_hooks_are_additive_deduped_against_the_standard_file_and_support_inline() {
        let root = temp_dir();
        let plug = root.join("p");
        write(
            &plug,
            "hooks/hooks.json",
            r#"{"PostToolUse":[{"hooks":[{"type":"command","command":"std"}]}]}"#,
        );
        write(
            &plug,
            "extra.json",
            r#"{"PreToolUse":[{"hooks":[{"type":"command","command":"extra"}]}]}"#,
        );
        // Entries in order: a back-reference to the standard file (deduped/skipped),
        // a second file, then an inline event-map.
        write(
            &plug,
            "plugin.json",
            r#"{"name":"p","hooks":["hooks/hooks.json","extra.json",{"SessionStart":[{"hooks":[{"type":"command","command":"start"}]}]}]}"#,
        );
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();
        let events: Vec<HookEvent> = plugin.hooks.iter().map(|h| h.event).collect();
        assert_eq!(
            events,
            vec![
                HookEvent::PostToolUse,
                HookEvent::PreToolUse,
                HookEvent::SessionStart
            ]
        );
    }

    #[tokio::test]
    async fn manifest_hooks_accept_a_bare_inline_event_map_object() {
        let root = temp_dir();
        let plug = root.join("bare");
        write(
            &plug,
            ".codex-plugin/plugin.json",
            r#"{"name":"bare","hooks":{}}"#,
        );
        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .expect("an object-valued hooks field must not fail the whole manifest");
        assert!(plugin.hooks.is_empty());

        let with = root.join("inline");
        write(
            &with,
            "plugin.json",
            r#"{"name":"inline","hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"start"}]}]}}"#,
        );
        let plugin = load_one_plugin(with.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();
        let events: Vec<HookEvent> = plugin.hooks.iter().map(|h| h.event).collect();
        assert_eq!(events, vec![HookEvent::SessionStart]);
    }

    #[tokio::test]
    async fn load_plugins_from_dir_loads_each_valid_subdir_and_skips_files() {
        let root = temp_dir();
        write(&root.join("one"), "plugin.json", r#"{"name":"one"}"#);
        write(
            &root.join("two"),
            ".klaus-plugin/plugin.json",
            r#"{"name":"two"}"#,
        );
        // A subdir without a manifest is skipped.
        std::fs::create_dir_all(root.join("nomanifest")).unwrap();
        // A plain file in the root is skipped.
        std::fs::write(root.join("loose.txt"), "x").unwrap();

        let plugins =
            load_plugins_from_dir(root.to_str().unwrap(), CommandLoadOptions::default()).await;
        let mut names: Vec<String> = plugins.iter().map(|p| p.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["one".to_string(), "two".to_string()]);
    }

    #[tokio::test]
    async fn load_report_surfaces_enabled_disabled_errors_warnings_and_runtime_capabilities() {
        let root = temp_dir();
        write(
            &root.join("enabled"),
            "plugin.json",
            r#"{"name":"enabled","runtimeCapabilities":["browser"]}"#,
        );
        write(
            &root.join("disabled"),
            "plugin.json",
            r#"{"name":"disabled","enabled":false}"#,
        );
        write(&root.join("bad-json"), "plugin.json", "{not json");
        write(
            &root.join("warn"),
            "plugin.json",
            r#"{"name":"warn","runtimeCapabilities":["browser","unknown-capability"]}"#,
        );

        let report =
            load_plugins_report_from_dir(root.to_str().unwrap(), CommandLoadOptions::default())
                .await;

        let enabled_names = report
            .enabled
            .iter()
            .map(|plugin| plugin.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(enabled_names, vec!["enabled", "warn"]);
        assert_eq!(
            report.enabled[0].runtime_capabilities,
            vec!["browser".to_string()]
        );
        assert_eq!(report.disabled.len(), 1);
        assert_eq!(
            report.disabled[0].name.as_ref(),
            Some(&RuntimeString::from("disabled"))
        );
        assert_eq!(report.errors.len(), 1);
        assert_eq!(report.errors[0].kind, PluginLoadIssueKind::InvalidJson);
        assert_eq!(report.warnings.len(), 1);
        assert_eq!(
            report.warnings[0].kind,
            PluginLoadIssueKind::UnsupportedRuntimeCapability
        );
    }

    #[tokio::test]
    async fn manifest_metadata_flows_into_loaded_plugin_identity() {
        let root = temp_dir();
        let plug = root.join("meta");
        write(
            &plug,
            "plugin.json",
            r#"{
              "name":"meta-plugin",
              "version":"1.2.3",
              "description":"  Adds   browser    automation.  ",
              "keywords":["browser","automation"],
              "dependencies":["auth@official","browser-core"],
              "userConfig":{
                "token":{"type":"string","title":"Token","description":"API token","sensitive":true}
              },
              "settings":{"agent":"browser"},
              "interface":{
                "displayName":"Meta Browser",
                "shortDescription":"Short browser summary",
                "capabilities":["browser-control","screenshots"]
              }
            }"#,
        );

        let plugin = load_one_plugin(plug.to_str().unwrap(), CommandLoadOptions::default())
            .await
            .unwrap();

        assert_eq!(plugin.display_name.as_deref(), Some("Meta Browser"));
        assert_eq!(
            plugin.description.as_deref(),
            Some("Adds browser automation.")
        );
        assert_eq!(plugin.plugin_namespace, "meta-plugin");
        assert_eq!(
            plugin.keywords,
            vec!["browser".to_string(), "automation".to_string()]
        );
        assert_eq!(
            plugin.interface_capabilities,
            vec!["browser-control".to_string(), "screenshots".to_string()]
        );
        assert_eq!(
            plugin.dependencies,
            vec!["auth@official".to_string(), "browser-core".to_string()]
        );
        assert!(plugin.user_config.contains_key("token"));
        assert_eq!(
            plugin.settings.get("agent"),
            Some(&serde_json::json!("browser"))
        );
    }

    #[tokio::test]
    async fn missing_dir_yields_no_plugins() {
        let root = temp_dir();
        let missing = root.join("does-not-exist");
        assert!(
            load_plugins_from_dir(missing.to_str().unwrap(), CommandLoadOptions::default())
                .await
                .is_empty()
        );
    }

    /// The helpers follow the host dialect, so the expectations are rendered with the
    /// host separator; the layout they describe is the same on every platform.
    #[test]
    fn path_helpers_match_host_semantics() {
        fn shaped(expected: &str) -> String {
            if cfg!(windows) {
                expected.replace('/', "\\")
            } else {
                expected.to_string()
            }
        }
        assert_eq!(path_join(&["/a/b", "c"]), shaped("/a/b/c"));
        assert_eq!(path_join(&["/a", ".."]), shaped("/"));
        assert_eq!(path_join(&["/a/b", "../c"]), shaped("/a/c"));
        assert_eq!(path_relative("/a/b", "/a/b/c"), "c");
        assert_eq!(path_relative("/a/b", "/a/x"), shaped("../x"));
        assert_eq!(path_relative("/a", "/a"), "");
        assert_eq!(
            resolve_under("/plugin", "commands").as_deref(),
            Some(shaped("/plugin/commands").as_str())
        );
        assert_eq!(resolve_under("/plugin", "../escape"), None);
    }
}
