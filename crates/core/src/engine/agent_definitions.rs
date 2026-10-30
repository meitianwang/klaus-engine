//! Agent definitions — named sub-agent roles that the `task` tool can delegate
//! to. Each definition gives a sub-agent a restricted tool allowlist, an optional
//! model override, and an optional custom system prompt, so a parent can spawn a
//! narrow, purpose-built child ("a read-only explorer", "a test-runner") instead
//! of an undifferentiated clone of itself.
//!
//! Definitions can be authored programmatically or loaded from Markdown files
//! (`.klaus/agents/*.md`) whose frontmatter carries the metadata and whose body
//! is the system prompt — mirroring how skills and slash commands are authored.
//!
//! This is the FULL, canonical agent definition (referenced widely: the session
//! registry, the built-in roles, the `task` tool). The reduced markdown loader in
//! `extensions::agent_loader` models only the flat-frontmatter subset for a
//! different authoring path; the two are intentionally distinct types.

use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;
use serde_json::Value;

use crate::engine::session::RuntimePolicyUpdate;
use crate::extensions::frontmatter::{parse_frontmatter, Frontmatter};
use crate::mcp::types::McpServerConfig;
use crate::memory::agent_memory::AgentMemoryScope;
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::effort::{parse_effort, EffortValue};
use crate::types::permission::PermissionMode;
use crate::types::tool::Isolation;

/// A named sub-agent role the `task` tool can delegate to (by `agentType`).
///
/// `Debug` is implemented by hand and `PartialEq` is intentionally not derived:
/// the `mcp_servers` field holds [`McpServerConfig`] values, an in-memory type
/// whose in-process variant carries a live server object with no `Debug`,
/// equality, or JSON representation. That field is programmatic-only and is
/// excluded from the exact runtime codec, matching the source, where it is never
/// written to the frontmatter or a rollout.
#[derive(Clone, Default)]
pub struct AgentDefinition {
    /// Engine-assigned provenance; host definitions are never allowed to claim it.
    pub builtin_identity: Option<RuntimeString>,
    /// Unique role name; the value passed as `agentType` to the `task` tool.
    pub name: RuntimeString,
    /// One-line description surfaced to the model so it knows when to delegate.
    pub description: RuntimeString,
    /// Allowlist of tool NAMES this agent may use. `None` inherits the parent's
    /// full tool set; an empty array yields a pure-reasoning agent with no tools.
    pub tools: Option<Vec<RuntimeString>>,
    /// Denylist of tool NAMES this agent may NOT use, applied AFTER `tools` — so
    /// it can subtract from an inherited full set.
    pub disallowed_tools: Option<Vec<RuntimeString>>,
    /// Model for this agent. Omitted or `inherit` keeps the parent's model.
    /// Accepts a tier (`low`/`medium`/`high`) or a concrete model id — tiers
    /// resolve against the session's tier map at spawn.
    pub model: Option<RuntimeString>,
    /// Canonical policy restrictions, intersected with the inherited authority.
    pub runtime_policy: Option<RuntimePolicyUpdate>,
    /// Per-agent reasoning effort: a named level (`low`|`medium`|`high`|`xhigh`|
    /// `max`) or an integer. At spawn it overrides the inherited session effort.
    /// Omitted = inherit the session effort.
    pub effort: Option<EffortValue>,
    /// Optional local orchestration selection, independent of native effort.
    pub ultra_mode: Option<bool>,
    /// Persistent per-agent memory scope: `user` | `project` | `local`. Presence
    /// both enables memory and selects the directory; the agent's `MEMORY.md`
    /// index is appended to its system prompt and the file tools are granted so it
    /// can persist. Omitted = no memory.
    pub memory: Option<AgentMemoryScope>,
    /// The agent's role system prompt. Appended as an extra cacheable system part
    /// so the child keeps the engine's base context (cwd / env / git) AND gains
    /// its role instructions.
    pub prompt: Option<RuntimeString>,
    /// Deprecated alias for `prompt`, kept for back-compat.
    pub system_prompt: Option<RuntimeString>,
    /// Experimental: a critical reminder appended to the system prompt as its own
    /// high-salience part.
    pub critical_system_reminder: Option<RuntimeString>,
    /// MCP servers exclusive to this sub-agent. They are connected when the agent
    /// spawns, their tools are ADDED to the child's tool set, and they are closed
    /// when the sub-agent finishes. Programmatic only — too structured for the
    /// simple `.md` frontmatter parser, and never serialized (the in-process
    /// variant has no JSON form).
    pub mcp_servers: Option<Vec<McpServerConfig>>,
    /// Default isolation for this agent. `worktree` runs every spawn of this agent
    /// in its own git worktree. A `task`-call `isolation` argument overrides this.
    /// Omitted = no isolation unless the call requests it.
    pub isolation: Option<Isolation>,
    /// Maximum number of agentic turns this agent runs before stopping. A positive
    /// integer that caps the child's loop budget (vs the session default).
    /// Omitted = the session/default budget.
    pub max_turns: Option<u32>,
    /// Always run this agent detached as a background task when spawned: a `task`
    /// delegation to it returns an id + result file immediately, even without an
    /// explicit `run_in_background`. Omitted/false = run synchronously unless the
    /// call asks for background.
    pub background: Option<bool>,
}

impl AgentDefinition {
    /// Reconstruct historical definition documents for migration fixtures.
    #[cfg(test)]
    pub(crate) fn legacy_runtime_encodings(&self) -> Result<Vec<RuntimeJson>, RuntimeCodecError> {
        let mut encodings = Vec::new();
        for mode in [
            PermissionMode::Default,
            PermissionMode::Bubble,
            PermissionMode::DontAsk,
            PermissionMode::AcceptEdits,
            PermissionMode::Auto,
            PermissionMode::BypassPermissions,
        ] {
            if migrate_agent_permission_mode(Some(mode)).ok().as_ref() != Some(&self.runtime_policy)
            {
                continue;
            }
            encodings.push(self.encode_legacy_runtime(mode)?);
        }
        Ok(encodings)
    }

    #[cfg(test)]
    fn encode_legacy_runtime(
        &self,
        mode: PermissionMode,
    ) -> Result<RuntimeJson, RuntimeCodecError> {
        let encoded = runtime_codec::encode_value(self)?;
        let Some(current) = encoded.as_object() else {
            unreachable!()
        };
        let mut legacy = crate::runtime_values::RuntimeObject::new();
        let mut inserted = false;
        for (key, value) in current.entries() {
            if key == "runtimePolicy" {
                legacy.insert("permissionMode", runtime_codec::encode_value(&mode)?);
                inserted = true;
            } else {
                if !inserted
                    && [
                        "ultraMode",
                        "effort",
                        "memory",
                        "prompt",
                        "systemPrompt",
                        "criticalSystemReminder",
                        "isolation",
                        "maxTurns",
                        "background",
                    ]
                    .iter()
                    .any(|name| key == *name)
                {
                    legacy.insert("permissionMode", runtime_codec::encode_value(&mode)?);
                    inserted = true;
                }
                legacy.insert(key.clone(), value.clone());
            }
        }
        if !inserted {
            legacy.insert("permissionMode", runtime_codec::encode_value(&mode)?);
        }
        Ok(RuntimeJson::Object(legacy))
    }

    pub(crate) fn allows_tool(&self, name: &RuntimeString) -> bool {
        self.tools.as_ref().is_none_or(|names| names.contains(name))
            && !self
                .disallowed_tools
                .as_ref()
                .is_some_and(|names| names.contains(name))
    }

    pub(crate) fn retain_tools(
        &self,
        tools: &mut Vec<std::sync::Arc<dyn crate::types::tool::Tool>>,
    ) {
        tools.retain(|tool| {
            self.allows_tool(&tool.name())
                || (!self
                    .disallowed_tools
                    .as_ref()
                    .is_some_and(|names| names.contains(&tool.name()))
                    && tool
                        .browser_dependency_owners()
                        .iter()
                        .any(|owner| self.allows_tool(owner)))
        });
    }

    /// Apply inherited execution defaults, not spawn-only lifecycle options.
    pub(crate) fn apply_root_config(
        &self,
        config: &mut crate::schemas::config::ResolvedConfig,
    ) -> Result<(), String> {
        if self
            .name
            .try_to_utf8()
            .map_or(true, |name| name.is_empty() || name.trim() != name)
            || self.max_turns == Some(0)
            || self.memory.is_some()
            || self.isolation.is_some()
            || self.background == Some(true)
            || self
                .mcp_servers
                .as_ref()
                .is_some_and(|servers| !servers.is_empty())
        {
            return Err(format!(
                "agent definition {:?} cannot run as root: expected a nonblank name, positive maxTurns, and no spawn-only memory/isolation/background/mcpServers",
                self.name
            ));
        }
        if let Some(model) = self.model.as_ref().filter(|model| *model != "inherit") {
            config.model = config.resolve_model_selection(model);
        }
        let mut effort = self.effort.clone();
        let mut ultra_mode = self.ultra_mode;
        crate::schemas::effort::normalize_legacy_ultra(&mut effort, &mut ultra_mode);
        config.effort = effort.or(config.effort.clone());
        config.ultra_mode = ultra_mode.unwrap_or(config.ultra_mode);
        if let Some(update) = &self.runtime_policy {
            let parent = crate::engine::session::RuntimePolicyState {
                approval_policy: config.approval_policy,
                sandbox_mode: config.sandbox_mode,
                approvals_reviewer: config.approvals_reviewer,
                generation: 0,
                sandbox_workspace_write: config.sandbox_workspace_write.clone(),
                network_allowed: crate::engine::session::runtime_network_allowed(
                    config.sandbox_mode,
                    None,
                    &config.sandbox_workspace_write,
                ),
            };
            let policy =
                crate::engine::session::subagent_runtime_policy_state_for_update(&parent, update)?;
            config.approval_policy = policy.approval_policy;
            config.sandbox_mode = policy.sandbox_mode;
            config.approvals_reviewer = policy.approvals_reviewer;
            config.sandbox_workspace_write = policy.sandbox_workspace_write;
            config.sandbox_workspace_write.network_access = policy.network_allowed;
        }
        Ok(())
    }
}

impl RuntimeEncode for AgentDefinition {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("description", &self.description)?;
        if let Some(identity) = &self.builtin_identity {
            object.field("builtinIdentity", identity)?;
        }
        if let Some(value) = &self.tools {
            object.field("tools", value)?;
        }
        if let Some(value) = &self.disallowed_tools {
            object.field("disallowedTools", value)?;
        }
        if let Some(value) = &self.model {
            object.field("model", value)?;
        }
        if let Some(value) = &self.runtime_policy {
            object.field("runtimePolicy", value)?;
        }
        if let Some(value) = &self.ultra_mode {
            object.field("ultraMode", value)?;
        }
        if let Some(value) = &self.effort {
            object.field("effort", value)?;
        }
        if let Some(value) = &self.memory {
            object.field("memory", value)?;
        }
        if let Some(value) = &self.prompt {
            object.field("prompt", value)?;
        }
        if let Some(value) = &self.system_prompt {
            object.field("systemPrompt", value)?;
        }
        if let Some(value) = &self.critical_system_reminder {
            object.field("criticalSystemReminder", value)?;
        }
        if let Some(Isolation::Worktree) = self.isolation {
            object.field("isolation", &RuntimeString::from_utf8("worktree"))?;
        }
        if let Some(value) = &self.max_turns {
            object.field("maxTurns", value)?;
        }
        if let Some(value) = &self.background {
            object.field("background", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AgentDefinition {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            let name = object.required("name", &[])?;
            let description = object.required("description", &[])?;
            let tools = object.optional("tools", &[])?;
            let disallowed_tools = object.optional("disallowedTools", &[])?;
            let model = object.optional("model", &[])?;
            let runtime_policy = object.optional("runtimePolicy", &[])?;
            let legacy_mode: Option<PermissionMode> = object.optional("permissionMode", &[])?;
            let legacy = migrate_agent_permission_mode(legacy_mode).map_err(|message| {
                RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(message.into()))
                    .prepend("permissionMode")
            })?;
            let runtime_policy = runtime_policy.or(legacy);
            let mut effort = object.optional("effort", &[])?;
            let mut ultra_mode = object.optional("ultraMode", &[])?;
            crate::schemas::effort::normalize_legacy_ultra(&mut effort, &mut ultra_mode);
            let memory = object.optional("memory", &[])?;
            let prompt = object.optional("prompt", &[])?;
            let system_prompt = object.optional("systemPrompt", &[])?;
            let critical_system_reminder = object.optional("criticalSystemReminder", &[])?;
            let isolation = object.optional::<RuntimeString>("isolation", &[])?;
            let isolation = match isolation {
                None => None,
                Some(value) if value == "worktree" => Some(Isolation::Worktree),
                Some(value) => {
                    return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                        RuntimeString::from_utf8("invalid isolation: ").concat(&value),
                    ))
                    .prepend("isolation"));
                }
            };
            let max_turns = object.optional("maxTurns", &[])?;
            let background = object.optional("background", &[])?;
            Ok(Self {
                builtin_identity: object.optional("builtinIdentity", &[])?,
                name,
                description,
                tools,
                disallowed_tools,
                model,
                runtime_policy,
                effort,
                ultra_mode,
                memory,
                prompt,
                system_prompt,
                critical_system_reminder,
                mcp_servers: None,
                isolation,
                max_turns,
                background,
            })
        })
    }
}

impl std::fmt::Debug for AgentDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `McpServerConfig` is not `Debug`; render the servers as their names.
        let mcp_servers = self
            .mcp_servers
            .as_ref()
            .map(|s| s.iter().map(|c| c.name().to_string()).collect::<Vec<_>>());
        f.debug_struct("AgentDefinition")
            .field("name", &self.name)
            .field("description", &self.description)
            .field("tools", &self.tools)
            .field("disallowed_tools", &self.disallowed_tools)
            .field("model", &self.model)
            .field("runtime_policy", &self.runtime_policy)
            .field("effort", &self.effort)
            .field("ultra_mode", &self.ultra_mode)
            .field("memory", &self.memory)
            .field("prompt", &self.prompt)
            .field("system_prompt", &self.system_prompt)
            .field("critical_system_reminder", &self.critical_system_reminder)
            .field("mcp_servers", &mcp_servers)
            .field("isolation", &self.isolation)
            .field("max_turns", &self.max_turns)
            .field("background", &self.background)
            .finish()
    }
}

/// A lookup table of [`AgentDefinition`]s keyed by name. Insertion order is
/// preserved; a later definition with the same name replaces the value in place
/// (keeping the original position), mirroring a JS `Map`.
#[derive(Debug, Clone, Default)]
pub struct AgentRegistry {
    order: Vec<AgentDefinition>,
    index: HashMap<RuntimeString, usize>,
}

impl AgentRegistry {
    /// Build a registry from a list of definitions. On a name collision the last
    /// definition wins (its value replaces the earlier one, keeping the earlier
    /// insertion position).
    pub fn new(defs: Vec<AgentDefinition>) -> Self {
        let mut reg = AgentRegistry::default();
        for def in defs {
            match reg.index.get(&def.name) {
                Some(&i) => reg.order[i] = def,
                None => {
                    let i = reg.order.len();
                    reg.index.insert(def.name.clone(), i);
                    reg.order.push(def);
                }
            }
        }
        reg
    }

    /// Look up a definition by name.
    pub fn get(&self, name: &RuntimeString) -> Option<&AgentDefinition> {
        self.index.get(name).map(|&i| &self.order[i])
    }

    /// Whether a definition with this name exists.
    pub fn has(&self, name: &RuntimeString) -> bool {
        self.index.contains_key(name)
    }

    /// Every definition, in insertion order.
    pub fn list(&self) -> Vec<AgentDefinition> {
        self.order.clone()
    }

    /// The number of distinct roles.
    pub fn size(&self) -> usize {
        self.order.len()
    }
}

/// Load agent definitions from a directory of `*.md` files (e.g.
/// `.klaus/agents/`). Missing/unreadable directories yield an empty list;
/// individual malformed files are skipped rather than failing the whole load.
/// Files are processed in sorted order for determinism.
pub async fn load_agent_definitions(dir: &str) -> Vec<AgentDefinition> {
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return Vec::new(),
    };

    // Collect `*.md` file NAMES, then sort for determinism.
    let mut files: Vec<String> = Vec::new();
    loop {
        match rd.next_entry().await {
            Ok(Some(entry)) => {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".md") {
                    files.push(name);
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    files.sort();

    let mut defs: Vec<AgentDefinition> = Vec::new();
    for file in &files {
        let path = Path::new(dir).join(file);
        if let Ok(raw) = tokio::fs::read_to_string(&path).await {
            if let Some(def) = parse_agent_markdown(&raw, file) {
                defs.push(def);
            }
        }
    }
    defs
}

/// Parse one agent Markdown document into a definition. Frontmatter keys:
/// `name` (defaults to the filename), `description`, `tools` / `allowed-tools`
/// (list or comma/space-separated string), `model`, `runtimePolicy`, `effort`,
/// `memory`, `disallowedTools` / `disallowed-tools`, `criticalSystemReminder` /
/// `critical-system-reminder`, `isolation`, `maxTurns`, `background`. The body
/// becomes the system prompt. Returns `None` only when no usable name can be
/// derived or a legacy planning-only definition requires explicit planning state.
pub fn parse_agent_markdown(raw: &str, file: &str) -> Option<AgentDefinition> {
    let Frontmatter { data, body } = parse_frontmatter(raw);

    let name = match data.get("name") {
        Some(Value::String(s)) if !s.trim().is_empty() => RuntimeString::from_utf8(s.trim()),
        _ => RuntimeString::from(base_name_no_ext(file)),
    };
    if name.is_empty() {
        return None;
    }

    let description = match data.get("description") {
        Some(Value::String(s)) => RuntimeString::from_utf8(s.trim()),
        _ => RuntimeString::default(),
    };
    let tools = parse_tool_list(data.get("tools").or_else(|| data.get("allowed-tools")));
    let disallowed_tools = parse_tool_list(
        data.get("disallowedTools")
            .or_else(|| data.get("disallowed-tools")),
    );
    // `inherit`/alias handling stays at spawn time — a concrete string is kept as-is.
    let model = match data.get("model") {
        Some(Value::String(s)) if !s.trim().is_empty() => Some(RuntimeString::from_utf8(s.trim())),
        _ => None,
    };
    let runtime_policy = match parse_runtime_policy(&data) {
        Ok(policy) => policy,
        Err(error) => {
            tracing::warn!(agent = ?name, %error, "Invalid agent runtime policy");
            return None;
        }
    };
    // Effort accepts named levels, custom strings and integers; malformed values are dropped.
    let mut effort = data.get("effort").and_then(parse_effort);
    let mut ultra_mode = data.get("ultraMode").and_then(Value::as_bool);
    crate::schemas::effort::normalize_legacy_ultra(&mut effort, &mut ultra_mode);
    // memory: a valid scope or dropped (invalid → None, not fatal).
    let memory = parse_memory_scope(data.get("memory"));
    // The markdown body is the agent's role prompt.
    let prompt = {
        let t = body.trim();
        if t.is_empty() {
            None
        } else {
            Some(RuntimeString::from_utf8(t))
        }
    };
    let critical_system_reminder = match data
        .get("criticalSystemReminder")
        .or_else(|| data.get("critical-system-reminder"))
    {
        Some(Value::String(s)) if !s.trim().is_empty() => Some(RuntimeString::from_utf8(s.trim())),
        _ => None,
    };
    // isolation: only the literal `worktree` is honored (`remote` is a host-only
    // cloud mode the engine doesn't run). Any other value is dropped.
    let isolation = match data.get("isolation") {
        Some(Value::String(s)) if s.trim() == "worktree" => Some(Isolation::Worktree),
        _ => None,
    };
    // maxTurns: a positive integer cap on the agent's loop. A number stays as-is;
    // anything else goes through base-10 `parseInt` (so "5"→5, "2.5"→2, "5abc"→5,
    // "1e2"→1, "0x10"→0), then it is kept only when a positive integer; else dropped.
    let max_turns: Option<u32> = match data.get("maxTurns") {
        Some(Value::Number(n)) => n.as_i64().filter(|&v| v > 0).map(|v| v as u32),
        None => None,
        Some(v) => parse_int_base10(&js_string_of(v))
            .filter(|&v| v > 0)
            .map(|v| v as u32),
    };
    // background: a strict bool ('true'/'false'/omitted). Stored only when true.
    let background = match data.get("background") {
        Some(Value::Bool(true)) => Some(true),
        Some(Value::String(s)) if s.trim() == "true" => Some(true),
        _ => None,
    };

    Some(AgentDefinition {
        name,
        description,
        tools,
        disallowed_tools,
        model,
        runtime_policy,
        effort,
        ultra_mode,
        memory,
        prompt,
        system_prompt: None,
        critical_system_reminder,
        mcp_servers: None,
        isolation,
        max_turns,
        background,
        builtin_identity: None,
    })
}

/// The name-splitting separator for a `tools` string: one-or-more whitespace or
/// commas (NOT parens-aware — this is the plain agent-definition split).
static TOOL_SEP: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[\s,]+").unwrap());

/// Normalize a frontmatter `tools` value (list or delimited string) to names.
/// Entries are trimmed and empties dropped; an empty result degrades to `None`.
fn parse_tool_list(value: Option<&Value>) -> Option<Vec<RuntimeString>> {
    match value {
        Some(Value::Array(arr)) => {
            let names: Vec<RuntimeString> = arr
                .iter()
                .filter_map(|v| v.as_str())
                .map(|v| RuntimeString::from_utf8(v.trim()))
                .filter(|s| !s.is_empty())
                .collect();
            if names.is_empty() {
                None
            } else {
                Some(names)
            }
        }
        Some(Value::String(s)) => {
            let names: Vec<RuntimeString> = TOOL_SEP
                .split(s)
                .map(|v| RuntimeString::from_utf8(v.trim()))
                .filter(|s| !s.is_empty())
                .collect();
            if names.is_empty() {
                None
            } else {
                Some(names)
            }
        }
        _ => None,
    }
}

/// Retired presets are consumed once at the definition input boundary.
fn migrate_agent_permission_mode(
    mode: Option<PermissionMode>,
) -> Result<Option<RuntimePolicyUpdate>, String> {
    use crate::types::permission::{ApprovalPolicy, ApprovalsReviewer, SandboxMode};
    let (approval_policy, sandbox_mode, approvals_reviewer) = match mode {
        None | Some(PermissionMode::Default | PermissionMode::Bubble) => return Ok(None),
        Some(PermissionMode::Plan) => return Err(
            "permissionMode=plan is no longer supported in agent definitions; select collaborationMode=plan for the session".into(),
        ),
        Some(PermissionMode::DontAsk) => (ApprovalPolicy::Never, SandboxMode::ReadOnly, ApprovalsReviewer::User),
        Some(PermissionMode::BypassPermissions) => (ApprovalPolicy::Never, SandboxMode::DangerFullAccess, ApprovalsReviewer::User),
        Some(PermissionMode::AcceptEdits) => (ApprovalPolicy::OnRequest, SandboxMode::WorkspaceWrite, ApprovalsReviewer::User),
        Some(PermissionMode::Auto) => (ApprovalPolicy::OnRequest, SandboxMode::WorkspaceWrite, ApprovalsReviewer::Automatic),
    };
    Ok(Some(RuntimePolicyUpdate {
        approval_policy: Some(approval_policy),
        sandbox_mode: Some(sandbox_mode),
        approvals_reviewer: Some(approvals_reviewer),
        sandbox_workspace_write: None,
        network_allowed: None,
    }))
}

pub(crate) fn parse_runtime_policy(
    data: &serde_json::Map<String, Value>,
) -> Result<Option<RuntimePolicyUpdate>, String> {
    let legacy_mode = data
        .get("permissionMode")
        .and_then(|value| serde_json::from_value::<PermissionMode>(value.clone()).ok());
    let legacy = migrate_agent_permission_mode(legacy_mode)?;
    let raw = if let Some(value) = data
        .get("runtimePolicy")
        .or_else(|| data.get("runtime_policy"))
    {
        Some(match value {
            Value::String(value) => value.clone(),
            value => serde_json::to_string(value).map_err(|error| error.to_string())?,
        })
    } else {
        let mut policy = serde_json::Map::new();
        for (key, alias) in [
            ("approvalPolicy", "approval_policy"),
            ("sandboxMode", "sandbox_mode"),
            ("approvalsReviewer", "approvals_reviewer"),
            ("networkAllowed", "network_allowed"),
            ("sandboxWorkspaceWrite", "sandbox_workspace_write"),
        ] {
            if let Some(value) = data.get(key).or_else(|| data.get(alias)) {
                let value = if key == "networkAllowed" {
                    match value {
                        Value::String(value) if value == "true" => Value::Bool(true),
                        Value::String(value) if value == "false" => Value::Bool(false),
                        value => value.clone(),
                    }
                } else if key == "sandboxWorkspaceWrite" {
                    match value {
                        Value::String(value) => {
                            serde_json::from_str(value).map_err(|error| error.to_string())?
                        }
                        value => value.clone(),
                    }
                } else {
                    value.clone()
                };
                policy.insert(key.into(), value);
            }
        }
        (!policy.is_empty()).then(|| Value::Object(policy).to_string())
    };
    if let Some(raw) = raw {
        let value = crate::runtime_values::runtime_json::parse(&RuntimeString::from(raw))
            .map_err(|error| error.to_string())?;
        return RuntimePolicyUpdate::decode_runtime(&value)
            .map(Some)
            .map_err(|error| error.to_string());
    }
    Ok(legacy)
}

impl RuntimeEncode for RuntimePolicyUpdate {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.approval_policy {
            object.field("approvalPolicy", value)?;
        }
        if let Some(value) = &self.sandbox_mode {
            object.field("sandboxMode", value)?;
        }
        if let Some(value) = &self.approvals_reviewer {
            object.field("approvalsReviewer", value)?;
        }
        if let Some(value) = &self.sandbox_workspace_write {
            object.field("sandboxWorkspaceWrite", value)?;
        }
        if let Some(value) = &self.network_allowed {
            object.field("networkAllowed", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for RuntimePolicyUpdate {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                approval_policy: object.optional("approvalPolicy", &["approval_policy"])?,
                sandbox_mode: object.optional("sandboxMode", &["sandbox_mode"])?,
                approvals_reviewer: object
                    .optional("approvalsReviewer", &["approvals_reviewer"])?,
                sandbox_workspace_write: object
                    .optional("sandboxWorkspaceWrite", &["sandbox_workspace_write"])?,
                network_allowed: object.optional("networkAllowed", &["network_allowed"])?,
            })
        })
    }
}

/// Coerce a frontmatter `memory` value to a valid scope, or `None`.
fn parse_memory_scope(value: Option<&Value>) -> Option<AgentMemoryScope> {
    let s = match value {
        Some(Value::String(s)) => s,
        _ => return None,
    };
    AgentMemoryScope::parse(s.trim())
}

/// The base name (after the last `/` or `\`) with a trailing `.md` removed.
fn base_name_no_ext(file: &str) -> String {
    let last_slash = file.rfind('/').map(|i| i as isize).unwrap_or(-1);
    let last_back = file.rfind('\\').map(|i| i as isize).unwrap_or(-1);
    let start = (last_slash.max(last_back) + 1) as usize;
    let base = &file[start..];
    base.strip_suffix(".md").unwrap_or(base).to_string()
}

/// Parse the leading base-10 integer of `s`, mirroring `parseInt(s, 10)`: skip
/// leading whitespace, read an optional sign, consume digits, and stop at the
/// first non-digit. Returns `None` (NaN) when no digits are present.
fn parse_int_base10(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let mut sign: i64 = 1;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        if bytes[i] == b'-' {
            sign = -1;
        }
        i += 1;
    }
    let start = i;
    let mut val: i64 = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        val = val
            .saturating_mul(10)
            .saturating_add((bytes[i] - b'0') as i64);
        i += 1;
    }
    if i == start {
        return None;
    }
    Some(sign * val)
}

/// Stringify a frontmatter value the way JS `String(x)` would (the small subset
/// the frontmatter parser can produce: strings and string lists).
fn js_string_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(a) => a.iter().map(js_string_of).collect::<Vec<_>>().join(","),
        Value::Object(_) => "[object Object]".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::effort::EffortLevel;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn root_agent_definition_applies_model_effort_and_canonical_policy_restrictions() {
        let mut config = crate::schemas::config::default_config();
        config.model = "parent-model".into();
        config.model_tiers.high = Some("specialist-model".into());
        let definition = AgentDefinition {
            name: "specialist".into(),
            model: Some("high".into()),
            effort: Some(EffortValue::Level(EffortLevel::High)),
            runtime_policy: migrate_agent_permission_mode(Some(PermissionMode::DontAsk)).unwrap(),
            ..Default::default()
        };
        definition.apply_root_config(&mut config).unwrap();
        assert_eq!(config.model, "specialist-model");
        assert_eq!(config.effort, definition.effort);
        assert_eq!(
            config.approval_policy,
            Some(crate::types::permission::ApprovalPolicy::Never)
        );
        assert_eq!(
            config.sandbox_mode,
            Some(crate::types::permission::SandboxMode::ReadOnly)
        );
        let broad = AgentDefinition {
            runtime_policy: migrate_agent_permission_mode(Some(PermissionMode::BypassPermissions))
                .unwrap(),
            ..definition.clone()
        };
        broad.apply_root_config(&mut config).unwrap();
        assert_eq!(
            config.sandbox_mode,
            Some(crate::types::permission::SandboxMode::ReadOnly),
            "a role declaration cannot expand the root filesystem ceiling"
        );
        assert_eq!(
            config.approval_policy,
            Some(crate::types::permission::ApprovalPolicy::Never)
        );
        let inherited = AgentDefinition {
            model: Some("inherit".into()),
            effort: None,
            ..definition
        };
        inherited.apply_root_config(&mut config).unwrap();
        assert_eq!(config.model, "specialist-model");
        assert_eq!(config.effort, Some(EffortValue::Level(EffortLevel::High)));
    }

    #[test]
    fn root_agent_definition_tool_policy_is_exact_and_deny_wins() {
        let mut definition = AgentDefinition::default();
        assert!(definition.allows_tool(&"read".into()));
        definition.tools = Some(vec![]);
        assert!(!definition.allows_tool(&"read".into()));
        definition.tools = Some(vec!["read".into(), "write".into()]);
        definition.disallowed_tools = Some(vec!["write".into(), "web_search".into()]);
        assert!(definition.allows_tool(&"read".into()));
        assert!(!definition.allows_tool(&"Read".into()));
        assert!(!definition.allows_tool(&"write".into()));
        assert!(!definition.allows_tool(&"tool_search".into()));
        definition.tools = None;
        assert!(!definition.allows_tool(&"web_search".into()));
    }

    #[test]
    fn root_agent_definition_rejects_spawn_only_semantics_instead_of_ignoring_them() {
        let valid = AgentDefinition {
            name: "specialist".into(),
            ..Default::default()
        };
        let invalid = [
            AgentDefinition {
                name: "".into(),
                ..valid.clone()
            },
            AgentDefinition {
                name: " specialist ".into(),
                ..valid.clone()
            },
            AgentDefinition {
                max_turns: Some(0),
                ..valid.clone()
            },
            AgentDefinition {
                memory: Some(AgentMemoryScope::Project),
                ..valid.clone()
            },
            AgentDefinition {
                isolation: Some(Isolation::Worktree),
                ..valid.clone()
            },
            AgentDefinition {
                background: Some(true),
                ..valid
            },
        ];
        for definition in invalid {
            let mut config = crate::schemas::config::default_config();
            assert!(
                definition.apply_root_config(&mut config).is_err(),
                "{definition:?}"
            );
        }
    }

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-agents-{}-{}-{}",
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

    #[test]
    fn parses_frontmatter_and_body_into_a_definition() {
        let def = parse_agent_markdown(
            "---\nname: explorer\ndescription: Read-only repo exploration\ntools:\n  - read\n  - grep\nmodel: fast-model\n---\nYou are a careful, read-only explorer. Never modify files.",
            "explorer.md",
        )
        .unwrap();
        assert_eq!(def.name, "explorer");
        assert_eq!(def.description, "Read-only repo exploration");
        assert_eq!(def.tools, Some(vec!["read".into(), "grep".into()]));
        assert!(def
            .model
            .as_ref()
            .is_some_and(|value| value == "fast-model"));
        assert_eq!(
            def.prompt.as_ref(),
            Some(&RuntimeString::from(
                "You are a careful, read-only explorer. Never modify files."
            ))
        );
        // No other fields set.
        assert!(def.disallowed_tools.is_none());
        assert!(def.runtime_policy.is_none());
        assert!(def.effort.is_none());
    }

    #[test]
    fn accepts_a_comma_space_separated_tools_string_and_allowed_tools_alias() {
        let def =
            parse_agent_markdown("---\nallowed-tools: read, grep glob\n---\nbody", "x.md").unwrap();
        assert_eq!(
            def.tools,
            Some(vec!["read".into(), "grep".into(), "glob".into()])
        );
    }

    #[test]
    fn defaults_the_name_to_the_filename_and_omits_an_empty_body() {
        let def = parse_agent_markdown("---\ndescription: d\n---\n", "planner.md").unwrap();
        assert_eq!(def.name, "planner");
        assert!(def.prompt.is_none());
    }

    #[test]
    fn parses_isolation_worktree_and_drops_other_values() {
        assert_eq!(
            parse_agent_markdown("---\nisolation: worktree\n---\nb", "a.md")
                .unwrap()
                .isolation,
            Some(Isolation::Worktree)
        );
        assert!(
            parse_agent_markdown("---\nisolation: remote\n---\nb", "a.md")
                .unwrap()
                .isolation
                .is_none()
        );
        assert!(parse_agent_markdown("---\nisolation: nope\n---\nb", "a.md")
            .unwrap()
            .isolation
            .is_none());
        assert!(parse_agent_markdown("---\ndescription: d\n---\nb", "a.md")
            .unwrap()
            .isolation
            .is_none());
    }

    #[test]
    fn parses_max_turns_like_base10_parse_int_dropping_non_positive() {
        assert_eq!(
            parse_agent_markdown("---\nmaxTurns: 5\n---\nb", "a.md")
                .unwrap()
                .max_turns,
            Some(5)
        );
        assert_eq!(
            parse_agent_markdown("---\nmaxTurns: \"8\"\n---\nb", "a.md")
                .unwrap()
                .max_turns,
            Some(8)
        );
        assert_eq!(
            parse_agent_markdown("---\nmaxTurns: 2.5\n---\nb", "a.md")
                .unwrap()
                .max_turns,
            Some(2)
        );
        assert_eq!(
            parse_agent_markdown("---\nmaxTurns: 5abc\n---\nb", "a.md")
                .unwrap()
                .max_turns,
            Some(5)
        );
        assert_eq!(
            parse_agent_markdown("---\nmaxTurns: 1e2\n---\nb", "a.md")
                .unwrap()
                .max_turns,
            Some(1)
        );
        for bad in ["0", "-3", "0x10", "nope"] {
            assert!(
                parse_agent_markdown(&format!("---\nmaxTurns: {bad}\n---\nb"), "a.md")
                    .unwrap()
                    .max_turns
                    .is_none(),
                "maxTurns: {bad} should drop"
            );
        }
        assert!(parse_agent_markdown("---\ndescription: d\n---\nb", "a.md")
            .unwrap()
            .max_turns
            .is_none());
    }

    #[test]
    fn parses_background_true_strictly() {
        assert_eq!(
            parse_agent_markdown("---\nbackground: true\n---\nb", "a.md")
                .unwrap()
                .background,
            Some(true)
        );
        assert_eq!(
            parse_agent_markdown("---\nbackground: \"true\"\n---\nb", "a.md")
                .unwrap()
                .background,
            Some(true)
        );
        assert!(
            parse_agent_markdown("---\nbackground: false\n---\nb", "a.md")
                .unwrap()
                .background
                .is_none()
        );
        assert!(
            parse_agent_markdown("---\nbackground: nope\n---\nb", "a.md")
                .unwrap()
                .background
                .is_none()
        );
        assert!(parse_agent_markdown("---\ndescription: d\n---\nb", "a.md")
            .unwrap()
            .background
            .is_none());
    }

    #[test]
    fn parses_disallowed_tools_frontmatter() {
        let def = parse_agent_markdown(
            "---\nname: x\ndisallowed-tools: write, bash\n---\nbody",
            "x.md",
        )
        .unwrap();
        assert_eq!(
            def.disallowed_tools,
            Some(vec!["write".into(), "bash".into()])
        );
    }

    #[test]
    fn parses_critical_system_reminder_and_body_prompt() {
        let def = parse_agent_markdown(
            "---\nname: x\ncriticalSystemReminder: stay safe\n---\nrole body",
            "x.md",
        )
        .unwrap();
        assert!(def
            .prompt
            .as_ref()
            .is_some_and(|value| value == "role body"));
        assert!(def
            .critical_system_reminder
            .as_ref()
            .is_some_and(|value| value == "stay safe"));
    }

    #[test]
    fn legacy_agent_presets_migrate_once_and_planning_requires_explicit_state() {
        use crate::types::permission::{ApprovalPolicy, ApprovalsReviewer, SandboxMode};
        for (mode, sandbox, approval, reviewer) in [
            (
                "dontAsk",
                SandboxMode::ReadOnly,
                ApprovalPolicy::Never,
                ApprovalsReviewer::User,
            ),
            (
                "bypassPermissions",
                SandboxMode::DangerFullAccess,
                ApprovalPolicy::Never,
                ApprovalsReviewer::User,
            ),
            (
                "acceptEdits",
                SandboxMode::WorkspaceWrite,
                ApprovalPolicy::OnRequest,
                ApprovalsReviewer::User,
            ),
            (
                "auto",
                SandboxMode::WorkspaceWrite,
                ApprovalPolicy::OnRequest,
                ApprovalsReviewer::Automatic,
            ),
        ] {
            let definition = parse_agent_markdown(
                &format!("---\nname: x\npermissionMode: {mode}\n---\nb"),
                "x.md",
            )
            .unwrap();
            let policy = definition.runtime_policy.as_ref().unwrap();
            assert_eq!(policy.sandbox_mode, Some(sandbox));
            assert_eq!(policy.approval_policy, Some(approval));
            assert_eq!(policy.approvals_reviewer, Some(reviewer));
            let encoded = runtime_codec::encode_value(&definition).unwrap();
            assert!(encoded.get("permissionMode").is_none());
            assert!(encoded.get("runtimePolicy").is_some());
            let decoded = AgentDefinition::decode_runtime(&encoded).unwrap();
            assert_eq!(decoded.runtime_policy, definition.runtime_policy);
        }
        assert!(
            parse_agent_markdown("---\nname: x\npermissionMode: plan\n---\nb", "x.md").is_none()
        );
        for mode in ["default", "bubble", "unknown"] {
            let definition = parse_agent_markdown(
                &format!("---\nname: x\npermissionMode: {mode}\n---\nb"),
                "x.md",
            )
            .unwrap();
            assert!(definition.runtime_policy.is_none());
        }
    }

    #[test]
    fn canonical_agent_policy_wins_over_legacy_presets() {
        use crate::types::permission::{ApprovalPolicy, SandboxMode};
        let definition = parse_agent_markdown(
            "---\nname: x\npermissionMode: bypassPermissions\nsandboxMode: read-only\napprovalPolicy: on-request\nnetworkAllowed: false\n---\nb", "x.md").unwrap();
        let policy = definition.runtime_policy.as_ref().unwrap();
        assert_eq!(policy.sandbox_mode, Some(SandboxMode::ReadOnly));
        assert_eq!(policy.approval_policy, Some(ApprovalPolicy::OnRequest));
        assert_eq!(policy.network_allowed, Some(false));
        let mut encoded = runtime_codec::encode_value(&definition).unwrap();
        encoded
            .as_object_mut()
            .unwrap()
            .insert("permissionMode", RuntimeJson::String("dontAsk".into()));
        let decoded = AgentDefinition::decode_runtime(&encoded).unwrap();
        assert_eq!(decoded.runtime_policy, definition.runtime_policy);
        encoded
            .as_object_mut()
            .unwrap()
            .insert("permissionMode", RuntimeJson::String("plan".into()));
        assert!(AgentDefinition::decode_runtime(&encoded).is_err());
    }

    #[test]
    fn markdown_agent_policy_accepts_canonical_key_spellings() {
        let definition = parse_agent_markdown(
            "---\nname: x\nsandbox_mode: workspace-write\napproval_policy: never\napprovals_reviewer: user\nnetwork_allowed: false\nsandbox_workspace_write: {\"writable_roots\":[\"/outside/approved\"]}\n---\nb", "x.md").unwrap();
        let policy = definition.runtime_policy.as_ref().unwrap();
        assert_eq!(
            policy.sandbox_mode,
            Some(crate::types::permission::SandboxMode::WorkspaceWrite)
        );
        assert_eq!(
            policy.approval_policy,
            Some(crate::types::permission::ApprovalPolicy::Never)
        );
        assert_eq!(policy.network_allowed, Some(false));
        assert_eq!(
            policy
                .sandbox_workspace_write
                .as_ref()
                .unwrap()
                .writable_roots,
            vec![RuntimeString::from("/outside/approved")]
        );
    }

    #[test]
    fn parses_named_numeric_and_custom_effort() {
        assert_eq!(
            parse_agent_markdown("---\nname: x\neffort: high\n---\nb", "x.md")
                .unwrap()
                .effort,
            Some(EffortValue::Level(EffortLevel::High))
        );
        assert_eq!(
            parse_agent_markdown("---\nname: x\neffort: 42\n---\nb", "x.md")
                .unwrap()
                .effort,
            Some(EffortValue::Number(42))
        );
        assert_eq!(
            parse_agent_markdown("---\nname: x\neffort: bogus\n---\nb", "x.md")
                .unwrap()
                .effort,
            Some(EffortValue::Custom("bogus".into()))
        );
    }

    #[test]
    fn parses_memory_scope_and_drops_invalid() {
        assert_eq!(
            parse_agent_markdown("---\nname: x\nmemory: user\n---\nb", "x.md")
                .unwrap()
                .memory,
            Some(AgentMemoryScope::User)
        );
        assert!(
            parse_agent_markdown("---\nname: x\nmemory: galaxy\n---\nb", "x.md")
                .unwrap()
                .memory
                .is_none()
        );
    }

    #[tokio::test]
    async fn loads_md_agent_files_ignoring_others_in_sorted_order() {
        let dir = temp_dir();
        std::fs::write(
            dir.join("a.md"),
            "---\nname: a\ndescription: A\n---\nbody a",
        )
        .unwrap();
        std::fs::write(
            dir.join("b.md"),
            "---\nname: b\ndescription: B\n---\nbody b",
        )
        .unwrap();
        std::fs::write(dir.join("notes.txt"), "ignored").unwrap();
        let defs = load_agent_definitions(dir.to_str().unwrap()).await;
        assert_eq!(
            defs.iter().map(|d| d.name.clone()).collect::<Vec<_>>(),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[tokio::test]
    async fn returns_empty_for_a_missing_directory() {
        let dir = temp_dir();
        let missing = dir.join("nope");
        assert!(load_agent_definitions(missing.to_str().unwrap())
            .await
            .is_empty());
    }

    #[test]
    fn registry_looks_up_lists_and_counts() {
        let reg = AgentRegistry::new(vec![
            AgentDefinition {
                name: "a".into(),
                description: "A".into(),
                ..Default::default()
            },
            AgentDefinition {
                name: "b".into(),
                description: "B".into(),
                ..Default::default()
            },
        ]);
        assert!(reg.has(&RuntimeString::from("a")));
        assert!(reg
            .get(&RuntimeString::from("b"))
            .is_some_and(|definition| definition.description == "B"));
        assert_eq!(reg.size(), 2);
        assert_eq!(
            reg.list()
                .iter()
                .map(|d| d.name.clone())
                .collect::<Vec<_>>(),
            vec![RuntimeString::from("a"), RuntimeString::from("b")]
        );
    }

    #[test]
    fn registry_last_wins_on_collision_keeping_position() {
        let reg = AgentRegistry::new(vec![
            AgentDefinition {
                name: "a".into(),
                description: "first".into(),
                ..Default::default()
            },
            AgentDefinition {
                name: "b".into(),
                description: "B".into(),
                ..Default::default()
            },
            AgentDefinition {
                name: "a".into(),
                description: "second".into(),
                ..Default::default()
            },
        ]);
        assert_eq!(reg.size(), 2);
        assert!(reg
            .get(&RuntimeString::from("a"))
            .is_some_and(|definition| definition.description == "second"));
        // Position preserved: a stays first.
        assert_eq!(
            reg.list()
                .iter()
                .map(|d| d.name.clone())
                .collect::<Vec<_>>(),
            vec![RuntimeString::from("a"), RuntimeString::from("b")]
        );
    }

    #[test]
    fn agent_definition_runtime_codec_is_camel_case_and_omits_none() {
        let def = AgentDefinition {
            name: "r".into(),
            description: "d".into(),
            disallowed_tools: Some(vec!["write".into()]),
            max_turns: Some(12),
            ..Default::default()
        };
        let v = runtime_codec::encode_value(&def).unwrap();
        assert_eq!(v["name"], RuntimeJson::from("r"));
        assert_eq!(v["disallowedTools"][0], RuntimeJson::from("write"));
        assert_eq!(v["maxTurns"], RuntimeJson::from(12));
        // Unset optionals are omitted.
        assert!(v.get("tools").is_none());
        assert!(v.get("permissionMode").is_none());
        // Round-trips (compared via their JSON, since AgentDefinition is not PartialEq).
        let back: AgentDefinition = runtime_codec::decode_value(&v).unwrap();
        assert_eq!(runtime_codec::encode_value(&back).unwrap(), v);
    }

    #[test]
    fn agent_definition_isolation_wire_contract() {
        let definition = AgentDefinition {
            name: "isolated".into(),
            description: "wire contract".into(),
            isolation: Some(Isolation::Worktree),
            ..Default::default()
        };
        let wire = runtime_codec::encode_value(&definition).unwrap();
        assert_eq!(wire["isolation"], RuntimeJson::from("worktree"));

        let decoded: AgentDefinition = runtime_codec::decode_value(&wire).unwrap();
        assert_eq!(decoded.isolation, Some(Isolation::Worktree));

        let mut null = wire.clone();
        null["isolation"] = RuntimeJson::Null;
        let decoded_null: AgentDefinition = runtime_codec::decode_value(&null).unwrap();
        assert_eq!(decoded_null.isolation, None);

        let mut invalid = wire;
        invalid["isolation"] = RuntimeJson::from("container");
        let error = runtime_codec::decode_value::<AgentDefinition>(&invalid).unwrap_err();
        assert!(matches!(error.kind(), RuntimeCodecErrorKind::Custom(_)));
        assert_eq!(error.pointer(), RuntimeString::from("/isolation"));
    }

    #[test]
    fn agent_definition_runtime_codec_and_registry_preserve_exact_utf16_identity() {
        let non_scalar = RuntimeString::from_utf16([b'a' as u16, 0xd800, b'b' as u16]);
        let definition = AgentDefinition {
            name: non_scalar.clone(),
            description: non_scalar.clone(),
            tools: Some(vec![non_scalar.clone()]),
            disallowed_tools: Some(vec![non_scalar.clone()]),
            model: Some(non_scalar.clone()),
            prompt: Some(non_scalar.clone()),
            system_prompt: Some(non_scalar.clone()),
            critical_system_reminder: Some(non_scalar.clone()),
            ..Default::default()
        };

        let encoded = runtime_codec::encode_value(&definition).unwrap();
        let decoded = runtime_codec::decode_value::<AgentDefinition>(&encoded).unwrap();
        assert_eq!(decoded.name, non_scalar);
        assert_eq!(decoded.description, non_scalar);
        assert_eq!(decoded.tools, Some(vec![non_scalar.clone()]));
        assert_eq!(decoded.disallowed_tools, Some(vec![non_scalar.clone()]));
        assert_eq!(decoded.model, Some(non_scalar.clone()));
        assert_eq!(decoded.prompt, Some(non_scalar.clone()));
        assert_eq!(decoded.system_prompt, Some(non_scalar.clone()));
        assert_eq!(decoded.critical_system_reminder, Some(non_scalar.clone()));

        let replacement = RuntimeString::from("a\u{fffd}b");
        let registry = AgentRegistry::new(vec![
            definition,
            AgentDefinition {
                name: replacement.clone(),
                description: "replacement".into(),
                ..Default::default()
            },
        ]);
        assert_eq!(registry.size(), 2);
        assert!(registry.get(&non_scalar).is_some());
        assert!(registry.get(&replacement).is_some());
    }
}

#[cfg(test)]
mod ultra_mode_boundary_tests {
    use super::*;
    use crate::schemas::effort::EffortLevel;

    #[test]
    fn agent_runtime_preserves_raw_ultra_without_overwriting_explicit_mode() {
        for (extra, expected) in [("", true), (",\"ultraMode\":false", false)] {
            let json = RuntimeString::from(format!(
                "{{\"name\":\"worker\",\"description\":\"Inspect the task\",\"effort\":\"ultra\"{extra}}}"
            ));
            let definition: AgentDefinition = runtime_codec::decode_text(&json).unwrap();
            assert_eq!(
                definition.effort,
                Some(EffortValue::Level(EffortLevel::Ultra))
            );
            assert_eq!(definition.ultra_mode, Some(expected));
        }
    }
}
