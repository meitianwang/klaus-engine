//! Load markdown sub-agent definitions from an `agents/` directory: each `*.md`
//! file's frontmatter supplies name/description/tools/model/…, and its body
//! becomes the agent's role system prompt. The result plugs into the engine's
//! agent registry. (The richer programmatic-only fields — MCP servers, critical
//! system reminder — are intentionally out of reach of the flat frontmatter
//! parser.)

use crate::engine::session::RuntimePolicyUpdate;
use crate::extensions::frontmatter::{
    coerce_description_to_string, extract_description_from_markdown, parse_frontmatter,
    parse_optional_bool_field, Frontmatter,
};
use crate::schemas::effort::{parse_effort, EffortValue};
use crate::types::json::JsonObject;
use crate::types::permission::ExternalPermissionMode;
use regex::Regex;
use serde_json::Value;
use std::sync::LazyLock;

/// A named sub-agent role loaded from markdown frontmatter + body. Only the
/// fields reachable from the flat frontmatter parser are modeled here.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentDefinition {
    /// Unique role name (the value delegated to as an agent type).
    pub name: String,
    /// One-line description surfaced to the model so it knows when to delegate.
    pub description: String,
    /// Allowlist of tool names this agent may use.
    pub tools: Option<Vec<String>>,
    /// Denylist of tool names this agent may NOT use, applied after `tools`.
    pub disallowed_tools: Option<Vec<String>>,
    /// Model for this agent (omitted or `inherit` keeps the parent's model).
    pub model: Option<String>,
    /// Canonical policy restrictions applied below the inherited parent ceiling.
    pub runtime_policy: Option<RuntimePolicyUpdate>,
    /// Per-agent reasoning effort: a named level or an integer.
    pub effort: Option<EffortValue>,
    /// Optional local orchestration selection.
    pub ultra_mode: Option<bool>,
    /// The agent's role system prompt (the markdown body).
    pub prompt: Option<String>,
}

/// A frontmatter value that is either a delimited string or a list of strings.
enum StrOrList {
    Str(String),
    List(Vec<String>),
}

/// Validated agent frontmatter fields (typed shape only — `description` /
/// `when-to-use` / `effort` are left as raw values / read separately).
struct AgentFm {
    name: Option<String>,
    description: Option<Value>,
    when_to_use: Option<Value>,
    tools: Option<StrOrList>,
    disallowed_tools: Option<StrOrList>,
    model: Option<String>,
    runtime_policy: Option<RuntimePolicyUpdate>,
    ultra_mode: Option<bool>,
}

/// Read an optional string field: absent → `None`, a string → the string,
/// anything else → schema error.
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

/// Validate the agent frontmatter shape. A bad typed field (e.g. a non-string
/// `model`, or a `permission-mode` outside the allowed set) fails the whole
/// parse, so the agent is skipped rather than partially loaded.
fn validate_agent_frontmatter(data: &JsonObject) -> Result<AgentFm, String> {
    let name = get_string_field(data, "name")?;
    let tools = get_str_or_list_field(data, "tools")?;
    let disallowed_tools = get_str_or_list_field(data, "disallowed-tools")?;
    let model = get_string_field(data, "model")?;
    let mut policy_data = data.clone();
    if let Some(value) = data
        .get("permission-mode")
        .or_else(|| data.get("permissionMode"))
    {
        let mode: ExternalPermissionMode = match value {
            Value::String(value) => serde_json::from_value(Value::String(value.clone()))
                .map_err(|_| "\"permission-mode\": invalid enum value".to_owned())?,
            _ => return Err("\"permission-mode\": expected string".to_owned()),
        };
        policy_data.insert(
            "permissionMode".into(),
            serde_json::to_value(mode).map_err(|error| error.to_string())?,
        );
    }
    let runtime_policy = crate::engine::agent_definitions::parse_runtime_policy(&policy_data)?;
    let description = data.get("description").cloned();
    let when_to_use = data.get("when-to-use").cloned();
    Ok(AgentFm {
        name,
        description,
        when_to_use,
        tools,
        disallowed_tools,
        model,
        runtime_policy,
        ultra_mode: parse_optional_bool_field(data, "ultraMode")?,
    })
}

static TOOL_SPLIT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[,\s]+").unwrap());

/// Split a `tools` frontmatter field — a comma/whitespace string or a list —
/// into tool names. Entries are trimmed and empties dropped; an empty result
/// degrades to `None`.
fn to_tool_list(value: Option<StrOrList>) -> Option<Vec<String>> {
    let value = value?;
    let list: Vec<String> = match value {
        StrOrList::List(l) => l,
        StrOrList::Str(s) => TOOL_SPLIT.split(&s).map(|x| x.to_string()).collect(),
    };
    let cleaned: Vec<String> = list
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

/// Strip a trailing `.md` (case-insensitive) extension from a file name.
fn strip_md_ext(name: &str) -> String {
    let b = name.as_bytes();
    let n = b.len();
    if n >= 3
        && b[n - 3] == b'.'
        && b[n - 2].eq_ignore_ascii_case(&b'm')
        && b[n - 1].eq_ignore_ascii_case(&b'd')
    {
        name[..n - 3].to_string()
    } else {
        name.to_string()
    }
}

/// The final path component, in the host's dialect.
///
/// A `/`-only version returned the entire `C:\\dir\\name.md` string on Windows, so the
/// asset was named after its full path instead of its file.
use crate::permission::bash::node_path::basename;

/// Whether a file name ends with `.md` (case-insensitive).
fn is_md(name: &str) -> bool {
    let b = name.as_bytes();
    let n = b.len();
    n >= 3
        && b[n - 3] == b'.'
        && b[n - 2].eq_ignore_ascii_case(&b'm')
        && b[n - 1].eq_ignore_ascii_case(&b'd')
}

async fn load_one_agent(file_path: &str) -> Option<AgentDefinition> {
    let raw = match tokio::fs::read_to_string(file_path).await {
        Ok(r) => r,
        Err(_) => return None,
    };
    let Frontmatter { data, body } = parse_frontmatter(&raw);
    let fm = match validate_agent_frontmatter(&data) {
        Ok(fm) => fm,
        Err(reason) => {
            tracing::warn!(
                "[extensions] skipping invalid agent at {}: {}",
                file_path,
                reason
            );
            return None;
        }
    };

    let name = fm
        .name
        .unwrap_or_else(|| strip_md_ext(&basename(file_path)));
    // description → `when-to-use` → markdown H1/first line → 'Agent'.
    let description = coerce_description_to_string(fm.description.as_ref())
        .or_else(|| coerce_description_to_string(fm.when_to_use.as_ref()))
        .unwrap_or_else(|| extract_description_from_markdown(&body, "Agent"));

    let mut def = AgentDefinition {
        name,
        description,
        tools: None,
        disallowed_tools: None,
        model: None,
        runtime_policy: None,
        effort: None,
        ultra_mode: None,
        prompt: None,
    };

    if let Some(tools) = to_tool_list(fm.tools) {
        def.tools = Some(tools);
    }
    if let Some(disallowed) = to_tool_list(fm.disallowed_tools) {
        def.disallowed_tools = Some(disallowed);
    }
    // `model: inherit` is the sentinel for "use the parent model" → leave unset.
    if let Some(model) = fm.model {
        if model != "inherit" {
            def.model = Some(model);
        }
    }
    def.runtime_policy = fm.runtime_policy;
    def.ultra_mode = fm.ultra_mode;
    // effort: a bad value degrades the field, not the whole agent.
    let effort = match data.get("effort") {
        Some(v) => parse_effort(v),
        None => None,
    };
    if let Some(effort) = effort {
        def.effort = Some(effort);
    }
    crate::schemas::effort::normalize_legacy_ultra(&mut def.effort, &mut def.ultra_mode);
    let prompt = body.trim();
    if !prompt.is_empty() {
        def.prompt = Some(prompt.to_string());
    }
    Some(def)
}

/// Load a single `*.md` agent definition file. `None` on failure.
pub async fn load_agent_from_file(file_path: &str) -> Option<AgentDefinition> {
    load_one_agent(file_path).await
}

/// Load every `*.md` agent definition in `dir`. Invalid agents are skipped; a
/// missing dir yields `[]`.
pub async fn load_agents_from_dir(dir: &str) -> Vec<AgentDefinition> {
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return Vec::new(),
    };
    let mut agents: Vec<AgentDefinition> = Vec::new();
    loop {
        let entry = match rd.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(_) => break,
        };
        let file_type = match entry.file_type().await {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if !file_type.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if !is_md(&file_name) {
            continue;
        }
        let path = entry.path();
        let path = path.to_string_lossy().into_owned();
        if let Some(agent) = load_one_agent(&path).await {
            agents.push(agent);
        }
    }
    agents
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::effort::EffortLevel;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-agent-{}-{}-{}",
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

    fn write_file(dir: &Path, rel: &str, content: &str) -> PathBuf {
        let full = dir.join(rel);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&full, content).unwrap();
        full
    }

    #[tokio::test]
    async fn parses_name_tools_disallowed_model_canonical_policy_effort_and_body_prompt() {
        let dir = temp_dir();
        write_file(
            &dir,
            "planner.md",
            &[
                "---",
                "description: plans work",
                "tools: Read Grep",
                "disallowed-tools: Bash, Write",
                "model: claude-opus-4-8",
                "permission-mode: dontAsk",
                "effort: high",
                "---",
                "You plan.",
            ]
            .join("\n"),
        );
        let agents = load_agents_from_dir(dir.to_str().unwrap()).await;
        assert_eq!(agents.len(), 1);
        let a = &agents[0];
        assert_eq!(a.name, "planner");
        assert_eq!(a.description, "plans work");
        assert_eq!(a.tools, Some(vec!["Read".to_string(), "Grep".to_string()]));
        assert_eq!(
            a.disallowed_tools,
            Some(vec!["Bash".to_string(), "Write".to_string()])
        );
        assert_eq!(a.model.as_deref(), Some("claude-opus-4-8"));
        let policy = a.runtime_policy.as_ref().unwrap();
        assert_eq!(
            policy.sandbox_mode,
            Some(crate::types::permission::SandboxMode::ReadOnly)
        );
        assert_eq!(
            policy.approval_policy,
            Some(crate::types::permission::ApprovalPolicy::Never)
        );
        assert_eq!(a.effort, Some(EffortValue::Level(EffortLevel::High)));
        assert_eq!(a.prompt.as_deref(), Some("You plan."));
    }

    #[tokio::test]
    async fn canonical_policy_wins_and_legacy_plan_requires_explicit_planning_state() {
        let dir = temp_dir();
        write_file(&dir, "restricted.md", "---\npermission-mode: bypassPermissions\nsandboxMode: read-only\napprovalPolicy: on-request\n---\nInspect files.");
        write_file(
            &dir,
            "old-plan.md",
            "---\npermission-mode: plan\n---\nPlan work.",
        );
        let agents = load_agents_from_dir(dir.to_str().unwrap()).await;
        assert_eq!(agents.len(), 1);
        let policy = agents[0].runtime_policy.as_ref().unwrap();
        assert_eq!(
            policy.sandbox_mode,
            Some(crate::types::permission::SandboxMode::ReadOnly)
        );
        assert_eq!(
            policy.approval_policy,
            Some(crate::types::permission::ApprovalPolicy::OnRequest)
        );
    }

    #[tokio::test]
    async fn falls_back_to_the_markdown_h1_when_description_is_absent() {
        let dir = temp_dir();
        write_file(
            &dir,
            "writer.md",
            "---\nname: writer\n---\n# Writes prose\nbody",
        );
        let agents = load_agents_from_dir(dir.to_str().unwrap()).await;
        assert_eq!(agents.len(), 1);
        let a = &agents[0];
        assert_eq!(a.name, "writer");
        assert_eq!(a.description, "Writes prose");
    }
}
