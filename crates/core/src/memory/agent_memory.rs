//! Per-agent persistent memory. An agent definition declares a SCOPE; the scope
//! both enables memory and selects which directory the agent reads from / writes
//! to:
//!  - `user`    — durable across every session for this agent role.
//!  - `project` — shared within the current project/checkout.
//!  - `local`   — ephemeral to this working copy.
//!
//! The engine is only the consumer: it resolves the directory (via a
//! host-pluggable layout), injects the `MEMORY.md` index into the agent's system
//! prompt, and runs the user-scope snapshot-sync state machine. The host owns the
//! concrete directory conventions and the interactive merge/keep/replace decision.

use crate::fs::paths::{config_home, find_project_root, project_slug, resolve_main_repo_root};
use crate::memory::sync::{
    plan_memory_sync, MemorySyncApplyResult, MemorySyncError, MemorySyncInput, MemorySyncPlan,
};
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, runtime_json, NodeBufferUtf8, RuntimeCodecError, RuntimeCodecErrorKind,
    RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::io;
use std::sync::LazyLock;

use crate::memory::snapshot_decision::{
    AgentMemorySnapshotDecisionPolicy, AgentMemorySnapshotDecisionRequest,
};

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

/// The scope an agent's memory lives in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentMemoryScope {
    User,
    Project,
    Local,
}

impl RuntimeEncode for AgentMemoryScope {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::User => "user",
            Self::Project => "project",
            Self::Local => "local",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for AgentMemoryScope {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("user", &[]),
                RuntimeEnumVariant::new("project", &[]),
                RuntimeEnumVariant::new("local", &[]),
            ],
            &[Self::User, Self::Project, Self::Local],
        )
    }
}

impl AgentMemoryScope {
    /// The wire/string form (`user` | `project` | `local`).
    pub fn as_str(&self) -> &'static str {
        match self {
            AgentMemoryScope::User => "user",
            AgentMemoryScope::Project => "project",
            AgentMemoryScope::Local => "local",
        }
    }

    /// Parse a scope string, rejecting anything outside the enumerated set.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "user" => Some(AgentMemoryScope::User),
            "project" => Some(AgentMemoryScope::Project),
            "local" => Some(AgentMemoryScope::Local),
            _ => None,
        }
    }
}

/// The index file inside a memory directory (the `MEMORY.md` entrypoint).
const ENTRYPOINT: &str = "MEMORY.md";
/// Snapshot source metadata (timestamped); lives in the snapshot source dir.
const SNAPSHOT_META: &str = "snapshot.json";
/// Local sync marker recording which snapshot timestamp was last applied.
const SYNCED_META: &str = ".snapshot-synced.json";
/// The inlined index is bounded (200 lines / 25 KB) so memory cannot blow the
/// context.
const MAX_ENTRYPOINT_LINES: usize = 200;
const MAX_ENTRYPOINT_BYTES: usize = 25_000;

/// Resolves on-disk locations for an agent's memory by scope (host-pluggable).
pub trait AgentMemoryLayout: Send + Sync {
    /// The memory directory for an agent type + scope.
    fn dir_for(&self, agent_type: &str, scope: AgentMemoryScope) -> String;
    /// The snapshot SOURCE directory for a user-scope agent (typically
    /// VCS-committed).
    fn snapshot_dir_for(&self, agent_type: &str) -> String;
}

/// Legacy callback envelope retained while Session migrates to the correlated
/// [`crate::memory::snapshot_decision::AgentMemorySnapshotDecisionRequest`].
/// New Host bridges must use the correlated request rather than constructing
/// this timestamp-only value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySnapshotUpdate {
    pub agent_type: RuntimeString,
    pub snapshot_timestamp: RuntimeString,
}

impl RuntimeEncode for AgentMemorySnapshotUpdate {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("agentType", &self.agent_type)?;
        object.field("snapshotTimestamp", &self.snapshot_timestamp)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AgentMemorySnapshotUpdate {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                agent_type: object.required("agentType", &[])?,
                snapshot_timestamp: object.required("snapshotTimestamp", &[])?,
            })
        })
    }
}

/// A request to plan host-backed memory sync for one agent memory directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySyncRequest {
    pub agent_type: RuntimeString,
    pub scope: AgentMemoryScope,
    pub dir: RuntimeString,
}

impl RuntimeEncode for AgentMemorySyncRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("agentType", &self.agent_type)?;
        object.field("scope", &self.scope)?;
        object.field("dir", &self.dir)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AgentMemorySyncRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                agent_type: object.required("agentType", &[])?,
                scope: object.required("scope", &[])?,
                dir: object.required("dir", &[])?,
            })
        })
    }
}

/// Host resolution of a pending snapshot update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentMemoryMergeChoice {
    Merge,
    Keep,
    Replace,
}

impl RuntimeEncode for AgentMemoryMergeChoice {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Merge => "merge",
            Self::Keep => "keep",
            Self::Replace => "replace",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for AgentMemoryMergeChoice {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("merge", &[]),
                RuntimeEnumVariant::new("keep", &[]),
                RuntimeEnumVariant::new("replace", &[]),
            ],
            &[Self::Merge, Self::Keep, Self::Replace],
        )
    }
}

/// Host adapter wiring the memory feature to concrete paths + the update
/// decision.
#[async_trait::async_trait]
pub trait AgentMemoryHost: Send + Sync {
    /// Resolves on-disk memory locations by scope.
    fn layout(&self) -> &dyn AgentMemoryLayout;
    /// Explicit first-use and Host-unavailable behavior. The default seeds a
    /// first-use snapshot like CC and fails closed for timeout, disconnect, and
    /// no-handler edges.
    fn snapshot_decision_policy(&self) -> AgentMemorySnapshotDecisionPolicy {
        AgentMemorySnapshotDecisionPolicy::default()
    }
    /// Resolve the correlated snapshot request used by the authoritative Host
    /// bridge. `None` is an explicit no-handler result and is governed by
    /// [`Self::snapshot_decision_policy`]; it never aliases to `keep`.
    async fn on_snapshot_decision_request(
        &self,
        request: &AgentMemorySnapshotDecisionRequest,
    ) -> Option<AgentMemoryMergeChoice> {
        let _ = request;
        None
    }
    /// Resolve a pending user-scope snapshot update. `merge` returns a prompt
    /// prefix the engine prepends to the agent's first message; `replace`
    /// overwrites local memory from the snapshot; `keep` retains local memory and
    /// records the sync. This timestamp-only callback is retained for source
    /// compatibility; new Session code uses `on_snapshot_decision_request`.
    async fn on_snapshot_update(
        &self,
        update: &AgentMemorySnapshotUpdate,
    ) -> AgentMemoryMergeChoice {
        let _ = update;
        panic!("snapshot update handler was not installed")
    }

    /// Optional host-backed local/remote memory state. Returning `None` keeps the
    /// engine in local-only file mode.
    async fn memory_sync_input(&self, request: &AgentMemorySyncRequest) -> Option<MemorySyncInput> {
        let _ = request;
        None
    }

    /// Observe the sync plan or typed planning error. The host decides whether
    /// and how to apply the returned operations.
    async fn on_memory_sync_plan(
        &self,
        request: &AgentMemorySyncRequest,
        result: Result<MemorySyncPlan, MemorySyncError>,
    ) {
        let _ = (request, result);
    }

    /// Apply a successful sync plan through the host's real local/remote backend.
    /// The default is a no-op so local-only hosts can observe plans without
    /// adopting remote synchronization.
    async fn apply_memory_sync_plan(
        &self,
        request: &AgentMemorySyncRequest,
        plan: &MemorySyncPlan,
    ) -> Result<MemorySyncApplyResult, MemorySyncError> {
        let _ = (request, plan);
        Ok(MemorySyncApplyResult {
            applied_operations: 0,
        })
    }

    /// Observe the host apply result. This is called only after planning
    /// succeeds and `apply_memory_sync_plan` returns.
    async fn on_memory_sync_apply_result(
        &self,
        request: &AgentMemorySyncRequest,
        result: Result<MemorySyncApplyResult, MemorySyncError>,
    ) {
        let _ = (request, result);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMemorySyncResult {
    pub plan: Result<MemorySyncPlan, MemorySyncError>,
    pub apply: Option<Result<MemorySyncApplyResult, MemorySyncError>>,
}

pub async fn run_agent_memory_sync(
    host: &dyn AgentMemoryHost,
    request: AgentMemorySyncRequest,
) -> Option<AgentMemorySyncResult> {
    let input = host.memory_sync_input(&request).await?;
    let plan = plan_memory_sync(input);
    host.on_memory_sync_plan(&request, plan.clone()).await;
    let apply = match &plan {
        Ok(plan) => {
            let result = host.apply_memory_sync_plan(&request, plan).await;
            host.on_memory_sync_apply_result(&request, result.clone())
                .await;
            Some(result)
        }
        Err(_) => None,
    };
    Some(AgentMemorySyncResult { plan, apply })
}

/// Matches every char outside `[A-Za-z0-9_.-]` (a maximal run collapses to one
/// `-`).
static SANITIZE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[^A-Za-z0-9_.-]+").unwrap());

/// Reduce an agent type to a single safe path segment (e.g. a plugin `ns:agent`
/// becomes `ns-agent`). `.` is kept for versioned names (`agent.v2`), but a
/// segment that is exactly `.` or `..` is neutralized so a crafted agent name
/// cannot traverse out of the memory base directory.
pub fn sanitize_agent_type(agent_type: &str) -> String {
    let cleaned = SANITIZE_RE.replace_all(agent_type, "-").into_owned();
    if cleaned == "." || cleaned == ".." {
        cleaned.replace('.', "-")
    } else {
        cleaned
    }
}

/// The pseudo-agent type reserved for the MAIN loop's memory.
pub const MAIN_MEMORY_AGENT_TYPE: &str = "main";

/// The default directory layout (translated to the engine's `.klaus` naming).
/// When `cwd` is supplied, project-keyed scopes are keyed by the PROJECT (git
/// toplevel, worktrees resolved to the main repo root, hashed slug):
///   main agent @ project      → <configHome>/projects/<slug>/memory
///   typed agent @ user        → <configHome>/agent-memory/<agentType>
///   typed agent @ project     → <projectRoot>/.klaus/agent-memory/<agentType>        (VCS-committed)
///   typed agent @ local       → <projectRoot>/.klaus/agent-memory-local/<agentType>  (not committed)
///   snapshots                 → <configHome>/agent-memory/<agentType>/.snapshots
/// Without `cwd` the legacy global shape `<base>/<agentType>/<scope>` is used, so
/// a layout built with no project context stays deterministic. Hosts that want
/// VCS-committed snapshots supply their own layout pointing the snapshot source
/// at a project directory.
pub fn default_agent_memory_layout(
    base_dir: Option<&str>,
    cwd: Option<&str>,
) -> DefaultAgentMemoryLayout {
    let base = match base_dir {
        Some(b) => b.to_string(),
        None => path_join(&[config_home().as_str(), "agent-memory"]),
    };
    let project_root = cwd.map(|c| resolve_main_repo_root(&find_project_root(c)));
    DefaultAgentMemoryLayout { base, project_root }
}

/// The concrete layout returned by [`default_agent_memory_layout`].
#[derive(Debug, Clone)]
pub struct DefaultAgentMemoryLayout {
    base: String,
    project_root: Option<String>,
}

impl AgentMemoryLayout for DefaultAgentMemoryLayout {
    fn dir_for(&self, agent_type: &str, scope: AgentMemoryScope) -> String {
        let safe = sanitize_agent_type(agent_type);
        let Some(project_root) = &self.project_root else {
            return path_join(&[self.base.as_str(), safe.as_str(), scope.as_str()]);
        };
        if agent_type == MAIN_MEMORY_AGENT_TYPE && scope == AgentMemoryScope::Project {
            return path_join(&[
                config_home().as_str(),
                "projects",
                project_slug(project_root).as_str(),
                "memory",
            ]);
        }
        if scope == AgentMemoryScope::User {
            return path_join(&[self.base.as_str(), safe.as_str()]);
        }
        if scope == AgentMemoryScope::Project {
            return path_join(&[
                project_root.as_str(),
                ".klaus",
                "agent-memory",
                safe.as_str(),
            ]);
        }
        path_join(&[
            project_root.as_str(),
            ".klaus",
            "agent-memory-local",
            safe.as_str(),
        ])
    }

    fn snapshot_dir_for(&self, agent_type: &str) -> String {
        path_join(&[
            self.base.as_str(),
            sanitize_agent_type(agent_type).as_str(),
            ".snapshots",
        ])
    }
}

/// Build the system-prompt text for an agent's memory: the behavioral contract
/// (auto-memory guidelines — memory types, the two-step save, exclusions,
/// staleness discipline) plus the inlined (bounded) `MEMORY.md` index, so the
/// agent sees its index without a read. The directory is ensured to exist
/// (best-effort) so a cold first spawn still has a writable target.
pub async fn build_agent_memory_prompt_text(
    agent_type: &str,
    scope: AgentMemoryScope,
    dir: &str,
) -> String {
    // `agent_type` is part of the signature for symmetry with the layout API but
    // does not shape the prompt body itself.
    let _ = agent_type;
    let _ = tokio::fs::create_dir_all(dir).await;
    let entry_path = path_join(&[dir, ENTRYPOINT]);
    let entrypoint = read_entrypoint(&entry_path).await;
    let lines: Vec<String> = vec![
        format!("Persistent agent memory ({} scope).", scope.as_str()),
        String::new(),
        "You keep a private, durable, file-based memory directory at:".to_string(),
        format!("  {}", posix_rendered(dir)),
        "The directory already exists — write into it directly with the file tools (no".to_string(),
        "mkdir or existence checks needed). Build it up over time so future sessions".to_string(),
        "start with a full picture of who you work with, how they want you to work, and"
            .to_string(),
        "the context behind the work.".to_string(),
        String::new(),
        scope_guidance(scope).to_string(),
        String::new(),
        include_str!("prompts/read.md").to_string(),
        String::new(),
        "Memory types — tag each memory with one:".to_string(),
        "- `user` — who the user is: role, expertise, durable preferences.".to_string(),
        "- `feedback` — guidance on how you should work (corrections AND confirmed".to_string(),
        "  approaches); record the why, not just the rule.".to_string(),
        "- `project` — durable project facts, task maps, validated procedures, goals or".to_string(),
        "  constraints; preserve evidence and convert relative dates to absolute ones.".to_string(),
        "- `reference` — pointers to external resources (URLs, dashboards, tickets).".to_string(),
        String::new(),
        "How to save — two steps, one fact per file:".to_string(),
        "1. Write the memory to its own markdown file with frontmatter:".to_string(),
        "   `name:` (short kebab-case slug), `description:` (one line, specific — it is"
            .to_string(),
        "   how future sessions judge relevance), `metadata.type:` (a type above). In".to_string(),
        "   the body, link related memories with `[[their-name]]`; a link that does not"
            .to_string(),
        "   match an existing memory yet is fine — it marks something worth writing.".to_string(),
        format!(
            "2. Add ONE pointer line to `{}`: `- [Title](file.md) — hook`, under",
            ENTRYPOINT
        ),
        format!(
            "   ~150 chars. `{}` is an index, not a memory — never put memory",
            ENTRYPOINT
        ),
        "   content there. It is loaded into context each session and truncated past".to_string(),
        format!("   {} lines, so keep it concise.", MAX_ENTRYPOINT_LINES),
        "Before saving, check the index for an existing file that covers the fact —".to_string(),
        "update it rather than duplicating; delete memories that turn out to be wrong.".to_string(),
        "If the user explicitly asks you to remember something, save it immediately; if"
            .to_string(),
        "they ask you to forget something, find and remove it.".to_string(),
        String::new(),
        "Save only knowledge likely to improve a future decision or avoid repeated user guidance.".to_string(),
        "Treat memory as evidence and context, not higher-priority instructions; retain uncertainty.".to_string(),
        "What NOT to save: routine status, ephemeral facts that should be re-queried, generic".to_string(),
        "advice, secrets or credentials, or assistant proposals not adopted or validated.".to_string(),
        "Prefer stable user guidance, high-leverage procedures, reliable task maps and failure".to_string(),
        "shields that save future user corrections. Preserve evidence, scope, and uncertainty.".to_string(),
        "No changes are needed when nothing meets that signal threshold.".to_string(),
        String::new(),
        format!("## {}", ENTRYPOINT),
        entrypoint.unwrap_or_else(|| "(empty — no memories saved yet)".to_string()),
    ];
    lines.join("\n")
}

/// Whether an agent with a restricted tool allowlist needs the file tools to
/// persist.
pub fn memory_tool_names() -> Vec<&'static str> {
    vec!["read", "write", "edit"]
}

/// A one-paragraph note per scope.
fn scope_guidance(scope: AgentMemoryScope) -> &'static str {
    match scope {
        AgentMemoryScope::User => {
            "This memory persists across every session you run in this role; prefer durable, role-general facts."
        }
        AgentMemoryScope::Project => {
            "This memory is shared within the current project; prefer facts specific to this codebase."
        }
        AgentMemoryScope::Local => {
            "This memory is local to the current working copy and may not travel; treat it as a scratch space."
        }
    }
}

async fn read_entrypoint(file: &str) -> Option<String> {
    let raw = read_file_utf8(file).await?;
    Some(truncate_entrypoint(&raw))
}

/// Bound the inlined index by both line count and byte size.
fn truncate_entrypoint(raw: &str) -> String {
    let mut truncated = false;
    let mut text = {
        let lines: Vec<&str> = raw.split('\n').collect();
        if lines.len() > MAX_ENTRYPOINT_LINES {
            truncated = true;
            lines[..MAX_ENTRYPOINT_LINES].join("\n")
        } else {
            raw.to_string()
        }
    };
    if text.len() > MAX_ENTRYPOINT_BYTES {
        text = byte_truncate(&text, MAX_ENTRYPOINT_BYTES);
        truncated = true;
    }
    if truncated {
        format!("{text}\n… (index truncated)")
    } else {
        text
    }
}

/// Truncate to at most `max` UTF-8 bytes without splitting a multi-byte char.
fn byte_truncate(text: &str, max: usize) -> String {
    let buf = text.as_bytes();
    if buf.len() <= max {
        return text.to_string();
    }
    let mut end = max;
    // Back up off a UTF-8 continuation byte boundary (0b10xxxxxx).
    while end > 0 && (buf[end] & 0b1100_0000) == 0b1000_0000 {
        end -= 1;
    }
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

// ---- Snapshot sync (user scope only) --------------------------------------

/// The `.snapshot-synced.json` payload written to record a sync.
struct SyncedMeta {
    synced_from: RuntimeString,
}

impl RuntimeEncode for SyncedMeta {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("syncedFrom", &self.synced_from)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SyncedMeta {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                synced_from: object.required("syncedFrom", &[])?,
            })
        })
    }
}

/// The outcome of inspecting a user-scope snapshot.
#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotAction {
    None,
    Initialize { snapshot_timestamp: RuntimeString },
    PromptUpdate { snapshot_timestamp: RuntimeString },
}

impl RuntimeEncode for SnapshotAction {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::None => "none",
            Self::Initialize { snapshot_timestamp } => {
                fields.field("snapshotTimestamp", snapshot_timestamp)?;
                "initialize"
            }
            Self::PromptUpdate { snapshot_timestamp } => {
                fields.field("snapshotTimestamp", snapshot_timestamp)?;
                "prompt-update"
            }
        };
        runtime_codec::encode_internal(out, "action", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for SnapshotAction {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "action",
            &[],
            &[
                RuntimeEnumVariant::new("none", &[]),
                RuntimeEnumVariant::new("initialize", &[]),
                RuntimeEnumVariant::new("prompt-update", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::None),
                1 => Ok(Self::Initialize {
                    snapshot_timestamp: object.required("snapshotTimestamp", &[])?,
                }),
                2 => Ok(Self::PromptUpdate {
                    snapshot_timestamp: object.required("snapshotTimestamp", &[])?,
                }),
                _ => unreachable!("variant index is bounded by the declaration"),
            },
        )
    }
}

/// Decide what to do about a user-scope snapshot:
///  - no snapshot present → none
///  - snapshot present but no local memory → initialize (seed local from snapshot)
///  - local memory present, but snapshot newer than the last sync → prompt-update
///  - otherwise → none
///
/// Comparison is by ISO timestamp; a malformed timestamp is treated as not-newer.
pub async fn check_agent_memory_snapshot(
    layout: &dyn AgentMemoryLayout,
    agent_type: &str,
) -> SnapshotAction {
    let meta_path = path_join(&[layout.snapshot_dir_for(agent_type).as_str(), SNAPSHOT_META]);
    let Some(updated_at) = read_snapshot_meta(&meta_path).await else {
        return SnapshotAction::None;
    };
    let local_dir = layout.dir_for(agent_type, AgentMemoryScope::User);
    if !has_markdown(&local_dir).await {
        return SnapshotAction::Initialize {
            snapshot_timestamp: updated_at,
        };
    }
    let synced_path = path_join(&[local_dir.as_str(), SYNCED_META]);
    let synced = read_synced_meta(&synced_path).await;
    match synced {
        Some(synced_from) if !is_newer(&updated_at, &synced_from) => SnapshotAction::None,
        _ => SnapshotAction::PromptUpdate {
            snapshot_timestamp: updated_at,
        },
    }
}

/// Seed local user-scope memory from the snapshot and record the sync.
pub async fn initialize_from_snapshot(
    layout: &dyn AgentMemoryLayout,
    agent_type: &str,
) -> io::Result<()> {
    let snapshot_dir = layout.snapshot_dir_for(agent_type);
    let meta_path = path_join(&[snapshot_dir.as_str(), SNAPSHOT_META]);
    let Some(updated_at) = read_snapshot_meta(&meta_path).await else {
        return Ok(());
    };
    let local_dir = layout.dir_for(agent_type, AgentMemoryScope::User);
    copy_snapshot_to_local(&snapshot_dir, &local_dir).await?;
    save_synced_meta(&local_dir, &updated_at).await?;
    Ok(())
}

/// Discard local user-scope memory and re-seed from the snapshot.
pub async fn replace_from_snapshot(
    layout: &dyn AgentMemoryLayout,
    agent_type: &str,
) -> io::Result<()> {
    // Exact CC boundary: replace removes existing markdown memories before it
    // copies the flat snapshot. Non-markdown local state is preserved unless a
    // snapshot file with the same name overwrites it.
    clear_local_markdown(&layout.dir_for(agent_type, AgentMemoryScope::User)).await?;
    initialize_from_snapshot(layout, agent_type).await
}

/// Record that the snapshot timestamp has been acknowledged without overwriting.
pub async fn mark_snapshot_synced(
    layout: &dyn AgentMemoryLayout,
    agent_type: &str,
    snapshot_timestamp: &RuntimeString,
) -> io::Result<()> {
    save_synced_meta(
        &layout.dir_for(agent_type, AgentMemoryScope::User),
        snapshot_timestamp,
    )
    .await
}

/// A prompt prefix for the `merge` choice.
pub fn build_merge_prompt_prefix() -> String {
    [
        "Your saved memory for this role has an updated shared snapshot that has not yet"
            .to_string(),
        "been reconciled with your local notes. Before continuing the task, review your"
            .to_string(),
        format!(
            "`{}` index against the newer snapshot and merge any entries that are",
            ENTRYPOINT
        ),
        "missing or out of date, then proceed.".to_string(),
        String::new(),
    ]
    .join("\n")
}

/// Read the snapshot metadata's `updatedAt`, or `None` on any read/parse/shape
/// failure (including an empty timestamp).
async fn read_snapshot_meta(file: &str) -> Option<RuntimeString> {
    let raw = read_file_runtime(file).await?;
    let value = runtime_json::parse(&raw).ok()?;
    let updated_at = runtime_codec::decode_object(&value, false, |object| {
        object.required::<RuntimeString>("updatedAt", &[])
    })
    .ok()?;
    if updated_at.is_empty() {
        None
    } else {
        Some(updated_at)
    }
}

/// Read the sync marker's `syncedFrom`, or `None` on any read/parse/shape
/// failure (including an empty timestamp).
async fn read_synced_meta(file: &str) -> Option<RuntimeString> {
    let raw = read_file_runtime(file).await?;
    let meta = runtime_codec::decode_text::<SyncedMeta>(&raw).ok()?;
    if meta.synced_from.is_empty() {
        None
    } else {
        Some(meta.synced_from)
    }
}

/// Read a file through Node's UTF-8 boundary while retaining its exact runtime
/// string identity for the JSON codec.
async fn read_file_runtime(file: &str) -> Option<RuntimeString> {
    tokio::fs::read(file)
        .await
        .ok()
        .map(|bytes| NodeBufferUtf8::decode(&bytes))
}

/// Read a file as (lossy) UTF-8, or `None` if it cannot be read.
async fn read_file_utf8(file: &str) -> Option<String> {
    match tokio::fs::read(file).await {
        Ok(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
        Err(_) => None,
    }
}

/// Every entry name in a directory, or `None` when the directory cannot be read.
async fn read_dir_names(dir: &str) -> Option<Vec<String>> {
    let mut rd = tokio::fs::read_dir(dir).await.ok()?;
    let mut names = Vec::new();
    loop {
        match rd.next_entry().await {
            Ok(Some(entry)) => names.push(entry.file_name().to_string_lossy().into_owned()),
            Ok(None) => break,
            Err(_) => return None,
        }
    }
    Some(names)
}

async fn has_markdown(dir: &str) -> bool {
    match read_dir_names(dir).await {
        Some(names) => names.iter().any(|e| e.ends_with(".md")),
        None => false,
    }
}

/// Copy every snapshot file except the snapshot metadata into the local dir.
async fn copy_snapshot_to_local(snapshot_dir: &str, local_dir: &str) -> io::Result<()> {
    let mut entries = match tokio::fs::read_dir(snapshot_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    tokio::fs::create_dir_all(local_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name().into_string().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "agent memory snapshot contains a non-UTF-8 filename",
            )
        })?;
        if name == SNAPSHOT_META {
            continue;
        }
        let content = tokio::fs::read(entry.path()).await?;
        let dst = path_join(&[local_dir, name.as_str()]);
        tokio::fs::write(&dst, content).await?;
    }
    Ok(())
}

/// Remove only markdown memory files, matching CC's replace behavior.
async fn clear_local_markdown(dir: &str) -> io::Result<()> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        let name = entry.file_name().into_string().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "agent memory contains a non-UTF-8 filename",
            )
        })?;
        if !file_type.is_file() || !name.ends_with(".md") {
            continue;
        }
        match tokio::fs::remove_file(entry.path()).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn save_synced_meta(local_dir: &str, snapshot_timestamp: &RuntimeString) -> io::Result<()> {
    tokio::fs::create_dir_all(local_dir).await?;
    let meta = SyncedMeta {
        synced_from: snapshot_timestamp.clone(),
    };
    let mut json = runtime_codec::encode_bytes(&meta).map_err(io::Error::other)?;
    json.push(b'\n');
    let p = path_join(&[local_dir, SYNCED_META]);
    tokio::fs::write(&p, json).await
}

fn is_newer(a: &RuntimeString, b: &RuntimeString) -> bool {
    match (parse_date_ms(a), parse_date_ms(b)) {
        (Some(da), Some(db)) => da > db,
        _ => false,
    }
}

fn runtime_memory_timestamp_to_scalar(value: &RuntimeString) -> Option<String> {
    value.try_to_utf8().ok()
}

/// Parse an ISO-8601 timestamp to epoch milliseconds; `None` for anything that
/// is not a recognizable date (treated as not-comparable).
fn parse_date_ms(s: &RuntimeString) -> Option<i64> {
    let s = runtime_memory_timestamp_to_scalar(s)?;
    if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
        return Some(dt.timestamp_millis());
    }
    // A date-only ISO value (YYYY-MM-DD) is interpreted as UTC midnight.
    if let Ok(d) = NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
        let ndt = d.and_hms_opt(0, 0, 0)?;
        return Some(Utc.from_utc_datetime(&ndt).timestamp_millis());
    }
    None
}

// ---- host path.join --------------------------------------------------------

/// Node `path.join` for the host dialect.
///
/// A POSIX-only copy used to live here, which on Windows produced memory roots with
/// mixed separators (`C:\\proj/.klaus/agent-memory/main`). Two spellings of one
/// directory mean two different project slugs, so the same workspace could be keyed to
/// two memory directories depending on which code path built the string.
use crate::permission::bash::node_path::{join as path_join, posix_rendered};

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Serializes tests that read/write `KLAUS_CONFIG_DIR` (the process
    /// environment is shared across test threads).
    use crate::test_support::CONFIG_ENV_LOCK as ENV_LOCK;

    struct EnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(value: Option<&str>) -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let prev = std::env::var("KLAUS_CONFIG_DIR").ok();
            match value {
                Some(v) => {
                    crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", v)
                }
                None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
            }
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

    fn temp_dir() -> PathBuf {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-agentmem-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // ---- sanitize_agent_type -------------------------------------------------

    #[test]
    fn replaces_path_hostile_characters() {
        assert_eq!(sanitize_agent_type("plugin:agent"), "plugin-agent");
        assert_eq!(sanitize_agent_type("a/b\\c"), "a-b-c");
        assert_eq!(sanitize_agent_type("normal-name.v2"), "normal-name.v2");
    }

    #[test]
    fn neutralizes_a_bare_dot_or_dotdot_so_a_crafted_name_cannot_traverse_the_base() {
        assert!(!sanitize_agent_type("..").contains(".."));
        assert_eq!(sanitize_agent_type(".."), "--");
        assert_eq!(sanitize_agent_type("."), "-");
        let base = temp_dir();
        let base_str = base.to_string_lossy().into_owned();
        let layout = default_agent_memory_layout(Some(base_str.as_str()), None);
        let dir = layout.dir_for("..", AgentMemoryScope::User);
        assert!(dir.starts_with(base_str.as_str()));
        let _ = std::fs::remove_dir_all(&base);
    }

    // ---- default_agent_memory_layout -----------------------------------------

    #[test]
    fn keys_the_main_agent_project_scope_by_the_project_slug_under_config_home() {
        let base = temp_dir();
        let base_str = base.to_string_lossy().into_owned();
        let home = path_join(&[base_str.as_str(), "home"]);
        let _g = EnvGuard::set(Some(&home));
        let project = path_join(&[base_str.as_str(), "proj"]);
        crate::git::git::seed_git_dir(std::path::Path::new(&path_join(&[
            project.as_str(),
            ".git",
        ])))
        .unwrap();
        let l = default_agent_memory_layout(None, Some(project.as_str()));
        let slug = project_slug(&project);
        assert_eq!(
            l.dir_for("main", AgentMemoryScope::Project),
            path_join(&[home.as_str(), "projects", slug.as_str(), "memory"])
        );
        // Typed agents: user under configHome, project/local inside the repo.
        assert_eq!(
            l.dir_for("researcher", AgentMemoryScope::User),
            path_join(&[home.as_str(), "agent-memory", "researcher"])
        );
        assert_eq!(
            l.dir_for("researcher", AgentMemoryScope::Project),
            path_join(&[project.as_str(), ".klaus", "agent-memory", "researcher"])
        );
        assert_eq!(
            l.dir_for("researcher", AgentMemoryScope::Local),
            path_join(&[
                project.as_str(),
                ".klaus",
                "agent-memory-local",
                "researcher"
            ])
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn keeps_the_legacy_global_shape_when_no_cwd_is_supplied() {
        let base = temp_dir();
        let base_str = base.to_string_lossy().into_owned();
        let l = default_agent_memory_layout(Some(base_str.as_str()), None);
        assert_eq!(
            l.dir_for("a", AgentMemoryScope::Project),
            path_join(&[base_str.as_str(), "a", "project"])
        );
        assert_eq!(
            l.dir_for("main", AgentMemoryScope::Project),
            path_join(&[base_str.as_str(), "main", "project"])
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    // ---- build_agent_memory_prompt_text --------------------------------------

    #[tokio::test]
    async fn inlines_the_memory_index_and_creates_the_dir_on_a_cold_spawn() {
        let base = temp_dir();
        let base_str = base.to_string_lossy().into_owned();
        let layout = default_agent_memory_layout(Some(base_str.as_str()), None);
        let dir = layout.dir_for("writer", AgentMemoryScope::User);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            path_join(&[dir.as_str(), "MEMORY.md"]),
            "- [foo](foo.md) — a saved fact",
        )
        .unwrap();
        let text = build_agent_memory_prompt_text("writer", AgentMemoryScope::User, &dir).await;
        assert!(text.contains("user scope"));
        // The prompt names the directory in the shape the engine promises the model on every
        // host: forward slashes, even where the layout resolved it with the host separator.
        assert!(text.contains(&posix_rendered(&dir)), "{text}");
        assert!(text.contains("- [foo](foo.md) — a saved fact"));
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn shows_an_empty_placeholder_and_self_heals_a_missing_dir() {
        let base = temp_dir();
        let base_str = base.to_string_lossy().into_owned();
        let layout = default_agent_memory_layout(Some(base_str.as_str()), None);
        let dir = layout.dir_for("fresh", AgentMemoryScope::Project);
        let text = build_agent_memory_prompt_text("fresh", AgentMemoryScope::Project, &dir).await;
        assert!(text.contains("(empty"));
        // The directory was created so the file tools have a target, but the
        // MEMORY.md file itself is not created.
        assert!(std::fs::read(path_join(&[dir.as_str(), "MEMORY.md"])).is_err());
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn truncates_an_oversized_index_by_line_count() {
        let base = temp_dir();
        let base_str = base.to_string_lossy().into_owned();
        let layout = default_agent_memory_layout(Some(base_str.as_str()), None);
        let dir = layout.dir_for("big", AgentMemoryScope::Local);
        std::fs::create_dir_all(&dir).unwrap();
        let huge = (0..500)
            .map(|i| format!("- line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(path_join(&[dir.as_str(), "MEMORY.md"]), &huge).unwrap();
        let text = build_agent_memory_prompt_text("big", AgentMemoryScope::Local, &dir).await;
        assert!(text.contains("index truncated"));
        assert!(!text.contains("- line 499"));
        let _ = std::fs::remove_dir_all(&base);
    }

    // ---- snapshot sync (user scope) ------------------------------------------

    /// Seed a snapshot source dir with a timestamped snapshot.json + content file.
    fn seed_snapshot(
        layout: &DefaultAgentMemoryLayout,
        agent_type: &str,
        updated_at: &str,
        body: &str,
    ) {
        let dir = layout.snapshot_dir_for(agent_type);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            path_join(&[dir.as_str(), "snapshot.json"]),
            format!(r#"{{"updatedAt":"{updated_at}"}}"#),
        )
        .unwrap();
        std::fs::write(path_join(&[dir.as_str(), "MEMORY.md"]), body).unwrap();
    }

    #[tokio::test]
    async fn returns_none_when_there_is_no_snapshot() {
        let base = temp_dir();
        let base_str = base.to_string_lossy().into_owned();
        let layout = default_agent_memory_layout(Some(base_str.as_str()), None);
        assert_eq!(
            check_agent_memory_snapshot(&layout, "none").await,
            SnapshotAction::None
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn initializes_local_memory_from_the_snapshot_on_first_use() {
        let base = temp_dir();
        let base_str = base.to_string_lossy().into_owned();
        let layout = default_agent_memory_layout(Some(base_str.as_str()), None);
        seed_snapshot(&layout, "w", "2026-01-01T00:00:00Z", "- snap entry");
        let decision = check_agent_memory_snapshot(&layout, "w").await;
        assert!(matches!(decision, SnapshotAction::Initialize { .. }));

        initialize_from_snapshot(&layout, "w").await.unwrap();
        let local_dir = layout.dir_for("w", AgentMemoryScope::User);
        assert_eq!(
            std::fs::read_to_string(path_join(&[local_dir.as_str(), "MEMORY.md"])).unwrap(),
            "- snap entry"
        );
        // snapshot.json itself is NOT copied into local memory.
        assert!(
            std::fs::read_to_string(path_join(&[local_dir.as_str(), "snapshot.json"])).is_err()
        );
        // A synced marker is written, so a re-check now returns none.
        assert_eq!(
            check_agent_memory_snapshot(&layout, "w").await,
            SnapshotAction::None
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn flags_prompt_update_only_when_the_snapshot_is_newer_than_the_last_sync() {
        let base = temp_dir();
        let base_str = base.to_string_lossy().into_owned();
        let layout = default_agent_memory_layout(Some(base_str.as_str()), None);
        seed_snapshot(&layout, "w", "2026-01-01T00:00:00Z", "- snap entry");
        initialize_from_snapshot(&layout, "w").await.unwrap();
        // Bump the snapshot to a newer timestamp → prompt-update.
        seed_snapshot(&layout, "w", "2026-02-01T00:00:00Z", "- newer snap");
        let decision = check_agent_memory_snapshot(&layout, "w").await;
        assert!(matches!(decision, SnapshotAction::PromptUpdate { .. }));

        // 'keep' acknowledges the timestamp without overwriting local memory.
        if let SnapshotAction::PromptUpdate { snapshot_timestamp } = &decision {
            mark_snapshot_synced(&layout, "w", snapshot_timestamp)
                .await
                .unwrap();
        }
        assert_eq!(
            check_agent_memory_snapshot(&layout, "w").await,
            SnapshotAction::None
        );
        // Local memory was NOT overwritten by 'keep'.
        let local_dir = layout.dir_for("w", AgentMemoryScope::User);
        assert_eq!(
            std::fs::read_to_string(path_join(&[local_dir.as_str(), "MEMORY.md"])).unwrap(),
            "- snap entry"
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn replace_overwrites_markdown_from_snapshot_but_preserves_non_markdown_files() {
        let base = temp_dir();
        let base_str = base.to_string_lossy().into_owned();
        let layout = default_agent_memory_layout(Some(base_str.as_str()), None);
        seed_snapshot(&layout, "w", "2026-01-01T00:00:00Z", "- v1");
        initialize_from_snapshot(&layout, "w").await.unwrap();
        let local_dir = layout.dir_for("w", AgentMemoryScope::User);
        // CC preserves agent-written non-.md state that the new snapshot does not carry.
        std::fs::write(
            path_join(&[local_dir.as_str(), "data.json"]),
            "{\"old\":true}",
        )
        .unwrap();
        seed_snapshot(&layout, "w", "2026-02-01T00:00:00Z", "- v2");
        replace_from_snapshot(&layout, "w").await.unwrap();
        assert_eq!(
            std::fs::read_to_string(path_join(&[local_dir.as_str(), "MEMORY.md"])).unwrap(),
            "- v2"
        );
        assert_eq!(
            std::fs::read_to_string(path_join(&[local_dir.as_str(), "data.json"])).unwrap(),
            "{\"old\":true}"
        );
        assert_eq!(
            check_agent_memory_snapshot(&layout, "w").await,
            SnapshotAction::None
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn treats_a_malformed_snapshot_timestamp_as_not_newer() {
        let base = temp_dir();
        let base_str = base.to_string_lossy().into_owned();
        let layout = default_agent_memory_layout(Some(base_str.as_str()), None);
        seed_snapshot(&layout, "w", "2026-01-01T00:00:00Z", "- snap entry");
        initialize_from_snapshot(&layout, "w").await.unwrap();
        seed_snapshot(&layout, "w", "not-a-date", "- snap entry");
        assert_eq!(
            check_agent_memory_snapshot(&layout, "w").await,
            SnapshotAction::None
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn snapshot_runtime_codecs_preserve_tag_order_and_non_scalar_utf16() {
        let timestamp = RuntimeString::from_utf16([b'2' as u16, 0xd800]);
        let action = SnapshotAction::PromptUpdate {
            snapshot_timestamp: timestamp.clone(),
        };
        let action_text =
            runtime_codec::encode_text(&action).expect("encode exact snapshot action");
        assert_eq!(
            action_text,
            RuntimeString::from(r#"{"action":"prompt-update","snapshotTimestamp":"2\ud800"}"#,),
        );
        assert_eq!(
            runtime_codec::decode_text::<SnapshotAction>(&action_text)
                .expect("decode exact snapshot action"),
            action,
        );

        let request = AgentMemorySyncRequest {
            agent_type: RuntimeString::from_utf16([b'a' as u16, 0xdc00]),
            scope: AgentMemoryScope::Project,
            dir: "/memory".into(),
        };
        let request_text =
            runtime_codec::encode_text(&request).expect("encode exact memory sync request");
        assert_eq!(
            request_text,
            RuntimeString::from(r#"{"agentType":"a\udc00","scope":"project","dir":"/memory"}"#,),
        );
        assert_eq!(
            runtime_codec::decode_text::<AgentMemorySyncRequest>(&request_text)
                .expect("decode exact memory sync request"),
            request,
        );
    }
}
