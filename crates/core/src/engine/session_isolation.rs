use super::append_developer_instructions;
use super::bare_tool_context;
use super::base_hook_input;
use super::derive_peer_name_from_scalar;
use super::fork_snapshot;
use super::join_path;
use super::join_runtime_strings;
use super::permission_decision_word;
use super::runtime_background_output_to_file_host;
use super::runtime_session_id_to_session_store_host;
use super::worktree_to_json;
use super::writable_roots;
use super::write_output_atomic;
use super::writes_are_unconfined;
use super::AgentIsolation;
use super::AgentWorktree;
use super::EngineEventStream;
use super::GetConversation;
use super::Isolation;
use super::PathContext;
use super::PermissionDecision;
use super::RunSubAgentOpts;
use super::Session;
use super::SessionWorkspaceState;
use super::SystemPromptProvider;
use super::ToolError;
use super::TEAMMATE_IDLE_POLL_MS;
use crate::context::system_prompt::create_system_prompt_provider;
use crate::context::system_prompt::SystemPromptProviderOptions;
use crate::engine::message_queue::QueuePriority;
use crate::engine::message_queue::QueuedMessage;
use crate::engine::message_queue::QueuedValue;
use crate::engine::query::abortable_sleep;
use crate::engine::subagent_lifecycle::SubagentKind;
use crate::git::git::current_branch;
use crate::hooks::types::HookEvent;
use crate::permission::engine::{PermissionEngine, StaticPermissionEngine};
use crate::permission::rules::parse_rule_strings;
use crate::provider::behavior::render_provider_text_envelope;
use crate::provider::behavior::ProviderBehaviorProfile;
use crate::provider::behavior::ProviderTextEnvelopeKind;
use crate::runtime_values::RuntimeJson;
use crate::runtime_values::RuntimeObject;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
use crate::sandbox::plan_readonly::PlanReadOnlySandbox;
use crate::store::session_store::AppendEventOptions;
use crate::store::session_store::LiveRollout;
use crate::sync_ext::{MutexExt, RwLockExt};
use crate::tasks::swarm_store::heartbeat_peer as swarm_heartbeat_peer;
use crate::tasks::swarm_store::read_cursor as swarm_read_cursor;
use crate::tasks::swarm_store::read_inbox_from as swarm_read_inbox_from;
use crate::tasks::swarm_store::register_peer as swarm_register_peer;
use crate::tasks::swarm_store::team_confirmed_gone as swarm_team_confirmed_gone;
use crate::tasks::swarm_store::write_cursor as swarm_write_cursor;
use crate::tools::exec::run_shell;
use crate::tools::exec::RunShellOptions;
use crate::tools::subagent_stream::append_transcript;
use crate::tools::subagent_stream::terminal_item;
use crate::types::abort::AbortSignal;
use crate::types::errors::EngineError;
use crate::types::errors::EngineErrorCode;
use crate::types::errors::TerminalReason;
use crate::types::events::EngineEvent;
use crate::types::events::ThreadItem;
use crate::types::file_state::FileStateCache;
use crate::types::json::Json;
use crate::types::permission::{ApprovalPolicy, PermissionContext};
use crate::types::sandbox::{SandboxBackend, SandboxKind};
use crate::types::tool::BackgroundTaskStatus;
use crate::types::tool::RunBashOptions;
use crate::types::tool::SpawnBackgroundAgent;
use crate::types::tool::SpawnBackgroundAgentResult;
use crate::types::tool::SpawnForegroundSubAgent;
use crate::types::tool::SpawnSubAgent;
use crate::types::tool::SpawnSubAgentOptions;
use crate::types::tool::WorktreeEnterResult;
use crate::types::tool::WorktreeExitAction;
use crate::types::tool::WorktreeExitResult;
use crate::worktree::worktree::build_worktree_notice;
use crate::worktree::worktree::count_worktree_changes;
use crate::worktree::worktree::find_canonical_git_root;
use crate::worktree::worktree::get_or_create_worktree;
use crate::worktree::worktree::remove_worktree;
use crate::worktree::worktree::validate_worktree_slug;
use crate::worktree::worktree::WorktreeSession;
use async_stream::stream;
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[path = "session_subagent_permission_origin.rs"]
mod permission_origin;
pub(crate) use permission_origin::{project_subagent_permission, SubagentPermissionOrigin};

#[path = "session_subagent_capabilities.rs"]
mod admitted_capabilities;
pub(crate) use admitted_capabilities::SubagentCapabilityCapture;

struct OwnedAgentIsolation {
    owner: Arc<Session>,
    isolation: AgentIsolation,
}

impl std::ops::Deref for OwnedAgentIsolation {
    type Target = AgentIsolation;

    fn deref(&self) -> &Self::Target {
        &self.isolation
    }
}

impl std::ops::DerefMut for OwnedAgentIsolation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.isolation
    }
}

impl OwnedAgentIsolation {
    async fn finish(mut self) {
        self.owner
            .teardown_agent_isolation(&self.isolation.worktree)
            .await;
        self.isolation.worktree = None;
    }
}

impl Drop for OwnedAgentIsolation {
    fn drop(&mut self) {
        let Some(worktree) = self.isolation.worktree.take() else {
            return;
        };
        let owner = self.owner.clone();
        self.owner
            .background_tasks
            .lock_recover()
            .handles
            .push(tokio::spawn(async move {
                owner.teardown_agent_isolation(&Some(worktree)).await;
            }));
    }
}

struct SubagentPermissionCeiling {
    parent: Arc<dyn PermissionEngine>,
    ceiling: StaticPermissionEngine,
    plan_ceiling: StaticPermissionEngine,
    plan_only: bool,
    captured_untrusted_review: bool,
    live_approval_policy: Option<SubagentApprovalPolicyReader>,
}

pub(crate) type SubagentApprovalPolicyReader = Arc<dyn Fn() -> ApprovalPolicy + Send + Sync>;

pub(crate) fn subagent_approval_policy_reader(
    owner: std::sync::Weak<Session>,
    parent_agent_id: Option<String>,
) -> SubagentApprovalPolicyReader {
    Arc::new(move || {
        owner
            .upgrade()
            .map_or(ApprovalPolicy::UnlessTrusted, |session| {
                session.ordinary_live_approval_policy(parent_agent_id.as_deref())
            })
    })
}

#[async_trait::async_trait]
impl PermissionEngine for SubagentPermissionCeiling {
    fn rule_snapshot(&self) -> crate::permission::permission_update::SessionPermissionState {
        self.parent.rule_snapshot()
    }
    fn bound_task_resource_scope(
        &self,
    ) -> Option<Arc<crate::permission::task_scope::CanonicalTaskResourceScope>> {
        self.parent.bound_task_resource_scope()
    }

    async fn check_task_resource_scope(
        &self,
        tool: &dyn crate::types::tool::Tool,
        input: &Json,
        ctx: &crate::types::tool::ToolContext,
    ) -> Result<(), String> {
        self.parent
            .check_task_resource_scope(tool, input, ctx)
            .await?;
        self.ceiling
            .check_task_resource_scope(tool, input, ctx)
            .await
    }

    async fn evaluate(
        &self,
        tool: &dyn crate::types::tool::Tool,
        input: &Json,
        ctx: &crate::types::tool::ToolContext,
    ) -> PermissionDecision {
        self.evaluate_with_prefix_policy(tool, input, ctx, false)
            .await
    }

    async fn evaluate_without_allow_prefix_rules(
        &self,
        tool: &dyn crate::types::tool::Tool,
        input: &Json,
        ctx: &crate::types::tool::ToolContext,
    ) -> PermissionDecision {
        self.evaluate_with_prefix_policy(tool, input, ctx, true)
            .await
    }
}

impl SubagentPermissionCeiling {
    async fn evaluate_with_prefix_policy(
        &self,
        tool: &dyn crate::types::tool::Tool,
        input: &Json,
        ctx: &crate::types::tool::ToolContext,
        ignore_prefix_rules: bool,
    ) -> PermissionDecision {
        let mut parent = if ignore_prefix_rules {
            self.parent
                .evaluate_without_allow_prefix_rules(tool, input, ctx)
                .await
        } else {
            self.parent.evaluate(tool, input, ctx).await
        };
        if matches!(
            parent,
            PermissionDecision::Deny { .. } | PermissionDecision::Fatal { .. }
        ) {
            return parent;
        }
        if tool.permission_path_kind() == Some(crate::types::tool::PermissionPathKind::Edit)
            && (ctx.approval_policy == Some(ApprovalPolicy::UnlessTrusted)
                || self.captured_untrusted_review
                || self
                    .live_approval_policy
                    .as_ref()
                    .is_some_and(|read| read() == ApprovalPolicy::UnlessTrusted))
        {
            // A disabled approval flow cannot erase an inherited review requirement.
            // Evaluate that canonical floor separately; never publish it as the flow.
            let effective_input = match &parent {
                PermissionDecision::Allow { input, .. } => input,
                _ => input,
            };
            let review = self
                .ceiling
                .evaluate_inherited_untrusted_review(
                    tool,
                    effective_input,
                    ctx,
                    ignore_prefix_rules,
                )
                .await;
            parent = match (parent, review) {
                (
                    _,
                    denied @ (PermissionDecision::Deny { .. } | PermissionDecision::Fatal { .. }),
                ) => {
                    return denied;
                }
                (
                    PermissionDecision::Allow { .. },
                    PermissionDecision::Ask {
                        message,
                        input,
                        suggestions,
                        ..
                    },
                ) => PermissionDecision::Ask {
                    message,
                    input,
                    suggestions,
                    reason: crate::types::permission::PermissionReason::SafetyCheck,
                },
                (
                    PermissionDecision::Ask {
                        message,
                        input,
                        suggestions,
                        ..
                    },
                    PermissionDecision::Ask { .. },
                ) => PermissionDecision::Ask {
                    message,
                    input,
                    suggestions,
                    reason: crate::types::permission::PermissionReason::SafetyCheck,
                },
                (decision, _) => decision,
            };
            if matches!(parent, PermissionDecision::Ask { .. })
                && ctx.permission.should_avoid_prompts
            {
                return PermissionDecision::Deny {
                    message: "Action requires approval but prompting is disabled.".into(),
                    reason: crate::types::permission::PermissionReason::SafetyCheck,
                };
            }
        }
        if self.plan_only && !ctx.planning_read_only {
            return parent;
        }
        let effective_input = match &parent {
            PermissionDecision::Allow { input, .. } => input,
            _ => input,
        };
        let engine = if ctx.planning_read_only {
            &self.plan_ceiling
        } else {
            &self.ceiling
        };
        let ceiling = match if ignore_prefix_rules {
            engine
                .evaluate_without_allow_prefix_rules(tool, effective_input, ctx)
                .await
        } else {
            engine.evaluate(tool, effective_input, ctx).await
        } {
            PermissionDecision::Ask {
                message,
                input,
                suggestions,
                ..
            } => PermissionDecision::Ask {
                message,
                input,
                suggestions,
                reason: crate::types::permission::PermissionReason::SafetyCheck,
            },
            decision => decision,
        };
        if matches!(
            ceiling,
            PermissionDecision::Deny { .. } | PermissionDecision::Fatal { .. }
        ) {
            return ceiling;
        }
        let decision = match (parent, ceiling) {
            (PermissionDecision::Allow { .. }, ceiling) => ceiling,
            (
                PermissionDecision::Ask {
                    message,
                    input,
                    suggestions,
                    ..
                },
                PermissionDecision::Ask { .. },
            ) => PermissionDecision::Ask {
                message,
                input,
                suggestions,
                reason: crate::types::permission::PermissionReason::SafetyCheck,
            },
            (decision, _) => decision,
        };
        if matches!(decision, PermissionDecision::Ask { .. }) && ctx.permission.should_avoid_prompts
        {
            return PermissionDecision::Deny {
                message: "Action requires approval but prompting is disabled.".into(),
                reason: crate::types::permission::PermissionReason::SafetyCheck,
            };
        }
        crate::permission::planning::intersect(tool, ctx, decision).await
    }
}

pub(crate) fn subagent_permission_ceiling(
    original_config: &crate::schemas::config::ResolvedConfig,
    parent: Arc<dyn PermissionEngine>,
    live_approval_policy: Option<SubagentApprovalPolicyReader>,
    boundary_approval_policy: Option<ApprovalPolicy>,
) -> Arc<dyn PermissionEngine> {
    build_subagent_permission_ceiling(
        original_config,
        parent,
        false,
        live_approval_policy,
        boundary_approval_policy,
    )
}

pub(crate) fn subagent_plan_permission_floor(
    original_config: &crate::schemas::config::ResolvedConfig,
    parent: Arc<dyn PermissionEngine>,
    live_approval_policy: Option<SubagentApprovalPolicyReader>,
    boundary_approval_policy: Option<ApprovalPolicy>,
) -> Arc<dyn PermissionEngine> {
    build_subagent_permission_ceiling(
        original_config,
        parent,
        true,
        live_approval_policy,
        boundary_approval_policy,
    )
}

fn build_subagent_permission_ceiling(
    original_config: &crate::schemas::config::ResolvedConfig,
    parent: Arc<dyn PermissionEngine>,
    plan_only: bool,
    live_approval_policy: Option<SubagentApprovalPolicyReader>,
    boundary_approval_policy: Option<ApprovalPolicy>,
) -> Arc<dyn PermissionEngine> {
    let mut config = original_config.clone();
    // Keep creation-time config grants; the live parent must still authorize each call.
    config.use_stored_permission_rules = false;
    let ceiling = StaticPermissionEngine::new(&config);
    // Planning is an independent effect ceiling, unaffected by ordinary grants.
    config.allow.clear();
    Arc::new(SubagentPermissionCeiling {
        parent,
        ceiling,
        plan_ceiling: StaticPermissionEngine::new(&config).without_context_grants(),
        plan_only,
        captured_untrusted_review: original_config.approval_policy
            == Some(ApprovalPolicy::UnlessTrusted)
            || boundary_approval_policy == Some(ApprovalPolicy::UnlessTrusted),
        live_approval_policy,
    })
}

pub(crate) struct SkillShellAuthority {
    pub(crate) planning_read_only: bool,
    pub(crate) bash_tool: Option<super::AnyTool>,
    pub(crate) runtime_policy: Arc<super::RuntimePolicySnapshot>,
    pub(crate) permission_engine: Arc<dyn PermissionEngine>,
}

pub(crate) fn tighten_subagent_permission(
    original: &PermissionContext,
    current: &PermissionContext,
    frozen: bool,
) -> PermissionContext {
    let mut permission = original.clone();
    for rule in &current.deny_rules {
        if !permission.deny_rules.contains(rule) {
            permission.deny_rules.push(rule.clone());
        }
    }
    for rule in &current.ask_rules {
        if !permission.ask_rules.contains(rule) {
            permission.ask_rules.push(rule.clone());
        }
    }
    if frozen {
        // Exact grants are a conservative intersection: never import new rules or roots.
        permission
            .allow_rules
            .retain(|rule| current.allow_rules.contains(rule));
        permission
            .additional_writable_roots
            .retain(|root| current.additional_writable_roots.contains(root));
        permission.should_avoid_prompts |= current.should_avoid_prompts;
    }
    permission
}

fn intersect_workspace_roots(
    original: &[RuntimeString],
    current: &[RuntimeString],
) -> Vec<RuntimeString> {
    let mut roots = Vec::new();
    for original in original {
        let original = RuntimePath::from(original.clone());
        for current in current {
            let current = RuntimePath::from(current.clone());
            let narrower = if !crate::fs::node_path::is_absolute_runtime(&original)
                || !crate::fs::node_path::is_absolute_runtime(&current)
            {
                None
            } else {
                // Root aliases are valid inputs, but their resolved targets must
                // intersect before a child can materialize a narrower authority.
                crate::fs::path_safety::canonical_descendant_scope(&original, &original)
                    .zip(crate::fs::path_safety::canonical_descendant_scope(
                        &current, &current,
                    ))
                    .and_then(|(original, current)| {
                        crate::fs::path_safety::canonical_descendant_scope(&original, &current)
                            .or_else(|| {
                                crate::fs::path_safety::canonical_descendant_scope(
                                    &current, &original,
                                )
                            })
                    })
            };
            if let Some(root) = narrower {
                if !roots.contains(root.as_runtime_string()) {
                    roots.push(root.as_runtime_string().clone());
                }
            }
        }
    }
    roots
}

/// Materialize the canonical intersection for captured additional roots.
/// The working directory is implicit; admitted task roots remain a separate capture.
pub(crate) fn constrain_subagent_canonical_roots(
    permission: &mut PermissionContext,
    captured: &super::RuntimePolicyState,
    effective: &super::RuntimePolicyState,
    origin: Option<&super::SubagentPermissionOrigin>,
) {
    if effective.sandbox_mode != Some(crate::types::permission::SandboxMode::WorkspaceWrite) {
        return;
    }
    let captured_roots = &captured.sandbox_workspace_write.writable_roots;
    let mut roots = Vec::new();
    for root in &permission.additional_writable_roots {
        let scoped = origin.is_some_and(|origin| origin.is_task_scoped_root(root))
            && !captured_roots.contains(root.as_runtime_string());
        let candidate = [root.as_runtime_string().clone()];
        // Provenance recognition must not use successful canonical validation:
        // a redirected descendant is still a captured root that must be revoked.
        let captured_lexically = captured_roots.iter().any(|captured| {
            let captured = RuntimePath::from(captured.clone());
            crate::fs::path_safety::is_within_roots(root, std::slice::from_ref(&captured))
                || crate::fs::path_safety::is_within_roots(&captured, std::slice::from_ref(root))
        });
        if scoped
            || (!captured_lexically
                && intersect_workspace_roots(&candidate, captured_roots).is_empty())
        {
            if !roots.contains(root) {
                roots.push(root.clone());
            }
            continue;
        }
        for narrowed in intersect_workspace_roots(
            &candidate,
            &effective.sandbox_workspace_write.writable_roots,
        ) {
            let narrowed = RuntimePath::from(narrowed);
            if !roots.contains(&narrowed) {
                roots.push(narrowed);
            }
        }
    }
    permission.additional_writable_roots = roots;
}

pub(crate) fn intersect_subagent_runtime_state(
    original: &super::RuntimePolicyState,
    current: &super::RuntimePolicyState,
) -> super::RuntimePolicyState {
    use crate::types::permission::{ApprovalPolicy, SandboxMode};
    let original_mode = original.sandbox_mode.unwrap_or(SandboxMode::ReadOnly);
    let current_mode = current.sandbox_mode.unwrap_or(SandboxMode::ReadOnly);
    let mut workspace = original.sandbox_workspace_write.clone();
    workspace.writable_roots = match (original_mode, current_mode) {
        (SandboxMode::DangerFullAccess, _) => {
            current.sandbox_workspace_write.writable_roots.clone()
        }
        (_, SandboxMode::DangerFullAccess) => {
            original.sandbox_workspace_write.writable_roots.clone()
        }
        _ => intersect_workspace_roots(
            &original.sandbox_workspace_write.writable_roots,
            &current.sandbox_workspace_write.writable_roots,
        ),
    };
    workspace.network_access = original.network_allowed && current.network_allowed;
    workspace.exclude_tmpdir_env_var |= current.sandbox_workspace_write.exclude_tmpdir_env_var;
    workspace.exclude_slash_tmp |= current.sandbox_workspace_write.exclude_slash_tmp;
    super::RuntimePolicyState {
        approval_policy: if original.approval_policy == Some(ApprovalPolicy::Never)
            || current.approval_policy == Some(ApprovalPolicy::Never)
        {
            Some(ApprovalPolicy::Never)
        } else {
            original.approval_policy.or(current.approval_policy)
        },
        sandbox_mode: Some(original_mode.intersect(current_mode)),
        approvals_reviewer: original.approvals_reviewer.or(current.approvals_reviewer),
        generation: current.generation,
        sandbox_workspace_write: workspace,
        network_allowed: original.network_allowed && current.network_allowed,
    }
}

/// A child may narrow the captured parent policy, never replace its ceiling.
pub(crate) fn subagent_runtime_policy_state_for_update(
    parent: &super::RuntimePolicyState,
    update: &super::RuntimePolicyUpdate,
) -> Result<super::RuntimePolicyState, String> {
    let mut requested = parent.clone();
    requested.approval_policy = update.approval_policy.or(parent.approval_policy);
    requested.sandbox_mode = update.sandbox_mode.or(parent.sandbox_mode);
    requested.approvals_reviewer = update.approvals_reviewer.or(parent.approvals_reviewer);
    if let Some(workspace) = &update.sandbox_workspace_write {
        requested.sandbox_workspace_write = workspace.clone();
        requested
            .sandbox_workspace_write
            .normalize_writable_roots(None)?;
    }
    requested.network_allowed = if update.sandbox_mode.is_some()
        || update.sandbox_workspace_write.is_some()
        || update.network_allowed.is_some()
    {
        super::runtime_network_allowed(
            requested.sandbox_mode,
            update.network_allowed,
            &requested.sandbox_workspace_write,
        )
    } else {
        parent.network_allowed
    };
    Ok(intersect_subagent_runtime_state(parent, &requested))
}

pub(crate) fn intersect_subagent_runtime_policy(
    original: &super::RuntimePolicySnapshot,
    current: &super::RuntimePolicySnapshot,
) -> Arc<super::RuntimePolicySnapshot> {
    use crate::types::permission::SandboxMode;
    let state = intersect_subagent_runtime_state(&original.state, &current.state);
    // Task scope and managed network bindings are immutable for this session.
    // Keep their captured decorators instead of selecting a new platform backend.
    let selected = if state.sandbox_mode
        == Some(current.state.sandbox_mode.unwrap_or(SandboxMode::ReadOnly))
    {
        current
    } else {
        original
    };
    Arc::new(super::RuntimePolicySnapshot {
        state,
        sandbox_backend: selected.sandbox_backend.clone(),
    })
}

pub(crate) fn apply_subagent_runtime_policy(
    policy: &super::RuntimePolicySnapshot,
    context: &mut crate::types::tool::ToolContext,
    memory_maintenance: bool,
) {
    policy.apply_to_context(context);
    if !memory_maintenance
        || context.planning_read_only
        || policy.sandbox_backend.enforces_planning_read_only()
        || (policy.sandbox_backend.task_resource_scope().is_some()
            && policy.state.sandbox_mode == Some(crate::types::permission::SandboxMode::ReadOnly))
    {
        return;
    }
    let Some(root) = context.permission.pinned_memory_root.as_ref() else {
        return;
    };
    if root.validate_root_namespace().is_err() {
        return;
    }
    // This native file-only capability is created by the maintenance scheduler.
    // Its pinned root remains the boundary even when the interactive parent is read-only.
    let root = crate::fs::node_path::runtime_path_from_native(root.canonical_root());
    context.cwd = root.clone();
    context.permission.additional_writable_roots = vec![root.clone()];
    let mut state = policy.state();
    state.sandbox_mode = Some(crate::types::permission::SandboxMode::WorkspaceWrite);
    state.network_allowed = false;
    state.sandbox_workspace_write.writable_roots = vec![root.into_runtime_string()];
    state.sandbox_workspace_write.network_access = false;
    state.sandbox_workspace_write.exclude_tmpdir_env_var = true;
    state.sandbox_workspace_write.exclude_slash_tmp = true;
    context.sandbox = policy.sandbox_backend.clone();
    super::RuntimePolicySnapshot {
        state,
        sandbox_backend: policy.sandbox_backend.clone(),
    }
    .apply_to_context(context);
}

impl SessionWorkspaceState {
    // ---- lock-scoped accessors for the working-tree cluster ----
    //
    // These own every raw lock touch on the cluster's three fields, so callers
    // read/mutate the working tree through named intent instead of reaching
    // into the `RwLock`/`Mutex` directly. Each method takes and releases a
    // single lock (never held across an await); worktree enter/exit sequences
    // that must order several of these writes against `switch_cwd` stay in
    // `Session` where that orchestration lives.

    /// The live working directory.
    pub(crate) fn cwd(&self) -> RuntimePath {
        self.cwd.read_recover().clone()
    }

    /// Replace the live working directory.
    pub(crate) fn set_cwd(&self, v: RuntimePath) {
        *self.cwd.write_recover() = v;
    }

    /// The cwd captured before the active worktree was entered.
    pub(crate) fn original_cwd(&self) -> RuntimePath {
        self.original_cwd.read_recover().clone()
    }

    /// Record the cwd to restore when the active worktree is left.
    pub(crate) fn set_original_cwd(&self, v: RuntimePath) {
        *self.original_cwd.write_recover() = v;
    }

    /// Whether an isolated worktree is currently active.
    pub(crate) fn is_in_worktree(&self) -> bool {
        self.current_worktree.lock_recover().is_some()
    }

    /// A clone of the active worktree session, if any.
    pub(crate) fn worktree_snapshot(&self) -> Option<WorktreeSession> {
        self.current_worktree.lock_recover().clone()
    }

    /// Install or clear the active worktree session.
    pub(crate) fn set_worktree(&self, wt: Option<WorktreeSession>) {
        *self.current_worktree.lock_recover() = wt;
    }
}

impl Session {
    // ---- worktree isolation + cwd switching ----

    /// Persist the current worktree state as a rollout event (a null payload clears it).
    pub(crate) async fn persist_worktree_state(&self) {
        let payload = self
            .workspace
            .worktree_snapshot()
            .as_ref()
            .map(worktree_to_json);
        if let Some(r) = self.persistence.rollout() {
            r.append_event(
                "worktree_state".into(),
                Some(AppendEventOptions {
                    turn_id: None,
                    payload: Some(payload.unwrap_or(Json::Null)),
                }),
            )
            .await;
        }
    }

    /// Switch the live cwd. When using the engine's default (cwd-derived) system
    /// prompt, rebuild it so its env/git context reflects the new directory.
    pub(crate) fn switch_cwd(&self, to: RuntimePath) {
        self.set_cwd(to);
        // Drop the checkpoint service pinned to the OLD directory.
        self.persistence.clear_checkpoint_service();
        self.rebuild_default_system_prompt();
    }

    pub(crate) fn rebuild_default_system_prompt(&self) {
        if self.system_prompt_is_default {
            let mcp_instructions = self.mcp.server_instructions();
            let active_style = self
                .plugin_projection()
                .map(|projection| projection.active_output_style.clone())
                .unwrap_or_else(|| self.active_output_style.clone());
            let output_style =
                active_style
                    .as_ref()
                    .map(|s| crate::context::system_prompt::OutputStyle {
                        name: s.name.clone(),
                        body: s.body.clone(),
                    });
            let provider = Arc::new(create_system_prompt_provider(
                self.process_context.clone(),
                SystemPromptProviderOptions {
                    cwd: self.cwd_now(),
                    config: self.inference_config(),
                    // Anchor the date to the STABLE session-start time, not a fresh clock read.
                    now_ms: Some(self.meta.created_at),
                    is_worktree: self.workspace.is_in_worktree(),
                    mcp_instructions,
                    output_style,
                    user_config_dir: self.user_config_dir.clone(),
                },
            ));
            let refresh = provider.clone();
            *self.workspace_instruction_cache_refresh.write_recover() =
                Some(Arc::new(move || refresh.invalidate_memory()));
            let sp: SystemPromptProvider = Arc::new(move || {
                let p = provider.clone();
                Box::pin(async move { p.prompt().await })
            });
            *self.system_prompt_provider.write_recover() =
                append_developer_instructions(sp, self.developer_instructions.clone());
        }
    }

    /// Custom host providers retain their own cache policy. Only the concrete
    /// default instruction loader installs an invalidation callback.
    pub(crate) fn refresh_workspace_instruction_cache(&self) {
        if let Some(refresh) = self
            .workspace_instruction_cache_refresh
            .read_recover()
            .clone()
        {
            refresh();
        }
    }

    /// Generate a short, slug-valid random worktree name when none is supplied.
    pub(crate) fn random_worktree_slug(&self) -> String {
        let hex: String = (self.deps.uuid)()
            .replace('-', "")
            .chars()
            .take(8)
            .collect();
        format!("wt-{hex}")
    }

    /// Enter an isolated worktree and switch the session's cwd into it.
    pub(crate) async fn enter_worktree(
        self: &Arc<Self>,
        name: Option<RuntimeString>,
    ) -> Result<WorktreeEnterResult, String> {
        {
            if let Some(wt) = self.workspace.worktree_snapshot().as_ref() {
                return Err(format!(
                    "Already in worktree \"{}\". Exit it before entering another.",
                    wt.worktree_name
                        .try_to_utf8()
                        .expect("validated worktree name remains scalar")
                ));
            }
        }
        let slug = match name.as_ref().and_then(|name| name.try_to_utf8().ok()) {
            Some(name) if !name.trim().is_empty() => name.trim().to_string(),
            _ => self.random_worktree_slug(),
        };
        validate_worktree_slug(&slug)?;
        let original = self.workspace.cwd();
        let original_branch = current_branch(&self.process_context, &original).await;

        // Host-managed (non-git) worktree via hook, if one claims it.
        if let Some(hook_path) = self.try_worktree_create_hook(&slug).await {
            self.workspace.set_worktree(Some(WorktreeSession {
                original_cwd: original.clone(),
                worktree_path: hook_path.clone(),
                worktree_name: slug.clone().into(),
                worktree_branch: None,
                original_branch: original_branch.clone(),
                original_head_commit: None,
                hook_based: Some(true),
            }));
            self.workspace.set_original_cwd(original);
            self.switch_cwd(hook_path.clone());
            self.persist_worktree_state().await;
            return Ok(WorktreeEnterResult {
                worktree_path: hook_path,
                worktree_branch: None,
                message: RuntimeString::from(format!("Entered host-managed worktree \"{slug}\".")),
            });
        }

        let root = match find_canonical_git_root(&self.process_context, &original).await {
            Some(r) => r,
            None => {
                return Err("Not inside a git repository; cannot create a worktree.".to_string())
            }
        };
        let created = get_or_create_worktree(&self.process_context, &root, &slug, None).await?;
        self.workspace.set_worktree(Some(WorktreeSession {
            original_cwd: original.clone(),
            worktree_path: created.worktree_path.clone(),
            worktree_name: slug.clone().into(),
            worktree_branch: Some(created.worktree_branch.clone()),
            original_branch: original_branch.clone(),
            original_head_commit: created.head_commit.clone(),
            hook_based: None,
        }));
        self.workspace.set_original_cwd(original);
        self.switch_cwd(created.worktree_path.clone());
        self.persist_worktree_state().await;
        Ok(WorktreeEnterResult {
            worktree_path: created.worktree_path,
            worktree_branch: Some(created.worktree_branch.clone()),
            message: if created.existed {
                RuntimeString::from(format!("Resumed worktree \"{slug}\" on branch "))
                    .concat(&created.worktree_branch)
                    .concat(&RuntimeString::from("."))
            } else {
                RuntimeString::from(format!("Created worktree \"{slug}\" on branch "))
                    .concat(&created.worktree_branch)
                    .concat(&RuntimeString::from("."))
            },
        })
    }

    /// Leave the active worktree, restoring the original cwd. `remove` deletes the
    /// worktree + branch (git) or fires `WorktreeRemove` (hook).
    pub(crate) async fn exit_worktree(
        self: &Arc<Self>,
        action: WorktreeExitAction,
    ) -> Result<WorktreeExitResult, String> {
        let wt = match self.workspace.worktree_snapshot() {
            Some(wt) => wt,
            None => return Err("Not currently in a worktree managed by this session.".to_string()),
        };
        let original_cwd = wt.original_cwd.clone();
        // Clear the worktree flag BEFORE rebuilding the prompt (so switchCwd drops the notice).
        self.workspace.set_worktree(None);
        // Restore cwd, so we never stand inside a directory we then remove.
        self.switch_cwd(original_cwd.clone());
        self.workspace.set_original_cwd(original_cwd.clone());

        if action == WorktreeExitAction::Remove {
            if wt.hook_based == Some(true) {
                self.fire_worktree_remove_hook(&wt).await;
            } else {
                let root = find_canonical_git_root(&self.process_context, &original_cwd).await;
                if let Some(root) = root {
                    remove_worktree(
                        &self.process_context,
                        &root,
                        &wt.worktree_path,
                        wt.worktree_branch.as_ref(),
                    )
                    .await;
                }
            }
        }
        self.persist_worktree_state().await;
        Ok(WorktreeExitResult {
            action,
            original_cwd,
            worktree_path: wt.worktree_path.clone(),
            worktree_branch: wt.worktree_branch.clone(),
            message: match action {
                WorktreeExitAction::Remove => RuntimeString::from("Exited and removed worktree \"")
                    .concat(&wt.worktree_name)
                    .concat(&RuntimeString::from("\".")),
                WorktreeExitAction::Keep => RuntimeString::from("Exited worktree \"")
                    .concat(&wt.worktree_name)
                    .concat(&RuntimeString::from("\" (kept on disk).")),
            },
        })
    }

    /// Fire a WorktreeCreate hook; returns its `worktree_path` if a handler claimed it.
    pub(crate) async fn try_worktree_create_hook(&self, slug: &str) -> Option<RuntimePath> {
        let bus = self.current_hook_bus()?;
        if !bus.has(HookEvent::WorktreeCreate) {
            return None;
        }
        let mut input = base_hook_input(HookEvent::WorktreeCreate, &self.cwd_now(), &self.id);
        let mut tool_input = RuntimeObject::new();
        tool_input.insert("name", RuntimeJson::String(slug.into()));
        input.tool_input = Some(RuntimeJson::Object(tool_input));
        let outcome = bus.dispatch(HookEvent::WorktreeCreate, &input).await;
        outcome.worktree_path.map(RuntimePath::from)
    }

    /// Fire a WorktreeRemove hook for a host-managed worktree (best-effort).
    pub(crate) async fn fire_worktree_remove_hook(&self, wt: &WorktreeSession) {
        let Some(bus) = self.current_hook_bus() else {
            return;
        };
        if !bus.has(HookEvent::WorktreeRemove) {
            return;
        }
        let mut input = base_hook_input(HookEvent::WorktreeRemove, &wt.original_cwd, &self.id);
        let mut tool_input = RuntimeObject::new();
        tool_input.insert("name", RuntimeJson::String(wt.worktree_name.clone()));
        tool_input.insert(
            "worktreePath",
            RuntimeJson::String(wt.worktree_path.as_runtime_string().clone()),
        );
        input.tool_input = Some(RuntimeJson::Object(tool_input));
        bus.dispatch(HookEvent::WorktreeRemove, &input).await;
    }

    /// Set up a spawned agent's isolation (a cwd override and/or a worktree).
    pub(crate) async fn setup_agent_isolation(
        self: &Arc<Self>,
        opts: &SpawnSubAgentOptions,
    ) -> Result<AgentIsolation, String> {
        if let Some(cwd) = &opts.cwd {
            return Ok(AgentIsolation {
                cwd_override: Some(cwd.clone()),
                worktree: None,
            });
        }
        if opts.isolation != Some(Isolation::Worktree) {
            return Ok(AgentIsolation {
                cwd_override: None,
                worktree: None,
            });
        }
        let hex: String = (self.deps.uuid)()
            .replace('-', "")
            .chars()
            .take(8)
            .collect();
        let slug = format!("agt-{hex}");
        let cwd = self.cwd_now();
        if let Some(hook_path) = self.try_worktree_create_hook(&slug).await {
            return Ok(AgentIsolation {
                cwd_override: Some(hook_path.clone()),
                worktree: Some(AgentWorktree::Hook {
                    notice: build_worktree_notice(&cwd, &hook_path),
                    path: hook_path,
                }),
            });
        }
        let root = match find_canonical_git_root(&self.process_context, &cwd).await {
            Some(r) => r,
            None => {
                return Ok(AgentIsolation {
                    cwd_override: None,
                    worktree: None,
                })
            }
        };
        let wt = get_or_create_worktree(&self.process_context, &root, &slug, None).await?;
        Ok(AgentIsolation {
            cwd_override: Some(wt.worktree_path.clone()),
            worktree: Some(AgentWorktree::Git {
                notice: build_worktree_notice(&cwd, &wt.worktree_path),
                path: wt.worktree_path,
                root,
                branch: wt.worktree_branch,
                head: wt.head_commit,
            }),
        })
    }

    /// Reclaim a spawned agent's worktree after it finishes. A hook-managed worktree
    /// is always KEPT; a git worktree is removed only when left pristine.
    pub(crate) async fn teardown_agent_isolation(&self, wt: &Option<AgentWorktree>) {
        let Some(wt) = wt else { return };
        let AgentWorktree::Git {
            path,
            root,
            branch,
            head,
            ..
        } = wt
        else {
            return;
        };
        let changes = count_worktree_changes(&self.process_context, path, head.as_ref()).await;
        if let Some(c) = changes {
            if c.changed_files == 0 && c.commits == 0 {
                remove_worktree(&self.process_context, root, path, Some(branch)).await;
            }
        }
    }

    /// Finish setup under an independent owner so cancellation cannot abandon
    /// a worktree between its external creation and the returned handle.
    async fn setup_owned_agent_isolation(
        self: &Arc<Self>,
        opts: &SpawnSubAgentOptions,
    ) -> Result<OwnedAgentIsolation, String> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        {
            let mut registry = self.background_tasks.lock_recover();
            if registry.closing || self.lifecycle.is_disposed() {
                return Err("The parent session is shutting down.".to_owned());
            }
            let owner = self.clone();
            let opts = opts.clone();
            registry.handles.push(tokio::spawn(async move {
                let result = owner
                    .setup_agent_isolation(&opts)
                    .await
                    .map(|isolation| OwnedAgentIsolation { owner, isolation });
                if let Err(Ok(isolation)) = sender.send(result) {
                    isolation.finish().await;
                }
            }));
        }
        receiver
            .await
            .map_err(|_| "Sub-agent isolation setup owner stopped.".to_owned())?
    }

    /// Run an inline-shell command from a skill body through the permission gate.
    pub(crate) async fn run_skill_bash(
        &self,
        command: String,
        permission: PermissionContext,
        cwd: String,
        signal: AbortSignal,
        run_opts: Option<RunBashOptions>,
        planning_read_only: bool,
    ) -> Result<String, ToolError> {
        self.run_skill_bash_with_policy(
            command,
            permission,
            cwd,
            signal,
            run_opts,
            SkillShellAuthority {
                planning_read_only,
                bash_tool: self
                    .current_tools()
                    .into_iter()
                    .find(|t| t.name() == "bash"),
                runtime_policy: self.runtime_policy_snapshot(),
                permission_engine: self.permission_engine.clone(),
            },
        )
        .await
    }

    pub(crate) async fn run_skill_bash_with_policy(
        &self,
        command: String,
        permission: PermissionContext,
        cwd: String,
        signal: AbortSignal,
        run_opts: Option<RunBashOptions>,
        authority: SkillShellAuthority,
    ) -> Result<String, ToolError> {
        let current_policy = self.runtime_policy_snapshot();
        let policy = intersect_subagent_runtime_policy(&authority.runtime_policy, &current_policy);
        let Some(bash_tool) = authority.bash_tool else {
            return Err(ToolError::new(
                "inline shell unavailable: the authorized tool set has no bash tool",
            ));
        };
        let allow_tools = run_opts.as_ref().and_then(|o| o.allow_tools.clone());
        let mut permission = permission;
        if let Some(at) = &allow_tools {
            if !at.is_empty() {
                let exact_rules = at.iter().map(RuntimeString::from).collect::<Vec<_>>();
                permission
                    .allow_rules
                    .extend(parse_rule_strings(&exact_rules));
            }
        }
        // The bash gate is pure command-string analysis (never consults fileState);
        // cwd/permission/signal are what actually scope the run.
        let runtime_cwd = RuntimePath::from(cwd.clone());
        let mut eval_ctx = bare_tool_context(
            self.process_context.clone(),
            self.background_processes.clone(),
            runtime_cwd.clone(),
            signal.clone(),
            self.file_state.clone() as Arc<dyn FileStateCache>,
            permission.clone(),
            policy.sandbox_backend.clone(),
        );
        eval_ctx.planning_read_only = authority.planning_read_only;
        policy.apply_to_context(&mut eval_ctx);
        let mut permission_input = RuntimeObject::new();
        permission_input.insert("command", RuntimeJson::String(command.clone().into()));
        let decision = authority
            .permission_engine
            .evaluate(
                bash_tool.as_ref(),
                &RuntimeJson::Object(permission_input),
                &eval_ctx,
            )
            .await;
        let decision =
            crate::permission::planning::intersect(bash_tool.as_ref(), &eval_ctx, decision).await;
        // Plan mode auto-allows an unprovable command with a read-only
        // enforcement marker; honor it on this inline-shell path too.
        let plan_enforced = matches!(&decision, PermissionDecision::Allow { input, .. }
            if input
                .get(crate::permission::PLAN_MODE_READONLY_ENFORCED_KEY)
                .and_then(RuntimeJson::as_bool)
                .unwrap_or(false));
        match decision {
            PermissionDecision::Allow { .. } => {}
            PermissionDecision::Fatal { error } => return Err(error),
            decision => {
                return Err(ToolError::new(format!(
                    "Inline shell command not permitted ({}): {}",
                    permission_decision_word(&decision),
                    command
                )));
            }
        }
        let backend = policy.sandbox_backend.clone();
        let wrapper = (plan_enforced && backend.kind() != SandboxKind::Process)
            .then(|| PlanReadOnlySandbox::new(backend.clone()));
        let sandbox_for_run: &dyn SandboxBackend = wrapper
            .as_ref()
            .map(|w| w as &dyn SandboxBackend)
            .unwrap_or_else(|| backend.as_ref());
        let network_for_run = if plan_enforced {
            Some(false)
        } else {
            eval_ctx.network_allowed
        };
        let res = run_shell(RunShellOptions {
            process_context: self.process_context.clone(),
            command: RuntimeString::from(command),
            cwd: runtime_cwd.clone(),
            sandbox: sandbox_for_run,
            network_allowed: network_for_run,
            managed_network: None,
            signal,
            writable_roots: Some(
                writable_roots(&PathContext {
                    cwd: &runtime_cwd,
                    additional_writable_roots: &permission.additional_writable_roots,
                    unconfined_writes: writes_are_unconfined(policy.state.sandbox_mode),
                })
                .into_iter()
                .map(RuntimePath::into_runtime_string)
                .collect(),
            ),
            unconfined_writes: writes_are_unconfined(policy.state.sandbox_mode),
            timeout_ms: self.config.tool_timeout_ms,
            max_output_bytes: self.config.max_aggregated_output_bytes as usize,
            env: None,
            on_chunk: None,
        })
        .await;
        Ok(String::from_utf8(res.output.to_node_utf8())
            .expect("Node UTF-8 shell output projection is valid UTF-8"))
    }
}

impl Session {
    // ---- background-agent lifecycle helpers (operate on the shared entry map) ----

    pub(crate) fn teammate_shutdown_requested(&self, agent_id: &str) -> bool {
        self.subagents
            .background_agents
            .lock_recover()
            .get(agent_id)
            .map(|e| e.shutdown_requested)
            .unwrap_or(false)
    }

    pub(crate) fn set_teammate_idle(&self, agent_id: &str, idle: bool) {
        if let Some(e) = self
            .subagents
            .background_agents
            .lock_recover()
            .get_mut(agent_id)
        {
            e.idle = idle;
            if !idle {
                if e.status == BackgroundTaskStatus::Failed {
                    e.partial = RuntimeString::default();
                }
                e.status = BackgroundTaskStatus::Running;
                e.notified = false;
            }
        }
    }

    /// Flag a teammate idle and fire+clear its wait-for-idle callbacks.
    pub(crate) async fn mark_teammate_idle(&self, agent_id: &str) {
        if !self
            .subagents
            .background_agents
            .lock_recover()
            .contains_key(agent_id)
        {
            return;
        }
        if let Err(error) = self.mark_subagent_idle(agent_id).await {
            self.note_subagent_persistence_error(agent_id, error);
        }
        let cbs = {
            let mut m = self.subagents.background_agents.lock_recover();
            let Some(e) = m.get_mut(agent_id) else { return };
            e.idle = true;
            std::mem::take(&mut e.on_idle)
        };
        for cb in cbs {
            let _ = cb.send(());
        }
    }

    /// Deliver a terminal result to its direct parent's passive mailbox.
    pub(crate) async fn notify_background_complete(&self, agent_id: &str) {
        if self.lifecycle.is_disposed() {
            return;
        }
        let Some((payload, state, spawn_turn_id)) =
            self.take_background_completion_notice(agent_id)
        else {
            return;
        };
        self.publish_background_completion_notice(agent_id, payload, state, spawn_turn_id)
            .await;
    }

    pub(crate) fn take_background_completion_notice(
        &self,
        agent_id: &str,
    ) -> Option<(RuntimeString, &'static str, Option<RuntimeString>)> {
        let (mut payload, state, spawn_turn_id, partial) = {
            let mut m = self.subagents.background_agents.lock_recover();
            let e = m.get_mut(agent_id)?;
            if e.notified || e.status == BackgroundTaskStatus::Running {
                return None;
            }
            e.notified = true;
            let state = match e.status {
                BackgroundTaskStatus::Failed => "failed",
                // A task the parent session lost to a restart never produced a
                // final answer. Reporting it as "completed" would send the model
                // to read a result that does not exist.
                BackgroundTaskStatus::Interrupted => "interrupted",
                BackgroundTaskStatus::Completed => "completed",
                BackgroundTaskStatus::Running => unreachable!("nonterminal notices are rejected"),
            };
            (
                self.render_background_completion_notice(&e.description, state),
                state,
                e.spawn_turn_id.clone(),
                e.partial.clone(),
            )
        };
        let result = if state != "completed" && !partial.is_empty() {
            partial
        } else {
            self.subagents
                .lifecycle
                .lock_recover()
                .get(agent_id)
                .and_then(|record| {
                    if state == "completed" {
                        record.last_result.clone()
                    } else {
                        record.last_error.clone()
                    }
                })
                .unwrap_or(partial)
        };
        payload = payload
            .concat(&RuntimeString::from("\nAgent: "))
            .concat(&RuntimeString::from(agent_id))
            .concat(&RuntimeString::from("\nResult:\n"))
            .concat(&result);
        Some((payload, state, spawn_turn_id))
    }

    pub(crate) async fn publish_background_completion_notice(
        &self,
        agent_id: &str,
        payload: RuntimeString,
        state: &'static str,
        spawn_turn_id: Option<RuntimeString>,
    ) {
        let parent_agent_id = {
            self.subagents
                .lifecycle
                .lock_recover()
                .get(agent_id)
                .and_then(|record| record.context.parent_agent_id.clone())
        };
        let parent_agent_id = parent_agent_id.or_else(|| {
            self.subagents
                .background_agents
                .lock_recover()
                .get(agent_id)
                .and_then(|entry| entry.parent_agent_id.clone())
        });
        let nested = self
            .subagents
            .background_agents
            .lock_recover()
            .get(agent_id)
            .is_some_and(|entry| entry.depth > 1);
        if nested && parent_agent_id.is_none() {
            tracing::warn!(
                agent_id,
                "nested completion has no direct parent; refusing root delivery"
            );
            return;
        }
        let notification_id: RuntimeString = (self.deps.uuid)().into();
        let received = self
            .conversation
            .queue
            .enqueue_pending_notification(QueuedMessage {
                trigger_turn: Some(false),
                value: QueuedValue::Text(payload),
                mode: "task-notification".into(),
                is_meta: Some(true),
                origin: Some("background-task".into()),
                priority: Some(QueuePriority::Next),
                uuid: Some(notification_id.clone()),
                session_id: Some(self.id.clone()),
                agent_id: parent_agent_id.clone(),
                correlation_id: None,
                pdf_bytes: 0,
                pdf_documents: 0,
                delivery: None,
                display_metadata: None,
            });
        if received.is_ok() {
            let receipt = crate::schemas::agent_notification::AgentNotification {
                notification_id,
                source: "background-task".into(),
                status: state.into(),
                agent_id: Some(agent_id.into()),
                agent_name: None,
                task_id: None,
                timestamp: (self.deps.now)(),
                turn_id: spawn_turn_id.clone(),
            };
            if let Some(parent_agent_id) = parent_agent_id {
                let rollout = self.subagents.parent_rollouts.writer(&parent_agent_id);
                if let Ok(parent_id) = parent_agent_id.try_to_utf8() {
                    if let Err(error) = self
                        .publish_subagent_notification(
                            &parent_id,
                            receipt,
                            spawn_turn_id.as_ref(),
                            rollout.as_ref(),
                            true,
                        )
                        .await
                    {
                        self.note_subagent_persistence_error(agent_id, error);
                    }
                }
            } else if let Err(error) = self.publish_agent_notification(receipt).await {
                self.note_subagent_persistence_error(agent_id, error);
            }
        }
    }

    /// Terminal-fail a background entry whose isolation setup failed before it ran.
    pub(crate) async fn fail_background_setup(
        &self,
        agent_id: &str,
        output_file: &str,
        message: String,
    ) {
        let body = format!("[failed] {message}");
        let is_teammate = {
            let mut m = self.subagents.background_agents.lock_recover();
            let Some(e) = m.get_mut(agent_id) else { return };
            e.partial = body.clone().into();
            e.teammate
        };
        write_output_atomic(output_file, &body).await;
        if let Some(e) = self
            .subagents
            .background_agents
            .lock_recover()
            .get_mut(agent_id)
        {
            e.status = BackgroundTaskStatus::Failed;
        }
        self.notify_background_complete(agent_id).await;
        if is_teammate {
            self.mark_teammate_idle(agent_id).await;
        }
    }

    /// Block until this teammate's next swarm mailbox message, or `None` on abort /
    /// graceful shutdown / team removal.
    pub(crate) async fn wait_for_teammate_message(
        &self,
        team: &RuntimeString,
        peer: &RuntimeString,
        signal: &AbortSignal,
        agent_id: &str,
        floor: i64,
        host_observable: bool,
    ) -> Option<(RuntimeString, i64, Vec<crate::schemas::message::Message>)> {
        let mut parked = false;
        loop {
            if signal.is_cancelled() || self.teammate_shutdown_requested(agent_id) {
                return None;
            }
            if swarm_team_confirmed_gone(team).await {
                return None;
            }
            let _ = swarm_heartbeat_peer(team, peer, (self.deps.now)()).await;
            let disk_cursor = swarm_read_cursor(team, peer).await;
            let from = disk_cursor
                .max(floor)
                .max(self.swarm_delivery_state.floor(team, peer).await);
            let batch = match swarm_read_inbox_from(team, peer, from, None).await {
                Ok(b) => b,
                Err(_) => {
                    if swarm_team_confirmed_gone(team).await {
                        return None;
                    }
                    abortable_sleep(TEAMMATE_IDLE_POLL_MS, signal).await;
                    continue;
                }
            };
            if !batch.messages.is_empty() {
                self.set_teammate_idle(agent_id, false);
                // The previous child turn has closed its writer. Append receipts
                // to that child's own rollout before consuming the next prompt.
                let rollout: Option<Arc<dyn LiveRollout>> =
                    if let Some(store) = self.persistence.store() {
                        let session_id = runtime_session_id_to_session_store_host(&self.id);
                        match store.load_sub_agent(&session_id, agent_id).await {
                            Ok(loaded) => {
                                self.restore_subagent_notification_ids(
                                    agent_id,
                                    loaded.turn_index.as_ref(),
                                );
                                match loaded.meta {
                                    Some(meta) => store
                                        .open_sub_agent(&session_id, agent_id, meta)
                                        .await
                                        .ok()
                                        .map(Arc::from),
                                    None => None,
                                }
                            }
                            Err(_) => None,
                        }
                    } else {
                        None
                    };
                for receipt in crate::tasks::swarm_store::mailbox_delivery_receipts(
                    team,
                    peer,
                    from,
                    &batch.messages,
                    (self.deps.now)(),
                ) {
                    if let Err(error) = self
                        .publish_subagent_notification(
                            agent_id,
                            receipt,
                            None,
                            rollout.as_ref(),
                            host_observable,
                        )
                        .await
                    {
                        self.note_subagent_persistence_error(agent_id, error);
                        if let Some(writer) = &rollout {
                            if let Err(error) = writer.shutdown().await {
                                self.note_subagent_persistence_error(
                                    agent_id,
                                    error.to_string().into(),
                                );
                            }
                        }
                        return None;
                    }
                }
                if let Some(rollout) = rollout {
                    let mut failure = None;
                    if let Err(error) = rollout.flush().await {
                        failure = Some(RuntimeString::from(error.to_string()));
                    }
                    if let Err(error) = rollout.shutdown().await {
                        failure.get_or_insert_with(|| RuntimeString::from(error.to_string()));
                    }
                    if let Some(error) = failure {
                        self.note_subagent_persistence_error(agent_id, error);
                        return None;
                    }
                }
                let lines = batch
                    .messages
                    .iter()
                    .map(|message| {
                        message
                            .from
                            .concat(&RuntimeString::from(": "))
                            .concat(&message.body)
                    })
                    .collect::<Vec<_>>();
                let text = join_runtime_strings(&lines, "\n\n");
                let messages = crate::tasks::swarm_store::mailbox_received_messages(
                    team,
                    peer,
                    from,
                    &batch.messages,
                );
                return Some((text, batch.next_cursor, messages));
            }
            if !parked {
                self.mark_teammate_idle(agent_id).await;
                parked = true;
            }
            abortable_sleep(TEAMMATE_IDLE_POLL_MS, signal).await;
        }
    }

    /// Block until every persistent teammate spawned by this session is idle
    /// (parked) or terminated.
    pub(crate) async fn wait_for_teammates_idle(&self) {
        let receivers: Vec<tokio::sync::oneshot::Receiver<()>> = {
            let mut m = self.subagents.background_agents.lock_recover();
            let mut rxs = Vec::new();
            for (_id, e) in m.iter_mut() {
                if e.teammate && e.status == BackgroundTaskStatus::Running && !e.idle {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    e.on_idle.push(tx);
                    rxs.push(rx);
                }
            }
            rxs
        };
        for rx in receivers {
            let _ = rx.await;
        }
    }

    /// Ask persistent teammates to shut down gracefully.
    pub fn request_teammate_shutdown(&self, agent_id: Option<String>) {
        let mut m = self.subagents.background_agents.lock_recover();
        for (id, e) in m.iter_mut() {
            if !e.teammate {
                continue;
            }
            if let Some(target) = &agent_id {
                if id != target {
                    continue;
                }
            }
            e.shutdown_requested = true;
        }
    }

    // ---- spawn factories ----

    /// A foreground sub-agent spawn fn: returns the child's stable id + event stream.
    ///
    /// Production spawn sites go through `make_spawn_sub_agent_with_permission_origin` so a
    /// child inherits its spawning step's frozen bundle. This no-capture wrapper is
    /// kept only for unit tests that drive a spawn directly without a live step.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn make_spawn_sub_agent(
        self: &Arc<Self>,
        depth: u32,
        parent_signal: AbortSignal,
        permission: PermissionContext,
        get_conversation: GetConversation,
        in_fork_child: bool,
        parent_agent_id: Option<String>,
        task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
    ) -> SpawnForegroundSubAgent {
        self.make_spawn_sub_agent_with_capture(
            depth,
            parent_signal,
            permission,
            get_conversation,
            in_fork_child,
            parent_agent_id,
            task_plan,
            None,
            None,
        )
    }

    /// A direct test spawn with captured inference and conservative permissions.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn make_spawn_sub_agent_with_capture(
        self: &Arc<Self>,
        depth: u32,
        parent_signal: AbortSignal,
        permission: PermissionContext,
        get_conversation: GetConversation,
        in_fork_child: bool,
        parent_agent_id: Option<String>,
        task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
        parent_capture: Option<Arc<super::InferenceActivityGuard>>,
        initiating_turn: Option<RuntimeString>,
    ) -> SpawnForegroundSubAgent {
        self.make_spawn_sub_agent_with_permission_origin(
            depth,
            parent_signal,
            permission,
            get_conversation,
            in_fork_child,
            parent_agent_id,
            task_plan,
            parent_capture,
            initiating_turn,
            None,
            Some(SubagentCapabilityCapture::capture(
                self.runtime_capability_snapshot_for_turn(&super::TurnOptions::default()),
            )),
            None,
        )
    }

    /// Bind the admitted permission origin before a tool can await approval or hooks.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn make_spawn_sub_agent_with_permission_origin(
        self: &Arc<Self>,
        depth: u32,
        parent_signal: AbortSignal,
        permission: PermissionContext,
        get_conversation: GetConversation,
        in_fork_child: bool,
        parent_agent_id: Option<String>,
        task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
        parent_capture: Option<Arc<super::InferenceActivityGuard>>,
        initiating_turn: Option<RuntimeString>,
        permission_origin: Option<SubagentPermissionOrigin>,
        runtime_capabilities: Option<SubagentCapabilityCapture>,
        collaboration_mode: Option<crate::planning::CollaborationMode>,
    ) -> SpawnForegroundSubAgent {
        let collaboration_mode = collaboration_mode.unwrap_or_else(|| self.planning.mode());
        let this = self.clone();
        Arc::new(move |opts: SpawnSubAgentOptions| {
            let this = this.clone();
            let permission = permission.clone();
            let get_conversation = get_conversation.clone();
            let parent_agent_id = parent_agent_id.clone();
            let task_plan = task_plan.clone();
            let parent_capture = parent_capture.clone();
            let permission_origin = permission_origin.clone();
            let runtime_capabilities = runtime_capabilities.clone();
            let parent_signal = parent_signal.clone();
            let initiating_turn = initiating_turn.clone();
            Box::pin(async move {
                let task_plan = task_plan
                    .as_ref()
                    .map(crate::tasks::task_store::TaskPlanBinding::fork);
                this.spawn_ordinary_foreground(
                    opts,
                    depth,
                    parent_signal.clone(),
                    permission.clone(),
                    get_conversation.clone(),
                    in_fork_child,
                    parent_agent_id.clone(),
                    task_plan,
                    parent_capture.clone(),
                    initiating_turn.clone(),
                    permission_origin.clone(),
                    runtime_capabilities.clone(),
                    collaboration_mode,
                    SubagentKind::Foreground,
                )
                .await
            })
        })
    }

    /// A workflow-runtime spawn function using the shared child admission pool.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn make_spawn_workflow_agent(
        self: &Arc<Self>,
        depth: u32,
        parent_signal: AbortSignal,
        permission: PermissionContext,
        parent_agent_id: Option<String>,
        task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
        parent_capture: Option<Arc<super::InferenceActivityGuard>>,
        initiating_turn: Option<RuntimeString>,
        runtime_capabilities: SubagentCapabilityCapture,
        collaboration_mode: crate::planning::CollaborationMode,
    ) -> SpawnSubAgent {
        let this = self.clone();
        Arc::new(move |opts: SpawnSubAgentOptions| {
            let task_plan = task_plan
                .as_ref()
                .map(crate::tasks::task_store::TaskPlanBinding::fork);
            this.run_workflow_agent(
                opts,
                depth + 1,
                parent_signal.clone(),
                permission.clone(),
                parent_agent_id.clone(),
                task_plan.clone(),
                parent_capture.clone(),
                initiating_turn.clone(),
                runtime_capabilities.clone(),
                collaboration_mode,
            )
        })
    }

    /// A background-spawn fn: fire-and-forget on a fresh (dispose-linked) token,
    /// writing the final result to a temp file.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn make_spawn_background_agent(
        self: &Arc<Self>,
        depth: u32,
        spawn_turn_id: Option<RuntimeString>,
        permission: PermissionContext,
        get_conversation: GetConversation,
        in_fork_child: bool,
        parent_agent_id: Option<String>,
        task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
    ) -> SpawnBackgroundAgent {
        self.make_spawn_background_agent_with_capture(
            depth,
            spawn_turn_id,
            permission,
            get_conversation,
            in_fork_child,
            parent_agent_id,
            task_plan,
            None,
        )
    }

    /// A direct test spawn without paired permission-origin evidence.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn make_spawn_background_agent_with_capture(
        self: &Arc<Self>,
        depth: u32,
        spawn_turn_id: Option<RuntimeString>,
        permission: PermissionContext,
        get_conversation: GetConversation,
        in_fork_child: bool,
        parent_agent_id: Option<String>,
        task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
        parent_capture: Option<Arc<super::InferenceActivityGuard>>,
    ) -> SpawnBackgroundAgent {
        self.make_spawn_background_agent_with_permission_origin(
            depth,
            self.turn.current_abort_snapshot().unwrap_or_default(),
            spawn_turn_id,
            permission,
            get_conversation,
            in_fork_child,
            parent_agent_id,
            task_plan,
            parent_capture,
            None,
            Some(SubagentCapabilityCapture::capture(
                self.runtime_capability_snapshot_for_turn(&super::TurnOptions::default()),
            )),
            None,
        )
    }

    /// Detached descendants retain the same admitted permission and inference evidence.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn make_spawn_background_agent_with_permission_origin(
        self: &Arc<Self>,
        depth: u32,
        caller_signal: AbortSignal,
        spawn_turn_id: Option<RuntimeString>,
        permission: PermissionContext,
        get_conversation: GetConversation,
        in_fork_child: bool,
        parent_agent_id: Option<String>,
        task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
        parent_capture: Option<Arc<super::InferenceActivityGuard>>,
        permission_origin: Option<SubagentPermissionOrigin>,
        runtime_capabilities: Option<SubagentCapabilityCapture>,
        collaboration_mode: Option<crate::planning::CollaborationMode>,
    ) -> SpawnBackgroundAgent {
        let collaboration_mode = collaboration_mode.unwrap_or_else(|| self.planning.mode());
        let this = self.clone();
        Arc::new(move |opts: SpawnSubAgentOptions| {
            let this = this.clone();
            let permission = permission.clone();
            let get_conversation = get_conversation.clone();
            let parent_agent_id = parent_agent_id.clone();
            let task_plan = task_plan.clone();
            let parent_capture = parent_capture.clone();
            let permission_origin = permission_origin.clone();
            let runtime_capabilities = runtime_capabilities.clone();
            let spawn_turn_id = spawn_turn_id.clone();
            let caller_signal = caller_signal.clone();
            Box::pin(async move {
                tokio::select! {
                    biased;
                    _ = caller_signal.cancelled() => Err("Sub-agent creation was cancelled.".into()),
                    result = async move {
                let task_plan = task_plan
                    .as_ref()
                    .map(crate::tasks::task_store::TaskPlanBinding::fork);
                if opts.swarm.is_none()
                    && opts.persistent != Some(true)
                    && opts.memory_dir.is_none()
                {
                    return this
                        .spawn_ordinary_subagent(
                            opts,
                            depth,
                            permission.clone(),
                            get_conversation.clone(),
                            in_fork_child,
                            parent_agent_id.clone(),
                            task_plan,
                            parent_capture.clone(),
                            spawn_turn_id.clone(),
                            permission_origin.clone(),
                            runtime_capabilities.clone(),
                            collaboration_mode,
                        )
                        .await;
                }
                let runtime_capabilities = runtime_capabilities.clone().unwrap_or_else(|| {
                    SubagentCapabilityCapture::capture(
                        this.runtime_capability_snapshot_for_turn(&super::TurnOptions::default()),
                    )
                });
                let abort = CancellationToken::new();
                let inference = parent_capture
                    .clone()
                    .map(Ok)
                    .unwrap_or_else(|| this.begin_inference_activity().map(Arc::new))
                    .map_err(RuntimeString::from)?;
                let agent_id = (this.deps.uuid)();
                let goal_lease = this.goal_runtime().map(|goal| {
                    Arc::new(goal.register_descendant(agent_id.clone(), parent_agent_id.as_deref()))
                });
                let fork_conversation = fork_snapshot(this.config.fork, &opts, || (get_conversation)());
                let mut preparation = RunSubAgentOpts {
                    inference_capture: Some(inference.clone()),
                    fork_conversation: fork_conversation.clone(),
                    parent_agent_id: parent_agent_id.clone(),
                    initiating_turn_id: spawn_turn_id.clone(),
                    ..Default::default()
                };
                this.prepare_fresh_subagent(&opts, &mut preparation, &abort).await?;
                let (agent_id, creation, pending, accepted) = this
                    .prepare_external_subagent(
                        &opts,
                        depth + 1,
                        parent_agent_id.clone(),
                        spawn_turn_id.clone(),
                        abort.clone(),
                        Some(agent_id),
                        &inference.bundle.config,
                    )
                    .await?;
                let output_file = join_path(
                    std::env::temp_dir()
                        .to_str()
                        .expect("temporary directory is a scalar host path"),
                    &format!("klaus-agent-bg-{agent_id}.txt"),
                );
                // Own the selected inference revision before publishing a detached
                // task, including its setup and persistent idle lifetime.
                // Admission and handle publication share the same lock that
                // shutdown closes before taking its snapshot. A fork can therefore
                // never be spawned after teardown has decided which handles to join.
                {
                    let registry_owner = this.clone();
                    let mut background_registry = registry_owner.background_tasks.lock_recover();
                    if background_registry.closing || this.lifecycle.is_disposed() {
                        return Err("session is shutting down".into());
                    }
                    let persistent = opts.persistent == Some(true);
                    let worker_creation = creation.clone();
                    let worker_id = agent_id.clone();
                    let owner = this.clone();
                    let creation_owner = this.creation_owner(&agent_id, &creation);
                    let task_agent_id = agent_id.clone();
                    let task_output_file = output_file.clone();
                    let handle = tokio::spawn(async move {
                        let _creation_owner = creation_owner;
                        worker_creation.drive(async move {
                        if persistent {
                            let this2 = this.clone();
                            let permission2 = permission.clone();
                            let out2 = task_output_file.clone();
                            let agent_id2 = task_agent_id.clone();
                            let parent_agent_id2 = parent_agent_id.clone();
                            let task_plan = task_plan.clone();
                            (async move {
                                let _parent_rollout_lease = match this2
                                    .acquire_parent_subagent_rollout(parent_agent_id2.as_deref())
                                    .await
                                {
                                    Ok(lease) => lease,
                                    Err(error) => {
                                        this2
                                            .fail_background_setup(
                                                &agent_id2,
                                                &out2,
                                                runtime_background_output_to_file_host(&error),
                                            )
                                            .await;
                                        return;
                                    }
                                };
                                let iso = match this2.setup_owned_agent_isolation(&opts).await {
                                    Ok(i) => i,
                                    Err(e) => {
                                        this2.fail_background_setup(&agent_id2, &out2, e).await;
                                        return;
                                    }
                                };
                                if opts.isolation == Some(Isolation::Worktree)
                                    && opts.cwd.is_none()
                                    && iso.worktree.is_none()
                                {
                                    this2
                            .fail_background_setup(
                                &agent_id2,
                                &out2,
                                "Cannot create worktree: not a git repo and no WorktreeCreate hook."
                                    .to_string(),
                            )
                            .await;
                                    return;
                                }
                                let cwd_override = iso.cwd_override.clone();
                                let worktree_notice =
                                    iso.worktree.as_ref().map(|w| w.notice().clone());
                                this2
                                    .run_persistent_teammate(
                                        opts,
                                        depth + 1,
                                        permission2,
                                        agent_id2,
                                        abort,
                                        out2,
                                        cwd_override,
                                        worktree_notice,
                                        parent_agent_id2,
                                        task_plan,
                                        inference.clone(),
                                        runtime_capabilities,
                                        collaboration_mode,
                                        Some(creation.clone()),
                                        goal_lease,
                                        preparation.prepared,
                                        fork_conversation,
                                    )
                                    .await;
                                iso.finish().await;
                            })
                            .await;
                        } else {
                            let this2 = this.clone();
                            let permission2 = permission.clone();
                            let out2 = task_output_file.clone();
                            let agent_id2 = task_agent_id.clone();
                            let parent_agent_id2 = parent_agent_id.clone();
                            let initiating_turn_id = spawn_turn_id.clone();
                            let task_plan = task_plan.clone();
                            (async move {
                                let _parent_rollout_lease = match this2
                                    .acquire_parent_subagent_rollout(parent_agent_id2.as_deref())
                                    .await
                                {
                                    Ok(lease) => lease,
                                    Err(error) => {
                                        this2
                                            .fail_background_setup(
                                                &agent_id2,
                                                &out2,
                                                runtime_background_output_to_file_host(&error),
                                            )
                                            .await;
                                        return;
                                    }
                                };
                                let mut final_text = RuntimeString::default();
                                let mut failure: Option<RuntimeString> = None;
                                let mut interrupted = false;
                                let mut terminal_seen = false;
                                let mut gen = this2.run_sub_agent_isolated(
                                    opts,
                                    depth + 1,
                                    abort.clone(),
                                    permission2,
                                    agent_id2.clone(),
                                    RunSubAgentOpts {
                                        prepared: preparation.prepared,
                                        creation_admission: Some(creation.clone()),
                                        execution_checked: true,
                                        collaboration_mode: Some(collaboration_mode),
                                        runtime_capabilities: Some(runtime_capabilities),
                                        inference_capture: Some(inference),
                                        goal_lease: Some(goal_lease),
                                        task_plan: task_plan.clone(),
                                        fork_conversation,
                                        in_fork_child: Some(in_fork_child),
                                        lifecycle_kind: Some(SubagentKind::Background),
                                        parent_agent_id: parent_agent_id2.clone(),
                                        initiating_turn_id,
                                        ..Default::default()
                                    },
                                );
                                while let Some(e) = gen.next().await {
                                    if let Some(item) = terminal_item(&e) {
                                        let mut m =
                                            this2.subagents.background_agents.lock_recover();
                                        if let Some(x) = m.get_mut(&agent_id2) {
                                            let np = append_transcript(&x.partial, item, None);
                                            x.partial = np;
                                        }
                                    }
                                    if let EngineEvent::ItemCompleted { item, .. } = &e {
                                        if let ThreadItem::AgentMessage { text, .. } = item {
                                            final_text = text.clone();
                                        }
                                    } else if let EngineEvent::TurnFailed { error, .. } = &e {
                                        terminal_seen = true;
                                        creation.reject(error.message.clone());
                                        failure = Some(error.message.clone());
                                    } else if let EngineEvent::TurnCompleted {
                                        stop_reason, ..
                                    } = &e
                                    {
                                        terminal_seen = true;
                                        interrupted = *stop_reason == TerminalReason::Aborted;
                                        if failure.is_none() {
                                            failure =
                                    crate::tools::subagent_stream::background_terminal_failure(
                                        *stop_reason,
                                    );
                                        }
                                    }
                                }
                                if abort.is_cancelled() && failure.is_none() {
                                    failure = crate::tools::subagent_stream::terminal_failure(
                                        TerminalReason::Aborted,
                                    );
                                }
                                if !terminal_seen && failure.is_none() {
                                    failure = Some(
                                        "Sub-agent stream ended without a terminal event.".into(),
                                    );
                                }
                                let partial = this2
                                    .subagents
                                    .background_agents
                                    .lock_recover()
                                    .get(&agent_id2)
                                    .map(|entry| entry.partial.clone())
                                    .unwrap_or_default();
                                let body = match &failure {
                                    Some(f) => {
                                        crate::tools::subagent_stream::incomplete_background_output(
                                            f, &partial,
                                        )
                                    }
                                    None => {
                                        if final_text.is_empty() {
                                            "(no output)".into()
                                        } else {
                                            final_text.clone()
                                        }
                                    }
                                };
                                write_output_atomic(
                                    &out2,
                                    &runtime_background_output_to_file_host(&body),
                                )
                                .await;
                                if let Some(x) = this2
                                    .subagents
                                    .background_agents
                                    .lock_recover()
                                    .get_mut(&agent_id2)
                                {
                                    if failure.is_some() {
                                        x.partial = body.clone();
                                    }
                                    x.status = if interrupted || abort.is_cancelled() {
                                        BackgroundTaskStatus::Interrupted
                                    } else if failure.is_some() {
                                        BackgroundTaskStatus::Failed
                                    } else {
                                        BackgroundTaskStatus::Completed
                                    };
                                }
                                if !interrupted && !abort.is_cancelled() {
                                    this2.notify_background_complete(&agent_id2).await;
                                }
                            })
                            .await;
                        };
                        }).await;
                        owner
                            .finish_external_subagent(&worker_id, &worker_creation)
                            .await;
                    });
                    background_registry.handles.push(handle);
                }
                pending.wait(accepted).await?;
                Ok(SpawnBackgroundAgentResult {
                    agent_id: agent_id.into(),
                    output_file: RuntimePath::from(output_file),
                })
                    } => result,
                }
            })
        })
    }

    /// Run a persistent swarm teammate: run the prompt, go idle, wait for the next
    /// mailbox message, re-prompt, repeat — until aborted or gracefully shut down.
    #[allow(clippy::too_many_arguments, unused_assignments)]
    pub(crate) async fn run_persistent_teammate(
        self: &Arc<Self>,
        opts: SpawnSubAgentOptions,
        depth: u32,
        permission: PermissionContext,
        agent_id: String,
        signal: AbortSignal,
        output_file: String,
        cwd_override: Option<RuntimePath>,
        worktree_notice: Option<RuntimeString>,
        parent_agent_id: Option<String>,
        task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
        inference: Arc<super::InferenceActivityGuard>,
        runtime_capabilities: SubagentCapabilityCapture,
        collaboration_mode: crate::planning::CollaborationMode,
        creation_admission: Option<Arc<super::OrdinaryCreationAdmission>>,
        initial_goal_lease: Option<Arc<crate::goal::runtime::GoalDescendantLease>>,
        mut initial_prepared: Option<super::PreparedSubagentConfig>,
        mut initial_fork_conversation: Option<Vec<crate::schemas::message::Message>>,
    ) {
        // Retrying the initial input shares one registration. Later mailbox
        // turns capture their own authority after this retained owner is released.
        let mut initial_goal_lease = Some(initial_goal_lease);
        let team = self.swarm_identity.lock_recover().clone().map(|i| i.team);
        let peer = match &opts.swarm {
            Some(s) => s.peer.clone(),
            None => derive_peer_name_from_scalar(&agent_id).into(),
        };
        if let Some(entry) = self
            .subagents
            .background_agents
            .lock_recover()
            .get_mut(&agent_id)
        {
            entry.mailbox_live = true;
        }
        if let Some(team) = &team {
            // Keep the mailbox reachable when initial inference admission is
            // refused, so later input can retry once capacity is available.
            let _ = swarm_register_peer(team, &peer, Some(&self.id), (self.deps.now)()).await;
        }
        let resume_key = opts
            .resume
            .clone()
            .unwrap_or_else(|| agent_id.clone().into());
        let mut last_text = RuntimeString::default();
        let mut failure: Option<RuntimeString> = None;
        let mut interrupted = false;
        let mut first = true;
        let mut prompt = opts.prompt.clone();
        let mut cursor_to_commit: Option<i64> = None;
        let mut delivered_floor: i64 = 0;
        let mut received_messages = Vec::new();
        while !signal.is_cancelled() && !self.teammate_shutdown_requested(&agent_id) {
            self.set_teammate_idle(&agent_id, false);
            failure = None;
            interrupted = false;
            let mut turn_started = false;
            {
                let mut terminal_seen = false;
                let sub_opts = SpawnSubAgentOptions {
                    prompt: prompt.clone(),
                    persistent: Some(false),
                    resume: if first {
                        opts.resume.clone()
                    } else {
                        Some(resume_key.clone())
                    },
                    ..opts.clone()
                };
                let run_opts = RunSubAgentOpts {
                    prepared: if first { initial_prepared.take() } else { None },
                    fork_conversation: if first {
                        initial_fork_conversation.take()
                    } else {
                        None
                    },
                    creation_admission: if first {
                        creation_admission.clone()
                    } else {
                        None
                    },
                    execution_checked: first && creation_admission.is_some(),
                    collaboration_mode: Some(if first {
                        collaboration_mode
                    } else {
                        self.planning.mode()
                    }),
                    runtime_capabilities: Some(runtime_capabilities.clone()),
                    inference_capture: Some(inference.clone()),
                    goal_lease: if first {
                        initial_goal_lease.clone()
                    } else {
                        None
                    },
                    task_plan: task_plan.clone(),
                    received_messages: received_messages.clone(),
                    cwd_override: cwd_override.clone(),
                    worktree_notice: if first { worktree_notice.clone() } else { None },
                    lifecycle_kind: Some(SubagentKind::Persistent),
                    parent_agent_id: parent_agent_id.clone(),
                    initiating_turn_id: self
                        .subagents
                        .background_agents
                        .lock_recover()
                        .get(&agent_id)
                        .and_then(|entry| entry.spawn_turn_id.clone()),
                    ..Default::default()
                };
                let mut gen = self.run_sub_agent(
                    sub_opts,
                    depth,
                    signal.clone(),
                    permission.clone(),
                    agent_id.clone(),
                    run_opts,
                );
                while let Some(e) = gen.next().await {
                    if signal.is_cancelled() {
                        failure = crate::tools::subagent_stream::terminal_failure(
                            TerminalReason::Aborted,
                        );
                        // The retained teammate operation must drain the child
                        // through its terminal append and flush after cancellation.
                    }
                    if matches!(&e, EngineEvent::TurnStarted { .. }) {
                        turn_started = true;
                    }
                    if let Some(item) = terminal_item(&e) {
                        let mut m = self.subagents.background_agents.lock_recover();
                        if let Some(x) = m.get_mut(&agent_id) {
                            let np = append_transcript(&x.partial, item, None);
                            x.partial = np;
                        }
                    }
                    if let EngineEvent::ItemCompleted { item, .. } = &e {
                        if let ThreadItem::AgentMessage { text, .. } = item {
                            last_text = text.clone();
                        }
                    } else if let EngineEvent::TurnFailed { error, .. } = &e {
                        terminal_seen = true;
                        failure = Some(error.message.clone());
                    } else if let EngineEvent::TurnCompleted { stop_reason, .. } = &e {
                        terminal_seen = true;
                        interrupted = *stop_reason == TerminalReason::Aborted;
                        if failure.is_none() {
                            failure = crate::tools::subagent_stream::background_terminal_failure(
                                *stop_reason,
                            );
                        }
                    }
                }
                if !terminal_seen && failure.is_none() {
                    failure = Some("Sub-agent stream ended without a terminal event.".into());
                }
            }
            if first && failure.is_some() {
                // Once creation is accepted, a provider failure belongs to the live peer.
                if let Some(admission) = creation_admission
                    .as_ref()
                    .filter(|admission| !admission.is_committed())
                {
                    admission.reject(failure.clone().expect("failed initial turn"));
                    return;
                }
            }
            let initial_input_accepted = first
                && creation_admission
                    .as_ref()
                    .is_some_and(|admission| admission.is_committed());
            if turn_started || initial_input_accepted {
                first = false;
                initial_goal_lease = None;
                received_messages.clear();
            } else if !received_messages.is_empty() && !signal.is_cancelled() {
                // Retain the typed batch while admission or initial persistence
                // is unavailable. The mailbox cursor has not been committed.
                abortable_sleep(TEAMMATE_IDLE_POLL_MS, &signal).await;
                continue;
            }
            if signal.is_cancelled() {
                break;
            }
            if let Some(reason) = &failure {
                let partial = self
                    .subagents
                    .background_agents
                    .lock_recover()
                    .get(&agent_id)
                    .map(|entry| entry.partial.clone())
                    .unwrap_or_default();
                let body =
                    crate::tools::subagent_stream::incomplete_background_output(reason, &partial);
                write_output_atomic(&output_file, &runtime_background_output_to_file_host(&body))
                    .await;
                if let Some(entry) = self
                    .subagents
                    .background_agents
                    .lock_recover()
                    .get_mut(&agent_id)
                {
                    entry.partial = body;
                    entry.status = BackgroundTaskStatus::Failed;
                }
                if !interrupted {
                    self.notify_background_complete(&agent_id).await;
                }
            }
            if let Some(error) = self.retained_subagent_persistence_error(&agent_id) {
                failure = Some(error);
                break;
            }
            if let (Some(team), Some(cursor)) = (&team, cursor_to_commit) {
                let _ = swarm_write_cursor(team, &peer, cursor).await;
                cursor_to_commit = None;
            }
            let Some(team) = &team else { break };
            let next = self
                .wait_for_teammate_message(team, &peer, &signal, &agent_id, delivered_floor, true)
                .await;
            let Some((next_prompt, next_cursor, messages)) = next else {
                break;
            };
            prompt = next_prompt;
            received_messages = messages;
            cursor_to_commit = Some(next_cursor);
            delivered_floor = next_cursor;
            self.swarm_delivery_state.advance(team, &peer, next_cursor);
        }
        if signal.is_cancelled() {
            failure = crate::tools::subagent_stream::terminal_failure(TerminalReason::Aborted);
        }
        // Terminal: count as idle so any remaining wait-for-idle waiters unblock.
        self.mark_teammate_idle(&agent_id).await;
        if failure.is_none() {
            failure = self.retained_subagent_persistence_error(&agent_id);
        }
        let partial = self
            .subagents
            .background_agents
            .lock_recover()
            .get(&agent_id)
            .map(|entry| entry.partial.clone())
            .unwrap_or_default();
        let body = match &failure {
            Some(f) => crate::tools::subagent_stream::incomplete_background_output(f, &partial),
            None => {
                if last_text.is_empty() {
                    "(no output)".into()
                } else {
                    last_text.clone()
                }
            }
        };
        write_output_atomic(&output_file, &runtime_background_output_to_file_host(&body)).await;
        if let Some(x) = self
            .subagents
            .background_agents
            .lock_recover()
            .get_mut(&agent_id)
        {
            x.mailbox_live = false;
            if failure.is_some() {
                x.partial = body.clone();
            }
            x.status = if interrupted || signal.is_cancelled() {
                BackgroundTaskStatus::Interrupted
            } else if failure.is_some() {
                BackgroundTaskStatus::Failed
            } else {
                BackgroundTaskStatus::Completed
            };
        }
        if !interrupted && !signal.is_cancelled() {
            self.notify_background_complete(&agent_id).await;
        }
    }

    /// Run one workflow agent under {@link setup_agent_isolation}. A no-git tree
    /// silently falls back to the shared cwd.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn run_workflow_agent(
        self: &Arc<Self>,
        opts: SpawnSubAgentOptions,
        depth: u32,
        parent_signal: AbortSignal,
        permission: PermissionContext,
        parent_agent_id: Option<String>,
        task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
        parent_capture: Option<Arc<super::InferenceActivityGuard>>,
        initiating_turn: Option<RuntimeString>,
        runtime_capabilities: SubagentCapabilityCapture,
        collaboration_mode: crate::planning::CollaborationMode,
    ) -> EngineEventStream {
        let inference = parent_capture
            .map(Ok)
            .unwrap_or_else(|| self.begin_inference_activity().map(Arc::new));
        let this = self.clone();
        Box::pin(stream! {
            let inference = match inference {
                Ok(inference) => inference,
                Err(error) => {
                    yield EngineEvent::TurnFailed { inference: None, error: super::internal_err(&error) };
                    return;
                }
            };
            let run = this.spawn_ordinary_foreground(
                opts, depth.saturating_sub(1), parent_signal, permission,
                Arc::new(Vec::new), false, parent_agent_id, task_plan,
                Some(inference), initiating_turn, None, Some(runtime_capabilities),
                collaboration_mode, SubagentKind::Workflow,
            ).await;
            match run {
                Ok(mut run) => while let Some(event) = run.events.next().await { yield event; },
                Err(error) => yield EngineEvent::TurnFailed { inference: None, error: super::internal_runtime_err(error) },
            }
        })
    }

    /// Run a `task`-spawned sub-agent under isolation. A `worktree` request that
    /// cannot be satisfied FAILS the run (unlike the workflow path).
    pub(crate) fn run_sub_agent_isolated(
        self: &Arc<Self>,
        opts: SpawnSubAgentOptions,
        depth: u32,
        parent_signal: AbortSignal,
        permission: PermissionContext,
        agent_id: String,
        mut run_opts: RunSubAgentOpts,
    ) -> EngineEventStream {
        self.capture_subagent_goal_lease(&agent_id, &mut run_opts);
        let runtime_owned = run_opts.creation_admission.is_some()
            || run_opts.message_continuation
            || run_opts.lifecycle_kind == Some(SubagentKind::Persistent);
        let detached = opts.memory_dir.is_none() && !runtime_owned;
        let caller_signal = parent_signal;
        let parent_signal = if detached {
            AbortSignal::new()
        } else {
            caller_signal.clone()
        };
        let worker_signal = parent_signal.clone();
        let identity = super::forwarded_resume(&opts)
            .string_value()
            .unwrap_or_else(|| agent_id.clone());
        let inference = match run_opts.inference_capture.take() {
            Some(inference) => Ok(inference),
            // A standalone spawn (no inherited parent-step capture) must capture
            // its OWN declared model so a concrete-id selection passes through,
            // matching `run_sub_agent_inner`. An inherited parent capture keeps
            // the parent's frozen scope and still rejects an uncaptured child
            // model — that protection lives in the `Some` branch, not here.
            None => self
                .begin_inference_activity_for(opts.model.as_ref())
                .map(Arc::new),
        };
        let this = self.clone();
        let events = Box::pin(stream! {
            run_opts.inference_capture = Some(match inference {
                Ok(inference) => inference,
                Err(error) => {
                    yield EngineEvent::TurnFailed { inference: None,
                        error: EngineError {
                            code: EngineErrorCode::Internal,
                            message: error.into(),
                            cause: None,
                            request_id: None,
                            provider_kind: None,
                            retry_after_ms: None,
                        },
                    };
                    return;
                }
            });
            if let Err(error) = this.prepare_fresh_subagent(&opts, &mut run_opts, &parent_signal).await {
                yield EngineEvent::TurnFailed { inference: None, error: super::internal_runtime_err(error) };
                return;
            }
            let mut iso = match this.setup_owned_agent_isolation(&opts).await {
                Ok(i) => i,
                Err(e) => {
                    yield EngineEvent::TurnFailed { inference: None,
                        error: EngineError {
                            code: EngineErrorCode::Internal,
                            message: format!(
                                "Worktree setup failed: {e}"
                            ).into(),
                            cause: None,
                            request_id: None,
                            provider_kind: None,
                            retry_after_ms: None,
                        },
                    };
                    return;
                }
            };
            if opts.isolation == Some(Isolation::Worktree)
                && opts.cwd.is_none()
                && iso.worktree.is_none()
            {
                yield EngineEvent::TurnFailed { inference: None,
                    error: EngineError {
                        code: EngineErrorCode::Internal,
                        message:
                            "Cannot create worktree: not in a git repository and no WorktreeCreate hook is configured."
                                .into(),
                        cause: None,
                        request_id: None,
                        provider_kind: None,
                        retry_after_ms: None,
                    },
                };
                return;
            }
            let mut ro = run_opts;
            if iso.cwd_override.is_some() {
                ro.cwd_override = iso.cwd_override.clone();
            }
            if let Some(w) = &iso.worktree {
                ro.worktree_notice = Some(w.notice().clone());
            }
            let isolation_agent_id = agent_id.clone();
            let mut gen =
                this.run_sub_agent_execution(opts, depth, parent_signal, permission, agent_id, ro);
            while let Some(ev) = gen.next().await {
                yield ev;
            }
            if !this.retain_ordinary_worktree(&isolation_agent_id, &mut iso.worktree) {
                iso.finish().await;
            }
        });
        if detached {
            self.own_subagent_execution(identity, caller_signal, worker_signal, events)
        } else {
            events
        }
    }
}

pub(crate) fn render_deferred_notice_for_profile(
    text: &RuntimeString,
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    render_provider_text_envelope(
        ProviderTextEnvelopeKind::SystemReminder,
        text.clone(),
        profile,
        Default::default(),
    )
}

pub(crate) fn render_background_completion_notice_for_profile(
    description: &RuntimeString,
    state: &str,
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    let body = RuntimeString::from("Background task \"")
        .concat(description)
        .concat(&RuntimeString::from("\" "))
        .concat(&RuntimeString::from(state))
        .concat(&RuntimeString::from(match state {
            // The task died with the parent session, so there is no final answer
            // to read and no live process to wait on. Telling the model not to
            // re-spawn it here would strand work that genuinely needs redoing.
            "interrupted" => ". It was interrupted and never produced a final answer; task_output returns only incomplete activity. Resume or re-spawn it if the work is still needed.",
            "failed" => ". It failed and did not produce a completed result. Read task_output for the failure and incomplete activity before deciding how to recover.",
            _ => ". Read its result with task_output (or its output file); do not re-spawn it.",
        }));
    render_deferred_notice_for_profile(&body, profile)
}

#[cfg(test)]
mod workflow_capture_tests {
    use super::*;
    use crate::engine::agent::{Agent, AgentOptions, StartSessionOptions};
    use crate::engine::deps::EngineDepsOverrides;
    use crate::schemas::config::default_config;
    use crate::testing::{fake_provider, scripted_call_model};
    use std::sync::Mutex;

    type Requests = Arc<Mutex<Vec<crate::types::provider::ModelRequest>>>;

    fn fixture() -> (Arc<Session>, Requests, tempfile::TempDir) {
        let root = tempfile::tempdir().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let call = scripted_call_model(Vec::new());
        let agent = Agent::new(AgentOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            provider: Arc::new(fake_provider()),
            config: Some(serde_json::json!({
                "model": "fake-model", "prewarm": false, "fork": false,
                "memory": {"enabled": false}, "autoMemory": false,
                "autoDream": false, "memoryRecall": false,
                "speculativeCompaction": false, "earlyMicrocompact": false,
                "idleMicrocompact": false, "compactionSelfCheck": false,
                "loopDetection": false,
            })),
            tools: Some(Vec::new()),
            web_fetch_page: None,
            browser_runtime: None,
            capability_roots: Vec::new(),
            cwd: None,
            deps: EngineDepsOverrides {
                call_model: Some(Box::new(move |request, signal| {
                    recorded.lock().unwrap().push(request.clone());
                    call(request, signal)
                })),
                ..Default::default()
            },
            compactor: None,
            approval_handler: None,
            approval_reviewer: None,
            elicit_handler: None,
            credential_store: None,
            hooks: Vec::new(),
            observer: Some(None),
            logger: None,
            pricing: None,
            agents: None,
            include_default_agents: Some(false),
            skills: None,
            output_styles: None,
            workflows: None,
            mcp_catalog: None,
            plugin_catalog: None,
            lsp_servers: None,
            agent_memory: None,
            mcp_clients: None,
            mcp_server_configs: Vec::new(),
            mcp_tools: None,
            refresh_tools: None,
            system_prompt_provider: None,
            plugins: Vec::new(),
            plugin_load_report: None,
        });
        let path = super::super::runtime_path_from_host_path(root.path());
        let session = agent.start_session(StartSessionOptions {
            cwd: Some(path.clone()),
            user_config_dir: Some(path),
            ..Default::default()
        });
        (session, requests, root)
    }

    fn spawn(
        session: &Arc<Session>,
        capture: Option<Arc<super::super::InferenceActivityGuard>>,
    ) -> EngineEventStream {
        session.make_spawn_workflow_agent(
            0,
            AbortSignal::default(),
            session.permission_context(),
            None,
            None,
            capture,
            None,
            SubagentCapabilityCapture::capture(
                session.runtime_capability_snapshot_for_turn(&super::super::TurnOptions::default()),
            ),
            session.planning.mode(),
        )(SpawnSubAgentOptions {
            prompt: "Complete this captured workflow child".into(),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn workflow_without_parent_capture_freezes_before_the_first_poll() {
        let (session, requests, _root) = fixture();
        let events = spawn(&session, None);
        assert!(requests.lock().unwrap().is_empty());
        let mut next = default_config();
        next.model = "replacement".into();
        session
            .reconfigure_inference(Arc::new(fake_provider()), next, None, None, 0)
            .unwrap();
        let events: Vec<_> = events.collect().await;
        assert!(
            events.iter().any(|event| matches!(
                event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Completed,
                    ..
                }
            )),
            "{events:?}"
        );
        let sent = requests.lock().unwrap().clone();
        assert!(!sent.is_empty());
        assert!(
            sent.iter().all(|request| request.model == "fake-model"),
            "{sent:?}"
        );
        session.shutdown_and_wait().await.unwrap();
    }

    #[tokio::test]
    async fn workflow_without_parent_capture_preserves_capture_failure() {
        let (session, requests, _root) = fixture();
        session
            .begin_model_update("pending-workflow".into(), Some(0))
            .unwrap();
        let events: Vec<_> = spawn(&session, None).collect().await;
        assert!(
            matches!(events.as_slice(), [EngineEvent::TurnFailed { error, .. }] if error.code == EngineErrorCode::Internal && error.message.contains("model update is pending")),
            "{events:?}"
        );
        assert!(requests.lock().unwrap().is_empty());
        session.shutdown_and_wait().await.unwrap();
    }

    #[tokio::test]
    async fn workflow_with_parent_capture_does_not_recapture_during_pending_publication() {
        let (session, requests, _root) = fixture();
        let capture = Arc::new(session.begin_inference_activity().unwrap());
        session
            .begin_model_update("pending-workflow".into(), Some(0))
            .unwrap();
        let events: Vec<_> = spawn(&session, Some(capture)).collect().await;
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            "{events:?}"
        );
        let sent = requests.lock().unwrap().clone();
        assert!(!sent.is_empty());
        assert!(
            sent.iter().all(|request| request.model == "fake-model"),
            "{sent:?}"
        );
        session.shutdown_and_wait().await.unwrap();
    }
}
