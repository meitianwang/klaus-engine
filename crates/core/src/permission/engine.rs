//! The permission-decision seam consumed by the model-call loop.
//!
//! The [`PermissionEngine`] trait is the injected seam and [`PermissionDecision`]
//! is the verdict for a single tool call. [`StaticPermissionEngine`] is the stock
//! implementation combines the canonical filesystem and approval policies with
//! explicit rules, tool self-checks and planning constraints. Permission presets
//! are translated at input boundaries and never participate in tool decisions.

use crate::permission::task_scope::CanonicalTaskResourceScope;
use crate::schemas::config::{TaskResourceMode, TaskResourceScope};
use async_trait::async_trait;
#[cfg(windows)]
use dunce::simplified;
use std::sync::{Arc, OnceLock};

use crate::fs::node_path::resolve_runtime;
#[cfg(windows)]
use crate::fs::node_path::{
    components_runtime, is_absolute_runtime, project_runtime_path, runtime_path_from_native,
};
use crate::fs::path_safety::{get_paths_for_permission_check, is_within_roots};
#[cfg(windows)]
use crate::fs::sensitive_paths::has_suspicious_path_pattern;
use crate::fs::sensitive_paths::{classify_sensitive_path, SensitiveClassification};
use crate::permission::bash::decision::{
    evaluate_bash_command, BashDecision, BashDecisionOptions, BashRuleSets,
};
use crate::permission::bash::match_rule::BashRuleEntry;
use crate::permission::bash::readonly::cwd_guard::is_cwd_bare_git_repo;
use crate::permission::bash::readonly::is_read_only_command;
use crate::permission::durable::DurablePermissionLayer;
use crate::permission::path_rules::{match_path_rule, PathRuleKind};
use crate::permission::permission_update::suggest_rule_for_ask;
use crate::permission::rules::{matches_any_rule, parse_rule_strings};
use crate::permission::PLAN_MODE_READONLY_ENFORCED_KEY;
use crate::runtime_values::{RuntimeJson, RuntimePath, RuntimeString};
use crate::schemas::config::ResolvedConfig;
use crate::types::errors::ToolError;
use crate::types::json::Json;
use crate::types::permission::{
    ApprovalPolicy, ApprovalPromptKind, ApprovalsReviewer, PermissionMode, PermissionReason,
    PermissionResult, PermissionRule, PermissionUpdate, SandboxMode,
};
use crate::types::sandbox::SandboxKind;
use crate::types::tool::{PermissionPathKind, Tool, ToolContext};

/// The verdict for a single tool call: allow (with the possibly-rewritten input),
/// deny (with a reason message), or ask the user (with the input to run on
/// approval plus any "always allow" suggestions).
#[derive(Debug, Clone)]
pub enum PermissionDecision {
    Allow {
        input: Json,
        reason: PermissionReason,
    },
    Deny {
        message: String,
        reason: PermissionReason,
    },
    Ask {
        message: String,
        input: Json,
        reason: PermissionReason,
        suggestions: Option<Vec<PermissionUpdate>>,
    },
    /// A trusted rewrite made the fixed permission boundary throw. Keep this
    /// separate from policy denial: it terminates the turn as an internal
    /// `ToolError`, without invoking Rust's panic hook.
    Fatal { error: ToolError },
}

/// Evaluate a tool call against the live permission state (rules, policies, safety
/// gates). Injected into the loop so the host owns the policy.
#[async_trait]
pub trait PermissionEngine: Send + Sync {
    fn rule_snapshot(&self) -> crate::permission::permission_update::SessionPermissionState {
        Default::default()
    }
    fn bound_task_resource_scope(&self) -> Option<Arc<CanonicalTaskResourceScope>> {
        None
    }
    /// Recheck the final input after hooks and owner approval rewrites.
    async fn check_task_resource_scope(
        &self,
        _tool: &dyn Tool,
        _input: &Json,
        _ctx: &ToolContext,
    ) -> Result<(), String> {
        Ok(())
    }
    async fn evaluate(
        &self,
        tool: &dyn Tool,
        input: &Json,
        ctx: &ToolContext,
    ) -> PermissionDecision;
    /// Evaluate while excluding saved argument-prefix allow grants. Engines
    /// without this capability must fail closed rather than reuse an approval.
    async fn evaluate_without_allow_prefix_rules(
        &self,
        tool: &dyn Tool,
        _input: &Json,
        _ctx: &ToolContext,
    ) -> PermissionDecision {
        PermissionDecision::Deny {
            message: format!(
                "The permission engine cannot evaluate {} without saved allow prefix rules.",
                permission_prompt_text(&tool.name()),
            ),
            reason: PermissionReason::SafetyCheck,
        }
    }
}

fn runtime_ascii_name_eq(name: &RuntimeString, expected: &str) -> bool {
    name.as_utf16()
        .iter()
        .copied()
        .map(ascii_lower_name_unit)
        .eq(expected.encode_utf16().map(ascii_lower_name_unit))
}

fn ascii_lower_name_unit(unit: u16) -> u16 {
    if (u16::from(b'A')..=u16::from(b'Z')).contains(&unit) {
        unit + u16::from(b'a' - b'A')
    } else {
        unit
    }
}

#[cfg(windows)]
fn simplified_runtime_path(path: &RuntimePath) -> Option<RuntimePath> {
    let native = project_runtime_path(path, "canonical permission path").ok()?;
    Some(runtime_path_from_native(simplified(&native)))
}

#[cfg(windows)]
fn is_clean_relative_sensitive_target(path: &RuntimePath) -> bool {
    !path.as_runtime_string().is_empty()
        && !is_absolute_runtime(path)
        && !has_suspicious_path_pattern(path)
        && !components_runtime(path)
            .iter()
            .any(|component| component == "..")
}

/// The resolved file targets of a path-scoped tool call: `allow` matches the
/// original forms, `deny_ask` the symlink-resolved forms (a link can't dodge a
/// deny/ask).
struct FilePaths {
    deny_ask: Vec<RuntimePath>,
    allow: Vec<RuntimePath>,
}

#[cfg(windows)]
async fn trusted_cwd_sensitive_target_is_safe(
    raw: &Json,
    target_forms: &[RuntimePath],
    ctx: &ToolContext,
) -> bool {
    if !has_suspicious_path_pattern(&ctx.cwd) {
        return false;
    }
    let Some(raw) = raw.as_string() else {
        return false;
    };
    let raw = RuntimePath::from(raw.clone());
    if !is_clean_relative_sensitive_target(&raw) {
        return false;
    }

    let cwd_forms = get_paths_for_permission_check(&ctx.cwd).await;
    let (Some(canonical_cwd), Some(canonical_target)) = (cwd_forms.get(1), target_forms.get(1))
    else {
        return false;
    };
    if !is_within_roots(canonical_target, std::slice::from_ref(canonical_cwd)) {
        return false;
    }
    if let Some(pinned_root) = &ctx.permission.pinned_memory_root {
        let pinned_canonical_root = runtime_path_from_native(pinned_root.canonical_root());
        if canonical_cwd != &pinned_canonical_root {
            return false;
        }
    }

    let Some(classification_cwd) = simplified_runtime_path(canonical_cwd) else {
        return false;
    };
    if has_suspicious_path_pattern(&classification_cwd) {
        return false;
    }
    let Ok(lexical_target) = resolve_runtime(&classification_cwd, std::slice::from_ref(&raw))
    else {
        return false;
    };
    let Some(canonical_target) = simplified_runtime_path(canonical_target) else {
        return false;
    };
    [lexical_target, canonical_target]
        .iter()
        .all(|form| classify_sensitive_path(form, &classification_cwd).is_safe())
}

pub(crate) async fn goal_owned_read_allowed(
    tool: &dyn Tool,
    input: &Json,
    ctx: &ToolContext,
) -> bool {
    if tool.name() != "read"
        || !tool.is_read_only(input)
        || tool.permission_path_kind() != Some(PermissionPathKind::Read)
    {
        return false;
    }
    let Some(resources) = &ctx.goal_owned_resources else {
        return false;
    };
    let Some(path) = input.get("path").and_then(RuntimeJson::as_string) else {
        return false;
    };
    let Ok(path) =
        crate::fs::node_path::resolve_runtime(&ctx.cwd, &[RuntimePath::from(path.clone())])
    else {
        return false;
    };
    resources.allows_read(&path).await
}
/// Static permission evaluation: filesystem policy + deny/allow/ask lists + the tool's
/// own self-check. The interactive "ask the user" step (with timeout downgrade) is
/// performed by the executor, which owns event emission.
pub struct StaticPermissionEngine {
    config_allow: Vec<PermissionRule>,
    config_deny: Vec<PermissionRule>,
    config_ask: Vec<PermissionRule>,
    durable: Arc<DurablePermissionLayer>,
    use_stored_permission_rules: bool,
    ignore_context_grants: bool,
    task_resource_scope: Option<TaskResourceScope>,
    bound_task_scope: OnceLock<Result<Arc<CanonicalTaskResourceScope>, String>>,
}

impl StaticPermissionEngine {
    pub fn task_scope(
        &self,
        cwd: &RuntimePath,
    ) -> Result<Option<Arc<CanonicalTaskResourceScope>>, String> {
        let Some(definition) = &self.task_resource_scope else {
            return Ok(None);
        };
        self.bound_task_scope
            .get_or_init(|| CanonicalTaskResourceScope::bind(definition, cwd))
            .clone()
            .map(Some)
    }

    pub fn new(config: &ResolvedConfig) -> StaticPermissionEngine {
        Self::with_durable_rules(config, Arc::new(DurablePermissionLayer::default()))
    }

    pub(crate) fn without_context_grants(mut self) -> Self {
        self.ignore_context_grants = true;
        self
    }

    pub fn with_durable_rules(
        config: &ResolvedConfig,
        durable: Arc<DurablePermissionLayer>,
    ) -> StaticPermissionEngine {
        StaticPermissionEngine {
            config_allow: parse_rule_strings(&config.allow),
            config_deny: parse_rule_strings(&config.deny),
            config_ask: parse_rule_strings(&config.ask),
            durable,
            use_stored_permission_rules: config.use_stored_permission_rules,
            ignore_context_grants: false,
            task_resource_scope: config.task_resource_scope.clone(),
            bound_task_scope: OnceLock::new(),
        }
    }
}

#[async_trait]
impl PermissionEngine for StaticPermissionEngine {
    fn rule_snapshot(&self) -> crate::permission::permission_update::SessionPermissionState {
        self.durable.snapshot()
    }
    fn bound_task_resource_scope(&self) -> Option<Arc<CanonicalTaskResourceScope>> {
        self.bound_task_scope
            .get()
            .and_then(|result| result.as_ref().ok())
            .cloned()
    }
    async fn check_task_resource_scope(
        &self,
        tool: &dyn Tool,
        input: &Json,
        ctx: &ToolContext,
    ) -> Result<(), String> {
        if let Some(scope) = self.task_scope(&ctx.cwd)? {
            scope.check_tool(tool, input, ctx)?;
        }
        Ok(())
    }
    async fn evaluate(
        &self,
        tool: &dyn Tool,
        input: &Json,
        ctx: &ToolContext,
    ) -> PermissionDecision {
        self.evaluate_with_prefix_policy(tool, input, ctx, false)
            .await
    }

    async fn evaluate_without_allow_prefix_rules(
        &self,
        tool: &dyn Tool,
        input: &Json,
        ctx: &ToolContext,
    ) -> PermissionDecision {
        self.evaluate_with_prefix_policy(tool, input, ctx, true)
            .await
    }
}

/// Inside a task resource scope the granted roots are the ceiling while the
/// session's live approval policy owns the ask level: a bypass preset approves
/// every in-scope action, an automatic-review preset approves edits inside the
/// write roots, anything else asks. The bound scope mode only seeds the
/// session's initial preset; it never overrides a later owner selection.
fn scoped_ask_ladder(ctx: &ToolContext) -> TaskResourceMode {
    match ctx.approval_policy {
        Some(ApprovalPolicy::Never) => TaskResourceMode::ScopedAuto,
        _ => match ctx.approvals_reviewer {
            Some(ApprovalsReviewer::Automatic) => TaskResourceMode::AutoEdit,
            _ => TaskResourceMode::Manual,
        },
    }
}

impl StaticPermissionEngine {
    async fn evaluate_with_prefix_policy(
        &self,
        tool: &dyn Tool,
        input: &Json,
        ctx: &ToolContext,
        ignore_allow_prefix: bool,
    ) -> PermissionDecision {
        self.evaluate_with_approval_policy(
            tool,
            input,
            ctx,
            ignore_allow_prefix,
            ctx.approval_policy,
        )
        .await
    }

    /// Intersect an inherited file-review requirement while preserving the exact
    /// execution context, including its approval flow and host capabilities.
    pub(crate) async fn evaluate_inherited_untrusted_review(
        &self,
        tool: &dyn Tool,
        input: &Json,
        ctx: &ToolContext,
        ignore_allow_prefix: bool,
    ) -> PermissionDecision {
        self.evaluate_with_approval_policy(
            tool,
            input,
            ctx,
            ignore_allow_prefix,
            Some(ApprovalPolicy::UnlessTrusted),
        )
        .await
    }

    async fn evaluate_with_approval_policy(
        &self,
        tool: &dyn Tool,
        input: &Json,
        ctx: &ToolContext,
        ignore_allow_prefix: bool,
        approval_policy: Option<ApprovalPolicy>,
    ) -> PermissionDecision {
        let command = match command_string_of(tool, input) {
            Ok(command) => command,
            Err(error) => return PermissionDecision::Fatal { error },
        };
        let read_only = tool.is_read_only(input);
        let tool_name = tool.name();
        let scope = match self.task_scope(&ctx.cwd) {
            Ok(scope) => scope,
            Err(message) => {
                return PermissionDecision::Deny {
                    message,
                    reason: PermissionReason::SafetyCheck,
                }
            }
        };
        let goal_read = goal_owned_read_allowed(tool, input, ctx).await;
        if let Some(scope) = &scope {
            if let Err(message) = scope.check_tool(tool, input, ctx) {
                return PermissionDecision::Deny {
                    message,
                    reason: PermissionReason::SafetyCheck,
                };
            }
        }
        let durable = self.durable.snapshot();

        let deny_rules: Vec<PermissionRule> = self
            .config_deny
            .iter()
            .cloned()
            .chain(durable.deny_rules)
            .chain(ctx.permission.deny_rules.iter().cloned())
            .collect();
        let allow_rules: Vec<PermissionRule> = self
            .config_allow
            .iter()
            .filter(|_| scope.is_none())
            .cloned()
            .chain(
                durable
                    .allow_rules
                    .into_iter()
                    .filter(|_| self.use_stored_permission_rules && scope.is_none()),
            )
            .chain(
                ctx.permission
                    .allow_rules
                    .iter()
                    .filter(|_| scope.is_none() && !self.ignore_context_grants)
                    .cloned(),
            )
            // This filter applies to all three grant sources. Deny/ask rules
            // and the original stored/context state are left intact.
            .filter(|rule| !ignore_allow_prefix || rule.argument_prefix.is_none())
            .collect();
        // Config ask rules and the session's own ask rules are one list: a durable
        // `ask` rule the user configured must gate exactly like one granted mid
        // session, and a config list nothing reads is a rule the user set that
        // silently does nothing.
        let ask_rules: Vec<PermissionRule> = self
            .config_ask
            .iter()
            .cloned()
            .chain(
                durable
                    .ask_rules
                    .into_iter()
                    .filter(|_| self.use_stored_permission_rules && scope.is_none()),
            )
            .chain(ctx.permission.ask_rules.iter().cloned())
            .collect();

        // File tools: resolve the target path(s) for path-scoped Read/Edit/Write
        // rules. deny/ask match the symlink-resolved forms too; allow matches the
        // original path only. A pathless call falls back to cwd.
        let path_kind = tool.permission_path_kind();
        let mut file_paths: Option<FilePaths> = None;
        if path_kind.is_some() {
            // `Array.prototype.map` in the fixed engine resolves every raw path
            // synchronously before its first async canonicalization. Preserve
            // that ordering (and Node's path.resolve type failure) exactly.
            let list = raw_path_values(tool, input, Some(&ctx.cwd));
            let allow: Vec<RuntimePath> = match list
                .iter()
                .map(|path| resolve_raw_path(&ctx.cwd, path))
                .collect()
            {
                Ok(paths) => paths,
                Err(error) => return PermissionDecision::Fatal { error },
            };
            let mut deny_ask: Vec<RuntimePath> = Vec::new();
            for p in &allow {
                deny_ask.extend(get_paths_for_permission_check(p).await);
            }
            file_paths = Some(FilePaths { deny_ask, allow });
        }

        // Bash evaluates each subcommand; a matching deny outranks a whole-command grant.
        if runtime_ascii_name_eq(&tool_name, "bash")
            || runtime_ascii_name_eq(&tool_name, "exec_command")
        {
            if let Some(cmd) = &command {
                let decision =
                    self.evaluate_bash(cmd, input, &deny_rules, &allow_rules, &ask_rules, ctx);
                if let Some(_scope) = &scope {
                    if matches!(
                        decision,
                        PermissionDecision::Deny { .. } | PermissionDecision::Fatal { .. }
                    ) {
                        return decision;
                    }
                    if tool.task_scope_host_capability()
                        && tool.is_read_only(input)
                        && ctx.sandbox_mode == Some(SandboxMode::ReadOnly)
                        && ctx.sandbox.kind() != SandboxKind::Process
                    {
                        return decision;
                    }
                    return if scoped_ask_ladder(ctx) == TaskResourceMode::ScopedAuto {
                        PermissionDecision::Allow {
                            input: input.clone(),
                            reason: PermissionReason::Default,
                        }
                    } else {
                        PermissionDecision::Ask {
                            message: "Allow this command within the task's authorized directories?"
                                .into(),
                            input: input.clone(),
                            reason: PermissionReason::SafetyCheck,
                            suggestions: None,
                        }
                    };
                }
                return decision;
            }
        }

        // 1) Explicit deny is a hard block under every runtime policy.
        if let Some(denied) = match_rule_set(
            &deny_rules,
            file_paths.as_ref(),
            path_kind,
            &tool_name,
            command.as_deref(),
            &ctx.cwd,
            false,
        ) {
            return PermissionDecision::Deny {
                message: format!("Denied by rule: {}", format_rule(&denied)),
                reason: PermissionReason::Rule { rule: denied },
            };
        }

        // 2) Explicit ask rules precede automatic grants.
        if let Some(asked) = match_rule_set(
            &ask_rules,
            file_paths.as_ref(),
            path_kind,
            &tool_name,
            command.as_deref(),
            &ctx.cwd,
            false,
        ) {
            return PermissionDecision::Ask {
                message: ask_message(&tool_name, command.as_deref()),
                input: input.clone(),
                suggestions: suggestions_for(&tool_name, command.as_deref()),
                reason: PermissionReason::Rule { rule: asked },
            };
        }

        // 3) A tool self-check denial is a hard block before automatic grants.
        // Other self-check results are retained for the final decision.
        let mut ask_message_self: Option<String> = None;
        let mut ask_input: Json = input.clone();
        let mut ask_suggestions: Option<Vec<PermissionUpdate>> = None;
        let mut self_check_allow: Option<Json> = None;
        if let Some(result) = tool.check_permissions(input, ctx).await {
            match result {
                PermissionResult::Deny { message, .. } => {
                    return PermissionDecision::Deny {
                        message: permission_prompt_text(&message),
                        reason: PermissionReason::ToolSelfCheck,
                    };
                }
                PermissionResult::Allow { updated_input, .. } => {
                    self_check_allow = Some(updated_input.unwrap_or_else(|| input.clone()));
                }
                PermissionResult::Ask {
                    message,
                    updated_input,
                    suggestions,
                    ..
                } => {
                    ask_message_self = Some(permission_prompt_text(&message));
                    ask_input = updated_input.unwrap_or_else(|| input.clone());
                    ask_suggestions = suggestions;
                }
                PermissionResult::Passthrough { .. } => {}
            }
        }

        if let Some(_scope) = &scope {
            if path_kind == Some(PermissionPathKind::Edit)
                && ctx.sandbox_mode == Some(SandboxMode::ReadOnly)
            {
                return PermissionDecision::Ask {
                    message:
                        "This write exceeds the read-only filesystem policy. Allow this operation?"
                            .into(),
                    input: input.clone(),
                    reason: PermissionReason::SafetyCheck,
                    suggestions: None,
                };
            }
            if path_kind.is_some() {
                return if path_kind == Some(PermissionPathKind::Read)
                    || scoped_ask_ladder(ctx) != TaskResourceMode::Manual
                {
                    PermissionDecision::Allow {
                        input: input.clone(),
                        reason: PermissionReason::Default,
                    }
                } else {
                    PermissionDecision::Ask {
                        message: "Allow editing within the task's authorized directories?".into(),
                        input: input.clone(),
                        reason: PermissionReason::SafetyCheck,
                        suggestions: None,
                    }
                };
            }
            if scoped_ask_ladder(ctx) == TaskResourceMode::ScopedAuto {
                return PermissionDecision::Allow {
                    input: input.clone(),
                    reason: PermissionReason::Default,
                };
            }
            if !read_only && tool.name() != "send_message_to_friend" {
                return PermissionDecision::Ask {
                    message: "Allow this task operation?".into(),
                    input: input.clone(),
                    reason: PermissionReason::SafetyCheck,
                    suggestions: None,
                };
            }
            if tool.name() == "send_message_to_friend" {
                return PermissionDecision::Allow {
                    input: input.clone(),
                    reason: PermissionReason::Default,
                };
            }
        }

        // The session's own plan document remains writable during planning and
        // execution. Explicit deny and ask rules above still apply; the plan's
        // ownership check precedes the general sensitive-path restriction.
        if path_kind == Some(PermissionPathKind::Edit) {
            if let (Some(plan), Some(fp)) = (ctx.plan_file.as_ref(), file_paths.as_ref()) {
                let plans_dir = permission_prompt_path(&plan.plans_dir);
                let name = permission_prompt_text(&plan.name);
                let all_plan_targets = !fp.allow.is_empty()
                    && fp.allow.iter().all(|p| {
                        crate::fs::plan_files::is_session_plan_file(
                            &permission_prompt_path(p),
                            &plans_dir,
                            &name,
                        )
                    });
                if all_plan_targets {
                    return PermissionDecision::Allow {
                        input: input.clone(),
                        reason: PermissionReason::Default,
                    };
                }
            }
        }

        // Untrusted-action review is independent of filesystem reach.
        if !ctx.planning_read_only
            && path_kind == Some(PermissionPathKind::Edit)
            && approval_policy == Some(ApprovalPolicy::UnlessTrusted)
        {
            return PermissionDecision::Ask {
                message: ask_message(&tool_name, command.as_deref()),
                input: input.clone(),
                suggestions: None,
                reason: PermissionReason::Default,
            };
        }

        // 4) Sensitive execution and configuration paths require review unless
        // the canonical filesystem policy already authorizes unrestricted writes.
        if let Some(sensitive) = self
            .check_sensitive_edit(tool, input, ctx, approval_policy)
            .await
        {
            return sensitive;
        }

        // Filesystem policy is a separate ceiling. Explicit allow rules cannot
        // silently grant a write that the captured sandbox would reject.
        if let Some(sandbox_mode) = ctx.sandbox_mode.filter(|_| !ctx.planning_read_only) {
            if !read_only && path_kind == Some(PermissionPathKind::Edit) {
                let permitted = match sandbox_mode {
                    SandboxMode::DangerFullAccess => true,
                    SandboxMode::ReadOnly => false,
                    SandboxMode::WorkspaceWrite => {
                        let mut roots = Vec::new();
                        for root in std::iter::once(&ctx.cwd)
                            .chain(ctx.permission.additional_writable_roots.iter())
                        {
                            roots.extend(get_paths_for_permission_check(root).await);
                        }
                        file_paths.as_ref().is_some_and(|paths| {
                            !paths.deny_ask.is_empty()
                                && paths
                                    .deny_ask
                                    .iter()
                                    .all(|path| is_within_roots(path, &roots))
                        })
                    }
                };
                return if permitted {
                    PermissionDecision::Allow {
                        input: input.clone(),
                        reason: PermissionReason::Default,
                    }
                } else if !approval_policy
                    .unwrap_or_default()
                    .allows_prompt(ApprovalPromptKind::Sandbox)
                {
                    PermissionDecision::Deny {
                        message: "Approval policy disallowed a sandbox approval prompt.".to_owned(),
                        reason: PermissionReason::Default,
                    }
                } else {
                    PermissionDecision::Ask { message: "This write exceeds the current filesystem policy. Allow this operation?".into(), input: input.clone(), reason: PermissionReason::Default, suggestions: None }
                };
            }
            if sandbox_mode == SandboxMode::DangerFullAccess && ask_message_self.is_none() {
                return PermissionDecision::Allow {
                    input: input.clone(),
                    reason: PermissionReason::Default,
                };
            }
        }

        // 5) Explicit allow list.
        if let Some(allowed) = match_rule_set(
            &allow_rules,
            file_paths.as_ref(),
            path_kind,
            &tool_name,
            command.as_deref(),
            &ctx.cwd,
            true,
        ) {
            return PermissionDecision::Allow {
                input: input.clone(),
                reason: PermissionReason::Rule { rule: allowed },
            };
        }

        // 6) Tool self-check ALLOW (deny/ask rules already cleared it).
        if let Some(updated) = self_check_allow {
            return PermissionDecision::Allow {
                input: updated,
                reason: PermissionReason::ToolSelfCheck,
            };
        }

        // Internal goal controls mutate only the private goal ledger. Their
        // eligibility and intent checks remain in the actual handler; ordinary
        // workspace-write baselines do not apply to that metadata capability.
        // Explicit deny/ask, hard task scope and self-check rejection ran first.
        if tool.is_goal_state_control() {
            if let Some(message) = ask_message_self {
                return PermissionDecision::Ask {
                    message,
                    input: ask_input,
                    suggestions: ask_suggestions,
                    reason: PermissionReason::ToolSelfCheck,
                };
            }
            return PermissionDecision::Allow {
                input: input.clone(),
                reason: PermissionReason::Default,
            };
        }

        if goal_read {
            return PermissionDecision::Allow {
                input: input.clone(),
                reason: PermissionReason::ToolSelfCheck,
            };
        }

        // 7) Independent planning constraints and read-only actions.
        if ctx.planning_read_only {
            if read_only {
                return PermissionDecision::Allow {
                    input: input.clone(),
                    reason: PermissionReason::Mode {
                        mode: PermissionMode::Plan,
                    },
                };
            }
            // The owned plan document was handled before this general planning
            // restriction; other edit targets remain read-only.
            // A mutating tool that EXPLICITLY asks via its self-check (exit_plan_mode)
            // is honored here rather than denied.
            if let Some(msg) = ask_message_self {
                return PermissionDecision::Ask {
                    message: msg,
                    input: ask_input,
                    suggestions: ask_suggestions,
                    reason: PermissionReason::ToolSelfCheck,
                };
            }
            return PermissionDecision::Deny {
                message: "Plan mode is read-only; no mutating actions are permitted.".to_string(),
                reason: PermissionReason::Mode {
                    mode: PermissionMode::Plan,
                },
            };
        }
        if read_only {
            return PermissionDecision::Allow {
                input: input.clone(),
                reason: PermissionReason::Default,
            };
        }
        // 8) Otherwise ask. Auto-generate the "always allow" suggestion when the
        // tool's self-check didn't supply one.
        let (message, suggestions) = match ask_message_self {
            Some(m) => (m, ask_suggestions),
            None => (
                ask_message(&tool_name, command.as_deref()),
                suggestions_for(&tool_name, command.as_deref()),
            ),
        };
        PermissionDecision::Ask {
            message,
            input: ask_input,
            suggestions,
            reason: PermissionReason::Default,
        }
    }
}

impl StaticPermissionEngine {
    /// Sensitive-file gate: for an edit-class tool, if any target path (symlink
    /// forms included) is a code-exec / config-hijack path, force an ask. Returns
    /// the ask decision, or `None` when nothing is sensitive. Carries NO "always
    /// allow" suggestion. Explicit rules and tool self-checks have already run.
    async fn check_sensitive_edit(
        &self,
        tool: &dyn Tool,
        input: &Json,
        ctx: &ToolContext,
        approval_policy: Option<ApprovalPolicy>,
    ) -> Option<PermissionDecision> {
        // Canonical full-disk access does not need path-based approval.
        // Planning and untrusted-action review remain separate ceilings.
        if tool.permission_path_kind() != Some(PermissionPathKind::Edit)
            || (ctx.sandbox_mode == Some(SandboxMode::DangerFullAccess)
                && !ctx.planning_read_only
                && approval_policy != Some(ApprovalPolicy::UnlessTrusted))
        {
            return None;
        }
        let tool_name = tool.name();
        let targets = raw_path_values(tool, input, None);
        let resolved: Vec<RuntimePath> = match targets
            .iter()
            .map(|path| resolve_raw_path(&ctx.cwd, path))
            .collect()
        {
            Ok(paths) => paths,
            Err(error) => return Some(PermissionDecision::Fatal { error }),
        };
        if !resolved.is_empty() && !ctx.permission.internal_writable_roots.is_empty() {
            let mut canonical_internal_roots = Vec::new();
            let mut roots_are_canonical = true;
            for root in &ctx.permission.internal_writable_roots {
                let forms = get_paths_for_permission_check(root).await;
                if forms.len() != 1 || forms.first() != Some(root) {
                    roots_are_canonical = false;
                    break;
                }
                canonical_internal_roots.push(root.clone());
            }
            let mut every_target_is_internal =
                roots_are_canonical && !canonical_internal_roots.is_empty();
            for target in &resolved {
                let forms = get_paths_for_permission_check(target).await;
                let Some(canonical_target) = forms.last() else {
                    every_target_is_internal = false;
                    break;
                };
                if !is_within_roots(canonical_target, &canonical_internal_roots) {
                    every_target_is_internal = false;
                    break;
                }
            }
            if every_target_is_internal {
                return None;
            }
        }
        #[cfg(windows)]
        let mut raw_targets = targets.iter();
        for target in resolved {
            let forms = get_paths_for_permission_check(&target).await;
            #[cfg(windows)]
            {
                let raw = raw_targets
                    .next()
                    .expect("resolved sensitive target preserves raw target count");
                if trusted_cwd_sensitive_target_is_safe(raw, &forms, ctx).await {
                    continue;
                }
            }
            for form in forms {
                if let SensitiveClassification::Unsafe { reason, .. } =
                    classify_sensitive_path(&form, &ctx.cwd)
                {
                    return Some(PermissionDecision::Ask {
                        message: format!(
                            "Allow {} to a sensitive path ({reason})?",
                            permission_prompt_text(&tool_name)
                        ),
                        input: input.clone(),
                        suggestions: None,
                        reason: PermissionReason::SafetyCheck,
                    });
                }
            }
        }
        None
    }

    /// Bash-specific decision: split the command into subcommands and evaluate each.
    /// Explicit subcommand rules are evaluated first; ask/passthrough decisions
    /// fall to the captured filesystem and approval policies.
    #[allow(clippy::too_many_arguments)]
    fn evaluate_bash(
        &self,
        command: &str,
        input: &Json,
        deny_rules: &[PermissionRule],
        allow_rules: &[PermissionRule],
        ask_rules: &[PermissionRule],
        ctx: &ToolContext,
    ) -> PermissionDecision {
        let result = evaluate_bash_command(
            command,
            &BashRuleSets {
                deny: to_bash_entries(deny_rules),
                allow: to_bash_entries(allow_rules),
                ask: to_bash_entries(ask_rules),
            },
            &BashDecisionOptions {
                cwd: permission_prompt_path(&ctx.cwd),
                writable_roots: ctx
                    .permission
                    .additional_writable_roots
                    .iter()
                    .map(permission_prompt_path)
                    .collect(),
                home_dir: std::env::var("HOME").ok(),
                is_read_only_subcommand: (ctx.approval_policy
                    != Some(ApprovalPolicy::UnlessTrusted))
                .then(|| {
                    Box::new(|s: &str| is_read_only_command(s))
                        as Box<dyn Fn(&str) -> bool + Send + Sync>
                }),
                is_cwd_bare_repo: Some({
                    let cwd = permission_prompt_path(&ctx.cwd);
                    Box::new(move || is_cwd_bare_git_repo(&cwd))
                }),
            },
        );

        // 1) Explicit subcommand deny is a hard block.
        if let BashDecision::Deny { rule } = &result {
            return PermissionDecision::Deny {
                message: match rule {
                    Some(r) => format!("Denied by rule: {}", r.source),
                    None => "Denied: bash subcommand".to_string(),
                },
                reason: match rule {
                    Some(r) => PermissionReason::Rule {
                        rule: bash_rule_to_perm(r),
                    },
                    None => PermissionReason::Default,
                },
            };
        }
        if let BashDecision::Ask {
            from_rule,
            dangerous_removal,
        } = &result
        {
            let category = if *from_rule {
                ApprovalPromptKind::Rules
            } else {
                ApprovalPromptKind::Sandbox
            };
            if (*from_rule || *dangerous_removal)
                && !ctx
                    .approval_policy
                    .unwrap_or_default()
                    .allows_prompt(category)
            {
                return PermissionDecision::Deny {
                    message: if *from_rule {
                        "Approval policy disallowed a command-rule approval prompt."
                    } else {
                        "Approval policy disallowed a sandbox approval prompt."
                    }
                    .to_owned(),
                    reason: PermissionReason::SafetyCheck,
                };
            }
        }
        if ctx.approval_policy == Some(ApprovalPolicy::UnlessTrusted)
            && !matches!(result, BashDecision::Allow { .. })
        {
            return PermissionDecision::Ask {
                message: format!("Allow shell command: {command}?"),
                input: input.clone(),
                reason: PermissionReason::Default,
                suggestions: suggestions_for(&RuntimeString::from("bash"), Some(command)),
            };
        }
        // 2.5) Catastrophic removal forces ASK and is a safety gate (allow-rule-immune
        // and hook-approve-immune via the safetyCheck reason).
        if let BashDecision::Ask {
            dangerous_removal: true,
            ..
        } = &result
        {
            return PermissionDecision::Ask {
                message: format!("Allow bash (deletes a critical path): {command}?"),
                input: input.clone(),
                suggestions: None,
                reason: PermissionReason::SafetyCheck,
            };
        }
        // A real sandbox enforces ordinary commands without a per-command
        // prompt. Content-specific ask rules and security checks still win.
        if let Some(sandbox_mode) = ctx.sandbox_mode.filter(|_| !ctx.planning_read_only) {
            if let BashDecision::Ask {
                from_rule: true, ..
            } = &result
            {
                return PermissionDecision::Ask {
                    message: format!("Allow shell command: {command}?"),
                    input: input.clone(),
                    suggestions: suggestions_for(&RuntimeString::from("bash"), Some(command)),
                    reason: PermissionReason::Default,
                };
            }
            if sandbox_mode != SandboxMode::DangerFullAccess
                && ctx.sandbox.kind() == SandboxKind::Process
            {
                return PermissionDecision::Deny {
                    message:
                        "The requested filesystem policy requires an available isolation backend."
                            .into(),
                    reason: PermissionReason::SafetyCheck,
                };
            }
            return PermissionDecision::Allow {
                input: input.clone(),
                reason: PermissionReason::Default,
            };
        }
        // 3) All-subcommands allow.
        if let BashDecision::Allow { rule } = &result {
            return PermissionDecision::Allow {
                input: input.clone(),
                reason: match rule {
                    Some(r) => PermissionReason::Rule {
                        rule: bash_rule_to_perm(r),
                    },
                    None => PermissionReason::Default,
                },
            };
        }
        // Planning research uses a read-only execution boundary.
        if ctx.planning_read_only {
            // A content-specific ASK rule is the user's own decision point and
            // stays one: plan mode's auto-allow below must not swallow it.
            if let BashDecision::Ask {
                from_rule: true, ..
            } = &result
            {
                return PermissionDecision::Ask {
                    message: format!("Allow bash: {command}?"),
                    input: input.clone(),
                    suggestions: suggestions_for(&RuntimeString::from("bash"), Some(command)),
                    reason: PermissionReason::Mode {
                        mode: PermissionMode::Plan,
                    },
                };
            }
            // Without a real isolation backend there is nothing to enforce the
            // read-only promise, so the undecidable command keeps going to a
            // human (and fails closed with no approval handler).
            if ctx.sandbox.kind() == SandboxKind::Process {
                if !ctx
                    .approval_policy
                    .unwrap_or_default()
                    .allows_prompt(ApprovalPromptKind::Sandbox)
                {
                    return PermissionDecision::Deny {
                        message: "Approval policy disallowed a sandbox approval prompt.".to_owned(),
                        reason: PermissionReason::Mode {
                            mode: PermissionMode::Plan,
                        },
                    };
                }
                return PermissionDecision::Ask {
                    message: format!(
                        "Plan mode is read-only, but this shell command could not be verified as read-only. Allow it for this planning step?\n\n{command}"
                    ),
                    input: input.clone(),
                    suggestions: None,
                    reason: PermissionReason::Mode { mode: PermissionMode::Plan },
                };
            }
            // ALLOW with an OS-level read-only enforcement marker instead of
            // asking. Reaching here means the per-subcommand analysis could not
            // PROVE this command read-only — but that is not evidence it
            // writes. What lands here is genuinely undecidable statically: an
            // interpreter (`python3 -c`, `node script.mjs`), a network fetch
            // piped into one, or a glob with no static prefix (`cat *`).
            //
            // Plan mode is for gathering evidence, and gathering evidence is
            // shell work: asking a human per command made a real research
            // session pop dozens of approvals, and hard-denying made planning
            // impossible ("plan 模式不允许派子代理，我自己读").
            //
            // The read-only promise is kept by the ENFORCEMENT side, not by a
            // prompt: the marker below tells the bash tool to run this command
            // under a read-only filesystem sandbox (only the per-user scratch
            // temp writable) with network off, so a write or an exfiltration
            // attempt fails at the OS layer. The marker rides the allow input
            // because the decision result itself is consumed and dropped by
            // the dispatch gate; the tool schema does not declare it, so a
            // model-supplied copy never reaches the tool. With no sandbox
            // isolation available (the env-scrub-only process backend) the
            // command still runs — plan mode's write tools remain the hard
            // gate for the plan document itself.
            let mut enforced_input = input.clone();
            if let Json::Object(object) = &mut enforced_input {
                object.insert(
                    RuntimeString::from(PLAN_MODE_READONLY_ENFORCED_KEY),
                    RuntimeJson::Bool(true),
                );
            }
            return PermissionDecision::Allow {
                input: enforced_input,
                reason: PermissionReason::Mode {
                    mode: PermissionMode::Plan,
                },
            };
        }
        if !matches!(
            result,
            BashDecision::Ask {
                from_rule: true,
                ..
            }
        ) && !ctx
            .approval_policy
            .unwrap_or_default()
            .allows_prompt(ApprovalPromptKind::Sandbox)
        {
            return PermissionDecision::Deny {
                message: "Approval policy disallowed a sandbox approval prompt.".to_owned(),
                reason: PermissionReason::Default,
            };
        }
        PermissionDecision::Ask {
            message: format!("Allow bash: {command}?"),
            input: input.clone(),
            suggestions: suggestions_for(&RuntimeString::from("bash"), Some(command)),
            reason: PermissionReason::Default,
        }
    }
}

/// The `PermissionPathKind` mapped to the path-rule matcher's class.
fn path_rule_kind(kind: PermissionPathKind) -> PathRuleKind {
    match kind {
        PermissionPathKind::Read => PathRuleKind::Read,
        PermissionPathKind::Edit => PathRuleKind::Edit,
    }
}

/// Match a rule set against a tool call: path-scoped when the tool exposes file
/// targets, else the generic tool-name/prefix matcher. `use_allow_forms` selects
/// the original (allow) vs symlink-resolved (deny/ask) target forms.
#[allow(clippy::too_many_arguments)]
fn match_rule_set(
    rules: &[PermissionRule],
    file_paths: Option<&FilePaths>,
    path_kind: Option<PermissionPathKind>,
    tool_name: &RuntimeString,
    command: Option<&str>,
    cwd: &RuntimePath,
    use_allow_forms: bool,
) -> Option<PermissionRule> {
    match (file_paths, path_kind) {
        (Some(fp), Some(kind)) => {
            let paths = if use_allow_forms {
                &fp.allow
            } else {
                &fp.deny_ask
            };
            match_path_rule(rules, path_rule_kind(kind), paths, cwd).cloned()
        }
        _ => matches_any_rule(rules, tool_name, command).cloned(),
    }
}

/// The `Allow {tool}{: cmd}?` ask message.
fn ask_message(tool_name: &RuntimeString, command: Option<&str>) -> String {
    let tool_name = permission_prompt_text(tool_name);
    match command {
        Some(c) => format!("Allow {tool_name}: {c}?"),
        None => format!("Allow {tool_name}?"),
    }
}

/// Build the "always allow" suggestion for an ask decision, or `None` when no safe
/// rule can be derived.
fn suggestions_for(
    tool_name: &RuntimeString,
    command: Option<&str>,
) -> Option<Vec<PermissionUpdate>> {
    suggest_rule_for_ask(&permission_prompt_text(tool_name), command).map(|s| vec![s])
}

/// Project the bash-tool rules to the splitter's entries (content = the `Bash(...)`
/// body).
fn to_bash_entries(rules: &[PermissionRule]) -> Vec<BashRuleEntry> {
    let mut out: Vec<BashRuleEntry> = Vec::new();
    for r in rules {
        if !runtime_ascii_name_eq(&r.tool_name, "bash")
            && !runtime_ascii_name_eq(&r.tool_name, "exec_command")
        {
            continue;
        }
        let content = match &r.argument_prefix {
            Some(p) if !p.is_empty() => Some(permission_prompt_text(p)),
            _ => None,
        };
        out.push(BashRuleEntry {
            source: format_rule(r),
            content,
        });
    }
    out
}

fn bash_rule_to_perm(entry: &BashRuleEntry) -> PermissionRule {
    PermissionRule {
        tool_name: RuntimeString::from("bash"),
        argument_prefix: entry.content.clone().map(RuntimeString::from),
        tool_name_prefix: None,
    }
}

fn command_string_of(tool: &dyn Tool, input: &Json) -> Result<Option<String>, ToolError> {
    if let Some(c) = tool.to_command_string(input) {
        return Ok(Some(permission_prompt_text(&c)));
    }
    if input.is_null() {
        return Err(ToolError::fatal(
            "Cannot read properties of null (reading 'command')",
        ));
    }
    Ok(input
        .get("command")
        .and_then(Json::as_string)
        .map(permission_prompt_text))
}

/// Reproduce `raw == null ? fallback : Array.isArray(raw) ? raw : [raw]`
/// without forcing trusted hook rewrites through the tool's declared schema.
pub(super) fn raw_path_values(
    tool: &dyn Tool,
    input: &Json,
    fallback: Option<&RuntimePath>,
) -> Vec<Json> {
    match tool.get_raw_path(input) {
        None => fallback
            .map(|path| vec![Json::String(path.as_runtime_string().clone())])
            .unwrap_or_default(),
        Some(path) if path.is_null() => fallback
            .map(|path| vec![Json::String(path.as_runtime_string().clone())])
            .unwrap_or_default(),
        Some(path) if path.is_array() => path
            .into_array()
            .expect("the exact array shape was checked before consuming"),
        Some(path) => vec![path],
    }
}

pub(super) fn resolve_raw_path(cwd: &RuntimePath, path: &Json) -> Result<RuntimePath, ToolError> {
    let Some(path) = path.as_string() else {
        return Err(ToolError::fatal(format!(
            "The \"paths[1]\" argument must be of type string. {}",
            node_received_value(path)
        )));
    };
    resolve_runtime(cwd, &[cwd.clone(), RuntimePath::from(path.clone())])
        .map_err(|error| ToolError::fatal(permission_prompt_text(error.message())))
}

pub(super) fn permission_prompt_text(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 projection is valid UTF-8")
}

pub(super) fn permission_prompt_path(path: &RuntimePath) -> String {
    // POSIX-rendered like every other path the engine shows: this prompt names a resolved
    // host path, so on Windows `/etc/hosts` came back as `\etc\hosts` in the question the
    // user is asked to answer.
    permission_prompt_text(crate::fs::node_path::posix_rendered_runtime(path).as_runtime_string())
}

fn node_received_value(value: &Json) -> String {
    match value {
        Json::Null => "Received null".to_string(),
        Json::Bool(value) => format!("Received type boolean ({value})"),
        Json::Number(value) => {
            let value = value.as_f64();
            let rendered = if value.is_nan() {
                "NaN".to_string()
            } else if value == f64::INFINITY {
                "Infinity".to_string()
            } else if value == f64::NEG_INFINITY {
                "-Infinity".to_string()
            } else if value == 0.0 && value.is_sign_negative() {
                "-0".to_string()
            } else {
                ryu_js::Buffer::new().format_finite(value).to_owned()
            };
            format!("Received type number ({rendered})")
        }
        Json::String(_) => unreachable!("strings are valid path arguments"),
        Json::Array(_) => "Received an instance of Array".to_string(),
        Json::Object(_) => "Received an instance of Object".to_string(),
    }
}

fn format_rule(rule: &PermissionRule) -> String {
    match &rule.argument_prefix {
        Some(p) if !p.is_empty() => format!(
            "{}({})",
            permission_prompt_text(&rule.tool_name),
            permission_prompt_text(p)
        ),
        _ => permission_prompt_text(&rule.tool_name),
    }
}

#[cfg(test)]
mod ignored_allow_prefix_tests {
    use super::*;
    use crate::permission::rule_store::DurableRules;
    use crate::permission::rules::parse_rule_string;
    use crate::runtime_values::RuntimeObject;
    use crate::schemas::config::default_config;
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::bash::create_bash_tool;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::tools::builtins::web_fetch::{create_web_fetch_tool, WebFetchToolOptions};
    use crate::tools::builtins::write::create_write_tool;
    use crate::types::permission::{PermissionBehavior, PermissionDestination};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn input(field: &str, value: &str) -> Json {
        let mut object = RuntimeObject::new();
        object.insert(field, Json::String(value.into()));
        Json::Object(object)
    }

    #[tokio::test]
    async fn every_saved_allow_prefix_source_is_ignored_without_mutating_grants() {
        let tool = create_bash_tool();
        let command = input("command", "npm install untrusted-package");
        let prefix = parse_rule_string("Bash(npm *)").unwrap();
        for source in ["config", "stored", "pending", "context"] {
            let mut config = default_config();
            config.use_stored_permission_rules = true;
            let durable = Arc::new(DurablePermissionLayer::default());
            let mut ctx = make_ctx("/tmp/klaus-prefix-policy");
            match source {
                "config" => config.allow = vec!["Bash(npm *)".into()],
                "stored" => durable.replace_stored(&DurableRules {
                    allow: vec!["Bash(npm *)".to_string()],
                    ..Default::default()
                }),
                "pending" => durable.grant_pending(PermissionUpdate::AddRules {
                    rules: vec![prefix.clone()],
                    behavior: PermissionBehavior::Allow,
                    destination: PermissionDestination::UserSettings,
                }),
                "context" => ctx.permission.allow_rules = vec![prefix.clone()],
                _ => unreachable!(),
            }
            let engine = StaticPermissionEngine::with_durable_rules(&config, durable);
            let before = engine.rule_snapshot();
            let context_before = ctx.permission.allow_rules.clone();
            assert!(
                matches!(
                    engine.evaluate(tool.as_ref(), &command, &ctx).await,
                    PermissionDecision::Allow { .. }
                ),
                "the fixture must exercise a saved {source} approval",
            );
            assert!(
                matches!(
                    engine
                        .evaluate_without_allow_prefix_rules(tool.as_ref(), &command, &ctx)
                        .await,
                    PermissionDecision::Ask { .. }
                ),
                "{source} prefix must not suppress assessment for an unproven command",
            );
            assert_eq!(engine.rule_snapshot(), before);
            assert_eq!(ctx.permission.allow_rules, context_before);
            assert!(matches!(
                engine.evaluate(tool.as_ref(), &command, &ctx).await,
                PermissionDecision::Allow { .. }
            ));
        }
    }

    #[tokio::test]
    async fn nonprefix_allow_grants_remain_effective() {
        let mut config = default_config();
        config.allow = vec!["Bash".into()];
        let engine = StaticPermissionEngine::new(&config);
        let tool = create_bash_tool();
        let ctx = make_ctx("/tmp/klaus-prefix-policy");
        assert!(matches!(
            engine
                .evaluate_without_allow_prefix_rules(
                    tool.as_ref(),
                    &input("command", "npm install package"),
                    &ctx
                )
                .await,
            PermissionDecision::Allow { .. }
        ));
    }

    #[tokio::test]
    async fn explicitly_empty_prefix_is_filtered_as_an_argument_prefix_grant() {
        let mut config = default_config();
        config.allow = vec!["Bash()".into()];
        let engine = StaticPermissionEngine::new(&config);
        let tool = create_bash_tool();
        let ctx = make_ctx("/tmp/klaus-prefix-policy");
        let command = input("command", "npm install package");
        assert!(matches!(
            engine.evaluate(tool.as_ref(), &command, &ctx).await,
            PermissionDecision::Allow { .. }
        ));
        assert!(matches!(
            engine
                .evaluate_without_allow_prefix_rules(tool.as_ref(), &command, &ctx)
                .await,
            PermissionDecision::Ask { .. }
        ));
    }

    #[tokio::test]
    async fn prefix_deny_and_ask_rules_keep_their_force_for_every_source() {
        let tool = create_bash_tool();
        for source in ["config", "stored", "context"] {
            let mut config = default_config();
            config.allow = vec!["Bash(npm *)".into()];
            config.use_stored_permission_rules = true;
            let durable = Arc::new(DurablePermissionLayer::default());
            let mut ctx = make_ctx("/tmp/klaus-prefix-policy");
            ctx.sandbox_mode = Some(SandboxMode::DangerFullAccess);
            match source {
                "config" => {
                    config.deny = vec!["Bash(npm publish)".into()];
                    config.ask = vec!["Bash(npm install)".into()];
                }
                "stored" => durable.replace_stored(&DurableRules {
                    deny: vec!["Bash(npm publish)".to_string()],
                    ask: vec!["Bash(npm install)".to_string()],
                    ..Default::default()
                }),
                "context" => {
                    ctx.permission.deny_rules =
                        vec![parse_rule_string("Bash(npm publish)").unwrap()];
                    ctx.permission.ask_rules =
                        vec![parse_rule_string("Bash(npm install)").unwrap()];
                }
                _ => unreachable!(),
            }
            let engine = StaticPermissionEngine::with_durable_rules(&config, durable);
            for approval in [ApprovalPolicy::OnRequest, ApprovalPolicy::Never] {
                ctx.approval_policy = Some(approval);
                assert!(
                    matches!(
                        engine
                            .evaluate_without_allow_prefix_rules(
                                tool.as_ref(),
                                &input("command", "npm publish package"),
                                &ctx
                            )
                            .await,
                        PermissionDecision::Deny {
                            reason: PermissionReason::Rule { .. },
                            ..
                        }
                    ),
                    "{source} deny must survive full filesystem access under {approval:?}"
                );
                let decision = engine
                    .evaluate_without_allow_prefix_rules(
                        tool.as_ref(),
                        &input("command", "npm install package"),
                        &ctx,
                    )
                    .await;
                assert!(
                    matches!(
                        (approval, decision),
                        (ApprovalPolicy::OnRequest, PermissionDecision::Ask { .. })
                            | (
                                ApprovalPolicy::Never,
                                PermissionDecision::Deny {
                                    reason: PermissionReason::SafetyCheck,
                                    ..
                                }
                            )
                    ),
                    "{source} ask must not be automatically allowed under {approval:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn safe_commands_stay_allowed_and_security_prompts_stay_required() {
        let mut config = default_config();
        config.allow = vec!["Bash(echo *)".into(), "Bash(rm *)".into()];
        let engine = StaticPermissionEngine::new(&config);
        let tool = create_bash_tool();
        let ctx = make_ctx("/tmp/klaus-prefix-policy");
        assert!(matches!(
            engine
                .evaluate_without_allow_prefix_rules(
                    tool.as_ref(),
                    &input("command", "echo hello"),
                    &ctx
                )
                .await,
            PermissionDecision::Allow {
                reason: PermissionReason::Default,
                ..
            }
        ));
        assert!(matches!(
            engine
                .evaluate_without_allow_prefix_rules(
                    tool.as_ref(),
                    &input("command", "rm -rf /"),
                    &ctx
                )
                .await,
            PermissionDecision::Ask {
                reason: PermissionReason::SafetyCheck,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn filesystem_and_plan_ceilings_remain_in_force() {
        let mut config = default_config();
        config.allow = vec!["Write".into()];
        let engine = StaticPermissionEngine::new(&config);
        let tool = create_write_tool();
        let mut ctx = make_ctx("/tmp/klaus-prefix-policy/workspace");
        ctx.sandbox_mode = Some(SandboxMode::WorkspaceWrite);
        assert!(matches!(
            engine
                .evaluate_without_allow_prefix_rules(
                    tool.as_ref(),
                    &input("path", "/tmp/klaus-prefix-policy/outside.txt"),
                    &ctx
                )
                .await,
            PermissionDecision::Ask { .. }
        ));
        ctx.sandbox_mode = Some(SandboxMode::ReadOnly);
        assert!(matches!(
            engine
                .evaluate_without_allow_prefix_rules(
                    tool.as_ref(),
                    &input("path", "inside.txt"),
                    &ctx
                )
                .await,
            PermissionDecision::Ask { .. }
        ));
        config.allow.clear();
        let engine = StaticPermissionEngine::new(&config);
        ctx.planning_read_only = true;
        assert!(matches!(
            engine
                .evaluate_without_allow_prefix_rules(
                    tool.as_ref(),
                    &input("path", "inside.txt"),
                    &ctx
                )
                .await,
            PermissionDecision::Deny {
                reason: PermissionReason::Mode {
                    mode: PermissionMode::Plan
                },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn task_owned_root_ceiling_still_rejects_outside_writes() {
        static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "klaus-prefix-task-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed),
        ));
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let workspace = std::fs::canonicalize(&workspace).unwrap();
        let mut config = default_config();
        config.allow = vec!["Write".into()];
        config.task_resource_scope = Some(TaskResourceScope {
            task_id: "prefix-filter-root-ceiling".to_string(),
            mode: TaskResourceMode::ScopedAuto,
            network_allowed: false,
            read_roots: vec![workspace.to_string_lossy().as_ref().into()],
            write_roots: vec![workspace.to_string_lossy().as_ref().into()],
        });
        let engine = StaticPermissionEngine::new(&config);
        let ctx = make_ctx(workspace.to_str().unwrap());
        let tool = create_write_tool();
        let outside = root.join("outside.txt");
        let decision = engine
            .evaluate_without_allow_prefix_rules(
                tool.as_ref(),
                &input("path", outside.to_str().unwrap()),
                &ctx,
            )
            .await;
        std::fs::remove_dir_all(&root).unwrap();
        assert!(matches!(
            decision,
            PermissionDecision::Deny { message, reason: PermissionReason::SafetyCheck }
                if message.contains("outside authorized directories")
        ));
    }

    #[tokio::test]
    async fn native_fetch_keeps_domain_and_captured_network_denials() {
        let mut config = default_config();
        config.allow = vec!["web_fetch".into(), "Bash(curl *)".into()];
        let engine = StaticPermissionEngine::new(&config);
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let mut ctx = make_ctx("/tmp/klaus-prefix-policy");
        ctx.network_allowed = Some(false);
        let calls = callback_calls.clone();
        ctx.request_network_access = Some(Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { false })
        }));
        for domain_blocked in [true, false] {
            let tool = create_web_fetch_tool(WebFetchToolOptions {
                blocked_domains: domain_blocked.then(|| vec!["blocked.example".into()]),
                ..Default::default()
            });
            let request = input("url", "https://blocked.example/");
            assert!(matches!(
                engine
                    .evaluate_without_allow_prefix_rules(tool.as_ref(), &request, &ctx)
                    .await,
                PermissionDecision::Allow { .. }
            ));
            let result = tool.call(request, &ctx, None).await.unwrap();
            assert_eq!(result.serializable.is_error, Some(true));
            let OutputBlock::Text(text) = &result.serializable.model_content[0] else {
                panic!("network denial must be rendered as text");
            };
            let text = text.text.try_to_utf8().unwrap();
            assert!(text.contains(if domain_blocked {
                "network domain policy"
            } else {
                "Network access denied"
            }));
            assert_eq!(
                callback_calls.load(Ordering::SeqCst),
                usize::from(!domain_blocked)
            );
        }
        assert_eq!(ctx.network_allowed, Some(false));
    }

    struct UnsupportedCustomEngine;

    #[async_trait]
    impl PermissionEngine for UnsupportedCustomEngine {
        async fn evaluate(
            &self,
            _tool: &dyn Tool,
            input: &Json,
            _ctx: &ToolContext,
        ) -> PermissionDecision {
            PermissionDecision::Allow {
                input: input.clone(),
                reason: PermissionReason::Default,
            }
        }
    }

    #[tokio::test]
    async fn custom_engine_without_filter_support_fails_closed() {
        let engine = UnsupportedCustomEngine;
        let tool = create_bash_tool();
        let ctx = make_ctx("/tmp/klaus-prefix-policy");
        let request = input("command", "npm install package");
        assert!(matches!(
            engine.evaluate(tool.as_ref(), &request, &ctx).await,
            PermissionDecision::Allow { .. }
        ));
        assert!(matches!(
            engine.evaluate_without_allow_prefix_rules(tool.as_ref(), &request, &ctx).await,
            PermissionDecision::Deny { message, reason: PermissionReason::SafetyCheck }
                if message.contains("cannot evaluate")
        ));
    }
}

#[cfg(test)]
mod trusted_path_tests {
    use super::{
        node_received_value, PermissionDecision, PermissionEngine, StaticPermissionEngine,
    };
    use crate::runtime_values::{
        runtime_codec, RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString,
    };
    use crate::schemas::config::default_config;
    use crate::tools::builtins::bash::create_bash_tool;
    use crate::tools::builtins::edit::create_edit_tool;
    use crate::tools::builtins::file_search::create_file_search_tool;
    use crate::tools::builtins::glob::create_glob_tool;
    use crate::tools::builtins::grep::create_grep_tool;
    use crate::tools::builtins::ls::create_ls_tool;
    use crate::tools::builtins::multi_edit::create_multi_edit_tool;
    use crate::tools::builtins::read::create_read_tool;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::tools::builtins::write::create_write_tool;
    use crate::types::errors::ToolErrorKind;
    use crate::types::permission::{ApprovalPolicy, PermissionReason, SandboxMode};
    use crate::types::tool::Tool;
    use std::sync::Arc;

    macro_rules! exact_json {
        (null) => {
            RuntimeJson::Null
        };
        ([ $($value:tt),* $(,)? ]) => {
            RuntimeJson::Array(vec![$(exact_json!($value)),*])
        };
        ({ $($key:literal : $value:tt),* $(,)? }) => {{
            let mut object = RuntimeObject::new();
            $(object.insert($key, exact_json!($value));)*
            RuntimeJson::Object(object)
        }};
        ($value:expr) => {
            runtime_codec::encode_value(&$value).expect("exact permission JSON expression must encode")
        };
    }

    fn raw_path_file_tools() -> Vec<Arc<dyn Tool>> {
        vec![
            create_read_tool(),
            create_edit_tool(),
            create_write_tool(),
            create_multi_edit_tool(),
            create_glob_tool(),
            create_ls_tool(),
            create_file_search_tool(),
        ]
    }

    #[test]
    fn node_path_type_error_uses_javascript_number_rendering() {
        let positive_infinity = RuntimeJson::Number(RuntimeNumber::from_f64(f64::INFINITY));
        let negative_infinity = RuntimeJson::Number(RuntimeNumber::from_f64(f64::NEG_INFINITY));
        assert_eq!(
            node_received_value(&positive_infinity),
            "Received type number (Infinity)"
        );
        assert_eq!(
            node_received_value(&negative_infinity),
            "Received type number (-Infinity)"
        );
        assert_eq!(
            node_received_value(&exact_json!(-0.0)),
            "Received type number (-0)"
        );
        assert_eq!(
            node_received_value(&exact_json!(9_007_199_254_740_993_u64)),
            "Received type number (9007199254740992)"
        );
    }

    #[tokio::test]
    async fn raw_path_arrays_keep_empty_and_external_targets() {
        let engine = StaticPermissionEngine::new(&default_config());
        let tool = create_grep_tool();
        let ctx = make_ctx("/tmp/klaus-permission-root");

        let empty = exact_json!({ "pattern": "x", "path": [] });
        assert!(matches!(
            engine.evaluate(tool.as_ref(), &empty, &ctx).await,
            PermissionDecision::Allow { input, .. } if input == empty
        ));

        let external = exact_json!({
            "pattern": "x",
            "path": ["inside", "/etc/passwd"]
        });
        assert!(matches!(
            engine.evaluate(tool.as_ref(), &external, &ctx).await,
            PermissionDecision::Allow { input, .. } if input == external
        ));
    }

    /// A config `ask` rule forces the prompt even when an `allow` rule (and the
    /// mode) would otherwise permit the action outright: ask is evaluated before
    /// allow, and config ask rules gate exactly like session-granted ones. This
    /// is the durable half of the `permission-rules.json` store the app-server
    /// seeds a session from.
    #[tokio::test]
    async fn config_ask_rule_forces_a_prompt_over_a_broad_allow() {
        let mut config = default_config();
        config.allow = vec![RuntimeString::from("Write")];
        config.ask = vec![RuntimeString::from("Write")];
        let engine = StaticPermissionEngine::new(&config);
        let tool = create_write_tool();
        let ctx = make_ctx("/tmp/klaus-permission-root");
        let input = exact_json!({ "path": "/tmp/klaus-permission-root/x.txt", "content": "y" });
        assert!(
            matches!(
                engine.evaluate(tool.as_ref(), &input, &ctx).await,
                PermissionDecision::Ask { .. }
            ),
            "an ask rule must outrank a broad allow rule"
        );
    }

    #[tokio::test]
    async fn workspace_policy_allows_edits_and_requires_an_isolation_backend_for_commands() {
        let engine = StaticPermissionEngine::new(&default_config());
        let write = create_write_tool();
        let bash = create_bash_tool();
        let mut ctx = make_ctx("/tmp/klaus-permission-root");
        ctx.sandbox_mode = Some(SandboxMode::WorkspaceWrite);
        ctx.approval_policy = Some(ApprovalPolicy::OnRequest);
        ctx.approvals_reviewer = Some(crate::types::permission::ApprovalsReviewer::Automatic);

        let inside = exact_json!({ "path": "src/main.rs", "content": "x" });
        assert!(matches!(
            engine.evaluate(write.as_ref(), &inside, &ctx).await,
            PermissionDecision::Allow { .. }
        ));

        let outside = exact_json!({ "path": "/tmp/outside.txt", "content": "x" });
        assert!(matches!(
            engine.evaluate(write.as_ref(), &outside, &ctx).await,
            PermissionDecision::Ask { .. }
        ));

        let command = exact_json!({ "command": "cargo check" });
        assert!(matches!(
            engine.evaluate(bash.as_ref(), &command, &ctx).await,
            PermissionDecision::Deny { .. }
        ));

        let dangerous = exact_json!({ "command": "rm -rf /" });
        assert!(matches!(
            engine.evaluate(bash.as_ref(), &dangerous, &ctx).await,
            PermissionDecision::Ask {
                reason: PermissionReason::SafetyCheck,
                ..
            }
        ));

        let mut ask_config = default_config();
        ask_config.ask = vec![RuntimeString::from("Bash(cargo check)")];
        let ask_engine = StaticPermissionEngine::new(&ask_config);
        assert!(matches!(
            ask_engine.evaluate(bash.as_ref(), &command, &ctx).await,
            PermissionDecision::Ask { .. }
        ));
    }

    #[tokio::test]
    async fn raw_path_array_non_string_is_an_exact_fatal_type_error() {
        let engine = StaticPermissionEngine::new(&default_config());
        let tool = create_grep_tool();
        let ctx = make_ctx("/tmp/klaus-permission-root");
        let PermissionDecision::Fatal { error } = engine
            .evaluate(
                tool.as_ref(),
                &exact_json!({ "pattern": "x", "path": ["inside", null, "/etc/passwd"] }),
                &ctx,
            )
            .await
        else {
            panic!("non-string path must be a typed fatal permission decision");
        };
        assert_eq!(error.kind, ToolErrorKind::Fatal);
        assert_eq!(
            error.message,
            "The \"paths[1]\" argument must be of type string. Received null"
        );
    }

    #[test]
    fn every_builtin_file_tool_preserves_trusted_raw_path_values() {
        let array = exact_json!(["inside", "/etc/passwd"]);
        let number = exact_json!(7);
        for tool in raw_path_file_tools() {
            assert_eq!(
                tool.get_raw_path(&exact_json!({
                    "pattern": "src/**/*.rs",
                    "path": (array.clone()),
                })),
                Some(array.clone()),
                "{:?} must preserve arrays from a trusted hook rewrite",
                tool.name()
            );
            assert_eq!(
                tool.get_raw_path(&exact_json!({
                    "pattern": "src/**/*.rs",
                    "path": (number.clone()),
                })),
                Some(number.clone()),
                "{:?} must preserve non-string values from a trusted hook rewrite",
                tool.name()
            );
        }

        for tool in [
            create_read_tool(),
            create_edit_tool(),
            create_write_tool(),
            create_multi_edit_tool(),
            create_ls_tool(),
            create_file_search_tool(),
        ] {
            assert_eq!(
                tool.get_raw_path(&exact_json!({ "path": null })),
                Some(RuntimeJson::Null),
                "{:?} must preserve explicit null",
                tool.name()
            );
        }

        assert_eq!(
            create_glob_tool().get_raw_path(&exact_json!({
                "pattern": "src/**/*.rs",
                "path": null,
            })),
            Some(exact_json!("src")),
            "glob's nullish path must fall back to the pattern base"
        );
    }

    #[tokio::test]
    async fn every_builtin_file_tool_reports_the_first_raw_path_type_error() {
        let engine = StaticPermissionEngine::new(&default_config());
        let ctx = make_ctx("/tmp/klaus-permission-root");
        let input = exact_json!({
            "pattern": "src/**/*.rs",
            "path": [false, null, "/etc/passwd"],
        });

        for tool in raw_path_file_tools() {
            let decision = engine.evaluate(tool.as_ref(), &input, &ctx).await;
            let PermissionDecision::Fatal { error } = decision else {
                panic!(
                    "{:?} did not return a typed fatal decision for a non-string raw path",
                    tool.name()
                );
            };
            assert_eq!(error.kind, ToolErrorKind::Fatal, "{:?}", tool.name());
            assert_eq!(
                error.message,
                "The \"paths[1]\" argument must be of type string. Received type boolean (false)",
                "{:?} must stop at the first invalid array element",
                tool.name()
            );
        }
    }
}

/// Plan mode's ONE mutable surface: the session's plan document. These lock the
/// exemption's shape — it must cover the session's own plan files (main and
/// sub-agent), must not widen to any other path, must not leak to non-edit
/// tools, and must not exist at all before `create_plan` binds a name.
#[cfg(test)]
mod plan_file_exemption_tests {
    use super::{PermissionDecision, PermissionEngine, StaticPermissionEngine};
    use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimePath, RuntimeString};
    use crate::schemas::config::default_config;
    use crate::tools::builtins::bash::create_bash_tool;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::tools::builtins::write::create_write_tool;
    use crate::types::tool::{PlanFileContext, ToolContext};

    const PLANS_DIR: &str = "/tmp/klaus-plan-exempt/plans";
    const NAME: &str = "align-queue-bar";

    fn write_input(path: &str) -> RuntimeJson {
        let mut object = RuntimeObject::new();
        object.insert("path", RuntimeJson::String(RuntimeString::from(path)));
        object.insert(
            "content",
            RuntimeJson::String(RuntimeString::from("# plan")),
        );
        RuntimeJson::Object(object)
    }

    /// A plan-mode context whose session has bound `<PLANS_DIR>/<NAME>.md`.
    fn plan_mode_ctx() -> ToolContext {
        let mut ctx = make_ctx("/tmp/klaus-plan-exempt/work");
        ctx.planning_read_only = true;
        ctx.plan_file = Some(PlanFileContext {
            plans_dir: RuntimePath::from(PLANS_DIR),
            name: RuntimeString::from(NAME),
            path: RuntimePath::from(format!("{PLANS_DIR}/{NAME}.md")),
        });
        ctx
    }

    #[tokio::test]
    async fn plan_mode_allows_writing_the_session_plan_file() {
        let engine = StaticPermissionEngine::new(&default_config());
        let tool = create_write_tool();
        let decision = engine
            .evaluate(
                tool.as_ref(),
                &write_input(&format!("{PLANS_DIR}/{NAME}.md")),
                &plan_mode_ctx(),
            )
            .await;
        assert!(
            matches!(decision, PermissionDecision::Allow { .. }),
            "the session's own plan file must be writable in plan mode, got {decision:?}"
        );
    }

    #[tokio::test]
    async fn plan_mode_allows_writing_a_sub_agent_plan_file() {
        let engine = StaticPermissionEngine::new(&default_config());
        let tool = create_write_tool();
        let decision = engine
            .evaluate(
                tool.as_ref(),
                &write_input(&format!("{PLANS_DIR}/{NAME}-agent-a1.md")),
                &plan_mode_ctx(),
            )
            .await;
        assert!(
            matches!(decision, PermissionDecision::Allow { .. }),
            "a planning sub-agent's plan file shares the exemption, got {decision:?}"
        );
    }

    /// The plan-file allowlist is consulted BEFORE the sensitive-path safety
    /// gate, so a plans directory living under `.klaus` (the desktop's per-project
    /// `plansDirectory = .klaus/specs`) never prompts. This was the exact live
    /// failure: the engine wrote `<project>/.klaus/specs/<name>.md` and got stuck
    /// on "Allow write to a sensitive path (sensitive-directory)?".
    #[tokio::test]
    async fn plan_file_exemption_outranks_the_sensitive_directory_gate() {
        let engine = StaticPermissionEngine::new(&default_config());
        let tool = create_write_tool();
        let sensitive_plans_dir = "/tmp/klaus-plan-exempt/work/.klaus/specs";
        let mut ctx = make_ctx("/tmp/klaus-plan-exempt/work");
        ctx.planning_read_only = true;
        ctx.plan_file = Some(PlanFileContext {
            plans_dir: RuntimePath::from(sensitive_plans_dir),
            name: RuntimeString::from(NAME),
            path: RuntimePath::from(format!("{sensitive_plans_dir}/{NAME}.md")),
        });
        let decision = engine
            .evaluate(
                tool.as_ref(),
                &write_input(&format!("{sensitive_plans_dir}/{NAME}.md")),
                &ctx,
            )
            .await;
        assert!(
            matches!(decision, PermissionDecision::Allow { .. }),
            "a plan file under .klaus must not trip the sensitive gate, got {decision:?}"
        );
    }

    /// The plan-file allowlist sits in the GENERAL write evaluator, not behind a
    /// plan-mode check: once the plan is approved the execution phase keeps
    /// updating the plan document without prompting.
    #[tokio::test]
    async fn plan_file_stays_writable_outside_plan_mode() {
        let engine = StaticPermissionEngine::new(&default_config());
        let tool = create_write_tool();
        let mut ctx = plan_mode_ctx();
        ctx.planning_read_only = false;
        let decision = engine
            .evaluate(
                tool.as_ref(),
                &write_input(&format!("{PLANS_DIR}/{NAME}.md")),
                &ctx,
            )
            .await;
        assert!(
            matches!(decision, PermissionDecision::Allow { .. }),
            "the execution phase keeps writing the plan document, got {decision:?}"
        );
    }

    #[tokio::test]
    async fn plan_mode_denies_writing_anything_else() {
        let engine = StaticPermissionEngine::new(&default_config());
        let tool = create_write_tool();
        // Ordinary source file, a DIFFERENT bound plan name, a nested path under
        // the plans dir, and a non-markdown sibling all stay blocked. The
        // different-name case is the document-level isolation that remains now
        // that names are model-supplied rather than session-random: two plans in
        // one directory never exempt each other. (Two plans that bind the SAME
        // name do share the exemption — that is inherent to a name the model
        // chooses, and is why the reminder reports the bound document.)
        let paths = [
            "/tmp/klaus-plan-exempt/work/src/main.rs".to_string(),
            format!("{PLANS_DIR}/some-other-plan.md"),
            format!("{PLANS_DIR}/nested/{NAME}.md"),
            format!("{PLANS_DIR}/{NAME}.txt"),
        ];
        for path in paths {
            let decision = engine
                .evaluate(tool.as_ref(), &write_input(&path), &plan_mode_ctx())
                .await;
            assert!(
                matches!(decision, PermissionDecision::Deny { .. }),
                "plan mode must still block writing {path}, got {decision:?}"
            );
        }
    }

    /// The plan-file write exemption is edit-class only: a SHELL command that
    /// names the plan file must never be waved through by it.
    ///
    /// The assertion is "not Allow" rather than "Deny" on purpose. Plan mode now
    /// ASKS for shell commands it cannot prove read-only (research needs them, and
    /// `rm -rf` on a specific path is undecidable statically), so a hard Deny is no
    /// longer the only safe outcome. What must never happen is the exemption
    /// silently permitting the deletion.
    #[tokio::test]
    async fn plan_mode_exemption_does_not_leak_to_non_edit_tools() {
        let engine = StaticPermissionEngine::new(&default_config());
        let tool = create_bash_tool();
        let mut object = RuntimeObject::new();
        object.insert(
            "command",
            RuntimeJson::String(RuntimeString::from(format!("rm -rf {PLANS_DIR}/{NAME}.md"))),
        );
        let decision = engine
            .evaluate(
                tool.as_ref(),
                &RuntimeJson::Object(object),
                &plan_mode_ctx(),
            )
            .await;
        assert!(
            !matches!(decision, PermissionDecision::Allow { .. }),
            "the exemption is edit-class only — a shell command naming the plan file must not be auto-allowed, got {decision:?}"
        );
    }

    #[tokio::test]
    async fn plan_mode_without_a_plan_file_blocks_every_write() {
        let engine = StaticPermissionEngine::new(&default_config());
        let tool = create_write_tool();
        let mut ctx = make_ctx("/tmp/klaus-plan-exempt/work");
        ctx.planning_read_only = true;
        // No plan bound yet → nothing is exempt.
        let decision = engine
            .evaluate(
                tool.as_ref(),
                &write_input(&format!("{PLANS_DIR}/{NAME}.md")),
                &ctx,
            )
            .await;
        assert!(
            matches!(decision, PermissionDecision::Deny { .. }),
            "without a plan-file context plan mode is fully read-only, got {decision:?}"
        );
    }
}

/// Plan mode is for gathering evidence, and gathering evidence is shell work.
///
/// Defect (2026-09-18): a real plan-mode session had 13 research commands
/// hard-denied with "Plan mode is read-only". Measured causes: `2>/dev/null` was
/// counted as a file write (it discards output), unquoted globs like
/// `--include=*.rs` were rejected because a glob can expand past the string
/// checks, and interpreters (`python3 -c`, `node`) are undecidable statically.
/// The session logged "plan 模式不允许派子代理，我自己读" and serialized its
/// research by hand.
///
/// Fixes, all keeping the default safe:
/// - the null device is no longer a redirect TARGET (see `split.rs`), so
///   `grep ... 2>/dev/null` is provably read-only and auto-allowed;
/// - a prefixed glob (`dir/*.swift`, `--include=*.rs`) is provably read-only
///   because its expansion keeps the static prefix (see `readonly/mod.rs`);
/// - a command that still cannot be PROVEN read-only ASKS instead of denying.
///
/// Asking is not a new plan-mode semantic: the safety-check gate above already
/// asked for `rm -rf ~`. And nothing becomes auto-allowed — an Ask with no
/// approval handler fails closed, so headless/scheduled plan-mode runs still
/// execute nothing.
#[cfg(test)]
mod plan_mode_research_shell {
    use super::*;
    use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
    use crate::schemas::config::default_config;
    use crate::tools::builtins::bash::create_bash_tool;
    use crate::tools::builtins::test_ctx::make_ctx;

    /// The 13 commands a real session had denied, replayed against the engine.
    /// None may be hard-denied any more; each is either provably read-only
    /// (auto-allowed) or ambiguous (asks the user).
    #[tokio::test]
    async fn real_denied_research_commands_are_never_hard_denied() {
        let raw = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/plan-mode/denied-bash-commands.json"
        ))
        .unwrap();
        let commands: Vec<String> = serde_json::from_str(&raw).unwrap();
        let tool = create_bash_tool();
        let engine = StaticPermissionEngine::new(&default_config());
        let (mut allow, mut ask, mut deny) = (0usize, 0usize, 0usize);
        for command in &commands {
            let mut ctx = make_ctx("/repo");
            ctx.planning_read_only = true;
            let mut object = RuntimeObject::new();
            object.insert(
                "command",
                RuntimeJson::String(RuntimeString::from(command.as_str())),
            );
            match engine
                .evaluate(tool.as_ref(), &RuntimeJson::Object(object), &ctx)
                .await
            {
                PermissionDecision::Allow { .. } => allow += 1,
                PermissionDecision::Ask { .. } => ask += 1,
                PermissionDecision::Deny { .. } => deny += 1,
                PermissionDecision::Fatal { .. } => {}
            }
        }
        // Nothing may be silently allowed that was not provably read-only before.
        assert_eq!(allow + ask + deny, commands.len());
        assert_eq!(
            deny, 0,
            "no research command should be hard-denied any more"
        );
    }

    /// Writes must still never be auto-allowed in plan mode, and the ones that
    /// reach the safety gate must still Ask.
    /// The other half of the guarantee: loosening research must not loosen writes.
    #[tokio::test]
    async fn writes_are_never_auto_allowed_in_plan_mode() {
        let tool = create_bash_tool();
        let engine = StaticPermissionEngine::new(&default_config());
        for command in [
            "echo x > /tmp/pwn",
            "rm -rf /repo",
            "rm -rf ~",
            "git commit -m x",
            "npm install",
            "sed -i '' 's/a/b/' f.txt",
            "sort -o /tmp/pwn *",
        ] {
            let mut ctx = make_ctx("/repo");
            ctx.planning_read_only = true;
            let mut object = RuntimeObject::new();
            object.insert("command", RuntimeJson::String(RuntimeString::from(command)));
            let verdict = match engine
                .evaluate(tool.as_ref(), &RuntimeJson::Object(object), &ctx)
                .await
            {
                PermissionDecision::Allow { .. } => "ALLOW",
                PermissionDecision::Ask { .. } => "ASK",
                PermissionDecision::Deny { .. } => "DENY",
                PermissionDecision::Fatal { .. } => "FATAL",
            };
            assert_ne!(
                verdict, "ALLOW",
                "a write must never be auto-allowed in plan mode: {command}"
            );
        }
    }

    /// Provably read-only research stays AUTO-allowed (no prompt), and the
    /// /dev/null fix is what unblocked it.
    /// Provably read-only research must not prompt — a plan-mode session that has
    /// to approve every `grep` is not usable. This is what the null-device fix
    /// buys: `2>/dev/null` stops looking like a write.
    #[tokio::test]
    async fn readonly_research_stays_auto_allowed() {
        let tool = create_bash_tool();
        let engine = StaticPermissionEngine::new(&default_config());
        for command in [
            "grep -rn foo src/",
            "grep -rn foo src/ 2>/dev/null",
            "grep -rn foo src/ 2>/dev/null | head -20",
            "grep -rn \"UNUserNotificationCenter\\|UserNotifications\" apps/ios/KlausMobile/KlausMobile/*.swift apps/ios/KlausMobile/KlausMobileApp.swift 2>/dev/null | grep -v \"//\" | head -15",
            "grep -rn \"UNUserNotificationCenter\\|UserNotifications\\|push\\|Notification\" apps/ios/KlausMobile/KlausMobile/*.swift apps/ios/KlausMobile/KlausMobileApp.swift 2>/dev/null | grep -v \"//\" | head -15; echo -----; grep -n \"UIBackgroundModes\\|remote-notification\" apps/ios/KlausMobile/KlausMobile/Info.plist",
            "grep -rn foo --include=*.rs crates/ 2>/dev/null | head -20",
            "cd /repo && grep -rn foo src/",
            "ls src/ | grep -i perm",
            "git status",
            "git log --oneline -5",
            "sed -n '1,20p' src/main.ts",
            "head -c 1200 file.json",
            "find . -name '*.ts'",
            "wc -l file",
        ] {
            let mut ctx = make_ctx("/repo");
            ctx.planning_read_only = true;
            let mut object = RuntimeObject::new();
            object.insert("command", RuntimeJson::String(RuntimeString::from(command)));
            let verdict = match engine
                .evaluate(tool.as_ref(), &RuntimeJson::Object(object), &ctx)
                .await
            {
                PermissionDecision::Allow { .. } => "ALLOW",
                PermissionDecision::Ask { .. } => "ASK",
                PermissionDecision::Deny { .. } => "DENY",
                PermissionDecision::Fatal { .. } => "FATAL",
            };
            assert_eq!(
                verdict, "ALLOW",
                "plain read-only research must not prompt: {command}"
            );
        }
    }

    /// A command the static analysis cannot prove read-only (an interpreter) is
    /// AUTO-allowed under a real isolation backend, carrying the read-only
    /// enforcement marker the bash tool turns into a read-only sandbox; and it
    /// still ASKS where no isolation backend exists to enforce anything.
    #[tokio::test]
    async fn unprovable_commands_auto_allow_with_the_enforcement_marker() {
        use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxPolicy, SpawnSpec};

        struct SeatbeltNoop;
        impl SandboxBackend for SeatbeltNoop {
            fn kind(&self) -> SandboxKind {
                SandboxKind::Seatbelt
            }
            fn is_available(&self) -> bool {
                true
            }
            fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
                SpawnSpec {
                    file: req.argv.first().cloned().unwrap_or_default(),
                    args: Vec::new(),
                    cwd: req.cwd.clone(),
                    env: crate::runtime_process::RuntimeEnv::new(),
                }
            }
        }

        let tool = create_bash_tool();
        let engine = StaticPermissionEngine::new(&default_config());
        let command = "python3 -c \"print(open('x').read())\"";

        let mut ctx = make_ctx("/repo");
        ctx.planning_read_only = true;
        ctx.sandbox = std::sync::Arc::new(SeatbeltNoop);
        let mut object = RuntimeObject::new();
        object.insert("command", RuntimeJson::String(RuntimeString::from(command)));
        match engine
            .evaluate(tool.as_ref(), &RuntimeJson::Object(object), &ctx)
            .await
        {
            PermissionDecision::Allow { input, .. } => {
                assert_eq!(
                    input
                        .get(crate::permission::PLAN_MODE_READONLY_ENFORCED_KEY)
                        .and_then(|v| v.as_bool()),
                    Some(true),
                    "the allow must carry the read-only enforcement marker"
                );
            }
            other => panic!("expected an enforced allow, got {other:?}"),
        }

        // No isolation backend → the human stays in the loop.
        let mut ctx = make_ctx("/repo");
        ctx.planning_read_only = true;
        let mut object = RuntimeObject::new();
        object.insert("command", RuntimeJson::String(RuntimeString::from(command)));
        assert!(
            matches!(
                engine
                    .evaluate(tool.as_ref(), &RuntimeJson::Object(object), &ctx)
                    .await,
                PermissionDecision::Ask { .. }
            ),
            "without an isolation backend the undecidable command must still ask"
        );
    }
}

#[cfg(test)]
mod approval_category_tests {
    use super::*;
    use crate::schemas::config::default_config;
    use crate::tools::builtins::bash::create_bash_tool;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::permission::GranularApprovalConfig;

    fn input() -> RuntimeJson {
        let mut value = crate::runtime_values::RuntimeObject::new();
        value.insert("command", RuntimeJson::String("git status".into()));
        RuntimeJson::Object(value)
    }

    #[tokio::test]
    async fn rule_prompt_permission_is_independent_of_sandbox_prompt_permission() {
        let mut config = default_config();
        config.ask = vec!["Bash(git status)".into()];
        let engine = StaticPermissionEngine::new(&config);
        let tool = create_bash_tool();
        let mut context = make_ctx("/tmp/approval-category");
        let mut categories = GranularApprovalConfig {
            sandbox_approval: false,
            rules: true,
            skill_approval: false,
            request_permissions: false,
            mcp_elicitations: false,
        };
        context.approval_policy = Some(ApprovalPolicy::Granular(categories));
        assert!(matches!(
            engine.evaluate(tool.as_ref(), &input(), &context).await,
            PermissionDecision::Ask { .. }
        ));
        categories.rules = false;
        categories.sandbox_approval = true;
        context.approval_policy = Some(ApprovalPolicy::Granular(categories));
        assert!(matches!(
            engine.evaluate(tool.as_ref(), &input(), &context).await,
            PermissionDecision::Deny { .. }
        ));
    }

    #[tokio::test]
    async fn untrusted_commands_require_explicit_rules_instead_of_read_only_heuristics() {
        let tool = create_bash_tool();
        let mut context = make_ctx("/tmp/approval-category");
        context.approval_policy = Some(ApprovalPolicy::UnlessTrusted);
        let engine = StaticPermissionEngine::new(&default_config());
        assert!(matches!(
            engine.evaluate(tool.as_ref(), &input(), &context).await,
            PermissionDecision::Ask { .. }
        ));
        let mut config = default_config();
        config.allow = vec!["Bash(git status)".into()];
        let engine = StaticPermissionEngine::new(&config);
        assert!(matches!(
            engine.evaluate(tool.as_ref(), &input(), &context).await,
            PermissionDecision::Allow { .. }
        ));
    }
}
