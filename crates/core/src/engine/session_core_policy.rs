use super::base_hook_input;
use super::join_runtime_strings;
#[cfg(test)]
use super::maybe_inject_setup_owned_task_panic;
use super::resolve_abs;
use super::ActiveSetupAttempt;
use super::ActiveSetupContextAdmission;
use super::RuntimePolicyCommit;
use super::RuntimePolicySnapshot;
use super::RuntimePolicyState;
use super::RuntimePolicyUpdate;
use super::Session;
use super::SetupAttemptGuard;
use super::SetupContextAdmissionReceipt;
use super::SetupContextClaim;
use super::SetupContextProviderAdmission;
use super::SetupContextProviderPhase;
use super::SetupDurabilityRegistry;
use super::SetupEffectPhase;
use super::SetupInitCloseReceipt;
use super::SetupOwnedTaskControl;
use super::SetupOwnedTaskKind;
use super::SetupOwnedTaskRegistration;
use super::SetupReceipt;
use super::SetupWaiterGuard;
use super::ShutdownPhaseFailure;
use super::ShutdownPhaseResult;
use crate::engine::query::ProviderCallAdmission;
use crate::engine::query::ProviderCallAdmissionReceipt;
use crate::hooks::setup::SetupContextAdmissionRecord;
use crate::hooks::setup::SetupContextAdmissionStatus;
use crate::hooks::setup::SetupLifecycleEvent;
use crate::hooks::setup::SetupLifecycleSnapshot;
use crate::hooks::setup::SetupRunRecord;
use crate::hooks::setup::SetupRunRequest;
use crate::hooks::setup::SetupRunResult;
use crate::hooks::setup::SetupRunStatus;
use crate::hooks::setup::SetupTrigger;
use crate::hooks::setup::SETUP_LIFECYCLE_ROLLOUT_EVENT;
use crate::hooks::types::HookEvent;
use crate::hooks::types::HookInput;
use crate::hooks::types::HookTrigger;
use crate::permission::permission_update::apply_add_rules;
use crate::runtime_values::runtime_codec;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
use crate::sandbox::create_sandbox_for_mode_with_engine_config;
use crate::sync_ext::{MutexExt, RwLockExt};
use crate::types::permission::ExternalPermissionMode;
use crate::types::permission::PermissionContext;
use crate::types::permission::PermissionUpdate;
use futures::future::FutureExt;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn managed_reviewer(
    value: crate::types::permission::ApprovalsReviewer,
) -> crate::engine::approval_requirements::ManagedReviewer {
    match value {
        crate::types::permission::ApprovalsReviewer::User => {
            crate::engine::approval_requirements::ManagedReviewer::User
        }
        crate::types::permission::ApprovalsReviewer::Automatic => {
            crate::engine::approval_requirements::ManagedReviewer::AutoReview
        }
    }
}

fn runtime_reviewer(
    value: crate::engine::approval_requirements::ManagedReviewer,
) -> crate::types::permission::ApprovalsReviewer {
    match value {
        crate::engine::approval_requirements::ManagedReviewer::User => {
            crate::types::permission::ApprovalsReviewer::User
        }
        crate::engine::approval_requirements::ManagedReviewer::AutoReview => {
            crate::types::permission::ApprovalsReviewer::Automatic
        }
    }
}

fn runtime_approval_policy(
    value: crate::engine::approval_requirements::ManagedApprovalPolicy,
) -> crate::types::permission::ApprovalPolicy {
    use crate::engine::approval_requirements::ManagedApprovalPolicy;
    use crate::types::permission::{ApprovalPolicy, GranularApprovalConfig};
    match value {
        ManagedApprovalPolicy::OnRequest => ApprovalPolicy::OnRequest,
        ManagedApprovalPolicy::Never => ApprovalPolicy::Never,
        ManagedApprovalPolicy::UnlessTrusted => ApprovalPolicy::UnlessTrusted,
        ManagedApprovalPolicy::Granular {
            sandbox_approval,
            rules,
            skill_approval,
            request_permissions,
            mcp_elicitations,
        } => ApprovalPolicy::Granular(GranularApprovalConfig {
            sandbox_approval,
            rules,
            skill_approval,
            request_permissions,
            mcp_elicitations,
        }),
    }
}

pub(super) fn apply_startup_review_requirements(
    config: &mut crate::schemas::config::ResolvedConfig,
    requirements: &crate::engine::approval_requirements::ManagedReviewRequirements,
) -> std::io::Result<()> {
    use crate::types::permission::{ApprovalPolicy, ApprovalsReviewer, SandboxMode};
    let initial_mode = config.sandbox_mode.unwrap_or(SandboxMode::ReadOnly);
    config.sandbox_mode = Some(initial_mode);
    let mut policy = config.approval_policy.unwrap_or_default();
    if let Err(error) = requirements.validate_approval_policy(policy) {
        let preferred = requirements.preferred_approval_policy().ok_or(error)?;
        policy = runtime_approval_policy(preferred);
        requirements.validate_approval_policy(policy)?;
        tracing::warn!(
            ?policy,
            "Configured approval policy is disallowed; using the managed default"
        );
        config.approval_policy = Some(policy);
    }
    let mut reviewer = config.approvals_reviewer.unwrap_or(ApprovalsReviewer::User);
    if let Err(error) = requirements.validate_reviewer(managed_reviewer(reviewer)) {
        reviewer = runtime_reviewer(requirements.preferred_reviewer().ok_or(error)?);
        requirements.validate_reviewer(managed_reviewer(reviewer))?;
        tracing::warn!(
            ?reviewer,
            "Configured approvals reviewer is disallowed; using the managed default"
        );
        config.approvals_reviewer = Some(reviewer);
    }
    if let Err(error) = requirements.validate_sandbox_mode(initial_mode) {
        requirements.validate_sandbox_mode(SandboxMode::ReadOnly)?;
        if initial_mode == SandboxMode::DangerFullAccess && policy == ApprovalPolicy::Never {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput,
                "Approval policy never cannot be used when managed requirements replace full access with read-only permissions."));
        }
        tracing::warn!(%error, "Configured sandbox mode is disallowed; using read-only permissions");
        config.sandbox_mode = Some(SandboxMode::ReadOnly);
    }
    let model = String::from_utf8(config.model.to_node_utf8()).expect("Node text is UTF-8");
    if requirements.requires_review_for_model(&model) {
        if config.sandbox_mode == Some(SandboxMode::DangerFullAccess) {
            requirements.validate_sandbox_mode(SandboxMode::WorkspaceWrite)?;
            config.sandbox_mode = Some(SandboxMode::WorkspaceWrite);
            config.sandbox_workspace_write.network_access = false;
        }
        requirements.validate_reviewer(managed_reviewer(ApprovalsReviewer::Automatic))?;
        config.approvals_reviewer = Some(ApprovalsReviewer::Automatic);
    }
    Ok(())
}

pub(super) fn validate_review_requirements(
    requirements: &crate::engine::approval_requirements::ManagedReviewRequirements,
    policy: &RuntimePolicyState,
    model: &crate::runtime_values::RuntimeString,
) -> Result<(), String> {
    use crate::types::permission::{ApprovalsReviewer, SandboxMode};
    let reviewer = policy.approvals_reviewer.unwrap_or(ApprovalsReviewer::User);
    let approval = policy.approval_policy.unwrap_or_default();
    let sandbox = policy.sandbox_mode.unwrap_or(SandboxMode::ReadOnly);
    requirements
        .validate_reviewer(managed_reviewer(reviewer))
        .map_err(|error| error.to_string())?;
    requirements
        .validate_approval_policy(approval)
        .map_err(|error| error.to_string())?;
    requirements
        .validate_sandbox_mode(sandbox)
        .map_err(|error| error.to_string())?;
    if requirements.requires_review_for_model(
        &String::from_utf8(model.to_node_utf8()).expect("Node text is UTF-8"),
    ) && (reviewer != ApprovalsReviewer::Automatic || sandbox == SandboxMode::DangerFullAccess)
    {
        return Err(
            "The selected model requires automatic approval review in a sandboxed workspace."
                .into(),
        );
    }
    Ok(())
}

// Permission evaluation and Setup-hook admission.
impl Session {
    // ---- permission ----

    pub(super) fn resolve_managed_step_policy(
        &self,
        requirements: &crate::engine::approval_requirements::ManagedReviewRequirements,
        model: &RuntimeString,
    ) -> Result<RuntimePolicyState, String> {
        let mut policy = self.runtime_policy_state();
        let published_model = self.inference_snapshot().config.model.clone();
        if model != &published_model
            && requirements.requires_review_for_model(
                &String::from_utf8(model.to_node_utf8()).expect("Node text is UTF-8"),
            )
        {
            policy.approvals_reviewer =
                Some(crate::types::permission::ApprovalsReviewer::Automatic);
        }
        validate_review_requirements(requirements, &policy, model)?;
        Ok(policy)
    }

    pub(crate) fn runtime_policy_snapshot(&self) -> Arc<RuntimePolicySnapshot> {
        self.runtime_policy.read_recover().clone()
    }

    pub fn runtime_policy_state(&self) -> RuntimePolicyState {
        self.runtime_policy_snapshot().state()
    }

    pub fn set_runtime_policy(
        &self,
        mut update: RuntimePolicyUpdate,
    ) -> Result<RuntimePolicyCommit, String> {
        let inference = self.inference_snapshot();
        let selection = self
            .pending_model()
            .unwrap_or_else(|| inference.config.model.clone());
        let selected = inference.for_selection(&selection)?;
        let requirements = selected
            .load_review_requirements_sync()
            .map_err(|error| error.to_string())?;
        let current_inference = self.inference.read_recover();
        if !Arc::ptr_eq(&current_inference, &inference) {
            return Err("Model changed while runtime policy requirements were prepared.".into());
        }
        let mut current = self.runtime_policy.write_recover();
        let previous = current.state();
        update.approval_policy = update.approval_policy.or(previous.approval_policy);
        update.sandbox_mode = update.sandbox_mode.or(previous.sandbox_mode);
        update.approvals_reviewer = update.approvals_reviewer.or(previous.approvals_reviewer);
        if self.config.task_resource_scope.is_some()
            && update.sandbox_mode == Some(crate::types::permission::SandboxMode::DangerFullAccess)
        {
            update.sandbox_mode = Some(crate::types::permission::SandboxMode::WorkspaceWrite);
        }
        let workspace_changed = update.sandbox_workspace_write.is_some();
        let mut workspace = update
            .sandbox_workspace_write
            .unwrap_or_else(|| previous.sandbox_workspace_write.clone());
        workspace.normalize_writable_roots(None)?;
        let network_override = update.network_allowed.or_else(|| {
            (update.sandbox_mode == previous.sandbox_mode
                && (!workspace_changed
                    || update.sandbox_mode
                        == Some(crate::types::permission::SandboxMode::ReadOnly)))
            .then_some(previous.network_allowed)
        });
        let network_allowed =
            super::runtime_network_allowed(update.sandbox_mode, network_override, &workspace);
        let candidate = RuntimePolicyState {
            approval_policy: update.approval_policy,
            sandbox_mode: update.sandbox_mode,
            approvals_reviewer: update.approvals_reviewer,
            generation: previous.generation.saturating_add(1),
            sandbox_workspace_write: workspace,
            network_allowed,
        };
        validate_review_requirements(&requirements, &candidate, &selected.config.model)?;
        let native_config_home = self.user_config_dir.clone().unwrap_or_else(|| {
            crate::runtime_values::RuntimePath::from(crate::fs::paths::config_home())
        });
        let sandbox_backend = if let Some(captured) = &self.required_network_sandbox {
            captured.clone()
        } else {
            let mode = candidate
                .sandbox_mode
                .unwrap_or(crate::types::permission::SandboxMode::ReadOnly);
            let executor_mode = if self.config.task_resource_scope.is_some()
                && mode == crate::types::permission::SandboxMode::DangerFullAccess
            {
                crate::types::permission::SandboxMode::WorkspaceWrite
            } else {
                mode
            };
            create_sandbox_for_mode_with_engine_config(
                executor_mode,
                &self.config,
                Some(&native_config_home),
            )?
        };
        let sandbox_backend: Arc<dyn crate::types::sandbox::SandboxBackend> =
            if self.config.task_resource_scope.is_some()
                && sandbox_backend.task_resource_scope().is_none()
            {
                let scope = self
                    .permission_engine
                    .bound_task_resource_scope()
                    .ok_or("Task resource scope is not initialized")?;
                Arc::new(crate::sandbox::task_scope::TaskScopedSandbox::new(
                    sandbox_backend,
                    scope,
                )?)
            } else {
                sandbox_backend
            };
        let effective = candidate;
        *current = Arc::new(RuntimePolicySnapshot {
            state: effective.clone(),
            sandbox_backend,
        });
        Ok(RuntimePolicyCommit {
            previous,
            effective,
        })
    }

    /// Translate a retired input at the compatibility boundary, then use the
    /// canonical update path and its managed requirements validation.
    pub(crate) fn request_permission_mode_change(
        &self,
        mode: ExternalPermissionMode,
    ) -> Result<(), String> {
        tracing::warn!("permissionMode is deprecated; use runtime policy or collaboration mode");
        if mode == ExternalPermissionMode::Plan {
            return self
                .planning
                .set_mode(crate::planning::CollaborationMode::Plan, None)
                .map_err(|error| crate::planning::runtime::host(&error));
        }
        let Some((approval_policy, sandbox_mode, approvals_reviewer)) =
            crate::schemas::config::legacy_permission_policy(mode)
        else {
            return Ok(());
        };
        self.set_runtime_policy(RuntimePolicyUpdate {
            approval_policy: Some(approval_policy),
            sandbox_mode: Some(sandbox_mode),
            approvals_reviewer: Some(approvals_reviewer),
            sandbox_workspace_write: None,
            network_allowed: None,
        })?;
        Ok(())
    }

    /// Apply a permission update to the session's live state.
    pub fn apply_permission_update(&self, update: PermissionUpdate) {
        match update {
            PermissionUpdate::SetMode { mode, .. } => {
                if let Err(error) = self.request_permission_mode_change(mode) {
                    tracing::warn!(%error, "Permission mode update rejected by managed requirements");
                }
            }
            add @ PermissionUpdate::AddRules {
                destination: crate::types::permission::PermissionDestination::Session,
                ..
            } => {
                apply_add_rules(&mut self.mode.permission.lock_recover(), &add);
            }
            add @ PermissionUpdate::AddRules { .. } => self.durable_permissions.grant_pending(add),
        }
    }

    /// The per-turn permission view (a snapshot; the live rule arrays live behind
    /// the shared `session_permission` handle threaded into query.rs).
    pub(crate) fn permission_context(&self) -> PermissionContext {
        let policy = self.runtime_policy_snapshot();
        self.permission_context_for_policy(&policy)
    }

    /// The baseline and scoped override share one policy and rule capture.
    pub(crate) fn permission_context_for_policy_with_origin(
        &self,
        policy: &RuntimePolicySnapshot,
    ) -> (PermissionContext, super::SubagentPermissionOrigin) {
        let permission = self.permission_context_for_policy(policy);
        let origin = super::SubagentPermissionOrigin::capture(&permission);
        (permission, origin)
    }

    pub(crate) fn permission_context_for_policy(
        &self,
        policy: &RuntimePolicySnapshot,
    ) -> PermissionContext {
        let sp = self.mode.permission.lock_recover();
        let mut roots: Vec<RuntimePath> = policy
            .state
            .sandbox_workspace_write
            .writable_roots
            .iter()
            .map(|root| resolve_abs(&RuntimePath::from(root.clone())))
            .collect();
        if policy.state.sandbox_mode == Some(crate::types::permission::SandboxMode::WorkspaceWrite)
            && self.config.task_resource_scope.is_none()
        {
            let process_env = crate::runtime_process::ParentEnvSnapshot::capture();
            let environment = self
                .shell_env
                .get()
                .and_then(Option::as_ref)
                .unwrap_or(&process_env);
            let resources = crate::types::sandbox::SandboxPolicy {
                writable_roots: roots
                    .iter()
                    .map(|root| root.as_runtime_string().clone())
                    .collect(),
                exclude_tmpdir_env_var: Some(
                    policy.state.sandbox_workspace_write.exclude_tmpdir_env_var,
                ),
                exclude_slash_tmp: Some(policy.state.sandbox_workspace_write.exclude_slash_tmp),
                exclude_cwd: Some(true),
                ..Default::default()
            };
            roots = crate::sandbox::policy::compute_writable_roots(
                &resources,
                &self.cwd_now(),
                if cfg!(target_os = "macos") {
                    "darwin"
                } else {
                    std::env::consts::OS
                },
                environment,
            )
            .into_iter()
            .map(RuntimePath::from)
            .collect();
        }
        if self.config.task_resource_scope.is_some() {
            roots = self
                .permission_engine
                .bound_task_resource_scope()
                .map(|scope| scope.writable_roots())
                .unwrap_or_default();
        } else if let Some(dir) = self.memory_roots.main() {
            roots.push(resolve_abs(&RuntimePath::from(dir.clone())));
        }
        PermissionContext {
            allow_rules: sp.allow_rules.clone(),
            deny_rules: sp.deny_rules.clone(),
            ask_rules: sp.ask_rules.clone(),
            additional_writable_roots: roots,
            internal_writable_roots: Vec::new(),
            pinned_memory_root: None,
            should_avoid_prompts: false,
        }
    }

    // ---- setup hooks ----

    pub(crate) fn start_pending_setup_recovery_owners(self: &Arc<Self>) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let mut registry = self.setup.durability.lock_recover();
        if registry.closing {
            return;
        }
        let pending = registry
            .receipts
            .values()
            .filter_map(|receipt| {
                (receipt.selected.is_none() && receipt.receive.borrow().is_none())
                    .then(|| receipt.terminal_record.clone())
                    .flatten()
            })
            .collect::<Vec<_>>();
        for terminal in pending {
            self.register_setup_terminal_owner_locked(&mut registry, terminal);
        }
        let pending_context = registry
            .context_recovery_receipt_ids
            .iter()
            .filter_map(|admission_id| {
                registry
                    .context_admission_receipts
                    .get(admission_id)
                    .filter(|receipt| {
                        !receipt.owner_registered && receipt.terminal_receive.borrow().is_none()
                    })
                    .and_then(|receipt| receipt.terminal_record.clone())
                    .map(|terminal| (admission_id.clone(), terminal))
            })
            .collect::<Vec<_>>();
        for (admission_id, terminal) in pending_context {
            self.register_setup_context_recovery_owner_locked(
                &mut registry,
                admission_id,
                terminal,
            );
        }
    }

    /// Await only process-local receipts for Setup terminals synthesized from
    /// constructor replay. The immutable id list excludes every subsequent
    /// live Setup request, and failures are aggregated after all receipts have
    /// settled so one failed owner cannot abandon another.
    pub(crate) async fn await_pending_setup_recovery_receipts(
        self: &Arc<Self>,
    ) -> Result<(), RuntimeString> {
        self.start_pending_setup_recovery_owners();
        let (receipts, context_receipts) = {
            let registry =
                self.setup.durability.lock().map_err(|_| {
                    RuntimeString::from("Setup recovery receipt registry was poisoned")
                })?;
            let receipts = registry
                .recovery_receipt_ids
                .iter()
                .map(|request_id| {
                    (
                        request_id.clone(),
                        registry
                            .receipts
                            .get(request_id)
                            .map(|receipt| receipt.receive.clone()),
                    )
                })
                .collect::<Vec<_>>();
            let context_receipts = registry
                .context_recovery_receipt_ids
                .iter()
                .map(|admission_id| {
                    (
                        admission_id.clone(),
                        registry
                            .context_admission_receipts
                            .get(admission_id)
                            .map(|receipt| receipt.terminal_receive.clone()),
                    )
                })
                .collect::<Vec<_>>();
            (receipts, context_receipts)
        };

        let mut failures = Vec::new();
        for (request_id, receipt) in receipts {
            let Some(mut receipt) = receipt else {
                failures.push(format!(
                    "{request_id:?}: constructor-synthesized Setup recovery receipt is missing"
                ));
                continue;
            };
            loop {
                let result = { receipt.borrow().clone() };
                if let Some(result) = result {
                    if result.status == SetupRunStatus::PersistenceFailed {
                        failures.push(format!(
                            "{request_id:?}: {}",
                            result
                                .error
                                .as_ref()
                                .map(|error| error
                                    .try_to_utf8()
                                    .unwrap_or_else(|_| format!("{error:?}")))
                                .unwrap_or_else(|| {
                                    "Setup recovery terminal persistence failed".to_string()
                                })
                        ));
                    }
                    break;
                }
                if receipt.changed().await.is_err() {
                    failures.push(format!(
                        "{request_id:?}: Session-owned Setup recovery durability task ended without a verdict"
                    ));
                    break;
                }
            }
        }
        for (admission_id, receipt) in context_receipts {
            let Some(mut receipt) = receipt else {
                failures.push(format!(
                    "{admission_id:?}: constructor-synthesized Setup context recovery receipt is missing"
                ));
                continue;
            };
            loop {
                if let Some(result) = receipt.borrow().clone() {
                    if let Err(error) = result {
                        failures.push(format!("{admission_id:?}: {error:?}"));
                    }
                    break;
                }
                if receipt.changed().await.is_err() {
                    failures.push(format!(
                        "{admission_id:?}: Session-owned Setup context recovery task ended without a verdict"
                    ));
                    break;
                }
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(RuntimeString::from(format!(
                "Setup recovery durability failed: {}",
                failures.join("; ")
            )))
        }
    }

    pub(crate) fn setup_persistence_failed_record(
        &self,
        terminal: &SetupRunRecord,
        error: impl std::fmt::Display,
    ) -> SetupRunRecord {
        let mut failed = terminal.clone();
        failed.status = SetupRunStatus::PersistenceFailed;
        failed.additional_context.clear();
        failed.error = Some(RuntimeString::from(format!(
            "failed to persist Setup terminal state before replying to the Host: {error}"
        )));
        failed
    }

    pub(crate) async fn finish_setup_terminal_durability(
        self: Arc<Self>,
        terminal: SetupRunRecord,
    ) -> SetupRunResult {
        let appended = self
            .record_setup_event(SetupLifecycleEvent::Run {
                run: terminal.clone(),
            })
            .await;
        #[cfg(test)]
        if appended.is_ok() {
            maybe_inject_setup_owned_task_panic(
                &terminal.request_id,
                SetupOwnedTaskKind::Durability,
            );
        }
        let durable = match appended {
            Ok(()) => self
                .flush_setup_rollout()
                .await
                .map_err(RuntimeString::from),
            Err(error) => Err(error),
        };
        match durable {
            Ok(()) => SetupRunResult::from_record(&terminal),
            Err(error) => {
                let failed = self.transition_setup_receipt_to_persistence_failed(
                    &terminal.request_id,
                    &terminal,
                    format!("{error:?}"),
                );
                let diagnostic = {
                    self.setup
                        .durability
                        .lock_recover()
                        .receipts
                        .get(&terminal.request_id)
                        .and_then(|receipt| receipt.terminal_record.clone())
                };
                if let Some(record) = diagnostic {
                    let _ = self
                        .record_setup_event(SetupLifecycleEvent::Run { run: record })
                        .await;
                }
                let _ = self.flush_setup_rollout().await;
                failed
            }
        }
    }

    pub(crate) fn setup_receipt_failure_result(
        source: &SetupRunResult,
        error: impl std::fmt::Display,
    ) -> SetupRunResult {
        let mut failed = source.clone();
        failed.status = SetupRunStatus::PersistenceFailed;
        failed.additional_context.clear();
        failed.error = Some(RuntimeString::from(format!(
            "Setup terminal durability receipt failed closed: {error}"
        )));
        failed
    }

    pub(crate) fn spawn_setup_owned_task_locked<F>(
        self: &Arc<Self>,
        registry: &mut SetupDurabilityRegistry,
        request_id: RuntimeString,
        kind: SetupOwnedTaskKind,
        work: F,
    ) where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let task_id = registry.next_task_id;
        registry.next_task_id = registry.next_task_id.wrapping_add(1);
        let (start_send, start_receive) = tokio::sync::oneshot::channel::<()>();
        let task_session = self.clone();
        let panic_request_id = request_id.clone();
        let panic_kind = kind;
        let task = registry.task_tracker.spawn(async move {
            let _registration = SetupOwnedTaskRegistration {
                session: Arc::downgrade(&task_session),
                task_id,
            };
            if start_receive.await.is_err() {
                return;
            }
            let guarded = async {
                #[cfg(test)]
                if panic_kind != SetupOwnedTaskKind::Durability {
                    maybe_inject_setup_owned_task_panic(&panic_request_id, panic_kind);
                }
                work.await;
            };
            let outcome = std::panic::AssertUnwindSafe(guarded).catch_unwind().await;
            if outcome.is_err() {
                match panic_kind {
                    SetupOwnedTaskKind::Attempt => {
                        task_session.settle_setup_attempt_after_owner_loss(&panic_request_id);
                    }
                    SetupOwnedTaskKind::Durability => {
                        task_session
                            .fail_setup_receipt_after_owner_panic(&panic_request_id)
                            .await;
                    }
                    SetupOwnedTaskKind::InitClose => {
                        task_session.fail_setup_init_close_after_owner_panic();
                    }
                    SetupOwnedTaskKind::ContextAdmission => {
                        task_session
                            .fail_setup_context_receipt_after_owner_panic(&panic_request_id)
                            .await;
                    }
                }
                if let Ok(mut registry) = task_session.setup.durability.lock() {
                    registry.failures.push(format!(
                        "Session-owned Setup {panic_kind:?} task panicked for request {panic_request_id:?}"
                    ));
                }
            }
        });
        let abort = task.abort_handle();
        registry
            .tasks
            .insert(task_id, SetupOwnedTaskControl { kind, abort });
        // The task cannot finish/remove its control until registration is
        // visible. TaskTracker, rather than this disposable JoinHandle, is the
        // cancellation-safe join owner.
        let _ = start_send.send(());
        drop(task);
    }

    pub(crate) fn publish_setup_receipt(&self, request_id: &RuntimeString, result: SetupRunResult) {
        let Ok(mut registry) = self.setup.durability.lock() else {
            return;
        };
        let Some(receipt) = registry.receipts.get_mut(request_id) else {
            return;
        };
        if receipt.receive.borrow().is_some() {
            return;
        }
        receipt.selected = Some(result.clone());
        let _ = receipt.send.send(Some(result));
    }

    pub(crate) async fn fail_setup_receipt_after_owner_panic(&self, request_id: &RuntimeString) {
        let terminal = self.setup.durability.lock().ok().and_then(|registry| {
            registry.receipts.get(request_id).and_then(|receipt| {
                if receipt.receive.borrow().is_some() {
                    None
                } else {
                    receipt.terminal_record.clone().or_else(|| {
                        registry
                            .active
                            .get(request_id)
                            .map(|active| active.run.clone())
                    })
                }
            })
        });
        let Some(terminal) = terminal else {
            return;
        };
        let failed = self.transition_setup_receipt_to_persistence_failed(
            request_id,
            &terminal,
            "Session-owned Setup durability task panicked unexpectedly",
        );
        let diagnostic = {
            let mut registry = self.setup.durability.lock_recover();
            registry.active.remove(request_id);
            if let Some(receipt) = registry.receipts.get_mut(request_id) {
                if receipt.started_receive.borrow().is_none() {
                    let _ = receipt.started_send.send(Some(Err(RuntimeString::from(
                        "Session-owned Setup durability task panicked unexpectedly",
                    ))));
                }
            }
            registry
                .receipts
                .get(request_id)
                .and_then(|receipt| receipt.terminal_record.clone())
        };
        if let Some(run) = diagnostic {
            let _ = self
                .record_setup_event(SetupLifecycleEvent::Run { run })
                .await;
        }
        let _ = self.flush_setup_rollout().await;
        self.publish_setup_receipt(request_id, failed);
    }

    pub(crate) fn publish_setup_init_close_receipt(&self, result: Result<(), RuntimeString>) {
        let Ok(mut registry) = self.setup.durability.lock() else {
            return;
        };
        let Some(receipt) = registry.init_close_receipt.as_mut() else {
            return;
        };
        if receipt.receive.borrow().is_some() {
            return;
        }
        let _ = receipt.send.send(Some(result));
    }

    pub(crate) fn fail_setup_init_close_after_owner_panic(&self) {
        self.publish_setup_init_close_receipt(Err(RuntimeString::from(
            "Session-owned Setup InitClosed durability task panicked unexpectedly",
        )));
    }

    pub(crate) fn setup_context_persistence_failed_record(
        &self,
        source: &SetupContextAdmissionRecord,
        error: impl std::fmt::Display,
    ) -> SetupContextAdmissionRecord {
        let mut failed = source.clone();
        failed.status = SetupContextAdmissionStatus::PersistenceFailed;
        failed.ended_at = failed.ended_at.or_else(|| Some((self.deps.now)()));
        failed.error = Some(RuntimeString::from(format!(
            "failed to persist Setup context admission before crossing its provider boundary: {error}"
        )));
        failed
    }

    pub(crate) fn publish_setup_context_started(
        &self,
        admission_id: &RuntimeString,
        result: Result<(), RuntimeString>,
    ) {
        let Ok(mut registry) = self.setup.durability.lock() else {
            return;
        };
        let Some(receipt) = registry.context_admission_receipts.get_mut(admission_id) else {
            return;
        };
        if receipt.started_receive.borrow().is_none() {
            let _ = receipt.started_send.send(Some(result));
        }
    }

    pub(crate) fn publish_setup_context_terminal(
        &self,
        admission_id: &RuntimeString,
        result: Result<(), RuntimeString>,
    ) {
        let Ok(mut registry) = self.setup.durability.lock() else {
            return;
        };
        let Some(receipt) = registry.context_admission_receipts.get_mut(admission_id) else {
            return;
        };
        if receipt.terminal_receive.borrow().is_none() {
            let _ = receipt.terminal_send.send(Some(result));
        }
    }

    pub(crate) fn setup_context_terminal_error(
        record: &SetupContextAdmissionRecord,
    ) -> RuntimeString {
        record.error.clone().unwrap_or_else(|| {
            RuntimeString::from(match record.status {
                SetupContextAdmissionStatus::Released => {
                    "Setup context claim was released before provider admission"
                }
                SetupContextAdmissionStatus::OutcomeUnknown => {
                    "Setup context provider-call outcome is unknown"
                }
                SetupContextAdmissionStatus::PersistenceFailed => {
                    "Setup context admission persistence failed"
                }
                SetupContextAdmissionStatus::Started => "Setup context admission is still pending",
                SetupContextAdmissionStatus::Consumed => {
                    "Setup context admission unexpectedly reported an error"
                }
            })
        })
    }

    pub(crate) fn setup_context_provider_receipt(
        self: &Arc<Self>,
        admission_id: &RuntimeString,
    ) -> ProviderCallAdmissionReceipt {
        let receipt = self.setup.durability.lock().ok().and_then(|registry| {
            registry
                .context_admission_receipts
                .get(admission_id)
                .map(|receipt| {
                    (
                        receipt.terminal_receive.clone(),
                        receipt.terminal_record_handle.clone(),
                    )
                })
        });
        let Some((mut receiver, terminal_record)) = receipt else {
            return Box::pin(async {
                Err(RuntimeString::from(
                    "Setup context admission durability receipt is missing",
                ))
            });
        };
        Box::pin(async move {
            loop {
                if let Some(result) = receiver.borrow().clone() {
                    result?;
                    break;
                }
                if receiver.changed().await.is_err() {
                    return Err(RuntimeString::from(
                        "Session-owned Setup context admission task ended without a verdict",
                    ));
                }
            }
            let record = terminal_record
                .lock()
                .map_err(|_| RuntimeString::from("Setup context terminal record was poisoned"))?
                .clone()
                .ok_or_else(|| {
                    RuntimeString::from("Setup context terminal receipt has no durable record")
                })?;
            if record.status == SetupContextAdmissionStatus::Consumed {
                Ok(())
            } else {
                Err(Self::setup_context_terminal_error(&record))
            }
        })
    }

    pub(crate) fn setup_context_before_provider_call(
        &self,
        admission_id: &RuntimeString,
    ) -> Result<(), RuntimeString> {
        let mut registry = self
            .setup
            .durability
            .lock()
            .map_err(|_| RuntimeString::from("Setup durability registry was poisoned"))?;
        let closing = registry.closing
            || self.lifecycle.is_disposed()
            || self.setup.persistence.finalized.load(Ordering::SeqCst);
        if let Some(active) = registry.active_context_admission.as_mut() {
            if &active.record.admission_id != admission_id {
                return Err("a different Setup context claim owns provider admission".into());
            }
            if closing {
                active.phase = match active.phase {
                    SetupContextProviderPhase::Prepared => {
                        SetupContextProviderPhase::ReleaseRequested
                    }
                    SetupContextProviderPhase::Calling => {
                        SetupContextProviderPhase::OutcomeUnknownRequested
                    }
                    phase => phase,
                };
                active.changed.notify_one();
                return Err("session shutdown closed Setup context provider admission".into());
            }
            return match active.phase {
                SetupContextProviderPhase::Prepared => {
                    active.phase = SetupContextProviderPhase::Calling;
                    active.changed.notify_one();
                    Ok(())
                }
                SetupContextProviderPhase::Calling | SetupContextProviderPhase::Admitted => Ok(()),
                SetupContextProviderPhase::ReleaseRequested => {
                    Err("Setup context claim was released before provider admission".into())
                }
                SetupContextProviderPhase::OutcomeUnknownRequested => {
                    Err("Setup context provider-call outcome is unknown".into())
                }
            };
        }
        let terminal = registry
            .context_admission_receipts
            .get(admission_id)
            .and_then(|receipt| receipt.terminal_record.as_ref());
        match terminal {
            Some(record) if record.status == SetupContextAdmissionStatus::Consumed => Ok(()),
            Some(record) => Err(Self::setup_context_terminal_error(record)),
            None => Err("Setup context admission is no longer registered".into()),
        }
    }

    pub(crate) fn setup_context_provider_call_returned(
        self: &Arc<Self>,
        admission_id: &RuntimeString,
    ) -> ProviderCallAdmissionReceipt {
        if let Ok(mut registry) = self.setup.durability.lock() {
            if let Some(active) = registry.active_context_admission.as_mut() {
                if &active.record.admission_id == admission_id {
                    active.phase = match active.phase {
                        SetupContextProviderPhase::Calling => SetupContextProviderPhase::Admitted,
                        SetupContextProviderPhase::Prepared => {
                            SetupContextProviderPhase::OutcomeUnknownRequested
                        }
                        phase => phase,
                    };
                    active.changed.notify_one();
                }
            }
        }
        self.setup_context_provider_receipt(admission_id)
    }

    pub(crate) fn setup_context_provider_call_panicked(
        self: &Arc<Self>,
        admission_id: &RuntimeString,
    ) -> ProviderCallAdmissionReceipt {
        if let Ok(mut registry) = self.setup.durability.lock() {
            if let Some(active) = registry.active_context_admission.as_mut() {
                if &active.record.admission_id == admission_id
                    && matches!(
                        active.phase,
                        SetupContextProviderPhase::Prepared | SetupContextProviderPhase::Calling
                    )
                {
                    active.phase = SetupContextProviderPhase::OutcomeUnknownRequested;
                    active.changed.notify_one();
                }
            }
        }
        self.setup_context_provider_receipt(admission_id)
    }

    pub(crate) fn setup_context_provider_lease_dropped(&self, admission_id: &RuntimeString) {
        let Ok(mut registry) = self.setup.durability.lock() else {
            return;
        };
        if let Some(receipt) = registry.context_admission_receipts.get_mut(admission_id) {
            receipt.provider_lease_released = true;
        }
        let Some(active) = registry.active_context_admission.as_mut() else {
            return;
        };
        if &active.record.admission_id != admission_id {
            return;
        }
        active.phase = match active.phase {
            SetupContextProviderPhase::Prepared => SetupContextProviderPhase::ReleaseRequested,
            SetupContextProviderPhase::Calling => {
                SetupContextProviderPhase::OutcomeUnknownRequested
            }
            phase => phase,
        };
        active.changed.notify_one();
    }

    pub(crate) fn setup_context_terminal_from_phase(
        &self,
        active: &ActiveSetupContextAdmission,
    ) -> Option<SetupContextAdmissionRecord> {
        let (status, error) = match active.phase {
            SetupContextProviderPhase::Admitted => (SetupContextAdmissionStatus::Consumed, None),
            SetupContextProviderPhase::ReleaseRequested => (
                SetupContextAdmissionStatus::Released,
                Some(RuntimeString::from(
                    "Setup context claim ended before a provider request was admitted",
                )),
            ),
            SetupContextProviderPhase::OutcomeUnknownRequested => (
                SetupContextAdmissionStatus::OutcomeUnknown,
                Some(RuntimeString::from(
                    "Setup context delivery outcome is unknown after the provider-call boundary was entered",
                )),
            ),
            SetupContextProviderPhase::Prepared | SetupContextProviderPhase::Calling => return None,
        };
        let mut terminal = active.record.clone();
        terminal.status = status;
        terminal.ended_at = Some((self.deps.now)());
        terminal.error = error;
        Some(terminal)
    }

    pub(crate) fn select_setup_context_terminal(
        &self,
        admission_id: &RuntimeString,
    ) -> Result<(SetupContextAdmissionRecord, bool), RuntimeString> {
        let mut registry = self
            .setup
            .durability
            .lock()
            .map_err(|_| RuntimeString::from("Setup durability registry was poisoned"))?;
        let Some(active) = registry.active_context_admission.as_ref() else {
            let terminal = registry
                .context_admission_receipts
                .get(admission_id)
                .and_then(|receipt| receipt.terminal_record.clone())
                .ok_or_else(|| RuntimeString::from("Setup context admission owner is missing"))?;
            return Ok((terminal, false));
        };
        if &active.record.admission_id != admission_id {
            return Err("a different Setup context admission owns the serial permit".into());
        }
        let terminal = self
            .setup_context_terminal_from_phase(active)
            .ok_or_else(|| RuntimeString::from("Setup context terminal phase is not selected"))?;
        registry.active_context_admission = None;

        let receipt = registry
            .context_admission_receipts
            .get_mut(admission_id)
            .ok_or_else(|| RuntimeString::from("Setup context durability receipt is missing"))?;
        receipt.set_terminal_record(terminal.clone());
        Ok((terminal, true))
    }

    pub(crate) async fn transition_setup_context_to_persistence_failed(
        &self,
        admission_id: &RuntimeString,
        source: &SetupContextAdmissionRecord,
        error: impl std::fmt::Display,
    ) -> (SetupContextAdmissionRecord, bool) {
        let failed = self.setup_context_persistence_failed_record(source, error);
        {
            let Ok(mut registry) = self.setup.durability.lock() else {
                return (failed, false);
            };
            registry.active_context_admission = None;
            if let Some(receipt) = registry.context_admission_receipts.get_mut(admission_id) {
                receipt.set_terminal_record(failed.clone());
            }
        }
        let appended = self
            .record_setup_event(SetupLifecycleEvent::ContextAdmission {
                admission: failed.clone(),
            })
            .await
            .is_ok();
        (failed, appended)
    }

    pub(crate) async fn finish_setup_context_terminal_durability(
        self: &Arc<Self>,
        admission_id: &RuntimeString,
        terminal: SetupContextAdmissionRecord,
        selected: bool,
    ) -> Result<(), RuntimeString> {
        if !selected {
            return Err(Self::setup_context_terminal_error(&terminal));
        }
        let appended = self
            .record_setup_event(SetupLifecycleEvent::ContextAdmission {
                admission: terminal.clone(),
            })
            .await;
        let durable = match appended {
            Ok(()) => self
                .flush_setup_rollout()
                .await
                .map_err(RuntimeString::from),
            Err(error) => Err(error),
        };
        match durable {
            Ok(()) if terminal.status != SetupContextAdmissionStatus::PersistenceFailed => Ok(()),
            Ok(()) => Err(Self::setup_context_terminal_error(&terminal)),
            Err(error) => {
                let (failed, appended) = self
                    .transition_setup_context_to_persistence_failed(
                        admission_id,
                        &terminal,
                        format!("{error:?}"),
                    )
                    .await;
                if appended {
                    let _ = self.flush_setup_rollout().await;
                }
                Err(Self::setup_context_terminal_error(&failed))
            }
        }
    }

    pub(crate) async fn run_setup_context_admission_owner(
        self: Arc<Self>,
        admission_id: RuntimeString,
        serial: tokio::sync::OwnedMutexGuard<()>,
    ) {
        let _serial = serial;
        let source = self
            .setup
            .durability
            .lock_recover()
            .active_context_admission
            .as_ref()
            .filter(|active| active.record.admission_id == admission_id)
            .map(|active| active.record.clone());
        let started = match source {
            Some(record) => {
                self.record_setup_event(SetupLifecycleEvent::ContextAdmission { admission: record })
                    .await
            }
            None => Err(RuntimeString::from(
                "Setup context Started lost its actual operation owner",
            )),
        };
        let started = match started {
            Ok(()) => self
                .flush_setup_rollout()
                .await
                .map_err(RuntimeString::from),
            Err(error) => Err(error),
        };
        if let Err(error) = started {
            let source = self
                .setup
                .durability
                .lock()
                .ok()
                .and_then(|registry| registry.active_context_admission.clone())
                .filter(|active| active.record.admission_id == admission_id)
                .map(|active| active.record);
            let Some(source) = source else {
                let error = RuntimeString::from(format!(
                    "Setup context Started flush failed after its registered owner disappeared: {error:?}"
                ));
                self.publish_setup_context_started(&admission_id, Err(error.clone()));
                self.publish_setup_context_terminal(&admission_id, Err(error));
                return;
            };
            let (failed, appended) = self
                .transition_setup_context_to_persistence_failed(
                    &admission_id,
                    &source,
                    format!("{error:?}"),
                )
                .await;
            if appended {
                let _ = self.flush_setup_rollout().await;
            }
            let error = Self::setup_context_terminal_error(&failed);
            self.publish_setup_context_started(&admission_id, Err(error.clone()));
            self.publish_setup_context_terminal(&admission_id, Err(error));
            return;
        }
        self.publish_setup_context_started(&admission_id, Ok(()));

        loop {
            let (phase, changed) = {
                let Ok(registry) = self.setup.durability.lock() else {
                    self.publish_setup_context_terminal(
                        &admission_id,
                        Err("Setup durability registry was poisoned".into()),
                    );
                    return;
                };
                let Some(active) = registry.active_context_admission.as_ref() else {
                    return;
                };
                if active.record.admission_id != admission_id {
                    return;
                }
                (active.phase, active.changed.clone())
            };
            if matches!(
                phase,
                SetupContextProviderPhase::Admitted
                    | SetupContextProviderPhase::ReleaseRequested
                    | SetupContextProviderPhase::OutcomeUnknownRequested
            ) {
                break;
            }
            changed.notified().await;
        }

        let result = match self.select_setup_context_terminal(&admission_id) {
            Ok((terminal, appended)) => {
                self.finish_setup_context_terminal_durability(&admission_id, terminal, appended)
                    .await
            }
            Err(error) => Err(error),
        };
        self.publish_setup_context_terminal(&admission_id, result);
    }

    pub(crate) fn register_setup_context_recovery_owner_locked(
        self: &Arc<Self>,
        registry: &mut SetupDurabilityRegistry,
        admission_id: RuntimeString,
        terminal: SetupContextAdmissionRecord,
    ) {
        let Some(receipt) = registry.context_admission_receipts.get_mut(&admission_id) else {
            return;
        };
        if receipt.owner_registered || receipt.terminal_receive.borrow().is_some() {
            return;
        }
        receipt.owner_registered = true;
        let session = self.clone();
        self.spawn_setup_owned_task_locked(
            registry,
            admission_id.clone(),
            SetupOwnedTaskKind::ContextAdmission,
            async move {
                let _serial = session
                    .setup
                    .context_admission_serial
                    .clone()
                    .lock_owned()
                    .await;
                let result = session
                    .finish_setup_context_terminal_durability(&admission_id, terminal, true)
                    .await;
                session.publish_setup_context_terminal(&admission_id, result);
            },
        );
    }

    pub(crate) async fn fail_setup_context_receipt_after_owner_panic(
        self: &Arc<Self>,
        admission_id: &RuntimeString,
    ) {
        let source = self.setup.durability.lock().ok().and_then(|registry| {
            registry
                .active_context_admission
                .as_ref()
                .filter(|active| &active.record.admission_id == admission_id)
                .map(|active| active.record.clone())
                .or_else(|| {
                    registry
                        .context_admission_receipts
                        .get(admission_id)
                        .and_then(|receipt| receipt.terminal_record.clone())
                })
        });
        let Some(source) = source else {
            return;
        };
        let (failed, appended) = self
            .transition_setup_context_to_persistence_failed(
                admission_id,
                &source,
                "Session-owned Setup context admission task panicked unexpectedly",
            )
            .await;
        if appended {
            let _ = self.flush_setup_rollout().await;
        }
        let error = Self::setup_context_terminal_error(&failed);
        self.publish_setup_context_started(admission_id, Err(error.clone()));
        self.publish_setup_context_terminal(admission_id, Err(error));
    }

    pub(crate) fn transition_setup_receipt_to_persistence_failed(
        &self,
        request_id: &RuntimeString,
        terminal: &SetupRunRecord,
        error: impl std::fmt::Display,
    ) -> SetupRunResult {
        let mut registry = match self.setup.durability.lock() {
            Ok(registry) => registry,
            Err(_) => {
                return SetupRunResult::from_record(
                    &self.setup_persistence_failed_record(terminal, error),
                );
            }
        };
        if let Some(result) = registry
            .receipts
            .get(request_id)
            .and_then(|receipt| receipt.receive.borrow().clone())
        {
            return result;
        }
        let failed = self.setup_persistence_failed_record(terminal, error);
        // The live receipt must fail closed even if the foreign append adapter
        // rejects or panics while writing the diagnostic marker.
        if let Some(receipt) = registry.receipts.get_mut(request_id) {
            receipt.selected = Some(SetupRunResult::from_record(&failed));
            receipt.terminal_record = Some(failed.clone());
        }
        SetupRunResult::from_record(&failed)
    }

    /// Caller holds `setup_durability`. The full terminal publication boundary
    /// is one registry critical section: lifecycle/event append, receipt
    /// creation, retained task spawn, and JoinHandle registration. Shutdown can
    /// therefore never snapshot tasks between those operations.
    pub(crate) fn register_setup_terminal_owner_locked(
        self: &Arc<Self>,
        registry: &mut SetupDurabilityRegistry,
        terminal: SetupRunRecord,
    ) -> tokio::sync::watch::Receiver<Option<SetupRunResult>> {
        debug_assert!(terminal.status != SetupRunStatus::Started);
        let request_id = terminal.request_id.clone();
        let receipt = registry
            .receipts
            .entry(request_id.clone())
            .or_insert_with(SetupReceipt::pending);
        if receipt.selected.is_none() && receipt.receive.borrow().is_none() {
            // Internal cancellation selection is immediate. Public queries stay
            // behind this receipt until the existing durability owner settles.
            let _gate = self.setup.persistence.event_gate.lock_recover();
            self.setup
                .lifecycle
                .lock_recover()
                .apply(&SetupLifecycleEvent::Run {
                    run: terminal.clone(),
                });
            receipt.selected = Some(SetupRunResult::from_record(&terminal));
            receipt.terminal_record = Some(terminal);
            receipt.changed.notify_one();
        }
        let receive = receipt.receive.clone();
        registry.active.remove(&request_id);
        self.register_setup_attempt_durability_owner_locked(registry, request_id, None);
        receive
    }

    /// Each accepted attempt owns one durability task before Started can be
    /// written. Synchronous cancellation selects only its one terminal slot.
    pub(crate) fn register_setup_attempt_durability_owner_locked(
        self: &Arc<Self>,
        registry: &mut SetupDurabilityRegistry,
        request_id: RuntimeString,
        started: Option<SetupRunRecord>,
    ) {
        let Some(receipt) = registry.receipts.get_mut(&request_id) else {
            return;
        };
        if receipt.owner_registered || receipt.receive.borrow().is_some() {
            return;
        }
        receipt.owner_registered = true;
        let session = self.clone();
        self.spawn_setup_owned_task_locked(
            registry, request_id.clone(), SetupOwnedTaskKind::Durability,
            async move {
                if let Some(started) = started {
                    let appended = session.record_setup_event(SetupLifecycleEvent::Run { run: started.clone() }).await;
                    let result = match appended {
                        Ok(()) => {
                            let closing = { session.setup.durability.lock_recover().receipts.get(&request_id).expect("accepted Setup owns its cancellation signal").started_cancel.clone() };
                            tokio::select! {
                                biased;
                                _ = closing.cancelled() => Ok(()),
                                result = session.flush_setup_rollout() => result.map_err(|error| RuntimeString::from(format!("failed to persist Setup started state before hook execution: {error}"))),
                            }
                        },
                        Err(error) => Err(RuntimeString::from(format!("failed to append Setup started state before hook execution: {error:?}"))),
                    };
                    let mut registry = session.setup.durability.lock_recover();
                    if let Err(error) = &result {
                        let mut failed = started;
                        failed.status = SetupRunStatus::Failed;
                        failed.ended_at = Some((session.deps.now)());
                        failed.error = Some(error.clone());
                        session.register_setup_terminal_owner_locked(&mut registry, failed);
                    }
                    if let Some(receipt) = registry.receipts.get_mut(&request_id) {
                        let _ = receipt.started_send.send(Some(result));
                    }
                }
                let terminal = loop {
                    let changed = {
                        let registry = session.setup.durability.lock_recover();
                        let Some(receipt) = registry.receipts.get(&request_id) else { return };
                        if let Some(terminal) = &receipt.terminal_record { break terminal.clone(); }
                        receipt.changed.clone()
                    };
                    changed.notified().await;
                };
                let result = session.clone().finish_setup_terminal_durability(terminal).await;
                session.publish_setup_receipt(&request_id, result);
            },
        );
    }

    pub(crate) fn setup_owner_loss_terminal(
        &self,
        active: &ActiveSetupAttempt,
        during_shutdown: bool,
    ) -> SetupRunRecord {
        let mut terminal = active.run.clone();
        terminal.ended_at = Some((self.deps.now)());
        terminal.additional_context.clear();
        terminal.system_messages.clear();
        terminal.error = Some(RuntimeString::from(match (active.phase, during_shutdown) {
            (SetupEffectPhase::PreEffect, false) => {
                "Setup hook execution was interrupted before external effect admission"
            }
            (SetupEffectPhase::PreEffect, true) => {
                "Setup hook execution was interrupted because session shutdown closed admission before external effects"
            }
            (SetupEffectPhase::EffectAdmitted, false) => {
                "Setup hook outcome is unknown because its caller was interrupted after external effect admission"
            }
            (SetupEffectPhase::EffectAdmitted, true) => {
                "Setup hook outcome is unknown because session shutdown began after external effect admission"
            }
        }));
        terminal.status = match active.phase {
            SetupEffectPhase::PreEffect => SetupRunStatus::Interrupted,
            SetupEffectPhase::EffectAdmitted => SetupRunStatus::OutcomeUnknown,
        };
        terminal
    }

    /// The boxed `Err` keeps this admission result off the hot return path
    /// (`SetupRunResult` is a large record).
    pub(crate) fn begin_setup_attempt(
        self: &Arc<Self>,
        started: SetupRunRecord,
    ) -> Result<SetupAttemptGuard, Box<SetupRunResult>> {
        let mut registry = self.setup.durability.lock_recover();
        let request = SetupRunRequest {
            request_id: started.request_id.clone(),
            trigger: started.trigger,
        };
        if registry.closing || self.lifecycle.is_disposed() {
            return Err(Box::new(SetupRunResult::terminal(
                &request,
                SetupRunStatus::Shutdown,
                None,
                Some(RuntimeString::from("session is shutting down")),
            )));
        }
        if registry.active.contains_key(&started.request_id)
            || registry.receipts.contains_key(&started.request_id)
        {
            return Err(Box::new(SetupRunResult::terminal(
                &request,
                SetupRunStatus::PersistenceFailed,
                Some(started.request_id.clone()),
                Some(RuntimeString::from(
                    "Setup request reached admission without its authoritative prior receipt",
                )),
            )));
        }

        registry.active.insert(
            started.request_id.clone(),
            ActiveSetupAttempt {
                run: started.clone(),
                phase: SetupEffectPhase::PreEffect,
            },
        );
        registry
            .receipts
            .insert(started.request_id.clone(), SetupReceipt::pending());
        self.register_setup_attempt_durability_owner_locked(
            &mut registry,
            started.request_id.clone(),
            Some(started),
        );
        Ok(SetupAttemptGuard {
            session: self.clone(),
            request_id: request.request_id,
            finished: false,
        })
    }

    pub(crate) fn admit_setup_effect_for_owned_attempt(
        self: &Arc<Self>,
        request_id: &RuntimeString,
    ) -> bool {
        let mut registry = self.setup.durability.lock_recover();
        if registry.closing {
            if let Some(active) = registry.active.get(request_id).cloned() {
                let terminal = self.setup_owner_loss_terminal(&active, true);
                self.register_setup_terminal_owner_locked(&mut registry, terminal);
            }
            return false;
        }
        if registry
            .receipts
            .get(request_id)
            .is_some_and(|receipt| receipt.selected.is_some() || receipt.receive.borrow().is_some())
        {
            return false;
        }
        let Some(active) = registry.active.get_mut(request_id) else {
            return false;
        };
        active.phase = SetupEffectPhase::EffectAdmitted;
        true
    }

    /// Transfers the Started durability barrier, the owned serial permit, and
    /// the attempt guard into one Session-owned task. The start gate in
    /// `spawn_setup_owned_task_locked` makes the AbortHandle visible before
    /// either rollout flush or HookFn can be polled.
    pub(crate) fn start_setup_attempt_owner(
        self: &Arc<Self>,
        request: SetupRunRequest,
        started: SetupRunRecord,
        serial: tokio::sync::OwnedMutexGuard<()>,
        attempt: SetupAttemptGuard,
    ) -> tokio::sync::watch::Receiver<Option<SetupRunResult>> {
        let mut registry = self.setup.durability.lock_recover();
        if registry.closing {
            if let Some(active) = registry.active.get(&request.request_id).cloned() {
                let terminal = self.setup_owner_loss_terminal(&active, true);
                self.register_setup_terminal_owner_locked(&mut registry, terminal);
            }
            let receive = registry
                .receipts
                .get(&request.request_id)
                .map(|receipt| receipt.receive.clone())
                .unwrap_or_else(|| {
                    let result = SetupRunResult::terminal(
                        &request,
                        SetupRunStatus::Shutdown,
                        None,
                        Some(RuntimeString::from("session is shutting down")),
                    );
                    let receipt = SetupReceipt::ready(result);
                    let receive = receipt.receive.clone();
                    registry
                        .receipts
                        .insert(request.request_id.clone(), receipt);
                    receive
                });
            attempt.complete();
            drop(serial);
            return receive;
        }
        let Some(_active) = registry.active.get(&request.request_id) else {
            let receive = registry
                .receipts
                .get(&request.request_id)
                .expect("a selected Setup terminal owns a receipt")
                .receive
                .clone();
            attempt.complete();
            drop(serial);
            return receive;
        };
        let receive = registry
            .receipts
            .get(&request.request_id)
            .expect("an active Setup attempt owns a pending receipt")
            .receive
            .clone();

        let task_session = self.clone();
        self.spawn_setup_owned_task_locked(
            &mut registry,
            request.request_id.clone(),
            SetupOwnedTaskKind::Attempt,
            async move {
                let _serial = serial;
                let mut started_receipt = {
                    task_session
                        .setup
                        .durability
                        .lock_recover()
                        .receipts
                        .get(&request.request_id)
                        .expect("accepted Setup owns a Started receipt")
                        .started_receive
                        .clone()
                };
                let started_result = loop {
                    if let Some(result) = started_receipt.borrow().clone() {
                        break result;
                    }
                    if started_receipt.changed().await.is_err() {
                        break Err(RuntimeString::from(
                            "Session-owned Setup Started owner ended without a verdict",
                        ));
                    }
                };
                if started_result.is_err() {
                    attempt.complete();
                    let _ = task_session
                        .await_setup_terminal_durability(SetupRunResult::from_record(&started))
                        .await;
                    return;
                }

                if !task_session.admit_setup_effect_for_owned_attempt(&request.request_id) {
                    attempt.complete();
                    let _ = task_session
                        .await_setup_terminal_durability(SetupRunResult::from_record(&started))
                        .await;
                    return;
                }

                let report = match &task_session.hook_bus {
                    Some(bus) => {
                        let trigger = match request.trigger {
                            SetupTrigger::Init => HookTrigger::Init,
                            SetupTrigger::Maintenance => HookTrigger::Maintenance,
                        };
                        bus.dispatch_force_sync(
                            HookEvent::Setup,
                            &HookInput {
                                trigger: Some(trigger),
                                ..base_hook_input(
                                    HookEvent::Setup,
                                    &task_session.cwd_now(),
                                    &task_session.id,
                                )
                            },
                        )
                        .await
                    }
                    None => crate::hooks::bus::HookDispatchReport::default(),
                };

                let mut terminal = started;
                terminal.ended_at = Some((task_session.deps.now)());
                terminal.hook_count = report.matching_hooks as u64;
                terminal.additional_context = report.outcome.additional_context;
                terminal.system_messages = report.outcome.system_messages;
                if report.failed_hooks > 0 || report.outcome.aborted {
                    terminal.status = SetupRunStatus::Failed;
                    terminal.error = Some(if terminal.system_messages.is_empty() {
                        RuntimeString::from(format!(
                            "{} Setup hook(s) failed",
                            report.failed_hooks.max(1)
                        ))
                    } else {
                        join_runtime_strings(&terminal.system_messages, "\n")
                    });
                } else {
                    terminal.status = SetupRunStatus::Completed;
                }
                let source = SetupRunResult::from_record(&terminal);
                task_session.complete_setup_attempt(terminal);
                attempt.complete();
                // Retain the serial permit through the authoritative terminal
                // durability receipt, even when the original caller vanished.
                let _ = task_session.await_setup_terminal_durability(source).await;
            },
        );
        receive
    }

    pub(crate) fn settle_setup_attempt_after_owner_loss(
        self: &Arc<Self>,
        request_id: &RuntimeString,
    ) {
        let mut registry = self.setup.durability.lock_recover();
        if let Some(receipt) = registry.receipts.get(request_id) {
            if receipt.selected.is_some() || receipt.receive.borrow().is_some() {
                return;
            }
        }
        let Some(active) = registry.active.get(request_id).cloned() else {
            return;
        };
        let terminal = self.setup_owner_loss_terminal(&active, registry.closing);
        self.register_setup_terminal_owner_locked(&mut registry, terminal);
    }

    pub(crate) fn complete_setup_attempt(
        self: &Arc<Self>,
        mut terminal: SetupRunRecord,
    ) -> tokio::sync::watch::Receiver<Option<SetupRunResult>> {
        let mut registry = self.setup.durability.lock_recover();
        if let Some(receipt) = registry.receipts.get(&terminal.request_id) {
            if receipt.selected.is_some() || receipt.receive.borrow().is_some() {
                // Shutdown or caller cancellation already selected the terminal.
                // Never publish a late hook completion over that first verdict.
                return receipt.receive.clone();
            }
        }

        let registered = registry
            .active
            .get(&terminal.request_id)
            .is_some_and(|active| {
                active.run.session_id == terminal.session_id
                    && active.run.trigger == terminal.trigger
                    && active.run.started_at == terminal.started_at
            });
        if !registered {
            terminal.status = SetupRunStatus::PersistenceFailed;
            terminal.additional_context.clear();
            terminal.error = Some(RuntimeString::from(
                "Setup terminal completion lost its registered attempt owner",
            ));
        }
        self.register_setup_terminal_owner_locked(&mut registry, terminal)
    }

    /// Close Setup admission and force-settle every live attempt under the same
    /// registry lock. There can be no later receipt/task registration after the
    /// returned boundary.
    pub(crate) fn close_setup_durability_registry(self: &Arc<Self>) {
        self.start_pending_setup_recovery_owners();
        let mut registry = self.setup.durability.lock_recover();
        if registry.closing {
            return;
        }
        registry.closing = true;
        // Cancelling only the Started flush releases its serial permit; the
        // same durability owner still commits and flushes the chosen terminal.
        for receipt in registry.receipts.values() {
            receipt.started_cancel.cancel();
        }
        if let Some(active) = registry.active_context_admission.as_mut() {
            active.phase = match active.phase {
                SetupContextProviderPhase::Prepared => SetupContextProviderPhase::ReleaseRequested,
                SetupContextProviderPhase::Calling => {
                    SetupContextProviderPhase::OutcomeUnknownRequested
                }
                phase => phase,
            };
            active.changed.notify_one();
        }
        let active = registry.active.values().cloned().collect::<Vec<_>>();
        for attempt in active {
            let terminal = self.setup_owner_loss_terminal(&attempt, true);
            self.register_setup_terminal_owner_locked(&mut registry, terminal);
        }
        for task in registry
            .tasks
            .values()
            .filter(|task| task.kind == SetupOwnedTaskKind::Attempt)
        {
            task.abort.abort();
        }
        registry.task_tracker.close();
        debug_assert!(registry.active.is_empty());
    }

    pub(crate) async fn await_setup_terminal_durability(
        &self,
        source: SetupRunResult,
    ) -> SetupRunResult {
        let receipt = self
            .setup
            .durability
            .lock_recover()
            .receipts
            .get(&source.request_id)
            .map(|receipt| receipt.receive.clone());
        let Some(mut receipt) = receipt else {
            return Self::setup_receipt_failure_result(
                &source,
                "authoritative terminal receipt is missing",
            );
        };
        loop {
            if let Some(result) = receipt.borrow().clone() {
                return result;
            }
            if receipt.changed().await.is_err() {
                return Self::setup_receipt_failure_result(
                    &source,
                    "Session-owned Setup durability task ended without a verdict",
                );
            }
        }
    }

    pub(crate) fn fail_pending_setup_receipts_after_deadline(&self, error: &str) {
        let mut registry = match self.setup.durability.lock() {
            Ok(registry) => registry,
            Err(_) => return,
        };
        let pending = registry
            .receipts
            .iter()
            .filter_map(|(request_id, receipt)| {
                if receipt.receive.borrow().is_some() {
                    return None;
                }
                receipt
                    .terminal_record
                    .clone()
                    .map(|terminal| (request_id.clone(), terminal))
            })
            .collect::<Vec<_>>();
        for (request_id, terminal) in pending {
            let failed = self.setup_persistence_failed_record(&terminal, error);
            if let Some(receipt) = registry.receipts.get_mut(&request_id) {
                let result = SetupRunResult::from_record(&failed);
                receipt.selected = Some(result.clone());
                receipt.terminal_record = Some(failed);
                let _ = receipt.send.send(Some(result));
            }
        }
        if let Some(receipt) = registry.init_close_receipt.as_mut() {
            if receipt.receive.borrow().is_none() {
                let _ = receipt.send.send(Some(Err(RuntimeString::from(error))));
            }
        }
        let pending_context = registry
            .context_admission_receipts
            .iter()
            .filter_map(|(admission_id, receipt)| {
                if receipt.terminal_receive.borrow().is_some() {
                    return None;
                }
                receipt
                    .terminal_record
                    .clone()
                    .or_else(|| {
                        registry
                            .active_context_admission
                            .as_ref()
                            .filter(|active| &active.record.admission_id == admission_id)
                            .map(|active| active.record.clone())
                    })
                    .map(|source| (admission_id.clone(), source))
            })
            .collect::<Vec<_>>();
        for (admission_id, source) in pending_context {
            let failed = self.setup_context_persistence_failed_record(&source, error);
            if registry
                .active_context_admission
                .as_ref()
                .is_some_and(|active| active.record.admission_id == admission_id)
            {
                registry.active_context_admission = None;
            }
            if let Some(receipt) = registry.context_admission_receipts.get_mut(&admission_id) {
                receipt.set_terminal_record(failed.clone());
                let failure = Self::setup_context_terminal_error(&failed);
                if receipt.started_receive.borrow().is_none() {
                    let _ = receipt.started_send.send(Some(Err(failure.clone())));
                }
                let _ = receipt.terminal_send.send(Some(Err(failure)));
            }
        }
    }

    pub(crate) async fn finalize_setup_persistence_fence(
        &self,
        deadline: tokio::time::Instant,
    ) -> ShutdownPhaseResult {
        // Close future producers before waiting for older work. If either gate
        // remains held past the bounded persistence phase, shutdown must not
        // establish a rollout final-flush/shutdown boundary that the older
        // Setup producer could subsequently cross.
        self.setup
            .persistence
            .finalized
            .store(true, Ordering::SeqCst);
        let _flush = tokio::time::timeout_at(deadline, self.setup.persistence.serial.lock())
            .await
            .map_err(|_| {
                ShutdownPhaseFailure::timeout(
                    "Setup persistence fence timed out waiting for an in-flight flush; final rollout flush/shutdown was skipped",
                )
            })?;
        loop {
            if self.try_complete_setup_event_fence()? {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ShutdownPhaseFailure::timeout(
                    "Setup persistence fence timed out waiting for an in-flight append; final rollout flush/shutdown was skipped",
                ));
            }
            tokio::task::yield_now().await;
        }
    }

    /// Synchronous probe keeps the non-Send `std::sync::MutexGuard` out of the
    /// shutdown future while still proving that every pre-fence append has
    /// left the event critical section.
    pub(crate) fn try_complete_setup_event_fence(&self) -> Result<bool, ShutdownPhaseFailure> {
        match self.setup.persistence.event_gate.try_lock() {
            Ok(_gate) => Ok(true),
            Err(std::sync::TryLockError::WouldBlock) => Ok(false),
            Err(std::sync::TryLockError::Poisoned(_)) => Err(ShutdownPhaseFailure::error(
                "Setup persistence fence could not verify a poisoned event gate; final rollout flush/shutdown was skipped",
            )),
        }
    }

    pub(crate) async fn drain_setup_durability(
        &self,
        drain_deadline: tokio::time::Instant,
        abort_join_deadline: tokio::time::Instant,
    ) -> ShutdownPhaseResult {
        let tracker = {
            let mut registry = self.setup.durability.lock().map_err(|_| {
                ShutdownPhaseFailure::error("Setup durability registry was poisoned")
            })?;
            if !registry.closing {
                registry.closing = true;
                registry.task_tracker.close();
            }
            registry.task_tracker.clone()
        };

        if tokio::time::timeout_at(drain_deadline, tracker.wait())
            .await
            .is_ok()
        {
            let pending_receipts = {
                let registry = self.setup.durability.lock().map_err(|_| {
                    ShutdownPhaseFailure::error("Setup durability registry was poisoned")
                })?;
                registry
                    .receipts
                    .values()
                    .filter(|receipt| receipt.receive.borrow().is_none())
                    .count()
                    + usize::from(
                        registry
                            .init_close_receipt
                            .as_ref()
                            .is_some_and(|receipt| receipt.receive.borrow().is_none()),
                    )
                    + registry
                        .context_admission_receipts
                        .values()
                        .map(|receipt| {
                            usize::from(receipt.started_receive.borrow().is_none())
                                + usize::from(receipt.terminal_receive.borrow().is_none())
                        })
                        .sum::<usize>()
            };
            if pending_receipts > 0 {
                self.fail_pending_setup_receipts_after_deadline(
                    "Session-owned Setup tasks drained without publishing a terminal verdict",
                );
            }
            let mut failures = {
                let registry = self.setup.durability.lock().map_err(|_| {
                    ShutdownPhaseFailure::error("Setup durability registry was poisoned")
                })?;
                let mut failures = registry.failures.clone();
                if let Some(Err(error)) = registry
                    .init_close_receipt
                    .as_ref()
                    .and_then(|receipt| receipt.receive.borrow().clone())
                {
                    failures.push(format!("Setup InitClosed durability failed: {error:?}"));
                }
                for receipt in registry.context_admission_receipts.values() {
                    if let Some(Err(error)) = receipt.terminal_receive.borrow().clone() {
                        failures.push(format!(
                            "Setup context admission durability failed: {error:?}"
                        ));
                    }
                }
                failures
            };
            if pending_receipts > 0 {
                failures.push(format!(
                    "Setup durability drained with {pending_receipts} pending receipt(s)"
                ));
            }
            return if failures.is_empty() {
                Ok(())
            } else {
                Err(ShutdownPhaseFailure::error(failures.join("; ")))
            };
        }

        {
            let registry = self.setup.durability.lock().map_err(|_| {
                ShutdownPhaseFailure::error("Setup durability registry was poisoned")
            })?;
            for task in registry
                .tasks
                .values()
                .filter(|task| task.kind != SetupOwnedTaskKind::ContextAdmission)
            {
                task.abort.abort();
            }
        }
        let joined = tokio::time::timeout_at(abort_join_deadline, tracker.wait())
            .await
            .is_ok();
        self.fail_pending_setup_receipts_after_deadline(
            "Session-owned Setup durability exceeded its shutdown deadline",
        );
        if joined {
            Err(ShutdownPhaseFailure::timeout(
                "Setup durability exceeded its shutdown deadline; abortable owners were stopped and context-admission owners reached a retained terminal",
            ))
        } else {
            // The task controls and TaskTracker remain retained. The caller's
            // bounded fence decides whether a final rollout boundary is safe;
            // if an older Setup producer still owns a gate, final rollout
            // flush/shutdown is skipped rather than allowing a late crossing.
            Err(ShutdownPhaseFailure::timeout(
                "Setup durability force-abort did not join before the final persistence boundary",
            ))
        }
    }

    /// Run one correlated Setup pass. Calls are serialized across both triggers.
    /// `init` success is cached and may occur only before the first admitted real
    /// run; a failed/interrupted init can be retried with a new request id.
    pub async fn run_setup_hooks(self: &Arc<Self>, request: SetupRunRequest) -> SetupRunResult {
        self.start_pending_setup_recovery_owners();
        let serial = self.setup.serial.clone().lock_owned().await;

        let existing = {
            // Never invert terminal publication's registry -> lifecycle order.
            let _registry = self.setup.durability.lock_recover();
            let state = self.setup.lifecycle.lock_recover();
            state.request_result(&request.request_id)
        };
        if let Some(mut existing) = existing {
            existing = self.await_setup_terminal_durability(existing).await;
            if existing.trigger != request.trigger {
                return SetupRunResult::terminal(
                    &request,
                    SetupRunStatus::RequestConflict,
                    Some(existing.request_id),
                    Some(RuntimeString::from(
                        "Setup request id was already used with a different trigger",
                    )),
                );
            }
            return existing;
        }

        if self.setup.durability.lock_recover().closing || self.lifecycle.is_disposed() {
            return SetupRunResult::terminal(
                &request,
                SetupRunStatus::Shutdown,
                None,
                Some(RuntimeString::from("session is shutting down")),
            );
        }

        if request.trigger == SetupTrigger::Init {
            let (init_closed, closed_init) = {
                let _registry = self.setup.durability.lock_recover();
                let state = self.setup.lifecycle.lock_recover();
                (
                    state.init_closed_at.is_some() || _registry.init_close_receipt.is_some(),
                    state.closed_init().cloned(),
                )
            };
            if init_closed {
                return SetupRunResult::terminal(
                    &request,
                    SetupRunStatus::TooLate,
                    None,
                    Some(RuntimeString::from(
                        "Setup init is closed because the session has admitted its first run",
                    )),
                );
            }
            if let Some(completed) = closed_init {
                let source = self
                    .await_setup_terminal_durability(SetupRunResult::from_record(&completed))
                    .await;
                let mut result = source;
                result.request_id = request.request_id.clone();
                result.status = SetupRunStatus::AlreadyCompleted;
                result.source_request_id = Some(completed.request_id.clone());
                return result;
            }
        }

        let started_at = (self.deps.now)();
        let started = SetupRunRecord {
            request_id: request.request_id.clone(),
            session_id: self.id.clone(),
            trigger: request.trigger,
            status: SetupRunStatus::Started,
            started_at,
            ended_at: None,
            hook_count: 0,
            additional_context: Vec::new(),
            system_messages: Vec::new(),
            error: None,
        };
        let attempt = match self.begin_setup_attempt(started.clone()) {
            Ok(attempt) => attempt,
            Err(result) => {
                if self
                    .setup
                    .durability
                    .lock()
                    .ok()
                    .is_some_and(|registry| registry.receipts.contains_key(&request.request_id))
                {
                    return self.await_setup_terminal_durability(*result).await;
                }
                return *result;
            }
        };
        let receive =
            self.start_setup_attempt_owner(request.clone(), started.clone(), serial, attempt);
        let waiter = SetupWaiterGuard {
            session: self.clone(),
            request_id: request.request_id.clone(),
            finished: false,
        };
        let result = {
            let mut receive = receive;
            loop {
                if let Some(result) = receive.borrow().clone() {
                    break result;
                }
                if receive.changed().await.is_err() {
                    break Self::setup_receipt_failure_result(
                        &SetupRunResult::from_record(&started),
                        "Session-owned Setup execution ended without a verdict",
                    );
                }
            }
        };
        waiter.complete();
        result
    }

    /// Authoritative live/replayed Setup audit state.
    pub fn setup_lifecycle_snapshot(&self) -> SetupLifecycleSnapshot {
        // Keep model-visible/query-visible state behind the same durability
        // receipt as the correlated response. While a terminal append/flush is
        // pending, expose the last safe Started projection instead of claiming
        // that an unacknowledged terminal survived a crash.
        let registry = self.setup.durability.lock_recover();
        let mut snapshot = self.setup.lifecycle.lock_recover().clone();
        if snapshot.init_closed_at.is_some()
            && !registry
                .init_close_receipt
                .as_ref()
                .is_some_and(|receipt| matches!(receipt.receive.borrow().as_ref(), Some(Ok(()))))
        {
            // InitClosed is a provider-admission boundary, not merely live
            // mutex state. Until its retained append+flush receipt succeeds,
            // reconnect/query consumers must see the last crash-safe state.
            snapshot.init_closed_at = None;
        }
        for (request_id, receipt) in &registry.receipts {
            if receipt
                .receive
                .borrow()
                .as_ref()
                .is_some_and(|result| result.status == SetupRunStatus::PersistenceFailed)
            {
                if let Some(terminal) = &receipt.terminal_record {
                    snapshot.apply(&SetupLifecycleEvent::Run {
                        run: terminal.clone(),
                    });
                }
                continue;
            }
            if receipt.receive.borrow().is_some() {
                continue;
            }
            let Some(terminal) = &receipt.terminal_record else {
                continue;
            };
            let Some(run) = snapshot
                .runs
                .iter_mut()
                .find(|run| &run.request_id == request_id)
            else {
                continue;
            };
            let mut started = terminal.clone();
            started.status = SetupRunStatus::Started;
            started.ended_at = None;
            started.hook_count = 0;
            started.additional_context.clear();
            started.system_messages.clear();
            started.error = None;
            *run = started;
        }
        for receipt in registry.context_admission_receipts.values() {
            if receipt
                .terminal_receive
                .borrow()
                .as_ref()
                .is_some_and(Result::is_err)
            {
                if let Some(terminal) = &receipt.terminal_record {
                    snapshot.apply(&SetupLifecycleEvent::ContextAdmission {
                        admission: terminal.clone(),
                    });
                }
                continue;
            }
            if receipt.terminal_receive.borrow().is_some() {
                continue;
            }
            let Some(terminal) = &receipt.terminal_record else {
                continue;
            };
            if snapshot
                .context_admission
                .as_ref()
                .is_none_or(|current| current.admission_id != terminal.admission_id)
            {
                continue;
            }
            let mut started = terminal.clone();
            started.status = SetupContextAdmissionStatus::Started;
            started.ended_at = None;
            started.error = None;
            snapshot.context_admission = Some(started);
            snapshot.context_consumed_request_id = None;
            snapshot.context_consumed_at = None;
        }
        snapshot
    }

    pub(crate) fn append_setup_event_under_gate(
        &self,
        event: &SetupLifecycleEvent,
        prepared: Option<crate::store::session_store::PreparedRolloutAppend<'_>>,
    ) -> Result<(), RuntimeString> {
        let payload = runtime_codec::encode_value(event).map_err(|error| {
            RuntimeString::from(format!("failed to encode Setup lifecycle event: {error}"))
        })?;
        let _gate = self
            .setup
            .persistence
            .event_gate
            .lock()
            .map_err(|_| RuntimeString::from("Setup event gate was poisoned"))?;
        if self.setup.persistence.finalized.load(Ordering::SeqCst) {
            return Err("Setup persistence is finalized".into());
        }
        self.setup
            .lifecycle
            .lock()
            .map_err(|_| RuntimeString::from("Setup lifecycle state was poisoned"))?
            .apply(event);
        if let Some(prepared) = prepared {
            // Catch inside the gate so a foreign synchronous commit panic does
            // not poison the lifecycle/event locks.
            let appended = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                prepared.commit(crate::schemas::rollout::RolloutItem::Event(
                    crate::schemas::rollout::EventItem {
                        event: SETUP_LIFECYCLE_ROLLOUT_EVENT.into(),
                        turn_id: None,
                        payload: Some(payload),
                    },
                ));
            }));
            if appended.is_err() {
                return Err("Setup rollout append_event adapter panicked".into());
            }
        }
        Ok(())
    }

    pub(crate) async fn record_setup_event(
        &self,
        event: SetupLifecycleEvent,
    ) -> Result<(), RuntimeString> {
        let _serial = self.setup.persistence.serial.lock().await;
        if self.setup.persistence.finalized.load(Ordering::SeqCst) {
            return Err("Setup persistence is finalized".into());
        }
        let prepared = match self.persistence.rollout() {
            Some(rollout) => match std::panic::AssertUnwindSafe(rollout.prepare_append())
                .catch_unwind()
                .await
            {
                Ok(Ok(prepared)) => Some(prepared),
                Ok(Err(error)) => {
                    return Err(format!("Setup rollout append admission failed: {error}").into())
                }
                Err(_) => return Err("Setup rollout append_event adapter panicked".into()),
            },
            None => None,
        };
        let _registry = self
            .setup
            .durability
            .lock()
            .map_err(|_| RuntimeString::from("Setup durability registry was poisoned"))?;
        self.append_setup_event_under_gate(&event, prepared)
    }

    pub(crate) async fn flush_setup_rollout(&self) -> Result<(), String> {
        let _flush = self.setup.persistence.serial.lock().await;
        {
            let _gate = self
                .setup
                .persistence
                .event_gate
                .lock()
                .map_err(|_| "Setup event gate was poisoned".to_string())?;
            if self.setup.persistence.finalized.load(Ordering::SeqCst) {
                return Err("Setup persistence is finalized".to_string());
            }
        }
        let flush = self.flush_rollout();
        match std::panic::AssertUnwindSafe(flush).catch_unwind().await {
            Ok(result) => result,
            Err(_) => Err("Setup rollout flush adapter panicked".to_string()),
        }
    }

    /// Close init at real-run admission and return the exact process-local
    /// durability receipt shared by every overlapping run. The append and
    /// retained flush-owner registration are atomic with Setup shutdown's
    /// registry snapshot, so caller cancellation cannot abandon the marker.
    pub(crate) fn close_setup_init_for_run(
        self: &Arc<Self>,
    ) -> tokio::sync::watch::Receiver<Option<Result<(), RuntimeString>>> {
        let mut registry = self.setup.durability.lock_recover();
        if let Some(receipt) = &registry.init_close_receipt {
            return receipt.receive.clone();
        }
        if registry.closing
            || self.lifecycle.is_disposed()
            || self.setup.persistence.finalized.load(Ordering::SeqCst)
        {
            return SetupInitCloseReceipt::ready(Err(
                "session shutdown closed Setup InitClosed durability admission".into(),
            ))
            .receive;
        }
        let event = SetupLifecycleEvent::InitClosed {
            session_id: self.id.clone(),
            at: (self.deps.now)(),
        };
        if self.persistence.rollout().is_none() {
            let result = self.append_setup_event_under_gate(&event, None);
            let receipt = SetupInitCloseReceipt::ready(result);
            let receive = receipt.receive.clone();
            registry.init_close_receipt = Some(receipt);
            return receive;
        }
        if tokio::runtime::Handle::try_current().is_err() {
            let receipt = SetupInitCloseReceipt::ready(Err(
                "cannot retain Setup InitClosed durability without an active Tokio runtime".into(),
            ));
            let receive = receipt.receive.clone();
            registry.init_close_receipt = Some(receipt);
            return receive;
        }
        let receipt = SetupInitCloseReceipt::pending();
        let receive = receipt.receive.clone();
        // Registering this receipt closes init immediately, before the owner
        // waits for writer capacity. It is still hidden from durable snapshots.
        registry.init_close_receipt = Some(receipt);
        let session = self.clone();
        self.spawn_setup_owned_task_locked(
            &mut registry,
            self.id.clone(),
            SetupOwnedTaskKind::InitClose,
            async move {
                let appended = session.record_setup_event(event).await;
                let result = match appended {
                    Ok(()) => session.flush_setup_rollout().await.map_err(|error| {
                        RuntimeString::from(format!(
                            "failed to persist Setup InitClosed before provider admission: {error}"
                        ))
                    }),
                    Err(error) => Err(RuntimeString::from(format!(
                        "failed to append Setup InitClosed before provider admission: {error:?}"
                    ))),
                };
                session.publish_setup_init_close_receipt(result);
            },
        );
        receive
    }

    pub(crate) async fn await_setup_init_close_durability(
        mut receipt: tokio::sync::watch::Receiver<Option<Result<(), RuntimeString>>>,
    ) -> Result<(), RuntimeString> {
        loop {
            if let Some(result) = receipt.borrow().clone() {
                return result;
            }
            if receipt.changed().await.is_err() {
                return Err(RuntimeString::from(
                    "Session-owned Setup InitClosed durability task ended without a verdict",
                ));
            }
        }
    }

    /// Claim completed init context for exactly one independent turn. The
    /// Session-owned task retains the serial permit from the durable Started
    /// barrier through a durable terminal even if this caller or Query is
    /// cancelled.
    pub(crate) async fn begin_setup_context_claim(
        self: &Arc<Self>,
    ) -> Result<Option<SetupContextClaim>, RuntimeString> {
        let serial = self
            .setup
            .context_admission_serial
            .clone()
            .lock_owned()
            .await;
        let (pending, mut started_receive, admission) = {
            let mut registry = self
                .setup
                .durability
                .lock()
                .map_err(|_| RuntimeString::from("Setup durability registry was poisoned"))?;
            if registry.closing
                || self.lifecycle.is_disposed()
                || self.setup.persistence.finalized.load(Ordering::SeqCst)
            {
                return Err("session shutdown closed Setup context admission".into());
            }
            if registry.active_context_admission.is_some() {
                return Err("another turn already owns the Setup context admission permit".into());
            }
            // Admission ids are process-internal and the lifecycle snapshot keeps
            // the authoritative durable terminal. Prune only after both waiters
            // settled and the last provider guard was dropped; a receipt future
            // already created by that guard retains its own terminal-record handle.
            // Constructor-recovery receipts remain addressable by their immutable
            // barrier ids, and persistence failures remain sticky for shutdown.
            let recovery_ids = registry
                .context_recovery_receipt_ids
                .iter()
                .cloned()
                .collect::<HashSet<_>>();
            registry
                .context_admission_receipts
                .retain(|admission_id, receipt| {
                    recovery_ids.contains(admission_id)
                        || !receipt.provider_lease_released
                        || receipt.started_receive.borrow().is_none()
                        || receipt.terminal_receive.borrow().is_none()
                        || receipt.terminal_record.as_ref().is_some_and(|record| {
                            record.status == SetupContextAdmissionStatus::PersistenceFailed
                        })
                });
            let pending = self
                .setup
                .lifecycle
                .lock()
                .map_err(|_| RuntimeString::from("Setup lifecycle state was poisoned"))?
                .pending_init_context()
                .cloned();
            let Some(pending) = pending else {
                drop(registry);
                drop(serial);
                return Ok(None);
            };

            let admission_id = RuntimeString::from((self.deps.uuid)());
            if registry
                .context_admission_receipts
                .contains_key(&admission_id)
            {
                return Err("Setup context admission id collided with an existing receipt".into());
            }
            let record = SetupContextAdmissionRecord {
                admission_id: admission_id.clone(),
                request_id: pending.request_id.clone(),
                session_id: self.id.clone(),
                status: SetupContextAdmissionStatus::Started,
                started_at: (self.deps.now)(),
                ended_at: None,
                error: None,
            };
            let changed = Arc::new(tokio::sync::Notify::new());
            registry.active_context_admission = Some(ActiveSetupContextAdmission {
                record: record.clone(),
                phase: SetupContextProviderPhase::Prepared,
                changed,
            });
            registry.context_admission_receipts.insert(
                admission_id.clone(),
                SetupContextAdmissionReceipt::pending(),
            );
            let started_receive = registry
                .context_admission_receipts
                .get(&admission_id)
                .expect("new Setup context claim owns a receipt")
                .started_receive
                .clone();

            registry
                .context_admission_receipts
                .get_mut(&admission_id)
                .expect("live Setup context claim retains its receipt")
                .owner_registered = true;
            let session = self.clone();
            let owner_admission_id = admission_id.clone();
            self.spawn_setup_owned_task_locked(
                &mut registry,
                admission_id.clone(),
                SetupOwnedTaskKind::ContextAdmission,
                async move {
                    session
                        .run_setup_context_admission_owner(owner_admission_id, serial)
                        .await;
                },
            );
            let admission: Arc<dyn ProviderCallAdmission> =
                Arc::new(SetupContextProviderAdmission {
                    session: Arc::downgrade(self),
                    admission_id: admission_id.clone(),
                });
            (pending, started_receive, admission)
        };

        loop {
            if let Some(result) = started_receive.borrow().clone() {
                result?;
                break;
            }
            if started_receive.changed().await.is_err() {
                return Err(RuntimeString::from(
                    "Session-owned Setup context Started task ended without a verdict",
                ));
            }
        }
        Ok(Some(SetupContextClaim {
            additional_context: pending.additional_context,
            admission,
        }))
    }
}

/// The shutdown gate's refusal messages are engine-internal constants that all
/// carry one marker phrase. A Turn refused by that gate died because its
/// session went away — the same user-visible outcome as an abort — so callers
/// settle it as interrupted instead of a provider-style failure. Durability
/// persistence failures deliberately do NOT carry the marker: those are real
/// internal errors the transcript must keep showing.
pub(crate) fn is_session_shutdown_refusal(message: &RuntimeString) -> bool {
    match message.try_to_utf8() {
        Ok(text) => text.contains(SESSION_SHUTDOWN_REFUSAL_MARKER),
        Err(_) => false,
    }
}

const SESSION_SHUTDOWN_REFUSAL_MARKER: &str = "session shutdown closed";

#[cfg(test)]
mod managed_requirement_tests {
    use super::*;
    use crate::engine::approval_requirements::{
        ManagedApprovalPolicy, ManagedReviewRequirements, ManagedReviewer, ManagedSandboxMode,
    };
    use crate::types::permission::{
        ApprovalPolicy, ApprovalsReviewer, GranularApprovalConfig, SandboxMode,
    };

    #[test]
    fn startup_defaults_preserve_exact_granular_policy_and_read_only_fallback() {
        let granular = GranularApprovalConfig {
            sandbox_approval: false,
            rules: true,
            skill_approval: false,
            request_permissions: true,
            mcp_elicitations: false,
        };
        let requirements = ManagedReviewRequirements {
            allowed_approval_policies: Some(vec![
                ManagedApprovalPolicy::Granular {
                    sandbox_approval: granular.sandbox_approval,
                    rules: granular.rules,
                    skill_approval: granular.skill_approval,
                    request_permissions: granular.request_permissions,
                    mcp_elicitations: granular.mcp_elicitations,
                },
                ManagedApprovalPolicy::Never,
            ]),
            allowed_reviewers: Some(vec![ManagedReviewer::AutoReview]),
            allowed_sandbox_modes: Some(vec![ManagedSandboxMode::ReadOnly]),
            ..Default::default()
        };
        let mut config = crate::schemas::config::default_config();
        config.approval_policy = Some(ApprovalPolicy::OnRequest);
        config.approvals_reviewer = Some(ApprovalsReviewer::User);
        config.sandbox_mode = Some(SandboxMode::WorkspaceWrite);
        apply_startup_review_requirements(&mut config, &requirements).unwrap();
        assert_eq!(
            config.approval_policy,
            Some(ApprovalPolicy::Granular(granular))
        );
        assert_eq!(
            config.approvals_reviewer,
            Some(ApprovalsReviewer::Automatic)
        );
        assert_eq!(config.sandbox_mode, Some(SandboxMode::ReadOnly));
    }

    #[test]
    fn protected_startup_downgrades_full_access_and_requires_automatic_reviewer() {
        let requirements = ManagedReviewRequirements {
            required_models: vec!["protected".into()],
            ..Default::default()
        };
        let mut config = crate::schemas::config::default_config();
        config.model = "protected".into();
        config.sandbox_mode = Some(SandboxMode::DangerFullAccess);
        // This historical input is consumed later by session planning admission.
        config.permission_mode = ExternalPermissionMode::Plan;
        config.sandbox_workspace_write.network_access = true;
        apply_startup_review_requirements(&mut config, &requirements).unwrap();
        assert_eq!(
            config.approvals_reviewer,
            Some(ApprovalsReviewer::Automatic)
        );
        assert_eq!(config.sandbox_mode, Some(SandboxMode::WorkspaceWrite));
        assert_eq!(
            config.permission_mode,
            ExternalPermissionMode::Plan,
            "managed policy must preserve a pending planning input"
        );
        assert!(!config.sandbox_workspace_write.network_access);
        let user_only = ManagedReviewRequirements {
            allowed_reviewers: Some(vec![ManagedReviewer::User]),
            ..requirements
        };
        assert!(apply_startup_review_requirements(&mut config, &user_only).is_err());
    }

    #[test]
    fn reviewer_constraint_does_not_apply_model_required_permission_downgrade() {
        let requirements = ManagedReviewRequirements {
            force_sync: true,
            allowed_reviewers: Some(vec![ManagedReviewer::AutoReview]),
            ..Default::default()
        };
        let mut config = crate::schemas::config::default_config();
        config.sandbox_mode = Some(SandboxMode::DangerFullAccess);
        apply_startup_review_requirements(&mut config, &requirements).unwrap();
        assert_eq!(
            config.approvals_reviewer,
            Some(ApprovalsReviewer::Automatic)
        );
        assert_eq!(config.sandbox_mode, Some(SandboxMode::DangerFullAccess));
    }

    #[test]
    fn full_access_with_approvals_disabled_cannot_fall_back_to_read_only() {
        let requirements = ManagedReviewRequirements {
            allowed_sandbox_modes: Some(vec![ManagedSandboxMode::ReadOnly]),
            ..Default::default()
        };
        let mut config = crate::schemas::config::default_config();
        config.sandbox_mode = Some(SandboxMode::DangerFullAccess);
        config.approval_policy = Some(ApprovalPolicy::Never);
        assert!(apply_startup_review_requirements(&mut config, &requirements).is_err());
    }

    #[test]
    fn live_constraints_validate_all_fields_without_changing_permissions() {
        let requirements = ManagedReviewRequirements {
            allowed_reviewers: Some(vec![ManagedReviewer::AutoReview]),
            allowed_approval_policies: Some(vec![ManagedApprovalPolicy::Never]),
            allowed_sandbox_modes: Some(vec![ManagedSandboxMode::ReadOnly]),
            required_models: vec!["protected".into()],
            ..Default::default()
        };
        let policy = RuntimePolicyState {
            approval_policy: Some(ApprovalPolicy::Never),
            sandbox_mode: Some(SandboxMode::ReadOnly),
            approvals_reviewer: Some(ApprovalsReviewer::Automatic),
            generation: 0,
            sandbox_workspace_write: Default::default(),
            network_allowed: false,
        };
        assert!(validate_review_requirements(&requirements, &policy, &"protected".into()).is_ok());
        assert!(validate_review_requirements(
            &requirements,
            &RuntimePolicyState {
                approval_policy: Some(ApprovalPolicy::OnRequest),
                ..policy.clone()
            },
            &"ordinary".into()
        )
        .is_err());
        assert!(validate_review_requirements(
            &requirements,
            &RuntimePolicyState {
                sandbox_mode: Some(SandboxMode::WorkspaceWrite),
                ..policy.clone()
            },
            &"ordinary".into()
        )
        .is_err());
        assert!(validate_review_requirements(
            &requirements,
            &RuntimePolicyState {
                approvals_reviewer: Some(ApprovalsReviewer::User),
                ..policy.clone()
            },
            &"ordinary".into()
        )
        .is_err());
        let unrestricted = ManagedReviewRequirements {
            required_models: vec!["protected".into()],
            ..Default::default()
        };
        assert!(validate_review_requirements(
            &unrestricted,
            &RuntimePolicyState {
                sandbox_mode: Some(SandboxMode::DangerFullAccess),
                ..policy.clone()
            },
            &"protected".into()
        )
        .is_err());
    }
}
