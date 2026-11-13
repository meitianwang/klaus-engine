use super::agent_memory_snapshot_reconcile_error;
use super::append_developer_instructions;
use super::derive_peer_name;
use super::is_ecmascript_trim_unit;
use super::memory_gates_from;
use super::message_uuid;
use super::normalize_document_filename;
use super::pdf_size_error;
use super::render_background_completion_notice_for_profile;
use super::render_deferred_notice_for_profile;
use super::resolve_abs;
use super::resolve_runtime_join;
use super::runtime_ascii_bytes;
use super::runtime_ascii_eq_ignore_case;
use super::runtime_error_message_to_public_host;
use super::runtime_path_from_host_path;
use super::runtime_path_to_agent_memory_host;
use super::runtime_path_to_skill_shell_host;
use super::runtime_prompt_to_background_host;
use super::runtime_session_id_to_session_store_host;
use super::runtime_session_id_to_skill_env_host;
use super::validate_native_pdf_provider;
use super::ActiveOutputStyle;
use super::AgentMemorySnapshotMergeDelivery;
use super::AsyncPersistenceGate;
use super::BackgroundRunRegistry;
use super::ConditionalSkillGate;
use super::CronRuntime;
use super::DefaultAgentMemoryHost;
use super::Input;
use super::IsolatedMaintenanceSinkState;
use super::LiveForkSnapshot;
use super::LiveSideTaskSnapshot;
use super::MaintenanceAdmissionTracker;
use super::MaintenanceRegistry;
use super::ManagedResourceClient;
use super::McpInstruction;
use super::RolloutSink;
use super::RuntimePolicySnapshot;
use super::RuntimePolicyState;
use super::Session;
use super::SessionConversationState;
use super::SessionFileChanges;
use super::SessionInit;
use super::SessionInitError;
use super::SessionLifecycleState;
use super::SessionMaintenanceState;
use super::SessionMcpRuntime;
use super::SessionMemoryRoots;
use super::SessionMemoryScheduler;
use super::SessionModeControl;
use super::SessionPersistenceState;
use super::SessionPluginProjection;
use super::SessionPluginPublication;
use super::SessionPluginRuntime;
use super::SessionPluginUpdate;
use super::SessionSetupState;
use super::SessionSnapshotState;
use super::SessionSubagentState;
use super::SessionTurnState;
use super::SessionWorkspaceState;
use super::SetupContextAdmissionReceipt;
use super::SetupDurabilityRegistry;
use super::SetupReceipt;
use super::SharedDeps;
use super::ShutdownPhaseFailure;
use super::ShutdownPhaseResult;
use super::SlashCommandOutcome;
use super::SnapshotDecisionAttemptGuard;
use super::SnapshotDurabilityOperation;
use super::SnapshotDurabilityOperationKind;
use super::SnapshotDurabilityProvisional;
use super::SnapshotDurabilityReceipt;
use super::SnapshotDurabilityRegistry;
use super::SnapshotDurabilityResult;
use super::SnapshotDurabilityTaskControl;
use super::SnapshotDurabilityTaskRegistration;
use super::SnapshotOrderedReply;
use super::SyncPersistenceGate;
use super::SystemPromptProvider;
use super::TaskListSubscriptionRegistry;
use super::TurnIndex;
use super::TurnOptions;
use super::UserContentPart;
use super::ValidatedInput;
use super::MAX_DIRECT_PDF_DOCUMENTS;
use super::MAX_DIRECT_PDF_INPUT_BYTES;
use super::MAX_ENCODED_PDF_INPUT_BYTES;
use crate::capabilities::RuntimeCapabilityRoot;
use crate::command_runtime::background::{
    BackgroundCommandRegistry, BackgroundProcessPersistenceSink,
    RolloutBackgroundProcessPersistence,
};
use crate::context::date_change_reminder::create_date_change_reminder_provider;
use crate::context::edited_files_reminder::create_edited_files_reminder_provider;
use crate::context::nested_memory_reminder::create_nested_memory_reminder_provider;
use crate::context::nested_memory_reminder::NestedMemoryReminderDeps;
use crate::context::system_prompt::create_system_prompt_provider;
use crate::context::system_prompt::McpServerInstructions;
use crate::context::system_prompt::SystemPromptProviderOptions;
use crate::context::ultracode_reminder::create_ultracode_reminder_provider;
use crate::context::ultrathink_reminder::create_ultrathink_reminder_provider;
use crate::engine::agent_definitions::AgentRegistry;
use crate::engine::message_queue::MessageQueue;
use crate::engine::message_queue::QueuedMessage;
use crate::engine::message_queue::QueuedValue;
use crate::engine::message_queue::QUEUE_STATE_ROLLOUT_EVENT;
use crate::engine::resource_governor::ResourceGovernor;
use crate::engine::resource_governor::ResourceGovernorOptions;
use crate::engine::subagent_lifecycle::SubagentLifecycleRegistry;
use crate::extensions::commands::skills_as_commands;
use crate::extensions::commands::Command;
use crate::extensions::commands::SkillsAsCommandsOptions;
use crate::extensions::commands::SlashCommandRegistry;
use crate::extensions::output_styles::resolve_output_style;
use crate::extensions::skills::expand_skill::SkillBashOpts;
use crate::extensions::skills::expand_skill::SkillBashRunner;
use crate::extensions::skills::loader::Skill;
use crate::extensions::skills::skill_listing_reminder::create_skill_listing_reminder_provider;
use crate::fs::path_safety::canonical_descendant_scope;
use crate::fs::paths::config_home;
use crate::fs::plan_files;
use crate::hooks::setup::SetupLifecycleEvent;
use crate::lsp::lsp_client::LspClientOptions;
use crate::lsp::lsp_manager::create_lsp_diagnostics_reminder_provider;
use crate::lsp::lsp_manager::LspManager;
use crate::mcp::client::ConnectMcpOptions;
use crate::mcp::client::ManagedClient;
use crate::mcp::connection_manager::McpConnectionManager;
use crate::mcp::connection_manager::McpConnectionManagerOptions;
use crate::mcp::elicitation::McpElicitationResponse;
use crate::mcp::elicitation::McpElicitationRouter;
use crate::mcp::oauth_provider::mcp_auth_status;
use crate::mcp::oauth_provider::revoke_mcp_authorization;
use crate::mcp::oauth_provider::McpAuthStatus;
use crate::mcp::runtime_snapshot::McpRuntimeSnapshot;
use crate::mcp::types::McpServerConfig;
use crate::memory::agent_memory::default_agent_memory_layout;
use crate::memory::agent_memory::run_agent_memory_sync;
use crate::memory::agent_memory::AgentMemoryHost;
use crate::memory::agent_memory::AgentMemoryLayout;
use crate::memory::agent_memory::AgentMemoryMergeChoice;
use crate::memory::agent_memory::AgentMemoryScope;
use crate::memory::agent_memory::AgentMemorySyncRequest;
use crate::memory::maintenance::MemoryMaintenanceQueryError;
use crate::memory::maintenance::MemoryMaintenanceSnapshot;
use crate::memory::memory_recall_reminder::create_memory_recall_reminder_provider;
use crate::memory::memory_recall_reminder::MemoryRecallDeps;
use crate::memory::memory_recall_reminder::RecallContext;
use crate::memory::memory_recall_reminder::RecallError;
use crate::memory::pinned_root::PinnedMemoryRoot;
use crate::memory::snapshot_decision::prepare_agent_memory_snapshot_request;
use crate::memory::snapshot_decision::stage_apply_recorded_agent_memory_snapshot_decision;
use crate::memory::snapshot_decision::stage_recover_marked_agent_memory_snapshot_decision;
use crate::memory::snapshot_decision::AgentMemorySnapshotApplyError;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionAudit;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionPolicy;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionRegistry;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionRequest;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionSource;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionStatus;
use crate::memory::snapshot_decision::AgentMemorySnapshotRegistryError;
use crate::memory::snapshot_decision::AgentMemorySnapshotStaged;
use crate::memory::snapshot_decision::AgentMemorySnapshotStagedFilesystemResult;
use crate::memory::snapshot_decision::AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT;
use crate::memory::store::RecallSemanticOptions;
use crate::memory::store::TypedMemoryStore;
use crate::memory::store::TypedMemoryStoreOptions;
use crate::network_proxy::ManagedNetworkSandboxBackend;
use crate::network_proxy::NetworkProxy;
use crate::network_proxy::NetworkProxyTransport;
use crate::network_proxy::WindowsNetworkGuardSandboxBackend;
use crate::observability::cost_tracker::CostTracker;
use crate::observability::otel::create_otel_observer;
use crate::observability::otel::CreateOtelObserverOptions;
use crate::permission::engine::PermissionEngine;
use crate::permission::engine::StaticPermissionEngine;
use crate::permission::permission_update::empty_session_permission_state;
use crate::provider::behavior::resolve_provider_behavior_profile;
use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::runtime_codec;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
use crate::sandbox::create_sandbox_for_mode_with_engine_config;
use crate::schemas::config::ResolvedConfig;
use crate::schemas::content::ContentBlock;
use crate::schemas::message::Message;
use crate::schemas::rollout::SessionMeta;
use crate::store::session_store::AppendEventOptions;
use crate::store::session_store::SessionStore;
use crate::sync_ext::{MutexExt, RwLockExt};
use crate::tasks::swarm_store::create_swarm_reminder_provider;
use crate::tasks::swarm_store::Identity as SwarmStoreIdentity;
use crate::tasks::task_store::create_task_reminder_provider;
use crate::tools::file_state::InMemoryFileStateCache;
use crate::types::abort::AbortSignal;
use crate::types::errors::InputValidationError;
use crate::types::errors::InputValidationErrorCode;
use crate::types::file_state::FileStateCache;
use crate::types::permission::ExternalPermissionMode;
use crate::types::permission::PermissionContext;
use crate::types::provider::SystemPromptPart;
use crate::types::reminder::ReminderProvider;
use crate::types::sandbox::SandboxBackend;
use crate::types::tool::McpResourceClient;
use crate::types::tool::PlanFileContext;
use crate::types::tool::RunBashOptions;
use crate::types::tool::SwarmIdentity;
use crate::types::tool::Tool;
use crate::worktree::worktree::WorktreeSession;
use base64::Engine;
use futures::future::FutureExt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::Weak;

// ---------------------------------------------------------------------------
// Cohesive-cluster accessors
//
// Each cluster owns the raw lock/atomic touches on its own fields, so callers
// read/mutate through named intent instead of reaching into the wrappers. Every
// method below takes and releases a single lock/atomic and is never held across
// an await; guard-binding sequences that hold a lock across multi-step work (or
// order several fields against each other) stay inline in `Session` where that
// orchestration lives.
// ---------------------------------------------------------------------------

impl SessionConversationState {
    /// A clone of the live message transcript.
    pub(crate) fn history_snapshot(&self) -> Vec<Message> {
        self.history.lock_recover().clone()
    }
    /// Append one message to the transcript.
    pub(crate) fn push_history(&self, m: Message) {
        self.history.lock_recover().push(m);
    }
    /// Apply a summarized snapshot without losing typed answers admitted while
    /// the compactor was awaiting its provider. Gather and replace atomically.
    pub(crate) fn apply_compaction(
        &self,
        messages: Vec<Message>,
        source_message_ids: &[RuntimeString],
    ) -> Vec<Message> {
        let retained = self.retained_goal_instructions.lock_recover();
        let mut history = self.history.lock_recover();
        *history = crate::store::projection::preserve_uncompacted_question_answers(
            messages,
            &history,
            source_message_ids,
        );
        *history = crate::goal::retained_context::preserve_instructions(
            std::mem::take(&mut *history),
            &retained,
        );
        history.clone()
    }
    /// The steering message queue handle.
    pub(crate) fn queue(&self) -> &Arc<MessageQueue> {
        &self.queue
    }
    /// The resumed checkpoint ids carried over from rollout.
    pub(crate) fn resumed_checkpoints(&self) -> &[Option<String>] {
        &self.resumed_checkpoints
    }
}

impl SessionLifecycleState {
    /// Whether session teardown has started or completed.
    pub(crate) fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::SeqCst)
    }
    /// Latch the dispose flag (idempotent).
    pub(crate) fn mark_disposed(&self) {
        self.disposed.store(true, Ordering::SeqCst);
    }
}

impl SessionModeControl {
    /// The model override staged for the next turn.
    pub(crate) fn pending_model(&self) -> Option<RuntimeString> {
        self.pending_model_override.lock_recover().clone()
    }
    /// Stage (or clear) the next-turn model override.
    pub(crate) fn set_pending_model(&self, model: Option<RuntimeString>) {
        *self.pending_model_override.lock_recover() = model;
    }
}

impl SessionMemoryRoots {
    /// The main-memory directory content, if configured.
    pub(crate) fn main(&self) -> Option<&String> {
        self.main.as_ref()
    }
    /// The construction-time pinned identity for the main memory directory.
    pub(crate) fn pinned_root(&self) -> Option<Arc<PinnedMemoryRoot>> {
        self.pinned_root.clone()
    }
    /// The default project-memory root eligible for the maintenance exemption.
    /// Only tests observe this pinned value; production reads the resolved
    /// directory through other paths.
    #[cfg(test)]
    pub(crate) fn internal_root(&self) -> Option<RuntimePath> {
        self.internal_root.clone()
    }
    /// The nested-memory trigger set.
    pub(crate) fn nested_triggers(&self) -> &Mutex<HashSet<RuntimePath>> {
        &self.nested_triggers
    }
    /// A shared handle to the nested-memory loaded-path set.
    pub(crate) fn nested_loaded_paths(&self) -> Arc<Mutex<HashSet<RuntimePath>>> {
        self.nested_loaded_paths.clone()
    }
}

impl SessionMemoryScheduler {
    // ---- auto-memory extraction flags ----

    /// Whether an auto-memory extraction is running.
    pub(crate) fn is_extraction_in_progress(&self) -> bool {
        self.extraction_in_progress.load(Ordering::SeqCst)
    }
    /// Claim the extraction slot; returns true if this caller won it.
    pub(crate) fn try_begin_extraction(&self) -> bool {
        self.extraction_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
    /// Release the extraction slot.
    pub(crate) fn end_extraction(&self) {
        self.extraction_in_progress.store(false, Ordering::SeqCst);
    }
    /// Record that another extraction should follow the current one.
    pub(crate) fn set_extraction_pending(&self) {
        self.extraction_pending.store(true, Ordering::SeqCst);
    }
    /// Clear the pending-extraction flag.
    pub(crate) fn clear_extraction_pending(&self) {
        self.extraction_pending.store(false, Ordering::SeqCst);
    }
    /// Take (and clear) the pending-extraction flag.
    pub(crate) fn take_extraction_pending(&self) -> bool {
        self.extraction_pending.swap(false, Ordering::SeqCst)
    }

    // ---- Dream scan flags + owner-generation protocol ----

    /// Whether an auto-Dream scan is running.
    pub(crate) fn is_dream_in_progress(&self) -> bool {
        self.dream_in_progress.load(Ordering::SeqCst)
    }
    /// The last auto-Dream scan timestamp.
    pub(crate) fn last_dream_scan_at(&self) -> i64 {
        self.last_dream_scan_at.load(Ordering::SeqCst)
    }
    /// Record the auto-Dream scan timestamp.
    pub(crate) fn set_last_dream_scan_at(&self, now: i64) {
        self.last_dream_scan_at.store(now, Ordering::SeqCst);
    }

    /// Claim the Dream slot and mint a fresh owner generation, or `None` if a
    /// scan is already running.
    pub(crate) fn try_begin_dream(&self) -> Option<u64> {
        self.dream_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .ok()?;
        let owner = self.next_dream_owner.fetch_add(1, Ordering::SeqCst).max(1);
        self.active_dream_owner.store(owner, Ordering::SeqCst);
        Some(owner)
    }

    /// Release the Dream slot iff `owner` still owns it.
    pub(crate) fn finish_dream_owner(&self, owner: u64) {
        if self
            .active_dream_owner
            .compare_exchange(owner, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.dream_in_progress.store(false, Ordering::SeqCst);
        }
    }

    /// Force the Dream slot open regardless of the current owner.
    pub(crate) fn force_clear_dream_owner(&self) {
        self.active_dream_owner.store(0, Ordering::SeqCst);
        self.dream_in_progress.store(false, Ordering::SeqCst);
    }

    // ---- last-processed memory cursor ----

    /// The uuid of the last message folded into memory.
    pub(crate) fn last_memory_uuid(&self) -> Option<RuntimeString> {
        self.last_memory_message_uuid.lock_recover().clone()
    }
    /// Record the last message folded into memory. The production commit path
    /// writes this cursor through its fallible durable-persistence gate; only
    /// test helpers set it directly.
    #[cfg(test)]
    pub(crate) fn set_last_memory_uuid(&self, uuid: Option<RuntimeString>) {
        *self.last_memory_message_uuid.lock_recover() = uuid;
    }

    // ---- per-session running notes (SessionMemory) ----

    pub(crate) fn session_memory_config(
        &self,
    ) -> crate::memory::session_memory::SessionMemoryConfig {
        *self.session_memory_config.lock_recover()
    }

    pub(crate) fn set_session_memory_config(
        &self,
        config: crate::memory::session_memory::SessionMemoryConfig,
    ) {
        *self.session_memory_config.lock_recover() = config;
    }

    pub(crate) fn session_memory_compact_config(
        &self,
    ) -> crate::memory::session_memory::SessionMemoryCompactConfig {
        *self.session_memory_compact_config.lock_recover()
    }

    pub(crate) fn set_session_memory_compact_config(
        &self,
        config: crate::memory::session_memory::SessionMemoryCompactConfig,
    ) {
        *self.session_memory_compact_config.lock_recover() = config;
    }

    pub(crate) fn is_session_memory_initialized(&self) -> bool {
        self.session_memory_initialized.load(Ordering::SeqCst)
    }

    pub(crate) fn mark_session_memory_initialized(&self) {
        self.session_memory_initialized
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn tokens_at_last_session_memory(&self) -> u64 {
        self.tokens_at_last_session_memory.load(Ordering::SeqCst)
    }

    pub(crate) fn record_session_memory_token_count(&self, tokens: u64) {
        self.tokens_at_last_session_memory
            .store(tokens, Ordering::SeqCst);
    }

    pub(crate) fn last_session_memory_cursor_uuid(&self) -> Option<RuntimeString> {
        self.last_session_memory_cursor_uuid.lock_recover().clone()
    }

    pub(crate) fn set_last_session_memory_cursor_uuid(&self, uuid: Option<RuntimeString>) {
        *self.last_session_memory_cursor_uuid.lock_recover() = uuid;
    }

    pub(crate) fn last_summarized_message_id(&self) -> Option<RuntimeString> {
        self.last_summarized_message_id.lock_recover().clone()
    }

    pub(crate) fn set_last_summarized_message_id(&self, uuid: Option<RuntimeString>) {
        *self.last_summarized_message_id.lock_recover() = uuid;
    }

    pub(crate) fn try_begin_session_memory(&self, now_ms: i64) -> bool {
        if self
            .session_memory_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.session_memory_started_at
                .store(now_ms.max(1), Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    pub(crate) fn finish_session_memory(&self) {
        self.session_memory_started_at.store(0, Ordering::SeqCst);
        self.session_memory_in_progress
            .store(false, Ordering::SeqCst);
        self.session_memory_settled.notify_waiters();
    }

    pub(crate) fn session_memory_started_at(&self) -> i64 {
        self.session_memory_started_at.load(Ordering::SeqCst)
    }

    pub(crate) fn session_memory_settled(&self) -> Arc<tokio::sync::Notify> {
        self.session_memory_settled.clone()
    }

    pub(crate) fn set_session_memory_abort(
        &self,
        token: Option<tokio_util::sync::CancellationToken>,
    ) {
        *self.session_memory_abort.lock_recover() = token;
    }

    pub(crate) fn take_session_memory_abort(&self) -> Option<tokio_util::sync::CancellationToken> {
        self.session_memory_abort.lock_recover().take()
    }
}

impl SessionMaintenanceState {
    // The SyncPersistenceGate (`persistence`), the live/isolated event sinks,
    // and the authoritative snapshot's apply/mutate sequences stay inline:
    // they cross the gate barrier or hold the snapshot lock across multi-step
    // work, which is Session-level orchestration.

    /// The sticky first-wins query-fatal fence, if tripped.
    pub(crate) fn query_fatal(&self) -> Option<MemoryMaintenanceQueryError> {
        self.query_fatal.get().cloned()
    }
    /// Trip the query-fatal fence (first writer wins; later writes are ignored).
    pub(crate) fn set_query_fatal(&self, error: MemoryMaintenanceQueryError) {
        let _ = self.query_fatal.set(error);
    }
    /// A clone of the authoritative maintenance snapshot.
    pub(crate) fn snapshot(&self) -> MemoryMaintenanceSnapshot {
        self.snapshot.lock_recover().clone()
    }
}

impl SessionSnapshotState {
    // The AsyncPersistenceGate, durability chains, decision registry/policy,
    // and audit channels are driven through their own APIs at the call sites,
    // where the gate-crossing order is load-bearing; only the late-bound
    // self-reference is a plain cell accessor.

    /// Install the late-bound owner used for lazy recovery from `&self`.
    pub(crate) fn set_self_ref(&self, session: Weak<Session>) {
        let _ = self.self_ref.set(session);
    }
    /// Resolve the owning `Arc<Session>` from the late-bound weak reference.
    pub(crate) fn resolve_self(&self) -> Option<Arc<Session>> {
        self.self_ref.get().and_then(Weak::upgrade)
    }
}

/// Session providers can retain their Session. A cancelled constructor must
/// transfer teardown before its unpublished local Arc can disappear.
struct UnpublishedSessionGuard(Option<Arc<Session>>);

impl Drop for UnpublishedSessionGuard {
    fn drop(&mut self) {
        if let Some(session) = self.0.take() {
            session.initiate_abandoned_resume_shutdown();
        }
    }
}

impl SessionPersistenceState {
    // The async persistence gates and durability registries live in the
    // higher clusters; this cluster's own fields are plain optional handles
    // plus the lazily installed checkpoint service.

    /// The live rollout sink, if persistence is enabled.
    pub(crate) fn rollout(&self) -> Option<&RolloutSink> {
        self.rollout.as_ref()
    }
    pub(crate) fn usage_receipts(
        &self,
    ) -> Option<&Arc<crate::store::receipt_broker::ReceiptBroker>> {
        self.usage_receipts.as_ref()
    }

    pub(crate) fn retain_admission_records(
        &self,
        records: Vec<crate::schemas::rollout::RolloutItem>,
    ) -> std::io::Result<()> {
        if let Some(rollout) = &self.ordered_rollout {
            rollout.register_prefix(records)?.start()?;
        }
        Ok(())
    }

    /// The session store, if persistence is enabled.
    pub(crate) fn store(&self) -> Option<&Arc<dyn SessionStore>> {
        self.store.as_ref()
    }
    /// Drop the checkpoint service pinned to the current directory.
    pub(crate) fn clear_checkpoint_service(&self) {
        *self.checkpoint_svc.lock_recover() = None;
    }
}

impl SessionMcpRuntime {
    // The async `manager` and `gate` locks stay inline at their call sites:
    // they are acquired with `.await` and held across multi-step connection
    // work, which is Session-level orchestration, not a field accessor.

    /// Whether the lazy connection manager has been initialized.
    pub(crate) fn is_manager_initialized(&self) -> bool {
        self.manager_initialized.get().is_some()
    }
    /// The plugin-declared server configs.
    pub(crate) fn plugin_servers(&self) -> Vec<McpServerConfig> {
        self.plugin_servers.read_recover().clone()
    }
    /// The host resource clients.
    pub(crate) fn resource_clients(&self) -> Vec<Arc<dyn McpResourceClient>> {
        self.resource_clients.clone()
    }
    /// Publish the live client set derived from the connection manager.
    pub(crate) fn set_runtime_clients(&self, clients: Vec<Arc<ManagedClient>>) {
        *self.runtime_clients.write_recover() = clients;
    }
    /// A snapshot of the live plugin MCP clients.
    pub(crate) fn runtime_clients_snapshot(&self) -> Vec<Arc<ManagedClient>> {
        self.runtime_clients.read_recover().clone()
    }
    /// The host MCP-server instructions mapped for the system prompt, or `None`
    /// when none were declared.
    pub(crate) fn server_instructions(&self) -> Option<Vec<McpServerInstructions>> {
        if self.instructions.is_empty() {
            None
        } else {
            Some(
                self.instructions
                    .iter()
                    .map(|m| McpServerInstructions {
                        name: m.name.clone(),
                        instructions: m.instructions.clone(),
                    })
                    .collect(),
            )
        }
    }
}

// Session construction, config resolution, and lock-scoped state accessors.
impl Session {
    pub fn new(init: SessionInit) -> Arc<Session> {
        Self::try_new(init).unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn try_new(init: SessionInit) -> Result<Arc<Session>, SessionInitError> {
        Self::try_new_with_message_queue(init, None)
    }

    pub(crate) async fn prepare_review_requirements_for_startup(
        provider: Arc<dyn crate::types::provider::ModelProvider>,
        user_config_dir: Option<&crate::runtime_values::RuntimePath>,
    ) -> Result<super::PreparedReviewRequirements, SessionInitError> {
        super::session_inference_part::prepare_review_requirements_for_provider(
            provider,
            user_config_dir,
        )
        .await
        .map_err(|error| SessionInitError::ApprovalRequirements {
            message: error.to_string(),
        })
    }

    /// Constructs an eligible authenticated transport after its cloud requirements load.
    pub async fn try_new_async(init: SessionInit) -> Result<Arc<Session>, SessionInitError> {
        let prepared = super::session_inference_part::prepare_review_requirements_for_provider(
            init.provider.clone(),
            init.user_config_dir.as_ref(),
        )
        .await
        .map_err(|error| SessionInitError::ApprovalRequirements {
            message: error.to_string(),
        })?;
        let session = Self::try_new_with_review_requirements(init, None, Some(prepared))?;
        let mut construction = UnpublishedSessionGuard(Some(session.clone()));
        if let Some(rollout) = &session.persistence.ordered_rollout {
            rollout
                .wait_prefixes()
                .await
                .map_err(|source| SessionInitError::Persistence { source })?;
        }
        construction.0.take();
        Ok(session)
    }

    pub(crate) fn try_new_with_message_queue(
        init: SessionInit,
        initial_queue: Option<(MessageQueue, bool)>,
    ) -> Result<Arc<Session>, SessionInitError> {
        Self::try_new_with_review_requirements(init, initial_queue, None)
    }

    pub(crate) fn try_new_with_review_requirements(
        mut init: SessionInit,
        initial_queue: Option<(MessageQueue, bool)>,
        prepared: Option<super::PreparedReviewRequirements>,
    ) -> Result<Arc<Session>, SessionInitError> {
        let fail_requirements = |error: std::io::Error| SessionInitError::ApprovalRequirements {
            message: error.to_string(),
        };
        let (review_cloud, approval_requirements) = match prepared {
            Some(prepared) => {
                if !Arc::ptr_eq(&prepared.provider, &init.provider) {
                    return Err(SessionInitError::ApprovalRequirements {
                        message: "Prepared approval requirements belong to a different provider."
                            .into(),
                    });
                }
                (prepared.cloud, prepared.requirements)
            }
            None if init.provider.managed_review_cloud_auth_source().is_some() => {
                return Err(SessionInitError::ApprovalRequirements { message: "The selected provider requires asynchronous approval requirements preparation.".into() });
            }
            None => (
                None,
                Arc::new(
                    crate::engine::approval_requirements::load_managed_review_requirements_sync()
                        .map_err(fail_requirements)?,
                ),
            ),
        };
        let requested_sandbox_mode = init.config.sandbox_mode;
        super::session_core_policy_part::apply_startup_review_requirements(
            &mut init.config,
            &approval_requirements,
        )
        .map_err(fail_requirements)?;
        if requested_sandbox_mode != init.config.sandbox_mode {
            init.network_allowed = None;
        }
        let deps = SharedDeps::from_engine(init.deps);
        let id = init
            .id
            .unwrap_or_else(|| RuntimeString::from((deps.uuid)()));
        let legacy_plan = init.config.permission_mode == ExternalPermissionMode::Plan;
        if legacy_plan {
            init.config.permission_mode = ExternalPermissionMode::Default;
        }
        let planning_root = init
            .user_config_dir
            .as_ref()
            .map(|root| {
                std::path::PathBuf::from(crate::planning::runtime::host(root.as_runtime_string()))
            })
            .or_else(|| Some(std::path::PathBuf::from(crate::fs::paths::config_home())));
        let planning = Arc::new(crate::planning::runtime::PlanningRuntime::new(
            id.clone(),
            planning_root,
            legacy_plan,
            init.config.request_user_input_default,
        ));
        planning
            .snapshot()
            .map_err(|error| SessionInitError::PlanningState {
                message: crate::planning::runtime::host(&error),
            })?;
        let mut cwd = resolve_abs(&init.cwd);
        let mut original_cwd = cwd.clone();
        let mut current_worktree: Option<WorktreeSession> = None;
        // Resume into an active worktree: live cwd is the worktree path and the
        // restore target is its recorded originalCwd.
        if let Some(wt) = init.current_worktree {
            cwd = wt.worktree_path.clone();
            original_cwd = wt.original_cwd.clone();
            current_worktree = Some(wt);
        }

        if init.config.task_resource_scope.is_some() {
            if init.config.sandbox_mode
                == Some(crate::types::permission::SandboxMode::DangerFullAccess)
            {
                init.config.sandbox_mode =
                    Some(crate::types::permission::SandboxMode::WorkspaceWrite);
            }
            init.config.use_stored_permission_rules = false;
            init.config.network_proxy.enabled = false;
            init.config.auto_memory = false;
            init.config.agent_memory = false;
            init.config.memory_recall = false;
            init.config.agent_memory_snapshot = false;
            init.config.auto_dream = false;
            init.config.fork = false;
            init.config.worktree = false;
            init.config.memory.enabled = false;
            init.config.memory.user = false;
            init.config.memory.project = false;
            init.config.memory.local = false;
        }

        let inference_rates = init
            .pricing
            .clone()
            .unwrap_or_else(|| crate::observability::cost_tracker::DEFAULT_PRICING.clone());
        let mut cost_tracker = match init.pricing {
            Some(p) => CostTracker::with_pricing(p),
            None => CostTracker::new(),
        };
        for record in init.initial_usage_ledger.unwrap_or_default() {
            cost_tracker.restore(&record);
        }
        let cost = Arc::new(Mutex::new(cost_tracker));
        let raw_rollout = init.rollout.take();
        let usage_receipts = raw_rollout
            .as_ref()
            .map(|raw| crate::store::receipt_broker::ReceiptBroker::new(raw.clone()));
        let ordered_rollout = raw_rollout.map(crate::store::session_rollout::SessionRollout::new);
        if let (Some(rollout), Some(broker)) = (&ordered_rollout, &usage_receipts) {
            let broker = broker.clone();
            rollout.set_write_fence(Arc::new(move || {
                let broker = broker.clone();
                Box::pin(async move { broker.flush_committed().await })
            }));
        }
        init.rollout = ordered_rollout
            .as_ref()
            .map(|rollout| rollout.clone() as Arc<dyn crate::store::session_store::LiveRollout>);
        let background_process_persistence = init.rollout.as_ref().map(|rollout| {
            Arc::new(RolloutBackgroundProcessPersistence::new(rollout.clone()))
                as BackgroundProcessPersistenceSink
        });
        let background_process_output_dir = init.store.as_ref().and_then(|store| {
            store.background_process_output_dir(&runtime_session_id_to_session_store_host(&id))
        });
        let background_processes = Arc::new(BackgroundCommandRegistry::with_options(
            Some(deps.now.clone()),
            background_process_persistence,
            None,
            background_process_output_dir,
            init.initial_background_processes.unwrap_or_default(),
        ));
        background_processes.set_max_poll_wait_ms(init.config.background_terminal_max_timeout);
        let mut subagent_lifecycle = SubagentLifecycleRegistry::restore_latest(
            init.initial_subagent_lifecycle.unwrap_or_default(),
        );
        let recovery_transitions =
            subagent_lifecycle.interrupt_nonterminal_for_recovery((deps.now)());
        let mut startup_records: Vec<_> = background_processes
            .take_initial_records()
            .into_iter()
            .map(|record| {
                crate::schemas::rollout::RolloutItem::BackgroundProcessLifecycle(
                    crate::schemas::rollout::BackgroundProcessLifecycleItem { record },
                )
            })
            .collect();
        startup_records.extend(recovery_transitions.into_iter().map(|record| {
            crate::schemas::rollout::RolloutItem::SubagentLifecycle(
                crate::schemas::rollout::SubagentLifecycleItem { record },
            )
        }));
        // Install the barrier before any queue or recovery producer receives the
        // wrapper. Its owner starts only after construction has succeeded.
        let startup_owner = ordered_rollout
            .as_ref()
            .map(|rollout| rollout.register_prefix(startup_records))
            .transpose()
            .map_err(|source| SessionInitError::Persistence { source })?;
        let memory_maintenance = init.initial_memory_maintenance.unwrap_or_default();
        let mut recovered_memory_maintenance = memory_maintenance.clone();
        let maintenance_recovery =
            recovered_memory_maintenance.interrupt_nonterminal_for_recovery((deps.now)());
        // Recovery-generated terminals belong to this process. Keep the
        // replayed Started state query-visible until a retained durability
        // owner appends and flushes the synthesized terminal in this process.
        let maintenance_registry = MaintenanceRegistry {
            pending_recovery: maintenance_recovery,
            ..Default::default()
        };
        let mut setup_lifecycle = init.initial_setup_lifecycle.unwrap_or_default();
        let setup_recovery = setup_lifecycle.interrupt_nonterminal_for_recovery((deps.now)());
        let mut setup_durability = SetupDurabilityRegistry::from_lifecycle(&setup_lifecycle);
        // Recovery-generated terminals belong to this process and have not yet
        // crossed its rollout flush boundary. Override the replay-ready receipt
        // with a pending one; after Arc<Session> construction the same retained
        // durability owner used by live terminals appends and flushes it.
        for event in &setup_recovery {
            match event {
                SetupLifecycleEvent::Run { run } => {
                    let mut receipt = SetupReceipt::pending();
                    receipt.terminal_record = Some(run.clone());
                    setup_durability
                        .receipts
                        .insert(run.request_id.clone(), receipt);
                    if !setup_durability
                        .recovery_receipt_ids
                        .contains(&run.request_id)
                    {
                        setup_durability
                            .recovery_receipt_ids
                            .push(run.request_id.clone());
                    }
                }
                SetupLifecycleEvent::ContextAdmission { admission } => {
                    let mut receipt = SetupContextAdmissionReceipt::pending();
                    let _ = receipt.started_send.send(Some(Ok(())));
                    receipt.set_terminal_record(admission.clone());
                    setup_durability
                        .context_admission_receipts
                        .insert(admission.admission_id.clone(), receipt);
                    if !setup_durability
                        .context_recovery_receipt_ids
                        .contains(&admission.admission_id)
                    {
                        setup_durability
                            .context_recovery_receipt_ids
                            .push(admission.admission_id.clone());
                    }
                }
                SetupLifecycleEvent::InitClosed { .. }
                | SetupLifecycleEvent::ContextConsumed { .. } => {}
            }
        }
        let agent_memory_snapshot_decisions = AgentMemorySnapshotDecisionRegistry::default();
        for audit in init
            .initial_agent_memory_snapshot_decisions
            .unwrap_or_default()
        {
            // A malformed historical diagnostic must not make the session
            // unresumable; valid records retain exactly-once identity.
            let _ = agent_memory_snapshot_decisions.restore(audit);
        }
        let snapshot_durability = SnapshotDurabilityRegistry {
            pending_recovery: agent_memory_snapshot_decisions
                .all()
                .into_iter()
                .filter(|audit| {
                    matches!(
                        audit.status,
                        AgentMemorySnapshotDecisionStatus::Requested
                            | AgentMemorySnapshotDecisionStatus::DecisionRecorded
                    )
                })
                .collect(),
            ..Default::default()
        };
        let (snapshot_fatal_events, _) = tokio::sync::watch::channel(None);
        let (agent_memory_snapshot_events, _) = tokio::sync::broadcast::channel(128);
        let recovered_memory_cursor = memory_maintenance.auto_memory_cursor_message_uuid.clone();
        let has_workflow_tool = init.tools.iter().any(|t| t.name() == "workflow");
        let mut surfaced: HashSet<RuntimeString> = HashSet::new();
        if let Some(names) = init.initial_surfaced_tools {
            for n in names {
                surfaced.insert(n);
            }
        }
        let durable_permissions = init.durable_permissions.take().unwrap_or_default();
        let static_permission_engine =
            StaticPermissionEngine::with_durable_rules(&init.config, durable_permissions.clone());
        let task_scope = static_permission_engine
            .task_scope(&cwd)
            .map_err(|message| SessionInitError::SandboxSelection { message })?;
        let network_allowed = super::runtime_network_allowed(
            init.config.sandbox_mode,
            init.network_allowed,
            &init.config.sandbox_workspace_write,
        );
        let native_config_home = init.user_config_dir.clone().unwrap_or_else(|| {
            crate::runtime_values::RuntimePath::from(crate::fs::paths::config_home())
        });
        let network_sandbox_required =
            init.config.network_proxy.enabled || init.trusted_windows_sandbox_context.is_some();
        let requested_mode = init
            .config
            .sandbox_mode
            .unwrap_or(crate::types::permission::SandboxMode::ReadOnly);
        let executor_mode = if (network_sandbox_required || task_scope.is_some())
            && requested_mode == crate::types::permission::SandboxMode::DangerFullAccess
        {
            crate::types::permission::SandboxMode::WorkspaceWrite
        } else {
            requested_mode
        };
        let sandbox = create_sandbox_for_mode_with_engine_config(
            executor_mode,
            &init.config,
            Some(&native_config_home),
        )
        .map_err(|message| SessionInitError::SandboxSelection { message })?;
        #[cfg(windows)]
        let mut windows_network = None;
        let sandbox: Arc<dyn SandboxBackend> = if init.config.network_proxy.enabled {
            let network_config = init
                .config
                .network_proxy
                .resolve_for_sandbox(sandbox.kind())
                .map_err(|source| SessionInitError::NetworkProxy { source })?;
            // Complete sandbox validation before starting any session listener.
            ManagedNetworkSandboxBackend::validate_kind(sandbox.kind())
                .map_err(|source| SessionInitError::ManagedNetworkSandbox { source })?;
            if cfg!(windows)
                && !crate::network_proxy::windows_sandbox_supports_network_transport_for_kind(
                    sandbox.kind(),
                    network_config.transport,
                )
            {
                return Err(SessionInitError::TrustedWindowsContext {
                    message: if sandbox.kind() == crate::types::sandbox::SandboxKind::WindowsMxc {
                        "managed native MXC networking requires transport=local with dedicated loopback listeners".to_string()
                    } else {
                        "managed Windows networking requires transport=windows-broker; local transport cannot enforce SID attribution".to_string()
                    },
                });
            }
            let proxy = match network_config.transport {
                NetworkProxyTransport::Local => {
                    if init.trusted_windows_sandbox_context.is_some() {
                        return Err(SessionInitError::TrustedWindowsContext {
                            message: "trusted context requires transport=windows-broker"
                                .to_string(),
                        });
                    }
                    NetworkProxy::start_sync(network_config.clone())
                }
                NetworkProxyTransport::WindowsBroker => {
                    if let Some(context) = init.trusted_windows_sandbox_context.take() {
                        // An explicitly trusted external route remains borrowed.
                        NetworkProxy::from_trusted_windows_context(network_config.clone(), context)
                    } else {
                        #[cfg(windows)]
                        {
                            NetworkProxy::start_session_owned(&id, network_config.clone())
                        }
                        #[cfg(not(windows))]
                        {
                            return Err(SessionInitError::TrustedWindowsContext {
                                message:
                                    "managed Windows networking requires a trusted broker route"
                                        .to_string(),
                            });
                        }
                    }
                }
            }
            .map_err(|source| SessionInitError::NetworkProxy { source })?;
            #[cfg(windows)]
            {
                windows_network = proxy.session_broker_owner();
            }
            Arc::new(
                ManagedNetworkSandboxBackend::try_new(sandbox, proxy)
                    .map_err(|source| SessionInitError::NetworkProxy { source })?,
            )
        } else if let Some(context) = init.trusted_windows_sandbox_context.take() {
            Arc::new(
                WindowsNetworkGuardSandboxBackend::try_new(sandbox, context)
                    .map_err(|source| SessionInitError::NetworkProxy { source })?,
            )
        } else {
            sandbox
        };
        let sandbox: Arc<dyn SandboxBackend> = if let Some(scope) = task_scope {
            Arc::new(
                crate::sandbox::task_scope::TaskScopedSandbox::new(sandbox, scope)
                    .map_err(|message| SessionInitError::SandboxSelection { message })?,
            )
        } else {
            sandbox
        };
        let governor = ResourceGovernor::new(ResourceGovernorOptions {
            max_concurrent_sub_agents: Some(init.config.max_concurrent_sub_agents as usize),
            max_concurrent_threads_per_session: init
                .config
                .max_concurrent_threads_per_session
                .map(|total| total as usize),
        });
        let permission_engine: Arc<dyn PermissionEngine> = Arc::new(static_permission_engine);
        let agent_registry = AgentRegistry::new(init.agents.unwrap_or_default());

        // Async-hook sink: deliver a background hook's context at the next
        // continuation boundary as a deferred task-notification.
        let hook_bus = init.hook_bus.clone();
        let (mq, queue_recovery_changed) = initial_queue
            .map(|(queue, changed)| (Arc::new(queue), changed))
            .unwrap_or_else(|| (Arc::new(MessageQueue::for_session(id.clone())), false));
        if let Some(rollout) = init.rollout.clone() {
            mq.set_persistence_sink(Arc::new(move |state| {
                let rollout = rollout.clone();
                Box::pin(async move {
                    rollout
                        .append_event(
                            QUEUE_STATE_ROLLOUT_EVENT.into(),
                            Some(AppendEventOptions {
                                turn_id: None,
                                payload: Some(
                                    runtime_codec::encode_value(&state)
                                        .expect("queue checkpoint has an exact runtime codec"),
                                ),
                            }),
                        )
                        .await;
                    rollout
                        .flush()
                        .await
                        .map_err(|error| RuntimeString::from(error.to_string()))
                })
            }));
            if queue_recovery_changed {
                // The old process died while owning reserved work. Persist the
                // atomic `interrupted` normalization before exposing this new
                // Session to a reconnecting Host.
                mq.persist_current_state()
                    .expect("recovered queue accepts its normalization checkpoint");
            }
        }
        let disposed = Arc::new(AtomicBool::new(false));

        // Adapt host MCP clients to the resource/prompt view + fold instructions.
        let mcp_resource_clients: Vec<Arc<dyn McpResourceClient>> = init
            .mcp_clients
            .iter()
            .map(|c| {
                Arc::new(ManagedResourceClient(c.clone(), "mcp-instructions"))
                    as Arc<dyn McpResourceClient>
            })
            .collect();
        let mcp_instructions: Vec<McpInstruction> = mcp_resource_clients
            .iter()
            .filter_map(|c| {
                c.instructions().map(|i| McpInstruction {
                    name: c.name().clone(),
                    instructions: i.clone(),
                })
            })
            .collect();

        // Swarm identity (team + peer); peer defaults to a session-derived name.
        let swarm_identity = init.swarm.map(|s| SwarmIdentity {
            team: s.team,
            peer: s.peer.unwrap_or_else(|| derive_peer_name(&id)),
        });

        // Language servers (engine owns their lifecycle; host supplies commands).
        let lsp_configs = if init.config.lsp {
            init.lsp_servers.as_ref().map(|f| f()).unwrap_or_default()
        } else {
            Vec::new()
        };
        let lsp_manager = if init.config.lsp && !lsp_configs.is_empty() {
            Some(Arc::new(LspManager::new(
                init.process_context.clone(),
                lsp_configs.clone(),
                cwd.clone(),
                LspClientOptions {
                    request_timeout_ms: Some(init.config.tool_timeout_ms),
                    diagnostics_settle_ms: None,
                },
            )))
        } else {
            None
        };

        // Per-agent memory: enabled by config; host layout or the default.
        let core_project_memory_dir =
            default_agent_memory_layout(None, Some(&runtime_path_to_agent_memory_host(&cwd)))
                .dir_for("main", AgentMemoryScope::Project);
        let uses_core_default_memory_host = init.config.agent_memory && init.agent_memory.is_none();
        let agent_memory_host: Option<Arc<dyn AgentMemoryHost>> = if init.config.agent_memory {
            Some(init.agent_memory.unwrap_or_else(|| {
                Arc::new(DefaultAgentMemoryHost {
                    layout: default_agent_memory_layout(
                        None,
                        Some(&runtime_path_to_agent_memory_host(&cwd)),
                    ),
                }) as Arc<dyn AgentMemoryHost>
            }))
        } else {
            None
        };
        let main_memory = agent_memory_host
            .as_ref()
            .map(|h| h.layout().dir_for("main", AgentMemoryScope::Project));
        let (main_memory_pinned_root, main_memory_pin_error) = match main_memory.as_deref() {
            // SHARED, not per-session: a pin holds one directory handle per path
            // component for as long as the session lives, so pinning here per
            // session multiplied one directory's seven handles by the number of
            // live sessions and sub-agents. Measured at twenty pins of the same
            // path, 141 of the process's 256 descriptors.
            Some(dir) => match PinnedMemoryRoot::pin_or_create_shared(Path::new(dir)) {
                Ok(root) => (Some(root), None),
                Err(error) => (None, Some(RuntimeString::from(error.to_string()))),
            },
            None => (None, None),
        };
        let agent_memory_snapshot_policy = init.agent_memory_snapshot_policy.unwrap_or_else(|| {
            agent_memory_host
                .as_ref()
                .map(|host| host.snapshot_decision_policy())
                .unwrap_or_default()
        });
        let resolved_core_memory = resolve_abs(&RuntimePath::from(core_project_memory_dir));
        let core_memory_scope = main_memory_pinned_root.as_ref().and_then(|root| {
            root.validate_root_namespace().ok()?;
            let pinned = runtime_path_from_host_path(root.canonical_root());
            canonical_descendant_scope(
                &resolve_abs(&RuntimePath::from(config_home())),
                &runtime_path_from_host_path(root.logical_root()),
            )
            .filter(|canonical| canonical == &pinned)
        });
        let internal_memory_root = main_memory
            .as_ref()
            .map(|dir| resolve_abs(&RuntimePath::from(dir.clone())))
            .filter(|dir| uses_core_default_memory_host && dir == &resolved_core_memory)
            .and(core_memory_scope);

        let mut runtime_capabilities = init.runtime_capabilities;
        if let Some(runtime) = &init.browser_runtime {
            runtime_capabilities.register(
                RuntimeCapabilityRoot::host("session.browser")
                    .with_browser_runtime(runtime.clone()),
            );
        }
        let file_state = Arc::new(InMemoryFileStateCache::new());
        let nested_memory_loaded_paths = Arc::new(Mutex::new(HashSet::new()));
        let session_permission = Arc::new(Mutex::new(empty_session_permission_state()));

        // Resolve the active output style ONCE.
        let loaded_styles = init.output_styles.as_ref().map(|f| f());
        let configured_output_style = init.config.output_style.as_ref().map(|name| {
            name.try_to_utf8()
                .expect("validated output-style config is a scalar string")
        });
        let style =
            resolve_output_style(configured_output_style.as_deref(), loaded_styles.as_deref());
        let active_output_style = style.map(|s| ActiveOutputStyle {
            name: s.name.clone(),
            body: s.body.clone(),
        });
        let mut extension_runtime = init.extension_runtime;
        extension_runtime.selected_output_style =
            active_output_style.as_ref().map(|style| style.name.clone());
        let system_output_style = style.cloned();

        let system_prompt_is_default =
            init.system_prompt_provider.is_none() && init.base_instructions.is_none();
        let created_at = (deps.now)();
        let mut workspace_instruction_cache_refresh: Option<Arc<dyn Fn() + Send + Sync>> = None;
        let system_prompt_provider = if let Some(base) = init.base_instructions.clone() {
            let sp: SystemPromptProvider = Arc::new(move || {
                let base = base.clone();
                Box::pin(async move {
                    vec![SystemPromptPart {
                        name: "base".into(),
                        text: base,
                        cacheable: Some(true),
                        cache_scope: None,
                    }]
                })
            });
            sp
        } else {
            init.system_prompt_provider.unwrap_or_else(|| {
                let provider = Arc::new(create_system_prompt_provider(
                    init.process_context.clone(),
                    SystemPromptProviderOptions {
                        cwd: cwd.clone(),
                        config: init.config.clone(),
                        now_ms: Some(created_at),
                        is_worktree: current_worktree.is_some(),
                        mcp_instructions: if mcp_instructions.is_empty() {
                            None
                        } else {
                            Some(
                                mcp_instructions
                                    .iter()
                                    .map(|m| McpServerInstructions {
                                        name: m.name.clone(),
                                        instructions: m.instructions.clone(),
                                    })
                                    .collect(),
                            )
                        },
                        output_style: system_output_style.map(|s| {
                            crate::context::system_prompt::OutputStyle {
                                name: s.name,
                                body: s.body,
                            }
                        }),
                        user_config_dir: init.user_config_dir.clone(),
                    },
                ));
                let refresh = provider.clone();
                workspace_instruction_cache_refresh =
                    Some(Arc::new(move || refresh.invalidate_memory()));
                let sp: SystemPromptProvider = Arc::new(move || {
                    let p = provider.clone();
                    Box::pin(async move { p.prompt().await })
                });
                sp
            })
        };
        let developer_instructions = init.developer_instructions.clone();
        let system_prompt_provider =
            append_developer_instructions(system_prompt_provider, developer_instructions.clone());
        let shell_env: tokio::sync::OnceCell<Option<RuntimeEnv>> = match init.shell_env {
            Some(env) => tokio::sync::OnceCell::new_with(Some(Some(env))),
            None => tokio::sync::OnceCell::new(),
        };

        let observer = match init.observer {
            Some(None) => None,
            Some(Some(o)) => Some(o),
            None => Some(Arc::new(Mutex::new(create_otel_observer(
                CreateOtelObserverOptions {
                    meter_name: None,
                    now: None,
                    meter: None,
                },
            )))),
        };

        let mcp_elicitation_router =
            McpElicitationRouter::new(init.mcp_elicitation_presenter.clone(), deps.uuid.clone());
        let observation_pause = mcp_elicitation_router.pause_state();
        background_processes.set_observation_pause_state(observation_pause.clone());
        init.elicit_handler = init.elicit_handler.take().map(|handler| {
            crate::engine::observation_wait::elicit(handler, observation_pause.clone())
        });
        init.approval_handler = init.approval_handler.take().map(|handler| {
            crate::engine::observation_wait::approval(handler, observation_pause.clone())
        });
        // Handed to the manager now, filled by the host later: the manager has to
        // own a sink from the moment it can dial, but the host installs its
        // notification bridge after the session exists.
        let mcp_status_observer = Arc::new(crate::mcp::types::McpStatusObserverSlot::new());
        let mcp_manager = McpConnectionManager::new(
            init.process_context.clone(),
            McpConnectionManagerOptions {
                connect_options: Some(ConnectMcpOptions {
                    cwd: Some(cwd.clone()),
                    credential_store: init.credential_store.clone(),
                    mcp_authorize_handler: init.mcp_authorize_handler.clone(),
                    oauth_redirect_uri: init.mcp_oauth_redirect_uri.clone(),
                    call_timeout_ms: Some(init.config.tool_timeout_ms),
                    mcp_elicitation_router: Some(mcp_elicitation_router.clone()),
                    ..ConnectMcpOptions::new(init.process_context.clone())
                }),
                status_observer: Some(mcp_status_observer.clone()),
                ..Default::default()
            },
        );

        // Same shape as the MCP status observer: the runtime owns a sink from
        // the moment it can change a goal, and the host installs its bridge
        // after the session exists.
        let goal_observer_slot = Arc::new(crate::engine::session::GoalObserverSlot::new());
        let persistent_goal_owner = init.store.is_some()
            && init.rollout.is_some()
            && init.parent_thread_id.is_none()
            && matches!(
                init.session_role,
                None | Some(crate::schemas::rollout::SessionRole::Main)
            );
        let goal_runtime = crate::engine::session::build_goal_runtime(
            id.clone(),
            persistent_goal_owner,
            init.config.maximum_goal_token_budget,
            deps.uuid.clone(),
            deps.now.clone(),
            goal_observer_slot.clone(),
            observer.clone(),
        );

        if let Some(goal) = &goal_runtime {
            goal.set_initial_enabled(init.config.goal && init.config.task_resource_scope.is_none());
        }
        let meta = SessionMeta {
            id: id.clone(),
            created_at,
            cwd: cwd.as_runtime_string().clone(),
            model: init.config.model.clone(),
            title: None,
            originator: None,
            cli_version: None,
            source: None,
            role: init.session_role,
            parent_thread_id: init.parent_thread_id.clone(),
            forked_from_id: init.forked_from_id.clone(),
            git: None,
            // Attached later by the host via update_host_metadata.
            host_metadata: None,
        };

        // Rebuild the request-view budget memory BEFORE the transcript moves
        // into the conversation state: frozen ids and cached replacement bodies
        // must match what a resumed model already saw.
        let tool_result_budget_state = Arc::new(Mutex::new(
            crate::context::tool_result_budget::ReplacementState::reconstruct(
                init.initial_history.as_deref().unwrap_or_default(),
                init.initial_tool_result_budget_records.unwrap_or_default(),
            ),
        ));

        let approval_review = super::session_approval_review_part::ApprovalReviewRuntime::new(
            init.initial_approval_context.as_ref(),
            init.initial_approval_reviews,
        );
        // Restore the original transcript before admitting any new input. Once a new
        // user is accepted, suffix ingestion must not skip the restored prefix.
        approval_review
            .history
            .lock_recover()
            .ingest(init.initial_history.as_deref().unwrap_or_default(), "")
            .map_err(|error| SessionInitError::ApprovalRequirements {
                message: format!("Initial approval context could not be restored: {error:?}"),
            })?;

        let session = Arc::new(Session {
            id: id.clone(),
            meta,
            process_context: init.process_context,
            background_processes,
            inference: RwLock::new(Arc::new(super::InferenceBundle {
                revision: 0,
                main_revision: 0,
                change_id: None,
                provider: init.provider,
                review_cloud,
                review_requirements: approval_requirements,
                config: init.config.clone(),
                context_window_override: init.context_window_override,
                deps: deps.clone(),
                pricing: extension_runtime.pricing.clone(),
                targets: None,
                advisor_target: None,
                orchestrator_target: None,
                orchestration_mode: false,
                feature_targets: Default::default(),
                source: None,
                rates: inference_rates,
                context_downshift: None,
            })),
            inference_admission: Arc::new(Mutex::new(super::InferenceAdmission::default())),
            inference_changed: tokio::sync::Notify::new(),
            deps: super::SessionIdentityDeps {
                uuid: deps.uuid.clone(),
                now: deps.now.clone(),
            },
            precompute: Arc::new(crate::engine::query::PrecomputeRegistry::default()),
            config: init.config.clone(),
            tools: init.tools.clone(),
            host_refresh_tools: init.refresh_tools,
            workspace: SessionWorkspaceState {
                cwd: RwLock::new(cwd.clone()),
                original_cwd: RwLock::new(original_cwd),
                current_worktree: Mutex::new(current_worktree),
            },
            workflow_journal_log: Mutex::new(init.prior_workflow_journal.unwrap_or_default()),
            has_workflow_tool,
            swarm_identity: Mutex::new(swarm_identity),
            swarm_delivery_state: Arc::new(
                crate::tasks::swarm_store::MailboxDeliveryState::default(),
            ),
            created_swarm_team: Mutex::new(None),
            task_list_identity_gate: Mutex::new(()),
            task_list_identity_generation: AtomicU64::new(0),
            lsp_manager,
            lsp_configs,
            agent_memory_host,
            snapshot: SessionSnapshotState {
                policy: agent_memory_snapshot_policy,
                decisions: agent_memory_snapshot_decisions,
                persistence: AsyncPersistenceGate::new(),
                durability: Mutex::new(snapshot_durability),
                fatal_events: snapshot_fatal_events,
                self_ref: std::sync::OnceLock::new(),
                ordered_sink: Mutex::new(None),
                events: agent_memory_snapshot_events,
            },
            runtime_capabilities,
            selected_capability_roots: init.selected_capability_roots,
            memory_roots: SessionMemoryRoots {
                main: main_memory,
                pinned_root: main_memory_pinned_root,
                pin_error: main_memory_pin_error,
                internal_root: internal_memory_root,
                nested_triggers: Mutex::new(HashSet::new()),
                nested_loaded_paths: nested_memory_loaded_paths,
                invoked_skill_content: Mutex::new(HashMap::new()),
            },
            user_config_dir: init.user_config_dir.clone(),
            required_network_sandbox: network_sandbox_required.then(|| sandbox.clone()),
            runtime_policy: std::sync::RwLock::new(Arc::new(RuntimePolicySnapshot {
                state: RuntimePolicyState {
                    approval_policy: init.config.approval_policy,
                    sandbox_mode: init.config.sandbox_mode,
                    approvals_reviewer: init.config.approvals_reviewer,
                    generation: 0,
                    sandbox_workspace_write: init.config.sandbox_workspace_write.clone(),
                    network_allowed,
                },
                sandbox_backend: sandbox,
            })),
            #[cfg(windows)]
            windows_network,
            file_state,
            context_countdown_anchor: Arc::new(Mutex::new(Default::default())),
            governor,
            permission_engine,
            durable_permissions,
            agent_registry,
            root_agent_definition: init.root_agent_definition,
            approval_handler: init.approval_handler,
            approval_review,
            approval_async: Arc::new(Default::default()),
            approval_reviewer: init.approval_reviewer,
            elicit_handler: init.elicit_handler,
            async_questions: Arc::new(crate::engine::questions::AsyncQuestionRegistry::default()),
            mcp_elicitation_router,
            mcp_authorize_handler: init.mcp_authorize_handler,
            mcp_oauth_redirect_uri: init.mcp_oauth_redirect_uri,
            credential_store: init.credential_store,
            planning,
            mode: SessionModeControl {
                pending_model_override: Mutex::new(None),
                plan_name: Mutex::new(None),
                permission: session_permission,
            },
            turn: SessionTurnState {
                active_stack: Mutex::new(Vec::new()),
                next_generation: AtomicU64::new(1),
                latest_prediction_source: Mutex::new(None),
                active: AtomicBool::new(false),
                current_abort: Mutex::new(None),
                thread_started_emitted: AtomicBool::new(false),
                cache_policy_warned: AtomicBool::new(false),
                last_response_at_ms: AtomicI64::new(0),
                thinking_context_cold: AtomicBool::new(false),
                context_window_usage: Arc::new(Mutex::new(Default::default())),
                context_window_update_gate: Arc::new(tokio::sync::Mutex::new(())),
                padded_retired_tokens: AtomicU64::new(0),
                padded_high_water_tokens: AtomicU64::new(0),
                user_interrupted_turn: Mutex::new(None),
            },
            hook_bus,
            setup: SessionSetupState {
                serial: Arc::new(tokio::sync::Mutex::new(())),
                context_admission_serial: Arc::new(tokio::sync::Mutex::new(())),
                lifecycle: Mutex::new(setup_lifecycle),
                persistence: SyncPersistenceGate::new(),
                durability: Mutex::new(setup_durability),
            },
            task_list_subscriptions: Mutex::new(TaskListSubscriptionRegistry::default()),
            lifecycle: SessionLifecycleState {
                disposed,
                abandoned_resume_cleanup: AtomicBool::new(false),
                abandoned_rollout_shutdown: std::sync::OnceLock::new(),
                shutdown_completion: std::sync::OnceLock::new(),
            },
            persistence: SessionPersistenceState {
                rollout: init.rollout,
                ordered_rollout,
                usage_receipts,
                tool_tasks: Arc::new(crate::engine::query::RetainedToolTasks::new()),
                store: init.store,
                checkpoint_svc: Mutex::new(None),
            },
            mcp: SessionMcpRuntime {
                resource_clients: mcp_resource_clients,
                instructions: mcp_instructions,
                plugin_servers: RwLock::new(init.plugin_mcp_servers),
                manual_servers: RwLock::new(Vec::new()),
                manager_initialized: tokio::sync::OnceCell::new(),
                registration_revision: AtomicU64::new(0),
                status_observer: mcp_status_observer,
                manager: tokio::sync::Mutex::new(mcp_manager),
                runtime_clients: RwLock::new(Vec::new()),
                gate: Arc::new(tokio::sync::RwLock::new(())),
            },
            slash_commands: init.slash_commands,
            reminder_providers: std::sync::OnceLock::new(), // filled below
            skills_thunk: init.skills,
            skill_gate: init.skill_gate,
            edited_files_reminder: std::sync::OnceLock::new(), // filled below
            system_prompt_provider: RwLock::new(system_prompt_provider),
            system_prompt_is_default,
            workspace_instruction_cache_refresh: RwLock::new(workspace_instruction_cache_refresh),
            developer_instructions,
            active_output_style,
            extension_runtime,
            plugin_publication: Mutex::new(SessionPluginRuntime::default()),
            extension_failure_sink: Mutex::new(None),
            cost,
            shell_env: Arc::new(shell_env),
            observer,
            logger: init.logger,
            conversation: SessionConversationState {
                history: Mutex::new(init.initial_history.unwrap_or_default()),
                retained_goal_instructions: Mutex::new(Vec::new()),
                goal_submission_ids: Mutex::new(HashSet::new()),
                goal_completions: Mutex::new(Vec::new()),
                hidden_message_ids: init
                    .hidden_context_message_ids
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
                resumed_checkpoints: init.initial_checkpoints.unwrap_or_default(),
                queue: mq,
            },
            turn_index: Mutex::new(TurnIndex::default()),
            tool_result_budget_state,
            context_hint_state: Arc::new(crate::context::context_hints::ContextHintState::new()),
            context_edit_state: Arc::new(
                crate::context::context_editing::ContextEditState::default(),
            ),
            file_changes: Mutex::new(SessionFileChanges::default()),
            subagents: SessionSubagentState {
                model_target_resolver: Mutex::new(None),
                notification_sink: Mutex::new(None),
                notification_ids_by_child: Mutex::new(HashMap::new()),
                event_sink: Mutex::new(None),
                background_agents: Arc::new(Mutex::new(HashMap::new())),
                lifecycle: Mutex::new(subagent_lifecycle),
                parent_rollouts: Arc::new(
                    crate::engine::subagent_rollouts::SubagentRollouts::default(),
                ),
                transcripts: Mutex::new(HashMap::new()),
                surfaced: Mutex::new(HashMap::new()),
                memory_recall_providers: Mutex::new(HashMap::new()),
            },
            maintenance: SessionMaintenanceState {
                snapshot: Mutex::new(memory_maintenance),
                query_fatal: std::sync::OnceLock::new(),
                persistence: SyncPersistenceGate::new(),
                event_sink: Mutex::new(None),
                isolated_sink: Mutex::new(IsolatedMaintenanceSinkState::default()),
            },
            surfaced_tools: Arc::new(Mutex::new(surfaced)),
            memory_scheduler: SessionMemoryScheduler {
                last_memory_message_uuid: Mutex::new(recovered_memory_cursor),
                extraction_in_progress: AtomicBool::new(false),
                extraction_pending: AtomicBool::new(false),
                last_dream_scan_at: AtomicI64::new(0),
                dream_in_progress: AtomicBool::new(false),
                next_dream_owner: AtomicU64::new(1),
                active_dream_owner: AtomicU64::new(0),
                session_memory_config: Mutex::new(
                    crate::memory::session_memory::SessionMemoryConfig::default(),
                ),
                session_memory_compact_config: Mutex::new(
                    crate::memory::session_memory::SessionMemoryCompactConfig::default(),
                ),
                session_memory_initialized: AtomicBool::new(false),
                tokens_at_last_session_memory: AtomicU64::new(0),
                last_session_memory_cursor_uuid: Mutex::new(None),
                last_summarized_message_id: Mutex::new(None),
                session_memory_started_at: AtomicI64::new(0),
                session_memory_in_progress: AtomicBool::new(false),
                session_memory_settled: Arc::new(tokio::sync::Notify::new()),
                session_memory_abort: Mutex::new(None),
            },
            #[cfg(test)]
            dream_before_lock_hook: Mutex::new(None),
            background_tasks: Mutex::new(BackgroundRunRegistry::default()),
            cron: CronRuntime::default(),
            goal: goal_runtime,
            goal_observer: goal_observer_slot,
            transport_prewarm: Mutex::new(None),
            transport_prewarm_consumed: AtomicBool::new(false),
            maintenance_tasks: Mutex::new(maintenance_registry),
            maintenance_admission: Arc::new(MaintenanceAdmissionTracker::default()),
        });

        let review_policy_owner = Arc::downgrade(&session);
        session
            .mcp_elicitation_router
            .set_permission_policy_provider(Arc::new(move || {
                use crate::mcp::elicitation::McpElicitationPolicy;
                use crate::types::permission::{ApprovalPolicy, SandboxMode};
                review_policy_owner
                    .upgrade()
                    .map(|session| {
                        let state = session.runtime_policy_state();
                        McpElicitationPolicy {
                            approval_policy: state.approval_policy.unwrap_or_default(),
                            sandbox_mode: state.sandbox_mode.unwrap_or(SandboxMode::ReadOnly),
                        }
                    })
                    .unwrap_or(McpElicitationPolicy {
                        approval_policy: ApprovalPolicy::Never,
                        sandbox_mode: SandboxMode::ReadOnly,
                    })
            }));
        if let Some(owner) = startup_owner {
            owner
                .start()
                .map_err(|source| SessionInitError::Persistence { source })?;
        }
        if let Some(rollout) = &session.persistence.ordered_rollout {
            if let Some(Err(source)) = rollout.completed_prefix_result() {
                return Err(SessionInitError::Persistence { source });
            }
        }
        session.start_pending_setup_recovery_owners();
        session.start_pending_memory_maintenance_recovery_owners();

        if let Some(bus) = &session.hook_bus {
            session.install_async_hook_sink(bus);
        }

        // Reminder providers read live state through independent state handles or
        // weak Session references so retained query snapshots cannot keep a closed
        // Session and its history alive.
        let edited: Option<Arc<dyn ReminderProvider>> = if init.config.edited_files {
            let file_state = session.file_state.clone();
            Some(Arc::new(create_edited_files_reminder_provider(move || {
                file_state.clone() as Arc<dyn FileStateCache>
            })))
        } else {
            None
        };
        let _ = session.edited_files_reminder.set(edited);
        let providers = Session::build_reminder_providers(&session, &init.config);
        let _ = session.reminder_providers.set(providers);
        // Constructor recovery is deliberately lazy: restore only records the
        // immutable pending set. The first query/resolve bridge (or Agent's
        // explicit resume barrier) installs Session-owned durability owners.
        // This prevents constructor return from racing an eager visible
        // mutation before protocol has installed its ordered sink.
        session.snapshot.set_self_ref(Arc::downgrade(&session));
        if session.meta.role.is_none() {
            session
                .inference
                .read()
                .unwrap()
                .deps
                .compactor
                .bind_session_memory(Some(Arc::new(super::SessionMemoryCompactAdapter {
                    session: Arc::downgrade(&session),
                })));
        }
        // Scheduled prompts: auto-start the cron scheduler when the project
        // file already has tasks (CronCreate starts it lazily otherwise).
        session.start_cron_scheduler_if_project_has_tasks();
        // Provider transport prewarm: complete the endpoint handshake in the
        // background so the first model request reuses a warm connection.
        session.schedule_transport_prewarm();
        Ok(session)
    }

    /// Build the per-turn ephemeral reminder providers. Assembled after the
    /// `Arc<Session>` exists so the provider closures read live session state.
    pub(crate) fn build_reminder_providers(
        this: &Arc<Session>,
        config: &ResolvedConfig,
    ) -> Vec<Arc<dyn ReminderProvider>> {
        let mut out: Vec<Arc<dyn ReminderProvider>> = Vec::new();
        // Main-loop memory recall: pull relevant saved memories into a per-turn
        // reminder, over the main memory dir.
        if config.memory_recall {
            if let Some(dir) = this.memory_roots.main().cloned() {
                let session = Arc::downgrade(this);
                let now = this.deps.now.clone();
                let now_for_usage = now.clone();
                out.push(Arc::new(create_memory_recall_reminder_provider(
                    MemoryRecallDeps {
                        recall: Box::new(move |query: RuntimeString, ctx: RecallContext| {
                            let dir = dir.clone();
                            let now_for_usage = now_for_usage.clone();
                            let capture = session
                                .upgrade()
                                .ok_or_else(|| "memory recall session closed".to_string())
                                .and_then(|session| session.begin_inference_activity());
                            Box::pin(async move {
                                let capture = capture.map_err(|error| {
                                    Box::new(std::io::Error::other(error)) as RecallError
                                })?;
                                let bundle = capture.bundle.clone();
                                let call_model = bundle.deps.call_model_owned();
                                let model = bundle.config.model.clone();
                                let store = TypedMemoryStore::new(TypedMemoryStoreOptions {
                                    dir: Some(RuntimePath::from(dir.clone())),
                                    redact_secrets: None,
                                });
                                let picked = store
                                    .recall_semantic(
                                        &query,
                                        RecallSemanticOptions {
                                            call_model: &call_model,
                                            model,
                                            limit: None,
                                            recent_tools: Some(&ctx.recent_tools),
                                            exclude: Some(&ctx.exclude),
                                            signal: ctx.signal,
                                        },
                                    )
                                    .await
                                    .map_err(|e| Box::new(e) as RecallError)?;
                                if !picked.is_empty() {
                                    // Record recall usage so consolidation can tell
                                    // proven core memories from stale ones. The
                                    // ledger is best-effort statistics; a failure
                                    // here must never fail the recall itself.
                                    let stems: Vec<String> = picked
                                        .iter()
                                        .filter_map(|record| {
                                            record.path.try_to_utf8().ok().and_then(|path| {
                                                std::path::Path::new(&path)
                                                    .file_stem()
                                                    .map(|stem| stem.to_string_lossy().into_owned())
                                            })
                                        })
                                        .collect();
                                    let usage_dir = dir.clone();
                                    tokio::spawn(async move {
                                        crate::memory::usage::record_usage(
                                            &usage_dir,
                                            &stems,
                                            now_for_usage(),
                                        )
                                        .await;
                                    });
                                }
                                Ok(picked)
                            })
                        }),
                        now: Box::new(move || now()),
                        recent_tool_window: None,
                    },
                )));
            }
        }

        out.push(Arc::new(create_task_reminder_provider()));

        if config.swarm {
            let id = this.id.clone();
            let now = this.deps.now.clone();
            // Identity read live from the session identity cell each turn.
            let identity_session = Arc::downgrade(this);
            let receiver = Arc::downgrade(this);
            out.push(Arc::new(
                create_swarm_reminder_provider(
                    move || {
                        let session = identity_session.upgrade()?;
                        let identity = session.swarm_identity.lock_recover().clone();
                        identity.map(|i| SwarmStoreIdentity {
                            team: i.team,
                            peer: i.peer,
                        })
                    },
                    move || now(),
                    Some(id),
                )
                .with_delivery_state(this.swarm_delivery_state.clone())
                .with_delivery_sink(Arc::new(move |notification| {
                    let receiver = receiver.clone();
                    Box::pin(async move {
                        if let Some(session) = receiver.upgrade() {
                            session.publish_agent_notification(notification).await?;
                        }
                        Ok(())
                    })
                })),
            ));
        }

        if config.lsp {
            let session = Arc::downgrade(this);
            out.push(Arc::new(create_lsp_diagnostics_reminder_provider(
                move || {
                    session
                        .upgrade()
                        .and_then(|session| session.current_lsp_manager())
                },
            )));
        }

        out.push(Arc::new(create_ultrathink_reminder_provider()));
        out.push(Arc::new(create_ultracode_reminder_provider()));
        {
            let now = this.deps.now.clone();
            out.push(Arc::new(create_date_change_reminder_provider(move || {
                now()
            })));
        }

        // Install the provider even for an empty startup catalog so a later
        // host publication can advertise skills in this same conversation.
        {
            let skills_session = Arc::downgrade(this);
            let window_session = Arc::downgrade(this);
            out.push(Arc::new(
                create_skill_listing_reminder_provider(
                    move || {
                        let Some(session) = skills_session.upgrade() else {
                            return Vec::new();
                        };
                        if session
                            .current_tools()
                            .iter()
                            .any(|tool| tool.name() == "skill")
                        {
                            session.gated_skills()
                        } else {
                            Vec::new()
                        }
                    },
                    None,
                )
                .with_context_window(move || {
                    let session = window_session.upgrade()?;
                    let bundle = session.inference_snapshot();
                    let model = session
                        .pending_model()
                        .unwrap_or_else(|| bundle.config.model.clone());
                    let window = bundle
                        .context_window_override
                        .unwrap_or_else(|| bundle.provider.capabilities(&model).context_window);
                    Some(i64::try_from(window).unwrap_or(i64::MAX))
                }),
            ));
        }

        // Lazy nested memory (rides the master memory switch).
        if config.memory.enabled {
            let trigger_session = Arc::downgrade(this);
            out.push(Arc::new(create_nested_memory_reminder_provider(
                NestedMemoryReminderDeps {
                    drain_triggers: Box::new(move || {
                        trigger_session.upgrade().map_or_else(Vec::new, |session| {
                            Session::drain_nested_memory_triggers(
                                session.memory_roots.nested_triggers(),
                            )
                        })
                    }),
                    loaded_paths: this.memory_roots.nested_loaded_paths(),
                    file_state: this.file_state.clone() as Arc<dyn FileStateCache>,
                    gates: Some(memory_gates_from(&config.memory)),
                    user_config_dir: this.user_config_dir.clone(),
                },
            )));
        }
        out
    }

    // ---- small state accessors (lock-scoped, never held across await) ----

    pub(crate) fn cwd_now(&self) -> RuntimePath {
        self.workspace.cwd()
    }
    pub(crate) fn set_cwd(&self, v: RuntimePath) {
        self.workspace.set_cwd(v);
    }
    pub(crate) fn history_snapshot(&self) -> Vec<Message> {
        self.conversation.history_snapshot()
    }
    pub(crate) fn push_history(&self, m: Message) {
        self.conversation.push_history(m);
    }
    /// Whether session teardown has started or completed.
    pub fn is_disposed(&self) -> bool {
        self.lifecycle.is_disposed()
    }
    pub(crate) fn pending_model(&self) -> Option<RuntimeString> {
        self.mode.pending_model()
    }

    // ---- plan-file state (plan mode's on-disk plan document) ----

    /// The session's bound plan-document name, once `create_plan` has created it.
    pub(crate) fn plan_name(&self) -> Option<RuntimeString> {
        self.mode.plan_name.lock_recover().clone()
    }
    /// Bind the plan-document name. Called by `create_plan` when it builds the
    /// document, and by session resume to reopen a legacy random-slug file.
    pub(crate) fn set_plan_name(&self, name: RuntimeString) {
        *self.mode.plan_name.lock_recover() = Some(name);
    }
    /// The resolved plans directory: the `plansDirectory` config is honored
    /// only inside the project root; default is `<config_home>/plans`.
    pub(crate) fn plans_directory_now(&self) -> String {
        if let Some(scope) = self.permission_engine.bound_task_resource_scope() {
            let scratch = runtime_path_to_skill_shell_host(&scope.scratch_root());
            return plan_files::plans_directory(Some(".klaus/plans"), &scratch);
        }
        let cwd = runtime_path_to_skill_shell_host(&self.cwd_now());
        let configured = self
            .config
            .plans_directory
            .as_ref()
            .map(|s| String::from_utf8_lossy(&s.to_node_utf8()).into_owned());
        plan_files::plans_directory(configured.as_deref(), &cwd)
    }
    /// The plan-file context for an ALREADY BOUND document name, or `None` when
    /// the session has no plan yet.
    ///
    /// Restore the document binding recorded by older conversation history.
    /// New planning artifacts use their own thread-owned document store.
    pub(crate) fn plan_file_context(
        &self,
        agent_id: Option<&RuntimeString>,
    ) -> Option<PlanFileContext> {
        let name = self.plan_name()?;
        Some(self.plan_file_context_for(&name, agent_id))
    }
    /// Resolve a historical document binding without granting a new one.
    pub(crate) fn plan_file_context_for(
        &self,
        name: &RuntimeString,
        agent_id: Option<&RuntimeString>,
    ) -> PlanFileContext {
        let plans_dir = self.plans_directory_now();
        let name_str = String::from_utf8_lossy(&name.to_node_utf8()).into_owned();
        let path = match agent_id {
            Some(id) => plan_files::agent_plan_file_path(
                &plans_dir,
                &name_str,
                &String::from_utf8_lossy(&id.to_node_utf8()),
            ),
            None => plan_files::plan_file_path(&plans_dir, &name_str),
        };
        PlanFileContext {
            plans_dir: RuntimePath::from(plans_dir),
            name: name.clone(),
            path: RuntimePath::from(path),
        }
    }

    pub(crate) fn render_notice_for_model(
        &self,
        text: &RuntimeString,
        model: &RuntimeString,
    ) -> RuntimeString {
        let profile = resolve_provider_behavior_profile(Some(self.provider().info()), model);
        render_deferred_notice_for_profile(text, &profile)
    }

    pub(crate) fn render_deferred_notice(&self, text: &RuntimeString) -> RuntimeString {
        let model = self
            .pending_model()
            .unwrap_or_else(|| self.inference_config().model.clone());
        self.render_notice_for_model(text, &model)
    }

    pub(crate) fn render_background_completion_notice(
        &self,
        description: &RuntimeString,
        state: &str,
    ) -> RuntimeString {
        let model = self
            .pending_model()
            .unwrap_or_else(|| self.inference_config().model.clone());
        let profile = resolve_provider_behavior_profile(Some(self.provider().info()), &model);
        render_background_completion_notice_for_profile(description, state, &profile)
    }

    pub(crate) fn steering_model(&self) -> RuntimeString {
        self.turn
            .active_stack
            .lock_recover()
            .last()
            .map(|turn| turn.model.clone())
            .or_else(|| self.pending_model())
            .unwrap_or_else(|| self.inference_config().model.clone())
    }

    /// Validate and normalize a user input against the effective model for a
    /// turn. The returned value is the only value that may enter history.
    /// The model a turn will actually run on: an explicit per-turn override,
    /// else the queued pending selection, else the captured main model.
    pub(crate) fn effective_turn_model(&self, opts: &TurnOptions) -> RuntimeString {
        opts.model
            .clone()
            .or_else(|| self.pending_model())
            .unwrap_or_else(|| self.inference_config().model.clone())
    }

    // Keep InputValidationError's public field layout source-compatible for callers
    // that construct and destructure it by value.
    #[allow(clippy::result_large_err)]
    pub fn validate_input_for_turn(
        &self,
        input: Input,
        opts: &TurnOptions,
    ) -> Result<ValidatedInput, InputValidationError> {
        let _inference = self
            .begin_inference_activity_for(opts.model.as_ref())
            .map_err(|error| {
                InputValidationError::new(InputValidationErrorCode::UnsupportedInput, error)
            })?;
        let target = self.target_for_turn(opts).map_err(|error| {
            InputValidationError::new(InputValidationErrorCode::UnsupportedInput, error)
        })?;
        self.validate_input_for_provider(
            input,
            &target.model,
            target.provider.as_ref(),
            target.revision,
        )
    }

    // Keep InputValidationError's public field layout source-compatible for callers
    // that construct and destructure it by value.
    #[allow(clippy::result_large_err)]
    pub(crate) fn validate_input_for_model(
        &self,
        input: Input,
        model: &RuntimeString,
    ) -> Result<ValidatedInput, InputValidationError> {
        let bundle = self.inference_snapshot();
        self.validate_input_for_provider(
            input,
            model,
            bundle.provider.as_ref(),
            super::InputModelRevision::Main(bundle.main_revision),
        )
    }

    /// Validate against an already captured bundle instead of the live session,
    /// so a step that started on revision A is still checked with A's
    /// capabilities after B is published mid-step.
    pub(crate) fn validate_input_for_bundle(
        &self,
        input: Input,
        model: &RuntimeString,
        bundle: &super::InferenceBundle,
    ) -> Result<ValidatedInput, Box<InputValidationError>> {
        let _admission = self.inference_admission.lock_recover();
        self.validate_input_for_provider(
            input,
            model,
            bundle.provider.as_ref(),
            super::InputModelRevision::Main(bundle.main_revision),
        )
        .map_err(Box::new)
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn validate_input_for_provider(
        &self,
        input: Input,
        model: &RuntimeString,
        provider: &dyn crate::types::provider::ModelProvider,
        provider_revision: super::InputModelRevision,
    ) -> Result<ValidatedInput, InputValidationError> {
        let Input::Parts(parts) = input else {
            return Ok(ValidatedInput {
                input,
                pdf_bytes: 0,
                pdf_documents: 0,
                has_images: false,
                model: model.clone(),
                provider_revision,
            });
        };
        if !provider_revision.matches(&self.inference_snapshot()) {
            return Err(InputValidationError::new(
                InputValidationErrorCode::UnsupportedInput,
                "The model changed before attachment validation; retry with the current model",
            ));
        }

        // Admission retains images for history, persistence and later model
        // selections. Vision limits apply only to the captured request view.
        let has_images = parts
            .iter()
            .any(|part| matches!(part, UserContentPart::Image { .. }));

        let supported_media = provider.model_metadata(model).media_input_mime_types;
        let mut media_count = 0usize;
        let mut media_bytes = 0usize;
        let mut normalized_parts = Vec::with_capacity(parts.len());
        for part in parts {
            let UserContentPart::Media {
                base64,
                mime_type,
                filename,
            } = part
            else {
                normalized_parts.push(part);
                continue;
            };
            use crate::provider::native_media::{
                size_error, validate_mime, MAX_MEDIA_INPUT_BASE64_BYTES, MAX_MEDIA_INPUT_PARTS,
                MAX_MEDIA_INPUT_TOTAL_BYTES,
            };
            let mime_type = validate_mime(model, &mime_type, &supported_media)?;
            media_count += 1;
            if media_count > MAX_MEDIA_INPUT_PARTS
                || base64.len_utf16() > MAX_MEDIA_INPUT_BASE64_BYTES
            {
                return Err(size_error());
            }
            let invalid_base64 = || {
                InputValidationError::new(
                    InputValidationErrorCode::UnsupportedInput,
                    "native media payload must contain valid non-empty standard base64",
                )
            };
            let encoded = runtime_ascii_bytes(&base64).ok_or_else(invalid_base64)?;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| invalid_base64())?;
            if decoded.is_empty() {
                return Err(invalid_base64());
            }
            media_bytes = media_bytes
                .checked_add(decoded.len())
                .filter(|total| *total <= MAX_MEDIA_INPUT_TOTAL_BYTES)
                .ok_or_else(size_error)?;
            let filename = filename.as_ref().map(normalize_document_filename).transpose()
                .map_err(|_| InputValidationError::new(
                    InputValidationErrorCode::UnsupportedInput,
                    "native media filename must be a safe non-empty basename of at most 255 UTF-8 bytes",
                ))?;
            normalized_parts.push(UserContentPart::Media {
                base64,
                mime_type,
                filename,
            });
        }
        let parts = normalized_parts;

        // Cheap structural checks and the capability gate run before decoding.
        // Unsupported models therefore never spend CPU or allocate decoded
        // buffers for an input they cannot deliver.
        let mut document_count = 0usize;
        let mut encoded_pdf_bytes = 0usize;
        for part in &parts {
            let UserContentPart::Document {
                base64, mime_type, ..
            } = part
            else {
                continue;
            };
            document_count += 1;
            if document_count > MAX_DIRECT_PDF_DOCUMENTS {
                let mut error = InputValidationError::new(
                    InputValidationErrorCode::TooManyDocuments,
                    format!(
                        "PDF input exceeds the {MAX_DIRECT_PDF_DOCUMENTS}-document per-turn limit"
                    ),
                );
                error.detail.limit_count = Some(MAX_DIRECT_PDF_DOCUMENTS as u64);
                return Err(error);
            }
            if !runtime_ascii_eq_ignore_case(mime_type, "application/pdf") {
                return Err(InputValidationError::new(
                    InputValidationErrorCode::InvalidDocumentMime,
                    "document MIME type must be application/pdf",
                ));
            }
            encoded_pdf_bytes = encoded_pdf_bytes
                .checked_add(base64.len_utf16())
                .filter(|total| *total <= MAX_ENCODED_PDF_INPUT_BYTES)
                .ok_or_else(pdf_size_error)?;
        }
        if document_count > 0 {
            validate_native_pdf_provider(provider, model)?;
        }

        let mut decoded_pdf_bytes = 0usize;
        let mut normalized = Vec::with_capacity(parts.len());
        for part in parts {
            let UserContentPart::Document {
                base64,
                mime_type: _,
                filename,
            } = part
            else {
                normalized.push(part);
                continue;
            };
            let encoded = runtime_ascii_bytes(&base64).ok_or_else(|| {
                InputValidationError::new(
                    InputValidationErrorCode::InvalidDocumentBase64,
                    "document payload must be valid standard base64",
                )
            })?;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| {
                    InputValidationError::new(
                        InputValidationErrorCode::InvalidDocumentBase64,
                        "document payload must be valid standard base64",
                    )
                })?;
            decoded_pdf_bytes = decoded_pdf_bytes
                .checked_add(decoded.len())
                .filter(|total| *total <= MAX_DIRECT_PDF_INPUT_BYTES)
                .ok_or_else(pdf_size_error)?;
            if !decoded.starts_with(b"%PDF-") {
                return Err(InputValidationError::new(
                    InputValidationErrorCode::InvalidDocumentSignature,
                    "document payload is not a PDF",
                ));
            }

            let filename = filename
                .as_ref()
                .map(normalize_document_filename)
                .transpose()?;
            normalized.push(UserContentPart::Document {
                base64,
                mime_type: "application/pdf".into(),
                filename,
            });
        }

        Ok(ValidatedInput {
            input: Input::Parts(normalized),
            pdf_bytes: decoded_pdf_bytes,
            pdf_documents: document_count,
            has_images,
            model: model.clone(),
            provider_revision,
        })
    }

    // Keep InputValidationError's public field layout source-compatible for callers
    // that construct and destructure it by value.
    #[allow(clippy::result_large_err)]
    pub(crate) fn validate_native_pdf_model(
        &self,
        model: &RuntimeString,
    ) -> Result<(), InputValidationError> {
        validate_native_pdf_provider(self.provider().as_ref(), model)
    }

    pub(crate) async fn sync_agent_memory(
        &self,
        host: &dyn AgentMemoryHost,
        agent_type: &str,
        scope: AgentMemoryScope,
        dir: &str,
    ) {
        let _ = run_agent_memory_sync(
            host,
            AgentMemorySyncRequest {
                agent_type: agent_type.into(),
                scope,
                dir: dir.into(),
            },
        )
        .await;
    }

    pub(crate) fn snapshot_failure_result(
        kind: SnapshotDurabilityOperationKind,
        detail: RuntimeString,
    ) -> SnapshotDurabilityResult {
        let apply_error = || AgentMemorySnapshotApplyError::PersistenceInconsistent(detail.clone());
        match kind {
            SnapshotDurabilityOperationKind::Register => {
                SnapshotDurabilityResult::Register(Err(apply_error()))
            }
            SnapshotDurabilityOperationKind::Resolve => {
                SnapshotDurabilityResult::Resolve(Err(apply_error()))
            }
            SnapshotDurabilityOperationKind::Terminal => {
                SnapshotDurabilityResult::Terminal(Err(apply_error()))
            }
            SnapshotDurabilityOperationKind::Apply => {
                SnapshotDurabilityResult::Apply(Err(apply_error()))
            }
            SnapshotDurabilityOperationKind::RecoverMarked => {
                SnapshotDurabilityResult::RecoverMarked(Err(apply_error()))
            }
            SnapshotDurabilityOperationKind::SettleUnavailable => {
                SnapshotDurabilityResult::SettleUnavailable(Err(apply_error()))
            }
            SnapshotDurabilityOperationKind::ConstructorRecovery => {
                SnapshotDurabilityResult::ConstructorRecovery(Err(detail))
            }
        }
    }

    pub(crate) fn snapshot_fatal_detail(&self) -> Option<RuntimeString> {
        self.snapshot
            .durability
            .lock()
            .ok()
            .and_then(|registry| registry.fatal_persistence_inconsistency.clone())
    }

    pub(crate) fn snapshot_fatal_apply_error(&self) -> Option<AgentMemorySnapshotApplyError> {
        self.snapshot_fatal_detail()
            .map(AgentMemorySnapshotApplyError::PersistenceInconsistent)
    }

    pub(crate) fn fail_closed_snapshot_persistence_synchronously(
        &self,
        detail: impl Into<RuntimeString>,
    ) -> RuntimeString {
        let detail = detail.into();
        let mut registry = self
            .snapshot
            .durability
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let selected = registry
            .fatal_persistence_inconsistency
            .get_or_insert_with(|| detail.clone())
            .clone();
        registry.closing = true;
        if !registry.failures.iter().any(|failure| {
            failure
                .message
                .contains("snapshot fatal persistence inconsistency")
        }) {
            registry.failures.push(ShutdownPhaseFailure::error(format!(
                "snapshot fatal persistence inconsistency: {}",
                runtime_error_message_to_public_host(&selected)
            )));
        }
        self.snapshot
            .fatal_events
            .send_replace(Some(selected.clone()));
        selected
    }

    pub(crate) async fn fail_closed_snapshot_persistence(
        &self,
        detail: impl Into<RuntimeString>,
    ) -> RuntimeString {
        let _event_gate = self.snapshot.persistence.event_gate.lock().await;
        self.fail_closed_snapshot_persistence_synchronously(detail)
    }

    pub(crate) async fn wait_snapshot_receipt(
        &self,
        mut receipt: SnapshotDurabilityReceipt,
    ) -> Result<(), RuntimeString> {
        let mut fatal = self.snapshot.fatal_events.subscribe();
        loop {
            if let Some(detail) = fatal.borrow().clone() {
                return Err(detail);
            }
            if let Some(verdict) = receipt.borrow().clone() {
                return verdict;
            }
            tokio::select! {
                changed = receipt.changed() => {
                    if changed.is_err() {
                        return Err(RuntimeString::from(
                            "Session-owned snapshot durability task ended without a verdict",
                        ));
                    }
                }
                changed = fatal.changed() => {
                    if changed.is_err() {
                        return Err(RuntimeString::from(
                            "snapshot fatal persistence watch ended unexpectedly",
                        ));
                    }
                }
            }
        }
    }

    pub(crate) fn register_snapshot_durability_owner_locked(
        self: &Arc<Self>,
        registry: &mut SnapshotDurabilityRegistry,
        operation: SnapshotDurabilityOperation,
        allow_while_closing: bool,
        result_send: tokio::sync::oneshot::Sender<SnapshotDurabilityResult>,
        recovery_send: Option<tokio::sync::watch::Sender<Option<Result<(), RuntimeString>>>>,
    ) -> ShutdownPhaseResult {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(ShutdownPhaseFailure::error(
                "snapshot durability owner requires an active Tokio runtime",
            ));
        }
        if let Some(detail) = registry.fatal_persistence_inconsistency.clone() {
            return Err(ShutdownPhaseFailure::error(format!(
                "snapshot durability admission is fail-closed: {}",
                runtime_error_message_to_public_host(&detail)
            )));
        }
        if registry.tracker_closed || self.snapshot.persistence.finalized.load(Ordering::SeqCst) {
            return Err(ShutdownPhaseFailure::error(
                "snapshot durability admission is closed",
            ));
        }
        if registry.closing && !allow_while_closing {
            return Err(ShutdownPhaseFailure::error(
                "snapshot durability admission is closing",
            ));
        }

        let request_id = operation.request_id().clone();
        let kind = operation.kind();
        let predecessor = registry
            .provisionals
            .get(&request_id)
            .map(|provisional| provisional.receipt.clone());
        let task_id = registry.next_task_id;
        registry.next_task_id = registry.next_task_id.wrapping_add(1);
        let (receipt_send, receipt_receive) = tokio::sync::watch::channel(None);
        registry.provisionals.insert(
            request_id.clone(),
            SnapshotDurabilityProvisional {
                owner_id: task_id,
                receipt: receipt_receive,
            },
        );

        let (start_send, start_receive) = tokio::sync::oneshot::channel();
        let task_session = self.clone();
        let task_request_id = request_id.clone();
        let task = registry.task_tracker.spawn(async move {
            let _registration = SnapshotDurabilityTaskRegistration {
                session: Arc::downgrade(&task_session),
                request_id: task_request_id,
                task_id,
            };
            if start_receive.await.is_err() {
                return;
            }

            if let Some(predecessor) = predecessor {
                if let Err(error) = task_session.wait_snapshot_receipt(predecessor).await {
                    let detail = task_session
                        .fail_closed_snapshot_persistence(format!(
                            "snapshot durability predecessor failed for {}: {}",
                            runtime_prompt_to_background_host(&request_id),
                            runtime_error_message_to_public_host(&error)
                        ))
                        .await;
                    let _ = result_send.send(Self::snapshot_failure_result(kind, detail.clone()));
                    let _ = receipt_send.send(Some(Err(detail.clone())));
                    if let Some(recovery_send) = recovery_send {
                        let _ = recovery_send.send(Some(Err(detail)));
                    }
                    return;
                }
            }

            let execution = std::panic::AssertUnwindSafe(
                task_session.execute_snapshot_durability_operation(operation),
            )
            .catch_unwind()
            .await;
            let (result, durability_verdict, recovery_verdict) = match execution {
                Ok((result, durability_verdict, recovery_verdict)) => {
                    (result, durability_verdict, recovery_verdict)
                }
                Err(_) => {
                    let detail = task_session
                        .fail_closed_snapshot_persistence(format!(
                            "Session-owned snapshot {:?} durability task panicked for {}",
                            kind,
                            runtime_prompt_to_background_host(&request_id)
                        ))
                        .await;
                    (
                        Self::snapshot_failure_result(kind, detail.clone()),
                        Err(detail.clone()),
                        Err(detail),
                    )
                }
            };
            let _ = result_send.send(result);
            let _ = receipt_send.send(Some(durability_verdict));
            if let Some(recovery_send) = recovery_send {
                let _ = recovery_send.send(Some(recovery_verdict));
            }
        });
        let abort = task.abort_handle();
        registry
            .tasks
            .insert(task_id, SnapshotDurabilityTaskControl { abort });
        let _ = start_send.send(());
        drop(task);
        Ok(())
    }

    pub(crate) fn schedule_snapshot_durability_operation(
        self: &Arc<Self>,
        operation: SnapshotDurabilityOperation,
        allow_while_closing: bool,
        recovery_send: Option<tokio::sync::watch::Sender<Option<Result<(), RuntimeString>>>>,
    ) -> Result<tokio::sync::oneshot::Receiver<SnapshotDurabilityResult>, ShutdownPhaseFailure>
    {
        let (result_send, result_receive) = tokio::sync::oneshot::channel();
        let mut registry = self.snapshot.durability.lock().map_err(|_| {
            ShutdownPhaseFailure::error("snapshot durability registry was poisoned")
        })?;
        self.register_snapshot_durability_owner_locked(
            &mut registry,
            operation,
            allow_while_closing,
            result_send,
            recovery_send,
        )?;
        Ok(result_receive)
    }

    pub(crate) fn start_pending_snapshot_recovery_owners(self: &Arc<Self>) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let mut registry = self
            .snapshot
            .durability
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if registry.closing
            || registry.tracker_closed
            || registry.fatal_persistence_inconsistency.is_some()
        {
            return;
        }
        let pending = std::mem::take(&mut registry.pending_recovery);
        for audit in pending {
            let request_id = audit.request.request_id.clone();
            let (recovery_send, recovery_receive) = tokio::sync::watch::channel(None);
            registry
                .recovery_receipts
                .insert(request_id.clone(), recovery_receive);
            let (result_send, _result_receive) = tokio::sync::oneshot::channel();
            if let Err(error) = self.register_snapshot_durability_owner_locked(
                &mut registry,
                SnapshotDurabilityOperation::ConstructorRecovery { audit },
                false,
                result_send,
                Some(recovery_send),
            ) {
                let detail = RuntimeString::from(error.message.clone());
                registry.failures.push(error);
                let (_send, receive) = tokio::sync::watch::channel(Some(Err(detail)));
                registry.recovery_receipts.insert(request_id, receive);
            }
        }
    }

    pub(crate) async fn await_pending_snapshot_recovery_owners(
        self: &Arc<Self>,
    ) -> Result<(), RuntimeString> {
        self.start_pending_snapshot_recovery_owners();
        let (receipts, unowned) = {
            let registry = self.snapshot.durability.lock().map_err(|_| {
                RuntimeString::from("snapshot recovery receipt registry was poisoned")
            })?;
            (
                registry
                    .recovery_receipts
                    .iter()
                    .map(|(request_id, receipt)| (request_id.clone(), receipt.clone()))
                    .collect::<Vec<_>>(),
                registry.pending_recovery.len(),
            )
        };
        let mut failures = Vec::new();
        if unowned > 0 {
            failures.push(format!(
                "{unowned} restored snapshot recovery owner(s) could not be admitted"
            ));
        }
        for (request_id, receipt) in receipts {
            if let Err(error) = self.wait_snapshot_receipt(receipt).await {
                failures.push(format!(
                    "{}: {}",
                    runtime_prompt_to_background_host(&request_id),
                    runtime_error_message_to_public_host(&error)
                ));
            }
        }
        if let Some(detail) = self.snapshot_fatal_detail() {
            failures.push(runtime_error_message_to_public_host(&detail));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(RuntimeString::from(format!(
                "snapshot recovery durability failed: {}",
                failures.join("; ")
            )))
        }
    }

    pub(crate) async fn await_current_snapshot_owner(
        &self,
        request_id: &RuntimeString,
    ) -> Result<(), AgentMemorySnapshotApplyError> {
        let Some(session) = self.snapshot.resolve_self() else {
            return Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                RuntimeString::from("snapshot Session owner is unavailable"),
            ));
        };
        session.start_pending_snapshot_recovery_owners();
        let receipt = session
            .snapshot
            .durability
            .lock()
            .map_err(|_| {
                AgentMemorySnapshotApplyError::PersistenceInconsistent(RuntimeString::from(
                    "snapshot durability registry was poisoned",
                ))
            })?
            .provisionals
            .get(request_id)
            .map(|provisional| provisional.receipt.clone());
        if let Some(receipt) = receipt {
            if let Err(error) = self.wait_snapshot_receipt(receipt).await {
                return Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                    error,
                ));
            }
        }
        if let Some(error) = self.snapshot_fatal_apply_error() {
            return Err(error);
        }
        Ok(())
    }

    pub(crate) async fn await_all_snapshot_owners(
        &self,
    ) -> Result<(), AgentMemorySnapshotApplyError> {
        let Some(session) = self.snapshot.resolve_self() else {
            return Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                RuntimeString::from("snapshot Session owner is unavailable"),
            ));
        };
        session.start_pending_snapshot_recovery_owners();
        let receipts = session
            .snapshot
            .durability
            .lock()
            .map_err(|_| {
                AgentMemorySnapshotApplyError::PersistenceInconsistent(RuntimeString::from(
                    "snapshot durability registry was poisoned",
                ))
            })?
            .provisionals
            .values()
            .map(|provisional| provisional.receipt.clone())
            .collect::<Vec<_>>();
        let mut failures = Vec::new();
        for receipt in receipts {
            if let Err(error) = self.wait_snapshot_receipt(receipt).await {
                failures.push(runtime_error_message_to_public_host(&error));
            }
        }
        if let Some(error) = self.snapshot_fatal_apply_error() {
            return Err(error);
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                RuntimeString::from(failures.join("; ")),
            ))
        }
    }

    pub(crate) fn publish_agent_memory_snapshot_audit_visible(
        &self,
        audit: &AgentMemorySnapshotDecisionAudit,
        publish_ordered_sink: bool,
    ) {
        if publish_ordered_sink {
            if let Some(sink) = self.snapshot.ordered_sink.lock_recover().clone() {
                let audit = audit.clone();
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(audit)));
            }
        }
        let _ = self.snapshot.events.send(audit.clone());
    }

    pub(crate) async fn append_agent_memory_snapshot_audit(
        &self,
        audit: &AgentMemorySnapshotDecisionAudit,
    ) -> ShutdownPhaseResult {
        if self.snapshot.persistence.finalized.load(Ordering::SeqCst) {
            return Err(ShutdownPhaseFailure::error(
                "snapshot decision persistence is finalized",
            ));
        }
        let payload = runtime_codec::encode_value(audit).map_err(|error| {
            ShutdownPhaseFailure::error(format!(
                "failed to encode snapshot decision audit: {error}"
            ))
        })?;
        if let Some(rollout) = self.persistence.rollout() {
            let appended = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(async {
                rollout
                    .append_event(
                        AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT.into(),
                        Some(AppendEventOptions {
                            turn_id: None,
                            payload: Some(payload),
                        }),
                    )
                    .await;
            }))
            .await;
            if appended.is_err() {
                return Err(ShutdownPhaseFailure::error(
                    "snapshot decision rollout append_event adapter panicked",
                ));
            }
        }
        Ok(())
    }

    pub(crate) async fn flush_agent_memory_snapshot_rollout(&self) -> ShutdownPhaseResult {
        if self.snapshot.persistence.finalized.load(Ordering::SeqCst) {
            return Err(ShutdownPhaseFailure::error(
                "snapshot decision persistence is finalized",
            ));
        }
        match std::panic::AssertUnwindSafe(self.flush_rollout())
            .catch_unwind()
            .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(ShutdownPhaseFailure::error(format!(
                "snapshot decision rollout flush failed: {error}"
            ))),
            Err(_) => Err(ShutdownPhaseFailure::error(
                "snapshot decision rollout flush adapter panicked",
            )),
        }
    }

    pub(crate) async fn persist_and_commit_snapshot_staged<T>(
        &self,
        staged: AgentMemorySnapshotStaged<T>,
        proposed_audit: Option<AgentMemorySnapshotDecisionAudit>,
        ordered_reply: &mut Option<SnapshotOrderedReply>,
        ordered_audit: Option<AgentMemorySnapshotDecisionAudit>,
    ) -> Result<T, AgentMemorySnapshotApplyError> {
        if let Some(audit) = &proposed_audit {
            if let Err(error) = self.append_agent_memory_snapshot_audit(audit).await {
                let detail = self
                    .fail_closed_snapshot_persistence(format!(
                        "snapshot audit append was attempted but did not complete safely: {error}"
                    ))
                    .await;
                return Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                    detail,
                ));
            }
            if let Err(error) = self.flush_agent_memory_snapshot_rollout().await {
                let detail = self
                    .fail_closed_snapshot_persistence(format!(
                        "snapshot audit append could not be proven durable: {error}"
                    ))
                    .await;
                return Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                    detail,
                ));
            }
        }

        let _event_gate = self.snapshot.persistence.event_gate.lock().await;
        if let Some(detail) = self.snapshot_fatal_detail() {
            return Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                detail,
            ));
        }
        let committed = match self.snapshot.decisions.commit_staged(staged) {
            Ok(committed) => committed,
            Err(error) if proposed_audit.is_some() => {
                let detail = self.fail_closed_snapshot_persistence_synchronously(format!(
                    "durable snapshot audit could not commit its staged registry CAS: {error}"
                ));
                return Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                    detail,
                ));
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(audit) = &proposed_audit {
            // An ordered resolve/settlement returns this audit as its
            // correlated wire result. That reply is itself the protocol
            // projection of the transition, so do not enqueue the same audit a
            // second time through the protocol sink. The in-process broadcast
            // remains authoritative for reconciliation observers.
            self.publish_agent_memory_snapshot_audit_visible(audit, ordered_audit.is_none());
        }
        if let Some(audit) = ordered_audit {
            if let Some(enqueue) = ordered_reply.take() {
                enqueue(audit);
            }
        }
        Ok(committed)
    }

    pub(crate) async fn persist_and_commit_snapshot_filesystem_staged<T>(
        &self,
        staged: AgentMemorySnapshotStagedFilesystemResult<T>,
        ordered_reply: &mut Option<SnapshotOrderedReply>,
        ordered_audit: Option<AgentMemorySnapshotDecisionAudit>,
    ) -> Result<T, AgentMemorySnapshotApplyError> {
        let proposed_audit = staged.proposed_audit().cloned();
        if let Some(audit) = &proposed_audit {
            if let Err(error) = self.append_agent_memory_snapshot_audit(audit).await {
                let detail = self
                    .fail_closed_snapshot_persistence(format!(
                        "filesystem-backed snapshot audit append was attempted but did not complete safely: {error}"
                    ))
                    .await;
                return Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                    detail,
                ));
            }
            if let Err(error) = self.flush_agent_memory_snapshot_rollout().await {
                let detail = self
                    .fail_closed_snapshot_persistence(format!(
                        "filesystem-backed snapshot audit append could not be proven durable: {error}"
                    ))
                    .await;
                return Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                    detail,
                ));
            }
        }

        let _event_gate = self.snapshot.persistence.event_gate.lock().await;
        if let Some(detail) = self.snapshot_fatal_detail() {
            return Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                detail,
            ));
        }
        let committed = self
            .snapshot
            .decisions
            .commit_staged_filesystem_checked(staged);
        let result = match committed {
            Ok(result) => result,
            Err(error) if proposed_audit.is_some() => {
                let detail = self.fail_closed_snapshot_persistence_synchronously(format!(
                    "durable filesystem-backed snapshot audit could not commit its staged registry CAS: {error}"
                ));
                return Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                    detail,
                ));
            }
            Err(error) => return Err(error.into()),
        };
        let admit_ordered_reply = result.is_ok();
        if let Some(audit) = &proposed_audit {
            debug_assert_eq!(
                self.snapshot
                    .decisions
                    .query(&audit.request.request_id)
                    .as_ref(),
                Some(audit),
                "checked filesystem CAS must publish its exact proposed audit"
            );
            self.publish_agent_memory_snapshot_audit_visible(audit, true);
        }
        if admit_ordered_reply {
            if let Some(audit) = ordered_audit {
                if let Some(enqueue) = ordered_reply.take() {
                    enqueue(audit);
                }
            }
        }
        result
    }

    pub(crate) async fn record_agent_memory_snapshot_terminal_under_serial(
        &self,
        request_id: &RuntimeString,
        expected: AgentMemorySnapshotDecisionStatus,
        status: AgentMemorySnapshotDecisionStatus,
        error: Option<RuntimeString>,
        ordered_reply: &mut Option<SnapshotOrderedReply>,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError> {
        let staged = self.snapshot.decisions.stage_record_terminal(
            request_id,
            expected,
            status,
            (self.deps.now)(),
            error,
        )?;
        let transition = staged.value().clone();
        let proposed = (!transition.idempotent).then(|| transition.audit.clone());
        let ordered = ordered_reply.as_ref().map(|_| transition.audit.clone());
        self.persist_and_commit_snapshot_staged(staged, proposed, ordered_reply, ordered)
            .await
            .map(|transition| transition.audit)
    }

    pub(crate) async fn apply_agent_memory_snapshot_decision_under_serial(
        &self,
        request_id: &RuntimeString,
        ordered_reply: &mut Option<SnapshotOrderedReply>,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError> {
        let Some(host) = &self.agent_memory_host else {
            let _ = self
                .record_agent_memory_snapshot_terminal_under_serial(
                    request_id,
                    AgentMemorySnapshotDecisionStatus::DecisionRecorded,
                    AgentMemorySnapshotDecisionStatus::Failed,
                    Some(RuntimeString::from(
                        "agent memory Host is unavailable while applying snapshot decision",
                    )),
                    &mut None,
                )
                .await?;
            return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
                "agent memory Host is unavailable",
            )
            .into());
        };
        let staged = stage_apply_recorded_agent_memory_snapshot_decision(
            host.layout(),
            &self.snapshot.decisions,
            request_id,
            (self.deps.now)(),
        )
        .await;
        let ordered = ordered_reply.as_ref().and_then(|_| {
            staged
                .proposed_audit()
                .cloned()
                .or_else(|| self.snapshot.decisions.query(request_id))
        });
        self.persist_and_commit_snapshot_filesystem_staged(staged, ordered_reply, ordered)
            .await
            .map(|outcome| outcome.audit)
    }

    pub(crate) async fn recover_marked_agent_memory_snapshot_decision_under_serial(
        &self,
        request_id: &RuntimeString,
    ) -> Result<Option<AgentMemorySnapshotDecisionAudit>, AgentMemorySnapshotApplyError> {
        let Some(host) = &self.agent_memory_host else {
            return Ok(None);
        };
        let staged = stage_recover_marked_agent_memory_snapshot_decision(
            host.layout(),
            &self.snapshot.decisions,
            request_id,
        )
        .await;
        self.persist_and_commit_snapshot_filesystem_staged(staged, &mut None, None)
            .await
    }

    pub(crate) async fn resolve_agent_memory_snapshot_decision_under_serial(
        &self,
        request_id: &RuntimeString,
        decision: AgentMemoryMergeChoice,
        source: AgentMemorySnapshotDecisionSource,
        without_consumer: bool,
        ordered_reply: &mut Option<SnapshotOrderedReply>,
    ) -> Result<
        (
            AgentMemorySnapshotDecisionAudit,
            Option<AgentMemorySnapshotMergeDelivery>,
        ),
        AgentMemorySnapshotApplyError,
    > {
        let staged = self.snapshot.decisions.stage_record_decision(
            request_id,
            AgentMemorySnapshotDecisionStatus::Requested,
            decision,
            source,
            (self.deps.now)(),
        )?;
        let transition = staged.value().clone();
        let proposed = (!transition.idempotent).then(|| transition.audit.clone());
        let is_merge = Self::merge_delivery(&transition.audit).is_some();
        let ordered = (is_merge && ordered_reply.is_some()).then(|| transition.audit.clone());
        let transition = self
            .persist_and_commit_snapshot_staged(staged, proposed, ordered_reply, ordered)
            .await?;
        if let Some(delivery) = Self::merge_delivery(&transition.audit) {
            return Ok((transition.audit, (!without_consumer).then_some(delivery)));
        }
        self.apply_agent_memory_snapshot_decision_under_serial(request_id, ordered_reply)
            .await
            .map(|audit| (audit, None))
    }

    pub(crate) async fn settle_unavailable_agent_memory_snapshot_under_serial(
        &self,
        request_id: &RuntimeString,
        status: AgentMemorySnapshotDecisionStatus,
        error: Option<RuntimeString>,
        without_consumer: bool,
        ordered_reply: &mut Option<SnapshotOrderedReply>,
    ) -> Result<
        (
            AgentMemorySnapshotDecisionAudit,
            Option<AgentMemorySnapshotMergeDelivery>,
        ),
        AgentMemorySnapshotApplyError,
    > {
        if let Err(policy_error) = self.snapshot.policy.validate() {
            let audit = self
                .record_agent_memory_snapshot_terminal_under_serial(
                    request_id,
                    AgentMemorySnapshotDecisionStatus::Requested,
                    AgentMemorySnapshotDecisionStatus::Failed,
                    Some(RuntimeString::from(policy_error)),
                    ordered_reply,
                )
                .await?;
            return Ok((audit, None));
        }
        if let Some((decision, source)) = self.snapshot.policy.fallback(status, false) {
            return self
                .resolve_agent_memory_snapshot_decision_under_serial(
                    request_id,
                    decision,
                    source,
                    without_consumer,
                    ordered_reply,
                )
                .await;
        }
        self.record_agent_memory_snapshot_terminal_under_serial(
            request_id,
            AgentMemorySnapshotDecisionStatus::Requested,
            status,
            error,
            ordered_reply,
        )
        .await
        .map(|audit| (audit, None))
    }

    pub(crate) async fn execute_snapshot_durability_operation(
        &self,
        operation: SnapshotDurabilityOperation,
    ) -> (
        SnapshotDurabilityResult,
        Result<(), RuntimeString>,
        Result<(), RuntimeString>,
    ) {
        let kind = operation.kind();
        let _serial = self.snapshot.persistence.serial.lock().await;
        if let Some(detail) = self.snapshot_fatal_detail() {
            return (
                Self::snapshot_failure_result(kind, detail.clone()),
                Err(detail.clone()),
                Err(detail),
            );
        }
        if self.snapshot.persistence.finalized.load(Ordering::SeqCst) {
            let detail = RuntimeString::from("snapshot decision persistence is finalized");
            return (
                Self::snapshot_failure_result(kind, detail.clone()),
                Err(detail.clone()),
                Err(detail),
            );
        }

        let result = match operation {
            SnapshotDurabilityOperation::Register { request } => {
                let staged = self.snapshot.decisions.stage_register(request);
                SnapshotDurabilityResult::Register(match staged {
                    Ok(staged) => {
                        let registered = staged.value().clone();
                        let proposed = registered.created.then(|| registered.audit.clone());
                        self.persist_and_commit_snapshot_staged(staged, proposed, &mut None, None)
                            .await
                            .map(|registered| registered.audit)
                    }
                    Err(error) => Err(error.into()),
                })
            }
            SnapshotDurabilityOperation::Resolve {
                request_id,
                decision,
                source,
                mut ordered_reply,
            } => SnapshotDurabilityResult::Resolve(
                self.resolve_agent_memory_snapshot_decision_under_serial(
                    &request_id,
                    decision,
                    source,
                    false,
                    &mut ordered_reply,
                )
                .await,
            ),
            SnapshotDurabilityOperation::Terminal {
                request_id,
                expected,
                status,
                error,
            } => SnapshotDurabilityResult::Terminal(
                self.record_agent_memory_snapshot_terminal_under_serial(
                    &request_id,
                    expected,
                    status,
                    error,
                    &mut None,
                )
                .await,
            ),
            SnapshotDurabilityOperation::Apply { request_id } => SnapshotDurabilityResult::Apply(
                self.apply_agent_memory_snapshot_decision_under_serial(&request_id, &mut None)
                    .await,
            ),
            SnapshotDurabilityOperation::RecoverMarked { request_id } => {
                SnapshotDurabilityResult::RecoverMarked(
                    self.recover_marked_agent_memory_snapshot_decision_under_serial(&request_id)
                        .await,
                )
            }
            SnapshotDurabilityOperation::SettleUnavailable {
                request_id,
                status,
                error,
                without_consumer,
                mut ordered_reply,
            } => SnapshotDurabilityResult::SettleUnavailable(
                self.settle_unavailable_agent_memory_snapshot_under_serial(
                    &request_id,
                    status,
                    error,
                    without_consumer,
                    &mut ordered_reply,
                )
                .await,
            ),
            SnapshotDurabilityOperation::ConstructorRecovery { audit } => {
                let request_id = &audit.request.request_id;
                let recovered = match audit.status {
                    AgentMemorySnapshotDecisionStatus::Requested => self
                        .settle_unavailable_agent_memory_snapshot_under_serial(
                            request_id,
                            AgentMemorySnapshotDecisionStatus::Disconnected,
                            Some(RuntimeString::from(
                                "snapshot decision Host disconnected during session restart",
                            )),
                            true,
                            &mut None,
                        )
                        .await
                        .map(|_| ()),
                    AgentMemorySnapshotDecisionStatus::DecisionRecorded
                        if audit.decision != Some(AgentMemoryMergeChoice::Merge) =>
                    {
                        self.apply_agent_memory_snapshot_decision_under_serial(
                            request_id, &mut None,
                        )
                        .await
                        .map(|_| ())
                    }
                    AgentMemorySnapshotDecisionStatus::DecisionRecorded => self
                        .recover_marked_agent_memory_snapshot_decision_under_serial(request_id)
                        .await
                        .map(|_| ()),
                    _ => Ok(()),
                }
                .map_err(|error| RuntimeString::from(error.to_string()));
                SnapshotDurabilityResult::ConstructorRecovery(recovered)
            }
        };
        let durability_verdict = self.snapshot_fatal_detail().map_or(Ok(()), Err);
        let recovery_verdict = match &result {
            SnapshotDurabilityResult::ConstructorRecovery(Ok(())) => durability_verdict.clone(),
            SnapshotDurabilityResult::ConstructorRecovery(Err(error)) => Err(error.clone()),
            _ => durability_verdict.clone(),
        };
        (result, durability_verdict, recovery_verdict)
    }

    pub(crate) async fn run_snapshot_durability_operation(
        &self,
        operation: SnapshotDurabilityOperation,
        allow_while_closing: bool,
    ) -> Result<SnapshotDurabilityResult, AgentMemorySnapshotApplyError> {
        let Some(session) = self.snapshot.resolve_self() else {
            return Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(
                RuntimeString::from("snapshot Session owner is unavailable"),
            ));
        };
        session.start_pending_snapshot_recovery_owners();
        if let Some(error) = session.snapshot_fatal_apply_error() {
            return Err(error);
        }
        let receive = session
            .schedule_snapshot_durability_operation(operation, allow_while_closing, None)
            .map_err(|_error| {
                session.snapshot_fatal_apply_error().unwrap_or({
                    AgentMemorySnapshotApplyError::Registry(
                        AgentMemorySnapshotRegistryError::InvalidRequest(if allow_while_closing {
                            "snapshot durability shutdown repair was not admitted"
                        } else {
                            "snapshot durability operation was not admitted"
                        }),
                    )
                })
            })?;
        receive.await.map_err(|_| {
            AgentMemorySnapshotApplyError::PersistenceInconsistent(RuntimeString::from(
                "Session-owned snapshot durability task ended without a result",
            ))
        })
    }

    pub(crate) async fn register_agent_memory_snapshot_request(
        &self,
        request: AgentMemorySnapshotDecisionRequest,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError> {
        match self
            .run_snapshot_durability_operation(
                SnapshotDurabilityOperation::Register { request },
                false,
            )
            .await?
        {
            SnapshotDurabilityResult::Register(result) => result,
            _ => unreachable!("snapshot Register owner returned the wrong result variant"),
        }
    }

    pub(crate) async fn record_agent_memory_snapshot_terminal(
        &self,
        request_id: &RuntimeString,
        expected: AgentMemorySnapshotDecisionStatus,
        status: AgentMemorySnapshotDecisionStatus,
        error: Option<RuntimeString>,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError> {
        match self
            .run_snapshot_durability_operation(
                SnapshotDurabilityOperation::Terminal {
                    request_id: request_id.clone(),
                    expected,
                    status,
                    error,
                },
                false,
            )
            .await?
        {
            SnapshotDurabilityResult::Terminal(result) => result,
            _ => unreachable!("snapshot Terminal owner returned the wrong result variant"),
        }
    }

    pub(crate) async fn apply_agent_memory_snapshot_decision_with_policy(
        &self,
        request_id: &RuntimeString,
        allow_while_closing: bool,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError> {
        match self
            .run_snapshot_durability_operation(
                SnapshotDurabilityOperation::Apply {
                    request_id: request_id.clone(),
                },
                allow_while_closing,
            )
            .await?
        {
            SnapshotDurabilityResult::Apply(result) => result,
            _ => unreachable!("snapshot Apply owner returned the wrong result variant"),
        }
    }

    pub(crate) async fn apply_agent_memory_snapshot_decision(
        &self,
        request_id: &RuntimeString,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError> {
        self.apply_agent_memory_snapshot_decision_with_policy(request_id, false)
            .await
    }

    pub(crate) async fn apply_agent_memory_snapshot_merge_after_provider_admission(
        &self,
        request_id: &RuntimeString,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError> {
        let current = self
            .agent_memory_snapshot_decision(request_id)
            .await?
            .ok_or(AgentMemorySnapshotRegistryError::UnknownRequest)?;
        if current.decision != Some(AgentMemoryMergeChoice::Merge) {
            return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
                "provider-admission apply requires a recorded merge decision",
            )
            .into());
        }
        let audit = self
            .apply_agent_memory_snapshot_decision(request_id)
            .await?;
        if audit.status != AgentMemorySnapshotDecisionStatus::Applied {
            return Err(AgentMemorySnapshotRegistryError::InvalidRequest(
                "provider-admission merge decision did not reach applied",
            )
            .into());
        }
        Ok(audit)
    }

    pub(crate) async fn recover_marked_agent_memory_snapshot_decision_with_policy(
        &self,
        request_id: &RuntimeString,
        allow_while_closing: bool,
    ) -> Result<Option<AgentMemorySnapshotDecisionAudit>, AgentMemorySnapshotApplyError> {
        match self
            .run_snapshot_durability_operation(
                SnapshotDurabilityOperation::RecoverMarked {
                    request_id: request_id.clone(),
                },
                allow_while_closing,
            )
            .await?
        {
            SnapshotDurabilityResult::RecoverMarked(result) => result,
            _ => unreachable!("snapshot RecoverMarked owner returned the wrong result variant"),
        }
    }

    pub(crate) async fn recover_marked_agent_memory_snapshot_decision(
        &self,
        request_id: &RuntimeString,
    ) -> Result<Option<AgentMemorySnapshotDecisionAudit>, AgentMemorySnapshotApplyError> {
        self.recover_marked_agent_memory_snapshot_decision_with_policy(request_id, false)
            .await
    }

    pub(crate) fn merge_delivery(
        audit: &AgentMemorySnapshotDecisionAudit,
    ) -> Option<AgentMemorySnapshotMergeDelivery> {
        (audit.status == AgentMemorySnapshotDecisionStatus::DecisionRecorded
            && audit.decision == Some(AgentMemoryMergeChoice::Merge))
        .then(|| AgentMemorySnapshotMergeDelivery {
            prefix: crate::memory::agent_memory::build_merge_prompt_prefix(),
            request_id: audit.request.request_id.clone(),
        })
    }

    /// Resume an already-recorded decision without acknowledging a Merge
    /// before its prompt reaches the provider boundary. Keep/Replace retain
    /// their historical eager-apply behavior. A Merge marker left behind by a
    /// crash after provider admission is the sole cold-recovery shortcut to
    /// `Applied`; otherwise every reconciliation receives the same correlated
    /// delivery token again.
    pub(crate) async fn reconcile_recorded_agent_memory_snapshot_decision(
        &self,
        audit: &AgentMemorySnapshotDecisionAudit,
    ) -> Result<Option<AgentMemorySnapshotMergeDelivery>, AgentMemorySnapshotApplyError> {
        match audit.status {
            AgentMemorySnapshotDecisionStatus::Applied => Ok(None),
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
                if audit.decision == Some(AgentMemoryMergeChoice::Merge) =>
            {
                if matches!(
                    self.recover_marked_agent_memory_snapshot_decision(&audit.request.request_id)
                        .await?,
                    Some(AgentMemorySnapshotDecisionAudit {
                        status: AgentMemorySnapshotDecisionStatus::Applied,
                        ..
                    })
                ) {
                    return Ok(None);
                }
                let current = self
                    .agent_memory_snapshot_decision(&audit.request.request_id)
                    .await?;
                Ok(current.as_ref().and_then(Self::merge_delivery))
            }
            AgentMemorySnapshotDecisionStatus::DecisionRecorded => {
                self.apply_agent_memory_snapshot_decision(&audit.request.request_id)
                    .await?;
                Ok(None)
            }
            status if status.is_terminal() => Ok(None),
            AgentMemorySnapshotDecisionStatus::Requested => Ok(None),
            _ => unreachable!("snapshot registry status is exhaustive"),
        }
    }

    pub(crate) async fn resolve_agent_memory_snapshot_decision_with_source(
        &self,
        request_id: &RuntimeString,
        decision: AgentMemoryMergeChoice,
        source: AgentMemorySnapshotDecisionSource,
    ) -> Result<
        (
            AgentMemorySnapshotDecisionAudit,
            Option<AgentMemorySnapshotMergeDelivery>,
        ),
        AgentMemorySnapshotApplyError,
    > {
        match self
            .run_snapshot_durability_operation(
                SnapshotDurabilityOperation::Resolve {
                    request_id: request_id.clone(),
                    decision,
                    source,
                    ordered_reply: None,
                },
                false,
            )
            .await?
        {
            SnapshotDurabilityResult::Resolve(result) => result,
            _ => unreachable!("snapshot Resolve owner returned the wrong result variant"),
        }
    }

    pub(crate) async fn settle_unavailable_agent_memory_snapshot_request(
        &self,
        request_id: &RuntimeString,
        status: AgentMemorySnapshotDecisionStatus,
        error: Option<RuntimeString>,
    ) -> Result<
        (
            AgentMemorySnapshotDecisionAudit,
            Option<AgentMemorySnapshotMergeDelivery>,
        ),
        AgentMemorySnapshotApplyError,
    > {
        debug_assert!(matches!(
            status,
            AgentMemorySnapshotDecisionStatus::TimedOut
                | AgentMemorySnapshotDecisionStatus::Disconnected
                | AgentMemorySnapshotDecisionStatus::NoHandler
        ));
        match self
            .run_snapshot_durability_operation(
                SnapshotDurabilityOperation::SettleUnavailable {
                    request_id: request_id.clone(),
                    status,
                    error,
                    without_consumer: false,
                    ordered_reply: None,
                },
                false,
            )
            .await?
        {
            SnapshotDurabilityResult::SettleUnavailable(result) => result,
            _ => unreachable!("snapshot SettleUnavailable owner returned the wrong result variant"),
        }
    }

    /// Settle a Host-unavailable request when no live reconciliation caller can
    /// consume a merge prompt (restart/shutdown recovery). Keep/replace are
    /// applied eagerly, but merge remains `decision-recorded` until the next
    /// real agent reconciliation can attach the prompt before acknowledging the
    /// snapshot marker.
    pub(crate) async fn settle_unavailable_agent_memory_snapshot_without_consumer_with_policy(
        &self,
        request_id: &RuntimeString,
        status: AgentMemorySnapshotDecisionStatus,
        error: Option<RuntimeString>,
        allow_while_closing: bool,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError> {
        debug_assert!(matches!(
            status,
            AgentMemorySnapshotDecisionStatus::TimedOut
                | AgentMemorySnapshotDecisionStatus::Disconnected
                | AgentMemorySnapshotDecisionStatus::NoHandler
        ));
        match self
            .run_snapshot_durability_operation(
                SnapshotDurabilityOperation::SettleUnavailable {
                    request_id: request_id.clone(),
                    status,
                    error,
                    without_consumer: true,
                    ordered_reply: None,
                },
                allow_while_closing,
            )
            .await?
        {
            SnapshotDurabilityResult::SettleUnavailable(result) => result.map(|(audit, _)| audit),
            _ => unreachable!("snapshot SettleUnavailable owner returned the wrong result variant"),
        }
    }

    /// Interpret an exactly-once resolve/settle race without hiding a fatal
    /// durability break. Only the explicit exactly-once loser errors re-query
    /// checked authoritative state; I/O, malformed requests, unknown identity,
    /// staged-CAS failures, and persistence inconsistency propagate. This keeps
    /// a real failure from being downgraded to "no merge" while preserving a
    /// concurrently recorded winner (including its Merge delivery).
    pub(crate) async fn reconcile_snapshot_operation_result(
        &self,
        stage: &'static str,
        agent_type: &str,
        request_id: &RuntimeString,
        result: Result<
            (
                AgentMemorySnapshotDecisionAudit,
                Option<AgentMemorySnapshotMergeDelivery>,
            ),
            AgentMemorySnapshotApplyError,
        >,
    ) -> Result<Option<AgentMemorySnapshotMergeDelivery>, RuntimeString> {
        match result {
            Ok((_, merge)) => Ok(merge),
            Err(
                error @ AgentMemorySnapshotApplyError::Registry(
                    AgentMemorySnapshotRegistryError::DecisionConflict
                    | AgentMemorySnapshotRegistryError::StateConflict { .. }
                    | AgentMemorySnapshotRegistryError::ApplyInProgress,
                ),
            ) => {
                let current = self
                    .agent_memory_snapshot_decision(request_id)
                    .await
                    .map_err(|error| {
                        agent_memory_snapshot_reconcile_error(stage, agent_type, &error)
                    })?;
                let Some(current) = current else {
                    return Err(agent_memory_snapshot_reconcile_error(
                        stage, agent_type, &error,
                    ));
                };
                match current.status {
                    AgentMemorySnapshotDecisionStatus::DecisionRecorded
                    | AgentMemorySnapshotDecisionStatus::Applied => self
                        .reconcile_recorded_agent_memory_snapshot_decision(&current)
                        .await
                        .map_err(|error| {
                            agent_memory_snapshot_reconcile_error(stage, agent_type, &error)
                        }),
                    AgentMemorySnapshotDecisionStatus::Failed
                    | AgentMemorySnapshotDecisionStatus::TimedOut
                    | AgentMemorySnapshotDecisionStatus::Disconnected
                    | AgentMemorySnapshotDecisionStatus::NoHandler
                    | AgentMemorySnapshotDecisionStatus::Stale => Ok(None),
                    AgentMemorySnapshotDecisionStatus::Requested => Err(
                        agent_memory_snapshot_reconcile_error(stage, agent_type, &error),
                    ),
                }
            }
            Err(error) => Err(agent_memory_snapshot_reconcile_error(
                stage, agent_type, &error,
            )),
        }
    }

    /// Reconcile one user-scope per-agent snapshot. The `requested` transition
    /// is persisted and broadcast before the direct Host callback begins. The
    /// direct callback and protocol resolver then race through the same CAS.
    /// Failures before that durable request exists are returned to the agent
    /// admission path instead of being confused with "no reconciliation".
    pub(crate) async fn reconcile_agent_memory_snapshot(
        self: &Arc<Self>,
        host: &dyn AgentMemoryHost,
        agent_type: &str,
        signal: &AbortSignal,
    ) -> Result<Option<AgentMemorySnapshotMergeDelivery>, RuntimeString> {
        let policy = self.snapshot.policy.clone();
        let requested_at = (self.deps.now)();
        let deadline_at =
            requested_at.saturating_add(i64::try_from(policy.timeout_ms).unwrap_or(i64::MAX));
        let request = match prepare_agent_memory_snapshot_request(
            host.layout(),
            agent_type,
            requested_at,
            deadline_at,
        )
        .await
        {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(None),
            Err(error) => {
                return Err(agent_memory_snapshot_reconcile_error(
                    "prepare-request",
                    agent_type,
                    &error,
                ));
            }
        };

        self.reconcile_prepared_agent_memory_snapshot(host, agent_type, signal, policy, request)
            .await
    }

    pub(crate) async fn reconcile_prepared_agent_memory_snapshot(
        self: &Arc<Self>,
        host: &dyn AgentMemoryHost,
        agent_type: &str,
        signal: &AbortSignal,
        policy: AgentMemorySnapshotDecisionPolicy,
        request: AgentMemorySnapshotDecisionRequest,
    ) -> Result<Option<AgentMemorySnapshotMergeDelivery>, RuntimeString> {
        // Count existing observers before installing the reconciliation receiver
        // itself. Protocol's ordered sink is a Host path even though it does not
        // consume the SDK broadcast channel.
        let had_external_subscriber = self.snapshot.events.receiver_count() > 0
            || self.snapshot.ordered_sink.lock_recover().is_some();
        let mut events = self.snapshot.events.subscribe();
        // Arm before awaiting the Requested durability owner. If this caller is
        // cancelled at any await from registration onward, Drop synchronously
        // chains a Session-owned disconnect repair behind that owner.
        let mut attempt = SnapshotDecisionAttemptGuard::new(self, request.request_id.clone());
        let audit = match self
            .register_agent_memory_snapshot_request(request.clone())
            .await
        {
            Ok(audit) => audit,
            Err(error) => {
                return Err(agent_memory_snapshot_reconcile_error(
                    "register-request",
                    agent_type,
                    &error,
                ));
            }
        };
        let request_id = audit.request.request_id.clone();
        if request_id != request.request_id {
            attempt.complete();
            attempt = SnapshotDecisionAttemptGuard::new(self, request_id.clone());
        }

        if let Err(error) = policy.validate() {
            let result = self
                .record_agent_memory_snapshot_terminal(
                    &request_id,
                    AgentMemorySnapshotDecisionStatus::Requested,
                    AgentMemorySnapshotDecisionStatus::Failed,
                    Some(RuntimeString::from(error)),
                )
                .await
                .map(|audit| (audit, None));
            let outcome = self
                .reconcile_snapshot_operation_result(
                    "invalid-policy-terminal",
                    agent_type,
                    &request_id,
                    result,
                )
                .await;
            if outcome.is_ok() {
                attempt.complete();
            }
            return outcome;
        }

        if audit.status == AgentMemorySnapshotDecisionStatus::DecisionRecorded
            || audit.status == AgentMemorySnapshotDecisionStatus::Applied
        {
            let merge = self
                .reconcile_recorded_agent_memory_snapshot_decision(&audit)
                .await
                .map_err(|error| {
                    agent_memory_snapshot_reconcile_error("resume-decision", agent_type, &error)
                })?;
            attempt.complete();
            return Ok(merge);
        }
        if audit.status.is_terminal() {
            attempt.complete();
            return Ok(None);
        }

        if request.first_use {
            if let Some((decision, source)) =
                policy.fallback(AgentMemorySnapshotDecisionStatus::Requested, true)
            {
                let result = self
                    .resolve_agent_memory_snapshot_decision_with_source(
                        &request_id,
                        decision,
                        source,
                    )
                    .await;
                let outcome = self
                    .reconcile_snapshot_operation_result(
                        "first-use-policy",
                        agent_type,
                        &request_id,
                        result,
                    )
                    .await;
                if outcome.is_ok() {
                    attempt.complete();
                }
                return outcome;
            }
        }

        let callback = std::panic::AssertUnwindSafe(host.on_snapshot_decision_request(&request))
            .catch_unwind()
            .fuse();
        futures::pin_mut!(callback);
        let timeout = tokio::time::sleep(std::time::Duration::from_millis(policy.timeout_ms));
        tokio::pin!(timeout);

        loop {
            let current = match self.agent_memory_snapshot_decision(&request_id).await {
                Ok(Some(current)) => current,
                Ok(None) => {
                    let error = AgentMemorySnapshotApplyError::Registry(
                        AgentMemorySnapshotRegistryError::UnknownRequest,
                    );
                    return Err(agent_memory_snapshot_reconcile_error(
                        "query-decision",
                        agent_type,
                        &error,
                    ));
                }
                Err(error) => {
                    return Err(agent_memory_snapshot_reconcile_error(
                        "query-decision",
                        agent_type,
                        &error,
                    ));
                }
            };
            match current.status {
                AgentMemorySnapshotDecisionStatus::DecisionRecorded
                | AgentMemorySnapshotDecisionStatus::Applied => {
                    let merge = self
                        .reconcile_recorded_agent_memory_snapshot_decision(&current)
                        .await
                        .map_err(|error| {
                            agent_memory_snapshot_reconcile_error(
                                "resume-decision",
                                agent_type,
                                &error,
                            )
                        })?;
                    attempt.complete();
                    return Ok(merge);
                }
                status if status.is_terminal() => {
                    attempt.complete();
                    return Ok(None);
                }
                AgentMemorySnapshotDecisionStatus::Requested => {}
                _ => unreachable!("snapshot registry status is exhaustive"),
            }

            tokio::select! {
                callback_result = &mut callback => {
                    match callback_result {
                        Ok(Some(decision)) => {
                            let result = self
                                .resolve_agent_memory_snapshot_decision_with_source(
                                    &request_id,
                                    decision,
                                    AgentMemorySnapshotDecisionSource::Host,
                                )
                                .await;
                            let outcome = self
                                .reconcile_snapshot_operation_result(
                                    "host-decision",
                                    agent_type,
                                    &request_id,
                                    result,
                                )
                                .await;
                            if outcome.is_ok() {
                                attempt.complete();
                            }
                            return outcome;
                        }
                        Ok(None)
                            if had_external_subscriber
                                || self.snapshot.events.receiver_count() > 1 =>
                        {
                            // The in-process adapter has no answer, but a protocol
                            // Host is subscribed and owns the still-open request.
                        }
                        Ok(None) => {
                            let result = self
                                .settle_unavailable_agent_memory_snapshot_request(
                                    &request_id,
                                    AgentMemorySnapshotDecisionStatus::NoHandler,
                                    Some(RuntimeString::from(
                                        "no Agent Memory snapshot decision handler is installed",
                                    )),
                                )
                                .await;
                            let outcome = self
                                .reconcile_snapshot_operation_result(
                                    "no-handler",
                                    agent_type,
                                    &request_id,
                                    result,
                                )
                                .await;
                            if outcome.is_ok() {
                                attempt.complete();
                            }
                            return outcome;
                        }
                        Err(_) => {
                            let result = self
                                .record_agent_memory_snapshot_terminal(
                                    &request_id,
                                    AgentMemorySnapshotDecisionStatus::Requested,
                                    AgentMemorySnapshotDecisionStatus::Failed,
                                    Some(RuntimeString::from(
                                        "Agent Memory snapshot decision handler panicked",
                                    )),
                                )
                                .await
                                .map(|audit| (audit, None));
                            let outcome = self
                                .reconcile_snapshot_operation_result(
                                    "handler-panic-terminal",
                                    agent_type,
                                    &request_id,
                                    result,
                                )
                                .await;
                            if outcome.is_ok() {
                                attempt.complete();
                            }
                            return outcome;
                        }
                    }
                }
                _ = signal.cancelled() => {
                    let result = self
                        .settle_unavailable_agent_memory_snapshot_request(
                            &request_id,
                            AgentMemorySnapshotDecisionStatus::Disconnected,
                            Some(RuntimeString::from(
                                "snapshot decision Host disconnected before resolution",
                            )),
                        )
                        .await;
                    let outcome = self
                        .reconcile_snapshot_operation_result(
                            "host-disconnected",
                            agent_type,
                            &request_id,
                            result,
                        )
                        .await;
                    if outcome.is_ok() {
                        attempt.complete();
                    }
                    return outcome;
                }
                _ = &mut timeout => {
                    let result = self
                        .settle_unavailable_agent_memory_snapshot_request(
                            &request_id,
                            AgentMemorySnapshotDecisionStatus::TimedOut,
                            Some(RuntimeString::from(
                                "snapshot decision Host response timed out",
                            )),
                        )
                        .await;
                    let outcome = self
                        .reconcile_snapshot_operation_result(
                            "host-timeout",
                            agent_type,
                            &request_id,
                            result,
                        )
                        .await;
                    if outcome.is_ok() {
                        attempt.complete();
                    }
                    return outcome;
                }
                event = events.recv() => {
                    match event {
                        Ok(event) if event.request.request_id == request_id => {}
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            let result = self
                                .settle_unavailable_agent_memory_snapshot_request(
                                    &request_id,
                                    AgentMemorySnapshotDecisionStatus::Disconnected,
                                    Some(RuntimeString::from(
                                        "snapshot decision event channel closed",
                                    )),
                                )
                                .await;
                            let outcome = self
                                .reconcile_snapshot_operation_result(
                                    "event-channel-closed",
                                    agent_type,
                                    &request_id,
                                    result,
                                )
                                .await;
                            if outcome.is_ok() {
                                attempt.complete();
                            }
                            return outcome;
                        }
                    }
                }
            }
        }
    }

    /// The skills VISIBLE right now (all, or gate-filtered).
    pub(crate) fn gated_skills(&self) -> Vec<Skill> {
        let all = self
            .current_skills_thunk()
            .as_ref()
            .map(|f| f())
            .unwrap_or_default();
        match self.current_skill_gate() {
            Some(g) => g.filter(&all),
            None => all,
        }
    }

    pub(crate) fn slash_command_registry(
        self: &Arc<Self>,
        permission: PermissionContext,
        signal: AbortSignal,
        planning_read_only: bool,
        turn_id: RuntimeString,
    ) -> SlashCommandRegistry {
        let mut registry = SlashCommandRegistry::new();
        let commands = self
            .plugin_projection()
            .map(|projection| projection.commands.clone())
            .unwrap_or_else(|| self.slash_commands.clone());
        for command in &commands {
            registry.register(command.clone());
        }

        if self.current_skills_thunk().is_some() {
            let this = self.clone();
            let run_bash: SkillBashRunner =
                Arc::new(move |command: String, opts: SkillBashOpts| {
                    let this = this.clone();
                    let permission = permission.clone();
                    let signal = signal.clone();
                    Box::pin(async move {
                        let cwd = this.cwd_now();
                        this.run_skill_bash(
                            command,
                            permission,
                            runtime_path_to_skill_shell_host(&cwd),
                            signal,
                            Some(RunBashOptions {
                                shell: opts.shell,
                                allow_tools: opts.allow_tools,
                            }),
                            planning_read_only,
                        )
                        .await
                        .map_err(|e| e.to_string())
                    })
                });
            for command in skills_as_commands(
                &self.gated_skills(),
                SkillsAsCommandsOptions {
                    on_skill_expanded: Some({
                        let session = self.clone();
                        Arc::new(move |path, content| {
                            session.record_invoked_skill_content(None, path, content);
                        })
                    }),
                    on_skill_invoked: Some({
                        let session = self.clone();
                        let turn_id = turn_id.clone();
                        Arc::new(move |path| {
                            session.record_invoked_skill(None, &turn_id, path);
                        })
                    }),
                    vars: Some(vec![(
                        "KLAUS_SESSION_ID".to_string(),
                        runtime_session_id_to_skill_env_host(&self.id),
                    )]),
                    run_bash: Some(run_bash),
                },
            ) {
                registry.register(command);
            }
        }

        registry
    }

    pub(crate) async fn run_slash_command_input(
        self: &Arc<Self>,
        line: &RuntimeString,
        permission: PermissionContext,
        signal: AbortSignal,
        planning_read_only: bool,
        turn_id: RuntimeString,
    ) -> Result<Option<SlashCommandOutcome>, String> {
        let Some(first_non_whitespace) = line
            .as_utf16()
            .iter()
            .position(|unit| !is_ecmascript_trim_unit(*unit))
        else {
            return Ok(None);
        };
        if line.as_utf16().get(first_non_whitespace) != Some(&u16::from(b'/')) {
            return Ok(None);
        }
        let trimmed = line.slice_utf16(first_non_whitespace..line.len_utf16());
        let registry = self.slash_command_registry(permission, signal, planning_read_only, turn_id);
        let Some(parsed) = registry.parse(&trimmed) else {
            return Ok(None);
        };
        let Some(command) = registry.get(&parsed.name) else {
            return Ok(None);
        };
        match command {
            Command::Prompt(prompt) => prompt
                .get_content(parsed.args)
                .await
                .map(|content| Some(SlashCommandOutcome::Prompt(content))),
            Command::Local(local) => local
                .run(parsed.args)
                .await
                .map(|outcome| Some(SlashCommandOutcome::Local(outcome))),
        }
    }

    pub(crate) fn replace_user_message_content(
        messages: &mut [Message],
        user_uuid: &str,
        content: Vec<ContentBlock>,
    ) {
        for m in messages.iter_mut() {
            if let Message::User(u) = m {
                if u.uuid == user_uuid {
                    u.content = content;
                    break;
                }
            }
        }
    }

    fn install_async_hook_sink(self: &Arc<Self>, bus: &Arc<crate::hooks::bus::HookBus>) {
        let weak = Arc::downgrade(self);
        bus.set_async_sink(Arc::new(move |_event, outcome, input| {
            let Some(session) = weak.upgrade() else {
                return;
            };
            if session.lifecycle.is_disposed() {
                return;
            }
            let text = outcome.additional_context.or(outcome.system_message);
            let Some(text) = text else { return };
            let _ = session
                .conversation
                .queue
                .enqueue_pending_notification(QueuedMessage {
                    trigger_turn: None,
                    value: QueuedValue::Text(session.render_deferred_notice(&text)),
                    mode: "task-notification".into(),
                    priority: None,
                    uuid: Some((session.deps.uuid)().into()),
                    session_id: Some(session.id.clone()),
                    is_meta: Some(true),
                    origin: Some("async-hook".into()),
                    agent_id: input.agent_id.clone(),
                    correlation_id: None,
                    pdf_bytes: 0,
                    pdf_documents: 0,
                    delivery: None,
                    display_metadata: None,
                });
        }));
    }

    pub(crate) fn stage_plugin_update(
        self: &Arc<Self>,
        mut update: SessionPluginUpdate,
    ) -> Result<(), String> {
        let mut names = std::collections::HashSet::new();
        for config in crate::engine::agent::merge_session_mcp_servers(
            self.manual_mcp_servers(),
            update.plugin_mcp_servers.clone(),
        ) {
            if !names.insert(config.name().to_string()) {
                return Err(format!("duplicate MCP server name: {}", config.name()));
            }
        }
        let hook_bus = (!update.hooks.is_empty()).then(|| {
            let bus = Arc::new(crate::hooks::bus::HookBus::new(
                self.process_context.clone(),
                update.hooks,
            ));
            self.install_async_hook_sink(&bus);
            if let Some(sink) = self.extension_failure_sink.lock_recover().clone() {
                bus.set_failure_sink(sink);
            }
            bus
        });
        let configured_style = self.config.output_style.as_ref().map(|name| {
            name.try_to_utf8()
                .expect("validated output-style config is a scalar string")
        });
        let active_output_style =
            resolve_output_style(configured_style.as_deref(), Some(&update.output_styles)).map(
                |style| ActiveOutputStyle {
                    name: style.name.clone(),
                    body: style.body.clone(),
                },
            );
        update.extension_runtime.selected_output_style =
            active_output_style.as_ref().map(|style| style.name.clone());
        let lsp_configs = if self.config.lsp {
            update.lsp_servers
        } else {
            Vec::new()
        };
        let previous = self.plugin_projection();
        let old_configs = previous
            .as_ref()
            .map(|projection| &projection.lsp_configs)
            .unwrap_or(&self.lsp_configs);
        let lsp_manager = if lsp_configs == *old_configs {
            previous
                .as_ref()
                .map(|projection| projection.lsp_manager.clone())
                .unwrap_or_else(|| self.lsp_manager.clone())
        } else if lsp_configs.is_empty() {
            None
        } else {
            Some(Arc::new(LspManager::new(
                self.process_context.clone(),
                lsp_configs.clone(),
                self.cwd_now(),
                LspClientOptions {
                    request_timeout_ms: Some(self.config.tool_timeout_ms),
                    diagnostics_settle_ms: None,
                },
            )))
        };
        let selected = self.selected_capability_roots.as_deref();
        let browser = update
            .runtime_capabilities
            .resolve_selected(selected)
            .capabilities;
        let old_skills = self
            .current_skills_thunk()
            .map(|skills| skills())
            .unwrap_or_default();
        let next_skills = update
            .skills
            .as_ref()
            .map(|skills| skills())
            .unwrap_or_default();
        let skill_gate = self
            .current_skill_gate()
            .map(|gate| Arc::new(gate.for_catalog(&old_skills, &next_skills)));
        let mut tools = self.tools.clone();
        let subagent_config = self.inference_config();
        if !update.host_supplied_tools {
            let had_task = tools.iter().any(|tool| tool.name() == "task");
            tools.retain(|tool| tool.name() != "task" && tool.name() != "skill");
            if had_task {
                tools.push(crate::tools::builtins::task::create_task_tool(
                    crate::tools::builtins::task::TaskToolOptions {
                        model_catalog: subagent_config.subagent_model_catalog,
                        model_options: subagent_config.subagent_model_options.clone(),
                        agents: update
                            .agents
                            .iter()
                            .map(crate::tools::builtins::index::to_task_agent)
                            .collect(),
                    },
                ));
            }
            if let Some(skills) = &update.skills {
                if !self
                    .config
                    .disabled_tools
                    .contains(&RuntimeString::from("skill"))
                {
                    let skills = skills.clone();
                    let gate = skill_gate.clone();
                    tools.push(crate::extensions::skills::skill_tool::create_skill_tool(
                        move || {
                            let all = skills();
                            gate.as_ref()
                                .map_or_else(|| all.clone(), |gate| gate.filter(&all))
                        },
                    ));
                }
            }
            let browser_tools = crate::tools::builtins::browser::create_browser_tools();
            let browser_names: HashSet<_> = browser_tools.iter().map(|tool| tool.name()).collect();
            tools.retain(|tool| {
                !browser_names.contains(&tool.name())
                    || update.browser_tool_overrides.contains(&tool.name())
            });
            if browser.browser_runtime.is_some() {
                tools.extend(browser_tools.into_iter().filter(|tool| {
                    !update.browser_tool_overrides.contains(&tool.name())
                        && !self.config.disabled_tools.contains(&tool.name())
                }));
            }
            if let Some(definition) = &self.root_agent_definition {
                definition.retain_tools(&mut tools);
            }
            tools = crate::tools::builtins::tool_search::ensure_tool_search(tools);
        }
        let projection = Arc::new(SessionPluginProjection {
            skill_gate,
            agents: AgentRegistry::new(update.agents),
            skills: update.skills,
            commands: update.commands,
            hook_bus,
            tools,
            extension_runtime: update.extension_runtime,
            plugin_mcp_servers: update.plugin_mcp_servers,
            active_output_style,
            lsp_manager,
            lsp_configs,
            runtime_capabilities: update.runtime_capabilities,
        });
        let _turns = self.turn.active_stack.lock_recover();
        if self.lifecycle.is_disposed() {
            return Ok(());
        }
        self.plugin_publication.lock_recover().pending =
            Some(SessionPluginPublication { projection });
        if _turns.is_empty() {
            self.commit_pending_plugins_at_turn_boundary();
        }
        Ok(())
    }

    pub(crate) fn commit_pending_plugins_at_turn_boundary(&self) {
        let mut publication = self.plugin_publication.lock_recover();
        let Some(pending) = publication.pending.take() else {
            return;
        };
        *self.mcp.plugin_servers.write_recover() = crate::engine::agent::merge_session_mcp_servers(
            self.manual_mcp_servers(),
            pending.projection.plugin_mcp_servers.clone(),
        );
        let old_lsp = publication
            .current
            .as_ref()
            .map(|projection| projection.lsp_manager.clone())
            .unwrap_or_else(|| self.lsp_manager.clone());
        let new_lsp = pending.projection.lsp_manager.clone();
        let prior_gate = publication
            .current
            .as_ref()
            .map(|projection| projection.skill_gate.clone())
            .unwrap_or_else(|| self.skill_gate.clone());
        if let (Some(prior), Some(next)) = (prior_gate, pending.projection.skill_gate.as_ref()) {
            let old_skills = publication
                .current
                .as_ref()
                .and_then(|projection| projection.skills.as_ref())
                .or(self.skills_thunk.as_ref())
                .map(|skills| skills())
                .unwrap_or_default();
            let next_skills = pending
                .projection
                .skills
                .as_ref()
                .map(|skills| skills())
                .unwrap_or_default();
            next.inherit_unchanged(&prior, &old_skills, &next_skills);
        }
        self.begin_mcp_registration_change();
        publication.current = Some(pending.projection);
        publication.step_mcp_applied = None;
        publication.mcp_dirty = true;
        drop(publication);
        if let Some(old) = old_lsp {
            if new_lsp
                .as_ref()
                .is_none_or(|current| !Arc::ptr_eq(&old, current))
            {
                let pinned_by_session = self
                    .lsp_manager
                    .as_ref()
                    .is_some_and(|initial| Arc::ptr_eq(initial, &old));
                tokio::spawn(async move {
                    // A detached child can retain a controller captured before
                    // this boundary. Retire its manager only after those leases
                    // have dropped; the original manager is also pinned by Session.
                    let idle_refs = if pinned_by_session { 2 } else { 1 };
                    while Arc::strong_count(&old) > idle_refs {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                    old.dispose_all().await;
                });
            }
        }
        self.rebuild_default_system_prompt();
    }

    pub(crate) fn has_pending_plugin_update(&self) -> bool {
        self.plugin_publication.lock_recover().pending.is_some()
    }

    pub(crate) fn plugin_projection(&self) -> Option<Arc<SessionPluginProjection>> {
        self.plugin_publication.lock_recover().current.clone()
    }

    pub(crate) fn current_lsp_manager(&self) -> Option<Arc<LspManager>> {
        self.plugin_projection()
            .map(|projection| projection.lsp_manager.clone())
            .unwrap_or_else(|| self.lsp_manager.clone())
    }

    pub(crate) fn current_agent_registry(&self) -> AgentRegistry {
        self.plugin_projection()
            .map(|projection| projection.agents.clone())
            .unwrap_or_else(|| self.agent_registry.clone())
    }

    pub(crate) fn capture_tool_generation(
        &self,
    ) -> (Vec<Arc<dyn Tool>>, super::SessionSkillSnapshot) {
        match self.plugin_projection() {
            Some(projection) => (
                projection.tools.clone(),
                super::SessionSkillSnapshot {
                    skills: projection.skills.clone(),
                    gate: projection.skill_gate.clone(),
                },
            ),
            None => (
                self.tools.clone(),
                super::SessionSkillSnapshot {
                    skills: self.skills_thunk.clone(),
                    gate: self.skill_gate.clone(),
                },
            ),
        }
    }

    pub(crate) fn current_skill_gate(&self) -> Option<Arc<ConditionalSkillGate>> {
        self.plugin_projection()
            .map(|projection| projection.skill_gate.clone())
            .unwrap_or_else(|| self.skill_gate.clone())
    }

    pub(crate) fn current_skills_thunk(&self) -> Option<Arc<dyn Fn() -> Vec<Skill> + Send + Sync>> {
        self.plugin_projection()
            .map(|projection| projection.skills.clone())
            .unwrap_or_else(|| self.skills_thunk.clone())
    }

    pub(crate) fn current_hook_bus(&self) -> Option<Arc<crate::hooks::bus::HookBus>> {
        self.plugin_projection()
            .map(|projection| projection.hook_bus.clone())
            .unwrap_or_else(|| self.hook_bus.clone())
    }

    pub(crate) fn current_tools(&self) -> Vec<Arc<dyn Tool>> {
        let mut tools = self
            .plugin_projection()
            .map(|projection| projection.tools.clone())
            .unwrap_or_else(|| self.tools.clone());
        self.prepare_planning_tool_inventory(&mut tools);
        tools
    }

    pub(crate) fn prepare_subagent_tool_inventory(
        &self,
        tools: &mut [Arc<dyn Tool>],
        config: &ResolvedConfig,
    ) {
        if let Some(tool) = tools.iter_mut().find(|tool| tool.name() == "task") {
            *tool = crate::tools::builtins::task::create_task_tool(
                crate::tools::builtins::task::TaskToolOptions {
                    model_catalog: config.subagent_model_catalog,
                    model_options: config.subagent_model_options.clone(),
                    agents: self
                        .current_agent_registry()
                        .list()
                        .iter()
                        .map(crate::tools::builtins::index::to_task_agent)
                        .collect(),
                },
            );
        }
    }

    pub(crate) fn prepare_planning_tool_inventory(&self, tools: &mut Vec<Arc<dyn Tool>>) {
        tools.retain(|tool| {
            !matches!(
                tool.name().try_to_utf8().ok().as_deref(),
                Some("get_goal" | "create_goal" | "update_goal")
            )
        });
        if self
            .goal
            .as_ref()
            .map(|goal| goal.enabled())
            .unwrap_or(self.config.goal)
        {
            tools.extend(
                crate::goal::tools::create_goal_tools()
                    .into_iter()
                    .filter(|tool| !self.config.disabled_tools.contains(&tool.name())),
            );
        }
        tools.retain(|tool| {
            !matches!(
                tool.name().try_to_utf8().ok().as_deref(),
                Some(
                    "enter_plan_mode"
                        | "create_plan"
                        | "suggest_execution_mode"
                        | "request_user_input"
                        | "update_plan"
                )
            )
        });
        tools.extend(
            crate::planning::tools::create_planning_tools(self.planning.clone())
                .into_iter()
                .filter(|tool| !self.config.disabled_tools.contains(&tool.name())),
        );
    }

    pub(crate) fn mcp_registration_revision(&self) -> u64 {
        self.mcp.registration_revision.load(Ordering::SeqCst)
    }

    fn begin_mcp_registration_change(&self) {
        self.mcp
            .registration_revision
            .fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn set_manual_mcp_servers(&self, configs: Vec<McpServerConfig>) {
        *self.mcp.manual_servers.write_recover() = configs;
    }

    pub(crate) fn manual_mcp_servers(&self) -> Vec<McpServerConfig> {
        self.mcp.manual_servers.read_recover().clone()
    }

    async fn bind_native_mcp_config_origins(&self, configs: &mut [McpServerConfig]) {
        let cwd = String::from_utf8(self.cwd_now().as_runtime_string().to_node_utf8())
            .expect("Node path is UTF-8");
        crate::mcp::approval_provenance::bind_native_file_origins(&cwd, configs).await;
    }

    pub(crate) async fn reconcile_plugin_mcp_for_step(self: &Arc<Self>) -> Result<bool, String> {
        let mut changed = false;
        loop {
            let projection = {
                let publication = self.plugin_publication.lock_recover();
                let Some(pending) = publication.pending.as_ref() else {
                    return Ok(changed);
                };
                if publication
                    .step_mcp_applied
                    .as_ref()
                    .is_some_and(|applied| Arc::ptr_eq(applied, &pending.projection))
                {
                    return Ok(changed);
                }
                pending.projection.clone()
            };
            let mut configs = crate::engine::agent::merge_session_mcp_servers(
                self.manual_mcp_servers(),
                projection.plugin_mcp_servers.clone(),
            );
            self.bind_native_mcp_config_origins(&mut configs).await;
            let _gate = self.mcp.gate.write().await;
            self.ensure_mcp_manager_initialized().await;
            let mut manager = self.mcp.manager.lock().await;
            manager
                .set_connection_context(Some(self.cwd_now()), self.turn.current_abort_snapshot());
            self.begin_mcp_registration_change();
            manager.reload(configs.clone()).await?;
            self.sync_mcp_runtime_surface(&manager);
            drop(manager);
            *self.mcp.plugin_servers.write_recover() = configs;
            let mut publication = self.plugin_publication.lock_recover();
            publication.step_mcp_applied = Some(projection.clone());
            changed = true;
            if publication
                .pending
                .as_ref()
                .is_some_and(|pending| Arc::ptr_eq(&pending.projection, &projection))
            {
                return Ok(changed);
            }
        }
    }

    pub(crate) async fn reconcile_plugin_mcp_for_turn(self: &Arc<Self>) -> Result<(), String> {
        if !self.plugin_publication.lock_recover().mcp_dirty {
            return Ok(());
        }
        let _gate = self.mcp.gate.write().await;
        if let Some(projection) = self.plugin_projection() {
            *self.mcp.plugin_servers.write_recover() =
                crate::engine::agent::merge_session_mcp_servers(
                    self.manual_mcp_servers(),
                    projection.plugin_mcp_servers.clone(),
                );
        }
        self.ensure_mcp_manager_initialized().await;
        let mut configs = self.mcp.plugin_servers();
        self.bind_native_mcp_config_origins(&mut configs).await;
        let mut manager = self.mcp.manager.lock().await;
        manager.set_connection_context(Some(self.cwd_now()), self.turn.current_abort_snapshot());
        self.begin_mcp_registration_change();
        manager.reload(configs.clone()).await?;
        *self.mcp.plugin_servers.write_recover() = configs;
        self.sync_mcp_runtime_surface(&manager);
        self.plugin_publication.lock_recover().mcp_dirty = false;
        Ok(())
    }

    pub(crate) async fn plugin_mcp_clients(self: &Arc<Self>) -> Vec<Arc<ManagedClient>> {
        self.ensure_mcp_manager_initialized().await;
        self.plugin_mcp_clients_snapshot()
    }

    pub(crate) async fn ensure_mcp_manager_initialized(self: &Arc<Self>) {
        self.mcp
            .manager_initialized
            .get_or_init(|| async {
                let mut configs = self.mcp.plugin_servers();
                self.bind_native_mcp_config_origins(&mut configs).await;
                let mut manager = self.mcp.manager.lock().await;
                manager.set_connection_context(
                    Some(self.cwd_now()),
                    self.turn.current_abort_snapshot(),
                );
                self.begin_mcp_registration_change();
                manager.connect_all(configs.clone()).await;
                *self.mcp.plugin_servers.write_recover() = configs;
                self.sync_mcp_runtime_surface(&manager);
            })
            .await;
    }

    pub(crate) fn sync_mcp_runtime_surface(&self, manager: &McpConnectionManager) {
        self.mcp
            .set_runtime_clients(manager.clients().into_iter().map(Arc::new).collect());
    }

    pub(crate) fn plugin_mcp_clients_snapshot(&self) -> Vec<Arc<ManagedClient>> {
        self.mcp.runtime_clients_snapshot()
    }

    pub(crate) fn acquire_mcp_mutation_guard(
        &self,
    ) -> Result<tokio::sync::OwnedRwLockWriteGuard<()>, String> {
        let guard = self.mcp.gate.clone().try_write_owned().map_err(|_| {
            "session_busy: cannot mutate MCP runtime during an active turn".to_string()
        })?;
        if self.turn.is_active() {
            return Err(
                "session_busy: cannot mutate MCP runtime during an active turn".to_string(),
            );
        }
        if self.is_disposed() {
            return Err("session_closed: cannot mutate MCP runtime after shutdown".to_string());
        }
        Ok(guard)
    }

    pub async fn mcp_runtime_snapshot(self: &Arc<Self>) -> McpRuntimeSnapshot {
        self.ensure_mcp_manager_initialized().await;
        self.mcp.manager.lock().await.runtime_snapshot()
    }

    pub async fn reconnect_mcp_server(
        self: &Arc<Self>,
        server_name: &str,
    ) -> Result<McpRuntimeSnapshot, String> {
        let _mutation_guard = self.acquire_mcp_mutation_guard()?;
        self.ensure_mcp_manager_initialized().await;
        let mut manager = self.mcp.manager.lock().await;
        if manager.config(server_name).is_none() {
            return Err(format!("not_found: MCP server '{server_name}'"));
        }
        manager.set_connection_context(Some(self.cwd_now()), None);
        self.begin_mcp_registration_change();
        manager.reconnect(server_name).await;
        self.sync_mcp_runtime_surface(&manager);
        Ok(manager.runtime_snapshot())
    }

    pub async fn upsert_mcp_server(
        self: &Arc<Self>,
        mut config: McpServerConfig,
    ) -> Result<McpRuntimeSnapshot, String> {
        let _mutation_guard = self.acquire_mcp_mutation_guard()?;
        self.bind_native_mcp_config_origins(std::slice::from_mut(&mut config))
            .await;
        self.ensure_mcp_manager_initialized().await;
        let mut manager = self.mcp.manager.lock().await;
        manager.set_connection_context(Some(self.cwd_now()), None);
        self.begin_mcp_registration_change();
        manager.upsert(config.clone()).await;
        self.sync_mcp_runtime_surface(&manager);
        let mut manual = self.mcp.manual_servers.write_recover();
        manual.retain(|existing| existing.name() != config.name());
        manual.push(config);
        Ok(manager.runtime_snapshot())
    }

    pub async fn remove_mcp_server(
        self: &Arc<Self>,
        server_name: &str,
    ) -> Result<McpRuntimeSnapshot, String> {
        let _mutation_guard = self.acquire_mcp_mutation_guard()?;
        self.ensure_mcp_manager_initialized().await;
        let mut manager = self.mcp.manager.lock().await;
        self.begin_mcp_registration_change();
        if !manager.remove(server_name).await {
            return Err(format!("not_found: MCP server '{server_name}'"));
        }
        self.sync_mcp_runtime_surface(&manager);
        self.mcp
            .manual_servers
            .write_recover()
            .retain(|config| config.name() != server_name);
        Ok(manager.runtime_snapshot())
    }

    pub async fn reload_mcp_servers(
        self: &Arc<Self>,
        mut configs: Vec<McpServerConfig>,
    ) -> Result<McpRuntimeSnapshot, String> {
        let _mutation_guard = self.acquire_mcp_mutation_guard()?;
        self.bind_native_mcp_config_origins(&mut configs).await;
        self.ensure_mcp_manager_initialized().await;
        let mut manager = self.mcp.manager.lock().await;
        manager.set_connection_context(Some(self.cwd_now()), None);
        self.begin_mcp_registration_change();
        manager.reload(configs.clone()).await?;
        self.sync_mcp_runtime_surface(&manager);
        *self.mcp.manual_servers.write_recover() = configs;
        Ok(manager.runtime_snapshot())
    }

    /// Host-driven tool call on a connected MCP server. Distinct from a
    /// model-issued tool call: the host names the server and the RAW tool, and
    /// there is no permission gate because the caller is a human action.
    pub async fn host_call_mcp_tool(
        self: &Arc<Self>,
        server: &str,
        tool: &RuntimeString,
        args: crate::types::json::Json,
    ) -> Result<crate::mcp::types::McpCallResult, RuntimeString> {
        self.ensure_mcp_manager_initialized().await;
        let manager = self.mcp.manager.lock().await;
        manager.call_tool(server, tool, args, None).await
    }

    /// Host-driven resource read on a connected MCP server.
    pub async fn host_read_mcp_resource(
        self: &Arc<Self>,
        server: &str,
        uri: &RuntimeString,
    ) -> Vec<crate::types::tool::McpResourceContent> {
        self.ensure_mcp_manager_initialized().await;
        let manager = self.mcp.manager.lock().await;
        manager.read_resource(server, uri).await
    }

    /// Render a named prompt on a connected MCP server (the host projects this
    /// into a slash command's inserted content).
    pub async fn host_get_mcp_prompt(
        self: &Arc<Self>,
        server: &str,
        name: &RuntimeString,
        args: Option<crate::runtime_values::RuntimeObject>,
    ) -> Result<RuntimeString, RuntimeString> {
        self.ensure_mcp_manager_initialized().await;
        let manager = self.mcp.manager.lock().await;
        manager.get_prompt(server, name, args).await
    }

    pub fn resolve_mcp_elicitation(
        &self,
        request_id: &str,
        response: McpElicitationResponse,
    ) -> Result<(), String> {
        if self.is_disposed() {
            return Err(
                "session_closed: cannot resolve MCP elicitation after shutdown".to_string(),
            );
        }
        self.mcp_elicitation_router
            .resolve(&RuntimeString::from(request_id), response)
    }

    pub async fn mcp_authorization_status(
        self: &Arc<Self>,
        server_name: &str,
    ) -> Result<McpAuthStatus, String> {
        let manager = self.mcp.manager.lock().await;
        let config = if manager.is_initialized() {
            manager.config(server_name)
        } else {
            self.mcp
                .plugin_servers()
                .into_iter()
                .find(|config| config.name() == server_name)
        }
        .ok_or_else(|| format!("MCP server not found: {server_name}"))?;
        drop(manager);
        mcp_auth_status(&config, self.credential_store.clone())
            .await
            .map_err(|error| format!("failed to read MCP authorization status: {error}"))
    }

    pub async fn revoke_mcp_authorization(
        self: &Arc<Self>,
        server_name: &str,
    ) -> Result<McpAuthStatus, String> {
        let _mutation_guard = self.acquire_mcp_mutation_guard()?;
        let mut manager = self.mcp.manager.lock().await;
        let managed = manager.is_initialized();
        let config = if managed {
            manager.config(server_name)
        } else {
            self.mcp
                .plugin_servers()
                .into_iter()
                .find(|config| config.name() == server_name)
        }
        .ok_or_else(|| format!("MCP server not found: {server_name}"))?;
        let store = self.credential_store.clone().ok_or_else(|| {
            "MCP authorization revoke requires a session credential store".to_string()
        })?;
        let status = revoke_mcp_authorization(&config, store, None)
            .await
            .map_err(|error| format!("failed to revoke MCP authorization: {error}"))?;
        if managed {
            manager.mark_authorization_revoked(server_name).await;
            self.sync_mcp_runtime_surface(&manager);
        }
        Ok(status)
    }

    pub(crate) fn mcp_tools_from_clients(clients: &[Arc<ManagedClient>]) -> Vec<Arc<dyn Tool>> {
        clients.iter().flat_map(|client| client.tools()).collect()
    }

    pub(crate) fn all_mcp_resource_clients_snapshot(&self) -> Vec<Arc<dyn McpResourceClient>> {
        let mut clients = self.mcp.resource_clients();
        clients.extend(
            self.plugin_mcp_clients_snapshot()
                .into_iter()
                .map(|client| {
                    Arc::new(ManagedResourceClient(client, "plugin-mcp-instructions"))
                        as Arc<dyn McpResourceClient>
                }),
        );
        clients
    }

    pub(crate) async fn plugin_mcp_instruction_part(self: &Arc<Self>) -> Option<SystemPromptPart> {
        self.plugin_mcp_clients().await;
        self.plugin_mcp_instruction_snapshot()
    }

    pub(crate) fn plugin_mcp_instruction_snapshot(&self) -> Option<SystemPromptPart> {
        let instructions = self
            .plugin_mcp_clients_snapshot()
            .into_iter()
            .filter_map(|client| {
                client.instructions.as_ref().map(|text| McpInstruction {
                    name: client.name.clone(),
                    instructions: text.clone(),
                })
            })
            .collect::<Vec<_>>();
        if instructions.is_empty() {
            return None;
        }
        let mut body = RuntimeString::default();
        for (index, server) in instructions.iter().enumerate() {
            if index != 0 {
                body = body.concat(&RuntimeString::from("\n\n"));
            }
            body = body
                .concat(&RuntimeString::from("## "))
                .concat(&server.name)
                .concat(&RuntimeString::from("\n"))
                .concat(&server.instructions);
        }
        Some(SystemPromptPart {
            name: "plugin-mcp-instructions".into(),
            text: RuntimeString::from(
                "# MCP Server Instructions\n\nThe following MCP servers have provided usage instructions:\n\n",
            )
            .concat(&body),
            cacheable: Some(true),
            cache_scope: None,
        })
    }

    /// A file tool touched these paths → activate any conditional skill matching
    /// them (best-effort; a bad glob never fails the file op).
    pub(crate) fn activate_skill_paths(&self, paths: &[RuntimePath], cwd: &RuntimePath) {
        let (Some(gate), Some(thunk)) = (self.current_skill_gate(), self.current_skills_thunk())
        else {
            return;
        };
        let _ = gate.activate_for_paths(&thunk(), paths, cwd);
    }

    /// A file tool touched these paths → record them for nested-memory lookup.
    pub(crate) fn feed_nested_memory_triggers(
        &self,
        buffer: &Mutex<HashSet<RuntimePath>>,
        paths: &[RuntimePath],
        cwd: &RuntimePath,
    ) {
        if !self.config.memory.enabled {
            return;
        }
        let mut b = buffer.lock_recover();
        for p in paths {
            b.insert(resolve_runtime_join(cwd, p.as_runtime_string()));
        }
    }

    /// Drain (return + clear) a nested-memory trigger buffer.
    pub(crate) fn drain_nested_memory_triggers(
        buffer: &Mutex<HashSet<RuntimePath>>,
    ) -> Vec<RuntimePath> {
        let mut b = buffer.lock_recover();
        let out = b.iter().cloned().collect();
        b.clear();
        out
    }

    /// A read-only view of the history.
    pub fn history(&self) -> Vec<Message> {
        self.history_snapshot()
    }

    /// Read only the selected conversation's recent visible text under a strict byte limit.
    pub fn realtime_context_text(&self, max_bytes: usize) -> String {
        let limit = max_bytes.min(16 * 1024);
        let history = self.conversation.history.lock_recover();
        let mut lines = Vec::new();
        let mut remaining = limit;
        'messages: for message in history.iter().rev() {
            let (role, content) = match message {
                Message::User(message) if message.is_meta != Some(true) => {
                    ("User", &message.content)
                }
                Message::Assistant(message) => ("Assistant", &message.content),
                _ => continue,
            };
            for block in content.iter().rev() {
                let ContentBlock::Text(block) = block else {
                    continue;
                };
                if block.harness_envelope.is_some() {
                    continue;
                }
                let Ok(text) = block.text.try_to_utf8() else {
                    continue;
                };
                if text.is_empty() {
                    continue;
                }
                let prefix = format!("{role}: ");
                let overhead = prefix.len() + 1;
                if remaining <= overhead {
                    break 'messages;
                }
                let available = remaining - overhead;
                let mut start = text.len().saturating_sub(available);
                while !text.is_char_boundary(start) {
                    start += 1;
                }
                let line = format!("{prefix}{}\n", &text[start..]);
                remaining -= line.len();
                lines.push(line);
                if start > 0 {
                    break 'messages;
                }
            }
        }
        lines.reverse();
        lines.concat()
    }

    /// Append one provider-confirmed realtime transcript item with stable replay identity.
    pub async fn append_realtime_transcript(
        &self,
        id: &str,
        role: &str,
        text: &str,
        model: &str,
    ) -> Result<bool, String> {
        if self.is_disposed() {
            return Err("the session is closing".into());
        }
        if id.is_empty() || text.trim().is_empty() || text.len() > 1024 * 1024 {
            return Err("invalid realtime transcript item".into());
        }
        let content = vec![ContentBlock::Text(crate::schemas::content::TextBlock {
            text: text.into(),
            citations: None,
            harness_envelope: None,
            message_id: Some(id.into()),
            phase: (role == "assistant").then(|| "final_answer".into()),
        })];
        let message = match role {
            "user" => Message::User(crate::schemas::message::UserMessage {
                uuid: id.into(),
                question_answer: None,
                content,
                is_meta: None,
            }),
            "assistant" => Message::Assistant(crate::schemas::message::AssistantMessage {
                uuid: id.into(),
                content,
                model: model.into(),
                usage: None,
            }),
            _ => return Err("realtime transcripts require a user or assistant role".into()),
        };
        let admission = match self.persistence.rollout() {
            Some(rollout) => Some(
                rollout
                    .prepare_append()
                    .await
                    .map_err(|error| error.to_string())?,
            ),
            None => None,
        };
        {
            let mut history = self.conversation.history.lock_recover();
            if self.is_disposed() {
                return Err("the session is closing".into());
            }
            if history
                .iter()
                .any(|existing| message_uuid(existing) == &RuntimeString::from(id))
            {
                return Ok(false);
            }
            history.push(message.clone());
            if let Some(admission) = admission {
                admission.commit(crate::schemas::rollout::RolloutItem::Message { message });
            }
        }
        self.flush_rollout().await?;
        Ok(true)
    }

    /// Capture only completed parent context without waiting for its active turn.
    /// Keep history and lifecycle selection together; retain the completed
    /// context prefix without copying the active parent's unfinished tail.
    pub fn side_task_snapshot(&self) -> Result<LiveSideTaskSnapshot, String> {
        let _turns = self.turn.active_stack.lock_recover();
        if self.lifecycle.is_disposed() {
            return Err("cannot create a side task from a closing session".to_string());
        }
        let inherited = {
            let history = self.conversation.history.lock_recover();
            let index = self.turn_index.lock_recover();
            crate::engine::agent::completed_context_prefix(&history, &index)
        };
        let approval_context = self
            .approval_review
            .history
            .lock_recover()
            .prefix_for_messages(&inherited)
            .checkpoint();
        Ok(LiveSideTaskSnapshot {
            approval_context,
            source_thread_id: self.id.clone(),
            cwd: self.cwd_now(),
            history: inherited,
        })
    }

    /// A named atomic start must not accept a factory that ignored its side intent.
    pub fn validate_side_task_identity(&self, parent_thread_id: &str) -> Result<(), String> {
        if self.meta.role != Some(crate::schemas::rollout::SessionRole::SideTask)
            || self.meta.parent_thread_id.as_ref() != Some(&RuntimeString::from(parent_thread_id))
            || self.meta.forked_from_id.is_some()
        {
            return Err("endpoint factory did not create the requested side task".to_string());
        }
        Ok(())
    }

    /// Whether this session is the ephemeral side-task variant that persists
    /// nothing, as opposed to a persisted side thread with its own rollout.
    pub fn is_ephemeral(&self) -> bool {
        self.persistence.store.is_none() && self.persistence.rollout.is_none()
    }

    /// Capture a complete idle transcript under the same lock that admits
    /// ActiveTurnLease and closes turn admission. The source can start another
    /// turn after this synchronous capture; its immutable fork is independent.
    pub fn persistent_fork_snapshot(
        &self,
        before_message_uuid: Option<&str>,
    ) -> Result<LiveForkSnapshot, String> {
        let turns = self.turn.active_stack.lock_recover();
        if self.lifecycle.is_disposed() {
            return Err("cannot fork a closing live session".to_string());
        }
        if !turns.is_empty() {
            return Err("cannot fork a running live session".to_string());
        }
        let mut history = self.history_snapshot();
        if let Some(cut) = before_message_uuid {
            let position = history
                .iter()
                .position(|message| *message_uuid(message) == *cut)
                .ok_or_else(|| {
                    format!("fork cut point {cut} does not exist in the live session")
                })?;
            history.truncate(position);
        }
        let planning = self
            .planning
            .snapshot()
            .map_err(|error| crate::planning::runtime::host(&error))?;
        // The empty active-turn lease is still held here. Read the stored
        // index directly rather than re-entering its live-status projection.
        let index = self.turn_index.lock_recover().clone();
        let mut retained_planning_turns: std::collections::HashSet<RuntimeString> = history
            .iter()
            .filter_map(|message| index.message_turn.get(message_uuid(message)).cloned())
            .collect();
        if before_message_uuid.is_none() {
            retained_planning_turns.extend(planning.plans.iter().map(|plan| plan.turn_id.clone()));
            retained_planning_turns.extend(planning.questions.iter().filter_map(|q| {
                q.get("turnId")
                    .and_then(crate::runtime_values::RuntimeJson::as_str)
                    .cloned()
            }));
        }
        let approval_context = {
            let captured = self.approval_review.history.lock_recover();
            if before_message_uuid.is_some() {
                captured.prefix_for_messages(&history).checkpoint()
            } else {
                captured.checkpoint()
            }
        };
        Ok(LiveForkSnapshot {
            approval_context,
            planning,
            retained_planning_turns,
            inherited_goal: None,
            source_thread_id: self.id.clone(),
            cwd: self.cwd_now(),
            history,
        })
    }

    /// A live-fork facade must not report success when a custom endpoint factory
    /// ignored its snapshot or returned another ephemeral session.
    pub async fn flush_persistent_fork(&self, snapshot: &LiveForkSnapshot) -> Result<(), String> {
        if self.persistence.store.is_none()
            || self.persistence.rollout.is_none()
            || self.meta.forked_from_id.as_ref() != Some(&snapshot.source_thread_id)
            || self.meta.role != Some(crate::schemas::rollout::SessionRole::Main)
            || self.meta.parent_thread_id.is_some()
            || self.id == snapshot.source_thread_id
            || self.history_snapshot() != snapshot.history
            || !self.conversation.hidden_message_ids.is_empty()
        {
            return Err(
                "endpoint factory did not create a persistent ordinary live fork".to_string(),
            );
        }
        self.flush_rollout().await
    }

    pub fn visible_history(&self) -> Vec<Message> {
        self.history_snapshot()
            .into_iter()
            .filter(|message| {
                !crate::context::compact::full_compact::is_compaction_output(message)
                    && !self
                        .conversation
                        .hidden_message_ids
                        .contains(message_uuid(message))
            })
            .collect()
    }

    pub(crate) fn model_history(&self) -> Vec<Message> {
        self.history_snapshot()
            .into_iter()
            .filter(|message| {
                !matches!(message, Message::System(system)
                    if system.subtype == crate::schemas::message::SystemSubtype::UserDisplay)
            })
            .collect()
    }

    /// The session's live working directory.
    pub fn cwd(&self) -> RuntimePath {
        self.cwd_now()
    }

    /// Persistently switch the model for SUBSEQUENT turns.
    pub fn set_model(&self, model: impl Into<RuntimeString>) -> Result<(), String> {
        self.set_legacy_model(model.into(), false)
    }

    /// Switch the model mid-turn: writes into the innermost running turn's
    /// live override slot (picked up before the next LLM call in the ReAct
    /// loop) and also stages it for subsequent turns.
    pub fn set_active_turn_model(&self, model: impl Into<RuntimeString>) -> Result<(), String> {
        self.set_legacy_model(model.into(), true)
    }

    /// Translate a deprecated preset into canonical runtime policy fields.
    /// Historical planning inputs select collaboration mode independently.
    pub fn set_permission_mode(&self, mode: ExternalPermissionMode) -> Result<(), String> {
        self.request_permission_mode_change(mode)
    }
}

impl Session {
    pub fn collaboration_mode(&self) -> crate::planning::CollaborationMode {
        self.planning.desired_mode()
    }
    pub fn planning_runtime(&self) -> Arc<crate::planning::runtime::PlanningRuntime> {
        self.planning.clone()
    }
}
