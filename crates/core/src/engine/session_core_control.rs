use super::bare_tool_context;
use super::base_hook_input;
use super::budget_state;
use super::conversation_text_for_title;
use super::join_opts;
use super::join_runtime_strings;
use super::memory_maintenance_event_status;
use super::nonempty_trimmed;
use super::parse_generated_title;
use super::reap_task_list_subscriptions;
use super::runtime_assistant_id_to_cost_host;
use super::runtime_error_message_to_public_host;
use super::runtime_model_to_cost_host;
use super::runtime_prompt_to_background_host;
use super::runtime_session_id_to_session_store_host;
use super::runtime_session_id_to_title_store_host;
use super::runtime_title_to_store_host;
use super::runtime_transcript_to_background_host;
use super::runtime_trim;
use super::side_question_input;
use super::title_output_schema;
use super::to_content_blocks;
use super::validate_native_pdf_provider;
use super::BackgroundAgentInfo;
use super::CacheTtl;
use super::CompactOutcome;
use super::DreamInProgressGuard;
use super::DreamNowCompletion;
use super::DreamNowReason;
use super::DreamNowTask;
use super::EnqueueOptions;
use super::FeatureSlot;
use super::GenerateTitleResult;
use super::Input;
use super::IsolatedMaintenanceSinkDispatcher;
use super::MaintenanceDurabilityTaskControl;
use super::MaintenanceDurabilityTaskRegistration;
use super::MaintenanceEventSinkRegistration;
use super::MaintenanceProvisional;
use super::MaintenanceRegistry;
use super::MaintenanceRunKey;
use super::MaintenanceTask;
use super::MaintenanceTaskKind;
use super::MemoryForkFailureKind;
use super::ReadonlyQueryOptions;
use super::ReadonlyQueryOutcome;
use super::RollbackOutcome;
use super::Session;
use super::SessionContextSnapshot;
use super::ShutdownPhaseFailure;
use super::ShutdownPhaseResult;
use super::SideQuestionResult;
use super::SnapshotDurabilityOperation;
use super::SnapshotDurabilityResult;
use super::SnapshotOrderedReply;
use super::SystemPromptResolvable;
use super::SystemPromptThunk;
use super::TaskListSubscriptionReaper;
use super::ThinkingConfig;
use super::MAINTENANCE_FORCE_ABORT_ERROR;
use super::MAINTENANCE_SHUTDOWN_ERROR;
use super::MANUAL_DREAM_SHUTDOWN_ERROR;
use super::SESSION_TITLE_PROMPT;
use super::SIDE_QUESTION_SYSTEM_PROMPT;
use crate::checkpoint::checkpoint_service::CheckpointService;
use crate::checkpoint::checkpoint_service::CheckpointServiceOptions;
use crate::command_runtime::background::BackgroundProcessEventSink;
use crate::context::env_info::environment_block;
use crate::context::env_info::EnvBlockOptions;
use crate::context::tokens::estimate_context_tokens;
use crate::context::tokens::invalidate_context_usage;
use crate::engine::message_queue::MessageQueue;
use crate::engine::message_queue::QueueEdit;
use crate::engine::message_queue::QueueMutationResult;
use crate::engine::message_queue::QueueMutationStatus;
use crate::engine::message_queue::QueueSnapshot;
use crate::engine::message_queue::QueueStateSubscriber;
use crate::engine::message_queue::QueueTerminalState;
use crate::engine::message_queue::QueuedMessage;
use crate::engine::message_queue::QueuedValue;
use crate::engine::query::compaction_summary_of;
use crate::engine::query::QueryParams;
use crate::engine::query::ToolContextFactory;
use crate::engine::subagent_lifecycle::{SubagentContext, SubagentSnapshot};
use crate::extensions::failure::ExtensionFailureSink;
use crate::extensions::runtime_snapshot::ExtensionRuntimeSnapshot;
use crate::hooks::types::HookEvent;
use crate::hooks::types::HookInput;
use crate::hooks::types::HookTrigger;
use crate::memory::agent_memory::build_agent_memory_prompt_text;
use crate::memory::agent_memory::AgentMemoryMergeChoice;
use crate::memory::agent_memory::AgentMemoryScope;
use crate::memory::consolidation_lock::try_acquire_consolidation_lock_pinned;
use crate::memory::maintenance::AutoDreamRunReason;
use crate::memory::maintenance::AutoMemoryRunReason;
use crate::memory::maintenance::MemoryMaintenanceEvent;
use crate::memory::maintenance::MemoryMaintenanceEventSink;
use crate::memory::maintenance::MemoryMaintenanceQueryError;
use crate::memory::maintenance::MemoryMaintenanceSnapshot;
use crate::memory::maintenance::MemoryMaintenanceStatus;
use crate::memory::maintenance::MEMORY_MAINTENANCE_ROLLOUT_EVENT;
use crate::memory::snapshot_decision::AgentMemorySnapshotApplyError;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionAudit;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionSource;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionStatus;
use crate::memory::snapshot_decision::AgentMemorySnapshotRegistryError;
use crate::observability::cost_tracker::{CostSnapshot, CostTracker, UsageAttribution};
use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::runtime_codec;
use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::schemas::background_process::{
    BackgroundProcessOutput, BackgroundProcessSnapshot, BackgroundProcessStopResult,
};
use crate::schemas::message::AssistantMessage;
use crate::schemas::message::Message;
use crate::schemas::message::UserMessage;
use crate::store::history_rewind::count_user_turns;
use crate::store::history_rewind::drop_last_n_user_turns;
use crate::store::session_store::CheckpointRecord;
use crate::store::session_store::CompactionRecord;
use crate::sync_ext::{MutexExt, RwLockExt};
use crate::tasks::task_store::get_task_list_id;
use crate::tasks::task_store::query_task_list_ordered;
use crate::tasks::task_store::subscribe_task_states;
use crate::tasks::task_store::TaskListBinding;
use crate::tasks::task_store::TaskListChange;
use crate::tasks::task_store::TaskListChangeEnvelope;
use crate::tasks::task_store::TaskListChangeEnvelopeSubscriber;
use crate::tasks::task_store::TaskListChangeSubscriber;
use crate::tasks::task_store::TaskListQueryOptions;
use crate::tasks::task_store::TaskListQueryResult;
use crate::tasks::task_store::TaskListSnapshot;
use crate::tools::shell_env_snapshot::capture_login_shell_env;
use crate::tools::shell_env_snapshot::CaptureShellEnvOptions;
use crate::types::abort::AbortSignal;
use crate::types::errors::InputValidationError;
use crate::types::errors::InputValidationErrorCode;
use crate::types::errors::TerminalReason;
use crate::types::events::EngineEvent;
use crate::types::events::ThreadItem;
use crate::types::file_state::FileStateCache;
use crate::types::json::runtime_schema_from_static;
use crate::types::provider::ModelCapabilities;
use crate::types::provider::SystemPromptPart;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

// Rewind, checkpoints, steering, and turn-control surfaces.
pub(crate) const PREDICTION_SOURCE_ROLLOUT_EVENT: &str = "prediction_source";

impl Session {
    /// Current authoritative background-process state for this Session.
    pub fn background_process_snapshot(&self) -> BackgroundProcessSnapshot {
        self.background_processes.snapshot()
    }

    /// Offset-based output read that does not advance the model's bash_output cursor.
    pub fn read_background_process_output(
        &self,
        shell_id: &RuntimeString,
        after_offset: Option<u64>,
        max_bytes: Option<usize>,
    ) -> BackgroundProcessOutput {
        self.background_processes
            .read_output(shell_id, after_offset, max_bytes)
    }

    /// Request that one Session-owned background process stop.
    pub fn stop_background_process(
        &self,
        shell_id: &RuntimeString,
        signal: crate::tools::shell::Signal,
    ) -> BackgroundProcessStopResult {
        self.background_processes.stop_with_signal(shell_id, signal)
    }

    /// Signal immediately and fence the monitor-owned lifecycle record.
    pub async fn stop_background_process_and_wait(
        &self,
        shell_id: &RuntimeString,
        signal: crate::tools::shell::Signal,
    ) -> Result<BackgroundProcessStopResult, RuntimeString> {
        self.background_processes
            .stop_with_signal_and_wait(shell_id, signal)
            .await
    }

    /// Subscribe to cc-style lifecycle changes for later Host/protocol bridging.
    pub fn subscribe_background_process_changes(
        &self,
        sink: BackgroundProcessEventSink,
    ) -> Box<dyn Fn() + Send + Sync> {
        self.background_processes.subscribe(sink)
    }

    // ---- rewind / compaction ----

    /// Rewind the conversation by `num_turns` user turns. Throws if `num_turns < 1`
    /// or a turn is currently in progress.
    pub async fn rollback(&self, num_turns: u32) -> Result<RollbackOutcome, String> {
        if num_turns < 1 {
            return Err("rollback requires an integer numTurns >= 1".to_string());
        }
        {
            let turns = self.turn.active_stack.lock_recover();
            if !turns.is_empty() || self.turn.is_active() {
                return Err("cannot rollback while a turn is in progress".to_string());
            }
        }
        let context_update = self.turn.context_window_update_gate.lock().await;
        let rollout = self.persistence.rollout();
        let prepared = match rollout {
            Some(rollout) => Some(
                rollout
                    .prepare_batch(4)
                    .await
                    .map_err(|error| format!("cannot admit rollback persistence: {error}"))?,
            ),
            None => None,
        };
        let (outcome, records) = {
            let turns = self.turn.active_stack.lock_recover();
            if !turns.is_empty() || self.turn.is_active() {
                return Err("cannot rollback while a turn is in progress".to_string());
            }
            let generation = self
                .turn
                .reserve_generation()
                .ok_or_else(|| "turn admission counter is exhausted".to_string())?;
            let mut hist = self.conversation.history.lock_recover();
            let before_turns = count_user_turns(&hist);
            let new_hist = drop_last_n_user_turns(&hist, num_turns as f64);
            let removed = &hist[new_hist.len()..];
            let first_removed = removed.iter().find_map(|message| match message {
                Message::User(user) if user.is_meta != Some(true) => Some(user.uuid.clone()),
                _ => None,
            });
            let index = self.turn_index.lock_recover();
            let removed_turns = removed
                .iter()
                .filter_map(|message| match message {
                    Message::User(user) => Some(&user.uuid),
                    Message::Assistant(assistant) => Some(&assistant.uuid),
                    Message::ToolResult(result) => Some(&result.uuid),
                    _ => None,
                })
                .filter_map(|id| index.message_turn.get(id))
                .cloned()
                .collect::<std::collections::HashSet<_>>();
            drop(index);
            if let Some(boundary) = first_removed {
                let removed_names = removed_turns
                    .iter()
                    .map(|id| String::from_utf8(id.to_node_utf8()).expect("Node text is UTF-8"))
                    .collect::<Vec<_>>();
                self.approval_review.history.lock_recover().rewind(
                    &String::from_utf8(boundary.to_node_utf8()).expect("Node text is UTF-8"),
                    &removed_names.iter().map(String::as_str).collect::<Vec<_>>(),
                );
                self.approval_review
                    .events
                    .lock_recover()
                    .retain(|review| !removed_turns.contains(&review.turn_id));
                self.approval_async.reset();
            }
            *hist = new_hist;
            self.update_prediction_source_checkpoint(generation, None);
            let remaining_turns = count_user_turns(&hist);
            let messages = hist.len();
            drop(hist);
            let mut prediction = crate::runtime_values::RuntimeObject::new();
            prediction.insert("generation", RuntimeJson::from(generation.to_string()));
            let mut records = vec![
                crate::schemas::rollout::RolloutItem::Event(crate::schemas::rollout::EventItem {
                    event: PREDICTION_SOURCE_ROLLOUT_EVENT.into(),
                    turn_id: None,
                    payload: Some(RuntimeJson::Object(prediction)),
                }),
                crate::schemas::rollout::RolloutItem::Rollback(
                    crate::schemas::rollout::RollbackItem { num_turns },
                ),
            ];
            let active_tokens = estimate_context_tokens(&self.model_history());
            let window = {
                let mut state = self.turn.context_window_usage.lock_recover();
                state.active_tokens = Some(active_tokens);
                state.prefill_input_tokens = None;
                state.window_number = state.window_number.saturating_add(1);
                state.clone()
            };
            records.push(crate::schemas::rollout::RolloutItem::Event(
                crate::schemas::rollout::EventItem {
                    event: crate::context::window_usage::WINDOW_USAGE_EVENT.into(),
                    turn_id: None,
                    payload: Some(crate::context::window_usage::encode(&window)),
                },
            ));
            if let Some(payload) = self.review_parent_checkpoint_payload() {
                records.push(crate::schemas::rollout::RolloutItem::Event(
                    crate::schemas::rollout::EventItem {
                        event: crate::engine::approval_capture::APPROVAL_PARENT_HISTORY_EVENT
                            .into(),
                        turn_id: None,
                        payload: Some(payload),
                    },
                ));
            }
            drop(turns);
            (
                RollbackOutcome {
                    dropped_turns: before_turns.saturating_sub(remaining_turns),
                    remaining_turns,
                    messages,
                },
                records,
            )
        };
        if let Some(prepared) = prepared {
            // Invalidation, rollback and their replacement checkpoints commit
            // without a suspension point after changing conversation state.
            prepared.commit(records);
        }
        drop(context_update);
        Ok(outcome)
    }

    /// Host-triggered manual compaction. Refused while a turn is active.
    pub async fn compact(
        &self,
        instructions: Option<RuntimeString>,
    ) -> Result<CompactOutcome, String> {
        self.compact_with_selection(instructions, None).await
    }

    /// Summarize one side of a stable message boundary through the manual policy.
    pub async fn compact_selected(
        &self,
        instructions: Option<RuntimeString>,
        message_id: RuntimeString,
        direction: crate::context::compact::full_compact::PartialCompactionDirection,
    ) -> Result<CompactOutcome, String> {
        self.compact_with_selection(instructions, Some((message_id, direction)))
            .await
    }

    async fn compact_with_selection(
        &self,
        instructions: Option<RuntimeString>,
        selection: Option<(
            RuntimeString,
            crate::context::compact::full_compact::PartialCompactionDirection,
        )>,
    ) -> Result<CompactOutcome, String> {
        let lease = super::ManualCompactionLease::acquire(self)?;
        let (outcome, accepted_summary) = tokio::select! {
            biased;
            _ = lease.signal.cancelled() => return Err("Context compaction was interrupted.".to_owned()),
            result = self.compact_with_selection_inner(instructions, selection, &lease.signal) => result?,
        };
        if outcome.compacted {
            if let Some(bus) = self
                .current_hook_bus()
                .filter(|bus| bus.has(HookEvent::PostCompact))
            {
                let input = HookInput {
                    trigger: Some(HookTrigger::Manual),
                    message: accepted_summary,
                    ..base_hook_input(HookEvent::PostCompact, &self.cwd_now(), &self.id)
                };
                let hook = tokio::select! {
                    biased;
                    _ = lease.signal.cancelled() => return Err("Context compaction was interrupted after history replacement.".to_owned()),
                    hook = bus.dispatch(HookEvent::PostCompact, &input) => hook,
                };
                if hook.aborted || hook.blocked {
                    return Err("Context compaction was stopped by its completion hook after history replacement.".to_owned());
                }
            }
        }
        Ok(outcome)
    }

    async fn compact_with_selection_inner(
        &self,
        instructions: Option<RuntimeString>,
        selection: Option<(
            RuntimeString,
            crate::context::compact::full_compact::PartialCompactionDirection,
        )>,
        signal: &AbortSignal,
    ) -> Result<(CompactOutcome, Option<RuntimeString>), String> {
        let capture = self.capture_inference(signal).await?;
        let captured_bundle = capture.bundle.clone();
        let config = captured_bundle
            .select(self.pending_model().as_ref())?
            .config;
        let full_snapshot = self.history_snapshot();
        let snapshot = self.model_history();
        let before_tokens = estimate_context_tokens(&snapshot);
        let before_messages = snapshot.len();
        let unchanged = |reason, diagnostics| {
            (
                CompactOutcome {
                    compacted: false,
                    freed_tokens: 0,
                    reason: Some(reason),
                    diagnostics,
                    before_tokens,
                    after_tokens: before_tokens,
                    before_messages,
                    after_messages: before_messages,
                    boundary: None,
                },
                None,
            )
        };
        if config.compaction_disabled {
            return Ok(unchanged(
                crate::context::compact::CompactionNoopReason::CompactorDisabled,
                Vec::new(),
            ));
        }
        let selected = if let Some((id, direction)) = selection {
            let mut matching = snapshot
                .iter()
                .enumerate()
                .filter(|(_, message)| super::message_uuid(message) == &id);
            let index = matching
                .next()
                .map(|(index, _)| index)
                .ok_or_else(|| "selected compaction message is no longer in context".to_owned())?;
            if matching.next().is_some() {
                return Err("selected compaction message identity is ambiguous".to_owned());
            }
            Some((index, direction))
        } else {
            None
        };
        let mut instructions = instructions
            .map(|text| runtime_trim(&text))
            .filter(|text| !text.is_empty());
        if let Some(bus) = self
            .current_hook_bus()
            .filter(|bus| bus.has(HookEvent::PreCompact))
        {
            let outcome = bus
                .dispatch(
                    HookEvent::PreCompact,
                    &HookInput {
                        trigger: Some(HookTrigger::Manual),
                        ..base_hook_input(HookEvent::PreCompact, &self.cwd_now(), &self.id)
                    },
                )
                .await;
            if outcome.aborted || outcome.blocked {
                return Err("pre-compact-hook-rejected".to_owned());
            }
            if !outcome.additional_context.is_empty() {
                instructions = join_opts(
                    instructions,
                    Some(join_runtime_strings(&outcome.additional_context, "\n")),
                );
            }
        }
        let required_request = self.manual_compaction_request(&config, &snapshot).await;
        let bundle = self.auxiliary_bundle_for(captured_bundle)?;
        bundle
            .deps
            .compactor
            .prepare_request(&required_request, signal);
        bundle
            .deps
            .compactor
            .bind_output_observer(self.compaction_output_sink());
        let reduced = match selected {
            Some((index, direction)) => {
                bundle
                    .deps
                    .compactor
                    .partial_compact(&snapshot, index, direction, &config, instructions.as_ref())
                    .await
            }
            None => {
                bundle
                    .deps
                    .compactor
                    .manual_compact(&snapshot, &config, instructions.as_ref())
                    .await
            }
        };
        if !reduced.compacted {
            if reduced.diagnostics.iter().any(|diagnostic| {
                diagnostic.kind == crate::context::compact::CompactionDiagnosticKind::PromptTooLong
            }) {
                if let Some(session) = self.snapshot.resolve_self() {
                    session
                        .context_window()
                        .mark_full(crate::context::compact::policy::usable_context_window(
                            &config,
                        ))
                        .await;
                }
            }
            return Ok(unchanged(
                reduced
                    .reason
                    .unwrap_or(crate::context::compact::CompactionNoopReason::NoProgress),
                reduced.diagnostics,
            ));
        }
        if !reduced.has_new_checkpoint(&snapshot) {
            return Err(
                "The summary operation did not return a new context checkpoint.".to_owned(),
            );
        }
        if signal.is_cancelled() {
            return Err(
                "Context compaction was interrupted before history replacement.".to_owned(),
            );
        }
        let mut replacement = if selected.is_none() {
            crate::context::compact::full_compact::refresh_local_user_history(
                &self.history_snapshot(),
                reduced.messages,
            )
        } else {
            reduced.messages
        };
        replacement = self
            .rebuild_compaction_context(&config, replacement, signal)
            .await;
        if signal.is_cancelled() {
            return Err(
                "Context compaction was interrupted before history replacement.".to_owned(),
            );
        }
        replacement.retain(|message| {
            !crate::context::instruction_policy::is_workspace_instruction_projection(message)
        });
        invalidate_context_usage(&mut replacement);
        let accepted_summary = compaction_summary_of(&replacement);
        let source_ids = full_snapshot
            .iter()
            .map(|message| super::message_uuid(message).clone())
            .collect::<Vec<_>>();
        replacement = self.conversation.apply_compaction(replacement, &source_ids);
        let after_tokens = estimate_context_tokens(&replacement);
        let after_messages = replacement.len();
        let boundary = replacement.iter().rev().find_map(|message| match message {
            Message::CompactBoundary(boundary) => Some(boundary.clone()),
            _ => None,
        });
        self.file_state.clear();
        self.refresh_workspace_instruction_cache();
        self.memory_roots
            .nested_loaded_paths()
            .lock_recover()
            .clear();
        self.memory_roots.nested_triggers().lock_recover().clear();
        self.turn
            .record_padded_countdown_retirement(before_tokens.saturating_sub(after_tokens));
        if let Some(observer) = &self.observer {
            observer.lock_recover().record_manual_compaction();
        }
        self.persist_review_compaction(CompactionRecord {
            summary: accepted_summary.clone(),
            replaced_range: (0, full_snapshot.len() as u64),
            source_message_ids: Some(source_ids),
            replacement_history: Some(replacement),
            approval_parent_history: None,
        })
        .await;
        self.persist_context_usage_checkpoint().await;
        self.reset_context_window(after_tokens.saturating_add(
            crate::context::tokens::estimate_prefix_tokens(
                &required_request.system,
                &required_request.tools,
            ),
        ))
        .await;
        Ok((
            CompactOutcome {
                compacted: true,
                freed_tokens: reduced.freed_tokens,
                reason: None,
                diagnostics: reduced.diagnostics,
                before_tokens,
                after_tokens,
                before_messages,
                after_messages,
                boundary,
            },
            Some(accepted_summary),
        ))
    }

    // ---- checkpoint (filesystem half of rewind) ----

    pub(crate) fn checkpoint_svc(&self) -> Option<Arc<CheckpointService>> {
        if !self.config.checkpointing {
            return None;
        }
        let mut slot = self.persistence.checkpoint_svc.lock_recover();
        if slot.is_none() {
            *slot = Some(Arc::new(CheckpointService::new(
                self.process_context.clone(),
                CheckpointServiceOptions {
                    project_root: self.cwd_now(),
                    base_dir: None,
                },
            )));
        }
        slot.clone()
    }

    /// Snapshot the working tree; returns the commit hash to restore to.
    pub async fn snapshot(&self, label: Option<String>) -> Option<String> {
        let svc = self.checkpoint_svc()?;
        svc.snapshot(&label.unwrap_or_else(|| "snapshot".to_string()))
            .await
            .ok()
            .flatten()
    }

    /// Persist a per-turn checkpoint (rewind file-ledger). No-op when off.
    pub async fn record_checkpoint(&self, hash: Option<String>) {
        if !self.config.checkpointing {
            return;
        }
        if let Some(r) = self.persistence.rollout() {
            r.append_checkpoint(CheckpointRecord { hash }).await;
        }
    }

    /// The resume-reconstructed rewind checkpoint ledger.
    pub fn resumed_checkpoints(&self) -> &[Option<String>] {
        self.conversation.resumed_checkpoints()
    }

    /// Restore the working tree to a prior snapshot (DESTRUCTIVE). Throws when off.
    pub async fn restore_checkpoint(&self, hash: &str) -> Result<(), String> {
        let svc = self.checkpoint_svc().ok_or_else(|| {
            "checkpointing is disabled (config.checkpointing = false)".to_string()
        })?;
        svc.restore(hash).await.map_err(|e| e.to_string())?;
        self.file_state.clear();
        Ok(())
    }

    /// Which files a [`Self::restore_checkpoint`] of `hash` would touch. Read-only:
    /// nothing is restored, so the file-state baselines are deliberately kept.
    pub async fn checkpoint_changed_files(&self, hash: &str) -> Result<Vec<String>, String> {
        let svc = self.checkpoint_svc().ok_or_else(|| {
            "checkpointing is disabled (config.checkpointing = false)".to_string()
        })?;
        svc.changed_files(hash).await.map_err(|e| e.to_string())
    }

    /// Resolve the captured interactive login-shell env (single-flight + cached).
    pub(crate) async fn resolve_shell_env(&self) -> Option<RuntimeEnv> {
        let cwd = self.cwd_now();
        let process_context = self.process_context.clone();
        self.shell_env
            .get_or_init(|| async move {
                capture_login_shell_env(
                    &process_context,
                    &CaptureShellEnvOptions {
                        shell: None,
                        cwd,
                        timeout_ms: None,
                    },
                )
                .await
            })
            .await
            .clone()
    }

    // ---- steering / control ----

    pub async fn wait_for_active_turns(&self) {
        while !self.turn.is_stack_empty() {
            tokio::task::yield_now().await;
        }
    }

    /// Interrupt the current turn because the USER asked for it.
    ///
    /// Same cancellation as [`Self::interrupt`], plus the marker that lets the
    /// turn's terminal record persist model-visible guidance about the abort.
    /// Shutdown and the poisoned-queue path keep calling [`Self::interrupt`]
    /// directly: those stop a turn nobody asked to stop, so claiming "the user
    /// interrupted on purpose" would be false.
    pub fn interrupt_by_user(&self) {
        if let Some(goal) = self.goal_runtime() {
            goal.suppress_continuation();
        }
        self.interrupt_by_user_scoped();
    }

    /// Cancel the captured user turn after its caller has resolved goal authority.
    pub fn interrupt_by_user_scoped(&self) {
        self.turn.mark_user_interrupt();
        self.interrupt();
    }

    /// Interrupt the current turn (abort).
    pub fn interrupt(&self) {
        if let Some(signal) = self.turn.current_abort_snapshot() {
            signal.cancel();
        }
    }

    pub(crate) fn close_turn_admission_and_interrupt(&self) {
        let mut inference_admission = self.inference_admission.lock_recover();
        inference_admission.closed = true;
        self.inference_changed.notify_waiters();
        let signals = {
            let turns = self.turn.active_stack.lock_recover();
            self.lifecycle.mark_disposed();
            turns
                .iter()
                .map(|turn| turn.signal.clone())
                .collect::<Vec<_>>()
        };
        drop(inference_admission);
        for signal in signals {
            signal.cancel();
        }
        self.cancel_ordinary_runtimes_for_shutdown();
        self.cancel_transport_prewarm();
        self.mcp_elicitation_router.cancel_all();
    }

    /// Enqueue a mid-turn steering message.
    // Keep InputValidationError's public field layout source-compatible for callers
    // that construct and destructure it by value.
    #[allow(clippy::result_large_err)]
    pub fn enqueue(&self, input: Input, opts: EnqueueOptions) -> Result<(), InputValidationError> {
        if self.lifecycle.is_disposed() {
            return Err(InputValidationError::new(
                InputValidationErrorCode::QueueUnavailable,
                "steering queue is unavailable because the session is shutting down",
            ));
        }
        if opts.queue_message_id.as_ref().is_some_and(|id| {
            id.try_to_utf8().ok().is_none_or(|id| {
                !uuid::Uuid::parse_str(&id).is_ok_and(|uuid| uuid.to_string() == id)
            })
        }) {
            return Err(InputValidationError::new(
                InputValidationErrorCode::UnsupportedInput,
                "queueMessageId must be a canonical UUID",
            ));
        }
        let validated = self.validate_input_for_model(input, &self.steering_model())?;
        // Preserve a string value as-is (so a leading '/' is still detectable as a
        // slash command); structured input becomes content blocks.
        let value = match &validated.input {
            Input::Text(s) => QueuedValue::Text(s.clone()),
            _ => QueuedValue::Blocks(to_content_blocks(&validated.input)),
        };
        let queued = QueuedMessage {
            trigger_turn: None,
            value,
            mode: opts.mode.unwrap_or_else(|| "prompt".into()),
            uuid: Some(
                opts.queue_message_id
                    .unwrap_or_else(|| (self.deps.uuid)().into()),
            ),
            session_id: Some(self.id.clone()),
            priority: opts.priority,
            is_meta: opts.is_meta,
            origin: opts.origin,
            agent_id: opts.agent_id,
            correlation_id: opts.correlation_id,
            delivery: opts.delivery,
            display_metadata: opts.display_metadata,
            pdf_bytes: validated.pdf_bytes,
            pdf_documents: validated.pdf_documents,
        };
        let result = self.conversation.queue.enqueue(queued.clone());
        if result.is_ok()
            && queued.is_meta != Some(true)
            && queued.origin.is_none()
            && queued.agent_id.is_none()
        {
            let content = match queued.value {
                QueuedValue::Text(text) => vec![crate::schemas::content::ContentBlock::Text(
                    crate::schemas::content::TextBlock {
                        text,
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    },
                )],
                QueuedValue::Blocks(blocks) => blocks,
            };
            let message =
                crate::schemas::message::Message::User(crate::schemas::message::UserMessage {
                    question_answer: None,
                    uuid: queued.uuid.unwrap_or_default(),
                    content,
                    is_meta: None,
                });
            let turn_id = self
                .turn
                .active_stack
                .lock_recover()
                .last()
                .and_then(|turn| turn.turn_id.clone())
                .unwrap_or_default();
            if let Some(record) = self.capture_review_message_record(&message, &turn_id) {
                if let Err(error) = self.persistence.retain_admission_records(vec![record]) {
                    return Err(InputValidationError::new(
                        InputValidationErrorCode::QueueUnavailable,
                        format!("steering evidence persistence failed: {error}"),
                    ));
                }
            }
        }
        result.map_err(|error| {
            InputValidationError::new(
                InputValidationErrorCode::QueueUnavailable,
                format!(
                    "steering queue is unavailable: {}",
                    runtime_error_message_to_public_host(&error)
                ),
            )
        })
    }

    /// A snapshot of the steering queue.
    pub fn queued_messages(&self) -> Vec<Arc<QueuedMessage>> {
        self.conversation.queue().snapshot()
    }

    /// Authoritative revisioned queue state for reconnecting Hosts.
    pub fn queue_snapshot(&self) -> QueueSnapshot {
        self.conversation.queue().queue_snapshot()
    }

    /// Wait until every queue checkpoint submitted before this call has a
    /// durability verdict. Protocol Hosts use this as the InputApplied barrier.
    pub async fn await_queue_durability(&self) -> Result<(), RuntimeString> {
        self.conversation.queue().await_pending_durability().await
    }

    /// Core-resolved TodoV2 identity. The Host never supplies or guesses a
    /// filesystem path: environment override wins, then live team, then this
    /// session's stable id.
    pub(crate) fn task_list_id_under_identity_gate(&self) -> RuntimeString {
        let team = self
            .swarm_identity
            .lock_recover()
            .as_ref()
            .map(|identity| identity.team.clone());
        get_task_list_id(Some(&self.id), team.as_ref())
    }

    pub(crate) fn task_list_identity_snapshot(&self) -> (RuntimeString, u64) {
        let _gate = self.task_list_identity_gate.lock_recover();
        (
            self.task_list_id_under_identity_gate(),
            self.task_list_identity_generation.load(Ordering::SeqCst),
        )
    }

    pub fn task_list_id(&self) -> RuntimeString {
        self.task_list_identity_snapshot().0
    }

    /// Recheck a concrete task without making current-plan membership its identity.
    pub async fn task_identity(
        &self,
        task_id: &RuntimeString,
    ) -> std::io::Result<Option<crate::tasks::task_store::TaskIdentity>> {
        loop {
            let (list_id, generation) = self.task_list_identity_snapshot();
            let binding = crate::tasks::task_store::TaskPlanBinding::capture(&list_id);
            let result = crate::tasks::task_store::get_task_identity_bound(
                &list_id,
                task_id,
                Some(&binding),
            )
            .await;
            let _gate = self.task_list_identity_gate.lock_recover();
            if self.task_list_identity_generation.load(Ordering::SeqCst) == generation
                && self.task_list_id_under_identity_gate() == list_id
            {
                return result;
            }
        }
    }

    /// Authoritative persisted TodoV2 snapshot with revision-aware pagination.
    pub async fn get_task_list(
        &self,
        options: TaskListQueryOptions,
    ) -> std::io::Result<TaskListSnapshot> {
        self.get_task_list_with_publication_cutoff(options)
            .await
            .map(|result| result.snapshot)
    }

    /// Protocol-only query form retaining the process-local publication fence.
    /// The cutoff is never serialized or exposed as part of TodoV2's Host wire.
    #[doc(hidden)]
    pub async fn get_task_list_with_publication_cutoff(
        &self,
        options: TaskListQueryOptions,
    ) -> std::io::Result<TaskListQueryResult> {
        loop {
            let (list_id, generation) = self.task_list_identity_snapshot();
            let result = query_task_list_ordered(&self.id, &list_id, options.clone()).await;
            let _gate = self.task_list_identity_gate.lock_recover();
            if self.task_list_identity_generation.load(Ordering::SeqCst) == generation
                && self.task_list_id_under_identity_gate() == list_id
            {
                return result;
            }
            // TeamCreate raced the disk query. Never return an authoritative
            // snapshot for an identity the Session no longer owns.
        }
    }

    /// Subscribe to ordered changes for whichever list identity this session
    /// currently resolves. The live identity check also follows team changes.
    pub fn subscribe_task_list_changes(
        self: &Arc<Self>,
        subscriber: TaskListChangeSubscriber,
    ) -> Box<dyn Fn() + Send + Sync> {
        self.subscribe_task_list_change_envelopes(Arc::new(move |envelope| {
            subscriber(envelope.change)
        }))
    }

    /// Protocol-only subscription retaining the process-local publication
    /// sequence used to reject callbacks superseded by a correlated query.
    #[doc(hidden)]
    pub fn subscribe_task_list_change_envelopes(
        self: &Arc<Self>,
        subscriber: TaskListChangeEnvelopeSubscriber,
    ) -> Box<dyn Fn() + Send + Sync> {
        let mut registry = self.task_list_subscriptions.lock_recover();
        if registry.closing || self.lifecycle.is_disposed() {
            return Box::new(|| {});
        }
        if registry.retiring_tx.is_none() {
            let (send, receive) = tokio::sync::mpsc::unbounded_channel();
            let reaper = match tokio::runtime::Handle::try_current() {
                Ok(runtime) => TaskListSubscriptionReaper::Tokio(
                    runtime.spawn(reap_task_list_subscriptions(receive)),
                ),
                Err(_) => {
                    let (completion_send, completion) = tokio::sync::watch::channel(None);
                    let handle = std::thread::spawn(move || {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("TodoV2 subscription reaper runtime construction failed");
                        let result = runtime.block_on(reap_task_list_subscriptions(receive));
                        let _ = completion_send.send(Some(result));
                    });
                    TaskListSubscriptionReaper::Thread { handle, completion }
                }
            };
            registry.retiring_tx = Some(send);
            registry.reaper = Some(reaper);
        }
        let source_session = Arc::downgrade(self);
        let list_id_source = Arc::new(move || {
            source_session
                .upgrade()
                .filter(|session| !session.lifecycle.is_disposed())
                .map(|session| {
                    let (list_id, generation) = session.task_list_identity_snapshot();
                    TaskListBinding {
                        session_id: session.id.clone(),
                        list_id,
                        generation,
                    }
                })
        });
        let callback_session = Arc::downgrade(self);
        let subscription = subscribe_task_states(
            list_id_source,
            Arc::new(move |state, generation, publication_sequence| {
                let Some(session) = callback_session.upgrade() else {
                    return;
                };
                {
                    let _identity = session.task_list_identity_gate.lock_recover();
                    if session.task_list_identity_generation.load(Ordering::SeqCst) != generation
                        || session.task_list_id_under_identity_gate() != state.task_list_id
                    {
                        return;
                    }
                }
                if session.lifecycle.is_disposed()
                    || session.task_list_subscriptions.lock_recover().closing
                {
                    return;
                }
                subscriber(TaskListChangeEnvelope {
                    change: TaskListChange {
                        plan_id: state.plan_id.clone(),
                        plan_kind: state.plan_kind.clone(),
                        session_id: session.id.clone(),
                        task_list_id: state.task_list_id,
                        generation: state.storage_generation,
                        revision: state.revision,
                        fingerprint: state.fingerprint,
                        tasks: state.tasks,
                    },
                    publication_sequence,
                });
            }),
        );
        let id = registry.next_id;
        registry.next_id = registry
            .next_id
            .checked_add(1)
            .expect("TodoV2 Session subscription id overflow");
        registry.active.insert(id, subscription.clone());
        drop(registry);
        let weak = Arc::downgrade(self);
        Box::new(move || {
            subscription.close();
            if let Some(session) = weak.upgrade() {
                session.retire_task_list_subscription(id);
            }
        })
    }

    pub(crate) fn retire_task_list_subscription(&self, id: u64) {
        let (subscription, retiring) = {
            let mut registry = self.task_list_subscriptions.lock_recover();
            (registry.active.remove(&id), registry.retiring_tx.clone())
        };
        if let Some(subscription) = subscription {
            subscription.close();
            if let Some(retiring) = retiring {
                let _ = retiring.send(subscription);
            }
        }
    }

    pub(crate) fn notify_task_list_identity_changed(&self) {
        let generation = self.task_list_identity_generation.load(Ordering::SeqCst);
        let subscriptions = self
            .task_list_subscriptions
            .lock_recover()
            .active
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for subscription in subscriptions {
            subscription.identity_changed(generation);
        }
    }

    pub(crate) fn close_task_list_subscriptions(&self) {
        let (subscriptions, retiring) = {
            let mut registry = self.task_list_subscriptions.lock_recover();
            registry.closing = true;
            (
                std::mem::take(&mut registry.active)
                    .into_values()
                    .collect::<Vec<_>>(),
                registry.retiring_tx.clone(),
            )
        };
        for subscription in subscriptions {
            subscription.close();
            if let Some(retiring) = &retiring {
                let _ = retiring.send(subscription);
            }
        }
    }

    pub(crate) async fn drain_task_list_subscriptions(&self) -> Result<(), String> {
        self.close_task_list_subscriptions();
        let reaper = {
            let mut registry = self.task_list_subscriptions.lock_recover();
            registry.retiring_tx.take();
            registry.reaper.take()
        };
        let result = match reaper {
            Some(TaskListSubscriptionReaper::Tokio(reaper)) => reaper
                .await
                .map_err(|error| format!("TodoV2 watcher reaper failed: {error}"))?,
            Some(TaskListSubscriptionReaper::Thread {
                handle,
                mut completion,
            }) => {
                let result = loop {
                    if let Some(result) = completion.borrow().clone() {
                        break result;
                    }
                    if completion.changed().await.is_err() {
                        break Err(
                            "TodoV2 watcher reaper ended without a completion verdict".to_string()
                        );
                    }
                };
                // The verdict is sent at the end of the OS-thread entrypoint,
                // so this join is immediate. If an outer cleanup deadline
                // drops us before the verdict, dropping the ordinary handle
                // detaches it instead of parking Tokio's blocking pool.
                handle
                    .join()
                    .map_err(|_| "TodoV2 watcher reaper thread panicked".to_string())?;
                result
            }
            None => Ok(()),
        };
        result.map_err(|error| format!("TodoV2 watcher shutdown failed: {error}"))
    }

    pub fn subscribe_queue_changes(
        &self,
        subscriber: QueueStateSubscriber,
    ) -> Box<dyn Fn() + Send + Sync> {
        self.conversation.queue().subscribe_state(subscriber)
    }

    /// Subscribe before starting snapshot-bearing work to receive the durable
    /// `requested` transition before any in-process Host callback is awaited.
    /// Reconnecting Hosts should pair this with
    /// [`Self::agent_memory_snapshot_decisions`] to close the subscribe/query
    /// race without guessing request identity.
    pub fn subscribe_agent_memory_snapshot_decisions(
        &self,
    ) -> tokio::sync::broadcast::Receiver<AgentMemorySnapshotDecisionAudit> {
        self.snapshot.events.subscribe()
    }

    /// Read one authoritative snapshot-decision state. The event gate prevents
    /// observing a registry transition before its matching rollout append.
    pub async fn agent_memory_snapshot_decision(
        &self,
        request_id: &RuntimeString,
    ) -> Result<Option<AgentMemorySnapshotDecisionAudit>, AgentMemorySnapshotApplyError> {
        self.await_current_snapshot_owner(request_id).await?;
        let _gate = self.snapshot.persistence.event_gate.lock().await;
        if let Some(error) = self.snapshot_fatal_apply_error() {
            return Err(error);
        }
        Ok(self.snapshot.decisions.query(request_id))
    }

    /// Read every authoritative snapshot-decision state in deterministic order.
    pub async fn agent_memory_snapshot_decisions(
        &self,
    ) -> Result<Vec<AgentMemorySnapshotDecisionAudit>, AgentMemorySnapshotApplyError> {
        self.await_all_snapshot_owners().await?;
        let _gate = self.snapshot.persistence.event_gate.lock().await;
        if let Some(error) = self.snapshot_fatal_apply_error() {
            return Err(error);
        }
        Ok(self.snapshot.decisions.all())
    }

    /// Record one Host choice through the request-id CAS. Keep/Replace apply
    /// immediately; Merge remains `DecisionRecorded` until a provider accepts
    /// the correlated prompt. Repeating the same choice is idempotent and a
    /// different choice is a stable
    /// [`AgentMemorySnapshotRegistryError::DecisionConflict`].
    pub async fn resolve_agent_memory_snapshot_decision(
        &self,
        request_id: &RuntimeString,
        decision: AgentMemoryMergeChoice,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError> {
        self.resolve_agent_memory_snapshot_decision_with_source(
            request_id,
            decision,
            AgentMemorySnapshotDecisionSource::Host,
        )
        .await
        .map(|(audit, _)| audit)
    }

    /// Resolve a Host decision and invoke a trusted nonblocking adapter enqueue
    /// under the same order gate as the DecisionRecorded/Applied events. The
    /// callback must only admit into its FIFO and must not block or re-enter the
    /// Session. In particular a Merge reply carrying `DecisionRecorded` is
    /// admitted before reconciliation can cross the provider boundary and
    /// publish `Applied`.
    pub async fn resolve_agent_memory_snapshot_decision_ordered<R>(
        &self,
        request_id: &RuntimeString,
        decision: AgentMemoryMergeChoice,
        enqueue: impl FnOnce(AgentMemorySnapshotDecisionAudit) -> R + Send + 'static,
    ) -> Result<R, AgentMemorySnapshotApplyError>
    where
        R: Send + 'static,
    {
        let (reply_send, reply_receive) = tokio::sync::oneshot::channel();
        let ordered_reply: SnapshotOrderedReply = Box::new(move |audit| {
            let reply = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| enqueue(audit)))
                .map_err(|_| ());
            let _ = reply_send.send(reply);
        });
        match self
            .run_snapshot_durability_operation(
                SnapshotDurabilityOperation::Resolve {
                    request_id: request_id.clone(),
                    decision,
                    source: AgentMemorySnapshotDecisionSource::Host,
                    ordered_reply: Some(ordered_reply),
                },
                false,
            )
            .await?
        {
            SnapshotDurabilityResult::Resolve(result) => {
                let _ = result?;
            }
            _ => unreachable!("ordered snapshot Resolve owner returned the wrong result variant"),
        }
        match reply_receive.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(())) => Err(AgentMemorySnapshotRegistryError::InvalidRequest(
                "ordered snapshot decision reply callback panicked",
            )
            .into()),
            Err(_) => Err(AgentMemorySnapshotRegistryError::InvalidRequest(
                "ordered snapshot decision owner completed without admitting its reply",
            )
            .into()),
        }
    }

    /// Settle a Host-unavailable edge and admit its correlated reply at the
    /// exact durable transition boundary. This is required for a Merge policy:
    /// the DecisionRecorded reply must enter the protocol FIFO before the
    /// reconciliation receiver can cross provider admission and publish
    /// Applied.
    pub(crate) async fn mark_agent_memory_snapshot_unavailable_ordered<R>(
        &self,
        request_id: &RuntimeString,
        status: AgentMemorySnapshotDecisionStatus,
        error: Option<RuntimeString>,
        enqueue: impl FnOnce(AgentMemorySnapshotDecisionAudit) -> R + Send + 'static,
    ) -> Result<R, AgentMemorySnapshotApplyError>
    where
        R: Send + 'static,
    {
        debug_assert!(matches!(
            status,
            AgentMemorySnapshotDecisionStatus::TimedOut
                | AgentMemorySnapshotDecisionStatus::Disconnected
                | AgentMemorySnapshotDecisionStatus::NoHandler
        ));
        let (reply_send, reply_receive) = tokio::sync::oneshot::channel();
        let ordered_reply: SnapshotOrderedReply = Box::new(move |audit| {
            let reply = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| enqueue(audit)))
                .map_err(|_| ());
            let _ = reply_send.send(reply);
        });
        match self
            .run_snapshot_durability_operation(
                SnapshotDurabilityOperation::SettleUnavailable {
                    request_id: request_id.clone(),
                    status,
                    error,
                    without_consumer: false,
                    ordered_reply: Some(ordered_reply),
                },
                false,
            )
            .await?
        {
            SnapshotDurabilityResult::SettleUnavailable(result) => {
                let _ = result?;
            }
            _ => unreachable!(
                "ordered snapshot SettleUnavailable owner returned the wrong result variant"
            ),
        }
        match reply_receive.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(())) => Err(AgentMemorySnapshotRegistryError::InvalidRequest(
                "ordered snapshot unavailable reply callback panicked",
            )
            .into()),
            Err(_) => Err(AgentMemorySnapshotRegistryError::InvalidRequest(
                "ordered snapshot unavailable owner completed without admitting its reply",
            )
            .into()),
        }
    }

    pub async fn mark_agent_memory_snapshot_timed_out_ordered<R>(
        &self,
        request_id: &RuntimeString,
        error: Option<RuntimeString>,
        enqueue: impl FnOnce(AgentMemorySnapshotDecisionAudit) -> R + Send + 'static,
    ) -> Result<R, AgentMemorySnapshotApplyError>
    where
        R: Send + 'static,
    {
        self.mark_agent_memory_snapshot_unavailable_ordered(
            request_id,
            AgentMemorySnapshotDecisionStatus::TimedOut,
            error,
            enqueue,
        )
        .await
    }

    pub async fn mark_agent_memory_snapshot_host_disconnected_ordered<R>(
        &self,
        request_id: &RuntimeString,
        error: Option<RuntimeString>,
        enqueue: impl FnOnce(AgentMemorySnapshotDecisionAudit) -> R + Send + 'static,
    ) -> Result<R, AgentMemorySnapshotApplyError>
    where
        R: Send + 'static,
    {
        self.mark_agent_memory_snapshot_unavailable_ordered(
            request_id,
            AgentMemorySnapshotDecisionStatus::Disconnected,
            error,
            enqueue,
        )
        .await
    }

    pub async fn mark_agent_memory_snapshot_no_handler_ordered<R>(
        &self,
        request_id: &RuntimeString,
        error: Option<RuntimeString>,
        enqueue: impl FnOnce(AgentMemorySnapshotDecisionAudit) -> R + Send + 'static,
    ) -> Result<R, AgentMemorySnapshotApplyError>
    where
        R: Send + 'static,
    {
        self.mark_agent_memory_snapshot_unavailable_ordered(
            request_id,
            AgentMemorySnapshotDecisionStatus::NoHandler,
            error,
            enqueue,
        )
        .await
    }

    /// Settle a still-pending request at the Host timeout boundary. The
    /// configured timeout policy is authoritative; `require-host` records an
    /// explicit `timed-out` terminal state without writing a sync marker.
    pub async fn mark_agent_memory_snapshot_timed_out(
        &self,
        request_id: &RuntimeString,
        error: Option<RuntimeString>,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError> {
        self.settle_unavailable_agent_memory_snapshot_request(
            request_id,
            AgentMemorySnapshotDecisionStatus::TimedOut,
            error,
        )
        .await
        .map(|(audit, _)| audit)
    }

    /// Settle a still-pending request when its Host connection disappears.
    pub async fn mark_agent_memory_snapshot_host_disconnected(
        &self,
        request_id: &RuntimeString,
        error: Option<RuntimeString>,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError> {
        self.settle_unavailable_agent_memory_snapshot_request(
            request_id,
            AgentMemorySnapshotDecisionStatus::Disconnected,
            error,
        )
        .await
        .map(|(audit, _)| audit)
    }

    /// Settle a still-pending request after all Host decision paths are known
    /// to be absent.
    pub async fn mark_agent_memory_snapshot_no_handler(
        &self,
        request_id: &RuntimeString,
        error: Option<RuntimeString>,
    ) -> Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError> {
        self.settle_unavailable_agent_memory_snapshot_request(
            request_id,
            AgentMemorySnapshotDecisionStatus::NoHandler,
            error,
        )
        .await
        .map(|(audit, _)| audit)
    }

    pub(crate) async fn run_retained_queue_mutation<F>(
        self: &Arc<Self>,
        mutation_id: RuntimeString,
        operation: F,
    ) -> QueueMutationResult
    where
        F: FnOnce(Arc<MessageQueue>) -> BoxFuture<'static, QueueMutationResult> + Send + 'static,
    {
        let queue = self.conversation.queue().clone();
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        {
            // Admission and handle publication share the exact registry lock
            // shutdown closes before taking its join snapshot. The waiter may be
            // cancelled, but the durability owner cannot escape Session teardown.
            let mut registry = self.background_tasks.lock_recover();
            if registry.closing || self.lifecycle.is_disposed() {
                if let Some(result) = queue.mutation_result(&mutation_id) {
                    return result;
                }
                return queue.mutation_not_admitted(
                    mutation_id,
                    "queue mutation was not admitted because the session is shutting down".into(),
                );
            }
            let worker_queue = queue.clone();
            let worker_mutation_id = mutation_id.clone();
            let handle = tokio::spawn(async move {
                let future = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    operation(worker_queue.clone())
                }));
                let result = match future {
                    Ok(future) => match std::panic::AssertUnwindSafe(future).catch_unwind().await {
                        Ok(result) => result,
                        Err(_) => worker_queue.retained_mutation_owner_failed(
                            &worker_mutation_id,
                            "queue mutation durability owner panicked".into(),
                        ),
                    },
                    Err(_) => worker_queue.retained_mutation_owner_failed(
                        &worker_mutation_id,
                        "queue mutation durability owner panicked before execution".into(),
                    ),
                };
                let _ = completion_tx.send(result);
            });
            registry.handles.push(handle);
        }

        let result = completion_rx.await.unwrap_or_else(|_| {
            queue.retained_mutation_owner_failed(
                &mutation_id,
                "queue mutation durability owner ended without a terminal result".into(),
            )
        });
        if matches!(
            result.status,
            QueueMutationStatus::OutcomeUnknown | QueueMutationStatus::PersistenceFailed
        ) {
            // A turn must not continue against a queue whose visible revision
            // can no longer be proven durable. MessageQueue is already poisoned
            // at this point; cancellation closes the provider/tool side too.
            self.interrupt();
        }
        result
    }

    pub async fn edit_queued_message(
        self: &Arc<Self>,
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        expected_revision: u64,
        edit: QueueEdit,
    ) -> QueueMutationResult {
        let result_id = mutation_id.clone();
        self.run_retained_queue_mutation(result_id, move |queue| {
            Box::pin(async move {
                queue
                    .edit_queued(mutation_id, uuid, expected_revision, edit)
                    .await
            })
        })
        .await
    }

    pub async fn remove_queued_message(
        self: &Arc<Self>,
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        expected_revision: u64,
    ) -> QueueMutationResult {
        let result_id = mutation_id.clone();
        self.run_retained_queue_mutation(result_id, move |queue| {
            Box::pin(async move {
                queue
                    .remove_queued(mutation_id, uuid, expected_revision)
                    .await
            })
        })
        .await
    }

    pub async fn reorder_queued_message(
        self: &Arc<Self>,
        mutation_id: RuntimeString,
        uuid: RuntimeString,
        before_uuid: Option<RuntimeString>,
        expected_revision: u64,
    ) -> QueueMutationResult {
        let result_id = mutation_id.clone();
        self.run_retained_queue_mutation(result_id, move |queue| {
            Box::pin(async move {
                queue
                    .reorder_queued(mutation_id, uuid, before_uuid, expected_revision)
                    .await
            })
        })
        .await
    }

    /// Remove + return main-thread steering messages still queued after a turn.
    pub async fn take_pending_main_messages(
        &self,
    ) -> Result<Vec<Arc<QueuedMessage>>, RuntimeString> {
        self.conversation
            .queue
            .dequeue_all_matching_with_state_durable(
                &|cmd| cmd.agent_id.is_none(),
                QueueTerminalState::Taken,
            )
            .await
    }

    /// Read-only snapshot of the background sub-agents spawned this session.
    pub fn background_agent_summaries(&self) -> Vec<BackgroundAgentInfo> {
        let m = self.subagents.background_agents.lock_recover();
        m.iter()
            .map(|(id, e)| BackgroundAgentInfo {
                id: id.clone(),
                description: runtime_prompt_to_background_host(&e.description),
                status: e.status,
                teammate: e.teammate,
                idle: e.idle,
                transcript: runtime_transcript_to_background_host(&e.partial),
                output_file: e.output_file.clone(),
            })
            .collect()
    }

    /// Read-only snapshot of sub-agent lifecycle records observed by this session.
    pub fn subagent_lifecycle_snapshot(&self) -> SubagentSnapshot {
        self.subagents.lifecycle.lock_recover().snapshot()
    }

    /// Resolve event ownership without cloning the complete lifecycle snapshot.
    pub fn subagent_context(&self, agent_id: &str) -> Option<SubagentContext> {
        self.subagents
            .lifecycle
            .lock_recover()
            .get(agent_id)
            .map(|record| record.context.clone())
    }

    pub(crate) fn start_pending_memory_maintenance_recovery_owners(self: &Arc<Self>) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let mut registry = self.maintenance_tasks.lock_recover();
        if registry.closing || registry.durability_closed {
            return;
        }
        let pending = std::mem::take(&mut registry.pending_recovery);
        for event in pending {
            let key = MaintenanceRunKey::from_event(&event);
            let (recovery_send, recovery_receive) = tokio::sync::watch::channel(None);
            registry
                .recovery_receipts
                .insert(key.clone(), recovery_receive);
            let (result_send, _result_receive) = tokio::sync::oneshot::channel();
            if let Err(error) = self.register_memory_maintenance_durability_owner_locked(
                &mut registry,
                event,
                None,
                true,
                result_send,
                Some(recovery_send),
            ) {
                let message = error.message.clone();
                let recovery_error = RuntimeString::from(message.clone());
                registry.failures.push(error);
                let (_failure_send, failure_receive) =
                    tokio::sync::watch::channel(Some(Err(recovery_error)));
                registry
                    .recovery_receipts
                    .insert(key.clone(), failure_receive);
                self.install_memory_maintenance_query_fatal(
                    Some(&key),
                    RuntimeString::from(format!(
                        "memory maintenance recovery durability admission failed: {message}"
                    )),
                );
            }
        }
    }

    /// Await only the durability owners created for constructor-synthesized
    /// Auto Memory/Auto Dream recovery terminals. Every captured owner is
    /// allowed to settle before failures are aggregated; live runs registered
    /// after construction are outside this resume barrier by design.
    pub(crate) async fn await_pending_memory_maintenance_recovery_owners(
        self: &Arc<Self>,
    ) -> Result<(), RuntimeString> {
        self.start_pending_memory_maintenance_recovery_owners();
        let (receipts, unowned_recovery) = {
            let registry = self.maintenance_tasks.lock().map_err(|_| {
                RuntimeString::from("memory maintenance recovery receipt registry was poisoned")
            })?;
            (
                registry
                    .recovery_receipts
                    .iter()
                    .map(|(key, receipt)| (key.clone(), receipt.clone()))
                    .collect::<Vec<_>>(),
                registry.pending_recovery.len(),
            )
        };

        let mut failures = Vec::new();
        if unowned_recovery > 0 {
            failures.push(format!(
                "{unowned_recovery} constructor-synthesized memory maintenance recovery owner(s) could not be admitted"
            ));
        }
        for (key, mut receipt) in receipts {
            loop {
                let verdict = { receipt.borrow().clone() };
                if let Some(verdict) = verdict {
                    if let Err(error) = verdict {
                        let error = error.try_to_utf8().unwrap_or_else(|_| format!("{error:?}"));
                        failures.push(format!("{key:?}: {error}"));
                    }
                    break;
                }
                if receipt.changed().await.is_err() {
                    failures.push(format!(
                        "{key:?}: Session-owned recovery durability task ended without a verdict"
                    ));
                    break;
                }
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(RuntimeString::from(format!(
                "memory maintenance recovery durability failed: {}",
                failures.join("; ")
            )))
        }
    }

    pub(crate) fn install_memory_maintenance_query_fatal_direct(
        &self,
        key: Option<&MaintenanceRunKey>,
        message: impl Into<RuntimeString>,
    ) -> MemoryMaintenanceQueryError {
        let error = MemoryMaintenanceQueryError::persistence_inconsistent(
            message,
            key.map(MaintenanceRunKey::run_kind),
            key.map(MaintenanceRunKey::run_id),
        );
        self.maintenance.set_query_fatal(error);
        self.maintenance
            .query_fatal
            .get()
            .expect("the first maintenance query failure is retained")
            .clone()
    }

    /// Linearize the sticky failure with ordered event/query delivery. A query
    /// already holding the gate may legally return the preceding snapshot;
    /// every query that enters after this method returns observes the same Err.
    pub(crate) fn install_memory_maintenance_query_fatal(
        &self,
        key: Option<&MaintenanceRunKey>,
        message: impl Into<RuntimeString>,
    ) -> MemoryMaintenanceQueryError {
        let message = message.into();
        match self.maintenance.persistence.event_gate.lock() {
            Ok(_gate) => self.install_memory_maintenance_query_fatal_direct(key, message),
            Err(_) => self.install_memory_maintenance_query_fatal_direct(
                key,
                RuntimeString::from("memory maintenance event gate was poisoned: ")
                    .concat(&message),
            ),
        }
    }

    pub(crate) fn memory_maintenance_query_fatal(&self) -> Option<MemoryMaintenanceQueryError> {
        self.maintenance.query_fatal()
    }

    /// Authoritative reconnect-safe state for automatic extraction/consolidation.
    pub fn memory_maintenance_snapshot(
        self: &Arc<Self>,
    ) -> Result<MemoryMaintenanceSnapshot, MemoryMaintenanceQueryError> {
        self.start_pending_memory_maintenance_recovery_owners();
        let _event_gate = self
            .maintenance
            .persistence
            .event_gate
            .lock()
            .map_err(|_| {
                self.install_memory_maintenance_query_fatal_direct(
                    None,
                    "memory maintenance event gate was poisoned",
                )
            })?;
        if let Some(error) = self.memory_maintenance_query_fatal() {
            return Err(error);
        }
        self.maintenance
            .snapshot
            .lock()
            .map(|snapshot| snapshot.clone())
            .map_err(|_| {
                self.install_memory_maintenance_query_fatal_direct(
                    None,
                    "memory maintenance lifecycle state was poisoned",
                )
            })
    }

    /// Capture an authoritative maintenance snapshot and enqueue its reply at
    /// the same linearization point used by lifecycle events. `enqueue` must be
    /// a trusted, nonblocking FIFO admission callback and must not re-enter this
    /// Session. Protocol uses this to make the only observable orders either
    /// old-query -> started -> terminal or started -> terminal -> new-query.
    pub fn with_ordered_memory_maintenance_snapshot<R>(
        self: &Arc<Self>,
        enqueue: impl FnOnce(MemoryMaintenanceSnapshot) -> R,
    ) -> Result<R, MemoryMaintenanceQueryError> {
        self.start_pending_memory_maintenance_recovery_owners();
        let _event_gate = self
            .maintenance
            .persistence
            .event_gate
            .lock()
            .map_err(|_| {
                self.install_memory_maintenance_query_fatal_direct(
                    None,
                    "memory maintenance event gate was poisoned",
                )
            })?;
        if let Some(error) = self.memory_maintenance_query_fatal() {
            return Err(error);
        }
        let snapshot = self
            .maintenance
            .snapshot
            .lock()
            .map(|snapshot| snapshot.clone())
            .map_err(|_| {
                self.install_memory_maintenance_query_fatal_direct(
                    None,
                    "memory maintenance lifecycle state was poisoned",
                )
            })?;
        Ok(enqueue(snapshot))
    }

    #[cfg(test)]
    pub(crate) fn memory_message_cursor(&self) -> Option<RuntimeString> {
        self.memory_scheduler.last_memory_uuid()
    }

    #[cfg(test)]
    pub(crate) fn set_memory_message_cursor_for_test(&self, cursor: Option<RuntimeString>) {
        self.memory_scheduler.set_last_memory_uuid(cursor);
    }

    /// Replace the live Host event sink. Arbitrary callbacks are isolated on a
    /// serial worker so they cannot block or panic a maintenance task.
    pub fn set_memory_maintenance_event_sink(&self, sink: Option<MemoryMaintenanceEventSink>) {
        *self.maintenance.event_sink.lock_recover() =
            sink.map(MaintenanceEventSinkRegistration::Isolated);
    }

    /// Install the sink that receives MCP connection lifecycle transitions.
    ///
    /// Kept as a post-construction setter rather than a `SessionInit` field for
    /// the same reason the extension-failure sink is: the host wires its
    /// notification bridge after the session exists, and a new init field would
    /// have to be spelled out at every construction site including the tests.
    ///
    /// Transitions that happen before the host installs its sink are dropped on
    /// purpose. They are not lost information: the host reads the runtime
    /// snapshot when it attaches, which is the authoritative current state.
    pub fn set_mcp_status_sink(&self, sink: Option<crate::mcp::types::McpStatusSink>) {
        self.mcp.status_observer.set(sink);
    }

    /// Route hook failures to the host. Kept as a Session method rather than
    /// exposing the hook bus, so a host cannot reach past the session into the
    /// dispatcher; a session with no hooks configured accepts the call and does
    /// nothing, which is the same shape as having no failures.
    pub fn set_extension_failure_sink(&self, sink: Option<ExtensionFailureSink>) {
        *self.extension_failure_sink.lock_recover() = sink.clone();
        if let (Some(bus), Some(sink)) = (self.current_hook_bus(), sink) {
            bus.set_failure_sink(sink);
        }
    }

    /// Install a trusted nonblocking ordered sink. Protocol uses this to enqueue
    /// directly into its own FIFO at the same linearization point as rollout
    /// persistence, preventing a newer query snapshot from overtaking an event.
    pub fn set_ordered_memory_maintenance_event_sink(
        &self,
        sink: Option<MemoryMaintenanceEventSink>,
    ) {
        *self.maintenance.event_sink.lock_recover() =
            sink.map(MaintenanceEventSinkRegistration::Ordered);
    }

    #[cfg(test)]
    pub(crate) fn record_memory_maintenance_event(&self, event: MemoryMaintenanceEvent) {
        let _event_gate = self.maintenance.persistence.event_gate.lock_recover();
        if self.maintenance.snapshot.lock_recover().apply(&event) {
            self.publish_memory_maintenance_event(event);
        }
    }

    #[cfg(test)]
    pub(crate) fn publish_memory_maintenance_event(&self, event: MemoryMaintenanceEvent) {
        // Synchronous projection helpers are restricted to in-memory fixtures.
        // Production persistence always belongs to a registered durability task.
        assert!(
            self.persistence.rollout().is_none(),
            "persistent maintenance events require their retained operation owner"
        );
        self.publish_memory_maintenance_event_visible(event);
    }

    pub(crate) fn publish_memory_maintenance_event_visible(&self, event: MemoryMaintenanceEvent) {
        let sink = self.maintenance.event_sink.lock_recover().clone();
        if let Some(sink) = sink {
            match sink {
                MaintenanceEventSinkRegistration::Ordered(sink) => {
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(event)));
                }
                MaintenanceEventSinkRegistration::Isolated(sink) => {
                    self.enqueue_isolated_memory_maintenance_event(sink, event);
                }
            }
        }
    }

    pub(crate) async fn append_memory_maintenance_event(
        &self,
        event: &MemoryMaintenanceEvent,
    ) -> Result<(), ShutdownPhaseFailure> {
        if self
            .maintenance
            .persistence
            .finalized
            .load(Ordering::SeqCst)
        {
            return Err(ShutdownPhaseFailure::error(
                "memory maintenance persistence is finalized",
            ));
        }
        let payload = runtime_codec::encode_value(event).map_err(|error| {
            ShutdownPhaseFailure::error(format!(
                "failed to encode memory maintenance lifecycle event: {error}"
            ))
        })?;
        // The existing operation owner retains the async serial permit while
        // awaiting wrapped writer capacity; no standard mutex spans this wait.
        let prepared = match self.persistence.rollout() {
            Some(rollout) => match std::panic::AssertUnwindSafe(rollout.prepare_append())
                .catch_unwind()
                .await
            {
                Ok(Ok(prepared)) => Some(prepared),
                Ok(Err(error)) => {
                    return Err(ShutdownPhaseFailure::error(format!(
                        "memory maintenance rollout append admission failed: {error}"
                    )))
                }
                Err(_) => {
                    return Err(ShutdownPhaseFailure::error(
                        "memory maintenance rollout append_event adapter panicked",
                    ))
                }
            },
            None => None,
        };
        let _gate = self
            .maintenance
            .persistence
            .event_gate
            .lock()
            .map_err(|_| {
                ShutdownPhaseFailure::error("memory maintenance event gate was poisoned")
            })?;
        if self
            .maintenance
            .persistence
            .finalized
            .load(Ordering::SeqCst)
        {
            return Err(ShutdownPhaseFailure::error(
                "memory maintenance persistence is finalized",
            ));
        }
        if let Some(prepared) = prepared {
            let appended = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                prepared.commit(crate::schemas::rollout::RolloutItem::Event(
                    crate::schemas::rollout::EventItem {
                        event: MEMORY_MAINTENANCE_ROLLOUT_EVENT.into(),
                        turn_id: None,
                        payload: Some(payload),
                    },
                ));
            }));
            if appended.is_err() {
                return Err(ShutdownPhaseFailure::error(
                    "memory maintenance rollout append_event adapter panicked",
                ));
            }
        }
        Ok(())
    }

    pub(crate) async fn flush_memory_maintenance_rollout(&self) -> ShutdownPhaseResult {
        if self
            .maintenance
            .persistence
            .finalized
            .load(Ordering::SeqCst)
        {
            return Err(ShutdownPhaseFailure::error(
                "memory maintenance persistence is finalized",
            ));
        }
        match std::panic::AssertUnwindSafe(self.flush_rollout())
            .catch_unwind()
            .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(ShutdownPhaseFailure::error(format!(
                "memory maintenance rollout flush failed: {error}"
            ))),
            Err(_) => Err(ShutdownPhaseFailure::error(
                "memory maintenance rollout flush adapter panicked",
            )),
        }
    }

    pub(crate) fn memory_maintenance_cursor_matches(
        event: &MemoryMaintenanceEvent,
        cursor: Option<&RuntimeString>,
    ) -> bool {
        match (event, cursor) {
            (MemoryMaintenanceEvent::AutoMemory { run }, Some(cursor)) => {
                matches!(
                    (run.status, run.reason),
                    (MemoryMaintenanceStatus::Completed, _)
                        | (
                            MemoryMaintenanceStatus::Skipped,
                            Some(AutoMemoryRunReason::DirectMemoryWrite)
                        )
                ) && run.cursor_message_uuid.as_ref() == Some(cursor)
            }
            (MemoryMaintenanceEvent::AutoMemory { run }, None) => !matches!(
                (run.status, run.reason),
                (MemoryMaintenanceStatus::Completed, _)
                    | (
                        MemoryMaintenanceStatus::Skipped,
                        Some(AutoMemoryRunReason::DirectMemoryWrite)
                    )
            ),
            (MemoryMaintenanceEvent::AutoDream { .. }, None) => true,
            (MemoryMaintenanceEvent::AutoDream { .. }, Some(_)) => false,
        }
    }

    pub(crate) fn shutdown_terminal_for_started_memory_maintenance(
        &self,
        event: &MemoryMaintenanceEvent,
    ) -> Option<MemoryMaintenanceEvent> {
        let now = (self.deps.now)();
        match event {
            MemoryMaintenanceEvent::AutoMemory { run }
                if run.status == MemoryMaintenanceStatus::Started =>
            {
                let mut run = run.clone();
                run.status = MemoryMaintenanceStatus::Failed;
                run.ended_at = Some(run.terminal_timestamp(now));
                run.reason = Some(AutoMemoryRunReason::Shutdown);
                run.error =
                    Some("automatic memory extraction stopped before effect admission".into());
                Some(MemoryMaintenanceEvent::AutoMemory { run })
            }
            MemoryMaintenanceEvent::AutoDream { run }
                if run.status == MemoryMaintenanceStatus::Started =>
            {
                let mut run = run.clone();
                run.status = MemoryMaintenanceStatus::Failed;
                run.ended_at = Some(run.terminal_timestamp(now));
                run.reason = Some(AutoDreamRunReason::Shutdown);
                run.error =
                    Some("automatic Dream consolidation stopped before effect admission".into());
                Some(MemoryMaintenanceEvent::AutoDream { run })
            }
            _ => None,
        }
    }

    pub(crate) fn register_memory_maintenance_durability_owner_locked(
        self: &Arc<Self>,
        registry: &mut MaintenanceRegistry,
        event: MemoryMaintenanceEvent,
        cursor: Option<RuntimeString>,
        allow_while_closing: bool,
        result_send: tokio::sync::oneshot::Sender<bool>,
        recovery_send: Option<tokio::sync::watch::Sender<Option<Result<(), RuntimeString>>>>,
    ) -> ShutdownPhaseResult {
        if let Some(error) = self.memory_maintenance_query_fatal() {
            return Err(ShutdownPhaseFailure::error(format!(
                "memory maintenance durability admission is fail-closed: {error}"
            )));
        }
        if registry.durability_closed
            || self
                .maintenance
                .persistence
                .finalized
                .load(Ordering::SeqCst)
        {
            return Err(ShutdownPhaseFailure::error(
                "memory maintenance durability admission is closed",
            ));
        }
        if registry.closing && !allow_while_closing {
            return Err(ShutdownPhaseFailure::error(
                "memory maintenance durability admission is closing",
            ));
        }
        let key = MaintenanceRunKey::from_event(&event);
        if registry.provisionals.contains_key(&key) {
            return Err(ShutdownPhaseFailure::error(
                "memory maintenance run already has a provisional transition owner",
            ));
        }
        if !Self::memory_maintenance_cursor_matches(&event, cursor.as_ref()) {
            return Err(ShutdownPhaseFailure::error(
                "memory maintenance provisional cursor does not match its lifecycle transition",
            ));
        }
        // Retain the gate through provisional/task registration. A failing
        // durability owner installs the sticky error under this same gate, so
        // registration is now wholly before or wholly after that fatal
        // linearization point, never partly across it.
        let _event_gate = self.maintenance.persistence.event_gate.lock_recover();
        if let Some(error) = self.memory_maintenance_query_fatal() {
            return Err(ShutdownPhaseFailure::error(format!(
                "memory maintenance durability admission is fail-closed: {error}"
            )));
        }
        let mut projected = self.maintenance.snapshot();
        if !projected.apply(&event) {
            return Err(ShutdownPhaseFailure::error(
                "memory maintenance provisional transition was rejected by the authoritative lifecycle",
            ));
        }
        drop(projected);

        let task_id = registry.next_durability_task_id;
        registry.next_durability_task_id = registry.next_durability_task_id.wrapping_add(1);
        registry.provisionals.insert(
            key.clone(),
            MaintenanceProvisional {
                owner_id: task_id,
                event: event.clone(),
                cursor: cursor.clone(),
            },
        );
        let (start_send, start_receive) = tokio::sync::oneshot::channel::<()>();
        let task_session = self.clone();
        let task_key = key.clone();
        let task_event = event;
        let advances_standalone_auto_memory_cursor = matches!(
            &task_event,
            MemoryMaintenanceEvent::AutoMemory { run }
                if run.status == MemoryMaintenanceStatus::Skipped
                    && run.reason == Some(AutoMemoryRunReason::DirectMemoryWrite)
        ) && cursor.is_some();
        let task = registry.durability_tracker.spawn(async move {
            let _registration = MaintenanceDurabilityTaskRegistration {
                session: Arc::downgrade(&task_session),
                task_id,
                key: task_key.clone(),
            };
            if start_receive.await.is_err() {
                return;
            }
            let persisted = std::panic::AssertUnwindSafe(
                task_session.persist_and_promote_memory_maintenance_event(
                    task_id,
                    task_key.clone(),
                    task_event,
                    cursor,
                ),
            )
            .catch_unwind()
            .await;
            let (succeeded, recovery_verdict) = match persisted {
                Ok(Ok(effect_admitted)) => (effect_admitted, Ok(())),
                Ok(Err(error)) => {
                    let recovery_error = RuntimeString::from(error.message.clone());
                    task_session.fail_memory_maintenance_provisional(task_id, &task_key, error);
                    (false, Err(recovery_error))
                }
                Err(_) => {
                    let error = ShutdownPhaseFailure::error(
                        "Session-owned memory maintenance durability task panicked",
                    );
                    task_session.fail_memory_maintenance_provisional(
                        task_id,
                        &task_key,
                        error.clone(),
                    );
                    (false, Err(RuntimeString::from(error.message)))
                }
            };
            let _ = result_send.send(succeeded);
            if let Some(recovery_send) = recovery_send {
                let _ = recovery_send.send(Some(recovery_verdict));
            }
            if advances_standalone_auto_memory_cursor
                && task_session.memory_scheduler.take_extraction_pending()
                && !task_session.lifecycle.is_disposed()
                && !task_session.maintenance_is_closing()
            {
                task_session.maybe_extract_memories();
            }
        });
        let abort = task.abort_handle();
        registry
            .durability_tasks
            .insert(task_id, MaintenanceDurabilityTaskControl { abort });
        let _ = start_send.send(());
        drop(task);
        Ok(())
    }

    pub(crate) fn fail_memory_maintenance_provisional(
        &self,
        task_id: u64,
        key: &MaintenanceRunKey,
        error: ShutdownPhaseFailure,
    ) {
        let message = error.message.clone();
        let Ok(mut registry) = self.maintenance_tasks.lock() else {
            self.install_memory_maintenance_query_fatal_direct(
                Some(key),
                RuntimeString::from(format!(
                    "memory maintenance registry was poisoned after a durability failure: {message}"
                )),
            );
            return;
        };
        if registry
            .provisionals
            .get(key)
            .is_some_and(|provisional| provisional.owner_id == task_id)
        {
            registry.provisionals.remove(key);
        }
        registry.persistence_failed_runs.insert(key.clone());
        registry.failures.push(error);
        drop(registry);
        self.install_memory_maintenance_query_fatal(
            Some(key),
            RuntimeString::from(format!(
                "memory maintenance persistence is inconsistent: {message}"
            )),
        );
    }

    pub(crate) async fn persist_and_promote_memory_maintenance_event(
        &self,
        task_id: u64,
        key: MaintenanceRunKey,
        event: MemoryMaintenanceEvent,
        cursor: Option<RuntimeString>,
    ) -> Result<bool, ShutdownPhaseFailure> {
        let _persistence = self.maintenance.persistence.serial.lock().await;
        let result = async {
            if let Some(error) = self.memory_maintenance_query_fatal() {
                return Err(ShutdownPhaseFailure::error(format!(
                    "memory maintenance persistence is fail-closed: {error}"
                )));
            }
            if self
                .maintenance
                .persistence
                .finalized
                .load(Ordering::SeqCst)
            {
                return Err(ShutdownPhaseFailure::error(
                    "memory maintenance persistence was finalized before its owner completed",
                ));
            }
            self.append_memory_maintenance_event(&event).await?;
            self.flush_memory_maintenance_rollout().await?;

            let closing_started = {
                let registry = self.maintenance_tasks.lock().map_err(|_| {
                    ShutdownPhaseFailure::error("memory maintenance task registry was poisoned")
                })?;
                let provisional_matches =
                    registry.provisionals.get(&key).is_some_and(|provisional| {
                        provisional.owner_id == task_id
                            && provisional.event == event
                            && provisional.cursor == cursor
                    });
                if !provisional_matches {
                    return Err(ShutdownPhaseFailure::error(
                        "memory maintenance durability owner lost its provisional transition",
                    ));
                }
                memory_maintenance_event_status(&event) == MemoryMaintenanceStatus::Started
                    && registry.closing
            };
            if closing_started {
                let terminal = self
                    .shutdown_terminal_for_started_memory_maintenance(&event)
                    .expect("a Started maintenance event has a shutdown terminal");
                self.append_memory_maintenance_event(&terminal).await?;
                self.flush_memory_maintenance_rollout().await?;

                let mut registry = self.maintenance_tasks.lock().map_err(|_| {
                    ShutdownPhaseFailure::error("memory maintenance task registry was poisoned")
                })?;
                let provisional_matches =
                    registry.provisionals.get(&key).is_some_and(|provisional| {
                        provisional.owner_id == task_id
                            && provisional.event == event
                            && provisional.cursor == cursor
                    });
                if !provisional_matches {
                    return Err(ShutdownPhaseFailure::error(
                        "memory maintenance durability owner lost its closing Started transition",
                    ));
                }
                let _event_gate = self
                    .maintenance
                    .persistence
                    .event_gate
                    .lock()
                    .map_err(|_| {
                        ShutdownPhaseFailure::error("memory maintenance event gate was poisoned")
                    })?;
                let mut snapshot = self.maintenance.snapshot.lock().map_err(|_| {
                    ShutdownPhaseFailure::error("memory maintenance lifecycle state was poisoned")
                })?;
                if !snapshot.apply(&event) || !snapshot.apply(&terminal) {
                    return Err(ShutdownPhaseFailure::error(
                        "durable closing maintenance transition could not be promoted",
                    ));
                }
                drop(snapshot);
                registry.provisionals.remove(&key);
                registry.persistence_failed_runs.remove(&key);
                drop(registry);
                self.publish_memory_maintenance_event_visible(event.clone());
                self.publish_memory_maintenance_event_visible(terminal);
                return Ok(false);
            }
            let mut registry = self.maintenance_tasks.lock().map_err(|_| {
                ShutdownPhaseFailure::error("memory maintenance task registry was poisoned")
            })?;
            let _event_gate = self
                .maintenance
                .persistence
                .event_gate
                .lock()
                .map_err(|_| {
                    ShutdownPhaseFailure::error("memory maintenance event gate was poisoned")
                })?;
            if !self
                .maintenance
                .snapshot
                .lock()
                .map_err(|_| {
                    ShutdownPhaseFailure::error("memory maintenance lifecycle state was poisoned")
                })?
                .apply(&event)
            {
                return Err(ShutdownPhaseFailure::error(
                    "durable memory maintenance transition could not be promoted",
                ));
            }
            if let Some(cursor) = &cursor {
                *self
                    .memory_scheduler
                    .last_memory_message_uuid
                    .lock()
                    .map_err(|_| {
                        ShutdownPhaseFailure::error("memory maintenance cursor state was poisoned")
                    })? = Some(cursor.clone());
            }
            registry.provisionals.remove(&key);
            registry.persistence_failed_runs.remove(&key);
            drop(registry);
            self.publish_memory_maintenance_event_visible(event.clone());
            Ok(true)
        }
        .await;
        if let Err(error) = &result {
            // Keep the serial guard held until the sticky failure is visible so
            // a later Started owner cannot slip through and admit new effects.
            self.install_memory_maintenance_query_fatal(
                Some(&key),
                RuntimeString::from(format!(
                    "memory maintenance persistence is inconsistent: {}",
                    error.message
                )),
            );
        }
        result
    }

    pub(crate) async fn commit_memory_maintenance_event_durable(
        self: &Arc<Self>,
        event: MemoryMaintenanceEvent,
        cursor: Option<RuntimeString>,
        allow_while_closing: bool,
    ) -> bool {
        let key = MaintenanceRunKey::from_event(&event);
        let terminal = memory_maintenance_event_status(&event) != MemoryMaintenanceStatus::Started;
        let (result_send, result_receive) = tokio::sync::oneshot::channel();
        let receive = {
            let mut registry = match self.maintenance_tasks.lock() {
                Ok(registry) => registry,
                Err(_) => {
                    if terminal {
                        self.install_memory_maintenance_query_fatal_direct(
                            Some(&key),
                            "memory maintenance registry was poisoned before a terminal durability owner could be registered",
                        );
                    }
                    return false;
                }
            };
            match self.register_memory_maintenance_durability_owner_locked(
                &mut registry,
                event,
                cursor,
                allow_while_closing,
                result_send,
                None,
            ) {
                Ok(()) => result_receive,
                Err(error) => {
                    let message = error.message.clone();
                    registry.failures.push(error);
                    drop(registry);
                    if terminal {
                        self.install_memory_maintenance_query_fatal(
                            Some(&key),
                            RuntimeString::from(format!(
                                "memory maintenance terminal durability admission failed: {message}"
                            )),
                        );
                    }
                    return false;
                }
            }
        };
        receive.await.unwrap_or(false)
    }

    pub(crate) fn schedule_memory_maintenance_event_durable(
        self: &Arc<Self>,
        event: MemoryMaintenanceEvent,
        cursor: Option<RuntimeString>,
    ) -> bool {
        self.schedule_memory_maintenance_event_durable_with_policy(event, cursor, false)
    }

    pub(crate) fn schedule_memory_maintenance_event_durable_with_policy(
        self: &Arc<Self>,
        event: MemoryMaintenanceEvent,
        cursor: Option<RuntimeString>,
        allow_while_closing: bool,
    ) -> bool {
        let key = MaintenanceRunKey::from_event(&event);
        let effect_already_visible = cursor.is_some();
        let (result_send, _result_receive) = tokio::sync::oneshot::channel();
        let mut registry = match self.maintenance_tasks.lock() {
            Ok(registry) => registry,
            Err(_) => {
                if effect_already_visible {
                    self.install_memory_maintenance_query_fatal_direct(
                        Some(&key),
                        "memory maintenance registry was poisoned after an effect became visible",
                    );
                }
                return false;
            }
        };
        match self.register_memory_maintenance_durability_owner_locked(
            &mut registry,
            event,
            cursor,
            allow_while_closing,
            result_send,
            None,
        ) {
            Ok(()) => true,
            Err(error) => {
                let message = error.message.clone();
                // A standalone gate-skip has no admitted worker/effect. If
                // shutdown won the admission race, rejecting that audit event
                // is the clean boundary rather than a persistence failure for
                // an in-flight run.
                if allow_while_closing || !registry.closing {
                    registry.failures.push(error);
                }
                drop(registry);
                if effect_already_visible {
                    self.install_memory_maintenance_query_fatal(
                        Some(&key),
                        RuntimeString::from(format!(
                            "memory maintenance effect could not register its durability owner: {message}"
                        )),
                    );
                }
                false
            }
        }
    }

    pub(crate) fn enqueue_isolated_memory_maintenance_event(
        &self,
        sink: MemoryMaintenanceEventSink,
        event: MemoryMaintenanceEvent,
    ) {
        let mut state = self.maintenance.isolated_sink.lock_recover();
        if state.closing {
            return;
        }
        if state.dispatcher.is_none() {
            let (send, receive) =
                std::sync::mpsc::channel::<(MemoryMaintenanceEventSink, MemoryMaintenanceEvent)>();
            let (finished_send, finished_receive) = tokio::sync::oneshot::channel();
            let spawned = std::thread::Builder::new()
                .name(format!(
                    "klaus-memory-events-{}",
                    self.id
                        .try_to_utf8()
                        .unwrap_or_else(|_| "session".to_string())
                ))
                .spawn(move || {
                    while let Ok((sink, event)) = receive.recv() {
                        let _ =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(event)));
                    }
                    let _ = finished_send.send(());
                });
            if spawned.is_err() {
                return;
            }
            state.dispatcher = Some(IsolatedMaintenanceSinkDispatcher {
                sender: Some(send),
                finished: Some(finished_receive),
            });
        }
        if let Some(sender) = state
            .dispatcher
            .as_ref()
            .and_then(|dispatcher| dispatcher.sender.as_ref())
        {
            let _ = sender.send((sink, event));
        }
    }

    pub(crate) async fn drain_isolated_memory_maintenance_sink(
        &self,
        deadline: tokio::time::Instant,
    ) -> ShutdownPhaseResult {
        // Remove the public callback first, then permanently close dispatcher
        // admission. An event that cloned the callback just before this point
        // still observes `closing` below and cannot recreate a post-ACK thread.
        *self.maintenance.event_sink.lock_recover() = None;
        let finished = {
            let mut state = self.maintenance.isolated_sink.lock_recover();
            state.closing = true;
            let Some(mut dispatcher) = state.dispatcher.take() else {
                return Ok(());
            };
            let _ = dispatcher.sender.take();
            dispatcher.finished.take()
        };
        let Some(finished) = finished else {
            return Ok(());
        };
        match tokio::time::timeout_at(deadline, finished).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(ShutdownPhaseFailure::error(
                "memory maintenance event dispatcher stopped unexpectedly",
            )),
            Err(_) => Err(ShutdownPhaseFailure::timeout(
                "memory maintenance event dispatcher timed out during shutdown",
            )),
        }
    }

    /// Commit exactly one terminal transition for an in-flight run. Shutdown
    /// and the worker can race, but only the winner is persisted/emitted.
    #[cfg(test)]
    pub(crate) fn finish_memory_maintenance_if_started(
        &self,
        event: MemoryMaintenanceEvent,
        completed_auto_memory_cursor: Option<RuntimeString>,
    ) -> bool {
        let _event_gate = self.maintenance.persistence.event_gate.lock_recover();
        let accepted = {
            let mut snapshot = self.maintenance.snapshot.lock_recover();
            let started = match &event {
                MemoryMaintenanceEvent::AutoMemory { run } => snapshot
                    .auto_memory_runs
                    .iter()
                    .find(|existing| existing.run_id == run.run_id)
                    .is_some_and(|existing| existing.status == MemoryMaintenanceStatus::Started),
                MemoryMaintenanceEvent::AutoDream { run } => snapshot
                    .auto_dream_runs
                    .iter()
                    .find(|existing| existing.dream_run_id == run.dream_run_id)
                    .is_some_and(|existing| existing.status == MemoryMaintenanceStatus::Started),
            };
            if !started {
                false
            } else {
                let cursor_matches_terminal = match (&event, &completed_auto_memory_cursor) {
                    (MemoryMaintenanceEvent::AutoMemory { run }, Some(cursor)) => {
                        run.status == MemoryMaintenanceStatus::Completed
                            && run.cursor_message_uuid.as_ref() == Some(cursor)
                    }
                    (MemoryMaintenanceEvent::AutoMemory { run }, None) => {
                        run.status != MemoryMaintenanceStatus::Completed
                    }
                    (MemoryMaintenanceEvent::AutoDream { .. }, None) => true,
                    (MemoryMaintenanceEvent::AutoDream { .. }, Some(_)) => false,
                };
                if !cursor_matches_terminal || !snapshot.apply(&event) {
                    false
                } else {
                    if let Some(cursor) = &completed_auto_memory_cursor {
                        self.memory_scheduler
                            .set_last_memory_uuid(Some(cursor.clone()));
                    }
                    true
                }
            }
        };
        if accepted {
            self.publish_memory_maintenance_event(event);
        }
        accepted
    }

    pub(crate) fn register_maintenance_task(
        self: &Arc<Self>,
        task: MaintenanceTask,
        started: Option<MemoryMaintenanceEvent>,
    ) -> Result<(), MaintenanceTask> {
        let mut registry = self.maintenance_tasks.lock_recover();
        registry.tasks.retain(|task| !task.handle.is_finished());
        if registry.closing {
            return Err(task);
        }
        if let Some(started) = started {
            if self.persistence.rollout().is_some() {
                // Persisted automatic work must use the gated registration
                // API, which owns a Started verdict before releasing effects.
                return Err(task);
            } else {
                let _event_gate = self.maintenance.persistence.event_gate.lock_recover();
                if !self.maintenance.snapshot.lock_recover().apply(&started) {
                    return Err(task);
                }
                registry.tasks.push(task);
                drop(registry);
                self.publish_memory_maintenance_event_visible(started);
            }
        } else {
            registry.tasks.push(task);
        }
        Ok(())
    }

    /// Atomically registers the worker, its provisional Started transition,
    /// and the Session-owned durability owner that alone may release the
    /// worker after append+flush+visibility promotion.
    pub(crate) fn register_durable_auto_maintenance_task(
        self: &Arc<Self>,
        task: MaintenanceTask,
        started: MemoryMaintenanceEvent,
        started_result: tokio::sync::oneshot::Sender<bool>,
    ) -> Result<(), MaintenanceTask> {
        let mut registry = self.maintenance_tasks.lock_recover();
        registry.tasks.retain(|task| !task.handle.is_finished());
        if registry.closing || registry.durability_closed {
            return Err(task);
        }
        registry.tasks.push(task);
        if let Err(error) = self.register_memory_maintenance_durability_owner_locked(
            &mut registry,
            started,
            None,
            false,
            started_result,
            None,
        ) {
            let task = registry
                .tasks
                .pop()
                .expect("the just-registered maintenance worker remains last");
            registry.failures.push(error);
            return Err(task);
        }
        Ok(())
    }

    pub(crate) fn maintenance_is_closing(&self) -> bool {
        self.maintenance_tasks.lock_recover().closing
    }

    pub(crate) fn fail_started_maintenance_on_shutdown(
        self: &Arc<Self>,
        kind: MaintenanceTaskKind,
        run_id: &RuntimeString,
    ) {
        let Some(key) = (match kind {
            MaintenanceTaskKind::AutoMemory => Some(MaintenanceRunKey::AutoMemory(run_id.clone())),
            MaintenanceTaskKind::AutoDream => Some(MaintenanceRunKey::AutoDream(run_id.clone())),
            MaintenanceTaskKind::ManualDream => None,
        }) else {
            return;
        };
        // Registry ownership is the terminal CAS. Promotion and every other
        // owner admission also require this lock, so the Started check and
        // fallback owner registration below cannot race a worker terminal into
        // a spurious shutdown failure.
        let mut registry = self.maintenance_tasks.lock_recover();
        if registry.provisionals.contains_key(&key)
            || registry.persistence_failed_runs.contains(&key)
        {
            return;
        }
        let now = (self.deps.now)();
        let event = {
            let snapshot = self.maintenance.snapshot.lock_recover();
            match kind {
                MaintenanceTaskKind::AutoMemory => snapshot
                    .auto_memory_runs
                    .iter()
                    .find(|run| {
                        run.run_id == *run_id && run.status == MemoryMaintenanceStatus::Started
                    })
                    .cloned()
                    .map(|mut run| {
                        run.status = MemoryMaintenanceStatus::Failed;
                        run.ended_at = Some(run.terminal_timestamp(now));
                        run.reason = Some(AutoMemoryRunReason::Shutdown);
                        run.error =
                            Some("automatic memory extraction stopped during shutdown".into());
                        MemoryMaintenanceEvent::AutoMemory { run }
                    }),
                MaintenanceTaskKind::AutoDream => snapshot
                    .auto_dream_runs
                    .iter()
                    .find(|run| {
                        run.dream_run_id == *run_id
                            && run.status == MemoryMaintenanceStatus::Started
                    })
                    .cloned()
                    .map(|mut run| {
                        run.status = MemoryMaintenanceStatus::Failed;
                        run.ended_at = Some(run.terminal_timestamp(now));
                        run.reason = Some(AutoDreamRunReason::Shutdown);
                        run.error =
                            Some("automatic Dream consolidation stopped during shutdown".into());
                        MemoryMaintenanceEvent::AutoDream { run }
                    }),
                MaintenanceTaskKind::ManualDream => None,
            }
        };
        if let Some(event) = event {
            let (result_send, _result_receive) = tokio::sync::oneshot::channel();
            if let Err(error) = self.register_memory_maintenance_durability_owner_locked(
                &mut registry,
                event,
                None,
                true,
                result_send,
                None,
            ) {
                registry.failures.push(error);
            }
        }
    }

    pub(crate) async fn fail_started_maintenance_on_panic(
        self: &Arc<Self>,
        kind: MaintenanceTaskKind,
        run_id: &RuntimeString,
    ) {
        let now = (self.deps.now)();
        let event = {
            let snapshot = self.maintenance.snapshot.lock_recover();
            match kind {
                MaintenanceTaskKind::AutoMemory => snapshot
                    .auto_memory_runs
                    .iter()
                    .find(|run| run.run_id == *run_id)
                    .cloned()
                    .map(|mut run| {
                        run.status = MemoryMaintenanceStatus::Failed;
                        run.ended_at = Some(run.terminal_timestamp(now));
                        run.reason = Some(AutoMemoryRunReason::ForkFailure);
                        run.error = Some("automatic memory extraction worker panicked".into());
                        MemoryMaintenanceEvent::AutoMemory { run }
                    }),
                MaintenanceTaskKind::AutoDream => snapshot
                    .auto_dream_runs
                    .iter()
                    .find(|run| run.dream_run_id == *run_id)
                    .cloned()
                    .map(|mut run| {
                        run.status = MemoryMaintenanceStatus::Failed;
                        run.ended_at = Some(run.terminal_timestamp(now));
                        run.reason = Some(AutoDreamRunReason::ForkFailure);
                        run.error = Some("automatic Dream consolidation worker panicked".into());
                        MemoryMaintenanceEvent::AutoDream { run }
                    }),
                MaintenanceTaskKind::ManualDream => None,
            }
        };
        if let Some(event) = event {
            let _ = self
                .commit_memory_maintenance_event_durable(event, None, true)
                .await;
        }
    }

    /// Close maintenance admission, cancel every registered fork, and join all
    /// workers behind one bounded grace period. A worker that ignores the
    /// cooperative signal is force-aborted and joined before its
    /// lifecycle/completion surface is made terminal, so observers never see a
    /// terminal run while its writes or OS lock can still be active.
    pub(crate) async fn drain_maintenance_tasks(
        self: &Arc<Self>,
        grace_deadline: tokio::time::Instant,
        force_deadline: tokio::time::Instant,
        durability_deadline: tokio::time::Instant,
        durability_abort_deadline: tokio::time::Instant,
    ) -> ShutdownPhaseResult {
        let mut tasks = {
            let mut registry = self.maintenance_tasks.lock_recover();
            registry.closing = true;
            self.memory_scheduler.clear_extraction_pending();
            std::mem::take(&mut registry.tasks)
        };
        for task in &tasks {
            task.cancel.cancel();
        }

        let mut joined = vec![false; tasks.len()];
        let mut worker_panicked = false;
        let mut worker_cancelled = false;
        let timed_out = tokio::time::timeout_at(grace_deadline, async {
            for (index, task) in tasks.iter_mut().enumerate() {
                if let Err(error) = (&mut task.handle).await {
                    worker_panicked |= error.is_panic();
                    worker_cancelled |= error.is_cancelled();
                }
                joined[index] = true;
            }
        })
        .await
        .is_err();

        let mut force_abort_incomplete = false;
        if timed_out {
            for (index, task) in tasks.iter_mut().enumerate() {
                if !joined[index] && !task.handle.is_finished() {
                    task.handle.abort();
                }
            }
            force_abort_incomplete = tokio::time::timeout_at(force_deadline, async {
                for (index, task) in tasks.iter_mut().enumerate() {
                    if !joined[index] {
                        if let Err(error) = (&mut task.handle).await {
                            worker_panicked |= error.is_panic();
                            // Cancellation here is expected only because
                            // the hard-timeout branch issued abort above.
                        }
                        joined[index] = true;
                    }
                }
            })
            .await
            .is_err();
        }

        // Cooperative workers normally publish/send these themselves. For a
        // hard timeout, force-abort/join above first so observers cannot see a
        // terminal lifecycle or manual completion while writes/locks remain.
        // The CAS and sender slot make this fallback idempotent in races.
        for (index, task) in tasks.iter().enumerate() {
            if !joined[index] {
                // A synchronous/foreign blocking section cannot be stopped by
                // Tokio abort.  Keep its lifecycle non-terminal and its manual
                // waiter owned by the still-running worker; publishing a
                // shutdown result here would falsely claim its writes and OS
                // lock had ended.
                continue;
            }
            self.fail_started_maintenance_on_shutdown(task.kind, &task.run_id);
            if let Some(slot) = &task.manual_completion {
                if let Some(done_tx) = slot.lock_recover().take() {
                    let _ = done_tx.send(DreamNowCompletion {
                        ok: false,
                        reason: Some(MANUAL_DREAM_SHUTDOWN_ERROR.into()),
                        files_touched: None,
                        turn_count: None,
                    });
                }
            }
        }

        let unjoined_auto_memory = tasks
            .iter()
            .enumerate()
            .any(|(index, task)| !joined[index] && task.kind == MaintenanceTaskKind::AutoMemory);
        let unjoined_dream = tasks.iter().enumerate().any(|(index, task)| {
            !joined[index]
                && matches!(
                    task.kind,
                    MaintenanceTaskKind::AutoDream | MaintenanceTaskKind::ManualDream
                )
        });
        if !unjoined_auto_memory {
            self.memory_scheduler.end_extraction();
        }
        if !unjoined_dream {
            self.force_clear_dream_owner();
        }
        let mut errors = Vec::<String>::new();
        if timed_out {
            errors.push(MAINTENANCE_SHUTDOWN_ERROR.to_string());
        }
        if force_abort_incomplete {
            errors.push(MAINTENANCE_FORCE_ABORT_ERROR.to_string());
        }
        if worker_panicked {
            errors
                .push("one or more memory maintenance tasks panicked during shutdown".to_string());
        }
        if worker_cancelled && !timed_out {
            errors.push(
                "one or more memory maintenance tasks were cancelled unexpectedly".to_string(),
            );
        }
        let durability_tracker = {
            let mut registry = self.maintenance_tasks.lock().map_err(|_| {
                ShutdownPhaseFailure::error("memory maintenance task registry was poisoned")
            })?;
            registry.durability_closed = true;
            registry.durability_tracker.close();
            registry.durability_tracker.clone()
        };
        let durability_timed_out =
            tokio::time::timeout_at(durability_deadline, durability_tracker.wait())
                .await
                .is_err();
        let mut durability_force_incomplete = false;
        if durability_timed_out {
            let aborts = {
                let registry = self.maintenance_tasks.lock().map_err(|_| {
                    ShutdownPhaseFailure::error("memory maintenance task registry was poisoned")
                })?;
                registry
                    .durability_tasks
                    .values()
                    .map(|task| task.abort.clone())
                    .collect::<Vec<_>>()
            };
            for abort in aborts {
                abort.abort();
            }
            durability_force_incomplete =
                tokio::time::timeout_at(durability_abort_deadline, durability_tracker.wait())
                    .await
                    .is_err();
        }
        if durability_timed_out {
            errors.push("memory maintenance durability exceeded its shutdown deadline".to_string());
        }
        if durability_force_incomplete {
            errors.push("memory maintenance durability force-abort did not join".to_string());
        }
        let prior_failures = self
            .maintenance_tasks
            .lock()
            .map_err(|_| {
                ShutdownPhaseFailure::error("memory maintenance task registry was poisoned")
            })?
            .failures
            .clone();
        for failure in &prior_failures {
            errors.push(failure.message.clone());
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ShutdownPhaseFailure {
                message: errors.join("; "),
                timed_out: timed_out
                    || force_abort_incomplete
                    || durability_timed_out
                    || durability_force_incomplete
                    || prior_failures.iter().any(|failure| failure.timed_out),
            })
        }
    }

    pub(crate) async fn finalize_memory_maintenance_persistence_fence(
        &self,
        deadline: tokio::time::Instant,
    ) -> ShutdownPhaseResult {
        self.maintenance
            .persistence
            .finalized
            .store(true, Ordering::SeqCst);
        let _persistence = tokio::time::timeout_at(
            deadline,
            self.maintenance.persistence.serial.lock(),
        )
        .await
        .map_err(|_| {
            ShutdownPhaseFailure::timeout(
                "memory maintenance persistence fence timed out waiting for an in-flight barrier; final rollout flush/shutdown was skipped",
            )
        })?;
        loop {
            if self.try_complete_memory_maintenance_event_fence()? {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ShutdownPhaseFailure::timeout(
                    "memory maintenance persistence fence timed out waiting for visibility promotion; final rollout flush/shutdown was skipped",
                ));
            }
            tokio::task::yield_now().await;
        }
    }

    pub(crate) fn try_complete_memory_maintenance_event_fence(
        &self,
    ) -> Result<bool, ShutdownPhaseFailure> {
        match self.maintenance.persistence.event_gate.try_lock() {
            Ok(_gate) => Ok(true),
            Err(std::sync::TryLockError::WouldBlock) => Ok(false),
            Err(std::sync::TryLockError::Poisoned(_)) => Err(ShutdownPhaseFailure::error(
                "memory maintenance persistence fence could not verify a poisoned event gate; final rollout flush/shutdown was skipped",
            )),
        }
    }

    pub(crate) async fn drain_background_tasks(
        &self,
        mut tasks: Vec<tokio::task::JoinHandle<()>>,
        grace_deadline: tokio::time::Instant,
        force_deadline: tokio::time::Instant,
    ) -> ShutdownPhaseResult {
        let mut failure: Option<ShutdownPhaseFailure> = None;
        loop {
            if let Err(error) = self
                .drain_background_batch(tasks, grace_deadline, force_deadline)
                .await
            {
                if let Some(previous) = failure.as_mut() {
                    previous.message.push_str("; ");
                    previous.message.push_str(&error.message);
                    previous.timed_out |= error.timed_out;
                } else {
                    failure = Some(error);
                }
            }
            // Resource guards can publish cleanup while their original owners
            // settle. Drain those handles under the same shutdown deadlines.
            tasks = std::mem::take(&mut self.background_tasks.lock_recover().handles);
            if tasks.is_empty() {
                break;
            }
        }
        failure.map_or(Ok(()), Err)
    }

    async fn drain_background_batch(
        &self,
        mut tasks: Vec<tokio::task::JoinHandle<()>>,
        grace_deadline: tokio::time::Instant,
        force_deadline: tokio::time::Instant,
    ) -> ShutdownPhaseResult {
        if tasks.is_empty() {
            self.close_ordinary_worker_resources().await;
            return Ok(());
        }
        let mut joined = vec![false; tasks.len()];
        let mut panicked = false;
        let mut unexpectedly_cancelled = false;
        let timed_out = tokio::time::timeout_at(grace_deadline, async {
            for (index, task) in tasks.iter_mut().enumerate() {
                if let Err(error) = (&mut *task).await {
                    panicked |= error.is_panic();
                    unexpectedly_cancelled |= error.is_cancelled();
                }
                joined[index] = true;
            }
        })
        .await
        .is_err();
        let mut force_abort_incomplete = false;
        if timed_out {
            for (index, task) in tasks.iter().enumerate() {
                if !joined[index] && !task.is_finished() {
                    task.abort();
                }
            }
            force_abort_incomplete = tokio::time::timeout_at(force_deadline, async {
                for (index, task) in tasks.iter_mut().enumerate() {
                    if !joined[index] {
                        if let Err(error) = (&mut *task).await {
                            panicked |= error.is_panic();
                        }
                        joined[index] = true;
                    }
                }
            })
            .await
            .is_err();
        }
        let mut errors = Vec::new();
        if joined.iter().all(|joined| *joined)
            && tokio::time::timeout_at(force_deadline, self.close_ordinary_worker_resources())
                .await
                .is_err()
        {
            errors.push("ordinary worker resource shutdown exceeded the shared deadline");
        }
        if timed_out {
            errors.push("background task shutdown exceeded the shared 60000ms grace period");
        }
        if force_abort_incomplete {
            errors.push("background task force-abort did not join within 5000ms");
        }
        if panicked {
            errors.push("one or more background tasks panicked during shutdown");
        }
        if unexpectedly_cancelled && !timed_out {
            errors.push("one or more background tasks were cancelled unexpectedly");
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ShutdownPhaseFailure {
                message: errors.join("; "),
                timed_out: timed_out || force_abort_incomplete,
            })
        }
    }

    pub(crate) async fn wait_for_active_turn_shutdown(
        &self,
        deadline: tokio::time::Instant,
    ) -> ShutdownPhaseResult {
        let wait = async {
            while !self.turn.is_stack_empty() {
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout_at(deadline, wait).await.map_err(|_| {
            ShutdownPhaseFailure::timeout(
                "active turn did not stop within the shared 60000ms grace period",
            )
        })
    }

    /// Names of sub-agent roles currently registered for `task` delegation.
    pub fn agent_definition_names(&self) -> Vec<RuntimeString> {
        self.current_agent_registry()
            .list()
            .into_iter()
            .map(|agent| agent.name)
            .collect()
    }

    /// Names of skills visible through the current skill gate.
    pub fn visible_skill_names(&self) -> Vec<RuntimeString> {
        self.gated_skills()
            .into_iter()
            .map(|skill| skill.name)
            .collect()
    }

    pub fn active_output_style_name(&self) -> Option<RuntimeString> {
        self.plugin_projection()
            .map(|projection| projection.active_output_style.clone())
            .unwrap_or_else(|| self.active_output_style.clone())
            .as_ref()
            .map(|style| style.name.clone())
    }

    /// Effective extension graph assembled by the core for this session.
    pub fn extension_runtime_snapshot(&self) -> ExtensionRuntimeSnapshot {
        let mut snapshot = self
            .plugin_projection()
            .map(|projection| projection.extension_runtime.clone())
            .unwrap_or_else(|| self.extension_runtime.clone());
        snapshot.pricing = self.inference_snapshot().pricing.clone();
        snapshot
    }

    pub fn lsp_available(&self) -> bool {
        self.current_lsp_manager()
            .as_ref()
            .map(|manager| manager.available())
            .unwrap_or(false)
    }

    pub fn cost_snapshot(&self) -> CostSnapshot {
        self.cost.lock_recover().snapshot()
    }

    pub(crate) async fn reserve_usage_receipt(
        &self,
        signal: &AbortSignal,
    ) -> std::io::Result<Option<crate::store::receipt_broker::ReceiptSlot>> {
        crate::store::receipt_broker::reserve_receipt(self.persistence.usage_receipts(), signal)
            .await
    }

    /// Account and transfer an already reserved physical request without a
    /// suspension point, including when its stream is cancelled or dropped.
    pub(crate) fn record_reserved_assistant_usage_for(
        &self,
        assistant: &AssistantMessage,
        bundle: &super::InferenceBundle,
        slot: Option<crate::store::receipt_broker::ReceiptSlot>,
    ) {
        if let Some(record) = self.account_assistant_usage_for(assistant, bundle) {
            if let Some(slot) = slot {
                slot.commit(record);
            }
        }
    }

    /// In-memory accounting must survive closed or failed persistence admission.
    pub(crate) fn account_assistant_usage_for(
        &self,
        assistant: &AssistantMessage,
        bundle: &super::InferenceBundle,
    ) -> Option<crate::schemas::rollout::UsageLedgerItem> {
        let usage = assistant.usage.as_ref()?;
        // One assistant message is emitted per completed provider request, so
        // its engine-owned UUID is also the stable billing request identity.
        let wire_model = runtime_model_to_cost_host(&assistant.model);
        // An auxiliary target may share its wire model id with the main model, so
        // book it under the bundle's source-qualified identity instead of the bare
        // name; the wire name still drives the price lookup.
        let ledger_model = CostTracker::source_qualified_ledger_model(bundle.source.as_ref());
        let record = self.cost.lock_recover().record_attributed(
            runtime_assistant_id_to_cost_host(&assistant.uuid),
            runtime_assistant_id_to_cost_host(&assistant.uuid),
            &wire_model,
            usage,
            &bundle.rates,
            UsageAttribution {
                inference: Some(bundle.identity(assistant.model.clone())),
                ledger_model: ledger_model.as_deref(),
            },
        );
        Some(record)
    }

    pub fn context_snapshot(&self) -> SessionContextSnapshot {
        let (_, config, _) = self.model_runtime();
        let model = config.model.clone();
        let context_window = crate::context::compact::window::actual_model_window(&config);
        let history = self.model_history();
        let window_usage = self.turn.context_window_usage.lock().unwrap().clone();
        let total_tokens =
            estimate_context_tokens(&history).max(window_usage.active_tokens.unwrap_or(0));
        let status = crate::context::compact::policy::context_window_status(
            &config,
            total_tokens,
            window_usage.prefill_input_tokens,
        );
        let compact_threshold_tokens = match config.auto_compact_scope {
            crate::schemas::config::AutoCompactScope::Total => status.auto_compact_scope_limit,
            crate::schemas::config::AutoCompactScope::BodyAfterPrefix => status
                .auto_compact_scope_limit
                .saturating_add(window_usage.prefill_input_tokens.unwrap_or(total_tokens)),
        }
        .min(status.full_context_window_limit);
        let cost = self.cost.lock_recover();
        let token_totals = cost.total_usage();
        let total_cost_usd = cost.total_cost_usd();
        let cost_snapshot = cost.snapshot();
        drop(cost);
        let budget_remaining_usd = self
            .config
            .max_budget_usd
            .map(|max| (max - total_cost_usd).max(0.0));
        SessionContextSnapshot {
            thread_id: self.id.clone(),
            cwd: self.cwd_now().into_runtime_string(),
            model,
            context_window,
            output_reserve_tokens: crate::context::compact::policy::output_reservation(&config),
            compact_threshold: (compact_threshold_tokens as f64
                / status.full_context_window_limit.max(1) as f64)
                .clamp(0.0, 1.0),
            compact_threshold_tokens,
            total_tokens,
            token_totals,
            cost: cost_snapshot,
            window_state: if total_tokens == 0 {
                super::ContextWindowState::Empty
            } else if status.full_context_window_limit_reached {
                super::ContextWindowState::OverLimit
            } else if config.auto_compaction && status.token_limit_reached {
                super::ContextWindowState::NearLimit
            } else {
                super::ContextWindowState::Normal
            },
            max_budget_usd: self.config.max_budget_usd,
            budget_remaining_usd,
            budget_state: budget_state(total_cost_usd, self.config.max_budget_usd),
            history_messages: history.len(),
        }
    }

    pub async fn dream_now(self: &Arc<Self>) -> Result<DreamNowTask, String> {
        let inference = Arc::new(self.begin_inference_activity()?);
        let Some(_admission) = self.maintenance_admission.begin() else {
            return Err("cannot dream while the session is shutting down".to_string());
        };
        if self.is_disposed() {
            return Err("cannot dream on a disposed session".to_string());
        }
        let Some(dir) = self
            .memory_roots
            .main
            .clone()
            .filter(|_| self.config.auto_dream)
        else {
            return Ok(DreamNowTask::not_started(DreamNowReason::GateClosed));
        };
        let Some(dream_owner) = self.try_begin_dream() else {
            return Ok(DreamNowTask::not_started(DreamNowReason::AlreadyRunning));
        };
        let now = (self.deps.now)();
        let boundary = match self.memory_roots.pinned_root() {
            Some(boundary) => boundary,
            None => {
                self.finish_dream_owner(dream_owner);
                return Err(format!(
                    "failed to use construction-time memory root: {}",
                    self.memory_roots
                        .pin_error
                        .as_ref()
                        .map(runtime_error_message_to_public_host)
                        .unwrap_or_else(|| "pinned root unavailable".to_string())
                ));
            }
        };
        let lock = match try_acquire_consolidation_lock_pinned(
            boundary,
            std::process::id() as i64,
            now as f64,
        )
        .await
        {
            Ok(Some(lock)) => lock,
            Ok(None) => {
                self.finish_dream_owner(dream_owner);
                return Ok(DreamNowTask::not_started(DreamNowReason::LockHeld));
            }
            Err(err) => {
                self.finish_dream_owner(dream_owner);
                return Err(format!("failed to acquire consolidation lock: {err}"));
            }
        };
        let this = self.clone();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let completion_slot = Arc::new(Mutex::new(Some(done_tx)));
        let task_completion_slot = completion_slot.clone();
        let abort = CancellationToken::new();
        let task_abort = abort.clone();
        let run_id = RuntimeString::from((self.deps.uuid)());
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _dream_owner = DreamInProgressGuard::new(&this, dream_owner);
            if start_rx.await.is_err() {
                return;
            }
            let worker = std::panic::AssertUnwindSafe(async {
                match this
                    .consolidate_under_lock(&dir, now, lock, task_abort, inference)
                    .await
                {
                    Ok(result) => DreamNowCompletion {
                        ok: true,
                        reason: None,
                        files_touched: Some(
                            result.files_touched.into_iter().map(Into::into).collect(),
                        ),
                        turn_count: Some(result.turn_count),
                    },
                    Err(reason) => DreamNowCompletion {
                        ok: false,
                        reason: Some(
                            if reason.kind == MemoryForkFailureKind::Shutdown {
                                MANUAL_DREAM_SHUTDOWN_ERROR.to_string()
                            } else {
                                reason.to_string()
                            }
                            .into(),
                        ),
                        files_touched: None,
                        turn_count: None,
                    },
                }
            })
            .catch_unwind()
            .await;
            let completion = match worker {
                Ok(completion) => completion,
                Err(_) => DreamNowCompletion {
                    ok: false,
                    reason: Some("memory consolidation worker panicked".into()),
                    files_touched: None,
                    turn_count: None,
                },
            };
            if let Some(done_tx) = task_completion_slot.lock_recover().take() {
                let _ = done_tx.send(completion);
            }
        });
        let task = MaintenanceTask {
            run_id,
            kind: MaintenanceTaskKind::ManualDream,
            cancel: abort,
            handle,
            manual_completion: Some(completion_slot),
        };
        if let Err(mut task) = self.register_maintenance_task(task, None) {
            task.cancel.cancel();
            task.handle.abort();
            let _ = (&mut task.handle).await;
            self.finish_dream_owner(dream_owner);
            return Err("cannot dream while the session is shutting down".to_string());
        }
        let _ = start_tx.send(());
        Ok(DreamNowTask {
            ok: true,
            reason: None,
            done: Some(done_rx),
        })
    }

    pub(crate) fn try_begin_dream(&self) -> Option<u64> {
        self.memory_scheduler.try_begin_dream()
    }

    pub(crate) fn finish_dream_owner(&self, owner: u64) {
        self.memory_scheduler.finish_dream_owner(owner);
    }

    pub(crate) fn force_clear_dream_owner(&self) {
        self.memory_scheduler.force_clear_dream_owner();
    }

    pub async fn ask_side_question(
        self: &Arc<Self>,
        question: String,
    ) -> Result<SideQuestionResult, String> {
        self.ask_side_question_with_signal(question, AbortSignal::new())
            .await
    }

    pub async fn ask_side_question_with_signal(
        self: &Arc<Self>,
        question: String,
        signal: AbortSignal,
    ) -> Result<SideQuestionResult, String> {
        self.ask_side_question_input_with_signal(Input::Text(question.into()), signal)
            .await
    }

    /// Ask a read-only side question with inline text, image, and PDF content.
    pub async fn ask_side_question_input(
        self: &Arc<Self>,
        question: Input,
    ) -> Result<SideQuestionResult, String> {
        self.ask_side_question_input_with_signal(question, AbortSignal::new())
            .await
    }

    pub async fn ask_side_question_input_with_signal(
        self: &Arc<Self>,
        question: Input,
        signal: AbortSignal,
    ) -> Result<SideQuestionResult, String> {
        if signal.is_cancelled() {
            return Err("side question cancelled".to_string());
        }
        let input = side_question_input(question)?;
        let outcome = self
            .run_readonly_query(ReadonlyQueryOptions {
                input,
                system_append: Some(SIDE_QUESTION_SYSTEM_PROMPT.into()),
                output_schema: None,
                max_output_tokens: None,
                // A side question is conversational traffic, not maintenance:
                // keep the session's current main model.
                model_tier: None,
                feature: None,
                include_history: true,
                bare_system_prompt: false,
                inherit_session_reasoning: true,
                effort: None,
                sync_memory: true,
                signal,
                cancelled_error: "side question cancelled",
            })
            .await?;
        Ok(SideQuestionResult {
            response: outcome.final_text,
            synthetic: false,
            usage: outcome.usage,
            stop_reason: outcome.stop_reason,
        })
    }

    pub async fn generate_title(
        self: &Arc<Self>,
        instructions: Option<String>,
        prompt: Option<String>,
        persist: bool,
    ) -> Result<GenerateTitleResult, String> {
        self.generate_title_with_signal(instructions, prompt, persist, AbortSignal::new())
            .await
    }

    /// Choose the least expensive thinking config for a title request.
    /// Disable thinking when the model accepts that shape, otherwise keep the
    /// lowest available reasoning rung so the request does not fail.
    fn title_thinking_for(capabilities: &ModelCapabilities) -> Option<ThinkingConfig> {
        match capabilities.thinking {
            Some(cap) if cap.can_disable_thinking => Some(ThinkingConfig::Disabled),
            Some(_) => Some(ThinkingConfig::Adaptive),
            None => None,
        }
    }

    pub async fn generate_title_with_signal(
        self: &Arc<Self>,
        instructions: Option<String>,
        prompt: Option<String>,
        persist: bool,
        signal: AbortSignal,
    ) -> Result<GenerateTitleResult, String> {
        if signal.is_cancelled() {
            return Err("title generation cancelled".to_string());
        }
        // A host-supplied prompt is authoritative: it carries the caller's own
        // view of the conversation (capped and framed) for the title model.
        let description = match prompt.and_then(nonempty_trimmed) {
            Some(host_prompt) => RuntimeString::from(host_prompt),
            None => conversation_text_for_title(&self.history_snapshot()),
        };
        if runtime_trim(&description).is_empty() {
            return Ok(GenerateTitleResult {
                title: None,
                persisted: false,
                raw: RuntimeString::default(),
                usage: None,
            });
        }
        let prompt = match instructions.and_then(nonempty_trimmed) {
            Some(extra) => description
                .concat(&RuntimeString::from("\n\nAdditional title guidance: "))
                .concat(&RuntimeString::from(extra)),
            None => description,
        };
        let outcome = self
            .run_readonly_query(ReadonlyQueryOptions {
                input: Input::Text(prompt),
                system_append: Some(SESSION_TITLE_PROMPT.into()),
                output_schema: Some(title_output_schema()),
                // The budget must leave room for reasoning on thinking
                // models; a starved budget returns no title text at all.
                max_output_tokens: Some(2048),
                // Titles ride their configured feature target; an unset or
                // unavailable target follows the session's main model.
                model_tier: None,
                feature: Some(FeatureSlot::Title),
                // The host prompt already carries the capped seed text; the
                // raw transcript would only add latency and reasoning spend.
                include_history: false,
                bare_system_prompt: true,
                inherit_session_reasoning: false,
                effort: None,
                sync_memory: false,
                signal: signal.clone(),
                cancelled_error: "title generation cancelled",
            })
            .await?;
        let title = parse_generated_title(&outcome.final_text);
        let mut persisted = false;
        if persist {
            if let Some(title) = title.as_ref() {
                tokio::select! {
                    biased;
                    _ = signal.cancelled() => {
                        return Err("title generation cancelled".to_string());
                    }
                    result = self.persist_title(title) => {
                        result?;
                    }
                }
                persisted = true;
            }
        }
        Ok(GenerateTitleResult {
            title,
            persisted,
            raw: outcome.final_text,
            usage: outcome.usage,
        })
    }

    /// Read the current composer eligibility from the active inference bundle.
    pub async fn prediction_context(self: &Arc<Self>) -> Result<super::PredictionContext, String> {
        let signal = AbortSignal::new();
        let capture = self.capture_inference(&signal).await?;
        let history = self.history_snapshot();
        let index = self.turn_index_snapshot();
        let turn_id = self.latest_prediction_turn(&history, &index);
        let running = self.turn.active_stack.lock_recover().iter().any(|turn| {
            turn.turn_id.as_ref().is_none_or(|id| {
                index
                    .turn_status
                    .get(id)
                    .is_none_or(|status| status == "running" || status == "inProgress")
            })
        }) || turn_id.as_ref().is_some_and(|id| {
            index
                .turn_status
                .get(id)
                .is_none_or(|status| status == "running" || status == "inProgress")
        });
        let turn_id = turn_id.filter(|id| index.turn_status.contains_key(id));
        Ok(super::PredictionContext {
            turn_id,
            model: capture.bundle.config.model.clone(),
            provider: capture.bundle.provider.info().name.clone(),
            plan_mode: self.collaboration_mode() == crate::planning::CollaborationMode::Plan,
            running,
        })
    }

    /// Run a context-bound suggestion without admitting or persisting a user turn.
    pub async fn predict_message_with_signal(
        self: &Arc<Self>,
        prompt: String,
        source_turn_id: String,
        effort: Option<crate::schemas::effort::EffortValue>,
        signal: AbortSignal,
    ) -> Result<Option<RuntimeString>, String> {
        if signal.is_cancelled() {
            return Err("prediction cancelled".to_string());
        }
        if prompt.trim().is_empty() {
            return Err("prediction prompt must not be empty".to_string());
        }
        let generation = self.turn.next_generation.load(Ordering::SeqCst);
        if !self.prediction_source_is_current(&source_turn_id) {
            return Ok(None);
        }
        let schema = serde_json::json!({
            "type": "object",
            "properties": { "suggestion": { "anyOf": [{ "type": "string" }, { "type": "null" }] } },
            "required": ["suggestion"],
            "additionalProperties": false
        })
        .as_object()
        .cloned()
        .unwrap_or_default();
        let query = self.run_readonly_query(ReadonlyQueryOptions {
            input: Input::Text(prompt.into()),
            system_append: None,
            output_schema: Some(schema),
            max_output_tokens: None,
            model_tier: None,
            // Predictions ride their configured feature target; an unset or
            // unavailable target follows the session's main model.
            feature: Some(FeatureSlot::Prediction),
            include_history: true,
            bare_system_prompt: false,
            inherit_session_reasoning: true,
            effort,
            sync_memory: false,
            signal: signal.clone(),
            cancelled_error: "prediction cancelled",
        });
        // Admission changes before provider IO or while it is streaming invalidate
        // this snapshot. Dropping the losing query future releases its capture.
        let changed = async {
            loop {
                if self.turn.next_generation.load(Ordering::SeqCst) != generation
                    || !self.prediction_source_is_current(&source_turn_id)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };
        let outcome = tokio::select! {
            biased;
            _ = signal.cancelled() => return Err("prediction cancelled".to_string()),
            _ = changed => {
                signal.cancel();
                return Ok(None);
            }
            result = query => result?,
        };
        if signal.is_cancelled() {
            return Err("prediction cancelled".to_string());
        }
        if self.turn.next_generation.load(Ordering::SeqCst) != generation
            || !self.prediction_source_is_current(&source_turn_id)
        {
            return Ok(None);
        }
        if outcome.stop_reason != TerminalReason::Completed {
            return Ok(None);
        }
        let value = crate::runtime_values::runtime_json::parse(&outcome.final_text)
            .map_err(|_| "prediction response must be structured JSON".to_string())?;
        match value.get("suggestion") {
            Some(RuntimeJson::Null) => Ok(None),
            Some(RuntimeJson::String(text)) => {
                Ok(Some(runtime_trim(text)).filter(|text| !text.is_empty()))
            }
            _ => Err("prediction response must contain suggestion".to_string()),
        }
    }

    fn update_prediction_source_checkpoint(&self, generation: u64, turn_id: Option<RuntimeString>) {
        let mut latest = self.turn.latest_prediction_source.lock_recover();
        if latest
            .as_ref()
            .is_some_and(|(current, _)| *current >= generation)
        {
            return;
        }
        *latest = Some((generation, turn_id));
    }

    #[cfg(test)]
    pub(crate) async fn record_prediction_source_checkpoint(
        &self,
        generation: u64,
        turn_id: Option<RuntimeString>,
    ) {
        let record = self.admit_prediction_source_checkpoint(generation, turn_id);
        self.persistence
            .retain_admission_records(vec![record])
            .expect("prediction checkpoint registration");
        if let Some(rollout) = &self.persistence.ordered_rollout {
            rollout
                .wait_prefixes()
                .await
                .expect("prediction checkpoint admission");
        }
    }

    pub(crate) fn admit_prediction_source_checkpoint(
        &self,
        generation: u64,
        turn_id: Option<RuntimeString>,
    ) -> crate::schemas::rollout::RolloutItem {
        self.update_prediction_source_checkpoint(generation, turn_id.clone());
        let mut payload = crate::runtime_values::RuntimeObject::new();
        payload.insert("generation", RuntimeJson::from(generation.to_string()));
        crate::schemas::rollout::RolloutItem::Event(crate::schemas::rollout::EventItem {
            event: PREDICTION_SOURCE_ROLLOUT_EVENT.into(),
            turn_id,
            payload: Some(RuntimeJson::Object(payload)),
        })
    }

    pub(crate) fn restore_prediction_source_checkpoint(
        &self,
        checkpoint: (u64, Option<RuntimeString>),
    ) {
        let next = checkpoint
            .0
            .checked_add(1)
            .expect("validated prediction source generation");
        self.turn.next_generation.fetch_max(next, Ordering::SeqCst);
        *self.turn.latest_prediction_source.lock_recover() = Some(checkpoint);
    }

    pub(crate) fn restore_prediction_source_turn(&self, turn_id: Option<RuntimeString>) {
        *self.turn.latest_prediction_source.lock_recover() = turn_id.map(|id| (0, Some(id)));
    }

    fn latest_prediction_turn(
        &self,
        history: &[Message],
        index: &super::TurnIndex,
    ) -> Option<RuntimeString> {
        self.turn
            .latest_prediction_source
            .lock_recover()
            .as_ref()
            .and_then(|(_, id)| id.clone())
            .or_else(|| {
                history.iter().rev().find_map(|message| {
                    index
                        .message_turn
                        .get(super::message_uuid(message))
                        .cloned()
                })
            })
    }

    fn prediction_source_is_current(&self, source_turn_id: &str) -> bool {
        if self.is_disposed() {
            return false;
        }
        let source = RuntimeString::from(source_turn_id);
        if self
            .turn
            .active_stack
            .lock_recover()
            .iter()
            .any(|turn| turn.turn_id.as_ref() != Some(&source))
        {
            return false;
        }
        let history = self.history_snapshot();
        let index = self.turn_index_snapshot();
        let latest = self.latest_prediction_turn(&history, &index);
        latest.as_ref() == Some(&source)
            && index
                .turn_status
                .get(&source)
                .is_some_and(|status| status != "running" && status != "inProgress")
    }

    pub async fn persist_title(&self, title: &RuntimeString) -> Result<(), String> {
        let store_title = runtime_title_to_store_host(title);
        if let Some(rollout) = self.persistence.rollout() {
            return rollout
                .update_title(&store_title)
                .await
                .map_err(|err| err.to_string());
        }
        if let Some(store) = self.persistence.store() {
            let store_session_id = runtime_session_id_to_title_store_host(&self.id);
            return store
                .update_title(&store_session_id, &store_title)
                .await
                .map_err(|err| err.to_string());
        }
        Ok(())
    }

    pub async fn persist_host_metadata(
        &self,
        host_metadata: Option<RuntimeJson>,
    ) -> Result<(), String> {
        if let Some(rollout) = self.persistence.rollout() {
            return rollout
                .update_host_metadata(host_metadata)
                .await
                .map_err(|err| err.to_string());
        }
        if let Some(store) = self.persistence.store() {
            let store_session_id = runtime_session_id_to_session_store_host(&self.id);
            return store
                .update_host_metadata(&store_session_id, host_metadata)
                .await
                .map_err(|err| err.to_string());
        }
        Ok(())
    }

    pub(crate) async fn run_readonly_query(
        self: &Arc<Self>,
        opts: ReadonlyQueryOptions,
    ) -> Result<ReadonlyQueryOutcome, String> {
        let capture = self.capture_inference(&opts.signal).await?;
        let bundle = match opts.feature {
            Some(slot) => capture.bundle.for_feature(slot)?,
            None => match opts.model_tier {
                Some(tier) => capture.bundle.for_tier(tier)?,
                None => capture.bundle.clone(),
            },
        };
        let capture = capture.with_bundle(bundle.clone());
        if self.is_disposed() {
            return Err("cannot run a readonly query on a disposed session".to_string());
        }
        if opts.signal.is_cancelled() {
            return Err(opts.cancelled_error.to_string());
        }
        let model = bundle.config.model.clone();
        // Side questions have their own 5 x 10 MiB binary budget. Validate each
        // part through the standard model/payload gate without imposing the
        // ordinary turn's separate 20 MiB aggregate PDF budget.
        let inputs = match opts.input {
            Input::Text(text) => vec![Input::Text(text)],
            Input::Parts(parts) => parts
                .into_iter()
                .map(|part| Input::Parts(vec![part]))
                .collect(),
        };
        let mut content = Vec::new();
        let mut input_pdf_bytes = 0;
        let mut input_pdf_documents = 0;
        for input in inputs {
            let validated = self
                .validate_input_for_bundle(input, &model, &bundle)
                .map_err(|error| error.message)?;
            if validated.has_images && !bundle.provider.capabilities(&model).vision {
                return Err("The selected model cannot read image attachments. Select a vision-capable model for this side question.".to_string());
            }
            input_pdf_bytes += validated.pdf_bytes;
            input_pdf_documents += validated.pdf_documents;
            content.extend(to_content_blocks(&validated.input));
        }
        let abort = opts.signal;
        let user_uuid = (self.deps.uuid)();
        let mut messages = if opts.include_history {
            self.model_history()
        } else {
            Vec::new()
        };
        messages.push(Message::User(UserMessage {
            question_answer: None,
            uuid: user_uuid.into(),
            content,
            is_meta: None,
        }));
        let mut config = bundle.config_for_model(&model);
        if let Some(max) = opts.max_output_tokens {
            config.max_output_tokens =
                Some(max.min(bundle.provider.capabilities(&model).max_output_tokens));
        }
        let system_prompt = self.readonly_system_prompt(
            model.clone(),
            opts.system_append.clone(),
            bundle.clone(),
            opts.sync_memory,
            opts.bare_system_prompt,
        );
        let permission = self.permission_context();
        let tool_context_base: ToolContextFactory = {
            let this = self.clone();
            let abort = abort.clone();
            let permission = permission.clone();
            Arc::new(move || {
                let mut ctx = bare_tool_context(
                    this.process_context.clone(),
                    this.background_processes.clone(),
                    this.cwd_now(),
                    abort.clone(),
                    this.file_state.clone() as Arc<dyn FileStateCache>,
                    permission.clone(),
                    this.runtime_policy_snapshot().sandbox_backend.clone(),
                );
                let policy = this.runtime_policy_snapshot();
                policy.apply_to_context(&mut ctx);
                ctx
            })
        };
        let cost_so_far: crate::engine::query::CostSoFar = {
            let cost = self.cost.clone();
            Box::new(move || cost.lock_recover().total_cost_usd())
        };
        let readonly_suffix =
            RuntimeString::from(":readonly:").concat(&RuntimeString::from((self.deps.uuid)()));
        // A title asks for the lowest reasoning rung the model actually
        // declares. A model with no effort ladder gets no effort instead of a
        // level its endpoint never named. Side questions and predictions keep
        // the session's own selection.
        let readonly_effort = if opts.inherit_session_reasoning {
            opts.effort.clone().or(bundle.config.effort.clone())
        } else {
            bundle
                .provider
                .model_metadata(&model)
                .effort
                .as_ref()
                .and_then(|capability| capability.lowest())
                .map(crate::schemas::effort::EffortValue::Level)
        };
        let params = QueryParams {
            multi_agent_instructions: false,
            thread_id: self.id.concat(&readonly_suffix),
            turn_id: RuntimeString::from((self.deps.uuid)()),
            model: model.clone(),
            provider_info: Some(bundle.provider.info().clone()),
            messages,
            compaction_protected_message_ids: HashSet::new(),
            context_window: None,
            precompute: None,
            compaction_history: None,
            recover_compaction_history: None,
            compaction_output: None,
            tools: Vec::new(),
            tool_specs: Vec::new(),
            refresh_tools: None,
            refresh_tools_async: None,
            // A title request carries no agent policy; side questions and
            // predictions keep the session's definition.
            agent_tool_policy: if opts.inherit_session_reasoning {
                self.root_agent_definition.clone()
            } else {
                None
            },
            system_prompt: SystemPromptResolvable::Fn(system_prompt),
            config,
            max_turns: None,
            cost_so_far: Some(cost_so_far),
            tool_context_base,
            permission_engine: self.permission_engine.clone(),
            hook_bus: None,
            signal: abort.clone(),
            approval_handler: None,
            approval_reviewer: None,
            output_schema: opts.output_schema.as_ref().map(runtime_schema_from_static),
            token_budget: None,
            goal: None,
            goal_request_identity: None,
            goal_pending_input: None,
            goal_execution_enabled: None,
            goal_usage: None,
            web_search: None,
            on_message: None,
            prepare_request_usage: Some(self.request_usage_recorder({
                let bundle = bundle.clone();
                Arc::new(move || bundle.clone())
            })),
            retained_tool_tasks: Some(self.persistence.tool_tasks.clone()),
            message_rollout: None,
            on_permission_update: None,
            on_compaction: None,
            surfaced_tools: None,
            on_surface_tools: None,
            on_tool_lifecycle: None,
            provider_call_admission: None,
            on_provider_request_admitted: None,
            emit_thread_started: false,
            reminder_providers: Vec::new(),
            edited_files_reminder: None,
            effort: readonly_effort,
            // Side questions read the session's orchestration mode. A title
            // request is a fixed-shape call and never orchestrates.
            ultra_mode: if opts.inherit_session_reasoning {
                bundle.config.ultra_mode
            } else {
                false
            },
            // A title should not inherit the session's reasoning spend, but it
            // must still satisfy the selected model. If the model can turn
            // thinking off, do so; otherwise keep the lowest available rung
            // (adaptive) rather than emit a disabled shape the model rejects.
            thinking: if !opts.inherit_session_reasoning {
                Self::title_thinking_for(&bundle.provider.model_metadata(&model))
            } else if bundle.config.thinking {
                Some(ThinkingConfig::Adaptive)
            } else {
                None
            },
            skip_cache_write: Some(true),
            cache_ttl: if self.config.cache1h_ttl {
                Some(CacheTtl::OneHour)
            } else {
                None
            },
            cache_domain: None,
            cache_key: Some(self.id.clone()),
            plan_file_path: None,
            message_queue: None,
            input_pdf_bytes,
            input_pdf_documents,
            validate_native_pdf: {
                let provider = bundle.provider.clone();
                Some(Arc::new(move |model| {
                    validate_native_pdf_provider(provider.as_ref(), model)
                }))
            },
            agent_id: None,
            agent_type: None,
            // A read-only side query runs on its own thread id and owns no
            // long-lived cached prefix, so the idle pass never applies.
            context_hint_state: None,
            file_state: None,
            tool_result_budget_state: None,
            on_tool_result_budget_records: None,
            thinking_context_cold: self.turn.thinking_context_cold(),
            padded_countdown_spend: self.turn.padded_countdown_spend(),
            task_countdown_anchor: None,
            live_model_override: None,
        };
        let mut events = crate::engine::query::query_with_inference(
            params,
            bundle.deps.to_engine_deps(),
            crate::engine::query::InferenceCapture {
                slot: Arc::new(Mutex::new(Some(capture))),
                next: None,
                model_override: None,
                effort_override: opts.effort,
                ultra_override: None,
            },
        );
        let mut final_text = RuntimeString::default();
        let mut usage = None;
        let mut stop_reason = TerminalReason::Completed;
        loop {
            let ev = tokio::select! {
                biased;
                _ = abort.cancelled() => {
                    return Err(opts.cancelled_error.to_string());
                }
                ev = events.next() => ev,
            };
            let Some(ev) = ev else {
                break;
            };
            match ev {
                EngineEvent::ItemCompleted {
                    inference: _,
                    item: ThreadItem::AgentMessage { text, .. },
                } => final_text = text,
                EngineEvent::TurnCompleted {
                    usage: u,
                    stop_reason: reason,
                    ..
                } => {
                    usage = u;
                    stop_reason = reason;
                }
                EngineEvent::TurnFailed { error, .. } => {
                    return Err(runtime_error_message_to_public_host(&error.message));
                }
                _ => {}
            }
        }
        Ok(ReadonlyQueryOutcome {
            final_text,
            usage,
            stop_reason,
        })
    }

    fn readonly_system_prompt(
        self: &Arc<Self>,
        turn_model: RuntimeString,
        append: Option<RuntimeString>,
        bundle: Arc<super::InferenceBundle>,
        sync_memory: bool,
        bare: bool,
    ) -> SystemPromptThunk {
        let this = self.clone();
        Arc::new(move || {
            let this = this.clone();
            let turn_model = turn_model.clone();
            let bundle = bundle.clone();
            let append = append.clone();
            Box::pin(async move {
                let base_provider = this.system_prompt_provider.read_recover().clone();
                // Resolve the session prompt even for bare queries: resolving
                // it is part of the session's ordering barrier; only its
                // content is dropped here.
                let base_parts = (base_provider)().await;
                let mut parts = if bare { Vec::new() } else { base_parts };
                if this.system_prompt_is_default {
                    let is_worktree = this.workspace.is_in_worktree();
                    let cwd = this.cwd_now();
                    for p in parts.iter_mut() {
                        if p.name == "env" {
                            p.text = environment_block(
                                &cwd,
                                &bundle.config,
                                EnvBlockOptions {
                                    plat: None,
                                    shell: None,
                                    model: Some(turn_model.clone()),
                                    is_worktree,
                                },
                            );
                        }
                    }
                }
                let memory_dir = if bare { None } else { this.memory_roots.main() };
                if let Some(dir) = memory_dir {
                    if let Some(host) = this.agent_memory_host.as_ref().filter(|_| sync_memory) {
                        this.sync_agent_memory(
                            host.as_ref(),
                            "main",
                            AgentMemoryScope::Project,
                            dir,
                        )
                        .await;
                    }
                    let text =
                        build_agent_memory_prompt_text("main", AgentMemoryScope::Project, dir)
                            .await;
                    if !text.is_empty() {
                        parts.push(SystemPromptPart {
                            name: "memory".into(),
                            text: text.into(),
                            cacheable: None,
                            cache_scope: None,
                        });
                    }
                }
                if let Some(append) = append {
                    parts.push(SystemPromptPart {
                        name: "readonly".into(),
                        text: append,
                        cacheable: None,
                        cache_scope: None,
                    });
                }
                parts
            })
        })
    }
}

impl Session {
    /// Persist one host-rendered message as a completed turn without running a
    /// model. Friend conversations park inbound messages while the owner
    /// decides whether the task may run, so they cannot start a turn, yet their
    /// content has to live in the same history as every other message.
    ///
    /// The message joins the live history as well as the rollout, so the next
    /// turn of a session that stays open reads the same context a resumed one
    /// would. The op runs on the session's serial chain, never inside a turn,
    /// so it cannot split a tool call from its result.
    pub async fn append_display_message(
        &self,
        message_id: RuntimeString,
        input: Input,
        display_metadata: RuntimeJson,
    ) -> Result<(), RuntimeString> {
        let Some(rollout) = self.persistence.rollout() else {
            return Err(RuntimeString::from("the session has no persisted rollout"));
        };
        let turn_id: RuntimeString = (self.deps.uuid)().into();
        let started_at_ms = (self.deps.now)();
        let model = self.inference_config().model.clone();
        let policy = self.runtime_policy_snapshot();
        rollout
            .append_turn_context(crate::schemas::rollout::TurnContextSnapshot {
                cwd: self.cwd_now().into_runtime_string(),
                model,
                permission_mode: crate::types::permission::ExternalPermissionMode::Default,
                turn_id: Some(turn_id.clone()),
                started_at_ms: Some(started_at_ms),
                approval_policy: policy.state.approval_policy,
                sandbox_mode: policy.state.sandbox_mode,
                approvals_reviewer: policy.state.approvals_reviewer,
                network_allowed: Some(policy.state.network_allowed),
                writable_roots: None,
            })
            .await;
        let message = Message::User(UserMessage {
            question_answer: None,
            uuid: message_id.clone(),
            content: to_content_blocks(&input),
            is_meta: None,
        });
        let display = crate::engine::session::user_display_message(
            message_id.clone(),
            &display_metadata,
            (self.deps.uuid)().into(),
        );
        rollout.append_message(message.clone()).await;
        rollout.append_message(display.clone()).await;
        // A host retry with the same message id is already in the live history.
        let known = self
            .history_snapshot()
            .iter()
            .any(|existing| matches!(existing, Message::User(user) if user.uuid == message_id));
        if !known {
            {
                let mut index = self.turn_index.lock_recover();
                for item in [&message, &display] {
                    index
                        .message_turn
                        .insert(super::message_uuid(item).clone(), turn_id.clone());
                }
                // The appended row is a finished turn of its own. Without a live
                // status the composer treats it as unknown and offers no
                // suggestion until the session is reopened.
                index
                    .turn_status
                    .insert(turn_id.clone(), RuntimeString::from("completed"));
            }
            for item in [message, display] {
                self.push_history(item);
            }
            // The newest row is now the source the composer suggests from. A
            // running turn keeps that role: it already sees this row and ends
            // after it, so the checkpoint only moves while the session is idle.
            // The generation is reserved first, so a turn admitted after this
            // point still outranks the display row.
            if let Some(generation) = self.turn.reserve_generation() {
                if self.turn.is_stack_empty() {
                    if let crate::schemas::rollout::RolloutItem::Event(event) =
                        self.admit_prediction_source_checkpoint(generation, Some(turn_id.clone()))
                    {
                        rollout
                            .append_event(
                                event.event,
                                Some(crate::store::session_store::AppendEventOptions {
                                    turn_id: event.turn_id,
                                    payload: event.payload,
                                }),
                            )
                            .await;
                    }
                }
            }
        }
        let mut payload = crate::runtime_values::RuntimeObject::new();
        payload.insert("status", RuntimeJson::String("completed".into()));
        payload.insert(
            "startedAtMs",
            RuntimeJson::Number(crate::runtime_values::RuntimeNumber::from(started_at_ms)),
        );
        payload.insert(
            "finishedAtMs",
            RuntimeJson::Number(crate::runtime_values::RuntimeNumber::from(started_at_ms)),
        );
        payload.insert(
            "durationMs",
            RuntimeJson::Number(crate::runtime_values::RuntimeNumber::from(0i64)),
        );
        rollout
            .append_event(
                crate::engine::session::TURN_LIFECYCLE_ROLLOUT_EVENT.into(),
                Some(crate::store::session_store::AppendEventOptions {
                    turn_id: Some(turn_id),
                    payload: Some(RuntimeJson::Object(payload)),
                }),
            )
            .await;
        rollout
            .flush()
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))
    }
}
