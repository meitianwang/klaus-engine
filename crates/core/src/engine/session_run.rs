use super::bare_tool_context;
use super::continuation_turn_options;
use super::home_dir;
use super::is_unc_path;
use super::join_path;
use super::join_runtime_strings;
use super::output_to_content;
use super::resolve_runtime_join;
use super::runtime_error_message_to_public_host;
use super::runtime_path_to_file_io_host;
use super::runtime_path_to_skill_shell_host;
use super::runtime_provider_name_to_log_host;
use super::runtime_text_to_at_mention_host;
use super::runtime_trim;
use super::to_content_blocks;
use super::to_store_compaction;
use super::tool_lifecycle_rollout_writer;
use super::validate_native_pdf_provider;
use super::ActiveTurnLease;
use super::AnyTool;
use super::CacheTtl;
use super::EngineEventStream;
use super::Input;
use super::PermissionDecision;
use super::QueryCompactionRecord;
use super::Session;
use super::SetupContextClaim;
use super::SlashCommandOutcome;
use super::SnapshotDecisionOrderedSink;
use super::SystemPromptResolvable;
use super::SystemPromptThunk;
use super::ThinkingConfig;
use super::Turn;
use super::TurnAdmissionError;
use super::TurnLifecycleStream;
use super::TurnOptions;
use super::TurnResult;
use super::UserContentPart;
use super::ValidatedInput;
use super::SIDE_TASK_SYSTEM_PROMPT;
use crate::capabilities::RuntimeCapabilityResolution;
use crate::context::at_mention::extract_agent_mentions;
use crate::context::at_mention::extract_file_mentions;
use crate::context::at_mention::has_at_mention;
use crate::context::at_mention::FileMention;
use crate::context::env_info::environment_block;
use crate::context::env_info::EnvBlockOptions;
use crate::context::tokens::estimate_context_tokens;
use crate::context::tokens::tokens_from_usage;
use crate::engine::message_queue::QueuePriority;
use crate::engine::message_queue::QueuedMessage;
use crate::engine::message_queue::QueuedValue;
use crate::engine::query::QueryParams;
use crate::engine::query::ToolContextFactory;
use crate::engine::web_search_select::select_web_search;
use crate::extensions::commands::LocalRunOutcome;

struct ManagedStepAdmission {
    session: Arc<Session>,
    slot: super::InferenceSlot,
    initial: Arc<super::InferenceBundle>,
    reviewer: Arc<RwLock<Option<crate::types::permission::ApprovalsReviewer>>>,
    inner: Option<Arc<dyn crate::engine::query::ProviderCallAdmission>>,
}

impl crate::engine::query::ProviderCallAdmission for ManagedStepAdmission {
    fn prepare_request(
        &self,
        request: &mut crate::types::provider::ModelRequest,
    ) -> Result<(), RuntimeString> {
        if let Some(inner) = &self.inner {
            inner.prepare_request(request)?;
        }
        let bundle = Session::bundle_from_slot(&self.slot, &self.initial);
        let requirements = bundle
            .load_review_requirements_sync()
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        let policy = self
            .session
            .resolve_managed_step_policy(&requirements, &request.model)
            .map_err(RuntimeString::from)?;
        *self.reviewer.write_recover() = policy.approvals_reviewer;
        // Enter the inner call boundary only after every pre-transport refusal.
        // A rejected request leaves a pending Setup claim prepared, so its
        // existing lease drop releases it rather than recording an unknown call.
        // The provider factory follows this method without an intervening await.
        if let Some(inner) = &self.inner {
            inner.before_call()?;
        }
        Ok(())
    }

    fn take_admitted_request_context(
        &self,
    ) -> Option<(Vec<Message>, Vec<crate::types::provider::SystemPromptPart>)> {
        self.inner
            .as_ref()
            .and_then(|inner| inner.take_admitted_request_context())
    }

    fn input_budget_exceeded(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| inner.input_budget_exceeded())
    }

    fn clear_input_budget_failure(&self) {
        if let Some(inner) = &self.inner {
            inner.clear_input_budget_failure();
        }
    }

    fn before_call(&self) -> Result<(), RuntimeString> {
        // The driver invokes this before the final wire request exists.
        // The composed inner boundary is admitted at the end of preparation.
        Ok(())
    }

    fn call_returned(&self) -> crate::engine::query::ProviderCallAdmissionReceipt {
        self.inner.as_ref().map_or_else(
            || Box::pin(async { Ok(()) }) as crate::engine::query::ProviderCallAdmissionReceipt,
            |inner| inner.call_returned(),
        )
    }

    fn call_panicked(&self) -> crate::engine::query::ProviderCallAdmissionReceipt {
        self.inner.as_ref().map_or_else(
            || Box::pin(async { Ok(()) }) as crate::engine::query::ProviderCallAdmissionReceipt,
            |inner| inner.call_panicked(),
        )
    }
}

/// The uuid every message variant carries, for turn-index bookkeeping.
fn message_uuid(message: &Message) -> Option<&RuntimeString> {
    match message {
        Message::User(m) => Some(&m.uuid),
        Message::Assistant(m) => Some(&m.uuid),
        Message::ToolResult(m) => Some(&m.uuid),
        Message::System(m) => Some(&m.uuid),
        Message::CompactBoundary(m) => Some(&m.uuid),
    }
}

/// Persist the tool lifecycle before publishing the same transition to the live
/// turn index.
///
/// The rollout is the durable record, but a LIVE `get_history` reads the index, so
/// a tool the stop killed has to be known in both places or the same turn answers
/// differently before and after a reopen.
fn tool_lifecycle_index_writer(
    session: Arc<Session>,
    sink: Option<crate::engine::query::OnToolLifecycle>,
) -> Option<crate::engine::query::OnToolLifecycle> {
    let sink = sink.map(Arc::new);
    Some(Box::new(move |record| {
        let session = session.clone();
        let sink = sink.clone();
        Box::pin(async move {
            if let Some(sink) = sink {
                sink(record.clone()).await?;
            }
            if record.phase == crate::types::events::ToolLifecyclePhase::Cancelled {
                let mut index = session.turn_index.lock_recover();
                let cancelled = index
                    .turn_cancelled_tools
                    .entry(record.turn_id)
                    .or_default();
                if !cancelled.contains(&record.tool_use_id) {
                    cancelled.push(record.tool_use_id);
                }
            }
            Ok(())
        })
    }))
}

/// Group one directly-persisted message under the running turn.
async fn record_turn_message(session: &Session, message: &Message, turn_id: &RuntimeString) {
    session.capture_review_message(message, turn_id).await;
    index_turn_message(session, message, turn_id);
}

fn index_turn_message(session: &Session, message: &Message, turn_id: &RuntimeString) {
    if let Some(uuid) = message_uuid(message) {
        session
            .turn_index
            .lock_recover()
            .message_turn
            .insert(uuid.clone(), turn_id.clone());
    }
}

/// Model-visible guidance persisted when the user interrupts a turn.
///
/// The turn's aborted state alone tells the model nothing about what an
/// interrupt did and did not stop, and it keeps acting on stale assumptions:
/// waiting on a background shell that is still running, re-running a command
/// that already half-applied, or expecting a sub-agent that was just cancelled.
/// This is written as a durable message rather than injected at resume, so the
/// guidance reaches the model on the very next request AND survives a restart
/// (history replay carries it like any other message).
///
/// Every claim here is the interrupt path's actual behaviour, not a generic
/// warning:
///   - background shells keep running; only the turn's own cancellation tokens
///     are fired;
///   - a tool call or command that was in flight may have partially executed,
///     since cancellation cannot roll back a side effect;
///   - accepted sub-agents retain their independent task and mailbox lifetimes;
///   - an unfinished child creation may be cancelled before input acceptance.
fn user_interrupt_guidance_message(turn_id: &RuntimeString) -> Message {
    let text = RuntimeString::from(
        "The user interrupted the previous turn on purpose. Background shells started with runInBackground are still running and were not stopped by the interrupt: read their output with bash_output, and stop one with kill_shell when it is no longer wanted. Any tool call or command that was in flight may have partially executed, so check the current state before repeating it. Accepted sub-agents, including persistent teammates, continue independently. Stop a specific sub-agent explicitly when needed; an unfinished creation may have been cancelled before acceptance.",
    );
    Message::User(UserMessage {
        question_answer: None,
        // Unique per turn: a message uuid is the turn-attribution key, so two
        // interrupts in one session must not share an identity and cross-wire
        // their turn grouping on replay.
        uuid: RuntimeString::from("turn-interrupted-").concat(turn_id),
        content: vec![ContentBlock::Text(TextBlock {
            text,
            citations: None,
            harness_envelope: Some(crate::schemas::content::HarnessEnvelope::SystemReminder),
            message_id: None,
            phase: None,
        })],
        is_meta: Some(true),
    })
}

/// Build the host-only rendering record paired with one durable user message.
pub(crate) fn user_display_message(
    message_uuid: RuntimeString,
    metadata: &RuntimeJson,
    record_uuid: RuntimeString,
) -> Message {
    let mut envelope = RuntimeObject::new();
    envelope.insert("messageUuid", RuntimeJson::String(message_uuid));
    envelope.insert("localRender", metadata.clone());
    Message::System(SystemMessage {
        uuid: record_uuid,
        text: runtime_json::stringify(&RuntimeJson::Object(envelope)),
        subtype: SystemSubtype::UserDisplay,
    })
}

/// Rollout event discriminator for a turn's terminal state record: the renderer
/// reads these (alongside the `turn_context` boundary) to group history messages
/// by turn and to know whether each turn ended completed/interrupted/failed.
pub const TURN_LIFECYCLE_ROLLOUT_EVENT: &str = "turn_lifecycle";
/// Rollout event discriminator for ONE file edit this session performed. Written
/// per edit rather than as a running total so a resume can replay the same fold
/// the live session applied; a host asks for the folded set with
/// `get_file_changes`.
pub const FILE_CHANGE_ROLLOUT_EVENT: &str = "file_change";

/// Keep the complete edit event so read-only conversation inspection can serve
/// its original diff after the turn finishes or the session is archived.
fn file_change_rollout_payload(change: &crate::types::events::FileChange) -> Option<RuntimeJson> {
    crate::runtime_values::runtime_codec::encode_value(change).ok()
}
use crate::engine::session::is_session_shutdown_refusal;
use crate::goal::runtime::{GoalContinuation, GoalStopReason};
use crate::memory::agent_memory::build_agent_memory_prompt_text;
use crate::memory::agent_memory::AgentMemoryScope;
use crate::memory::snapshot_decision::AgentMemorySnapshotApplyError;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionAudit;
use crate::runtime_values::runtime_json;
use crate::runtime_values::RuntimeJson;
use crate::runtime_values::RuntimeObject;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
use crate::schemas::config::TotalTokensReminderMode;
use crate::schemas::content::ContentBlock;
use crate::schemas::content::TextBlock;
use crate::schemas::message::Message;
use crate::schemas::message::SystemMessage;
use crate::schemas::message::SystemSubtype;
use crate::schemas::message::Usage;
use crate::schemas::message::UserMessage;
use crate::schemas::rollout::SessionRole;
use crate::schemas::rollout::TurnContextSnapshot;
use crate::store::session_store::AppendEventOptions;
use crate::sync_ext::{MutexExt, RwLockExt};
use crate::tasks::task_store::get_task_list_id;
use crate::tools::builtins::tool_search::ensure_tool_search;
use crate::tools::pool::build_tool_pool;
use crate::tools::pool::to_tool_specs;
use crate::tools::pool::BuildPoolOptions;
use crate::types::errors::EngineError;
use crate::types::errors::EngineErrorCode;
use crate::types::errors::InputValidationError;
use crate::types::errors::TerminalReason;
use crate::types::errors::ToolError;
use crate::types::events::EngineEvent;
use crate::types::events::NotificationLevel;
use crate::types::events::ThreadItem;
use crate::types::file_state::FileStateCache;
use crate::types::json::runtime_schema_from_static;
use crate::types::json::Json;
use crate::types::permission::PermissionUpdate;
use crate::types::provider::SystemPromptPart;
use crate::types::provider::PROMPT_CACHE_COLD_AFTER_MS;
use crate::types::tool::RunBashOptions;
use crate::types::tool::ToolContext;
use async_stream::stream;
use futures::stream::StreamExt;
use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, RwLock};
use tokio_util::sync::CancellationToken;

/// Own the complete pending batch until history durably owns its original IDs.
struct PendingResumeBatch {
    queue: Arc<crate::engine::message_queue::MessageQueue>,
    items: Vec<Arc<QueuedMessage>>,
    content: Vec<Vec<ContentBlock>>,
}

impl Drop for PendingResumeBatch {
    fn drop(&mut self) {
        self.queue.release_pending_resume(&self.items);
    }
}

impl Session {
    async fn recover_portable_context(
        &self,
        messages: Vec<Message>,
    ) -> Result<Vec<Message>, RuntimeString> {
        if let Some(rollout) = self.persistence.rollout() {
            rollout.flush().await.map_err(|error| {
                RuntimeString::from(format!(
                    "Cannot flush local context before migration: {error}"
                ))
            })?;
        }
        let store = self.persistence.store().ok_or_else(|| {
            RuntimeString::from("Original local history is unavailable for this conversation.")
        })?;
        let mut snapshots = Vec::new();
        let mut thread = self
            .id
            .try_to_utf8()
            .map_err(|_| RuntimeString::from("The conversation identity is invalid."))?;
        let mut visited = std::collections::HashSet::new();
        for _ in 0..32 {
            if !visited.insert(thread.clone()) {
                return Err("The local context ancestry contains a cycle.".into());
            }
            let snapshot = store.read_thread(&thread).await.map_err(|error| {
                RuntimeString::from(format!("Cannot read original local context: {error}"))
            })?;
            // Side tasks and subagents link to their origin through the parent
            // thread while forks use the fork source; both inherit history whose
            // checkpoint archives live in the origin rollout.
            let parent = snapshot
                .meta
                .forked_from_id
                .clone()
                .or_else(|| snapshot.meta.parent_thread_id.clone())
                .and_then(|id| id.try_to_utf8().ok());
            snapshots.push(snapshot.raw_items);
            match crate::store::portable_history::recover_portable_history(&messages, &snapshots) {
                Ok(restored) => return Ok(restored),
                Err(error) => match parent {
                    Some(parent) => thread = parent,
                    None => return Err(error),
                },
            }
        }
        Err("The local context ancestry exceeds the recovery limit.".into())
    }

    // ---- the turn loop ----

    pub fn runtime_capability_snapshot_for_turn(
        &self,
        opts: &TurnOptions,
    ) -> RuntimeCapabilityResolution {
        let selected = opts
            .selected_capability_roots
            .as_deref()
            .or(self.selected_capability_roots.as_deref());
        self.plugin_projection()
            .map(|projection| projection.runtime_capabilities.clone())
            .unwrap_or_else(|| self.runtime_capabilities.clone())
            .resolve_selected(selected)
    }

    /// Atomically claim one pending main-thread now/next batch for ONE turn.
    /// Caller must serialize this with foreground runs and supply its stop fence.
    pub async fn resume_pending_streamed(
        self: &Arc<Self>,
        opts: TurnOptions,
    ) -> Result<Option<Turn>, RuntimeString> {
        if self.meta.role == Some(SessionRole::SideTask) || self.meta.parent_thread_id.is_some() {
            return Err("pending continuation requires an ordinary main session".into());
        }
        if self.turn.is_active() {
            return Err("a foreground turn is still active".into());
        }
        if opts
            .signal
            .as_ref()
            .is_some_and(|signal| signal.is_cancelled())
        {
            return Err("pending continuation interrupted".into());
        }
        let queue = self.conversation.queue().clone();
        let mut batch = PendingResumeBatch {
            items: queue.reserve_pending_resume()?,
            content: Vec::new(),
            queue,
        };
        if batch.items.is_empty() {
            return Ok(None);
        }
        batch.queue.await_pending_durability().await?;
        let mut validated = self
            .validate_input_for_turn(Input::Parts(Vec::new()), &opts)
            .map_err(|error| RuntimeString::from(error.message))?;
        let mut media_count = 0usize;
        let mut media_encoded_bytes = 0usize;
        for item in &batch.items {
            let input = match &item.value {
                QueuedValue::Text(text) => Input::Text(text.clone()),
                QueuedValue::Blocks(blocks) => Input::Parts(
                    blocks
                        .iter()
                        .map(|block| match block {
                            ContentBlock::Text(text) => Ok(UserContentPart::Text {
                                text: text.text.clone(),
                            }),
                            ContentBlock::Image(image) => Ok(UserContentPart::Image {
                                base64: image.base64.clone(),
                                mime_type: image.mime_type.clone(),
                                source_path: image.source_path.clone(),
                            }),
                            ContentBlock::Document(document) => Ok(UserContentPart::Document {
                                base64: document.base64.clone(),
                                mime_type: document.mime_type.clone(),
                                filename: document.filename.clone(),
                            }),
                            ContentBlock::Media(media) => Ok(UserContentPart::Media {
                                base64: media.base64.clone(),
                                mime_type: media.mime_type.clone(),
                                filename: media.filename.clone(),
                            }),
                            _ => Err(RuntimeString::from(
                                "pending input contains unsupported user content",
                            )),
                        })
                        .collect::<Result<_, _>>()?,
                ),
            };
            let checked = self
                .validate_input_for_turn(input, &opts)
                .map_err(|error| RuntimeString::from(error.message))?;
            if let Input::Parts(parts) = &checked.input {
                for part in parts {
                    if let UserContentPart::Media { base64, .. } = part {
                        media_count += 1;
                        media_encoded_bytes = media_encoded_bytes
                            .checked_add(base64.len_utf16())
                            .ok_or_else(|| {
                            RuntimeString::from("pending media byte count overflow")
                        })?;
                    }
                }
                if media_count > crate::provider::native_media::MAX_MEDIA_INPUT_PARTS
                    || media_encoded_bytes
                        > crate::provider::native_media::MAX_MEDIA_INPUT_TOTAL_BYTES.div_ceil(3) * 4
                {
                    return Err("Pending input exceeds the native media limit; edit the queued batch before retrying".into());
                }
            }
            batch.content.push(to_content_blocks(&checked.input));
            validated.pdf_bytes = validated
                .pdf_bytes
                .checked_add(item.pdf_bytes)
                .ok_or_else(|| RuntimeString::from("pending PDF byte count overflow"))?;
            validated.pdf_documents = validated
                .pdf_documents
                .checked_add(item.pdf_documents)
                .ok_or_else(|| RuntimeString::from("pending PDF document count overflow"))?;
        }
        if validated.pdf_bytes > super::MAX_DIRECT_PDF_INPUT_BYTES
            || validated.pdf_documents > super::MAX_DIRECT_PDF_DOCUMENTS
        {
            return Err("Pending input exceeds the per-turn PDF limit; edit the queued batch before retrying".into());
        }
        if validated.pdf_bytes > 0 {
            self.validate_native_pdf_model(&validated.model)
                .map_err(|error| RuntimeString::from(error.message))?;
        }
        if opts
            .signal
            .as_ref()
            .is_some_and(|signal| signal.is_cancelled())
        {
            return Err("pending continuation interrupted".into());
        }
        Ok(Some(self.run_streamed_validated_with_pending(
            validated,
            opts,
            Some(batch),
        )))
    }

    /// Run a turn, returning its live engine-event stream.
    pub fn run_streamed(self: &Arc<Self>, input: Input, opts: TurnOptions) -> Turn {
        let Ok(_inference_activity) = self.begin_inference_activity() else {
            return self.defer_model_update_turn(input, opts, None);
        };
        let validated = match self.validate_input_for_turn(input, &opts) {
            Ok(input) => input,
            Err(error) => {
                let engine_error = EngineError {
                    code: EngineErrorCode::InvalidInput,
                    message: error.message.clone().into(),
                    cause: Some(Box::new(error)),
                    request_id: None,
                    provider_kind: None,
                    retry_after_ms: None,
                };
                let rejected = self.clone();
                let events = stream! {
                    let event = EngineEvent::TurnFailed { inference: None,
                        error: engine_error
                    };
                    if let Some(observer) = &rejected.observer {
                        observer.lock_recover().on_event(&event);
                    }
                    rejected.log_event(&event);
                    yield event;
                };
                return Turn {
                    events: Box::pin(events),
                };
            }
        };
        self.run_streamed_validated(validated, opts)
    }

    /// Start a turn from the opaque validation result used by protocol
    /// boundaries. This prevents a second base64 decode between pre-checkpoint
    /// validation and core persistence.
    ///
    /// Each call owns one real turn. Goal continuations return to the ordinary
    /// admission queue before another turn can start.
    pub fn run_streamed_validated(
        self: &Arc<Self>,
        validated: ValidatedInput,
        opts: TurnOptions,
    ) -> Turn {
        self.run_streamed_validated_with_pending(validated, opts, None)
    }

    fn run_streamed_validated_with_pending(
        self: &Arc<Self>,
        validated: ValidatedInput,
        opts: TurnOptions,
        pending_resume: Option<PendingResumeBatch>,
    ) -> Turn {
        self.run_goal_coordinated(validated, opts, pending_resume, None)
    }

    /// Admit saved goal work through the same turn boundary as explicit input.
    pub async fn run_goal_idle_streamed(
        self: &Arc<Self>,
        opts: TurnOptions,
    ) -> Result<Option<Turn>, RuntimeString> {
        self.run_goal_idle_streamed_for(None, opts).await
    }

    /// A reserved successor may consume only the generation it reserved.
    pub async fn run_goal_idle_streamed_for(
        self: &Arc<Self>,
        expected: Option<&RuntimeString>,
        opts: TurnOptions,
    ) -> Result<Option<Turn>, RuntimeString> {
        if self.collaboration_mode() == crate::planning::CollaborationMode::Plan
            || opts
                .goal_pending_input
                .as_ref()
                .is_some_and(|pending| pending())
            || self.goal_idle_has_pending_work()
        {
            return Ok(None);
        }
        let Some(goal) = self.goal_runtime() else {
            return Ok(None);
        };
        goal.set_progress_tool_enabled(
            self.current_tools()
                .iter()
                .any(|tool| tool.name() == "update_plan"),
        );
        match goal
            .continuation_if_idle(self.has_pending_user_prompt())
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?
        {
            GoalContinuation::Stop => Ok(None),
            GoalContinuation::Continue { goal, prompt } => {
                if expected.is_some_and(|expected| expected != &goal.goal_id) {
                    return Ok(None);
                }
                let opts = continuation_turn_options(&opts);
                let input = self
                    .validate_input_for_turn(Input::Text(prompt.into()), &opts)
                    .map_err(|error| RuntimeString::from(error.message))?;
                Ok(Some(self.run_goal_coordinated(
                    input,
                    opts,
                    None,
                    Some(goal.goal_id),
                )))
            }
        }
    }

    fn goal_idle_has_pending_work(&self) -> bool {
        self.turn.is_active()
            || self.planning.snapshot().is_ok_and(|snapshot| {
                snapshot
                    .implementation
                    .is_some_and(|receipt| receipt.admission == "pending")
            })
            || !self.pending_questions().is_empty()
            || self
                .goal_runtime()
                .is_some_and(|goal| goal.has_descendant_work())
    }

    fn run_goal_coordinated(
        self: &Arc<Self>,
        validated: ValidatedInput,
        opts: TurnOptions,
        pending_resume: Option<PendingResumeBatch>,
        expected_goal: Option<RuntimeString>,
    ) -> Turn {
        if opts.permission_mode_override.is_some() {
            return self.rejected_input_turn(InputValidationError::new(
                crate::types::errors::InputValidationErrorCode::UnsupportedInput,
                "Per-turn permissionMode is retired; update the session runtime policy instead.",
            ));
        }
        let Some(goal) = self.goal_runtime() else {
            return self.run_one_turn_streamed_validated(validated, opts, pending_resume, None);
        };
        let this = self.clone();
        let events = stream! {
            let captured_mode=this.collaboration_mode();
            let executing=captured_mode!=crate::planning::CollaborationMode::Plan;
            let automatic=expected_goal.is_some();
            if automatic&&opts.goal_pending_input.as_ref().is_some_and(|pending|pending()){return;}
            if let Err(error)=goal.on_turn_start_for(automatic,expected_goal.as_ref(),executing).await {
                if error.kind()!=std::io::ErrorKind::Interrupted {yield EngineEvent::TurnFailed{inference:None,error:super::internal_err(&format!("goal turn admission failed: {error}"))};}
                return;
            }
            #[cfg(any(test, feature="testing"))]
            goal.testing_note_phase("goal.admitted");
            let turn_epoch=goal.callback_epoch();
            let mut settlement=goal.turn_guard(turn_epoch);
            let mut failure=None;
            let mut aborted=false;
            {
                let mut events=this.run_one_turn_streamed_validated(validated,opts,pending_resume,Some(captured_mode)).events;
                while let Some(event)=events.next().await {
                    goal.note_event(&event);
                    match &event {
                        EngineEvent::TurnFailed{error,..}=>failure=Some(if error.provider_kind==Some(crate::types::errors::ProviderErrorKind::Quota){GoalStopReason::UsageLimit}else{GoalStopReason::TurnError}),
                        EngineEvent::TurnCompleted{stop_reason,..}=>{aborted=*stop_reason==TerminalReason::Aborted;if !matches!(stop_reason,TerminalReason::Completed|TerminalReason::Aborted){failure=Some(GoalStopReason::TurnError);}},
                        _=>{}
                    }
                    if let Some(reason)=failure {settlement.note_terminal(crate::goal::runtime::GoalTurnSettlement::Failed(reason));}
                    else if matches!(&event,EngineEvent::TurnCompleted{..}) {settlement.note_terminal(if aborted {crate::goal::runtime::GoalTurnSettlement::Aborted}else{crate::goal::runtime::GoalTurnSettlement::Completed});}
                    yield event;
                }
            }
            let settled=settlement.finish().await;
            if let Err(error)=settled {yield EngineEvent::TurnFailed{inference:None,error:super::internal_err(&format!("goal turn settlement failed: {error}"))};}
            let next_mode=this.collaboration_mode();
            if next_mode!=captured_mode {
                if let Err(error)=goal.synchronize_execution_mode(next_mode!=crate::planning::CollaborationMode::Plan).await {yield EngineEvent::TurnFailed{inference:None,error:super::internal_err(&format!("goal planning boundary failed: {error}"))};}
            }
            // Return the foreground owner after this real turn. The facade's
            // ordinary admission queue arbitrates the next idle event.
        };
        Turn {
            events: Box::pin(events),
        }
    }

    /// Run exactly ONE turn from an already-validated input.
    fn run_one_turn_streamed_validated(
        self: &Arc<Self>,
        validated: ValidatedInput,
        mut opts: TurnOptions,
        pending_resume: Option<PendingResumeBatch>,
        captured_mode: Option<crate::planning::CollaborationMode>,
    ) -> Turn {
        crate::schemas::effort::normalize_legacy_ultra(&mut opts.effort, &mut opts.ultra_mode);
        let this = self.clone();
        let Ok(inference_activity) = this.begin_inference_activity_for(opts.model.as_ref()) else {
            return this.defer_model_update_turn(validated.input, opts, pending_resume);
        };
        let initial_bundle = inference_activity.bundle.clone();
        let advisor_enabled = initial_bundle.advisor_target.is_some();
        let turn_model = opts
            .model
            .clone()
            .or_else(|| this.pending_model())
            .unwrap_or_else(|| initial_bundle.config.model.clone());
        let inference_slot: super::InferenceSlot = Arc::new(Mutex::new(None));
        let resolved_reviewer = Arc::new(RwLock::new(None));
        let inference_capture = crate::engine::query::InferenceCapture {
            slot: inference_slot.clone(),
            next: Some({
                let session = this.clone();
                let model_override = opts.model.clone();
                let reviewer = resolved_reviewer.clone();
                Arc::new(move |signal| {
                    let session = session.clone();
                    let model_override = model_override.clone();
                    let reviewer = reviewer.clone();
                    Box::pin(async move {
                        let capture = session
                            .capture_inference_for(&signal, model_override.as_ref())
                            .await?;
                        let requirements = capture
                            .bundle
                            .load_review_requirements()
                            .await
                            .map_err(|error| error.to_string())?;
                        let policy = session.resolve_managed_step_policy(
                            &requirements,
                            &capture.bundle.config.model,
                        )?;
                        *reviewer.write_recover() = policy.approvals_reviewer;
                        session
                            .consume_transport_prewarm_for(capture.bundle.revision)
                            .await;
                        Ok(capture)
                    })
                })
            }),
            model_override: opts.model.clone(),
            effort_override: opts.effort.clone(),
            ultra_override: opts.ultra_mode,
        };
        // The turn identity is minted up front so every turn-scoped artifact this
        // run produces (the rollout turn_context boundary, the query params the
        // provider loop sees, and the closing turn lifecycle record) shares ONE
        // id. The renderer groups history messages by it.
        let turn_id = RuntimeString::from((this.deps.uuid)());
        // Open this turn's volume measurement: from here, each file's added/removed
        // is measured against what the turn FIRST finds there, so editing one file
        // repeatedly does not charge the same lines twice and the previous turn's
        // volume does not carry over.
        this.file_state.begin_turn();
        let validated = if !validated.provider_revision.matches(&initial_bundle)
            || validated.model != turn_model
        {
            match this.validate_input_for_bundle(validated.input, &turn_model, &initial_bundle) {
                Ok(validated) => validated,
                Err(error) => return this.rejected_input_turn(*error),
            }
        } else {
            validated
        };
        let input_pdf_bytes = validated.pdf_bytes;
        let input_pdf_documents = validated.pdf_documents;
        let input = validated.input;
        let abort = match &opts.signal {
            Some(s) => s.child_token(),
            None => CancellationToken::new(),
        };
        let turn_lease = match ActiveTurnLease::try_new_admitted(
            this.clone(),
            turn_model.clone(),
            abort.clone(),
            inference_activity,
        ) {
            Ok(lease) => lease,
            Err(TurnAdmissionError::Closed) => return this.rejected_closed_turn(),
            Err(TurnAdmissionError::GenerationExhausted) => {
                return this.rejected_input_turn(InputValidationError::new(
                    crate::types::errors::InputValidationErrorCode::UnsupportedInput,
                    "turn admission counter is exhausted",
                ));
            }
            Err(TurnAdmissionError::Busy) => {
                return this.rejected_input_turn(InputValidationError::new(
                    crate::types::errors::InputValidationErrorCode::UnsupportedInput,
                    "context compaction is in progress: retry after it completes",
                ));
            }
            Err(TurnAdmissionError::PluginRefreshPending) => {
                return this.rejected_input_turn(InputValidationError::new(
                    crate::types::errors::InputValidationErrorCode::UnsupportedInput,
                    "plugin update pending: retry this turn after the active turn ends",
                ));
            }
        };
        // Capture the live-model-override slot that set_active_turn_model will
        // write into; the query loop drains it before each LLM request.
        let live_model_override = this
            .turn
            .active_stack
            .lock_recover()
            .iter_mut()
            .find(|state| state.generation == turn_lease.generation)
            .map(|state| {
                state.turn_id = Some(turn_id.clone());
                state.live_model_override.clone()
            });
        let mut admission_records =
            vec![this
                .admit_prediction_source_checkpoint(turn_lease.generation, Some(turn_id.clone()))];
        this.async_questions
            .open(turn_id.clone(), turn_lease.generation, abort.clone());
        let question_generation = turn_lease.generation;
        // A validated, admitted turn closes Setup init immediately. The
        // receipt owns its append+flush independently from this Turn stream;
        // a Setup attempt that acquired the serial gate first is still allowed
        // to finish before SessionStart.
        let setup_init_close_receipt = this.close_setup_init_for_run();
        let turn_started_at_ms = (this.deps.now)();
        let automatic = this
            .goal_runtime()
            .is_some_and(|goal| goal.is_automatic_turn());
        let allow_mcp_form_input = !opts.meta_input
            && !automatic
            && this.meta.parent_thread_id.is_none()
            && (this.meta.role.is_none() || this.meta.role == Some(SessionRole::Main))
            && this.config.task_resource_scope.is_none();
        let turn_goal_metadata = super::TurnGoalMetadata {
            goal_continuation: automatic,
            input_origin: if automatic {
                "automatic".into()
            } else {
                "user".into()
            },
            goal_id: this
                .goal_runtime()
                .and_then(|goal| goal.request_identity().generation),
        };
        this.turn_index
            .lock_recover()
            .turn_goal_metadata
            .insert(turn_id.clone(), turn_goal_metadata.clone());
        if this.persistence.rollout().is_some() {
            if let Ok(payload) =
                crate::runtime_values::runtime_codec::encode_value(&turn_goal_metadata)
            {
                admission_records.push(crate::schemas::rollout::RolloutItem::Event(
                    crate::schemas::rollout::EventItem {
                        event: "turn_goal_metadata".into(),
                        turn_id: Some(turn_id.clone()),
                        payload: Some(payload),
                    },
                ));
            }
        }

        {
            let mut index = this.turn_index.lock_recover();
            index
                .turn_started_at_ms
                .insert(turn_id.clone(), turn_started_at_ms);
        }
        if let Some(goal) = this.goal_runtime().filter(|goal| goal.is_submission_turn()) {
            if let Some(id) = goal.take_submission_message() {
                let display = id.concat(&"-display".into());
                {
                    let mut index = this.turn_index.lock_recover();
                    index.message_turn.insert(id.clone(), turn_id.clone());
                    index.message_turn.insert(display.clone(), turn_id.clone());
                }
                if this.persistence.rollout().is_some() {
                    let mut payload = RuntimeObject::new();
                    payload.insert("messageUuid", RuntimeJson::String(id));
                    payload.insert("displayUuid", RuntimeJson::String(display));
                    admission_records.push(crate::schemas::rollout::RolloutItem::Event(
                        crate::schemas::rollout::EventItem {
                            event: "goal_submission_bound".into(),
                            turn_id: Some(turn_id.clone()),
                            payload: Some(RuntimeJson::Object(payload)),
                        },
                    ));
                }
            }
        }
        // Capture all provenance from one policy generation.
        if this.persistence.rollout().is_some() {
            let policy = this.runtime_policy_snapshot();
            admission_records.push(crate::schemas::rollout::RolloutItem::TurnContext {
                ctx: TurnContextSnapshot {
                    cwd: this.cwd_now().into_runtime_string(),
                    model: turn_model.clone(),
                    permission_mode: crate::types::permission::ExternalPermissionMode::Default,
                    turn_id: Some(turn_id.clone()),
                    started_at_ms: Some(turn_started_at_ms),
                    approval_policy: policy.state.approval_policy,
                    sandbox_mode: policy.state.sandbox_mode,
                    approvals_reviewer: policy.state.approvals_reviewer,
                    network_allowed: Some(policy.state.network_allowed),
                    writable_roots: Some(
                        this.permission_context_for_policy(&policy)
                            .additional_writable_roots
                            .iter()
                            .map(|root| root.as_runtime_string().clone())
                            .collect(),
                    ),
                },
            });
        }
        let input_text = match &input {
            Input::Text(s) => Some(s.clone()),
            Input::Parts(parts) => parts.first().and_then(|part| match part {
                UserContentPart::Text { text } => Some(text.clone()),
                _ => None,
            }),
        };
        let structured_input = matches!(&input, Input::Parts(_));
        let user_uuid = (this.deps.uuid)();
        // Retain the original input; the captured model gates only its request view.
        let user_content = to_content_blocks(&input);
        // A turn started with NO input is a continuation: the client asks the model
        // to pick the thread back up after an interrupt, without adding a message.
        // Recording an empty user message for it would put a contentless turn into
        // history and the rollout, which each provider mapper then has to guess at
        // (Anthropic drops it, the OpenAI mappers forward an empty content array).
        let has_user_input = !user_content.is_empty();
        let ordinary_goal_input = has_user_input && !opts.meta_input;
        let display_msg = opts.display_metadata.as_ref().map(|metadata| {
            user_display_message(
                user_uuid.clone().into(),
                metadata,
                (this.deps.uuid)().into(),
            )
        });
        if this
            .goal
            .as_ref()
            .is_some_and(|goal| goal.is_automatic_turn())
        {
            if let Some(goal) = &this.goal {
                let wrapup = goal.active_generation().is_some() && goal.is_budget_wrapup_turn();
                goal.register_context_input(user_uuid.clone().into(), wrapup);
            }
        }
        let user_msg = UserMessage {
            question_answer: None,
            uuid: user_uuid.clone().into(),
            content: user_content.clone(),
            is_meta: opts.meta_input.then_some(true),
        };
        if has_user_input {
            if let Some(record) =
                this.capture_review_message_record(&Message::User(user_msg.clone()), &turn_id)
            {
                admission_records.push(record);
            }
            index_turn_message(&this, &Message::User(user_msg.clone()), &turn_id);
            this.push_history(Message::User(user_msg.clone()));
            if let Some(display_msg) = &display_msg {
                if let Some(record) = this.capture_review_message_record(display_msg, &turn_id) {
                    admission_records.push(record);
                }
                index_turn_message(&this, display_msg, &turn_id);
                this.push_history(display_msg.clone());
            }
        }
        if let Err(error) = this.persistence.retain_admission_records(admission_records) {
            return this.rejected_input_turn(InputValidationError::new(
                crate::types::errors::InputValidationErrorCode::QueueUnavailable,
                format!("turn persistence admission failed: {error}"),
            ));
        }
        let turn_collaboration_mode = match captured_mode {
            Some(mode) => this.planning.begin_turn_in_mode(turn_id.clone(), mode),
            None => this.planning.begin_turn(turn_id.clone()),
        };
        // Goal metadata controls remain callable in either collaboration mode.
        // The captured mode still owns pursuit admission and accounting.
        let turn_goal = this.goal_runtime();
        if turn_collaboration_mode == crate::planning::CollaborationMode::Plan {
            if let Some(goal) = &turn_goal {
                goal.freeze_clock_for_planning();
            }
        }
        let turn_runtime_capabilities = super::SubagentCapabilityCapture::capture(
            this.runtime_capability_snapshot_for_turn(&opts),
        );
        let turn_browser_runtime = turn_runtime_capabilities
            .capabilities()
            .browser_runtime
            .clone();
        let turn_browser_tool_mapping = turn_runtime_capabilities
            .capabilities()
            .browser_tool_mapping
            .clone();
        let turn_lsp_manager = this.current_lsp_manager();

        // Web-search selection.
        let web_selection = select_web_search(
            ensure_tool_search(build_tool_pool(BuildPoolOptions {
                builtins: this.current_tools(),
                mcp_tools: {
                    let tools =
                        Session::mcp_tools_from_clients(&this.plugin_mcp_clients_snapshot());
                    if tools.is_empty() {
                        None
                    } else {
                        Some(tools)
                    }
                },
                plugin_tools: None,
            })),
            Some(&initial_bundle.config.web_search),
            initial_bundle.provider.capabilities(&turn_model).web_search,
        );
        let mut available_tools = web_selection.tools;
        available_tools.retain(|tool| advisor_enabled || tool.name() != "advisor");
        crate::planning::tools::retain_for_mode(
            &mut available_tools,
            turn_collaboration_mode,
            this.meta.role.is_none() || this.meta.role == Some(SessionRole::Main),
            this.config.request_user_input_default,
        );
        if let Some(definition) = &this.root_agent_definition {
            definition.retain_tools(&mut available_tools);
        }
        this.prepare_subagent_tool_inventory(&mut available_tools, &initial_bundle.config);
        let tools = if initial_bundle.provider.capabilities(&turn_model).tools {
            available_tools.clone()
        } else {
            Vec::new()
        };
        if let Some(goal) = &turn_goal {
            goal.set_progress_tool_enabled(tools.iter().any(|tool| tool.name() == "update_plan"));
        }
        let refresh_tools_async: Option<crate::engine::query::AsyncRefreshTools> = {
            let this = this.clone();
            let slot = inference_slot.clone();
            let initial = initial_bundle.clone();
            let model_override = opts.model.clone();
            Some(Box::new(move || {
                let this = this.clone();
                let bundle = Session::bundle_from_slot(&slot, &initial);
                let turn_model = model_override
                    .clone()
                    .unwrap_or_else(|| bundle.config.model.clone());
                Box::pin(async move {
                    let refresh_system_prompt = this.reconcile_plugin_mcp_for_step().await?;
                    let mut base = this
                        .host_refresh_tools
                        .as_ref()
                        .map(|refresh| refresh())
                        .unwrap_or_else(|| this.current_tools());
                    this.prepare_planning_tool_inventory(&mut base);
                    this.prepare_subagent_tool_inventory(&mut base, &bundle.config);
                    let plugin_mcp_tools =
                        Session::mcp_tools_from_clients(&this.plugin_mcp_clients_snapshot());
                    let mut tools = select_web_search(
                        ensure_tool_search(build_tool_pool(BuildPoolOptions {
                            builtins: base,
                            mcp_tools: if plugin_mcp_tools.is_empty() {
                                None
                            } else {
                                Some(plugin_mcp_tools)
                            },
                            plugin_tools: None,
                        })),
                        Some(&bundle.config.web_search),
                        bundle.provider.capabilities(&turn_model).web_search,
                    )
                    .tools;
                    tools.retain(|tool| advisor_enabled || tool.name() != "advisor");
                    crate::planning::tools::retain_for_mode(
                        &mut tools,
                        turn_collaboration_mode,
                        this.meta.role.is_none() || this.meta.role == Some(SessionRole::Main),
                        this.config.request_user_input_default,
                    );
                    if let Some(definition) = &this.root_agent_definition {
                        definition.retain_tools(&mut tools);
                    }
                    let tools = if bundle.provider.capabilities(&turn_model).tools {
                        tools
                    } else {
                        Vec::new()
                    };
                    Ok(crate::engine::query::AsyncToolRefresh {
                        tools,
                        refresh_system_prompt,
                    })
                })
            }))
        };

        let permission = this.permission_context();

        // Base tool-context factory (mints a fresh ToolContext per tool call).
        let task_plan = crate::tasks::task_store::TaskPlanBinding::capture(&this.task_list_id());
        let mut goal_identity = turn_goal
            .as_ref()
            .map(|goal| goal.request_identity())
            .unwrap_or_default();
        if turn_collaboration_mode == crate::planning::CollaborationMode::Plan {
            goal_identity.accounting_enabled = false;
            goal_identity.goal_work = false;
        }
        let goal_generation = Arc::new(Mutex::new(goal_identity));
        let tool_context_base: ToolContextFactory = {
            let goal_generation = goal_generation.clone();
            let turn_goal = turn_goal.clone();
            let this = this.clone();
            let abort = abort.clone();
            let question_turn_id = turn_id.clone();
            // Spawn-bearing tools stamp this onto detached children so a late
            // completion receipt still settles into the turn that started the
            // child, even when the child finishes long after it closed.
            let spawn_turn_id = turn_id.clone();
            let slot = inference_slot.clone();
            let initial = initial_bundle.clone();
            let model_override = opts.model.clone();
            let turn_runtime_capabilities = turn_runtime_capabilities.clone();
            let turn_browser_runtime = turn_browser_runtime.clone();
            let turn_browser_tool_mapping = turn_browser_tool_mapping.clone();
            let turn_lsp_manager = turn_lsp_manager.clone();
            let resolved_reviewer = resolved_reviewer.clone();
            Arc::new(move || {
                let policy = this.runtime_policy_snapshot();
                let (permission, permission_origin) =
                    this.permission_context_for_policy_with_origin(&policy);
                let mut ctx = bare_tool_context(
                    this.process_context.clone(),
                    this.background_processes.clone(),
                    this.cwd_now(),
                    abort.clone(),
                    this.file_state.clone() as Arc<dyn FileStateCache>,
                    permission.clone(),
                    policy.sandbox_backend.clone(),
                );
                {
                    let session = this.clone();
                    let turn = question_turn_id.clone();
                    ctx.apply_skill_context = Some(Box::new(move |change| {
                        if let Some(path) = change.invoked_skill_path {
                            if let Some(content) = change.invoked_skill_content {
                                session.record_invoked_skill_content(None, &path, &content);
                            }
                            session.record_invoked_skill(None, &turn, &path);
                        }
                    }));
                }
                ctx.planning_read_only =
                    turn_collaboration_mode == crate::planning::CollaborationMode::Plan;
                policy.apply_to_context(&mut ctx);
                if let Some(reviewer) = *resolved_reviewer.read_recover() {
                    ctx.approvals_reviewer = Some(reviewer);
                }
                {
                    let this_e = this.clone();
                    ctx.enqueue_background_notice = Some(Arc::new(move |text: RuntimeString| {
                        if this_e.lifecycle.is_disposed() {
                            return;
                        }
                        let rendered = this_e.render_deferred_notice(&text);
                        let _ = this_e.conversation.queue().enqueue_pending_notification(
                            QueuedMessage {
                                trigger_turn: None,
                                value: QueuedValue::Blocks(vec![ContentBlock::Text(TextBlock {
                                    text: rendered,
                                    citations: None,
                                    harness_envelope: Some(
                                        crate::schemas::content::HarnessEnvelope::SystemReminder,
                                    ),
                                    message_id: None,
                                    phase: None,
                                })]),
                                mode: "task-notification".into(),
                                is_meta: Some(true),
                                origin: Some("background-shell".into()),
                                priority: Some(QueuePriority::Next),
                                uuid: Some((this_e.deps.uuid)().into()),
                                session_id: Some(this_e.id.clone()),
                                agent_id: None,
                                correlation_id: None,
                                pdf_bytes: 0,
                                pdf_documents: 0,
                                delivery: None,
                                display_metadata: None,
                            },
                        );
                    }));
                }
                if let Some(sink) = this.persistence.rollout().cloned() {
                    let record: crate::types::tool::RecordRolloutEventFn =
                        Arc::new(move |event: RuntimeString, payload: Option<Json>| {
                            let sink = sink.clone();
                            Box::pin(async move {
                                let prepared = sink.prepare_append().await.map_err(|error| {
                                    ToolError::fatal(format!(
                                        "Tool trace persistence admission failed: {error}"
                                    ))
                                })?;
                                prepared.commit(crate::schemas::rollout::RolloutItem::Event(
                                    crate::schemas::rollout::EventItem {
                                        event,
                                        turn_id: None,
                                        payload,
                                    },
                                ));
                                Ok(())
                            })
                        });
                    ctx.record_rollout_event = Some(record);
                }
                let bundle = Session::bundle_from_slot(&slot, &initial);
                ctx.advisor = advisor_enabled.then(|| crate::types::tool::AdvisorContext {
                    request: None,
                    query: {
                        let session = this.clone();
                        let mut captured = (*bundle).clone();
                        captured.advisor_target = initial.advisor_target.clone();
                        let bundle = Arc::new(captured);
                        Arc::new(move |question, request, signal| {
                            let session = session.clone();
                            let bundle = bundle.clone();
                            Box::pin(async move {
                                session
                                    .run_advisor_query(bundle, request, question, signal)
                                    .await
                            })
                        })
                    },
                });
                let turn_model = model_override
                    .clone()
                    .unwrap_or_else(|| bundle.config.model.clone());
                let turn_caps = bundle.provider.capabilities(&turn_model);
                ctx.native_pdf = Some(turn_caps.native_pdf);
                ctx.vision = Some(turn_caps.vision);
                if this.config.shell_env_capture || this.shell_env.get().is_some() {
                    let this_s = this.clone();
                    ctx.shell_env = Some(Arc::new(move || {
                        let this_s = this_s.clone();
                        Box::pin(async move { this_s.resolve_shell_env().await })
                    }));
                }
                {
                    let this_a = this.clone();
                    ctx.activate_skill_paths = Some(Box::new(move |paths: Vec<RuntimePath>| {
                        let cwd = this_a.cwd_now();
                        this_a.activate_skill_paths(&paths, &cwd);
                        this_a.feed_nested_memory_triggers(
                            this_a.memory_roots.nested_triggers(),
                            &paths,
                            &cwd,
                        );
                    }));
                }
                {
                    let this_b = this.clone();
                    let perm = permission.clone();
                    let sig = abort.clone();
                    ctx.run_bash = Some(Arc::new(
                        move |command: String, run_opts: Option<RunBashOptions>| {
                            let this_b = this_b.clone();
                            let perm = perm.clone();
                            let sig = sig.clone();
                            let cwd = this_b.cwd_now();
                            Box::pin(async move {
                                this_b
                                    .run_skill_bash(
                                        command,
                                        perm,
                                        runtime_path_to_skill_shell_host(&cwd),
                                        sig,
                                        run_opts,
                                        turn_collaboration_mode
                                            == crate::planning::CollaborationMode::Plan,
                                    )
                                    .await
                            })
                        },
                    ));
                }
                ctx.task_plan = Some(task_plan.clone());
                ctx.depth = Some(0);
                if this.config.forward_subagent_text {
                    ctx.forward_subagent_text = Some(true);
                }
                // Hand the spawning step's frozen capture to any sub-agent this
                // step produces, so a child of step A keeps A's bundle even when
                // its first poll lands after B is published or the catalog
                // refreshes. The slot holds this step's admitted guard.
                let parent_capture = slot.lock_recover().clone();
                ctx.spawn_sub_agent = Some(this.make_spawn_sub_agent_with_permission_origin(
                    0,
                    abort.clone(),
                    permission.clone(),
                    {
                        let s = this.clone();
                        Arc::new(move || s.history_snapshot())
                    },
                    false,
                    None,
                    Some(task_plan.fork()),
                    parent_capture.clone(),
                    Some(spawn_turn_id.clone()),
                    Some(permission_origin.clone()),
                    Some(turn_runtime_capabilities.clone()),
                    Some(turn_collaboration_mode),
                ));
                ctx.spawn_background_agent =
                    Some(this.make_spawn_background_agent_with_permission_origin(
                        0,
                        abort.clone(),
                        Some(spawn_turn_id.clone()),
                        permission.clone(),
                        {
                            let s = this.clone();
                            Arc::new(move || s.history_snapshot())
                        },
                        false,
                        None,
                        Some(task_plan.fork()),
                        parent_capture.clone(),
                        Some(permission_origin),
                        Some(turn_runtime_capabilities.clone()),
                        Some(turn_collaboration_mode),
                    ));
                if this.has_workflow_tool {
                    ctx.spawn_workflow_agent = Some(this.make_spawn_workflow_agent(
                        0,
                        abort.clone(),
                        permission.clone(),
                        None,
                        Some(task_plan.fork()),
                        parent_capture,
                        Some(spawn_turn_id.clone()),
                        turn_runtime_capabilities.clone(),
                        turn_collaboration_mode,
                    ));
                }
                ctx.workflow_journal = this.workflow_journal();
                ctx.swarm = this.swarm_controller(this.main_swarm_cell());
                ctx.cron = this.cron_controller();
                ctx.goal = turn_goal.clone();
                ctx.goal_generation = Some(goal_generation.clone());
                if let Some(lsp) = &turn_lsp_manager {
                    ctx.lsp = Some(lsp.controller());
                }
                ctx.browser = turn_browser_runtime.clone();
                ctx.browser_tool_mapping = turn_browser_tool_mapping.clone();
                {
                    let clients = this.all_mcp_resource_clients_snapshot();
                    ctx.mcp_clients = Some(Box::new(move || clients.clone()));
                }
                ctx.background_tasks = Some(this.background_task_registry_in_context(
                    None,
                    Some(super::SubagentResumeContext {
                        mcp: this.current_subagent_mcp_capture(),
                        bundle: bundle.clone(),
                        permission: ctx.permission.clone(),
                        runtime_policy: policy.clone(),
                        cwd: ctx.cwd.clone(),
                        runtime_capabilities: turn_runtime_capabilities.clone(),
                    }),
                ));
                let team = this
                    .swarm_identity
                    .lock_recover()
                    .as_ref()
                    .map(|i| i.team.clone());
                ctx.task_list_id = Some(get_task_list_id(Some(&this.id), team.as_ref()));
                task_plan.observe(ctx.task_list_id.as_ref().unwrap());
                ctx.run_hook = this.make_run_hook();
                ctx.worktree = this.worktree_controller();
                // Preserve a bound document restored from historical session data.
                ctx.plan_file = this.plan_file_context(None);
                ctx.elicit = this.elicit_handler.clone();
                ctx.async_question = Some(
                    this.async_question_context(question_turn_id.clone(), question_generation),
                );
                ctx
            })
        };

        // Per-turn system prompt.
        let advisor_exposed = tools.iter().any(|tool| tool.name() == "advisor");
        let subagents_exposed = tools.iter().any(|tool| tool.name() == "task");
        let system_prompt: SystemPromptThunk = {
            let this = this.clone();
            let slot = inference_slot.clone();
            let initial = initial_bundle.clone();
            let model_override = opts.model.clone();
            let append = opts.system_prompt_append.clone();
            Arc::new(move || {
                let this = this.clone();
                let bundle = Session::bundle_from_slot(&slot, &initial);
                let turn_model = model_override
                    .clone()
                    .unwrap_or_else(|| bundle.config.model.clone());
                let append = append.clone();
                Box::pin(async move {
                    let base_provider = this.system_prompt_provider.read_recover().clone();
                    let mut parts = (base_provider)().await;
                    if subagents_exposed {
                        parts.push(crate::types::provider::SystemPromptPart {
                            name: "subagent-model-selection".into(),
                            text: crate::tools::builtins::task::model_selection_description(
                                bundle.config.subagent_model_catalog,
                                &bundle.config.subagent_model_options,
                            ),
                            cacheable: Some(true),
                            cache_scope: None,
                        });
                    }
                    if advisor_exposed {
                        let boundary = parts
                            .iter()
                            .position(|part| {
                                part.cache_scope != Some(crate::types::provider::CacheScope::Global)
                            })
                            .unwrap_or(parts.len());
                        parts.insert(boundary, crate::context::system_prompt::advisor_guidance());
                    }
                    if this.system_prompt_is_default {
                        let is_wt = this.workspace.is_in_worktree();
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
                                        is_worktree: is_wt,
                                        windows_shell: None,
                                    },
                                );
                            }
                        }
                    }
                    if let Some(part) = this.plugin_mcp_instruction_part().await {
                        parts.push(part);
                    }
                    if let Some(definition) = &this.root_agent_definition {
                        for (name, text) in [
                            (
                                "agent-role",
                                definition
                                    .prompt
                                    .as_ref()
                                    .or(definition.system_prompt.as_ref()),
                            ),
                            (
                                "agent-critical",
                                definition.critical_system_reminder.as_ref(),
                            ),
                        ] {
                            if let Some(text) = text {
                                parts.push(SystemPromptPart {
                                    name: name.into(),
                                    text: text.clone(),
                                    cacheable: Some(true),
                                    cache_scope: None,
                                });
                            }
                        }
                    }
                    if let Some(dir) = this.memory_roots.main() {
                        if let Some(host) = &this.agent_memory_host {
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
                    if this.meta.role.as_ref() == Some(&SessionRole::SideTask) {
                        parts.push(SystemPromptPart {
                            name: "side-task".into(),
                            text: SIDE_TASK_SYSTEM_PROMPT.into(),
                            cacheable: None,
                            cache_scope: None,
                        });
                    }
                    if let Some(a) = &append {
                        parts.push(SystemPromptPart {
                            name: "append".into(),
                            text: a.clone(),
                            cacheable: None,
                            cache_scope: None,
                        });
                    }
                    parts.retain(|part| part.name != "collaboration-mode");
                    parts.push(crate::planning::instructions::instruction_part(
                        &this.config,
                        turn_collaboration_mode,
                    ));
                    parts
                })
            })
        };

        // File mentions are expanded by the host before provider admission. Keep
        // that local preprocessing available even when a plain-chat model must
        // receive no tool schemas and cannot invoke tools itself.
        let read_tool = available_tools
            .iter()
            .find(|tool| tool.name() == "read")
            .cloned();

        let on_message: crate::engine::query::OnMessage = {
            let this = this.clone();
            let turn_id = turn_id.clone();
            Box::new(move |m: Message| {
                let this = this.clone();
                let turn_id = turn_id.clone();
                Box::pin(async move {
                    let context_update = this.turn.context_window_update_gate.lock().await;
                    let prepared = match this.persistence.rollout() {
                        Some(rollout) => Some(rollout.prepare_batch(5).await.map_err(|error| {
                            RuntimeString::from(format!(
                                "message persistence admission failed: {error}"
                            ))
                        })?),
                        None => None,
                    };
                    let mut records = Vec::with_capacity(5);
                    if let Some(record) = this.capture_review_message_record(&m, &turn_id) {
                        records.push(record);
                    }
                    this.push_history(m.clone());
                    // Group this message under the running turn so `get_history`
                    // can serve the renderer a turn id per message.
                    if let Some(uuid) = message_uuid(&m) {
                        this.turn_index
                            .lock_recover()
                            .message_turn
                            .insert(uuid.clone(), turn_id.clone());
                    }
                    if let Message::Assistant(assistant) = &m {
                        if let Some(usage) = &assistant.usage {
                            let mut window = this.turn.context_window_usage.lock_recover();
                            let previous = window.clone();
                            window.prefill_input_tokens.get_or_insert(
                                usage
                                    .input_tokens
                                    .saturating_add(usage.cache_read_input_tokens)
                                    .saturating_add(usage.cache_creation_input_tokens),
                            );
                            window.active_tokens = Some(tokens_from_usage(usage));
                            if *window != previous {
                                records.push(crate::schemas::rollout::RolloutItem::Event(
                                    crate::schemas::rollout::EventItem {
                                        event: crate::context::window_usage::WINDOW_USAGE_EVENT
                                            .into(),
                                        turn_id: None,
                                        payload: Some(crate::context::window_usage::encode(
                                            &window,
                                        )),
                                    },
                                ));
                            }
                        }
                        let has_tool_call = assistant
                            .content
                            .iter()
                            .any(|block| matches!(block, ContentBlock::ToolUse(_)));
                        // Stable answer text tagged as a partial answer may be
                        // followed by more output or tools, so on its own it
                        // never stamps the turn's final message.
                        let has_text = assistant.content.iter().any(|block| matches!(block,
                            ContentBlock::Text(text) if text.phase.as_ref().is_none_or(|phase| phase != "commentary" && phase != "partial_answer")));
                        let has_explicit_final = assistant.content.iter().any(|block| matches!(block,
                            ContentBlock::Text(text) if text.phase.as_ref().is_some_and(|phase| phase == "final_answer")));
                        let mut index = this.turn_index.lock_recover();
                        if has_explicit_final || (has_text && !has_tool_call) {
                            index
                                .turn_final_message
                                .insert(turn_id.clone(), assistant.uuid.clone());
                        }
                        drop(index);
                        // Restart the idle clock: a provider response just landed, so
                        // the prompt cache is warm as of now.
                        this.turn.mark_response_at_ms((this.deps.now)());
                        // Raise the long-horizon high-water mark behind the padded
                        // remaining-context countdown, so the figure can only descend.
                        if this.config.total_tokens_reminder
                            == TotalTokensReminderMode::PaddedCountdown
                        {
                            if let Some(usage) = &assistant.usage {
                                let spend = this.turn.padded_countdown_spend();
                                this.turn.advance_padded_countdown_high_water(
                                    spend.total_with(tokens_from_usage(usage)),
                                );
                                records.push(crate::schemas::rollout::RolloutItem::Event(crate::schemas::rollout::EventItem {
                                    event: crate::context::usage_checkpoint::CONTEXT_USAGE_ROLLOUT_EVENT.into(), turn_id: None,
                                    payload: Some(crate::context::usage_checkpoint::encode_checkpoint(this.turn.padded_countdown_spend())),
                                }));
                            }
                        }
                    }
                    if this.persistence.rollout().is_some() {
                        records.push(crate::schemas::rollout::RolloutItem::Message {
                            message: m.clone(),
                        });
                        // Structured results are inspection data, not provider
                        // replay content. Keep them in a separate rollout event.
                        if let Message::ToolResult(result) = &m {
                            if let Some(output) = &result.output {
                                let mut payload = RuntimeObject::new();
                                payload.insert(
                                    "toolUseId",
                                    RuntimeJson::String(result.tool_use_id.clone()),
                                );
                                payload.insert("output", output.clone());
                                records.push(crate::schemas::rollout::RolloutItem::Event(
                                    crate::schemas::rollout::EventItem {
                                        event: "tool_result_output".into(),
                                        turn_id: Some(turn_id.clone()),
                                        payload: Some(RuntimeJson::Object(payload)),
                                    },
                                ));
                            }
                        }
                    }
                    if let Some(prepared) = prepared {
                        prepared.commit(records);
                    }
                    drop(context_update);
                    if Self::is_ordinary_communication(&m) {
                        this.publish_subagent_prompt(
                            &String::from_utf8(this.id.to_node_utf8())
                                .expect("session ID projection is UTF-8"),
                            &m,
                        );
                    }
                    Ok(())
                })
            })
        };
        let on_compaction: crate::engine::query::OnCompaction = {
            let this = this.clone();
            Box::new(move |mut rec: QueryCompactionRecord| {
                let this = this.clone();
                Box::pin(async move {
                    let context_update = this.turn.context_window_update_gate.lock().await;
                    let prepared = match this.persistence.rollout() {
                        Some(rollout) => Some(rollout.prepare_batch(3).await.map_err(|error| {
                            RuntimeString::from(format!(
                                "compaction persistence admission failed: {error}"
                            ))
                        })?),
                        None => None,
                    };
                    let mut records = Vec::with_capacity(3);
                    if !rec.retires_context {
                        // The query sees model history without host rendering
                        // records. Repair the complete transcript so those records
                        // and their ordering survive the durable checkpoint.
                        rec.replacement_history = this.history_snapshot();
                        rec.source_message_ids = rec
                            .replacement_history
                            .iter()
                            .map(super::message_uuid)
                            .cloned()
                            .collect();
                        rec.replaced_range = (0, rec.source_message_ids.len());
                        rec.replacement_history.retain(|message| {
                            !crate::context::instruction_policy::is_workspace_instruction_projection(
                                message,
                            )
                        });
                        crate::context::tokens::invalidate_context_usage(
                            &mut rec.replacement_history,
                        );
                    }
                    // Whatever this compaction retires is spent for good: fold it into
                    // the padded countdown's base BEFORE the history is replaced, or the
                    // reported remainder would rewind upward every time history is
                    // summarized away.
                    if rec.retires_context {
                        let source: std::collections::HashSet<_> =
                            rec.source_message_ids.iter().collect();
                        let retired_history: Vec<_> = this
                            .history_snapshot()
                            .into_iter()
                            .filter(|message| match message {
                                Message::User(user) if user.question_answer.is_some() => {
                                    source.contains(&user.uuid)
                                }
                                _ => true,
                            })
                            .collect();
                        let retired = estimate_context_tokens(&retired_history)
                            .saturating_sub(estimate_context_tokens(&rec.replacement_history));
                        this.turn.record_padded_countdown_retirement(retired);
                    }
                    let new_summary = rec.retires_context
                        && rec.replacement_history.iter().any(|message| {
                            matches!(message, Message::CompactBoundary(boundary)
                            if !rec.source_message_ids.contains(&boundary.uuid))
                        });
                    if new_summary {
                        this.refresh_workspace_instruction_cache();
                        this.file_state.clear();

                        this.memory_roots
                            .nested_loaded_paths()
                            .lock_recover()
                            .clear();
                        this.memory_roots.nested_triggers().lock_recover().clear();
                    }
                    rec.replacement_history = this
                        .conversation
                        .apply_compaction(rec.replacement_history, &rec.source_message_ids);
                    let mut compacted = to_store_compaction(&rec);
                    compacted.approval_parent_history = this.review_parent_checkpoint_payload();
                    records.push(crate::schemas::rollout::RolloutItem::Compacted(
                        crate::schemas::rollout::CompactedItem {
                            summary: compacted.summary,
                            replaced_range: compacted.replaced_range,
                            replacement_history: compacted.replacement_history,
                            source_message_ids: compacted.source_message_ids,
                            approval_parent_history: compacted.approval_parent_history,
                        },
                    ));
                    if new_summary {
                        let mut window = this.turn.context_window_usage.lock_recover();
                        window.active_tokens = Some(estimate_context_tokens(&this.model_history()));
                        window.prefill_input_tokens = None;
                        window.window_number = window.window_number.saturating_add(1);
                        records.push(crate::schemas::rollout::RolloutItem::Event(
                            crate::schemas::rollout::EventItem {
                                event: crate::context::window_usage::WINDOW_USAGE_EVENT.into(),
                                turn_id: None,
                                payload: Some(crate::context::window_usage::encode(&window)),
                            },
                        ));
                    }
                    if rec.retires_context {
                        records.push(crate::schemas::rollout::RolloutItem::Event(
                            crate::schemas::rollout::EventItem {
                                event:
                                    crate::context::usage_checkpoint::CONTEXT_USAGE_ROLLOUT_EVENT
                                        .into(),
                                turn_id: None,
                                payload: Some(crate::context::usage_checkpoint::encode_checkpoint(
                                    this.turn.padded_countdown_spend(),
                                )),
                            },
                        ));
                    }
                    if let Some(prepared) = prepared {
                        prepared.commit(records);
                    }
                    drop(context_update);
                    Ok(())
                })
            })
        };
        let on_permission_update: crate::engine::query::OnPermissionUpdate = {
            let this = this.clone();
            Box::new(move |updates: Vec<PermissionUpdate>| {
                for u in updates {
                    this.apply_permission_update(u);
                }
            })
        };
        let on_surface_tools: crate::engine::query::OnSurfaceTools = {
            let this = this.clone();
            Box::new(move |names: Vec<RuntimeString>| {
                let this = this.clone();
                Box::pin(async move {
                    let sink = this.persistence.rollout().cloned();
                    this.record_surfaced_tools(&this.surfaced_tools, sink.as_ref(), names)
                        .await
                })
            })
        };
        let cost_so_far: crate::engine::query::CostSoFar = {
            let c = this.cost.clone();
            Box::new(move || c.lock_recover().total_cost_usd())
        };

        let mention_factory = tool_context_base.clone();
        let mut model_history = this.model_history();
        let mut planning_protected = std::collections::HashSet::new();
        if let Ok(snapshot) = this.planning.snapshot() {
            if let Some(plan) = snapshot
                .plans
                .iter()
                .rfind(|plan| plan.status == "proposed" || plan.status == "accepted")
            {
                let context_id: RuntimeString = format!(
                    "planning-context-{}-{}",
                    crate::planning::runtime::host(&plan.id),
                    plan.revision
                )
                .into();
                let content = RuntimeString::from("<proposed_plan>\n")
                    .concat(&plan.content)
                    .concat(&RuntimeString::from("\n</proposed_plan>"));
                let present=model_history.iter().find_map(|message|match message{Message::Assistant(message) if message.uuid==context_id||message.content.iter().any(|block|matches!(block,ContentBlock::Text(text) if text.text.as_utf16().windows(content.len_utf16()).any(|units| units==content.as_utf16())))=>Some(message.uuid.clone()),_=>None});
                if present.is_none() {
                    let before_user = model_history
                        .iter()
                        .rposition(|message| matches!(message, Message::User(_)))
                        .unwrap_or(model_history.len());
                    model_history.insert(
                        before_user,
                        Message::Assistant(crate::schemas::message::AssistantMessage {
                            uuid: context_id.clone(),
                            content: vec![ContentBlock::Text(crate::schemas::content::TextBlock {
                                text: content,
                                citations: None,
                                harness_envelope: None,
                                message_id: None,
                                phase: None,
                            })],
                            model: turn_model.clone(),
                            usage: None,
                        }),
                    );
                }
                planning_protected.insert(present.unwrap_or(context_id));
            }
        }
        // One-shot tool-result reclaim: this turn starts after the provider's
        // prompt cache has certainly expired, so the whole prefix re-sends
        // regardless and the budget pass may trade aged full-size results down
        // to their persisted previews at no extra cache cost.
        if this.config.aggregate_tool_results
            && this.config.reclaim_tool_results_when_cache_cold
            && this
                .turn
                .cold_cache_reclaim_due((this.deps.now)(), PROMPT_CACHE_COLD_AFTER_MS)
        {
            this.tool_result_budget_state()
                .lock_recover()
                .arm_cold_reclaim();
        }
        let params = QueryParams {
            multi_agent_instructions: this.meta.role
                != Some(crate::schemas::rollout::SessionRole::SideTask),
            thread_id: this.id.clone(),
            turn_id: turn_id.clone(),
            model: turn_model.clone(),
            provider_info: Some(initial_bundle.provider.info().clone()),
            messages: model_history,
            compaction_protected_message_ids: planning_protected,
            context_window: Some(this.context_window()),
            precompute: Some(this.precompute_registry()),
            compaction_history: Some({
                let session = Arc::downgrade(&this);
                Arc::new(move || {
                    session
                        .upgrade()
                        .map(|session| session.history_snapshot())
                        .unwrap_or_default()
                })
            }),
            recover_compaction_history: this.persistence.store().map(|_| {
                let session = Arc::downgrade(&this);
                Arc::new(move |messages| {
                    let session = session.clone();
                    Box::pin(async move {
                        let session = session.upgrade().ok_or_else(|| RuntimeString::from("The conversation closed before its local context could be restored."))?;
                        session.recover_portable_context(messages).await
                    }) as futures::future::BoxFuture<'static, Result<Vec<Message>, RuntimeString>>
                }) as crate::engine::query::RecoverCompactionHistory
            }),
            compaction_output: this.compaction_output_sink(),
            tools: tools.clone(),
            tool_specs: to_tool_specs(&tools),
            refresh_tools: None,
            refresh_tools_async,
            agent_tool_policy: this.root_agent_definition.clone(),
            system_prompt: SystemPromptResolvable::Fn(system_prompt),
            config: initial_bundle.config_for_model(&turn_model),
            max_turns: this
                .root_agent_definition
                .as_ref()
                .and_then(|definition| definition.max_turns),
            cost_so_far: Some(cost_so_far),
            tool_context_base,
            permission_engine: this.permission_engine.clone(),
            hook_bus: this.current_hook_bus(),
            signal: abort.clone(),
            approval_handler: opts
                .approval_handler
                .clone()
                .or_else(|| this.approval_handler.clone()),
            approval_reviewer: Some(
                this.approval_reviewer_for_turn(
                    initial_bundle.clone(),
                    opts.approval_reviewer
                        .clone()
                        .or_else(|| this.approval_reviewer.clone()),
                ),
            ),
            output_schema: opts.output_schema.as_ref().map(runtime_schema_from_static),
            token_budget: opts.token_budget,
            goal: turn_goal.clone(),
            goal_request_identity: Some(goal_generation.clone()),
            goal_pending_input: opts.goal_pending_input.clone(),
            goal_execution_enabled: Some({
                let this = this.clone();
                Arc::new(move || {
                    this.collaboration_mode() != crate::planning::CollaborationMode::Plan
                })
            }),
            goal_usage: None,
            web_search: web_selection.request,
            on_message: Some(on_message),
            prepare_request_usage: Some({
                let slot = inference_slot.clone();
                let initial = initial_bundle.clone();
                this.request_usage_recorder(Arc::new(move || {
                    Session::bundle_from_slot(&slot, &initial)
                }))
            }),
            retained_tool_tasks: Some(this.persistence.tool_tasks.clone()),
            message_rollout: this.persistence.rollout().cloned(),
            on_permission_update: Some(on_permission_update),
            on_compaction: Some(on_compaction),
            surfaced_tools: Some(this.surfaced_tools.lock_recover().iter().cloned().collect()),
            on_surface_tools: Some(on_surface_tools),
            on_tool_lifecycle: tool_lifecycle_index_writer(
                this.clone(),
                tool_lifecycle_rollout_writer(this.persistence.rollout().cloned()),
            ),
            provider_call_admission: None,
            on_provider_request_admitted: opts.on_provider_request_admitted.clone(),
            emit_thread_started: !this.turn.thread_started_emitted(),
            reminder_providers: this.reminder_providers.get().cloned().unwrap_or_default(),
            edited_files_reminder: this.edited_files_reminder.get().and_then(|o| o.clone()),
            effort: opts
                .effort
                .clone()
                .or_else(|| initial_bundle.config.effort.clone()),
            ultra_mode: opts.ultra_mode.unwrap_or(initial_bundle.config.ultra_mode),
            thinking: if initial_bundle.config.thinking {
                Some(ThinkingConfig::Adaptive)
            } else {
                None
            },
            skip_cache_write: None,
            cache_ttl: if this.config.cache1h_ttl {
                Some(CacheTtl::OneHour)
            } else {
                None
            },
            cache_domain: None,
            cache_key: Some(this.id.clone()),
            // Only a BOUND document has a path. Nothing is derived from the mode,
            // so entering plan mode does not by itself conjure a plan file — the
            // reminder then reports that no plan exists yet, which is the truth.
            plan_file_path: this.plan_file_context(None).map(|p| p.path),
            message_queue: Some(this.conversation.queue().clone()),
            input_pdf_bytes,
            input_pdf_documents,
            validate_native_pdf: {
                let provider = initial_bundle.provider.clone();
                Some(Arc::new(move |model| {
                    validate_native_pdf_provider(provider.as_ref(), model)
                }))
            },
            agent_id: None,
            agent_type: None,
            tool_result_budget_state: Some(this.tool_result_budget_state()),
            context_hint_state: (this.meta.role
                != Some(crate::schemas::rollout::SessionRole::SideTask))
                .then(|| this.context_hint_state.clone()),
            file_state: (this.meta.role
                != Some(crate::schemas::rollout::SessionRole::SideTask))
                .then(|| this.file_state.clone()),
            on_tool_result_budget_records: {
                let this = this.clone();
                Some(Arc::new(move |records| {
                    let this = this.clone();
                    Box::pin(async move {
                        let sink = this.persistence.rollout().cloned();
                        this.record_tool_result_budget(sink.as_ref(), records).await
                    })
                }))
            },
            padded_countdown_spend: this.turn.padded_countdown_spend(),
            task_countdown_anchor: Some(this.context_countdown_anchor.clone()),
            // Only a main-loop turn may advance the latch, so a side call cannot
            // flip the declaration the main thread is caching against.
            thinking_context_cold: this
                .turn
                .latch_thinking_context_cold((this.deps.now)(), PROMPT_CACHE_COLD_AFTER_MS),
            context_edit_state: Some(this.context_edit_state.clone()),
            live_model_override,
        };
        this.turn
            .thread_started_emitted
            .store(true, Ordering::SeqCst);
        let cache_warning = this.cache_policy_noop_warning(&turn_model, &initial_bundle);
        let deps = initial_bundle.deps.to_engine_deps();

        let this2 = this.clone();
        let abort2 = abort.clone();
        // The lifecycle writer needs the turn id even when the turn ends in
        // TurnFailed (that event carries no id), and `inner` moves `turn_id`.
        let instrument_turn_id = turn_id.clone();
        let goal_context_owner = this.clone();
        #[cfg(any(test, feature = "testing"))]
        if let Some(goal) = this2.goal_observation_runtime() {
            goal.testing_note_phase("run.stream.defined");
        }
        let inner = stream! {
            if let Some(rollout) = &this2.persistence.ordered_rollout {
                if let Err(error) = rollout.wait_prefixes().await {
                    yield EngineEvent::TurnFailed { inference: None, error: EngineError {
                        code: EngineErrorCode::Internal,
                        message: format!("turn persistence admission failed: {error}").into(),
                        cause: None, request_id: None, provider_kind: None, retry_after_ms: None,
                    }};
                    return;
                }
            }
            #[cfg(any(test, feature = "testing"))]
            if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("run.stream.firstPoll"); }
            let managed_admission = match initial_bundle.load_review_requirements().await {
                Ok(requirements) => this2.resolve_managed_step_policy(&requirements, &initial_bundle.config.model, ),
                Err(error) => Err(error.to_string()),
            };
            match managed_admission {
                Ok(policy) => { *resolved_reviewer.write_recover() = policy.approvals_reviewer; }
                Err(message) => {
                    yield EngineEvent::TurnFailed { inference: None, error: EngineError {
                        code: EngineErrorCode::InvalidInput, message: message.into(), cause: None,
                        request_id: None, provider_kind: None, retry_after_ms: None,
                    }};
                    return;
                }
            }
            // If init won admission before this run, wait for it to publish its
            // terminal result before SessionStart/the first model prompt. If the
            // run won admission, a later init observes the durable closed gate.
            #[cfg(any(test, feature = "testing"))]
            if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("setup.serial"); }
            {
                let _setup = this2.setup.serial.lock().await;
            }
            #[cfg(any(test, feature = "testing"))]
            if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("setup.serial.ready"); }
            #[cfg(any(test, feature = "testing"))]
            if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("setup.initClosed"); }
            if let Err(error) =
                Session::await_setup_init_close_durability(setup_init_close_receipt).await
            {
                #[cfg(any(test, feature = "testing"))]
                if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("setup.initClosed.error"); }
                // The session is closing under this Turn: settle it as aborted,
                // exactly like a user cancel, instead of a red internal failure.
                if is_session_shutdown_refusal(&error) {
                    yield EngineEvent::TurnCompleted { turn_id: turn_id.clone(), usage: None, stop_reason: TerminalReason::Aborted };
                    return;
                }
                yield EngineEvent::TurnFailed { inference: None,
                    error: EngineError {
                        code: EngineErrorCode::Internal,
                        message: error,
                        cause: None,
                        request_id: None,
                        provider_kind: None,
                        retry_after_ms: None,
                    },
                };
                return;
            }
            #[cfg(any(test, feature = "testing"))]
            if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("setup.initClosed.ready"); }
            // A real run cannot cross SessionStart/provider admission until
            // the InitClosed marker, any pre-admitted Setup terminal, and this
            // turn's context are acknowledged by the rollout writer. The
            // dedicated receipt may settle before append_turn_context above,
            // so this second barrier is intentionally not redundant.
            #[cfg(any(test, feature = "testing"))]
            if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("setup.flush"); }
            if let Err(error) = this2.flush_setup_rollout().await {
                #[cfg(any(test, feature = "testing"))]
                if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("setup.flush.error"); }
                yield EngineEvent::TurnFailed { inference: None,
                    error: EngineError {
                        code: EngineErrorCode::Internal,
                        message: RuntimeString::from(format!(
                            "failed to persist Setup and turn admission boundary before provider execution: {error}"
                        )),
                        cause: None,
                        request_id: None,
                        provider_kind: None,
                        retry_after_ms: None,
                    },
                };
                return;
            }
            #[cfg(any(test, feature = "testing"))]
            if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("setup.flush.ready"); }
            let mut params = params;
            let mut mention_inputs = Vec::new();
            if let Some(batch) = pending_resume {
                if abort2.is_cancelled() {
                    yield EngineEvent::TurnCompleted { turn_id: turn_id.clone(), usage: None, stop_reason: TerminalReason::Aborted };
                    return;
                }
                for (item, content) in batch.items.iter().zip(&batch.content) {
                    mention_inputs.push((item.uuid.clone().expect("pending queue identity"), content.clone()));
                    let message = Message::User(UserMessage {
            question_answer: None,
                        uuid: item.uuid.clone().expect("pending queue identity"),
                        content: content.clone(),
                        is_meta: item.is_meta,
                    });
                    record_turn_message(&this2, &message, &turn_id).await;
                    this2.push_history(message.clone());
                    params.messages.push(message.clone());
                    let display_message = item.display_metadata.as_ref().map(|metadata| {
                        user_display_message(
                            item.uuid.clone().expect("pending queue identity"),
                            metadata,
                            (this2.deps.uuid)().into(),
                        )
                    });
                    if let Some(display) = &display_message {
                        record_turn_message(&this2, display, &turn_id).await;
                        this2.push_history(display.clone());
                    }
                    if let Some(rollout) = this2.persistence.rollout() {
                        rollout.append_message(message).await;
                        if let Some(display) = display_message {
                            rollout.append_message(display).await;
                        }
                        rollout.append_event("queue_resume_input".into(), Some(AppendEventOptions {
                            turn_id: Some(turn_id.clone()),
                            payload: Some(crate::runtime_values::runtime_codec::encode_value(item.as_ref())
                                .expect("queued input has an exact codec")),
                        })).await;
                    }
                }
                if let Err(error) = batch.queue.remove_with_state_durable(
                    &batch.items, crate::engine::message_queue::QueueTerminalState::Drained,
                ).await {
                    yield EngineEvent::TurnFailed { inference: None, error: EngineError {
                        code: EngineErrorCode::Internal,
                        message: RuntimeString::from(format!("failed to persist pending input handoff: {}", runtime_error_message_to_public_host(&error))),
                        cause: None, request_id: None, provider_kind: None, retry_after_ms: None,
                    }};
                    return;
                }
                for item in &batch.items {
                    yield EngineEvent::InputApplied { correlation_id: item.correlation_id.clone() };
                }
            }
            #[cfg(any(test, feature = "testing"))]
            if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("mcp.reconcile"); }
            if let Err(error) = this2.reconcile_plugin_mcp_for_turn().await {
                yield EngineEvent::TurnFailed { inference: None, error: EngineError {
                    code: EngineErrorCode::Internal,
                    message: RuntimeString::from(error),
                    cause: None, request_id: None, provider_kind: None, retry_after_ms: None,
                }};
                return;
            }
            #[cfg(any(test, feature = "testing"))]
            if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("mcp.readLease"); }
            let _mcp_runtime_guard = this2.mcp.gate.read().await;
            #[cfg(any(test, feature = "testing"))]
            if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("mcp.clients"); }
            let plugin_mcp_clients = this2.plugin_mcp_clients().await;
            if allow_mcp_form_input {
                this2
                    .mcp_elicitation_router
                    .enable_form_input_for_turn(question_generation);
            }
            if initial_bundle.provider.capabilities(&turn_model).tools && !plugin_mcp_clients.is_empty() {
                let plugin_mcp_tools = Session::mcp_tools_from_clients(&plugin_mcp_clients);
                let selected = select_web_search(
                    ensure_tool_search(build_tool_pool(BuildPoolOptions {
                        builtins: this2.current_tools(),
                        mcp_tools: if plugin_mcp_tools.is_empty() {
                            None
                        } else {
                            Some(plugin_mcp_tools)
                        },
                        plugin_tools: None,
                    })),
                    Some(&initial_bundle.config.web_search),
                    initial_bundle.provider.capabilities(&turn_model).web_search,
                );
                params.tools = selected.tools;
                params.tools.retain(|tool| advisor_enabled || tool.name() != "advisor");
                crate::planning::tools::retain_for_mode(&mut params.tools,turn_collaboration_mode,this2.meta.role.is_none()||this2.meta.role==Some(SessionRole::Main),this2.config.request_user_input_default);
                if let Some(definition) = &this2.root_agent_definition {
                    definition.retain_tools(&mut params.tools);
                }
                params.tool_specs = to_tool_specs(&params.tools);
                params.web_search = selected.request;
            }
            let mut active_user_content = user_content.clone();
            if let Some(line) = input_text.as_ref() {
                match this2
                    .run_slash_command_input(
                        line,
                        permission.clone(),
                        abort2.clone(),
                        turn_collaboration_mode == crate::planning::CollaborationMode::Plan,
                        turn_id.clone(),
                    )
                    .await
                {
                    Ok(Some(SlashCommandOutcome::Prompt(content))) => {
                        active_user_content = content;
                        if structured_input {
                            active_user_content.extend(user_content.iter().skip(1).cloned());
                        }
                        Session::replace_user_message_content(
                            &mut params.messages,
                            &user_uuid,
                            active_user_content.clone(),
                        );
                        let mut hist = this2.conversation.history.lock_recover();
                        Session::replace_user_message_content(
                            &mut hist,
                            &user_uuid,
                            active_user_content.clone(),
                        );
                    }
                    Ok(Some(SlashCommandOutcome::Local(outcome))) => {
                        if let Some(r) = this2.persistence.rollout() {
                            let msg = Message::User(UserMessage {
            question_answer: None,
                                uuid: user_uuid.clone().into(),
                                content: active_user_content.clone(),
                                is_meta: None,
                            });
                            record_turn_message(&this2, &msg, &turn_id).await;
                            r.append_message(msg).await;
                            if let Some(display_msg) = display_msg.clone() {
                                record_turn_message(&this2, &display_msg, &turn_id).await;
                                r.append_message(display_msg).await;
                            }
                        }
                        match outcome {
                            LocalRunOutcome::Message { text } => {
                                let msg = Message::System(SystemMessage {
                                    uuid: (this2.deps.uuid)().into(),
                                    text: text.clone().into(),
                                    subtype: SystemSubtype::LocalCommand,
                                });
                                this2.push_history(msg.clone());
                                if let Some(r) = this2.persistence.rollout() {
                                    record_turn_message(&this2, &msg, &turn_id).await;
                                    r.append_message(msg).await;
                                }
                                yield EngineEvent::Notification {
                                    level: NotificationLevel::Info,
                                    message: text.into(),
                                };
                            }
                            LocalRunOutcome::Noop => {}
                        }
                        yield EngineEvent::TurnCompleted {
                            turn_id: turn_id.clone(),
                            usage: None,
                            stop_reason: TerminalReason::Completed,
                        };
                        return;
                    }
                    Ok(None) => {}
                    Err(message) => {
                        if let Some(r) = this2.persistence.rollout() {
                            let msg = Message::User(UserMessage {
            question_answer: None,
                                uuid: user_uuid.clone().into(),
                                content: active_user_content.clone(),
                                is_meta: None,
                            });
                            record_turn_message(&this2, &msg, &turn_id).await;
                            r.append_message(msg).await;
                            if let Some(display_msg) = display_msg.clone() {
                                record_turn_message(&this2, &display_msg, &turn_id).await;
                                r.append_message(display_msg).await;
                            }
                        }
                        yield EngineEvent::TurnFailed { inference: None,
                            error: EngineError {
                                code: EngineErrorCode::Internal,
                                message: message.into(),
                                cause: None,
                                request_id: None,
                                provider_kind: None,
                                retry_after_ms: None,
                            },
                        };
                        return;
                    }
                }
            }
            if has_user_input {
                if let Some(r) = this2.persistence.rollout() {
                    let msg = Message::User(UserMessage {
            question_answer: None,
                        uuid: user_uuid.clone().into(),
                        content: active_user_content.clone(),
                        is_meta: params.goal.as_ref().is_some_and(|goal| goal.is_goal_context_turn()).then_some(true),
                    });
                    record_turn_message(&this2, &msg, &turn_id).await;
                    r.append_message(msg).await;
                    if let Some(display_msg) = display_msg.clone() {
                        record_turn_message(&this2, &display_msg, &turn_id).await;
                        r.append_message(display_msg).await;
                    }
                }
            }
            // `@`-mention expansion at DELIVERY (before query reads the messages).
            // Pending messages keep their own identities and expansion boundaries.
            if has_user_input {
                mention_inputs.push((user_uuid.clone().into(), active_user_content));
            }
            // Expanded content belongs to the authority captured before asynchronous reads.
            // Accounting-only changes may refresh the prompt without invalidating that content.
            let goal_context_authority = params.goal.as_ref()
                .filter(|goal| goal.is_goal_context_turn())
                .and(params.goal_request_identity.as_ref())
                .map(|identity| identity.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner).clone());
            let mut goal_context_added = Vec::new();
            for (mention_uuid, mention_content) in mention_inputs {
                let needs_at_mention = mention_content.iter().any(
                    |b| matches!(
                        b,
                        ContentBlock::Text(t)
                            if has_at_mention(&runtime_text_to_at_mention_host(&t.text))
                    ),
                );
                if needs_at_mention {
                    let mut ctx = (mention_factory)();
                    ctx.emit = std::sync::Arc::new(|_| Box::pin(async {}));
                    ctx.signal = abort2.clone();
                    let added = this2
                        .expand_at_mentions(&mention_content, &ctx, read_tool.as_ref())
                        .await;
                    if !added.is_empty() {
                        if mention_uuid == user_uuid && goal_context_authority.is_some() {
                            goal_context_added.extend_from_slice(&added);
                        }
                        for m in params.messages.iter_mut() {
                            if let Message::User(u) = m {
                                if u.uuid == mention_uuid {
                                    u.content.extend(added.clone());
                                    break;
                                }
                            }
                        }
                        let mut hist = this2.conversation.history.lock_recover();
                        for m in hist.iter_mut() {
                            if let Message::User(u) = m {
                                if u.uuid == mention_uuid {
                                    u.content.extend(added.clone());
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            let setup_claim = match this2.begin_setup_context_claim().await {
                Ok(claim) => claim,
                Err(message) => {
                    // The shutdown gate refused the claim because the session is
                    // closing: the Turn ends as an abort, not as a failure the
                    // user has to read as a broken request.
                    if is_session_shutdown_refusal(&message) {
                        yield EngineEvent::TurnCompleted { turn_id: turn_id.clone(), usage: None, stop_reason: TerminalReason::Aborted };
                        return;
                    }
                    yield EngineEvent::TurnFailed { inference: None,
                        error: EngineError {
                            code: EngineErrorCode::Internal,
                            message,
                            cause: None,
                            request_id: None,
                            provider_kind: None,
                            retry_after_ms: None,
                        },
                    };
                    return;
                }
            };
            if let Some(SetupContextClaim {
                additional_context: setup_context,
                admission,
            }) = setup_claim
            {
                if !setup_context.is_empty() {
                    // Claude's Setup additionalContext is an attachment message,
                    // not a system-prompt mutation. Keep it ephemeral to this
                    // first model run (so later turns do not receive it again),
                    // mark it meta so it is not a genuine user turn, and place it
                    // immediately before the real prompt so UserPromptSubmit still
                    // observes the actual user's text.
                    let content = setup_context
                        .into_iter()
                        .map(|text| {
                            ContentBlock::Text(TextBlock {
                                text,
                                citations: None,
                                harness_envelope: None,
                                message_id: None,
                                phase: None,
                            })
                        })
                        .collect();
                    let insert_at = params
                        .messages
                        .iter()
                        .rposition(|message| {
                            matches!(message, Message::User(user) if user.uuid == user_uuid)
                        })
                        .unwrap_or(params.messages.len());
                    params.messages.insert(
                        insert_at,
                        Message::User(UserMessage {
            question_answer: None,
                            uuid: RuntimeString::from((this2.deps.uuid)()),
                            content,
                            is_meta: Some(true),
                        }),
                    );
                }
                params.provider_call_admission = Some(admission);
            }
            params.provider_call_admission = Some(Arc::new(ManagedStepAdmission {
                session: this2.clone(),
                slot: inference_slot.clone(),
                initial: initial_bundle.clone(),
                reviewer: resolved_reviewer.clone(),
                inner: params.provider_call_admission.take(),
            }));
            // Cache-policy advisory at the head (warn-once, set atomically at delivery).
            // The Setup admission lease already lives in `params`, so dropping
            // the stream after this yielded advisory durably releases the claim.
            if let Some(w) = cache_warning {
                if !this2.turn.mark_cache_policy_warned() {
                    yield w;
                }
            }
            // A model step may publish a new plugin MCP binding after the prior tool batch exits.
            drop(_mcp_runtime_guard);
            if let Some(goal)=&params.goal {
                if goal.is_goal_context_turn() {
                    let expansion = goal_context_authority.as_ref()
                        .filter(|_| !goal_context_added.is_empty())
                        .map(|authority| (authority, goal_context_added.as_slice()));
                    if let Err(error)=goal.refresh_context_input(&mut params.messages,&RuntimeString::from(user_uuid.clone()),expansion).await {
                        if error.kind() == std::io::ErrorKind::Interrupted {
                            yield EngineEvent::TurnCompleted {turn_id:turn_id.clone(),usage:None,stop_reason:TerminalReason::Aborted};
                        } else {
                            yield EngineEvent::TurnFailed{inference:None,error:super::internal_err(&format!("goal context could not be prepared: {error}"))};
                        }
                        return;
                    }
                }
            }
            if ordinary_goal_input {
                if let Some(goal)=goal_context_owner.goal_runtime().filter(|goal|!goal.is_goal_context_turn()) {
                    let progress=params.tools.iter().any(|tool|tool.name()=="update_plan");
                    if let Some(text)=crate::goal::reminder::render_controlled(goal.goal().await,progress,turn_collaboration_mode==crate::planning::CollaborationMode::Default,goal.continuation_suppressed()) {
                        params.messages.push(Message::User(UserMessage {question_answer:None,uuid:(goal_context_owner.deps.uuid)().into(),content:vec![ContentBlock::Text(TextBlock{text:text.into(),citations:None,harness_envelope:None,message_id:None,phase:None})],is_meta:Some(true)}));
                    }
                }
            }
            #[cfg(any(test, feature = "testing"))]
            if let Some(goal) = this2.goal_observation_runtime() { goal.testing_note_phase("query.dispatch"); }
            let mut gen = crate::engine::query::query_with_inference(params, deps, inference_capture);
            while let Some(ev) = gen.next().await {
                yield ev;
            }
        };
        Turn {
            events: Box::pin(TurnLifecycleStream {
                inner: this.instrument(
                    this.project_proposed_plans(
                        Box::pin(inner),
                        instrument_turn_id.clone(),
                        turn_collaboration_mode,
                    ),
                    instrument_turn_id,
                ),
                lease: Some(turn_lease),
            }),
        }
    }

    pub(crate) fn rejected_input_turn(self: &Arc<Self>, error: InputValidationError) -> Turn {
        let engine_error = EngineError {
            code: EngineErrorCode::InvalidInput,
            message: error.message.clone().into(),
            cause: Some(Box::new(error)),
            request_id: None,
            provider_kind: None,
            retry_after_ms: None,
        };
        let rejected = self.clone();
        let events = stream! {
            let event = EngineEvent::TurnFailed { inference: None, error: engine_error };
            if let Some(observer) = &rejected.observer {
                observer.lock_recover().on_event(&event);
            }
            rejected.log_event(&event);
            yield event;
        };
        Turn {
            events: Box::pin(events),
        }
    }

    fn defer_model_update_turn(
        self: &Arc<Self>,
        input: Input,
        opts: TurnOptions,
        pending_resume: Option<PendingResumeBatch>,
    ) -> Turn {
        let session = self.clone();
        let signal = opts.signal.clone().unwrap_or_default();
        Turn {
            events: Box::pin(stream! {
                let capture = match session.capture_inference_for(&signal, opts.model.as_ref()).await {
                    Ok(capture) => capture,
                    Err(error) => {
                        let mut events = session.rejected_model_update_turn(error).events;
                        while let Some(event) = events.next().await { yield event; }
                        return;
                    }
                };
                let model = opts.model.clone().unwrap_or_else(|| capture.bundle.config.model.clone());
                let validated = match session.validate_input_for_bundle(input, &model, &capture.bundle) {
                    Ok(validated) => validated,
                    Err(error) => {
                        let mut events = session.rejected_input_turn(*error).events;
                        while let Some(event) = events.next().await { yield event; }
                        return;
                    }
                };
                let mut events = session.run_streamed_validated_with_pending(validated, opts, pending_resume).events;
                while let Some(event) = events.next().await { yield event; }
            }),
        }
    }

    pub(crate) fn rejected_closed_turn(self: &Arc<Self>) -> Turn {
        self.rejected_model_update_turn("session is shutting down".to_string())
    }

    fn rejected_model_update_turn(self: &Arc<Self>, message: String) -> Turn {
        let engine_error = EngineError {
            code: EngineErrorCode::Aborted,
            message: message.into(),
            cause: None,
            request_id: None,
            provider_kind: None,
            retry_after_ms: None,
        };
        let rejected = self.clone();
        let events = stream! {
            let event = EngineEvent::TurnFailed { inference: None, error: engine_error };
            if let Some(observer) = &rejected.observer {
                observer.lock_recover().on_event(&event);
            }
            rejected.log_event(&event);
            yield event;
        };
        Turn {
            events: Box::pin(events),
        }
    }

    /// Run a turn to completion, returning the aggregated outcome.
    pub async fn run(self: &Arc<Self>, input: Input, opts: TurnOptions) -> TurnResult {
        let has_schema = opts.output_schema.is_some();
        let turn = self.run_streamed(input, opts);
        let mut items: Vec<ThreadItem> = Vec::new();
        let mut final_text = RuntimeString::default();
        let mut usage: Option<Usage> = None;
        let mut stop_reason = TerminalReason::Completed;
        let mut error: Option<EngineError> = None;
        let mut events = turn.events;
        while let Some(ev) = events.next().await {
            match ev {
                EngineEvent::ItemCompleted { item, .. } => {
                    if let ThreadItem::AgentMessage { text, phase, .. } = &item {
                        if phase.as_ref().is_none_or(|phase| phase != "commentary") {
                            final_text = text.clone();
                        }
                    }
                    if let ThreadItem::ProposedPlan { plan, .. } = &item {
                        if plan.status == "proposed" {
                            final_text = plan.content.clone();
                        }
                    }
                    items.push(item);
                }
                EngineEvent::TurnCompleted {
                    usage: u,
                    stop_reason: sr,
                    ..
                } => {
                    usage = u;
                    stop_reason = sr;
                }
                EngineEvent::TurnFailed { error: e, .. } => {
                    stop_reason = TerminalReason::Error;
                    error = Some(e);
                }
                _ => {}
            }
        }
        let structured_output = if has_schema && !runtime_trim(&final_text).is_empty() {
            crate::runtime_values::runtime_json::parse(&final_text).ok()
        } else {
            None
        };
        TurnResult {
            items,
            final_text,
            usage,
            stop_reason,
            error,
            structured_output,
        }
    }

    /// Auto-wire observability + fire the turn-end memory/compaction hooks.
    ///
    /// `turn_id` is the turn id minted for this run. Everything this stream
    /// records per turn sources it from here: the file changes it folds and
    /// persists, and the terminal record for a failed turn (TurnFailed carries no
    /// id of its own).
    pub(crate) fn instrument(
        self: &Arc<Self>,
        mut events: EngineEventStream,
        turn_id: RuntimeString,
    ) -> EngineEventStream {
        let this = self.clone();
        Box::pin(stream! {
            let mut item_inference = std::collections::HashMap::new();
            while let Some(mut ev) = events.next().await {
                if let EngineEvent::ToolEvent { event: crate::types::events::ToolDomainEvent::PlanUpdated { progress }, .. } = &ev {
                    ev = EngineEvent::PlanUpdated { progress: progress.clone() };
                }
                match &ev {
                    EngineEvent::ItemStarted { item: ThreadItem::AgentMessage { id, .. }, inference: Some(inference) }
                    | EngineEvent::ItemUpdated { item: ThreadItem::AgentMessage { id, .. }, inference: Some(inference) }
                    | EngineEvent::ItemCompleted { item: ThreadItem::AgentMessage { id, .. }, inference: Some(inference) } => {
                        item_inference.insert(id.clone(), inference.clone());
                    }
                    _ => {}
                }
                let terminal = matches!(&ev, EngineEvent::TurnCompleted {..} | EngineEvent::TurnFailed {..});
                if terminal {
                    this.approval_review.breaker.lock_recover().clear_turn(&String::from_utf8(turn_id.to_node_utf8()).expect("Node text is UTF-8"));
                    let state = if matches!(&ev, EngineEvent::TurnCompleted {stop_reason: TerminalReason::Aborted, ..}) {"cancelled"} else {"expired"};
                    let _ = this.planning.finish_pending_questions(Some(&turn_id), state);
                    for question_resolved in this.async_questions.finish(&turn_id, state).await {
                        yield EngineEvent::QuestionResolved {question_resolved};
                    }
                }
                if !terminal {
                    if let Some(o) = &this.observer {
                        o.lock_recover().on_event(&ev);
                    }
                    this.log_event(&ev);
                }
                // What this TURN changed on disk, recorded per file. The host's
                // produced-output surface reads this, so it stays answerable in a
                // directory with no repository and after a resume, and it covers
                // what a sub-agent edited too. The turn id is what keeps one
                // turn's volume from being summed into the next one's.
                this.record_event_file_changes(Some(&turn_id), &ev);
                if let Some(r) = this.persistence.rollout() {
                    for change in crate::engine::session::file_changes_in_event(&ev) {
                        r.append_event(
                            FILE_CHANGE_ROLLOUT_EVENT.into(),
                            Some(AppendEventOptions {
                                turn_id: Some(turn_id.clone()),
                                payload: file_change_rollout_payload(change),
                            }),
                        ).await;
                    }
                }
                // Persist the turn's terminal state next to its messages so
                // history reads can group messages by turn AND tell a completed
                // turn from an interrupted/failed one (the renderer's fold
                // decision reads exactly this). The turn_context boundary record
                // written at turn start carries the same id.
                let lifecycle: Option<(&RuntimeString, &str)> = match &ev {
                    EngineEvent::TurnCompleted { turn_id, stop_reason, .. } => {
                        Some((turn_id, match stop_reason {
                            TerminalReason::Completed => "completed",
                            TerminalReason::Aborted => "interrupted",
                            _ => "failed",
                        }))
                    }
                    // A failed turn never reaches TurnCompleted. Without this
                    // record the rollout reader classes the turn "interrupted"
                    // on resume and a provider error (e.g. a 404 from a
                    // misconfigured BYO endpoint) vanishes from history.
                    EngineEvent::TurnFailed { .. } => Some((&turn_id, "failed")),
                    _ => None,
                };
                let mut persistence_error = None;
                let mut terminal_payload = None;
                if let Some((turn_id, status)) = lifecycle {
                    // Persist model-visible interrupt guidance BEFORE the terminal
                    // record, so a client that re-reads the rollout on the abort
                    // event already sees it, and so history replay carries it into
                    // the next request. Only a USER-initiated interrupt takes its
                    // marker: a shutdown or poisoned-queue abort set none, and this
                    // guidance would then claim an intent that never happened.
                    if status == "interrupted" && this.turn.take_user_interrupt(turn_id) {
                        let guidance = user_interrupt_guidance_message(turn_id);
                        if let Some(r) = this.persistence.rollout() {
                            r.append_message(guidance.clone()).await;
                            if let Err(error) = r.flush().await {
                                tracing::warn!(
                                    reason = %error,
                                    "failed to flush interrupted-turn guidance before its terminal record"
                                );
                            }
                        }
                        record_turn_message(&this, &guidance, turn_id).await;
                        this.push_history(guidance);
                    }
                    let finished_at_ms = (this.deps.now)();
                    // A failed turn also records WHY: the live error card is
                    // ephemeral, so this message is the only way a resumed
                    // history can show the failure instead of a bare bubble.
                    let failure_message = match &ev {
                        EngineEvent::TurnFailed { error, .. } => Some(RuntimeString::from(
                            runtime_error_message_to_public_host(&error.message),
                        )),
                        EngineEvent::TurnCompleted { stop_reason, .. } => match stop_reason {
                            TerminalReason::PromptTooLong => Some(
                                "The conversation exceeded the model context window.".into(),
                            ),
                            TerminalReason::AuthFailed => Some(
                                "Authentication failed for the selected model provider.".into(),
                            ),
                            TerminalReason::TurnLimitExhausted => Some(
                                "Execution stopped after reaching its configured turn limit.".into(),
                            ),
                            TerminalReason::SpendLimitExhausted => Some(
                                "Execution stopped after reaching the session spending limit.".into(),
                            ),
                            TerminalReason::Error => {
                                Some("The turn ended with an unspecified engine error.".into())
                            }
                            TerminalReason::Completed | TerminalReason::Aborted => None,
                        },
                        _ => None,
                    };
                    // The normalized provider kind rides alongside the message so
                    // a resumed history can localize the failure card instead of
                    // parsing the raw text.
                    let failure_kind = match &ev {
                        EngineEvent::TurnFailed { error, .. } => {
                            error.provider_kind.map(|kind| RuntimeString::from(kind.as_str()))
                        }
                        EngineEvent::TurnCompleted { stop_reason: TerminalReason::AuthFailed, .. } => {
                            Some(RuntimeString::from("auth"))
                        }
                        _ => None,
                    };
                    let failure_details = match &ev {
                        EngineEvent::TurnFailed { error, .. } => {
                            crate::runtime_values::runtime_codec::encode_value(error).ok()
                                .and_then(|details| super::sanitize_turn_error_details(&details))
                        }
                        _ => None,
                    };
                    {
                        let mut index = this.turn_index.lock_recover();
                        let started_at_ms = index.turn_started_at_ms.get(turn_id).copied().unwrap_or(0);
                        let duration_ms = finished_at_ms
                            .saturating_sub(started_at_ms)
                            .max(0) as u64;
                        index.turn_status.insert(turn_id.clone(), status.into());
                        if status != "completed" {
                            index.turn_final_message.remove(turn_id);
                        }
                        index.turn_finished_at_ms.insert(turn_id.clone(), finished_at_ms);
                        index.turn_duration_ms.insert(turn_id.clone(), duration_ms);
                        if let Some(message) = &failure_message {
                            index.turn_errors.insert(turn_id.clone(), message.clone());
                        }
                        if let Some(kind) = &failure_kind {
                            index.turn_error_kinds.insert(turn_id.clone(), kind.clone());
                        }
                        if let Some(details) = &failure_details {
                            index.turn_error_details.insert(turn_id.clone(), details.clone());
                        } else {
                            index.turn_error_details.remove(turn_id);
                        }
                    }
                    if let Some(r) = this.persistence.rollout() {
                        let mut payload = RuntimeObject::new();
                        payload.insert("status", RuntimeJson::String(status.into()));
                        if let Some(metadata)=this.turn_index.lock_recover().turn_goal_metadata.get(turn_id) { payload.insert("goalContinuation",RuntimeJson::Bool(metadata.goal_continuation)); payload.insert("inputOrigin",RuntimeJson::String(metadata.input_origin.clone())); payload.insert("goalId",metadata.goal_id.clone().map(RuntimeJson::String).unwrap_or(RuntimeJson::Null)); }

                        let (started_at_ms, duration_ms, final_message_id) = {
                            let index = this.turn_index.lock_recover();
                            (
                                index.turn_started_at_ms.get(turn_id).copied().unwrap_or(0),
                                index.turn_duration_ms.get(turn_id).copied().unwrap_or(0),
                                index.turn_final_message.get(turn_id).cloned(),
                            )
                        };
                        if started_at_ms > 0 {
                            payload.insert(
                                "startedAtMs",
                                RuntimeJson::Number(crate::runtime_values::RuntimeNumber::from(started_at_ms)),
                            );
                        }
                        payload.insert(
                            "finishedAtMs",
                            RuntimeJson::Number(crate::runtime_values::RuntimeNumber::from(finished_at_ms)),
                        );
                        payload.insert(
                            "durationMs",
                            RuntimeJson::Number(crate::runtime_values::RuntimeNumber::from(duration_ms)),
                        );
                        if status == "completed" {
                            if let Some(message_id) = final_message_id {
                                payload.insert(
                                    "finalMessageId",
                                    RuntimeJson::String(message_id),
                                );
                            }
                        }
                        if let Some(message) = failure_message {
                            payload.insert("error", RuntimeJson::String(message));
                        }
                        if let Some(kind) = failure_kind {
                            payload.insert("kind", RuntimeJson::String(kind));
                        }
                        if let Some(details) = failure_details {
                            payload.insert("errorDetails", details);
                        }
                        if let EngineEvent::TurnFailed { error, .. } = &ev {
                            if let Some(provider) = error.provider_error() {
                                if let Ok(detail) = crate::runtime_values::runtime_codec::encode_value(provider) {
                                    payload.insert("providerError", detail);
                                }
                            }
                            if let Some(reason) = error.recovery_reason() {
                                payload.insert("recoveryReason", RuntimeJson::String(reason.clone()));
                            }
                        }
                        if matches!(&ev, EngineEvent::TurnFailed { .. }) {
                            if let Some(inference) = ev.inference() {
                                if let Ok(identity) = crate::runtime_values::runtime_codec::encode_value(inference) {
                                    payload.insert("inference", identity);
                                }
                            }
                        }
                        terminal_payload = Some(payload.clone());
                        r.append_event(
                            TURN_LIFECYCLE_ROLLOUT_EVENT.into(),
                            Some(AppendEventOptions {
                                turn_id: Some(turn_id.clone()),
                                payload: Some(RuntimeJson::Object(payload)),
                            }),
                        ).await;
                        // A terminal notification is a read-after-write boundary.
                        // Readers must see the accepted originals and their terminal
                        // record without flushing or activating a session themselves.
                        if let Err(error) = r.flush().await {
                            persistence_error = Some(error.to_string());
                        }
                    }
                }
                if let Some(reason) = persistence_error {
                    let message = RuntimeString::from(format!("Failed to persist the turn completion boundary: {reason}"));
                    let already_failed = matches!(&ev, EngineEvent::TurnFailed { .. });
                    let inference = ev.inference().cloned();
                    let mut error = match ev {
                        EngineEvent::TurnFailed { error, .. } => error,
                        _ => EngineError {
                            code: EngineErrorCode::Internal, message: message.clone(), cause: None,
                            request_id: None, provider_kind: None, retry_after_ms: None,
                        },
                    };
                    if already_failed {
                        error.message = error.message.concat(&RuntimeString::from("\n")).concat(&message);
                    }
                    let details = crate::runtime_values::runtime_codec::encode_value(&error).ok()
                        .and_then(|details| super::sanitize_turn_error_details(&details));
                    {
                        let mut index = this.turn_index.lock_recover();
                        index.turn_status.insert(turn_id.clone(), "failed".into());
                        index.turn_final_message.remove(&turn_id);
                        index.turn_errors.insert(turn_id.clone(), error.message.clone());
                        if let Some(details) = &details {
                            index.turn_error_details.insert(turn_id.clone(), details.clone());
                        }
                    }
                    if let Some(r) = this.persistence.rollout() {
                        let mut payload = terminal_payload.unwrap_or_default();
                        payload.insert("status", RuntimeJson::String("failed".into()));
                        payload.insert("error", RuntimeJson::String(error.message.clone()));
                        payload.insert("finalMessageId", RuntimeJson::Null);
                        if let Some(details) = details { payload.insert("errorDetails", details); }
                        r.append_event(TURN_LIFECYCLE_ROLLOUT_EVENT.into(), Some(AppendEventOptions {
                            turn_id: Some(turn_id.clone()), payload: Some(RuntimeJson::Object(payload)),
                        })).await;
                        if let Err(error) = r.flush().await {
                            tracing::warn!(reason = %error, "failed to persist the terminal storage failure");
                        }
                    }
                    ev = EngineEvent::TurnFailed { inference, error };
                }
                if terminal {
                    if let Some(o) = &this.observer {
                        o.lock_recover().on_event(&ev);
                    }
                    this.log_event(&ev);
                }
                if let EngineEvent::TurnCompleted { stop_reason, .. } = &ev {
                    if *stop_reason == TerminalReason::Completed {
                        // Providers without a native phase remain unclassified while streaming.
                        // The completed Turn's authoritative final identity settles them here.
                        let final_id = this.turn_index.lock_recover().turn_final_message.get(&turn_id).cloned();
                        if let Some(final_id) = final_id {
                            let final_item = this.history().into_iter().find_map(|message| {
                                let Message::Assistant(assistant) = message else { return None };
                                if assistant.uuid != final_id { return None; }
                                assistant.content.into_iter().filter_map(|block| match block {
                                    ContentBlock::Text(text) if text.phase.as_ref().is_none_or(|phase| phase != "commentary") => Some(text),
                                    _ => None,
                                }).max_by_key(|text| text.phase.as_ref().is_some_and(|phase| phase == "final_answer"))
                                    .map(|text| ThreadItem::AgentMessage {
                                        id: text.message_id.unwrap_or_else(|| final_id.clone()),
                                        text: text.text,
                                        phase: Some("final_answer".into()),
                                    })
                            });
                            if let Some(item) = final_item {
                                let inference = match &item {
                                    ThreadItem::AgentMessage { id, .. } => item_inference.get(id).cloned(),
                                    _ => None,
                                };
                                yield EngineEvent::ItemUpdated { item, inference };
                            }
                        }
                        this.maybe_extract_memories();
                        this.maybe_extract_session_memory();
                        this.maybe_auto_dream();
                    }
                }
                yield ev;
            }
        })
    }

    pub(crate) fn log_event(&self, ev: &EngineEvent) {
        let Some(log) = &self.logger else { return };
        match ev {
            EngineEvent::TurnFailed { error, .. } => {
                log.error(
                    json!({
                        "event": "turn.failed",
                        "error": {
                            "code": format!("{:?}", error.code),
                            "message": runtime_error_message_to_public_host(&error.message),
                            "providerStatus": error.provider_error().and_then(|provider| provider.status),
                            "providerCode": error.provider_error().and_then(|provider| provider.code.as_ref()).map(runtime_error_message_to_public_host),
                            "providerType": error.provider_error().and_then(|provider| provider.error_type.as_ref()).map(runtime_error_message_to_public_host),
                            "requestId": error.request_id.as_ref().map(runtime_error_message_to_public_host),
                            "recoveryReason": error.recovery_reason().map(runtime_error_message_to_public_host),
                        }
                    }),
                    "turn failed",
                );
            }
            EngineEvent::TurnCompleted {
                stop_reason, usage, ..
            } => {
                let usage = usage.as_ref().map(|usage| {
                    json!({
                        "inputTokens": usage.input_tokens,
                        "outputTokens": usage.output_tokens,
                        "cacheReadInputTokens": usage.cache_read_input_tokens,
                        "cacheCreationInputTokens": usage.cache_creation_input_tokens,
                        "totalTokens": usage.total_tokens,
                    })
                });
                log.debug(
                    json!({ "event": "turn.completed", "stopReason": stop_reason, "usage": usage }),
                    "turn completed",
                );
            }
            EngineEvent::Compaction { mode, freed_tokens } => {
                log.debug(
                    json!({ "event": "compaction", "mode": mode, "freedTokens": freed_tokens }),
                    "compaction",
                );
            }
            EngineEvent::Notification { level, message } => {
                let message = runtime_error_message_to_public_host(message);
                if matches!(level, NotificationLevel::Warn) {
                    log.warn(json!({ "event": "notification" }), &message);
                } else {
                    log.info(json!({ "event": "notification" }), &message);
                }
            }
            _ => {}
        }
    }

    /// One-time advisory when a cache-policy flag is set but the provider can't act.
    pub(crate) fn cache_policy_noop_warning(
        &self,
        model: &RuntimeString,
        bundle: &super::InferenceBundle,
    ) -> Option<EngineEvent> {
        if self.turn.cache_policy_warned() || !self.config.cache1h_ttl {
            return None;
        }
        if bundle.provider.capabilities(model).prompt_cache_control {
            return None;
        }
        let name = runtime_provider_name_to_log_host(&bundle.provider.info().name);
        Some(EngineEvent::Notification {
            level: NotificationLevel::Warn,
            message: format!(
                "config.cache1hTtl is set, but the active provider '{name}' does not support request-side prompt-cache control, so the flag has no effect (any caching is automatic and provider-managed). Disable cache1hTtl or use an Anthropic provider."
            )
            .into(),
        })
    }

    /// Detect `@`-mentions and produce their expansion blocks (files/dirs → contents,
    /// `@agent-name` → a delegation nudge). Errors never surface.
    pub(crate) async fn expand_at_mentions(
        &self,
        content: &[ContentBlock],
        ctx: &ToolContext,
        read_tool: Option<&AnyTool>,
    ) -> Vec<ContentBlock> {
        let text = join_runtime_strings(
            &content
                .iter()
                .filter_map(|b| {
                    if let ContentBlock::Text(t) = b {
                        Some(t.text.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
            "\n",
        );
        let text = runtime_text_to_at_mention_host(&text);
        let file_mentions = extract_file_mentions(&text);
        let agent_mentions = extract_agent_mentions(&text);
        let mut added: Vec<ContentBlock> = Vec::new();
        for m in file_mentions {
            if let Some(blocks) = self.expand_file_mention(&m, ctx, read_tool).await {
                added.extend(blocks);
            }
        }
        for name in agent_mentions {
            let type_name = name.strip_prefix("agent-").unwrap_or(&name).to_string();
            if !self
                .current_agent_registry()
                .has(&RuntimeString::from(&type_name))
            {
                continue;
            }
            added.push(ContentBlock::Text(TextBlock {
                text: format!(
                    "The user referenced the @{name} agent. If it fits the task, invoke it via the task tool (subagent_type \"{type_name}\") with the needed context."
                )
                .into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            }));
        }
        added
    }

    /// Expand a single file/dir mention into content blocks, or `None` to drop it.
    pub(crate) async fn expand_file_mention(
        &self,
        m: &FileMention,
        ctx: &ToolContext,
        read_tool: Option<&AnyTool>,
    ) -> Option<Vec<ContentBlock>> {
        let read_tool = read_tool?;
        let expanded = if m.path == "~" {
            home_dir().unwrap_or_default()
        } else if let Some(rest) = m.path.strip_prefix("~/") {
            join_path(&home_dir().unwrap_or_default(), rest)
        } else {
            m.path.clone()
        };
        if is_unc_path(&expanded) {
            return None;
        }
        let mut input_map = RuntimeObject::new();
        input_map.insert("path", RuntimeJson::String(expanded.clone().into()));
        if let Some(start) = m.line_start {
            input_map.insert(
                "offset",
                RuntimeJson::Number(crate::runtime_values::RuntimeNumber::from(start)),
            );
            let end = m.line_end.unwrap_or(start);
            input_map.insert(
                "limit",
                RuntimeJson::Number(crate::runtime_values::RuntimeNumber::from(
                    (end.saturating_sub(start) + 1).max(1),
                )),
            );
        }
        let input = Json::Object(input_map);
        let decision = self
            .permission_engine
            .evaluate(read_tool.as_ref(), &input, ctx)
            .await;
        if matches!(
            decision,
            PermissionDecision::Deny { .. } | PermissionDecision::Fatal { .. }
        ) {
            return None;
        }
        let abs_path = resolve_runtime_join(&ctx.cwd, &RuntimeString::from(expanded.clone()));
        let abs = runtime_path_to_file_io_host(&abs_path);
        let meta = tokio::fs::metadata(&abs).await.ok()?;
        if meta.is_dir() {
            let mut rd = tokio::fs::read_dir(&abs).await.ok()?;
            let mut entries: Vec<String> = Vec::new();
            while let Ok(Some(e)) = rd.next_entry().await {
                if let Ok(name) = e.file_name().into_string() {
                    entries.push(name);
                }
            }
            const LIMIT: usize = 1000;
            let shown = entries
                .iter()
                .take(LIMIT)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n");
            let extra = if entries.len() > LIMIT {
                format!("\n… and {} more entries", entries.len() - LIMIT)
            } else {
                String::new()
            };
            return Some(vec![ContentBlock::Text(TextBlock {
                text: format!("Contents of directory @{}:\n{shown}{extra}", m.path).into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })]);
        }
        // A whole-file mention already read unchanged this session is NOT re-injected.
        if m.line_start.is_none() {
            if let Some(seen) = ctx.file_state.get(&abs_path) {
                let stat_mtime = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs_f64() * 1000.0)
                    .unwrap_or(0.0);
                if seen.content.is_some() && seen.mtime_ms.floor() == stat_mtime.floor() {
                    return Some(Vec::new());
                }
            }
        }
        let result = read_tool.call(input, ctx, None).await.ok()?;
        if result.serializable.is_error.unwrap_or(false) {
            return None;
        }
        let mut body: Vec<ContentBlock> = vec![ContentBlock::Text(TextBlock {
            text: format!("Contents of @{}:", m.raw).into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })];
        for b in result.serializable.model_content {
            body.push(output_to_content(b));
        }
        if let Some(extra) = result.serializable.extra_user_blocks {
            body.extend(extra);
        }
        Some(body)
    }
}

impl Session {
    /// Install or clear a trusted nonblocking adapter at the same boundary that
    /// orders snapshot registry mutation, rollout append, and event publication.
    pub async fn set_ordered_agent_memory_snapshot_decision_sink(
        &self,
        sink: Option<SnapshotDecisionOrderedSink>,
    ) {
        let _gate = self.snapshot.persistence.event_gate.lock().await;
        *self.snapshot.ordered_sink.lock_recover() = sink;
    }

    /// Run one trusted nonblocking adapter action under the snapshot event gate.
    pub async fn with_agent_memory_snapshot_order<R>(&self, action: impl FnOnce() -> R + Send) -> R
    where
        R: Send,
    {
        let _gate = self.snapshot.persistence.event_gate.lock().await;
        action()
    }

    /// Capture one authoritative decision and hand it to a trusted adapter while
    /// the event-order gate remains held.
    pub async fn with_agent_memory_snapshot_decision<R>(
        &self,
        request_id: &RuntimeString,
        action: impl FnOnce(Option<AgentMemorySnapshotDecisionAudit>) -> R + Send,
    ) -> Result<R, AgentMemorySnapshotApplyError>
    where
        R: Send,
    {
        self.await_current_snapshot_owner(request_id).await?;
        let _gate = self.snapshot.persistence.event_gate.lock().await;
        if let Some(error) = self.snapshot_fatal_apply_error() {
            return Err(error);
        }
        Ok(action(self.snapshot.decisions.query(request_id)))
    }

    /// Capture all authoritative decisions under the same ordered boundary.
    pub async fn with_agent_memory_snapshot_decisions<R>(
        &self,
        action: impl FnOnce(Vec<AgentMemorySnapshotDecisionAudit>) -> R + Send,
    ) -> Result<R, AgentMemorySnapshotApplyError>
    where
        R: Send,
    {
        self.await_all_snapshot_owners().await?;
        let _gate = self.snapshot.persistence.event_gate.lock().await;
        if let Some(error) = self.snapshot_fatal_apply_error() {
            return Err(error);
        }
        Ok(action(self.snapshot.decisions.all()))
    }
}
