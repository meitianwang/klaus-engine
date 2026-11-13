//! The engine's spine: the `query` entry point and the `run_turn` stage
//! functions (entry hooks, compaction check, stream retry, tool results,
//! loop guard, stop hook, budget continuation).

#[path = "media_recovery.rs"]
mod media_recovery;

#[path = "model_context_migration.rs"]
mod model_context_migration;

#[path = "portable_migration.rs"]
mod portable_migration;

#[path = "preflight_compaction.rs"]
mod preflight_compaction;

// ---------------------------------------------------------------------------
// The engine's spine.
// ---------------------------------------------------------------------------

use super::backoff_ms;
use super::base_hook_input;
use super::compaction::compaction_summary_of;
use super::compaction::persist_history_repair;
use super::compaction::reinject_query_retained_fragments;
use super::concat_runtime_strings;
use super::count_mcp_tools;
use super::is_retryable;
use super::is_valid_runtime_json;
use super::join_runtime_strings;
use super::last_user_text;
use super::persist_compaction;
use super::prepare_model_request;
use super::provider_meta_nudge;
use super::provider_metadata;
use super::query_path_dialect;
use super::queue_durability_engine_error;
use super::render_stop_hook_continuation;
use super::render_structured_output_retry;
use super::render_token_budget_continuation;
use super::request::abortable_sleep;
use super::request::fixed_prefix_tokens;
use super::request::render_empty_closing_retry;
use super::request::render_reasoning_over_limit_reminder;
use super::request::render_truncation_continuation;
use super::request_contains_document;
use super::resolve_system_prompt;
use super::restore_compaction_protected;
use super::result_trim::render_spilled_tool_result;
use super::split_compaction_protected;
use super::stream_and_dispatch;
use super::support::notify;
use super::trim_ecmascript_whitespace;
use super::visible_tool_specs;
use super::EventQueue;
use super::InferenceCapture;
use super::Outcome;
use super::PreparedModelRequest;
use super::QueryParams;
use super::Shared;
use super::TruncationRecovery;
use super::TurnContext;
use super::TurnOverride;
use super::TurnRuntimeState;
use super::BUDGET_COMPLETION_THRESHOLD;
use super::BUDGET_DIMINISHING_STREAK;
use super::BUDGET_DIMINISHING_TOKENS;
use super::BUDGET_MIN_CONTINUATIONS;
use super::DEFAULT_TURN_OUTPUT_TOKENS;
use super::INLINE_DRAIN_MODES;
use super::MAX_EMPTY_CLOSING_RETRIES;
use super::MAX_EMPTY_CONTENT_RETRIES;
use super::MAX_OUTPUT_SCHEMA_RESAMPLES;
use super::MAX_STOP_CONTINUATIONS;
use super::MAX_TRUNCATION_CONTINUATIONS;
use super::MAX_TURN_PDF_BYTES;
use super::MAX_TURN_PDF_DOCUMENTS;
use crate::context::instruction_policy::is_workspace_instruction_projection;
use crate::context::tokens::estimate_context_tokens_with_prefix;
use crate::context::tokens::invalidate_context_usage;
use crate::context::total_tokens_reminder::render_total_tokens_reminder;
use crate::engine::async_queue::AsyncQueue;
use crate::engine::async_queue::AsyncQueueOptions;
use crate::engine::async_queue::CoalesceFn;
use crate::engine::deps::EngineDeps;
use crate::engine::loop_detection::LoopDetector;
use crate::engine::message_queue::queued_value_to_content_blocks;
use crate::engine::message_queue::queued_value_to_tool_result_parts;
use crate::engine::message_queue::MessageQueue;
use crate::engine::message_queue::QueuePriority;
use crate::engine::message_queue::QueueTerminalState;
use crate::engine::message_queue::QueuedMessage;
use crate::engine::message_queue::SteerDelivery;
use crate::hooks::types::HookEvent;
use crate::hooks::types::HookTrigger;
use crate::provider::behavior::ProviderBehaviorProfile;
use crate::provider::model_fallback::classify_model_fallback_error;
use crate::provider::observability::{self as provider_observability};
use crate::runtime_process::path_join;
use crate::runtime_values::RuntimeJson;
use crate::runtime_values::RuntimeObject;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, TextBlock};
use crate::schemas::message::Message;
use crate::schemas::message::ToolResultMessage;
use crate::schemas::message::Usage;
use crate::schemas::message::UserMessage;
use crate::sync_ext::MutexExt;
use crate::tools::builtins::tool_search::ensure_tool_search;
use crate::tools::pool::to_tool_spec;
use crate::tools::result_storage::maybe_spill_result;
use crate::tools::result_storage::SpillOptions;
use crate::types::errors::EngineError;
use crate::types::errors::EngineErrorCode;
use crate::types::errors::InputValidationError;
use crate::types::errors::InputValidationErrorCode;
use crate::types::errors::ProviderError;
use crate::types::errors::ProviderErrorKind;
use crate::types::errors::TerminalReason;
use crate::types::errors::ToolError;
use crate::types::events::CompactionMode;
use crate::types::events::EngineEvent;
use crate::types::events::NotificationLevel;
use crate::types::provider::ModelStopReason;
use crate::types::provider::SystemPromptPart;
use crate::types::provider::ToolSpec;
use crate::types::reminder::ReminderContext;
use crate::types::tool::ElicitRequest;
use crate::types::tool::ElicitResponse;
use futures::future::FutureExt;
use futures::stream::Stream;
use std::collections::HashMap;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
struct QueryRunTask {
    task: tokio::task::JoinHandle<Result<(), crate::types::errors::ToolError>>,
    persistence: Arc<super::tool_tasks::QueryPersistence>,
    signal: crate::types::abort::AbortSignal,
    completed: Arc<std::sync::atomic::AtomicBool>,
}

struct PassiveMailboxBatch {
    queue: Arc<MessageQueue>,
    items: Vec<Arc<QueuedMessage>>,
}

impl Drop for PassiveMailboxBatch {
    fn drop(&mut self) {
        self.queue.release_pending_resume(&self.items);
    }
}

/// A new turn inherits passive mail, but passive mail never creates that turn.
async fn receive_passive_mailbox(shared: &Shared, messages: &mut Vec<Message>) -> bool {
    let Some(queue) = &shared.params.message_queue else {
        return true;
    };
    let items = match queue
        .reserve_passive_mailbox_durable(shared.params.agent_id.as_ref())
        .await
    {
        Ok(items) => items,
        Err(error) => {
            shared.q.push(EngineEvent::TurnFailed {
                inference: None,
                error: queue_durability_engine_error("reserving passive mailbox", error),
            });
            return false;
        }
    };
    let batch = PassiveMailboxBatch {
        queue: queue.clone(),
        items,
    };
    if batch.items.is_empty() {
        return true;
    }
    if shared.params.signal.is_cancelled() {
        return false;
    }
    for item in &batch.items {
        let uuid = item.uuid.clone().expect("mailbox queue identity");
        if messages
            .iter()
            .any(|message| matches!(message, Message::User(user) if user.uuid == uuid))
        {
            continue;
        }
        let message = Message::User(UserMessage {
            question_answer: None,
            uuid,
            content: crate::engine::session::ordinary_message_blocks(item),
            is_meta: Some(true),
        });
        if let Some(callback) = &shared.params.on_message {
            if callback(message.clone()).await.is_err() {
                return false;
            }
        }
        messages.push(message);
    }
    if let Some(rollout) = &shared.params.message_rollout {
        if let Err(error) = rollout.flush().await {
            shared.q.push(EngineEvent::TurnFailed {
                inference: None,
                error: queue_durability_engine_error(
                    "persisting passive mailbox",
                    error.to_string().into(),
                ),
            });
            return false;
        }
    }
    if let Err(error) = queue
        .remove_with_state_durable(&batch.items, QueueTerminalState::Drained)
        .await
    {
        shared.q.push(EngineEvent::TurnFailed {
            inference: None,
            error: queue_durability_engine_error("committing passive mailbox", error),
        });
        return false;
    }
    true
}

impl Drop for QueryRunTask {
    fn drop(&mut self) {
        if !self.completed.load(std::sync::atomic::Ordering::Acquire) {
            // Stop the caller's tools, then release the driver/provider promptly.
            // Tool tasks have already moved into the retained execution owner.
            self.signal.cancel();
            self.persistence.abort_if_idle(&self.task);
        }
    }
}

/// The engine's spine. Results travel as events (`turn.completed` /
/// `item.completed`), never as the stream's own return.
#[cfg(any(test, feature = "testing"))]
pub fn query(
    params: QueryParams,
    deps: EngineDeps,
) -> Pin<Box<dyn Stream<Item = EngineEvent> + Send>> {
    query_internal(params, deps, None)
}

/// Run an isolated query against an already captured provider and configuration.
pub(crate) fn query_isolated(
    params: QueryParams,
    deps: EngineDeps,
) -> Pin<Box<dyn Stream<Item = EngineEvent> + Send>> {
    query_internal(params, deps, None)
}

pub(crate) fn query_with_inference(
    params: QueryParams,
    deps: EngineDeps,
    capture: InferenceCapture,
) -> Pin<Box<dyn Stream<Item = EngineEvent> + Send>> {
    query_internal(params, deps, Some(capture))
}

fn query_internal(
    mut params: QueryParams,
    deps: EngineDeps,
    capture: Option<InferenceCapture>,
) -> Pin<Box<dyn Stream<Item = EngineEvent> + Send>> {
    params
        .retained_tool_tasks
        .get_or_insert_with(|| Arc::new(super::RetainedToolTasks::new()));
    let retained_tasks = params
        .retained_tool_tasks
        .as_ref()
        .expect("query owns retained tasks")
        .clone();
    let persistence = Arc::new(super::tool_tasks::QueryPersistence::default());
    let persistence_run = persistence.clone();
    let signal = params.signal.clone();
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let completed_run = completed.clone();
    // Under backpressure, fold consecutive text deltas together (lossless):
    // returns the merged tail item to replace it, or `None` to keep both.
    let coalesce: CoalesceFn<EngineEvent> = Box::new(|last, next| {
        if let (
            EngineEvent::TextDelta {
                text: lt,
                message_id: left_id,
                phase: left_phase,
                inference: left_inference,
            },
            EngineEvent::TextDelta {
                text: nt,
                message_id: right_id,
                phase: right_phase,
                inference: right_inference,
            },
        ) = (last, next)
        {
            if left_id == right_id && left_phase == right_phase && left_inference == right_inference
            {
                return Some(EngineEvent::TextDelta {
                    text: lt.concat(nt),
                    message_id: left_id.clone(),
                    phase: left_phase.clone(),
                    inference: left_inference.clone(),
                });
            }
        }
        None
    });
    let q = Arc::new(AsyncQueue::with_options(AsyncQueueOptions {
        high_water_mark: Some(512),
        coalesce: Some(coalesce),
    }));
    let q = Arc::new(EventQueue::new(q));
    // Register before invoking a session callback, including time spent waiting
    // for writer capacity. A failed acknowledgement closes publication before
    // cancelling the query so no later success event can escape the failure.
    if let Some(callback) = params.on_message.take() {
        let callback = Arc::new(callback);
        let persistence = persistence.clone();
        let q = q.clone();
        let signal = signal.clone();
        let tasks = retained_tasks.clone();
        params.on_message = Some(Box::new(move |message| {
            let guard = persistence.enter();
            let callback = callback.clone();
            let q = q.clone();
            let signal = signal.clone();
            let tasks = tasks.clone();
            Box::pin(async move {
                let _guard = guard.map_err(|error| error.message)?;
                let result = callback(message).await;
                if let Err(error) = &result {
                    let message = RuntimeString::from("Message persistence failed: ").concat(error);
                    tasks.record_failure(message.clone());
                    q.fail_persistence(message);
                    signal.cancel();
                }
                result
            })
        }));
    }
    if let Some(callback) = params.on_compaction.take() {
        let callback = Arc::new(callback);
        let persistence = persistence.clone();
        let q = q.clone();
        let signal = signal.clone();
        let tasks = retained_tasks.clone();
        params.on_compaction = Some(Box::new(move |record| {
            let guard = persistence.enter();
            let callback = callback.clone();
            let q = q.clone();
            let signal = signal.clone();
            let tasks = tasks.clone();
            Box::pin(async move {
                let _guard = guard.map_err(|error| error.message)?;
                let result = callback(record).await;
                if let Err(error) = &result {
                    let message =
                        RuntimeString::from("Compaction persistence failed: ").concat(error);
                    tasks.record_failure(message.clone());
                    q.fail_persistence(message);
                    signal.cancel();
                }
                result
            })
        }));
    }
    if let Some(callback) = params.on_tool_result_budget_records.take() {
        let persistence = persistence.clone();
        let q = q.clone();
        let signal = signal.clone();
        let tasks = retained_tasks.clone();
        params.on_tool_result_budget_records = Some(Arc::new(move |records| {
            let guard = persistence.enter();
            let callback = callback.clone();
            let q = q.clone();
            let signal = signal.clone();
            let tasks = tasks.clone();
            Box::pin(async move {
                let _guard = guard?;
                let result = callback(records).await;
                if let Err(error) = &result {
                    tasks.record_failure(error.message.clone());
                    q.fail_persistence(error.message.clone());
                    signal.cancel();
                }
                result
            })
        }));
    }
    let q_run = Arc::clone(&q);
    let run_task = retained_tasks.spawn_query(
        async move {
            let run = std::panic::AssertUnwindSafe(run_turn(
                params,
                deps,
                capture,
                Arc::clone(&q_run),
                persistence_run,
            ));
            let mut failure = None;
            if let Err(payload) = run.catch_unwind().await {
                // Only genuinely unexpected panics reach this boundary. Expected
                // fatal tool/validation/permission failures travel as typed values
                // through `Outcome::Fatal` and therefore never invoke the panic hook.
                let message: RuntimeString = if let Some(te) = payload.downcast_ref::<ToolError>() {
                    te.message.clone()
                } else if let Some(s) = payload.downcast_ref::<String>() {
                    s.clone().into()
                } else if let Some(s) = payload.downcast_ref::<&str>() {
                    (*s).into()
                } else {
                    "internal error".into()
                };
                failure = Some(crate::types::errors::ToolError::fatal(message.clone()));
                q_run.push(EngineEvent::TurnFailed {
                    inference: None,
                    error: EngineError {
                        code: EngineErrorCode::Internal,
                        message,
                        cause: None,
                        request_id: None,
                        provider_kind: None,
                        retry_after_ms: None,
                    },
                });
            }
            completed_run.store(true, std::sync::atomic::Ordering::Release);
            q_run.close();
            match failure {
                Some(error) => Err(error),
                None => Ok(()),
            }
        },
        persistence.expected_abort.clone(),
    );
    let run_task = match run_task {
        Ok(task) => task,
        Err(error) => {
            q.push(EngineEvent::TurnFailed {
                inference: None,
                error: EngineError {
                    code: EngineErrorCode::Internal,
                    message: error.message,
                    cause: None,
                    request_id: None,
                    provider_kind: None,
                    retry_after_ms: None,
                },
            });
            q.close();
            return Box::pin(futures::stream::unfold(q, |queue| async move {
                queue.next().await.map(|event| (event, queue))
            }));
        }
    };
    let q_drain = q;
    let run_owner = QueryRunTask {
        task: run_task,
        signal,
        completed,
        persistence,
    };
    Box::pin(async_stream::stream! {
        let _run_task = run_owner;
        while let Some(ev) = q_drain.next().await {
            yield ev;
        }
    })
}

async fn run_turn(
    mut params: QueryParams,
    deps: EngineDeps,
    inference: Option<InferenceCapture>,
    q: Arc<EventQueue>,
    persistence_in_flight: Arc<super::tool_tasks::QueryPersistence>,
) {
    let turn_state = TurnRuntimeState::from_params(&params);
    if params.emit_thread_started {
        q.push(turn_state.thread_started_event());
    }
    q.push(turn_state.turn_started_event());
    let captured = match &inference {
        Some(capture) => match capture.capture(&params.signal).await {
            Ok(guard) => Some(guard),
            Err(error) => {
                emit_capture_failure(&q, &params, error, None);
                return;
            }
        },
        None => None,
    };
    if let Some(policy) = &params.agent_tool_policy {
        policy.retain_tools(&mut params.tools);
        params.tool_specs = params
            .tools
            .iter()
            .map(|tool| to_tool_spec(tool.as_ref()))
            .collect();
        if !policy.allows_tool(&RuntimeString::from("web_search")) {
            params.web_search = None;
        }
    }
    // Probe the base tool context once for the scalars the loop reads directly.
    let (cwd, task_list_id, task_plan, background_tasks, async_question) = {
        let base = (params.tool_context_base)();
        (
            base.cwd.clone(),
            base.task_list_id.clone(),
            base.task_plan.clone(),
            base.background_tasks.clone(),
            base.async_question.clone(),
        )
    };
    let surfaced0: HashSet<RuntimeString> = params
        .surfaced_tools
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let mut turn_context0 = TurnContext {
        model: inference
            .as_ref()
            .and_then(|capture| capture.model_override.clone())
            .or_else(|| {
                inference
                    .as_ref()
                    .filter(|capture| capture.next.is_some())?;
                Some(captured.as_ref()?.bundle.config.model.clone())
            })
            .unwrap_or_else(|| params.model.clone()),
        effort: if inference
            .as_ref()
            .is_some_and(|capture| capture.next.is_some())
        {
            inference
                .as_ref()
                .and_then(|capture| capture.effort_override.clone())
                .or_else(|| {
                    captured
                        .as_ref()
                        .and_then(|guard| guard.bundle.config.effort.clone())
                })
        } else {
            inference
                .as_ref()
                .and_then(|capture| capture.effort_override.clone())
                .or_else(|| params.effort.clone())
        },
        ultra_mode: if inference
            .as_ref()
            .is_some_and(|capture| capture.next.is_some())
        {
            inference
                .as_ref()
                .and_then(|capture| capture.ultra_override)
                .or_else(|| {
                    captured
                        .as_ref()
                        .map(|guard| guard.bundle.config.ultra_mode)
                })
                .unwrap_or(params.ultra_mode)
        } else {
            inference
                .as_ref()
                .and_then(|capture| capture.ultra_override)
                .unwrap_or(params.ultra_mode)
        },
        model_override: inference
            .as_ref()
            .and_then(|capture| capture.model_override.clone()),
        effort_override: inference
            .as_ref()
            .and_then(|capture| capture.effort_override.clone()),
        ultra_override: inference
            .as_ref()
            .and_then(|capture| capture.ultra_override),
        granted_tools: HashSet::new(),
        hook_aborted: false,
    };
    turn_context0.effort = super::selected_effort(
        turn_context0.effort.take(),
        turn_context0.ultra_mode,
        turn_context0.effort_override.clone(),
        turn_context0.ultra_override,
    );
    turn_context0.ultra_mode = matches!(
        turn_context0.effort,
        Some(crate::schemas::effort::EffortValue::Level(
            crate::schemas::effort::EffortLevel::Ultra
        ))
    );
    let tools0 = Arc::new(params.tools.clone());
    let specs0 = Arc::new(params.tool_specs.clone());

    let shared = Arc::new(Shared {
        persistence_in_flight,
        step_model: Mutex::new(turn_context0.model.clone()),
        step_subagent_context: Mutex::new(None),
        tools: Mutex::new(tools0),
        tool_specs: Mutex::new(specs0),
        surfaced: Mutex::new(surfaced0),
        turn_context: Mutex::new(turn_context0),
        turn_state,
        cwd,
        task_list_id,
        task_plan,
        background_tasks,
        q,
        live_model_override: params.live_model_override.clone(),
        reminder_anchor_pending: Mutex::new(
            params.config.total_tokens_reminder_after_user_turn == Some(true)
                && params.messages.last().is_some_and(|message| {
                    matches!(message,
                Message::User(user) if user.is_meta != Some(true) && user.question_answer.is_none())
                }),
        ),
        reminder_spend: Mutex::new(params.padded_countdown_spend),
        task_countdown_anchor: params
            .task_countdown_anchor
            .clone()
            .unwrap_or_else(|| Arc::new(Mutex::new(Default::default()))),
        params,
        deps: Arc::new(deps),
        inference,
        captured: Mutex::new(captured),
        advisor_request: Mutex::new(None),
        truncation: Mutex::new(TruncationRecovery::default()),
    });

    let private_review = shared
        .params
        .agent_type
        .as_ref()
        .is_some_and(|kind| kind == "approval-review");
    bind_step_model(&shared);
    let Some(mut system_prompt) = await_turn_preparation(
        &shared,
        "system-prompt",
        resolve_system_prompt(&shared.params.system_prompt),
    )
    .await
    else {
        emit_preparation_aborted(&shared, None);
        return;
    };
    let mut messages: Vec<Message> = shared.params.messages.clone();
    if messages.iter().any(is_workspace_instruction_projection) {
        let source_message_ids = messages
            .iter()
            .map(super::query_message_uuid)
            .cloned()
            .collect();
        messages.retain(|message| !is_workspace_instruction_projection(message));
        // Old responses measured the duplicated prefix; those anchors no longer
        // describe this request. Keep the repaired snapshot durable on resume.
        invalidate_context_usage(&mut messages);
        persist_history_repair(&shared.params, &messages, source_message_ids).await;
    }
    if let Some(state) = &shared.params.context_hint_state {
        let model = shared.turn_context.lock_recover().model.clone();
        if state.bind_model(&model) {
            let source_ids = messages
                .iter()
                .map(super::query_message_uuid)
                .cloned()
                .collect();
            invalidate_context_usage(&mut messages);
            persist_history_repair(&shared.params, &messages, source_ids).await;
        }
    }
    let turn_user_uuid = messages.iter().rev().find_map(|message| match message {
        Message::User(message) if message.is_meta != Some(true) => Some(message.uuid.clone()),
        _ => None,
    });
    let mut has_attempted_reactive_compact = false;
    let mut turn_count: u32 = 0;
    // Successful tool continuations only: retries and text-only resamples do
    // not mean work has begun. This survives compaction and resets each run.
    let mut completed_tool_rounds: u32 = 0;
    let mut tool_continuation = false;
    let mut loop_detector = if !private_review && shared.config().loop_detection {
        Some(LoopDetector::new())
    } else {
        None
    };
    let mut pending_loop_reminder: Option<RuntimeString> = None;
    let mut final_usage: Option<Usage> = None;
    // Whether this turn ever put something on screen: answer text or a tool call.
    // Kept for the settlement message only — it tells "nothing at all this turn"
    // apart from "earlier work, but no closing answer". The settlement CHECK is
    // per response (`response_produced_output` below), because a turn-level flag
    // let an empty final response inherit earlier output and settle as a normal
    // completion: the agent stopped mid-work with no closing word and no error.
    let mut produced_output = false;
    // What one response may ask for, resolved by `config_for_turn` from the model's
    // own ceiling (or the host's explicit cap).
    //
    // Mutable: the truncation ladder climbs it. A response that ran out of room is
    // evidence the budget was too small, so the recovery retry asks for more rather
    // than spending a whole response to land in the same place. `max_output_retry_
    // tokens` names the rung; unset doubles the resolved budget. The provider still
    // clamps to the model's own declared ceiling (`clamp_output_tokens`), so a host
    // that declares one keeps the ladder inside it.
    //
    // The fallback covers a hand-built config in a test.
    let mut resolved_max_output = shared
        .config()
        .max_output_tokens
        .unwrap_or(DEFAULT_TURN_OUTPUT_TOKENS);
    let mut effective_max_output = resolved_max_output;
    // The rung the truncation ladder retries with, resolved once: an explicit host
    // value, else double the budget that just failed.
    let mut truncation_retry_output = shared
        .config()
        .max_output_retry_tokens
        .unwrap_or_else(|| resolved_max_output.saturating_mul(2));
    let mut stop_continuations: u32 = 0;
    let mut empty_content_retries: u32 = 0;
    let mut empty_closing_retries: u32 = 0;
    let mut total_output_tokens: u64 = 0;
    let mut budget_continuations: u32 = 0;
    let mut budget_low_delta_streak: u32 = 0;
    let mut last_budget_tokens: u64 = 0;
    let mut fallback_chain: Vec<RuntimeString> = shared.config().fallback_models;
    let mut output_schema_resamples: u32 = 0;
    let mut turn_pdf_bytes = shared.params.input_pdf_bytes;
    let mut turn_pdf_documents = shared.params.input_pdf_documents;

    let signal = shared.params.signal.clone();
    let cwd = shared.cwd.clone();
    let thread_id = shared.params.thread_id.clone();

    if !receive_passive_mailbox(&shared, &mut messages).await {
        return;
    }

    // Session/prompt hooks fire once at turn entry; their context is folded into
    // the system prompt to keep role alternation intact.
    let Some(hooks_allowed) = await_turn_preparation(
        &shared,
        "entry-hooks",
        run_turn_entry_hooks(&shared, &cwd, &thread_id, &messages, &mut system_prompt),
    )
    .await
    else {
        emit_preparation_aborted(&shared, None);
        return;
    };
    if !hooks_allowed {
        return;
    }

    // Ephemeral per-turn reminders.
    let plan_file_path = shared.params.plan_file_path.clone();
    let plan_file_exists = plan_file_path.as_ref().map(|path| {
        let host = String::from_utf8_lossy(&path.as_runtime_string().to_node_utf8()).into_owned();
        std::path::Path::new(&host).exists()
    });
    let has_task_tool = shared.params.tools.iter().any(|t| t.name() == "task");
    for provider in &shared.params.reminder_providers {
        provider.begin_turn();
        if provider.request_refresh().is_some() || provider.retained_context() {
            continue;
        }
        let ctx = ReminderContext {
            messages: messages.clone(),
            cwd: cwd.clone(),
            task_list_id: shared.task_list_id.clone(),
            task_plan: shared.task_plan.clone(),
            signal: Some(signal.clone()),

            plan_file_path: plan_file_path.clone(),
            plan_file_exists,
            has_task_tool,
        };
        let Some(text) = await_turn_preparation(
            &shared,
            "entry-reminder",
            provider.reminder_with_context_window(&ctx, shared.config().context_window),
        )
        .await
        else {
            emit_preparation_aborted(&shared, None);
            return;
        };
        if let Some(t) = text {
            system_prompt.push(SystemPromptPart {
                name: "reminder".into(),
                text: t,
                // Recomputed for every request, so it declares itself volatile:
                // a wire with no cache breakpoint must keep it out of the cached
                // system string rather than let a changed reminder invalidate it.
                cacheable: Some(false),
                cache_scope: None,
            });
        }
    }

    let mut retrying_step = false;
    let mut review_budget_compacted = false;
    loop {
        if signal.is_cancelled() {
            shared.q.push(EngineEvent::TurnCompleted {
                turn_id: shared.turn_state.turn_id.clone(),
                usage: final_usage.clone(),
                stop_reason: TerminalReason::Aborted,
            });
            return;
        }
        if let Some(context) = &async_question {
            append_admitted_messages(
                &mut messages,
                &context
                    .registry
                    .take_ready(&context.turn_id)
                    .into_iter()
                    .map(Message::User)
                    .collect::<Vec<_>>(),
            );
        }
        let normal_step = !retrying_step;
        retrying_step = false;
        if normal_step {
            review_budget_compacted = false;
            // The first guard was captured before the base factory and prompt.
            // Every later normal request refreshes before any planning callback.
            if turn_count > 0 {
                let changed = match shared.capture_step().await {
                    Ok(changed) => changed,
                    Err(error) => {
                        emit_capture_failure(&shared.q, &shared.params, error, final_usage.clone());
                        return;
                    }
                };
                bind_step_model(&shared);
                if changed {
                    let config = shared.config();
                    resolved_max_output = config
                        .max_output_tokens
                        .unwrap_or(DEFAULT_TURN_OUTPUT_TOKENS);
                    effective_max_output = resolved_max_output;
                    truncation_retry_output = config
                        .max_output_retry_tokens
                        .unwrap_or_else(|| resolved_max_output.saturating_mul(2));
                    fallback_chain = config.fallback_models.clone();
                    invalidate_context_usage(&mut messages);
                    if !private_review && config.loop_detection && loop_detector.is_none() {
                        loop_detector = Some(LoopDetector::new());
                    } else if !config.loop_detection {
                        loop_detector = None;
                    }
                    if !refresh_captured_system_prompt(&shared, &mut system_prompt).await {
                        emit_preparation_aborted(&shared, final_usage.clone());
                        return;
                    }
                }
            }
            if let Some(slot) = &shared.live_model_override {
                if let Some(new_model) = slot.lock_recover().take() {
                    let mut context = shared.turn_context.lock_recover();
                    context.model = new_model.clone();
                    context.model_override = Some(new_model);
                }
            }
            bind_step_model(&shared);
            turn_count += 1;
        }
        if shared
            .params
            .max_turns
            .is_some_and(|max_turns| turn_count > max_turns)
        {
            shared.q.push(EngineEvent::TurnCompleted {
                turn_id: shared.turn_state.turn_id.clone(),
                usage: final_usage.clone(),
                stop_reason: TerminalReason::TurnLimitExhausted,
            });
            return;
        }
        // Session-cumulative USD ceiling.
        if let (Some(cap), Some(reader)) =
            (shared.config().max_budget_usd, &shared.params.cost_so_far)
        {
            if reader() >= cap {
                shared.q.push(EngineEvent::TurnCompleted {
                    turn_id: shared.turn_state.turn_id.clone(),
                    usage: final_usage.clone(),
                    stop_reason: TerminalReason::SpendLimitExhausted,
                });
                return;
            }
        }

        let refresh_here = tool_continuation;
        tool_continuation = false;
        let refreshed = if !normal_step {
            None
        } else if let Some(refresh) = &shared.params.refresh_tools_async {
            match refresh().await {
                Ok(result) => Some(result),
                Err(error) => {
                    shared.q.push(EngineEvent::TurnFailed {
                        inference: None,
                        error: EngineError {
                            code: EngineErrorCode::Internal,
                            message: RuntimeString::from(error),
                            cause: None,
                            request_id: None,
                            provider_kind: None,
                            retry_after_ms: None,
                        },
                    });
                    return;
                }
            }
        } else if refresh_here
            || shared
                .inference
                .as_ref()
                .is_some_and(|capture| capture.next.is_some())
        {
            shared
                .params
                .refresh_tools
                .as_ref()
                .map(|refresh| super::AsyncToolRefresh {
                    tools: refresh(),
                    refresh_system_prompt: false,
                })
        } else {
            None
        };
        if let Some(refreshed) = refreshed {
            if refreshed.refresh_system_prompt
                && !refresh_captured_system_prompt(&shared, &mut system_prompt).await
            {
                emit_preparation_aborted(&shared, final_usage.clone());
                return;
            }
            let mut refreshed = ensure_tool_search(refreshed.tools);
            if let Some(policy) = &shared.params.agent_tool_policy {
                policy.retain_tools(&mut refreshed);
            }
            let specs: Vec<ToolSpec> = refreshed.iter().map(|t| to_tool_spec(t.as_ref())).collect();
            let current = shared.tools.lock_recover().clone();
            let current_specs = shared.tool_specs.lock_recover().clone();
            let changed = specs != *current_specs
                || refreshed.len() != current.len()
                || refreshed
                    .iter()
                    .zip(current.iter())
                    .any(|(a, b)| !Arc::ptr_eq(a, b));
            if changed {
                let old_mcp = count_mcp_tools(&current);
                let new_mcp = count_mcp_tools(&refreshed);
                if old_mcp != new_mcp {
                    notify(
                        &shared.q,
                        NotificationLevel::Info,
                        format!(
                            "MCP tools refreshed: {old_mcp} → {new_mcp}{}",
                            if old_mcp == 0 && new_mcp > 0 {
                                " (server reconnected)"
                            } else {
                                ""
                            }
                        ),
                    );
                }
                *shared.tools.lock_recover() = Arc::new(refreshed);
                *shared.tool_specs.lock_recover() = Arc::new(specs);
            }
        }

        // 0) Per-request external-edit scan.
        if let Some(reminder) = &shared.params.edited_files_reminder {
            if let Some(prev) = system_prompt.iter().position(|p| p.name == "edited-files") {
                system_prompt.remove(prev);
            }
            let ctx = ReminderContext {
                messages: messages.clone(),
                cwd: cwd.clone(),
                task_list_id: None,
                task_plan: None,
                signal: Some(signal.clone()),

                plan_file_path: None,
                plan_file_exists: None,
                has_task_tool: false,
            };
            let Some(note) = await_turn_preparation(
                &shared,
                "edited-files-reminder",
                reminder.reminder_with_context_window(&ctx, shared.config().context_window),
            )
            .await
            else {
                emit_preparation_aborted(&shared, final_usage.clone());
                return;
            };
            if let Some(note) = note {
                system_prompt.push(SystemPromptPart {
                    name: "edited-files".into(),
                    text: note,
                    // Rescanned before every model request this turn.
                    cacheable: Some(false),
                    cache_scope: None,
                });
            }
        }

        // 0.5) One-shot loop-recovery nudge.
        if let Some(stale) = system_prompt.iter().position(|p| p.name == "loop-recovery") {
            system_prompt.remove(stale);
        }
        if let Some(reminder) = pending_loop_reminder.take() {
            system_prompt.push(SystemPromptPart {
                name: "loop-recovery".into(),
                text: reminder,
                // A one-shot nudge for this request only.
                cacheable: Some(false),
                cache_scope: None,
            });
        }
        // 1) Pre-turn compaction check.
        let tool_specs: Arc<Vec<ToolSpec>> = {
            let tools = shared.tools.lock_recover().clone();
            let precomp = shared.tool_specs.lock_recover().clone();
            let surfaced = shared.surfaced.lock_recover();
            Arc::new(visible_tool_specs(&tools, &surfaced, &precomp))
        };
        // Request-time reminders reflect the current plan after tool execution.
        // Replace the slot even when the provider returns None: completion and
        // tool filtering must remove an earlier missing/open-plan message.
        for provider in &shared.params.reminder_providers {
            let Some(refresh) = provider.request_refresh() else {
                continue;
            };
            system_prompt.retain(|part| part.name != refresh.name);
            if !tool_specs
                .iter()
                .any(|tool| tool.name == refresh.required_tool)
            {
                continue;
            }
            let ctx = ReminderContext {
                messages: messages.clone(),
                cwd: cwd.clone(),
                task_list_id: shared.task_list_id.clone(),
                task_plan: shared.task_plan.clone(),
                signal: Some(signal.clone()),

                plan_file_path: plan_file_path.clone(),
                plan_file_exists,
                has_task_tool,
            };
            if let Some(text) = provider
                .reminder_for_request(&ctx, completed_tool_rounds)
                .await
            {
                system_prompt.push(SystemPromptPart {
                    name: refresh.name.into(),
                    text,
                    cacheable: Some(false),
                    cache_scope: None,
                });
            }
        }
        refresh_retained_context(&shared, &mut messages, &cwd, has_task_tool).await;
        let turn_override = {
            let tc = shared.turn_context.lock_recover();
            TurnOverride {
                model: tc.model.clone(),
                effort: tc.effort.clone(),
                ultra_mode: tc.ultra_mode,
            }
        };
        if let Some(state) = &shared.params.context_hint_state {
            if state.bind_model(&turn_override.model) {
                let source_ids = messages
                    .iter()
                    .map(super::query_message_uuid)
                    .cloned()
                    .collect();
                invalidate_context_usage(&mut messages);
                persist_history_repair(&shared.params, &messages, source_ids).await;
            }
        }
        let mut prepared = shared.prepare(
            &messages,
            &system_prompt,
            effective_max_output,
            &tool_specs,
            Some(&turn_override),
        );
        if let Err(error) = run_turn_tool_result_budget(
            &shared,
            &mut messages,
            &mut prepared,
            &system_prompt,
            &tool_specs,
            &turn_override,
            effective_max_output,
        )
        .await
        {
            shared
                .params
                .retained_tool_tasks
                .as_ref()
                .expect("query owns retained tool tasks")
                .record_failure(error.message.clone());
            shared.q.push(EngineEvent::TurnFailed {
                inference: None,
                error: crate::types::errors::EngineError {
                    code: crate::types::errors::EngineErrorCode::Internal,
                    message: error.message,
                    cause: None,
                    request_id: None,
                    provider_kind: None,
                    retry_after_ms: None,
                },
            });
            return;
        }
        // Observe the rendered prefix after offload without starving compaction
        // or actual provider-error recovery on a heuristic size estimate.
        shared.observe_fixed_prefix_pressure(&prepared.request);
        if let Err(error) = shared.validate_media(&prepared.request) {
            emit_input_failure(&shared.q, *error);
            return;
        }
        if request_contains_document(&prepared.request) {
            if let Err(error) = shared.validate_pdf(&prepared.request.model) {
                emit_input_failure(&shared.q, *error);
                return;
            }
        }
        if !run_turn_compaction_check(
            &shared,
            &cwd,
            &thread_id,
            &turn_user_uuid,
            &mut system_prompt,
            &tool_specs,
            &turn_override,
            effective_max_output,
            &mut messages,
            &mut prepared,
        )
        .await
        {
            return;
        }
        // Compaction may have removed authoritative context after the first
        // request preparation. Restore it before crossing the provider boundary.
        if refresh_retained_context(&shared, &mut messages, &cwd, has_task_tool).await {
            prepared = shared.prepare(
                &messages,
                &system_prompt,
                effective_max_output,
                &tool_specs,
                Some(&turn_override),
            );
        }
        // 2/3) Stream + dispatch, with streaming-fallback retry on transient
        // errors.
        let StreamTurnResult {
            outcome,
            retry_preflight,
            admitted_messages,
            admitted_system,
            failure_recovery,
        } = run_turn_stream_with_retry(
            &shared,
            &signal,
            &messages,
            &mut prepared,
            &mut fallback_chain,
            &mut loop_detector,
        )
        .await;
        append_admitted_messages(&mut messages, &admitted_messages);
        append_admitted_system(&mut system_prompt, &admitted_system);
        if retry_preflight {
            continue;
        }

        let (
            mut messages_next,
            usage,
            tool_results,
            stop_reason,
            final_text,
            sleep_ran,
            truncated_tool_calls,
            _tool_result_handoff,
        ) = match outcome {
            Outcome::InputError { error } => {
                emit_input_failure(&shared.q, error);
                return;
            }
            Outcome::Fatal { error } => {
                let message = error.message.clone();
                shared.q.push(EngineEvent::TurnFailed {
                    inference: None,
                    error: EngineError {
                        code: EngineErrorCode::Internal,
                        message,
                        cause: Some(Box::new(error)),
                        request_id: None,
                        provider_kind: None,
                        retry_after_ms: None,
                    },
                });
                return;
            }
            Outcome::ProviderAdmissionError { message } => {
                let review_budget = shared
                    .params
                    .agent_type
                    .as_ref()
                    .is_some_and(|kind| kind == "approval-review")
                    && shared
                        .params
                        .provider_call_admission
                        .as_ref()
                        .is_some_and(|admission| admission.input_budget_exceeded());
                if review_budget
                    && !review_budget_compacted
                    && shared.deps().compactor.reactive_enabled()
                {
                    review_budget_compacted = true;
                    shared
                        .deps()
                        .compactor
                        .prepare_request(&prepared.request, &signal);
                    shared
                        .deps()
                        .compactor
                        .bind_output_observer(shared.params.compaction_output.clone());
                    match run_turn_compact(
                        &shared,
                        &cwd,
                        &thread_id,
                        &turn_user_uuid,
                        &prepared.profile,
                        &mut messages,
                        fixed_prefix_tokens(&prepared.request),
                        CompactionPhase::Recovery,
                    )
                    .await
                    {
                        Ok(_) => {
                            if !refresh_captured_system_prompt(&shared, &mut system_prompt).await {
                                emit_preparation_aborted(&shared, final_usage.clone());
                                return;
                            }
                            retrying_step = true;
                            continue;
                        }
                        Err(CompactionFailure::Aborted) => {
                            emit_compaction_failure(&shared, CompactionFailure::Aborted);
                            return;
                        }
                        Err(CompactionFailure::Service(error)) => {
                            if let Some(admission) = &shared.params.provider_call_admission {
                                admission.clear_input_budget_failure();
                            }
                            shared.q.push(EngineEvent::TurnFailed {
                                inference: None,
                                error,
                            });
                            return;
                        }
                        Err(CompactionFailure::Local(_)) => {}
                    }
                }
                // The admission refusal can be the session's shutdown gate
                // (its message carries the shutdown marker through the stage
                // prefix): the Turn then ends as an abort, like any user stop,
                // not as a red internal failure.
                if crate::engine::session::is_session_shutdown_refusal(&message)
                    || message
                        .try_to_utf8()
                        .is_ok_and(|message| message.contains("goal admission superseded"))
                {
                    shared.q.push(EngineEvent::TurnCompleted {
                        turn_id: shared.turn_state.turn_id.clone(),
                        usage: final_usage.clone(),
                        stop_reason: TerminalReason::Aborted,
                    });
                    return;
                }
                shared.q.push(EngineEvent::TurnFailed {
                    inference: None,
                    error: EngineError {
                        code: EngineErrorCode::Internal,
                        message,
                        cause: None,
                        request_id: None,
                        provider_kind: None,
                        retry_after_ms: None,
                    },
                });
                return;
            }
            Outcome::Error {
                error,
                emitted_text: _,
                partial_uuid: _,
            } => {
                let e = error;
                if !private_review && e.kind == ProviderErrorKind::PromptTooLong {
                    if let Some(window) = &shared.params.context_window {
                        window
                            .mark_full(crate::context::compact::window::actual_model_window(
                                &shared.config(),
                            ))
                            .await;
                    }
                }
                if e.media_repair_complete {
                    let reason = if signal.is_cancelled() {
                        "request-media-recovery-aborted"
                    } else {
                        "request-media-recovery-exhausted"
                    };
                    emit_provider_failure(&shared, e, reason);
                    return;
                }
                // A user interrupt can land here: the provider stream dies
                // the moment its signal is cancelled, which surfaces as a
                // Network error one check too late for the retry gate.
                // Cancellation of a transport remains an interruption. A size
                // rejection already received from the service keeps its original
                // error even when cancellation prevents the recovery operation.
                if signal.is_cancelled() {
                    if is_reactive_rejection(&e) {
                        emit_provider_failure(&shared, e, "reactive-compaction-aborted");
                    } else {
                        shared.q.push(EngineEvent::TurnCompleted {
                            turn_id: shared.turn_state.turn_id.clone(),
                            usage: final_usage.clone(),
                            stop_reason: TerminalReason::Aborted,
                        });
                    }
                    return;
                }
                if is_reactive_rejection(&e) {
                    if !private_review && e.kind == ProviderErrorKind::PromptTooLong {
                        emit_provider_failure(
                            &shared,
                            e,
                            "context-window-full-next-turn-compaction-required",
                        );
                        return;
                    }
                    if if private_review {
                        review_budget_compacted
                    } else {
                        has_attempted_reactive_compact
                    } {
                        emit_provider_failure(&shared, e, "reactive-attempt-exhausted");
                        return;
                    }
                    if !shared.deps().compactor.reactive_enabled() {
                        emit_provider_failure(&shared, e, "reactive-compaction-disabled");
                        return;
                    }
                    shared
                        .deps()
                        .compactor
                        .prepare_request(&prepared.request, &signal);
                    shared
                        .deps()
                        .compactor
                        .bind_output_observer(shared.params.compaction_output.clone());
                    if private_review {
                        review_budget_compacted = true;
                    }
                    has_attempted_reactive_compact = true;
                    let recovered = run_turn_compact(
                        &shared,
                        &cwd,
                        &thread_id,
                        &turn_user_uuid,
                        &prepared.profile,
                        &mut messages,
                        fixed_prefix_tokens(&prepared.request),
                        CompactionPhase::Recovery,
                    )
                    .await;
                    if signal.is_cancelled() {
                        emit_provider_failure(&shared, e, "reactive-compaction-aborted");
                        return;
                    }
                    if let Err(reason) = recovered {
                        match reason {
                            CompactionFailure::Aborted => {
                                emit_compaction_failure(&shared, CompactionFailure::Aborted);
                            }
                            CompactionFailure::Local(reason) => {
                                emit_provider_failure(&shared, e, reason)
                            }
                            CompactionFailure::Service(error) => {
                                if shared
                                    .params
                                    .agent_type
                                    .as_ref()
                                    .is_some_and(|kind| kind == "approval-review")
                                {
                                    shared.q.push(EngineEvent::TurnFailed {
                                        inference: None,
                                        error,
                                    });
                                } else {
                                    emit_provider_failure(&shared, e, error.message);
                                }
                            }
                        }
                        return;
                    }
                    if !refresh_captured_system_prompt(&shared, &mut system_prompt).await {
                        emit_preparation_aborted(&shared, final_usage.clone());
                        return;
                    }
                    // A 413 is recovery inside this step, not a fresh admission.
                    retrying_step = true;
                    continue;
                }
                if e.kind == ProviderErrorKind::Auth {
                    // Antigravity stamps a stable tag on the refusal. Replacing it
                    // with the generic auth sentence makes a network failure or an
                    // account ban read as "credentials rejected".
                    let antigravity_verdict = e
                        .code
                        .as_ref()
                        .and_then(|code| code.try_to_utf8().ok())
                        .is_some_and(|code| code.starts_with("antigravity_"))
                        || e.message
                            .try_to_utf8()
                            .ok()
                            .is_some_and(|message| message.contains("antigravity_"));
                    if antigravity_verdict
                        && shared
                            .params
                            .agent_type
                            .as_ref()
                            .is_none_or(|kind| kind != "approval-review")
                    {
                        emit_provider_failure(&shared, e, "auth: antigravity verdict");
                        return;
                    }
                    // The turn ends with a fixed sentence, so this record is the
                    // only place the refusal's identity survives: which upstream,
                    // what status, and the request id to match against the
                    // service's own logs.
                    record_provider_failure(&shared, &e, "auth: terminal by policy");
                    if shared
                        .params
                        .agent_type
                        .as_ref()
                        .is_some_and(|kind| kind == "approval-review")
                    {
                        shared.q.push(EngineEvent::TurnFailed {
                            inference: None,
                            error: EngineError::from_provider(e, "auth: terminal by policy"),
                        });
                    }
                    shared.q.push(EngineEvent::TurnCompleted {
                        turn_id: shared.turn_state.turn_id.clone(),
                        usage: final_usage.clone(),
                        stop_reason: TerminalReason::AuthFailed,
                    });
                    return;
                }
                emit_provider_failure(&shared, e, failure_recovery);
                return;
            }
            Outcome::Ok {
                messages,
                usage,
                tool_results,
                stop_reason,
                final_text,
                sleep_ran,
                truncated_tool_calls,
                tool_result_handoff,
            } => (
                messages,
                usage,
                tool_results,
                stop_reason,
                final_text,
                sleep_ran,
                truncated_tool_calls,
                tool_result_handoff,
            ),
        };

        // A successful provider response ends this overflow episode. A later
        // tool result may need a new recovery, even within the same user turn.
        has_attempted_reactive_compact = false;

        // The authoritative usage is carried on the assistant message itself.
        if let Some(usage) = &usage {
            final_usage = Some(usage.clone());
            if shared.params.on_message.is_none() {
                if let Some(window) = &shared.params.context_window {
                    let active = crate::context::tokens::tokens_from_usage(usage);
                    window
                        .observe(active.saturating_sub(usage.output_tokens), active)
                        .await;
                }
            }
        }
        // Per-response output: did THIS provider response put answer text or a
        // tool call on screen? This, not the turn-level flag, is what settlement
        // checks — an empty final response must not inherit earlier output and
        // settle as a normal completion. (`produced_output` stays only to word
        // the failure: "nothing all turn" vs "earlier work, no closing answer".)
        let response_produced_output =
            !trim_ecmascript_whitespace(&final_text).is_empty() || !tool_results.is_empty();
        // A response that produced no visible text or tool calls may still have
        // carried internal reasoning. Treating that as "empty" lets us retry
        // once and ask the model to surface an answer, instead of failing the
        // turn immediately when a reasoning model stops after thinking only.
        let response_is_thinking_only = messages_next.iter().any(|message| {
            if let Message::Assistant(assistant) = message {
                !assistant.content.is_empty()
                    && assistant.content.iter().all(|block| {
                        matches!(
                            block,
                            ContentBlock::Thinking(_) | ContentBlock::RedactedThinking(_)
                        )
                    })
            } else {
                false
            }
        });
        if response_produced_output {
            produced_output = true;
        }
        total_output_tokens += usage.as_ref().map(|u| u.output_tokens).unwrap_or(0);
        // Dispatch and advisor tools need the complete request view. Only its
        // conversational output crosses back into canonical history here.
        messages_next.retain(|message| !is_workspace_instruction_projection(message));
        messages = std::mem::take(&mut messages_next);

        // 4.5) Behavioral loop guard (two-strike).
        if !run_turn_loop_guard(
            &shared,
            &tool_results,
            &mut loop_detector,
            &mut pending_loop_reminder,
        )
        .await
        {
            return;
        }

        // 4.6) Goal accounting. Charging per provider response is what lets a
        // budget stop land INSIDE the turn that crossed it: the wrap-up
        // instruction joins this turn's messages instead of arriving a whole
        // turn late.
        if let Some(goal) = shared.params.goal.clone() {
            let wrap_up = match goal
                .account_progress(
                    crate::goal::types::GoalAccountingMode::ActiveOnly,
                    crate::goal::accounting::BudgetLimitedGoalDisposition::KeepActive,
                )
                .await
            {
                Ok(value) => value,
                Err(error) => {
                    shared.q.push(EngineEvent::TurnFailed {
                        inference: None,
                        error: EngineError {
                            code: EngineErrorCode::Internal,
                            message: format!("goal accounting failed: {error}").into(),
                            cause: Some(Box::new(error)),
                            request_id: None,
                            provider_kind: None,
                            retry_after_ms: None,
                        },
                    });
                    return;
                }
            };
            if let Some(wrap_up) = wrap_up {
                // Goal steering remains contextual user input; objective data
                // never gains developer or system authority.
                let goal_input_id: RuntimeString = (shared.deps.uuid)().into();
                goal.register_context_input(goal_input_id.clone(), true);
                let steer = Message::User(UserMessage {
                    question_answer: None,
                    uuid: goal_input_id,
                    is_meta: Some(true),
                    content: vec![ContentBlock::Text(TextBlock {
                        text: wrap_up.into(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })],
                });
                // This notice is pending request context. Persisting it as history
                // before a request consumes it would duplicate the next idle
                // wrap-up input. SQLite retains pending/delivered state, and the
                // provider admission receipt acknowledges the actual delivery.
                messages.push(steer);
            }
        }

        if private_review && stop_reason == ModelStopReason::MaxTokens {
            shared.q.push(EngineEvent::TurnFailed {
                inference: None,
                error: EngineError {
                    code: EngineErrorCode::ProviderError,
                    message: "Approval review response was incomplete because the output limit was reached.".into(),
                    cause: None, request_id: None,
                    provider_kind: Some(ProviderErrorKind::MaxOutput), retry_after_ms: None,
                },
            });
            return;
        }

        // 4.7) Output-truncation recovery ladder. Three distinct shapes, each with
        // its own response:
        //
        //   a) A tool call whose ARGUMENTS were cut off mid-JSON. The call was
        //      dropped, so nothing ran; the fix is more room (or less output), not
        //      a continuation — the model has to re-emit the call.
        //   b) A closing response cut off WITH visible text already on screen. The
        //      answer exists but stops mid-sentence, so ask for the rest.
        //   c) A closing response cut off having produced NO visible text: the
        //      whole budget went to reasoning. Asking for "the rest" would be
        //      meaningless — there is no rest — so ask for shorter thinking, and
        //      once that ladder is spent, stop and ask the user rather than
        //      settling a turn that produced nothing.
        //
        // Checked BEFORE the tool-result step because a dropped call leaves an error
        // result behind, which that step would otherwise treat as ordinary tool
        // output and hand straight back to the model.
        //
        // Only the (a) branch may fall through to step 5: (b) and (c) both
        // `continue`, and a response that produced tool results MUST reach step 5 —
        // that step is what persists them, drains the queue and folds in pdf
        // accounting. So (b) and (c) are gated on "this response produced nothing
        // but text", which is exactly what `response_produced_output` already
        // distinguishes. A tool-only response that hit the ceiling is neither a
        // half-written answer nor reasoning that ate the budget; it is a completed
        // tool round whose results have to land.
        if !private_review
            && (stop_reason == ModelStopReason::MaxTokens || !truncated_tool_calls.is_empty())
            && !signal.is_cancelled()
        {
            if !truncated_tool_calls.is_empty() {
                // (a): no `continue` here and no separate message. The dropped
                // call's error result already carries the retry instruction (it is
                // appended to `messages` by the assembler and persisted by step 5),
                // so the only loop-side action is to give the reissue a bigger
                // budget. This also covers invalid calls whose usage reached the
                // budget while the gateway reported tool completion. Falling
                // through keeps step 5's queue drain and pdf accounting intact.
                climb_budget_for_tool_over_limit(
                    &shared,
                    &mut effective_max_output,
                    truncation_retry_output,
                );
            } else if tool_results.is_empty() && !trim_ecmascript_whitespace(&final_text).is_empty()
            {
                // (b): answer exists but stops mid-sentence.
                if inject_truncation_continuation(
                    &shared,
                    &prepared,
                    &mut effective_max_output,
                    truncation_retry_output,
                    &mut messages,
                )
                .await
                {
                    continue;
                }
            } else if tool_results.is_empty() && !shared.truncation.lock_recover().footnote_written
            {
                // (c): nothing visible came back. The assembler's footnote branch
                // skips this shape on purpose — a footnote on an empty message
                // would be the only thing in it.
                if empty_content_retries < MAX_EMPTY_CONTENT_RETRIES {
                    empty_content_retries += 1;
                    inject_reasoning_over_limit_reminder(
                        &shared,
                        &prepared,
                        &mut effective_max_output,
                        truncation_retry_output,
                        &mut messages,
                    )
                    .await;
                    continue;
                }
                // Ladder spent. Qoder parks the turn in a manual-confirm node
                // rather than settling it; the equivalent here is to block on the
                // user. Three outcomes, because "nobody to ask" must not read as
                // "keep going" — that would loop forever with no visible output.
                match reasoning_over_limit_confirmation(&shared).await {
                    EmptyClosingDecision::Retry => {
                        empty_content_retries = 0;
                        inject_reasoning_over_limit_reminder(
                            &shared,
                            &prepared,
                            &mut effective_max_output,
                            truncation_retry_output,
                            &mut messages,
                        )
                        .await;
                        continue;
                    }
                    EmptyClosingDecision::Stop => {
                        shared.q.push(EngineEvent::TurnFailed { inference: None,
                            error: EngineError {
                                code: EngineErrorCode::ProviderError,
                                message: "the model spent its whole output budget on reasoning and returned no visible answer, and the retries did not change that; you chose to stop".into(),
                                cause: None,
                                request_id: None,
                                provider_kind: Some(ProviderErrorKind::MaxOutput),
                                retry_after_ms: None,
                            },
                        });
                        return;
                    }
                    // No way to ask (headless, sub-agent): fall through to the
                    // settlement below, which fails the turn with the truncation
                    // wording rather than looping or settling as a completion.
                    EmptyClosingDecision::Settle => {}
                }
            }
        }

        // 4.8) Empty closing response that stopped NORMALLY (not at the output
        // ceiling) after the turn already produced visible work. The truncation
        // ladder above does not cover this shape: nothing was cut off, the model
        // simply answered with no content and a normal stop. Left alone it settles
        // as a completion and the agent looks like it stopped working on its own,
        // mid-task, with no error and no closing message. One retry asks the model
        // to close or continue; if it answers empty again the settlement below
        // fails the turn instead of pretending the work finished.
        if !private_review
            && !signal.is_cancelled()
            && stop_reason != ModelStopReason::MaxTokens
            && !response_produced_output
            && (produced_output || response_is_thinking_only)
            && empty_closing_retries < MAX_EMPTY_CLOSING_RETRIES
            && !shared
                .params
                .goal
                .as_ref()
                .is_some_and(|goal| goal.is_automatic_turn())
        {
            empty_closing_retries += 1;
            notify(
                &shared.q,
                NotificationLevel::Warn,
                "the model returned an empty response; asking it to close or continue".to_string(),
            );
            let has_prior_work = produced_output;
            let nudge = provider_meta_nudge(
                &shared,
                render_empty_closing_retry(&prepared.profile, has_prior_work),
            );
            messages.push(nudge.clone());
            if let Some(cb) = &shared.params.on_message {
                if cb(nudge).await.is_err() {
                    return;
                }
            }
            continue;
        }

        // 5) Tool results produced → loop again so the model can react.
        if !tool_results.is_empty() {
            if !run_turn_tool_results(
                &shared,
                &signal,
                sleep_ran,
                &tool_results,
                &final_usage,
                &mut messages,
                &mut turn_pdf_bytes,
                &mut turn_pdf_documents,
            )
            .await
            {
                return;
            }
            completed_tool_rounds += 1;
            tool_continuation = true;
            continue;
        }

        if stop_reason == ModelStopReason::Continue {
            continue;
        }

        // 6) Output truncated (max_tokens) with no tool calls.
        //
        // Nothing to warn about here: the recovery ladder above already told the
        // user about every continuation it granted, and once that ladder is spent
        // the truncation is written into the answer itself (streamed live AND
        // persisted with the message body), so a reader who returns to this
        // conversation later still learns the answer was cut short. A separate
        // notice would say the same thing twice, on a channel that does not
        // survive a reload.
        //
        // The old comment here claimed the budget "already is what the model
        // accepts, so there is nothing left to raise". That was true while the
        // budget was fixed for the turn; the ladder now climbs it, so the claim no
        // longer holds and the branch it justified is gone.

        // 6b) Structured-output validity.
        if shared
            .params
            .agent_type
            .as_ref()
            .is_none_or(|kind| kind != "approval-review")
            && shared.params.output_schema.is_some()
            && !trim_ecmascript_whitespace(&final_text).is_empty()
            && !is_valid_runtime_json(&final_text)
            && output_schema_resamples < MAX_OUTPUT_SCHEMA_RESAMPLES
        {
            output_schema_resamples += 1;
            let nudge =
                provider_meta_nudge(&shared, render_structured_output_retry(&prepared.profile));
            messages.push(nudge.clone());
            if let Some(cb) = &shared.params.on_message {
                if cb(nudge).await.is_err() {
                    return;
                }
            }
            notify(
                &shared.q,
                NotificationLevel::Warn,
                "output not valid JSON; re-sampling".to_string(),
            );
            continue;
        }

        // 7) Stop / SubagentStop hook may force the turn to continue.
        if run_turn_stop_hook(
            &shared,
            &cwd,
            &thread_id,
            &prepared,
            &mut messages,
            &mut stop_continuations,
        )
        .await
        {
            continue;
        }

        // 8) Done — unless a loop-recovery nudge is pending.
        if pending_loop_reminder.is_some() {
            continue;
        }

        // 9) Token-budget auto-continue.
        if run_turn_budget_continue(
            &shared,
            &prepared,
            total_output_tokens,
            &mut messages,
            &mut budget_continuations,
            &mut budget_low_delta_streak,
            &mut last_budget_tokens,
        )
        .await
        {
            continue;
        }

        // Optional answers accepted during a text-only final response still belong
        // to THIS turn. Draining and closing admission share the registry lock.
        if let Some(context) = &async_question {
            let needs_continuation = loop {
                let answers = context
                    .registry
                    .drain_or_close(&context.turn_id, &signal)
                    .await;
                if answers.is_empty() {
                    break false;
                }
                let previous_len = messages.len();
                append_admitted_messages(
                    &mut messages,
                    &answers.into_iter().map(Message::User).collect::<Vec<_>>(),
                );
                if messages.len() != previous_len {
                    break true;
                }
                // A committed checkpoint can already contain these answers.
                // Drain again to close admission atomically after acknowledging them.
            };
            if needs_continuation {
                continue;
            }
        }

        // 10) Nothing visible came back. Three distinct situations are failures,
        // and only these:
        //
        // a) The closing response was CUT OFF at the model's output ceiling
        //    (`MaxTokens`) having produced no visible text at all. The model was
        //    mid-answer and the whole budget went to reasoning the user never sees.
        //    Settling that as a completion is what made a 62-minute turn end with
        //    no closing message and no error: on screen the agent had simply
        //    stopped working. This is checked against THIS response's stop reason,
        //    not against the turn — a turn-level flag let the empty closing
        //    response inherit the output of earlier tool rounds and slip through.
        //
        // b) The whole turn produced nothing: no answer text and no tool call in
        //    any response. Empty completions are real (an OpenAI-compatible gateway
        //    that answers with no content), so name what happened rather than
        //    reporting success with nothing in it.
        //
        // c) The closing response was EMPTY but stopped NORMALLY, after earlier
        //    visible work. The step-4.8 ladder already spent its retry asking the
        //    model to close or continue; arriving here means the retry came back
        //    empty too. Settling that as a completion is the same silent stop as
        //    (a) with a different stop reason: the task is unfinished (the model
        //    never said it was done), so the turn fails and says why.
        //
        // A stop the user asked for is exempt: an aborted request is expected to
        // come back empty, and it is settled as an interrupt elsewhere.
        if !private_review
            && !signal.is_cancelled()
            && !shared
                .params
                .goal
                .as_ref()
                .is_some_and(|goal| goal.is_automatic_turn())
        {
            let truncated = stop_reason == ModelStopReason::MaxTokens;
            let closing_response_empty = !response_produced_output;
            let turn_produced_nothing = !produced_output;
            if closing_response_empty || turn_produced_nothing {
                if !turn_produced_nothing && !truncated {
                    // (c) A normal-stop empty closing after visible work: the
                    // step-4.8 ladder spent its single retry. Park on the user
                    // the way the truncation ladder does once its retries are
                    // spent, instead of failing the turn outright — the work is
                    // on screen and the model may still be able to close.
                    match empty_closing_confirmation(&shared).await {
                        EmptyClosingDecision::Retry => {
                            empty_closing_retries = 0;
                            let nudge = provider_meta_nudge(
                                &shared,
                                render_empty_closing_retry(&prepared.profile, true),
                            );
                            messages.push(nudge.clone());
                            if let Some(cb) = &shared.params.on_message {
                                if cb(nudge).await.is_err() {
                                    return;
                                }
                            }
                            continue;
                        }
                        EmptyClosingDecision::Stop => {
                            shared.q.push(EngineEvent::TurnFailed {
                                inference: None,
                                error: EngineError {
                                    code: EngineErrorCode::ProviderError,
                                    message: "the model returned an empty closing response after doing visible work, and the retry did not produce one either; you chose to stop".into(),
                                    cause: None,
                                    request_id: None,
                                    provider_kind: None,
                                    retry_after_ms: None,
                                },
                            });
                            return;
                        }
                        // Nobody to ask (headless, sub-agent): fall through to
                        // the failure below, which keeps the pre-card wording.
                        EmptyClosingDecision::Settle => {}
                    }
                }
                let message = if turn_produced_nothing {
                    if truncated {
                        "the model returned no content: the output token limit was reached before any text"
                    } else {
                        "the model returned no content for this turn"
                    }
                } else if truncated {
                    // Earlier work is on screen; only the closing answer was lost.
                    "the model reached its output limit before producing any text in its closing response, so the turn ended without a closing message"
                } else {
                    "the model returned an empty closing response after doing visible work, and the retry did not produce one either, so the turn ended unfinished without a closing message"
                };
                shared.q.push(EngineEvent::TurnFailed {
                    inference: None,
                    error: EngineError {
                        code: EngineErrorCode::ProviderError,
                        message: message.into(),
                        cause: None,
                        request_id: None,
                        // The failure surface localizes by kind, and the raw
                        // sentence is English: naming the cause is what lets the
                        // renderer say "the answer was cut off by the output
                        // limit" instead of echoing this string verbatim.
                        provider_kind: truncated.then_some(ProviderErrorKind::MaxOutput),
                        retry_after_ms: None,
                    },
                });
                return;
            }
        }

        // Finish optional maintenance inside this task so its checkpoint cannot
        // race the next user turn or replace a conversation after cancellation.
        let config = shared.config();
        let prefix_tokens = fixed_prefix_tokens(&prepared.request);
        let estimate = estimate_context_tokens_with_prefix(&messages, prefix_tokens);
        let active = shared
            .params
            .context_window
            .as_ref()
            .map_or(estimate, |state| state.sample(estimate));
        let status = crate::context::compact::policy::context_window_status(
            &config,
            active,
            shared
                .params
                .context_window
                .as_ref()
                .and_then(|state| state.prefill()),
        );
        let pending_input = shared.params.message_queue.as_ref().is_some_and(|queue| {
            queue
                .admitted_snapshot()
                .iter()
                .any(|input| input.agent_id == shared.params.agent_id)
        });
        if !config.compaction_disabled
            && config.auto_compaction
            && config.post_turn_compact_threshold_percent > 0
            && status.turn_end_compaction_threshold_reached
            && !pending_input
            && !signal.is_cancelled()
        {
            let summary_request = shared.prepare(
                &messages,
                &system_prompt,
                effective_max_output,
                &tool_specs,
                Some(&turn_override),
            );
            shared
                .deps()
                .compactor
                .prepare_request(&summary_request.request, &signal);
            shared
                .deps()
                .compactor
                .bind_output_observer(shared.params.compaction_output.clone());
            match run_turn_compact(
                &shared,
                &cwd,
                &thread_id,
                &turn_user_uuid,
                &summary_request.profile,
                &mut messages,
                fixed_prefix_tokens(&summary_request.request),
                CompactionPhase::AfterTurn,
            )
            .await
            {
                Ok(()) => {}
                Err(CompactionFailure::Aborted) => {
                    shared.q.push(EngineEvent::TurnCompleted {
                        turn_id: shared.turn_state.turn_id.clone(),
                        usage: final_usage.clone(),
                        stop_reason: TerminalReason::Aborted,
                    });
                    return;
                }
                Err(CompactionFailure::Local(reason)) => {
                    notify(
                        &shared.q,
                        NotificationLevel::Warn,
                        RuntimeString::from(
                            "Post-turn compaction failed; the completed answer was preserved: ",
                        )
                        .concat(&reason),
                    );
                }
                Err(CompactionFailure::Service(error)) => {
                    // A session budget refusal ends automatic work while the completed
                    // answer remains in both live and durable conversation history.
                    shared.q.push(EngineEvent::TurnFailed {
                        inference: None,
                        error,
                    });
                    return;
                }
            }
        }
        shared.q.push(EngineEvent::TurnCompleted {
            turn_id: shared.turn_state.turn_id.clone(),
            usage: final_usage.clone(),
            stop_reason: if signal.is_cancelled() {
                TerminalReason::Aborted
            } else {
                TerminalReason::Completed
            },
        });
        return;
    }
}

fn emit_input_failure(q: &EventQueue, error: InputValidationError) {
    q.push(EngineEvent::InputRejected {
        correlation_id: None,
        message: error.message.clone().into(),
        detail: error.detail.clone(),
    });
    q.push(EngineEvent::TurnFailed {
        inference: None,
        error: EngineError {
            code: EngineErrorCode::InvalidInput,
            message: error.message.clone().into(),
            cause: Some(Box::new(error)),
            request_id: None,
            provider_kind: None,
            retry_after_ms: None,
        },
    });
}

fn emit_capture_failure(q: &EventQueue, params: &QueryParams, error: String, usage: Option<Usage>) {
    if params.signal.is_cancelled() {
        q.push(EngineEvent::TurnCompleted {
            turn_id: params.turn_id.clone(),
            usage,
            stop_reason: TerminalReason::Aborted,
        });
    } else {
        q.push(EngineEvent::TurnFailed {
            inference: None,
            error: EngineError {
                code: EngineErrorCode::Internal,
                message: error.into(),
                cause: None,
                request_id: None,
                provider_kind: None,
                retry_after_ms: None,
            },
        });
    }
}

fn bind_step_model(shared: &Shared) {
    let model = shared.turn_context.lock_recover().model.clone();
    *shared.step_model.lock_recover() = model.clone();
    shared
        .q
        .bind(shared.bundle().map(|bundle| bundle.identity(model)));
}

async fn refresh_retained_context(
    shared: &Arc<Shared>,
    messages: &mut Vec<Message>,
    cwd: &RuntimePath,
    has_task_tool: bool,
) -> bool {
    refresh_retained_context_inner(shared, messages, cwd, has_task_tool, true).await
}

async fn refresh_retained_context_inner(
    shared: &Arc<Shared>,
    messages: &mut Vec<Message>,
    cwd: &RuntimePath,
    has_task_tool: bool,
    record: bool,
) -> bool {
    let mut changed = false;
    for provider in &shared.params.reminder_providers {
        if !provider.retained_context() {
            continue;
        }
        let ctx = ReminderContext {
            messages: messages.clone(),
            cwd: cwd.clone(),
            task_list_id: shared.task_list_id.clone(),
            task_plan: shared.task_plan.clone(),
            signal: Some(shared.params.signal.clone()),

            plan_file_path: shared.params.plan_file_path.clone(),
            plan_file_exists: None,
            has_task_tool,
        };
        if let Some(received) = provider.retained_messages(&ctx).await {
            for message in received {
                if messages.iter().any(|existing| {
                    super::query_message_uuid(existing) == super::query_message_uuid(&message)
                }) {
                    continue;
                }
                messages.push(message.clone());
                if let Some(on_message) = shared.params.on_message.as_ref().filter(|_| record) {
                    if on_message(message).await.is_err() {
                        return false;
                    }
                }
                changed = true;
            }
            continue;
        }
        if let Some(text) = provider
            .reminder_with_context_window(&ctx, shared.config().context_window)
            .await
        {
            let mut message = provider_meta_nudge(shared, text);
            if let Message::User(user) = &mut message {
                for block in &mut user.content {
                    if let crate::schemas::content::ContentBlock::Text(text) = block {
                        text.harness_envelope =
                            Some(crate::schemas::content::HarnessEnvelope::SystemReminder);
                    }
                }
            }
            messages.push(message.clone());
            if let Some(on_message) = shared.params.on_message.as_ref().filter(|_| record) {
                if on_message(message).await.is_err() {
                    return false;
                }
            }
            changed = true;
        }
    }
    changed
}

use crate::context::compact::full_compact::place_retained_context;

/// Only cancellable preparation belongs here. Accepted writes and provider
/// admission receipts keep their existing owners and durability barriers.
async fn await_turn_preparation<T>(
    shared: &Shared,
    stage: &'static str,
    future: impl std::future::Future<Output = T>,
) -> Option<T> {
    use crate::observability::provider_diagnostics::record_named;
    let started = tokio::time::Instant::now();
    let fields = |elapsed_ms: u128| {
        serde_json::json!({
            "sessionId": shared.params.thread_id.try_to_utf8().ok(),
            "turnId": shared.turn_state.turn_id.try_to_utf8().ok(),
            "stage": stage,
            "elapsedMs": elapsed_ms as u64,
        })
    };
    record_named("turn-preparation.log", "started", fields(0));
    let result = tokio::select! {
        biased;
        _ = shared.params.signal.cancelled() => None,
        value = future => Some(value),
    };
    record_named(
        "turn-preparation.log",
        if result.is_some() {
            "completed"
        } else {
            "cancelled"
        },
        fields(started.elapsed().as_millis()),
    );
    result
}

fn emit_preparation_aborted(shared: &Shared, usage: Option<Usage>) {
    shared.q.push(EngineEvent::TurnCompleted {
        turn_id: shared.turn_state.turn_id.clone(),
        usage,
        stop_reason: TerminalReason::Aborted,
    });
}

async fn refresh_captured_system_prompt(
    shared: &Shared,
    system: &mut Vec<SystemPromptPart>,
) -> bool {
    let turn_parts = system
        .iter()
        .filter(|part| part.name == "hook-context" || part.name == "reminder")
        .cloned()
        .collect::<Vec<_>>();
    let Some(refreshed) = await_turn_preparation(
        shared,
        "system-prompt-refresh",
        resolve_system_prompt(&shared.params.system_prompt),
    )
    .await
    else {
        return false;
    };
    *system = refreshed;
    system.extend(turn_parts);
    true
}

/// Turn-entry SessionStart/UserPromptSubmit hook collection: their context is
/// folded into the system prompt to keep role alternation intact. Returns
/// `false` when a hook blocked the prompt — the turn has already been
/// completed on the event queue.
async fn run_turn_entry_hooks(
    shared: &Arc<Shared>,
    cwd: &RuntimePath,
    thread_id: &RuntimeString,
    messages: &[Message],
    system_prompt: &mut Vec<SystemPromptPart>,
) -> bool {
    if let Some(bus) = &shared.params.hook_bus {
        let mut extra_context: Vec<RuntimeString> = Vec::new();
        if shared.params.emit_thread_started && bus.has(HookEvent::SessionStart) {
            let mut input = base_hook_input(HookEvent::SessionStart, cwd, thread_id);
            input.event = HookEvent::SessionStart;
            let s = bus.dispatch(HookEvent::SessionStart, &input).await;
            for m in s.system_messages {
                notify(&shared.q, NotificationLevel::Info, m);
            }
            extra_context.extend(s.additional_context);
        }
        if bus.has(HookEvent::UserPromptSubmit) {
            let mut input = base_hook_input(HookEvent::UserPromptSubmit, cwd, thread_id);
            input.prompt = last_user_text(messages);
            let u = bus.dispatch(HookEvent::UserPromptSubmit, &input).await;
            for m in u.system_messages {
                notify(&shared.q, NotificationLevel::Info, m);
            }
            if u.blocked || u.aborted {
                notify(
                    &shared.q,
                    NotificationLevel::Warn,
                    "Prompt blocked by hook.".to_string(),
                );
                shared.q.push(EngineEvent::TurnCompleted {
                    turn_id: shared.turn_state.turn_id.clone(),
                    usage: None,
                    stop_reason: TerminalReason::Completed,
                });
                return false;
            }
            extra_context.extend(u.additional_context);
        }
        if !extra_context.is_empty() {
            system_prompt.push(SystemPromptPart {
                name: "hook-context".into(),
                text: join_runtime_strings(&extra_context, "\n"),
                // Collected per turn, and carried across a system-prompt refresh
                // alongside the reminder — turn-scoped, never session-stable.
                cacheable: Some(false),
                cache_scope: None,
            });
        }
    }
    true
}

/// Pre-request per-message tool-result budget (request view). The dispatch-time
/// aggregate budget only ever sees FRESH results, so history written before the
/// limit existed (or before a resume) can carry results that together blow the
/// per-message ceiling on every later request. This pass enforces the same
/// ceiling over what the model is about to see: previously replaced bodies
/// re-apply byte-identically from state (no I/O), and the largest fresh results
/// in an over-budget wire group are persisted to disk with a preview in their
/// place. Decisions freeze per tool_use_id so a later pass never rewrites a
/// prefix the provider already cached — unless the session armed the one-shot
/// cold window after idling past the longest prompt-cache lifetime, in which
/// case aged keeps are re-decided once under
/// [`crate::context::tool_result_budget::ColdReclaim`].
#[allow(clippy::too_many_arguments)]
async fn run_turn_tool_result_budget(
    shared: &Arc<Shared>,
    messages: &mut [Message],
    prepared: &mut PreparedModelRequest,
    system_prompt: &[SystemPromptPart],
    tool_specs: &[ToolSpec],
    turn_override: &TurnOverride,
    effective_max_output: u64,
) -> Result<(), crate::types::errors::ToolError> {
    if !shared.config().aggregate_tool_results {
        return Ok(());
    }
    let Some(state) = shared.params.tool_result_budget_state.as_ref() else {
        return Ok(());
    };
    let config = &shared.params.config;
    let limit = config.max_tool_results_per_message_chars as usize;
    let names = crate::context::tool_result_budget::tool_name_by_use_id(messages);
    let skip: HashSet<RuntimeString> = ["read".into()].into_iter().collect();
    // Preview cap for every spill this pass performs; also the reclaim floor,
    // since a body at or below its preview cannot shrink when replaced.
    const SPILL_PREVIEW_CHARS: usize = 2_000;
    let decision = {
        let mut guard = state.lock_recover();
        // Consume the one-shot arm regardless of the knob so its semantics
        // never depend on a settings flip mid-session.
        let armed = guard.take_cold_reclaim_arm();
        let reclaim = (armed && config.reclaim_tool_results_when_cache_cold).then_some(
            crate::context::tool_result_budget::ColdReclaim {
                keep_recent: config.reclaim_tool_results_keep_recent as usize,
                min_chars: SPILL_PREVIEW_CHARS,
            },
        );
        crate::context::tool_result_budget::decide(messages, &guard, limit, &skip, &names, reclaim)
    };
    if decision.reapply.is_empty() && decision.to_persist.is_empty() {
        return Ok(());
    }
    let mut replacements: HashMap<RuntimeString, RuntimeString> =
        decision.reapply.into_iter().collect();
    let spill_dir = path_join(
        query_path_dialect(),
        &[
            (shared.params.tool_context_base)().cwd,
            RuntimePath::from(".klaus"),
            RuntimePath::from("tool-results"),
        ],
    );
    let profile = &prepared.profile;
    let mut persisted: Vec<(RuntimeString, RuntimeString)> = Vec::new();
    for (id, _size) in &decision.to_persist {
        let current = messages.iter().find_map(|message| match message {
            Message::ToolResult(result) if &result.tool_use_id == id => {
                Some(result.content.clone())
            }
            _ => None,
        });
        let Some(content) = current else { continue };
        let options = SpillOptions {
            max_chars: SPILL_PREVIEW_CHARS as f64,
            dir: spill_dir.clone(),
            id: id.concat(&".aggregate".into()),
        };
        let spill = maybe_spill_result(content, options, |preview, full, path| {
            render_spilled_tool_result(preview, full, path, profile)
        })
        .await;
        if spill.spilled_path.is_none() {
            continue;
        }
        let body = crate::tools::result_storage::output_blocks_text(&spill.content);
        replacements.insert(id.clone(), body.clone());
        persisted.push((id.clone(), body));
    }
    // Spill files already exist. Persist their replacement bodies before the
    // synchronous model-history and budget-state publication below.
    if !persisted.is_empty() {
        if let Some(record) = &shared.params.on_tool_result_budget_records {
            let _persistence = shared.persistence_in_flight.enter()?;
            record(persisted.clone()).await?;
        }
    }
    if !crate::context::tool_result_budget::apply(messages, &replacements) {
        return Ok(());
    }
    {
        let mut guard = state.lock_recover();
        crate::context::tool_result_budget::settle(&mut guard, messages, &persisted);
    }
    *prepared = prepare_model_request(
        messages,
        &shared.params,
        system_prompt,
        effective_max_output,
        tool_specs,
        Some(turn_override),
    );
    prepared.request.deferred_tools = shared.deferred_request_specs(&prepared.profile);
    Ok(())
}

fn emit_preflight_compaction_failure(shared: &Arc<Shared>, reason: RuntimeString) {
    shared.q.push(EngineEvent::TurnFailed {
        inference: None,
        error: EngineError {
            code: EngineErrorCode::ProviderError,
            message: concat_runtime_strings(&[
                "Context compaction failed before the next model request: ".into(),
                reason,
            ]),
            cause: None,
            request_id: None,
            provider_kind: None,
            retry_after_ms: None,
        },
    });
}

/// Prepare the current conversation before every normal sampling request.
#[allow(clippy::too_many_arguments)]
async fn run_turn_compaction_check(
    shared: &Arc<Shared>,
    cwd: &RuntimePath,
    thread_id: &RuntimeString,
    turn_user_uuid: &Option<RuntimeString>,
    system_prompt: &mut Vec<SystemPromptPart>,
    tool_specs: &[ToolSpec],
    turn_override: &TurnOverride,
    effective_max_output: u64,
    messages: &mut Vec<Message>,
    prepared: &mut PreparedModelRequest,
) -> bool {
    let migration = tokio::select! {
        biased;
        _ = shared.params.signal.cancelled() => Err(CompactionFailure::Aborted),
        result = model_context_migration::compact(
            shared, cwd, thread_id, turn_user_uuid, system_prompt, tool_specs,
            turn_override, effective_max_output, messages, prepared,
        ) => result,
    };
    let migrated = match migration {
        Ok(migrated) => migrated,
        Err(error) => {
            emit_compaction_failure(shared, error);
            return false;
        }
    };

    let prefix_tokens = fixed_prefix_tokens(&prepared.request);
    let estimate = super::request::rendered_context_tokens(&prepared.request);
    let active = shared
        .params
        .context_window
        .as_ref()
        .map_or(estimate, |state| state.sample(estimate));
    if std::mem::take(&mut *shared.reminder_anchor_pending.lock_recover()) {
        let spend = *shared.reminder_spend.lock_recover();
        shared
            .task_countdown_anchor
            .lock_recover()
            .reanchor(spend, active);
    }
    let config = shared.config();
    let status = crate::context::compact::policy::context_window_status(
        &config,
        active,
        shared
            .params
            .context_window
            .as_ref()
            .and_then(|state| state.prefill()),
    );
    if !migrated && !config.compaction_disabled && config.auto_compaction {
        if !status.token_limit_reached {
            // Arm the background checkpoint summary while there is still lead
            // room before the automatic-compaction boundary.
            maybe_arm_compaction_precompute(
                shared,
                &status,
                &config,
                messages,
                &prepared.request,
                prefix_tokens,
                cwd,
                thread_id,
            );
        } else {
            match consume_precomputed_compaction(
                shared,
                cwd,
                thread_id,
                turn_user_uuid,
                &prepared.profile,
                messages,
                prefix_tokens,
                &config,
                &prepared.request,
            )
            .await
            {
                Some(Ok(())) => {}
                Some(Err(failure)) => {
                    emit_compaction_failure(shared, failure);
                    return false;
                }
                None => {
                    shared
                        .deps()
                        .compactor
                        .prepare_request(&prepared.request, &shared.params.signal);
                    shared
                        .deps()
                        .compactor
                        .bind_output_observer(shared.params.compaction_output.clone());
                    if let Err(error) = run_turn_compact(
                        shared,
                        cwd,
                        thread_id,
                        turn_user_uuid,
                        &prepared.profile,
                        messages,
                        prefix_tokens,
                        CompactionPhase::BeforeRequest,
                    )
                    .await
                    {
                        emit_compaction_failure(shared, error);
                        return false;
                    }
                }
            }
            if !refresh_captured_system_prompt(shared, system_prompt).await {
                emit_compaction_failure(shared, CompactionFailure::Aborted);
                return false;
            }
            *prepared = shared.prepare(
                messages,
                system_prompt,
                effective_max_output,
                tool_specs,
                Some(turn_override),
            );
        }
    }
    let prefix_tokens = fixed_prefix_tokens(&prepared.request);
    apply_total_tokens_reminder(
        shared,
        system_prompt,
        tool_specs,
        turn_override,
        effective_max_output,
        messages,
        prepared,
        prefix_tokens,
    );
    true
}

/// Name of the volatile system part carrying the remaining-context line.
const TOTAL_TOKENS_PART: &str = "total-tokens";

/// Refresh the remaining-context line the model is shown, and re-render the
/// request around it.
///
/// The part is appended LAST on purpose. Its number moves nearly every turn, so
/// anywhere earlier it would invalidate the cached prefix on every single request;
/// after the final breakpoint only the uncached tail is re-processed.
#[allow(clippy::too_many_arguments)]
fn apply_total_tokens_reminder(
    shared: &Arc<Shared>,
    system_prompt: &mut Vec<SystemPromptPart>,
    tool_specs: &[ToolSpec],
    turn_override: &TurnOverride,
    effective_max_output: u64,
    messages: &[Message],
    prepared: &mut PreparedModelRequest,
    prefix_tokens: u64,
) {
    let config = &shared.config();
    system_prompt.retain(|part| part.name != TOTAL_TOKENS_PART);
    let estimate = estimate_context_tokens_with_prefix(messages, prefix_tokens);
    let used = shared
        .params
        .context_window
        .as_ref()
        .map_or(estimate, |state| state.sample(estimate));
    let line = {
        let mut spend = shared.reminder_spend.lock_recover();
        spend.high_water_tokens = spend.total_with(used);
        render_total_tokens_reminder(
            config,
            used,
            *spend,
            Some(&mut shared.task_countdown_anchor.lock_recover()),
        )
    };
    let Some(line) = line else {
        return;
    };
    let after_tool_batch = prepared
        .request
        .messages
        .last()
        .is_some_and(|message| match message {
            Message::ToolResult(_) => true,
            Message::User(user) => user
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::ToolResult(_))),
            _ => false,
        });
    let after_user = config.total_tokens_reminder_after_user_turn == Some(true)
        && prepared.request.messages.last().is_some_and(|message| {
            matches!(message,
            Message::User(user) if user.is_meta != Some(true) && user.question_answer.is_none())
        });
    let attachment = (after_tool_batch || after_user).then(|| {
        Message::User(UserMessage {
            uuid: RuntimeString::from("total-tokens:").concat(&shared.params.turn_id),
            content: vec![ContentBlock::Text(TextBlock {
                text: line.clone(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: Some("total_tokens_reminder".into()),
            })],
            is_meta: Some(true),
            question_answer: None,
        })
    });
    system_prompt.push(SystemPromptPart {
        name: TOTAL_TOKENS_PART.into(),
        text: line,
        cacheable: Some(false),
        cache_scope: None,
    });
    *prepared = shared.prepare(
        messages,
        system_prompt,
        effective_max_output,
        tool_specs,
        Some(turn_override),
    );
    if let Some(attachment) = attachment {
        prepared.request.messages.push(attachment);
    }
}

/// Record a terminal provider failure before the turn closes.
///
/// The transcript keeps either a fixed sentence (auth) or the provider's own
/// message, and the rollout record carries neither the status nor the request id.
/// Without this, the failure's identity — which upstream, what status, the id to
/// match against the service's logs — is gone the moment the turn ends.
/// Best-effort: a diagnostic must never change the outcome.
fn record_provider_failure(shared: &Arc<Shared>, error: &ProviderError, recovery: &str) {
    use crate::observability::provider_diagnostics::ProviderFailure;
    let model = shared.turn_context.lock_recover().model.clone();
    let model = model.try_to_utf8().ok();
    let request_id = error
        .request_id
        .as_ref()
        .and_then(|id| id.try_to_utf8().ok());
    let detail = String::from_utf8(error.message.to_node_utf8())
        .unwrap_or_else(|_| "unreadable provider message".to_string());
    let session_id = shared.params.thread_id.try_to_utf8().ok();
    let service_code = error.code.as_ref().and_then(|code| code.try_to_utf8().ok());
    ProviderFailure {
        kind: "turn_failed",
        source: None,
        model: model.as_deref(),
        error_kind: Some(error.kind.as_str()),
        status: error.status,
        service_code: service_code.as_deref(),
        request_id: request_id.as_deref(),
        detail: Some(&detail),
        recovery: Some(recovery),
        session_id: session_id.as_deref(),
    }
    .record();
}

fn is_reactive_rejection(error: &ProviderError) -> bool {
    let message = error.message.try_to_utf8().unwrap_or_default();
    matches!(
        error.kind,
        ProviderErrorKind::PromptTooLong | ProviderErrorKind::RequestTooLarge
    ) || (error.status != Some(400) && crate::provider::classify::is_media_size_error(&message))
}

fn emit_provider_failure(
    shared: &Arc<Shared>,
    error: ProviderError,
    recovery: impl Into<RuntimeString>,
) {
    let recovery = recovery.into();
    record_provider_failure(shared, &error, &recovery.try_to_utf8().unwrap_or_default());
    shared.q.push(EngineEvent::TurnFailed {
        inference: None,
        error: EngineError::from_provider(error, recovery),
    });
}

struct StreamTurnResult {
    outcome: Outcome,
    retry_preflight: bool,
    admitted_messages: Vec<Message>,
    admitted_system: Vec<SystemPromptPart>,
    failure_recovery: &'static str,
}

fn append_admitted_messages(history: &mut Vec<Message>, admitted: &[Message]) {
    for message in admitted {
        if !history.iter().any(|existing| {
            super::query_message_uuid(existing) == super::query_message_uuid(message)
        }) {
            history.push(message.clone());
        }
    }
}

fn append_admitted_system(system: &mut Vec<SystemPromptPart>, admitted: &[SystemPromptPart]) {
    for part in admitted {
        if !system.contains(part) {
            system.push(part.clone());
        }
    }
}

/// Numbered phases 2/3 of the turn loop: stream + dispatch one prepared model
/// request, with streaming-fallback retry on transient errors and model
/// fallback rotation. Returns the final `Outcome` once retries are exhausted
/// or the request settles.
#[allow(clippy::too_many_arguments)]
async fn run_turn_stream_with_retry(
    shared: &Arc<Shared>,
    signal: &crate::types::abort::AbortSignal,
    messages: &[Message],
    prepared: &mut PreparedModelRequest,
    fallback_chain: &mut Vec<RuntimeString>,
    loop_detector: &mut Option<LoopDetector>,
) -> StreamTurnResult {
    let private_review = shared
        .params
        .agent_type
        .as_ref()
        .is_some_and(|kind| kind == "approval-review");
    let outcome;
    let mut admitted_messages = Vec::new();
    let mut admitted_system = Vec::new();
    let mut attempt: u32 = 0;
    let mut media_recovery = media_recovery::MediaRecovery::default();
    let mut hint_fallback_used = false;
    let mut failure_recovery = "not-retryable";
    let mut credential_failures = HashSet::new();
    loop {
        append_admitted_messages(&mut prepared.request.messages, &admitted_messages);
        append_admitted_system(&mut prepared.request.system, &admitted_system);
        let current_attempt = attempt + 1;
        let metadata = provider_metadata(shared, &prepared.request);
        shared.q.bind(
            shared
                .bundle()
                .map(|bundle| bundle.identity(prepared.request.model.clone())),
        );
        shared
            .q
            .push(EngineEvent::StreamRequestStart { inference: None });
        shared.q.push(EngineEvent::ProviderDecision {
            inference: None,
            event: provider_observability::request_started(&metadata, current_attempt),
        });
        let request_started = tokio::time::Instant::now();
        // The account this request authenticates with, so a quota refusal can
        // name the account whose allowance is spent.
        let request_account = shared
            .bundle()
            .and_then(|bundle| bundle.provider.credential_account_id());
        let mut out = stream_and_dispatch(
            shared,
            &prepared.request,
            &prepared.profile,
            current_attempt,
            loop_detector.as_mut(),
        )
        .await;
        // The prepared request owns transport projections. Rebuild the next
        // conversation from its canonical base plus newly produced output, so
        // image gating and instruction injection cannot become durable history.
        let canonical_base_len = if let Outcome::Ok {
            messages: response_history,
            ..
        } = &mut out
        {
            let produced = response_history
                .split_off(prepared.request.messages.len().min(response_history.len()));
            *response_history = messages.to_vec();
            append_admitted_messages(response_history, &admitted_messages);
            let insertion = response_history.len();
            response_history.extend(produced);
            Some(insertion)
        } else {
            None
        };
        if let Some((messages, system)) = shared
            .params
            .provider_call_admission
            .as_ref()
            .and_then(|admission| admission.take_admitted_request_context())
        {
            if let Outcome::Ok {
                messages: response_history,
                ..
            } = &mut out
            {
                let insertion = canonical_base_len.unwrap_or(response_history.len());
                let missing = messages
                    .iter()
                    .filter(|message| {
                        !response_history.iter().any(|existing| {
                            super::query_message_uuid(existing)
                                == super::query_message_uuid(message)
                        })
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                response_history.splice(insertion..insertion, missing);
            }
            append_admitted_messages(&mut prepared.request.messages, &messages);
            append_admitted_system(&mut prepared.request.system, &system);
            append_admitted_messages(&mut admitted_messages, &messages);
            append_admitted_system(&mut admitted_system, &system);
        }
        if let Outcome::Error {
            error,
            emitted_text,
            partial_uuid,
        } = &out
        {
            if !hint_fallback_used && !private_review && !signal.is_cancelled() {
                if let Some(next) =
                    crate::context::context_hints::stream_fallback_request(&prepared.request, error)
                        .await
                {
                    if *emitted_text {
                        shared.q.push(EngineEvent::Tombstone {
                            uuid: partial_uuid.clone(),
                        });
                    }
                    hint_fallback_used = true;
                    prepared.request = next;
                    super::streaming::persist_context_hint_state(shared).await;
                    continue;
                }
            }
            shared.q.push(EngineEvent::ProviderDecision {
                inference: None,
                event: provider_observability::request_failed(&metadata, current_attempt, error),
            });
            if error.media_repair_complete {
                outcome = out;
                break;
            }
            if !private_review && !signal.is_cancelled() {
                if let Some(reason) = media_recovery.heal(
                    &mut prepared.request,
                    error,
                    prepared.profile.provider == "anthropic",
                ) {
                    if *emitted_text {
                        shared.q.push(EngineEvent::Tombstone {
                            uuid: partial_uuid.clone(),
                        });
                    }
                    crate::observability::provider_diagnostics::record_named(
                        crate::observability::provider_diagnostics::TIMING_LOG_FILE_NAME,
                        "retry",
                        serde_json::json!({
                            "purpose": "media_repair",
                            "reason": reason,
                            "status": error.status,
                            "errorKind": error.kind.as_str(),
                            "requestId": error.request_id.as_ref().and_then(|id| id.try_to_utf8().ok()),
                            "model": prepared.request.model.try_to_utf8().ok(),
                            "sessionId": shared.params.thread_id.try_to_utf8().ok(),
                        }),
                    );
                    notify(
                        &shared.q,
                        NotificationLevel::Info,
                        "Retrying after removing rejected media.".to_string(),
                    );
                    continue;
                }
            }
            if !matches!(
                error.kind,
                ProviderErrorKind::PromptTooLong | ProviderErrorKind::RequestTooLarge
            ) && crate::context::media_budget::parse_media_rejection(
                error.status,
                &error.message.try_to_utf8().unwrap_or_default(),
            )
            .is_some()
            {
                let mut error = error.clone();
                error.media_repair_complete = true;
                outcome = Outcome::Error {
                    error,
                    emitted_text: *emitted_text,
                    partial_uuid: partial_uuid.clone(),
                };
                break;
            }
            // Credential rotation: this account's allowance is spent — advance
            // the multi-account queue and re-sample against the next account.
            // Retrying the SAME account on a quota refusal would only burn the
            // retry budget, so this branch sits ahead of the same-model retry.
            if !private_review
                && !signal.is_cancelled()
                && crate::provider::rotation::is_quota_rotation_error(error)
            {
                if error
                    .credential_recovery
                    .as_ref()
                    .is_some_and(|recovery| recovery.exhausted)
                {
                    outcome = out;
                    break;
                }
                if let Some(recovery) = &error.credential_recovery {
                    credential_failures.extend(recovery.attempted_account_ids.iter().cloned());
                }
                let failed_account = error
                    .credential_recovery
                    .as_ref()
                    .and_then(|recovery| recovery.failed_account_id.clone())
                    .or_else(|| request_account.clone());
                if let Some(failed) = failed_account {
                    credential_failures.insert(failed.clone());
                    let mut rotation = match shared.bundle() {
                        Some(bundle) => bundle.provider.rotate_credential(&failed).await,
                        None => None,
                    };
                    // Concurrent requests may have advanced the shared head to
                    // an account this request already tried. Skip it locally.
                    let mut skipped = HashSet::new();
                    while let Some(next) = rotation
                        .as_ref()
                        .and_then(|outcome| outcome.current())
                        .map(str::to_owned)
                        .filter(|id| credential_failures.contains(id))
                    {
                        if !skipped.insert(next.clone()) {
                            rotation = Some(crate::provider::rotation::RotateOutcome::Exhausted);
                            break;
                        }
                        rotation = match shared.bundle() {
                            Some(bundle) => bundle.provider.rotate_credential(&next).await,
                            None => None,
                        };
                    }
                    if matches!(
                        rotation,
                        Some(crate::provider::rotation::RotateOutcome::Exhausted)
                    ) {
                        let mut attempted: Vec<_> = credential_failures.iter().cloned().collect();
                        attempted.sort();
                        crate::observability::provider_diagnostics::record_named(
                            crate::observability::provider_diagnostics::TIMING_LOG_FILE_NAME,
                            "credential_exhausted",
                            serde_json::json!({
                                "sessionId": shared.params.thread_id.try_to_utf8().ok(),
                                "model": prepared.request.model.try_to_utf8().ok(),
                                "attemptedAccountIds": attempted,
                                "attemptedAccountCount": attempted.len(),
                                "upstreamRequestId": error.request_id.as_ref().and_then(|id| id.try_to_utf8().ok()),
                                "outcome": "exhausted",
                            }),
                        );
                        outcome = out;
                        break;
                    }
                    if rotation.as_ref().is_some_and(|outcome| outcome.retriable()) {
                        crate::observability::provider_diagnostics::record_named(
                            crate::observability::provider_diagnostics::TIMING_LOG_FILE_NAME,
                            "retry",
                            serde_json::json!({
                                "purpose": "credential_rotation",
                                "requestId": partial_uuid.try_to_utf8().ok(),
                                "model": prepared.request.model.try_to_utf8().ok(),
                                "providerId": shared.deps().provider_id.try_to_utf8().ok(),
                                "sessionId": shared.params.thread_id.try_to_utf8().ok(),
                                "failedAccountId": failed,
                                "nextAccountId": rotation.as_ref().and_then(|outcome| outcome.current()),
                                "errorKind": format!("{:?}", error.kind),
                            }),
                        );
                        if *emitted_text {
                            shared.q.push(EngineEvent::Tombstone {
                                uuid: partial_uuid.clone(),
                            });
                        }
                        notify(
                            &shared.q,
                            NotificationLevel::Warn,
                            "Subscription allowance spent; switching to the next account",
                        );
                        attempt = 0;
                        continue;
                    }
                }
            }
            // A locally enforced full stream timeout may already have cost ten
            // minutes. Re-sample once; explicit gateway 408 responses use the
            // normal retry budget because they can arrive within seconds.
            let retry_limit = if private_review {
                u32::from(
                    error.failure_phase() == crate::types::errors::ProviderFailurePhase::Streaming
                        && error.status.is_none(),
                )
            } else if error.kind == ProviderErrorKind::Timeout && error.status.is_none() {
                shared.config().max_retries.min(1)
            } else {
                shared.config().max_retries
            };
            let oversized = error.status == Some(413)
                || matches!(
                    error.kind,
                    ProviderErrorKind::PromptTooLong | ProviderErrorKind::RequestTooLarge
                );
            if !oversized && is_retryable(error) && attempt < retry_limit && !signal.is_cancelled()
            {
                let next_attempt = attempt + 1;
                let delay_ms = if private_review {
                    use rand::Rng as _;
                    error
                        .retry_after_ms
                        .unwrap_or_else(|| (200.0 * rand::thread_rng().gen_range(0.9..1.1)) as u64)
                } else {
                    backoff_ms(next_attempt, error.retry_after_ms)
                };
                crate::observability::provider_diagnostics::record_named(
                    crate::observability::provider_diagnostics::TIMING_LOG_FILE_NAME,
                    "retry",
                    serde_json::json!({
                        "purpose": "normal",
                        "requestId": partial_uuid.try_to_utf8().ok(),
                        "model": prepared.request.model.try_to_utf8().ok(),
                        "providerId": shared.deps().provider_id.try_to_utf8().ok(),
                        "sessionId": shared.params.thread_id.try_to_utf8().ok(),
                        "attempt": current_attempt,
                        "nextAttempt": next_attempt.saturating_add(1),
                        "retryDelayMs": delay_ms,
                        "errorKind": format!("{:?}", error.kind),
                        "elapsedMs": request_started.elapsed().as_millis() as u64,
                        "idleBudgetMs": shared.params.config.stream_idle_timeout_ms,
                        "totalBudgetMs": shared.params.config.stream_total_timeout_ms,
                    }),
                );
                shared.q.push(EngineEvent::ProviderDecision {
                    inference: None,
                    event: provider_observability::retry_scheduled(
                        &metadata,
                        current_attempt,
                        retry_limit,
                        provider_observability::RetryPhase::Stream,
                        delay_ms,
                        error,
                    ),
                });
                attempt = next_attempt;
                if *emitted_text {
                    shared.q.push(EngineEvent::Tombstone {
                        uuid: partial_uuid.clone(),
                    });
                }
                // Release builds hide a first transient network blip, but a
                // timeout may already have waited ten minutes and merits a notice.
                // Debug builds keep full visibility for diagnosis.
                if error.kind == ProviderErrorKind::Timeout || attempt > 1 || cfg!(debug_assertions)
                {
                    notify(
                        &shared.q,
                        NotificationLevel::Warn,
                        format!("Reconnecting... {}/{}", attempt, retry_limit),
                    );
                }
                abortable_sleep(delay_ms, signal).await;
                continue;
            }
            failure_recovery = if !is_retryable(error) {
                "not-retryable"
            } else if signal.is_cancelled() {
                "retry-aborted"
            } else if retry_limit == 0 {
                "retries-disabled"
            } else {
                "retries-exhausted"
            };
            // Model fallback: rotate to a backup and re-stream THIS request.
            if !private_review && !oversized && !signal.is_cancelled() && !fallback_chain.is_empty()
            {
                if let Some(reason) = classify_model_fallback_error(error) {
                    let from = {
                        let tc = shared.turn_context.lock_recover();
                        tc.model.clone()
                    };
                    let next = fallback_chain.remove(0);
                    if let Some(event) = provider_observability::model_fallback(
                        &metadata,
                        current_attempt,
                        next.clone(),
                        error,
                    ) {
                        shared.q.push(EngineEvent::ProviderDecision {
                            inference: None,
                            event,
                        });
                    }
                    {
                        let mut tc = shared.turn_context.lock_recover();
                        tc.model = next.clone();
                    }
                    if *emitted_text {
                        shared.q.push(EngineEvent::Tombstone {
                            uuid: partial_uuid.clone(),
                        });
                    }
                    notify(
                        &shared.q,
                        NotificationLevel::Warn,
                        concat_runtime_strings(&[
                            "model ".into(),
                            from,
                            " unavailable (".into(),
                            reason.as_str().into(),
                            "); falling back to ".into(),
                            next,
                        ]),
                    );
                    return StreamTurnResult {
                        outcome: out,
                        retry_preflight: true,
                        admitted_messages,
                        admitted_system,
                        failure_recovery,
                    };
                }
            }
        } else if matches!(
            &out,
            Outcome::InputError { .. } | Outcome::ProviderAdmissionError { .. }
        ) {
            shared.q.push(EngineEvent::ProviderDecision {
                inference: None,
                event: provider_observability::request_interrupted(&metadata, current_attempt),
            });
        }
        outcome = out;
        break;
    }
    StreamTurnResult {
        outcome,
        retry_preflight: false,
        admitted_messages,
        admitted_system,
        failure_recovery,
    }
}

#[derive(Clone, Copy)]
enum CompactionPhase {
    BeforeRequest,
    Recovery,
    AfterTurn,
}

enum CompactionFailure {
    Aborted,
    Local(RuntimeString),
    Service(EngineError),
}

struct CompactionOutputBinding {
    compactor: Arc<dyn crate::engine::deps::Compactor>,
    restore: Option<crate::context::compact::full_compact::CompactionOutputSink>,
}

impl Drop for CompactionOutputBinding {
    fn drop(&mut self) {
        self.compactor.bind_output_observer(self.restore.take());
    }
}
impl From<&str> for CompactionFailure {
    fn from(value: &str) -> Self {
        Self::Local(value.into())
    }
}
impl From<RuntimeString> for CompactionFailure {
    fn from(value: RuntimeString) -> Self {
        Self::Local(value)
    }
}

fn emit_compaction_failure(shared: &Arc<Shared>, failure: CompactionFailure) {
    match failure {
        CompactionFailure::Aborted => shared.q.push(EngineEvent::TurnCompleted {
            turn_id: shared.turn_state.turn_id.clone(),
            usage: None,
            stop_reason: TerminalReason::Aborted,
        }),
        CompactionFailure::Local(reason) => emit_preflight_compaction_failure(shared, reason),
        CompactionFailure::Service(error) => shared.q.push(EngineEvent::TurnFailed {
            inference: None,
            error,
        }),
    }
}

fn review_compaction_service_error(
    params: &QueryParams,
    diagnostics: &[crate::context::compact::outcome::CompactionDiagnostic],
) -> Option<EngineError> {
    if params
        .agent_type
        .as_ref()
        .is_none_or(|kind| kind != "approval-review")
    {
        return None;
    }
    let diagnostic = diagnostics.iter().find(|diagnostic| {
        diagnostic.stage == crate::context::compact::outcome::CompactionDiagnosticStage::Summary
    })?;
    Some(if let Some(provider) = &diagnostic.provider_error {
        EngineError::from_provider(provider.clone(), "review-budget-compaction")
    } else {
        EngineError {
            code: EngineErrorCode::Internal,
            message: diagnostic.message.clone().unwrap_or_else(|| {
                "The review summary service did not return a complete summary.".into()
            }),
            cause: None,
            request_id: None,
            provider_kind: None,
            retry_after_ms: None,
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_turn_compact(
    shared: &Arc<Shared>,
    cwd: &RuntimePath,
    thread_id: &RuntimeString,
    turn_user_uuid: &Option<RuntimeString>,
    profile: &ProviderBehaviorProfile,
    messages: &mut Vec<Message>,
    prefix_tokens: u64,
    phase: CompactionPhase,
) -> Result<(), CompactionFailure> {
    if shared.params.signal.is_cancelled() {
        return Err(CompactionFailure::Aborted);
    }
    let config = shared.config();
    if config.compaction_disabled || !config.auto_compaction {
        return Err("Automatic context compaction is disabled.".into());
    }
    let mut instructions = None;
    if let Some(bus) = &shared.params.hook_bus {
        if bus.has(HookEvent::PreCompact) {
            let mut input = base_hook_input(HookEvent::PreCompact, cwd, thread_id);
            input.trigger = Some(HookTrigger::Auto);
            let outcome = tokio::select! {
                biased;
                _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
                outcome = bus.dispatch(HookEvent::PreCompact, &input) => outcome,
            };
            for message in outcome.system_messages {
                notify(&shared.q, NotificationLevel::Info, message);
            }
            if outcome.aborted || outcome.blocked {
                return Err(CompactionFailure::Aborted);
            }
            if !outcome.additional_context.is_empty() {
                instructions = Some(join_runtime_strings(&outcome.additional_context, "\n"));
            }
        }
    }
    let staged_output = matches!(phase, CompactionPhase::AfterTurn)
        .then(|| Arc::new(Mutex::new(Vec::<Message>::new())));
    let _output_binding = staged_output.as_ref().map(|staged| {
        let compactor = shared.deps().compactor.clone();
        let output = staged.clone();
        compactor.bind_output_observer(Some(Arc::new(move |message| {
            output.lock_recover().push(message);
            Box::pin(futures::future::ready(()))
        })));
        CompactionOutputBinding {
            compactor,
            restore: shared.params.compaction_output.clone(),
        }
    });
    let (compactable, protected) =
        split_compaction_protected(messages, &shared.params.compaction_protected_message_ids);
    let before = crate::context::tokens::estimate_context_tokens(messages);
    let source_ids: Vec<RuntimeString> = messages
        .iter()
        .map(super::query_message_uuid)
        .cloned()
        .collect();
    shared.q.push(EngineEvent::CompactionProgress {
        phase: "started".into(),
    });
    let result = tokio::select! {
        biased;
        _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
        result = async {
            match phase {
                CompactionPhase::BeforeRequest => preflight_compaction::compact(
                    shared, &compactable, instructions.as_ref(),
                    estimate_context_tokens_with_prefix(messages, prefix_tokens),
                ).await,
                CompactionPhase::Recovery => shared.deps().compactor.reactive_compact(
                    &compactable, &config, instructions.as_ref(),
                ).await,
                CompactionPhase::AfterTurn => shared.deps().compactor.post_turn_compact(
                    &compactable, &config, instructions.as_ref(),
                ).await,
            }
        } => result,
    };
    shared.q.push(EngineEvent::CompactionProgress {
        phase: "completed".into(),
    });
    finish_turn_compact(
        shared,
        cwd,
        thread_id,
        turn_user_uuid,
        profile,
        messages,
        prefix_tokens,
        result,
        CompactedReplacement::RefreshUserHistory,
        phase,
        compactable,
        protected,
        source_ids,
        before,
        staged_output,
    )
    .await
}

/// How the compacted replacement history is derived from the summary result.
enum CompactedReplacement {
    /// Model-summary path: re-read the authoritative user input around the new
    /// checkpoint.
    RefreshUserHistory,
    /// Precomputed path: the splice already contains the retained tail and
    /// every message that arrived after the checkpoint boundary.
    Verbatim,
}

/// Apply a completed compaction result: validate it, rebuild the history,
/// replay staged summary output, persist the checkpoint and run the
/// PostCompact hook. Shared by the synchronous path and the precomputed
/// checkpoint path so both produce the identical visible sequence.
#[allow(clippy::too_many_arguments)]
async fn finish_turn_compact(
    shared: &Arc<Shared>,
    cwd: &RuntimePath,
    thread_id: &RuntimeString,
    turn_user_uuid: &Option<RuntimeString>,
    profile: &ProviderBehaviorProfile,
    messages: &mut Vec<Message>,
    prefix_tokens: u64,
    result: crate::engine::deps::CompactionResult,
    replacement_mode: CompactedReplacement,
    phase: CompactionPhase,
    compactable: Vec<Message>,
    protected: Vec<Message>,
    source_ids: Vec<RuntimeString>,
    before: u64,
    staged_output: Option<Arc<Mutex<Vec<Message>>>>,
) -> Result<(), CompactionFailure> {
    if shared.params.signal.is_cancelled() {
        return Err(CompactionFailure::Aborted);
    }
    let config = shared.config();
    if !result.compacted {
        if result.diagnostics.iter().any(|item| {
            item.kind == crate::context::compact::outcome::CompactionDiagnosticKind::PromptTooLong
        }) {
            if let Some(window) = &shared.params.context_window {
                window
                    .mark_full(crate::context::compact::policy::usable_context_window(
                        &config,
                    ))
                    .await;
            }
        }
        if matches!(phase, CompactionPhase::AfterTurn) {
            if let Some(error) = result
                .diagnostics
                .iter()
                .filter_map(|item| item.provider_error.as_ref())
                .find(|error| {
                    error.code.as_ref().is_some_and(|code| {
                        code == "session_budget_exceeded" || code == "budget_exceeded"
                    })
                })
            {
                return Err(CompactionFailure::Service(EngineError::from_provider(
                    error.clone(),
                    "post-turn-compaction",
                )));
            }
        }
        if let Some(error) = review_compaction_service_error(&shared.params, &result.diagnostics) {
            return Err(CompactionFailure::Service(error));
        }
        let diagnostics = result
            .diagnostics
            .iter()
            .filter_map(|item| item.message.clone())
            .collect::<Vec<_>>();
        return Err(if diagnostics.is_empty() {
            "The summary operation did not return a completed checkpoint.".into()
        } else {
            join_runtime_strings(&diagnostics, "; ").into()
        });
    }
    if !result.has_new_checkpoint(&compactable) {
        return Err("reactive-compaction-no-progress".into());
    }
    let replacement = match replacement_mode {
        CompactedReplacement::RefreshUserHistory => {
            let authoritative = shared
                .params
                .compaction_history
                .as_ref()
                .map_or_else(|| compactable.clone(), |snapshot| snapshot());
            let authoritative: Vec<_> = authoritative
                .into_iter()
                .filter(|message| {
                    !shared
                        .params
                        .compaction_protected_message_ids
                        .contains(super::query_message_uuid(message))
                })
                .collect();
            crate::context::compact::full_compact::refresh_local_user_history(
                &authoritative,
                result.messages,
            )
        }
        CompactedReplacement::Verbatim => result.messages,
    };
    let mut candidate =
        restore_compaction_protected(replacement, protected, turn_user_uuid.as_ref());
    invalidate_context_usage(&mut candidate);
    // Detect which retained reminder fragments the rewrite dropped and
    // re-inject only those; surviving copies contribute zero bytes.
    reinject_query_retained_fragments(shared, &mut candidate, profile);
    let has_task_tool = shared.params.tools.iter().any(|tool| tool.name() == "task");
    tokio::select! {
        biased;
        _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
        _ = refresh_retained_context_inner(shared, &mut candidate, cwd, has_task_tool, false) => {},
    }
    place_retained_context(&mut candidate);
    if shared.params.signal.is_cancelled() {
        return Err(CompactionFailure::Aborted);
    }
    if let (Some(staged), Some(output)) = (&staged_output, &shared.params.compaction_output) {
        let output_messages = { std::mem::take(&mut *staged.lock_recover()) };
        for message in output_messages {
            let Ok(_owner) = shared.persistence_in_flight.enter() else {
                return Err(CompactionFailure::Aborted);
            };
            output(message).await;
        }
    }
    persist_compaction(shared, &mut candidate, source_ids, before).await;
    if shared.params.on_compaction.is_none() {
        if let Some(window) = &shared.params.context_window {
            window
                .reset(estimate_context_tokens_with_prefix(
                    &candidate,
                    prefix_tokens,
                ))
                .await;
        }
    }
    *messages = candidate;
    shared.q.push(EngineEvent::Compaction {
        mode: CompactionMode::Full,
        freed_tokens: result.freed_tokens,
    });
    if let Some(bus) = &shared.params.hook_bus {
        if bus.has(HookEvent::PostCompact) {
            let mut input = base_hook_input(HookEvent::PostCompact, cwd, thread_id);
            input.trigger = Some(HookTrigger::Auto);
            input.message = Some(compaction_summary_of(messages));
            let outcome = tokio::select! {
                biased;
                _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
                outcome = bus.dispatch(HookEvent::PostCompact, &input) => outcome,
            };
            for message in outcome.system_messages {
                notify(&shared.q, NotificationLevel::Info, message);
            }
            if outcome.blocked || outcome.aborted {
                return Err(CompactionFailure::Aborted);
            }
        }
    }
    Ok(())
}

/// Apply a precomputed checkpoint with the same visible sequence the
/// synchronous path produces: hook notifications, progress events, staged
/// summary output, persistence and the PostCompact hook.
#[allow(clippy::too_many_arguments)]
async fn apply_precomputed_compaction(
    shared: &Arc<Shared>,
    cwd: &RuntimePath,
    thread_id: &RuntimeString,
    turn_user_uuid: &Option<RuntimeString>,
    profile: &ProviderBehaviorProfile,
    messages: &mut Vec<Message>,
    prefix_tokens: u64,
    swap: super::precompute::PrecomputedSwap,
) -> Result<(), CompactionFailure> {
    if shared.params.signal.is_cancelled() {
        return Err(CompactionFailure::Aborted);
    }
    tracing::info!(
        lead_ms = swap.lead_ms,
        total_ms = swap.total_ms,
        messages_since = swap.messages_since,
        borrowed = swap.borrowed,
        "applying precomputed compaction checkpoint"
    );
    // Replay the PreCompact hook notifications captured at arm time so they
    // keep the position the synchronous path gives them.
    for message in swap.hook_notifications {
        notify(&shared.q, NotificationLevel::Info, message);
    }
    let (compactable, protected) =
        split_compaction_protected(messages, &shared.params.compaction_protected_message_ids);
    let before = crate::context::tokens::estimate_context_tokens(messages);
    let source_ids: Vec<RuntimeString> = messages
        .iter()
        .map(super::query_message_uuid)
        .cloned()
        .collect();
    shared.q.push(EngineEvent::CompactionProgress {
        phase: "started".into(),
    });
    shared.q.push(EngineEvent::CompactionProgress {
        phase: "completed".into(),
    });
    let staged_output = Arc::new(Mutex::new(swap.staged_output));
    finish_turn_compact(
        shared,
        cwd,
        thread_id,
        turn_user_uuid,
        profile,
        messages,
        prefix_tokens,
        swap.result,
        CompactedReplacement::Verbatim,
        CompactionPhase::BeforeRequest,
        compactable,
        protected,
        source_ids,
        before,
        Some(staged_output),
    )
    .await
}

/// Try to apply the background-precomputed checkpoint at the compaction
/// boundary. `None` means no usable checkpoint exists and the caller runs the
/// synchronous compaction unchanged.
#[allow(clippy::too_many_arguments)]
async fn consume_precomputed_compaction(
    shared: &Arc<Shared>,
    cwd: &RuntimePath,
    thread_id: &RuntimeString,
    turn_user_uuid: &Option<RuntimeString>,
    profile: &ProviderBehaviorProfile,
    messages: &mut Vec<Message>,
    prefix_tokens: u64,
    config: &crate::schemas::config::ResolvedConfig,
    request: &crate::types::provider::ModelRequest,
) -> Option<Result<(), CompactionFailure>> {
    // The gate is checked on both ends so a session that disables precompute
    // mid-flight never applies an entry armed while it was enabled; the
    // boundary falls back to the synchronous compaction unchanged.
    if !config.precompute_compaction {
        return None;
    }
    let registry = shared.params.precompute.clone()?;
    let agent_key = shared
        .params
        .agent_id
        .clone()
        .unwrap_or_else(|| RuntimeString::from("main"));
    let (compactable, _protected) =
        split_compaction_protected(messages, &shared.params.compaction_protected_message_ids);
    let deps = shared.deps();
    let now = deps.now.clone();
    match registry
        .consume_native(
            &agent_key,
            &compactable,
            config,
            &shared.params.signal,
            now.as_ref(),
            deps.compactor.as_ref(),
            request,
        )
        .await
    {
        super::precompute::PrecomputeConsume::Miss => {
            // The boundary fell back to synchronous compaction; one
            // content-free marker shows no precomputed checkpoint was
            // available to swap in.
            crate::observability::provider_timing::record_precompute(
                "consumed",
                serde_json::json!({
                    "sessionId": thread_id.try_to_utf8().ok(),
                    "agentKey": agent_key.try_to_utf8().ok(),
                    "outcome": "miss",
                }),
            );
            None
        }
        super::precompute::PrecomputeConsume::Aborted => Some(Err(CompactionFailure::Aborted)),
        super::precompute::PrecomputeConsume::Hit(swap) => Some(
            apply_precomputed_compaction(
                shared,
                cwd,
                thread_id,
                turn_user_uuid,
                profile,
                messages,
                prefix_tokens,
                *swap,
            )
            .await,
        ),
    }
}

/// Arm one background checkpoint summary while the context is inside the
/// precompute lead window but has not reached the compaction boundary. The
/// background pass never emits user-visible events on its own. Enabled by
/// default through `precompute_compaction`; disabling it keeps the synchronous
/// compaction path unchanged.
#[allow(clippy::too_many_arguments)]
fn maybe_arm_compaction_precompute(
    shared: &Arc<Shared>,
    status: &crate::context::compact::policy::ContextWindowTokenStatus,
    config: &crate::schemas::config::ResolvedConfig,
    messages: &[Message],
    request: &crate::types::provider::ModelRequest,
    prefix_tokens: u64,
    cwd: &RuntimePath,
    thread_id: &RuntimeString,
) {
    if !config.precompute_compaction {
        return;
    }
    let Some(registry) = shared.params.precompute.as_ref() else {
        return;
    };
    let (scope_tokens, scope_limit) = match config.auto_compact_scope {
        crate::schemas::config::AutoCompactScope::Total => (
            status
                .auto_compact_scope_tokens
                .saturating_sub(prefix_tokens),
            status
                .auto_compact_scope_limit
                .saturating_sub(prefix_tokens),
        ),
        crate::schemas::config::AutoCompactScope::BodyAfterPrefix => (
            status.auto_compact_scope_tokens,
            status.auto_compact_scope_limit,
        ),
    };
    let Some(threshold) = crate::context::compact::policy::precompute_arm_threshold(scope_limit)
    else {
        return;
    };
    if scope_tokens < threshold {
        return;
    }
    let deps = shared.deps();
    let now = deps.now.clone();
    // Route the fork's model calls through the timing log so background
    // summary requests are visible next to the foreground ones under their
    // own purpose. The native summary transport bypasses this seam; its
    // lifecycle is covered by the registry's own records.
    let timing_context = crate::observability::provider_timing::PrecomputeTimingContext {
        uuid: deps.uuid.clone(),
        provider_id: deps.provider_id.clone(),
        session_id: thread_id.clone(),
        turn_id: shared.params.turn_id.clone(),
        idle_ms: config.stream_idle_timeout_ms,
        total_ms: config.stream_total_timeout_ms,
    };
    let call_model = crate::observability::provider_timing::precompute_call_model(
        deps.call_model,
        timing_context,
    );
    let Ok(compactor) = deps.compactor.fork_precompute(call_model, config) else {
        return;
    };
    let (compactable, _protected) =
        split_compaction_protected(messages, &shared.params.compaction_protected_message_ids);
    if !compactable
        .iter()
        .any(|message| matches!(message, Message::Assistant(_)))
    {
        return;
    }
    let agent_key = shared
        .params
        .agent_id
        .clone()
        .unwrap_or_else(|| RuntimeString::from("main"));
    let hook = shared
        .params
        .hook_bus
        .as_ref()
        .filter(|bus| bus.has(HookEvent::PreCompact))
        .map(|bus| precompute_hook_runner(bus.clone(), cwd.clone(), thread_id.clone()));
    registry.arm(super::precompute::PrecomputeArm {
        agent_key,
        session_id: thread_id.clone(),
        pre_compact_tokens: crate::context::tokens::estimate_context_tokens(&compactable),
        estimated_tokens: estimate_context_tokens_with_prefix(messages, prefix_tokens),
        snapshot: compactable,
        model: config.model.clone(),
        config: config.clone(),
        request: request.clone(),
        compactor,
        hook,
        now,
    });
}

/// Build the PreCompact hook runner for the background precompute task. The
/// notifications are captured, not emitted: the apply step replays them so
/// their visible position matches the synchronous path.
fn precompute_hook_runner(
    bus: Arc<crate::hooks::bus::HookBus>,
    cwd: RuntimePath,
    thread_id: RuntimeString,
) -> super::precompute::PrecomputeHook {
    Arc::new(move |signal: crate::types::abort::AbortSignal| {
        let bus = bus.clone();
        let cwd = cwd.clone();
        let thread_id = thread_id.clone();
        Box::pin(async move {
            let mut input = base_hook_input(HookEvent::PreCompact, &cwd, &thread_id);
            input.trigger = Some(HookTrigger::Auto);
            let outcome = tokio::select! {
                biased;
                _ = signal.cancelled() => {
                    return super::precompute::PrecomputeHookOutcome {
                        blocked: false,
                        notifications: Vec::new(),
                        instructions: None,
                    };
                }
                outcome = bus.dispatch(HookEvent::PreCompact, &input) => outcome,
            };
            super::precompute::PrecomputeHookOutcome {
                blocked: outcome.aborted || outcome.blocked,
                notifications: outcome.system_messages,
                instructions: if outcome.additional_context.is_empty() {
                    None
                } else {
                    Some(join_runtime_strings(&outcome.additional_context, "\n"))
                },
            }
        })
    })
}

/// Numbered phase 5 of the turn loop: fold queued inline input into the final
/// tool results, persist them once, durably commit queue consumption, and
/// honor a hook-initiated abort. Returns `false` when the turn has already
/// terminated (queue durability failure or hook abort); on `true` the caller
/// marks the tool continuation and loops again.
#[allow(clippy::too_many_arguments, clippy::ptr_arg)]
async fn run_turn_tool_results(
    shared: &Arc<Shared>,
    signal: &crate::types::abort::AbortSignal,
    sleep_ran: bool,
    tool_results: &Vec<ToolResultMessage>,
    final_usage: &Option<Usage>,
    messages: &mut Vec<Message>,
    turn_pdf_bytes: &mut usize,
    turn_pdf_documents: &mut usize,
) -> bool {
    let mut final_results = tool_results.clone();
    let mut drained_queue: Option<(Arc<MessageQueue>, Vec<Arc<QueuedMessage>>)> = None;
    let mut _mailbox_reservation = None;
    // User steers drained this continuation: persisted after the tool results so
    // the rollout keeps chronological order (see the drain site).
    let mut steered_user_messages: Vec<Message> = Vec::new();
    if let Some(queue) = &shared.params.message_queue {
        if !signal.is_cancelled() {
            let max = if sleep_ran {
                QueuePriority::Later
            } else {
                QueuePriority::Next
            };
            let agent_id = shared.params.agent_id.clone();
            let mut drained: Vec<Arc<QueuedMessage>> = Vec::new();
            let mut rejected: Vec<(Arc<QueuedMessage>, InputValidationError)> = Vec::new();
            let eligible = |cmd: &QueuedMessage| {
                if cmd.mode != "subagent-message" && MessageQueue::is_slash_command(&cmd.value) {
                    return false;
                }
                let parts = queued_value_to_tool_result_parts(&cmd.value);
                if parts.output_blocks.is_empty() && parts.extra_user_blocks.is_empty() {
                    return false;
                }
                match &agent_id {
                    None => {
                        cmd.agent_id.is_none()
                            && INLINE_DRAIN_MODES.iter().any(|mode| cmd.mode == *mode)
                    }
                    Some(aid) => {
                        (cmd.mode == "task-notification" || cmd.mode == "subagent-message")
                            && cmd.agent_id.as_ref() == Some(aid)
                    }
                }
            };
            let resumable = if let Some(agent_id) = &agent_id {
                match queue.reserve_agent_resume_durable(agent_id).await {
                    Ok(items) => items,
                    Err(error) => {
                        shared.q.push(EngineEvent::TurnFailed {
                            inference: None,
                            error: queue_durability_engine_error("reserving agent input", error),
                        });
                        return false;
                    }
                }
            } else {
                Vec::new()
            };
            let reserved = match queue
                .reserve_commands_by_max_priority_durable(max, &|cmd| {
                    eligible(cmd) && (agent_id.is_none() || cmd.mode != "subagent-message")
                })
                .await
            {
                Ok(reserved) => reserved,
                Err(error) => {
                    shared.q.push(EngineEvent::TurnFailed {
                        inference: None,
                        error: queue_durability_engine_error("reserving queued input", error),
                    });
                    return false;
                }
            };
            let mut reserved = reserved;
            reserved.extend(resumable);
            _mailbox_reservation = Some(PassiveMailboxBatch {
                queue: queue.clone(),
                items: reserved.clone(),
            });
            if signal.is_cancelled() {
                queue.release_pending_resume(&reserved);
                return false;
            }
            for cmd in reserved {
                if cmd.pdf_bytes > 0 {
                    let within_count = turn_pdf_documents
                        .checked_add(cmd.pdf_documents)
                        .is_some_and(|total| total <= MAX_TURN_PDF_DOCUMENTS);
                    if !within_count {
                        let mut error = InputValidationError::new(
                            InputValidationErrorCode::TooManyDocuments,
                            format!(
                                "PDF input exceeds the {MAX_TURN_PDF_DOCUMENTS}-document per-turn limit"
                            ),
                        );
                        error.detail.limit_count = Some(MAX_TURN_PDF_DOCUMENTS as u64);
                        rejected.push((cmd, error));
                        continue;
                    }
                    let within_budget = turn_pdf_bytes
                        .checked_add(cmd.pdf_bytes)
                        .is_some_and(|total| total <= MAX_TURN_PDF_BYTES);
                    if !within_budget {
                        let mut error = InputValidationError::new(
                            InputValidationErrorCode::DocumentTooLarge,
                            format!(
                                "PDF input exceeds the {MAX_TURN_PDF_BYTES}-byte per-turn limit"
                            ),
                        );
                        error.detail.limit_bytes = Some(MAX_TURN_PDF_BYTES as u64);
                        rejected.push((cmd, error));
                        continue;
                    }
                    // Main-thread steers belong to the NEXT step. Capability
                    // validation there uses its actual capture, not the provider
                    // that produced the tool results being persisted here.
                    if shared
                        .inference
                        .as_ref()
                        .is_none_or(|capture| capture.next.is_none())
                    {
                        let model = if shared.inference.is_some() {
                            shared.step_model.lock_recover().clone()
                        } else {
                            shared.turn_context.lock_recover().model.clone()
                        };
                        if let Err(error) = shared.validate_pdf(&model) {
                            rejected.push((cmd, *error));
                            continue;
                        }
                    }
                }
                *turn_pdf_bytes += cmd.pdf_bytes;
                *turn_pdf_documents += cmd.pdf_documents;
                drained.push(cmd);
            }
            if !rejected.is_empty() {
                let rejected_items = rejected
                    .iter()
                    .map(|(cmd, _)| Arc::clone(cmd))
                    .collect::<Vec<_>>();
                if let Err(error) = queue
                    .remove_with_state_durable(&rejected_items, QueueTerminalState::Rejected)
                    .await
                {
                    shared.q.push(EngineEvent::TurnFailed {
                        inference: None,
                        error: queue_durability_engine_error("rejecting queued input", error),
                    });
                    return false;
                }
                for (cmd, error) in rejected {
                    shared.q.push(EngineEvent::InputRejected {
                        correlation_id: cmd.correlation_id.clone(),
                        message: error.message.into(),
                        detail: error.detail,
                    });
                }
            }
            if !drained.is_empty() {
                // A human follow-up must survive the turn as its OWN user message:
                // gluing it into the last tool result made its bubble vanish once
                // history took over, because tool results are process evidence and
                // fold away. Delegations keep the tool-result shape: their envelope
                // carries the sender id the renderer lifts back out into a bubble.
                let (user_steers, inline_steers): (
                    Vec<Arc<QueuedMessage>>,
                    Vec<Arc<QueuedMessage>>,
                ) = drained.into_iter().partition(|cmd| {
                    cmd.delivery == Some(SteerDelivery::User) || !cmd.triggers_turn()
                });
                if !inline_steers.is_empty() {
                    let li = final_results.len() - 1;
                    for cmd in &inline_steers {
                        let parts = queued_value_to_tool_result_parts(&cmd.value);
                        final_results[li].content.extend(parts.output_blocks);
                        if !parts.extra_user_blocks.is_empty() {
                            final_results[li]
                                .extra_user_blocks
                                .get_or_insert_with(Vec::new)
                                .extend(parts.extra_user_blocks);
                        }
                    }
                }
                let keep = messages.len() - tool_results.len();
                messages.truncate(keep);
                messages.extend(final_results.iter().cloned().map(Message::ToolResult));
                for cmd in &user_steers {
                    let message = Message::User(UserMessage {
                        question_answer: None,
                        uuid: cmd
                            .uuid
                            .clone()
                            .unwrap_or_else(|| (shared.deps.uuid)().into()),
                        content: if cmd.mode == "subagent-message" {
                            crate::engine::session::ordinary_message_blocks(cmd)
                        } else {
                            queued_value_to_content_blocks(&cmd.value)
                        },
                        is_meta: cmd.is_meta,
                    });
                    messages.push(message.clone());
                    // Persisted AFTER the tool results below: the rollout order is
                    // the resume order, and chronologically the steer answers the
                    // turn's last tool call rather than preceding it.
                    let message_uuid = match &message {
                        Message::User(user) => user.uuid.clone(),
                        _ => unreachable!("a user steer is a user message"),
                    };
                    steered_user_messages.push(message);
                    if let Some(metadata) = &cmd.display_metadata {
                        // Persist the rendering record through on_message only:
                        // it must never enter the model-visible messages vector.
                        steered_user_messages.push(crate::engine::session::user_display_message(
                            message_uuid,
                            metadata,
                            (shared.deps.uuid)().into(),
                        ));
                    }
                }
                let mut drained_all = inline_steers;
                drained_all.extend(user_steers);
                drained_queue = Some((queue.clone(), drained_all));
            }
        }
    }
    // Persist the FINAL tool_results (possibly carrying steering) once.
    for r in &final_results {
        if let Some(cb) = &shared.params.on_message {
            if cb(Message::ToolResult(r.clone())).await.is_err() {
                return false;
            }
        }
    }
    for message in steered_user_messages.drain(..) {
        if let Some(cb) = &shared.params.on_message {
            if cb(message).await.is_err() {
                return false;
            }
        }
    }
    if let Some((queue, drained)) = drained_queue {
        if let Some(rollout) = &shared.params.message_rollout {
            if let Err(error) = rollout.flush().await {
                shared.q.push(EngineEvent::TurnFailed {
                    inference: None,
                    error: queue_durability_engine_error(
                        "persisting child queued input",
                        error.to_string().into(),
                    ),
                });
                return false;
            }
        }
        // The final tool result carrying the queued continuation is
        // durable before queue removal becomes visible. A crash or
        // Host mutation can therefore never lose both representations.
        if let Err(error) = queue
            .remove_with_state_durable(&drained, QueueTerminalState::Drained)
            .await
        {
            shared.q.push(EngineEvent::TurnFailed {
                inference: None,
                error: queue_durability_engine_error("committing queued input consumption", error),
            });
            return false;
        }
        for cmd in &drained {
            shared.q.push(EngineEvent::InputApplied {
                correlation_id: cmd.correlation_id.clone(),
            });
        }
    }
    let hook_aborted = shared.turn_context.lock_recover().hook_aborted;
    if hook_aborted {
        notify(
            &shared.q,
            NotificationLevel::Warn,
            "Turn halted by hook.".to_string(),
        );
        shared.q.push(EngineEvent::TurnCompleted {
            turn_id: shared.turn_state.turn_id.clone(),
            usage: final_usage.clone(),
            stop_reason: TerminalReason::Completed,
        });
        return false;
    }
    true
}

/// Numbered phase 4.5 of the turn loop: two-strike streamed-text repetition guard.
/// The first strike clears detection and arms a one-shot recovery reminder;
/// the second strike fails the turn. Returns `false` when the turn failed.
async fn run_turn_loop_guard(
    shared: &Arc<Shared>,
    tool_results: &[ToolResultMessage],
    loop_detector: &mut Option<LoopDetector>,
    pending_loop_reminder: &mut Option<RuntimeString>,
) -> bool {
    if let Some(ld) = loop_detector.as_mut() {
        let loop_res = ld.result();
        if loop_res.count > 1 {
            for r in tool_results {
                if let Some(cb) = &shared.params.on_message {
                    if cb(Message::ToolResult(r.clone())).await.is_err() {
                        return false;
                    }
                }
            }
            shared.q.push(EngineEvent::TurnFailed { inference: None,
                error: EngineError {
                    code: EngineErrorCode::LoopDetected,
                    message: concat_runtime_strings(&[
                        "Halted: the model appears stuck in a loop (".into(),
                        loop_res
                            .detail
                            .clone()
                            .unwrap_or_else(|| "repeating content".into()),
                        ") and did not change approach after a nudge. Re-approach the task or break it into smaller steps."
                            .into(),
                    ]),
                    cause: None,
                    request_id: None,
                    provider_kind: None,
                    retry_after_ms: None,
                },
            });
            return false;
        }
        if loop_res.count == 1 {
            ld.clear_detection();
            *pending_loop_reminder = Some(concat_runtime_strings(&[
                "A potential loop was detected (".into(),
                loop_res
                    .detail
                    .clone()
                    .unwrap_or_else(|| "repetitive actions".into()),
                "). Stop and take a step back — confirm you are making real forward progress toward the user's goal. If not, change your approach rather than repeating the same text."
                    .into(),
            ]));
            notify(
                &shared.q,
                NotificationLevel::Warn,
                "Possible loop detected — nudging the model to change approach.".to_string(),
            );
        }
    }
    true
}

/// Numbered phase 7 of the turn loop: the Stop / SubagentStop hook may force
/// the turn to continue. Returns `true` when the hook blocked the stop — the
/// continuation nudge is already appended and the caller loops again.
async fn run_turn_stop_hook(
    shared: &Arc<Shared>,
    cwd: &RuntimePath,
    thread_id: &RuntimeString,
    prepared: &PreparedModelRequest,
    messages: &mut Vec<Message>,
    stop_continuations: &mut u32,
) -> bool {
    let stop_event = if shared.params.agent_id.is_some() {
        HookEvent::SubagentStop
    } else {
        HookEvent::Stop
    };
    let has_stop = shared
        .params
        .hook_bus
        .as_ref()
        .map(|b| b.has(stop_event))
        .unwrap_or(false);
    if has_stop && *stop_continuations < MAX_STOP_CONTINUATIONS {
        let bus = shared.params.hook_bus.as_ref().unwrap();
        let mut input = base_hook_input(stop_event, cwd, thread_id);
        input.agent_id = shared.params.agent_id.clone();
        input.agent_type = shared.params.agent_type.clone();
        let stop = bus.dispatch(stop_event, &input).await;
        for m in &stop.system_messages {
            notify(&shared.q, NotificationLevel::Info, m.clone());
        }
        if stop.blocked {
            *stop_continuations += 1;
            let ac = join_runtime_strings(&stop.additional_context, "\n");
            let text = if !ac.is_empty() {
                ac
            } else {
                let sm = join_runtime_strings(&stop.system_messages, "\n");
                if !sm.is_empty() {
                    sm
                } else {
                    "Please continue.".into()
                }
            };
            let cont = provider_meta_nudge(
                shared,
                render_stop_hook_continuation(text, &prepared.profile),
            );
            messages.push(cont.clone());
            if let Some(cb) = &shared.params.on_message {
                if cb(cont).await.is_err() {
                    return false;
                }
            }
            return true;
        }
    }
    false
}

/// Numbered phase 9 of the turn loop: token-budget auto-continue with the
/// diminishing-returns breaker. Returns `true` when a continuation nudge was
/// appended and the caller loops again.
async fn run_turn_budget_continue(
    shared: &Arc<Shared>,
    prepared: &PreparedModelRequest,
    total_output_tokens: u64,
    messages: &mut Vec<Message>,
    budget_continuations: &mut u32,
    budget_low_delta_streak: &mut u32,
    last_budget_tokens: &mut u64,
) -> bool {
    if let Some(token_budget) = shared.params.token_budget {
        if token_budget > 0 && shared.params.output_schema.is_none() {
            let pending_user_steer = shared
                .params
                .message_queue
                .as_ref()
                .and_then(|queue| {
                    queue.peek(Some(&|cmd: &QueuedMessage| {
                        let parts = queued_value_to_tool_result_parts(&cmd.value);
                        cmd.agent_id.is_none()
                            && !MessageQueue::is_slash_command(&cmd.value)
                            && (!parts.output_blocks.is_empty()
                                || !parts.extra_user_blocks.is_empty())
                    }))
                })
                .is_some();
            if total_output_tokens > 0
                && (total_output_tokens as f64) < token_budget as f64 * BUDGET_COMPLETION_THRESHOLD
                && !pending_user_steer
            {
                let delta = total_output_tokens.saturating_sub(*last_budget_tokens);
                *budget_low_delta_streak = if *budget_continuations >= BUDGET_MIN_CONTINUATIONS
                    && delta < BUDGET_DIMINISHING_TOKENS
                {
                    *budget_low_delta_streak + 1
                } else {
                    0
                };
                if *budget_low_delta_streak < BUDGET_DIMINISHING_STREAK {
                    *budget_continuations += 1;
                    *last_budget_tokens = total_output_tokens;
                    let pct =
                        ((total_output_tokens as f64 / token_budget as f64) * 100.0).round() as i64;
                    let cont = provider_meta_nudge(
                        shared,
                        render_token_budget_continuation(
                            pct,
                            total_output_tokens,
                            token_budget,
                            &prepared.profile,
                        ),
                    );
                    messages.push(cont.clone());
                    if let Some(cb) = &shared.params.on_message {
                        if cb(cont).await.is_err() {
                            return false;
                        }
                    }
                    notify(
                        &shared.q,
                        NotificationLevel::Info,
                        format!("token-budget continuation (~{pct}% of target)"),
                    );
                    return true;
                }
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Output-truncation recovery.
//
// Three shapes, three responses. All three inject a meta user message and
// re-request; the
// difference is what they ask for and what happens when the ladder is spent.
// ---------------------------------------------------------------------------

/// Raise the per-response budget to the ladder's retry rung.
///
/// Truncation is evidence the budget was too small, so the retry asks for more
/// rather than spending a whole response to land in the same place. The provider
/// clamps to the model's own declared ceiling, so a host that declares one keeps
/// this inside it.
fn climb_output_budget(current: &mut u64, retry_rung: u64) -> bool {
    if *current >= retry_rung {
        return false;
    }
    *current = retry_rung;
    true
}

/// A closing response cut off WITH visible text: ask for the rest.
///
/// Returns `true` when a continuation was injected and the loop should re-request.
/// The counter lives on `Shared` because the assembler reads it to decide whether
/// to write the exhaustion footnote into the message it is about to persist — and
/// that write has to happen before persistence, since the rollout store is
/// append-only.
async fn inject_truncation_continuation(
    shared: &Arc<Shared>,
    prepared: &PreparedModelRequest,
    effective_max_output: &mut u64,
    retry_rung: u64,
    messages: &mut Vec<Message>,
) -> bool {
    let attempt = {
        let mut truncation = shared.truncation.lock_recover();
        if truncation.footnote_written || truncation.continuations >= MAX_TRUNCATION_CONTINUATIONS {
            return false;
        }
        truncation.continuations += 1;
        truncation.continuations
    };
    climb_output_budget(effective_max_output, retry_rung);
    let nudge = provider_meta_nudge(shared, render_truncation_continuation(&prepared.profile));
    messages.push(nudge.clone());
    if let Some(cb) = &shared.params.on_message {
        if cb(nudge).await.is_err() {
            return false;
        }
    }
    notify(
        &shared.q,
        NotificationLevel::Warn,
        format!(
            "the response stopped at the model's {effective_max_output}-token output limit and is cut off; continuing ({attempt}/{MAX_TRUNCATION_CONTINUATIONS})"
        ),
    );
    true
}

/// A response cut off having produced NO visible text: the whole budget went to
/// reasoning. Ask for shorter thinking rather than for "the rest", because there
/// is no rest — nothing was ever written.
async fn inject_reasoning_over_limit_reminder(
    shared: &Arc<Shared>,
    prepared: &PreparedModelRequest,
    effective_max_output: &mut u64,
    retry_rung: u64,
    messages: &mut Vec<Message>,
) {
    climb_output_budget(effective_max_output, retry_rung);
    let nudge = provider_meta_nudge(
        shared,
        render_reasoning_over_limit_reminder(&prepared.profile),
    );
    messages.push(nudge.clone());
    if let Some(cb) = &shared.params.on_message {
        if cb(nudge).await.is_err() {
            return;
        }
    }
    notify(
        &shared.q,
        NotificationLevel::Warn,
        format!(
            "the model spent its whole {effective_max_output}-token output budget on reasoning and returned no visible answer; asking it to respond directly"
        ),
    );
}

/// A tool call whose arguments were cut off mid-JSON: the call was dropped, so
/// nothing ran. The instruction to the model rides ON that dropped call's error
/// result (see `stream_and_dispatch`), because a separate message would be
/// persisted ahead of the result it explains and a resumed session would see the
/// `tool_use` followed by a user message with the `tool_result` after it — a
/// pairing a strict provider rejects.
///
/// What is left for the loop is the budget side: give the reissue more room than
/// the call that just failed. Raised on the FIRST retry only — by the second, the
/// larger budget has already proven insufficient and that attempt asks for HALF
/// the output, so more room would contradict the instruction just given.
fn climb_budget_for_tool_over_limit(
    shared: &Arc<Shared>,
    effective_max_output: &mut u64,
    retry_rung: u64,
) -> bool {
    let attempt = shared.truncation.lock_recover().tool_over_limit_retries;
    if attempt != 1 {
        return false;
    }
    let climbed = climb_output_budget(effective_max_output, retry_rung);
    if climbed {
        notify(
            &shared.q,
            NotificationLevel::Warn,
            format!(
                "a tool call was cut off by the output limit; retrying with a {effective_max_output}-token budget"
            ),
        );
    }
    climbed
}

/// The user's answer once an empty-closing ladder is spent: the truncation
/// ladder (zero visible text at the output ceiling) and the normal-stop ladder
/// (empty closing after visible work) both park here.
enum EmptyClosingDecision {
    /// Ask again with the ladder reset.
    Retry,
    /// Fail the turn.
    Stop,
    /// Nobody to ask. Fall through to settlement, which fails the turn with the
    /// ladder's wording — deliberately NOT `Retry`, since retrying with nobody
    /// able to stop it is an infinite loop producing nothing visible.
    Settle,
}

/// Block on the user when the model keeps burning its whole output budget on
/// reasoning and the retries did not change that.
///
/// Settling here would report success for a turn that produced no answer, and
/// retrying without a ceiling would loop forever. Qoder parks such a turn in a
/// manual-confirm node; this is the same stop, on the elicitation channel the
/// `ask_user` tool already uses.
async fn reasoning_over_limit_confirmation(shared: &Arc<Shared>) -> EmptyClosingDecision {
    let question = reasoning_over_limit_question_text();
    let request = reasoning_over_limit_request(&question);
    confirm_empty_closing(shared, question, request).await
}

/// Block on the user when a normal-stop closing response came back empty after
/// visible work and the step-4.8 retry did not change that. Same stop as the
/// reasoning-over-limit confirmation, on the same elicitation channel.
async fn empty_closing_confirmation(shared: &Arc<Shared>) -> EmptyClosingDecision {
    let question = empty_closing_question_text();
    let request = empty_closing_request(&question);
    confirm_empty_closing(shared, question, request).await
}

/// The shared block-on-the-user step for both empty-closing ladders.
///
/// Settling here would report success for a turn that produced no answer, and
/// retrying without a ceiling would loop forever. Qoder parks such a turn in a
/// manual-confirm node; this is the same stop, on the elicitation channel the
/// `ask_user` tool already uses.
async fn confirm_empty_closing(
    shared: &Arc<Shared>,
    question: RuntimeString,
    request: ElicitRequest,
) -> EmptyClosingDecision {
    // The base tool context is the only place the loop can reach an elicitation
    // handler: it is a per-session factory, and `QueryParams` carries none.
    let base = (shared.params.tool_context_base)();
    let Some(elicit) = base.elicit.clone() else {
        return EmptyClosingDecision::Settle;
    };
    // A headless or non-interactive session has no user to answer, and waiting
    // would hang the turn.
    if base.permission.should_avoid_prompts || shared.params.signal.is_cancelled() {
        return EmptyClosingDecision::Settle;
    }
    let response = elicit.elicit(request, shared.params.signal.clone()).await;
    match &response {
        ElicitResponse::Answers { answers, .. } => {
            let choice = answers.get(&question).cloned().unwrap_or_default();
            normalize_empty_closing_choice(&choice)
        }
        ElicitResponse::Accept { values } => {
            let choice = values.first().cloned().unwrap_or_default();
            normalize_empty_closing_choice(&choice)
        }
        // A declined or cancelled prompt means "do not keep going", which is the
        // safe reading: failing the turn is recoverable, looping is not.
        ElicitResponse::Decline { .. } | ElicitResponse::UserInput { .. } => {
            EmptyClosingDecision::Stop
        }
    }
}

/// The consultation question text; also the key in a structured answer map.
fn reasoning_over_limit_question_text() -> RuntimeString {
    RuntimeString::from(
        "The model keeps spending its whole output budget on reasoning and returns no answer. Keep retrying, or stop?",
    )
}

/// Two choices, structured so the host renders its question card.
fn reasoning_over_limit_request(question: &RuntimeString) -> ElicitRequest {
    let mut entry = RuntimeObject::new();
    entry.insert("question", RuntimeJson::String(question.clone()));
    entry.insert(
        "header",
        RuntimeJson::String(RuntimeString::from("Output limit")),
    );
    let mut options: Vec<RuntimeJson> = Vec::new();
    let mut retry = RuntimeObject::new();
    retry.insert(
        "label",
        RuntimeJson::String(RuntimeString::from("Keep retrying")),
    );
    retry.insert(
        "description",
        RuntimeJson::String(RuntimeString::from(
            "Ask again for a shorter reasoning pass and a visible answer.",
        )),
    );
    options.push(RuntimeJson::Object(retry));
    let mut stop = RuntimeObject::new();
    stop.insert("label", RuntimeJson::String(RuntimeString::from("Stop")));
    stop.insert(
        "description",
        RuntimeJson::String(RuntimeString::from(
            "End this turn as failed. Nothing visible was produced.",
        )),
    );
    options.push(RuntimeJson::Object(stop));
    entry.insert("options", RuntimeJson::Array(options));
    entry.insert("multiSelect", RuntimeJson::Bool(false));
    // Caller attribution the client uses to localize this engine-authored card;
    // metadata is never read back as an answer.
    let mut metadata = RuntimeObject::new();
    metadata.insert(
        "kind",
        RuntimeJson::String(RuntimeString::from("reasoning_over_limit")),
    );
    ElicitRequest {
        prompt: question.clone(),
        questions: Some(vec![entry]),
        attachments_supported: None,
        metadata: Some(metadata),
        options: None,
        multi_select: None,
    }
}

/// The consultation question text for a normal-stop empty closing; also the key
/// in a structured answer map.
fn empty_closing_question_text() -> RuntimeString {
    RuntimeString::from(
        "The model did visible work but never wrote a closing answer, and stayed empty after a retry. Keep retrying, or stop?",
    )
}

/// Two choices, structured so the host renders its question card.
fn empty_closing_request(question: &RuntimeString) -> ElicitRequest {
    let mut entry = RuntimeObject::new();
    entry.insert("question", RuntimeJson::String(question.clone()));
    entry.insert(
        "header",
        RuntimeJson::String(RuntimeString::from("No closing answer")),
    );
    let mut options: Vec<RuntimeJson> = Vec::new();
    let mut retry = RuntimeObject::new();
    retry.insert(
        "label",
        RuntimeJson::String(RuntimeString::from("Keep retrying")),
    );
    retry.insert(
        "description",
        RuntimeJson::String(RuntimeString::from(
            "Ask the model again to write the closing answer or continue with tool calls.",
        )),
    );
    options.push(RuntimeJson::Object(retry));
    let mut stop = RuntimeObject::new();
    stop.insert("label", RuntimeJson::String(RuntimeString::from("Stop")));
    stop.insert(
        "description",
        RuntimeJson::String(RuntimeString::from(
            "End this turn as failed. The work done so far stays visible.",
        )),
    );
    options.push(RuntimeJson::Object(stop));
    entry.insert("options", RuntimeJson::Array(options));
    entry.insert("multiSelect", RuntimeJson::Bool(false));
    // Caller attribution the client uses to localize this engine-authored card;
    // metadata is never read back as an answer.
    let mut metadata = RuntimeObject::new();
    metadata.insert(
        "kind",
        RuntimeJson::String(RuntimeString::from("empty_closing")),
    );
    ElicitRequest {
        prompt: question.clone(),
        questions: Some(vec![entry]),
        attachments_supported: None,
        metadata: Some(metadata),
        options: None,
        multi_select: None,
    }
}

/// Map a free-form or label-shaped answer onto the two decisions. Anything
/// unrecognised reads as Stop: ending the turn is recoverable, retrying forever
/// is not.
fn normalize_empty_closing_choice(value: &RuntimeString) -> EmptyClosingDecision {
    let Ok(text) = value.try_to_utf8() else {
        return EmptyClosingDecision::Stop;
    };
    let lowered = text.trim().to_lowercase();
    if lowered.contains("retry") || lowered.contains("keep going") || lowered.contains("continue") {
        return EmptyClosingDecision::Retry;
    }
    EmptyClosingDecision::Stop
}
