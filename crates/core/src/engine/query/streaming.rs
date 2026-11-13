//! Stream consumption: assembles assistant blocks in stream order, settles
//! provider admission receipts, and recovers provider/factory panics.

// ---------------------------------------------------------------------------
// Streaming + tool dispatch.
// ---------------------------------------------------------------------------

use super::concat_runtime_strings;
use super::dispatch_tool_calls;
use super::error_result as truncated_call_result;
use super::execute_tool_call_after_started;
use super::find_runtime_tool;
use super::provider_metadata;
use super::push_tool_call_started;
use super::record_tool_lifecycle_parts;
use super::request::tool_over_limit_halve_text;
use super::request::tool_over_limit_raise_text;
use super::safe_is_read_only;
use super::support::notify;
use super::support::trim_ecmascript_whitespace;
use super::tool_lifecycle_source_for_tool;
use super::Assembly;
use super::Outcome;
use super::PartialTool;
use super::ProviderCallAdmission;
use super::ProviderCallAdmissionReceipt;
use super::ResolvedToolCall;
use super::Shared;
use super::ToolCallResult;
use super::ToolLifecycleRecordParts;
use super::MAX_TOOL_OVER_LIMIT_RETRIES;
use super::MAX_TRUNCATION_CONTINUATIONS;
use crate::engine::loop_detection::LoopDetector;
use crate::observability::provider_timing::{RequestTiming, StreamStep};
use crate::provider::behavior::ProviderBehaviorProfile;
use crate::provider::observability as provider_observability;
use crate::runtime_values::RuntimeJson;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
use crate::schemas::content::ContentBlock;
use crate::schemas::content::HarnessEnvelope;
use crate::schemas::content::OutputBlock;
use crate::schemas::content::RedactedThinkingBlock;
use crate::schemas::content::TextBlock;
use crate::schemas::content::ThinkingBlock;
use crate::schemas::content::WebSearchCallBlock;
use crate::schemas::message::AssistantMessage;
use crate::schemas::message::Message;
use crate::schemas::message::Usage;
use crate::sync_ext::MutexExt;
use crate::types::errors::ProviderError;
use crate::types::errors::ProviderErrorKind;
use crate::types::errors::ToolError;
use crate::types::events::EngineEvent;
use crate::types::events::NotificationLevel;
use crate::types::events::ThreadItem;
use crate::types::events::ToolLifecyclePhase;
use crate::types::provider::ModelRequest;
use crate::types::provider::ModelStopReason;
use crate::types::provider::ModelStreamEvent;
use crate::types::provider::ToolInputFormat;
use futures::future::FutureExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
fn panic_payload_detail(payload: &(dyn std::any::Any + Send)) -> RuntimeString {
    if let Some(error) = payload.downcast_ref::<ToolError>() {
        error.message.clone()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone().into()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).into()
    } else {
        "internal panic".into()
    }
}

fn provider_admission_failure(stage: &'static str, detail: RuntimeString) -> RuntimeString {
    concat_runtime_strings(&[
        "Provider request admission failed while ".into(),
        stage.into(),
        ": ".into(),
        detail,
    ])
}

async fn settle_provider_admission_receipt(
    stage: &'static str,
    receipt: ProviderCallAdmissionReceipt,
) -> Result<(), RuntimeString> {
    match std::panic::AssertUnwindSafe(receipt).catch_unwind().await {
        Ok(result) => result.map_err(|error| provider_admission_failure(stage, error)),
        Err(payload) => Err(provider_admission_failure(
            stage,
            panic_payload_detail(payload.as_ref()),
        )),
    }
}

async fn settle_provider_admission_receipts(
    provider_receipt: Option<ProviderCallAdmissionReceipt>,
    request_admitted_receipt: Option<ProviderCallAdmissionReceipt>,
) -> Result<(), RuntimeString> {
    match (provider_receipt, request_admitted_receipt) {
        (Some(provider), Some(request)) => {
            // Both retained owners must be polled even if one fails. In
            // particular, a Snapshot durability error cannot strand Setup's
            // consumed-context receipt (or vice versa).
            let (provider, request) = tokio::join!(
                settle_provider_admission_receipt("persisting provider-call phase", provider),
                settle_provider_admission_receipt("persisting accepted request", request),
            );
            provider?;
            request
        }
        (Some(provider), None) => {
            settle_provider_admission_receipt("persisting provider-call phase", provider).await
        }
        (None, Some(request)) => {
            settle_provider_admission_receipt("persisting accepted request", request).await
        }
        (None, None) => Ok(()),
    }
}

async fn settle_provider_factory_panic(
    admission: Option<&Arc<dyn ProviderCallAdmission>>,
    payload: Box<dyn std::any::Any + Send>,
) -> RuntimeString {
    let panic_message = provider_admission_failure(
        "calling the provider factory",
        panic_payload_detail(payload.as_ref()),
    );
    let Some(admission) = admission else {
        return panic_message;
    };

    let receipt =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| admission.call_panicked()));
    match receipt {
        Ok(receipt) => match settle_provider_admission_receipt(
            "persisting an unknown provider-factory outcome",
            receipt,
        )
        .await
        {
            Ok(()) => panic_message,
            Err(durability_error) => {
                concat_runtime_strings(&[panic_message, "; ".into(), durability_error])
            }
        },
        Err(signal_panic) => concat_runtime_strings(&[
            panic_message,
            "; ".into(),
            provider_admission_failure(
                "recording an unknown provider-factory outcome",
                panic_payload_detail(signal_panic.as_ref()),
            ),
        ]),
    }
}

/// Persist what the model already said when a user stop kills the provider stream
/// mid-message.
///
/// The assistant message is normally recorded once, AFTER the stream assembles
/// (see the `on_message` call at the end of `stream_and_dispatch`). An abort
/// returns before that point, so nothing the model had already produced reaches
/// history: the renderer shows the partial reply from the live stream and the
/// first history read after the turn replaces it with nothing. This records the
/// text and reasoning that did arrive, under the same uuid its deltas carried.
///
/// Tool-use blocks are deliberately excluded. An aborted call has no result to
/// pair with, and a dangling tool_use is rejected by the provider on the next
/// request.
async fn persist_aborted_partial_message(
    shared: &Arc<Shared>,
    req: &ModelRequest,
    assistant_uuid: &str,
    assembly: &[Assembly],
    usage: &Option<Usage>,
) {
    if !shared.params.signal.is_cancelled() {
        return;
    }
    let Some(cb) = &shared.params.on_message else {
        return;
    };
    let mut blocks: Vec<ContentBlock> = Vec::new();
    for item in assembly {
        match item {
            Assembly::Text {
                text,
                citations,
                message_id,
                phase,
            } => {
                if text.is_empty() && citations.is_empty() {
                    continue;
                }
                blocks.push(ContentBlock::Text(TextBlock {
                    text: RuntimeString::from_utf16(text.clone()),
                    citations: if citations.is_empty() {
                        None
                    } else {
                        Some(citations.clone())
                    },
                    harness_envelope: None,
                    message_id: Some(message_id.clone()),
                    phase: phase.clone(),
                }));
            }
            Assembly::Thinking {
                text,
                message_id,
                signature,
                encrypted_content,
            } => {
                blocks.push(ContentBlock::Thinking(ThinkingBlock {
                    text: RuntimeString::from_utf16(text.clone()),
                    message_id: Some(message_id.clone()),
                    signature: signature.clone(),
                    encrypted_content: encrypted_content.clone(),
                }));
            }
            Assembly::Redacted { data } => {
                blocks.push(ContentBlock::RedactedThinking(RedactedThinkingBlock {
                    data: data.clone(),
                }));
            }
            Assembly::Search { .. } | Assembly::Tool { .. } => {}
        }
    }
    if blocks.is_empty() {
        return;
    }
    if let Err(error) = cb(Message::Assistant(AssistantMessage {
        uuid: RuntimeString::from(assistant_uuid),
        content: blocks,
        model: req.model.clone(),
        usage: usage.clone(),
    }))
    .await
    {
        // The query-owned hook has already published its persistence failure.
        tracing::debug!(?error, "Partial response persistence callback rejected");
    }
}

#[derive(Default)]
struct DisplayedTextMessage {
    provider_id: Option<RuntimeString>,
    id: RuntimeString,
    phase: Option<RuntimeString>,
    text: Vec<u16>,
}

fn select_displayed_text_message(
    shared: &Arc<Shared>,
    messages: &mut Vec<DisplayedTextMessage>,
    assistant_uuid: &str,
    provider_id: Option<RuntimeString>,
    phase: Option<RuntimeString>,
) -> usize {
    let existing = messages
        .iter()
        .position(|message| message.provider_id == provider_id)
        .or_else(|| (messages.len() == 1 && messages[0].provider_id.is_none()).then_some(0));
    if let Some(index) = existing {
        let message = &mut messages[index];
        if provider_id.is_some() {
            message.provider_id = provider_id;
        }
        if phase.is_some() && message.phase != phase {
            message.phase = phase;
            shared.q.push(EngineEvent::ItemUpdated {
                inference: None,
                item: ThreadItem::AgentMessage {
                    id: message.id.clone(),
                    text: RuntimeString::from_utf16(message.text.clone()),
                    phase: message.phase.clone(),
                },
            });
        }
        return index;
    }
    let id: RuntimeString = if messages.is_empty() {
        assistant_uuid.into()
    } else {
        format!("{assistant_uuid}-text-{}", messages.len() + 1).into()
    };
    shared.q.push(EngineEvent::ItemStarted {
        inference: None,
        item: ThreadItem::AgentMessage {
            id: id.clone(),
            text: RuntimeString::default(),
            phase: phase.clone(),
        },
    });
    messages.push(DisplayedTextMessage {
        provider_id,
        id,
        phase,
        text: Vec::new(),
    });
    messages.len() - 1
}

struct GoalRequestUsage {
    generation: Option<RuntimeString>,
    root: Option<Arc<crate::goal::runtime::GoalRuntime>>,
    descendant: Option<(Arc<crate::goal::runtime::GoalRuntime>, RuntimeString)>,
    latest: std::sync::Mutex<Option<Usage>>,
    settled: std::sync::atomic::AtomicBool,
}
impl Drop for GoalRequestUsage {
    fn drop(&mut self) {
        if self.settled.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let Some(usage) = self
            .latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            return;
        };
        if let Some(goal) = &self.root {
            goal.on_token_usage_for(self.generation.as_ref(), &usage);
        }
        if let Some((goal, generation)) = &self.descendant {
            goal.record_descendant_usage(generation.clone(), &usage);
        }
    }
}
impl GoalRequestUsage {
    fn observe(&self, usage: &Usage) {
        *self
            .latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(usage.clone());
    }
    async fn settle(&self, shared: &Arc<Shared>) -> Result<(), RuntimeString> {
        if self.settled.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        let usage = self
            .latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(usage) = usage else {
            return Ok(());
        };
        if let Some(goal) = &shared.params.goal {
            goal.on_token_usage_for(self.generation.as_ref(), &usage);
        }
        if let Some((goal, generation)) = &shared.params.goal_usage {
            goal.on_descendant_usage(generation.clone(), &usage)
                .await
                .map_err(|error| {
                    RuntimeString::from(format!("descendant goal accounting failed: {error}"))
                })?;
        }
        Ok(())
    }
}

pub(super) async fn stream_and_dispatch(
    shared: &Arc<Shared>,
    req: &ModelRequest,
    profile: &ProviderBehaviorProfile,
    current_attempt: u32,
    loop_detector: Option<&mut LoopDetector>,
) -> Outcome {
    let goal_usage = GoalRequestUsage {
        root: shared.params.goal.clone(),
        descendant: shared.params.goal_usage.clone(),
        generation: shared
            .params
            .goal_request_identity
            .as_ref()
            .and_then(|identity| {
                let captured = identity
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (captured.accounting_enabled && captured.goal_work)
                    .then(|| captured.generation.clone())
                    .flatten()
            }),
        latest: std::sync::Mutex::new(None),
        settled: std::sync::atomic::AtomicBool::new(false),
    };
    let deps = shared.deps();
    let assistant_uuid = (deps.uuid)();
    if let Some(reviewer) = &shared.params.approval_reviewer {
        reviewer.assistant_started(&RuntimeString::from(assistant_uuid.clone()));
    }
    let mut timing = RequestTiming::start(
        "normal",
        &assistant_uuid,
        &req.model,
        Some(&deps.provider_id),
        Some(&shared.params.thread_id),
        current_attempt,
        shared.params.config.stream_idle_timeout_ms,
        shared.params.config.stream_total_timeout_ms,
        shared.params.signal.clone(),
    );
    timing.bind_turn_id(&shared.params.turn_id);
    timing.record_prefix(req);
    let outcome = crate::context::media_budget::REQUEST_MEDIA_REPAIRED
        .scope(
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stream_and_dispatch_inner(
                shared,
                req,
                profile,
                loop_detector,
                StreamRequestIdentity {
                    request_attempt: current_attempt,
                    assistant_uuid: &assistant_uuid,
                },
                &mut timing,
                &goal_usage,
            ),
        )
        .await;
    persist_context_hint_state(shared).await;
    if let Err(message) = goal_usage.settle(shared).await {
        return Outcome::ProviderAdmissionError { message };
    }
    timing.finish(if shared.params.signal.is_cancelled() {
        "cancelled"
    } else if matches!(&outcome, Outcome::Ok { .. }) {
        "completed"
    } else {
        "failed"
    });
    outcome
}

pub(super) async fn persist_context_hint_state(shared: &Arc<Shared>) {
    let (Some(state), Some(rollout)) = (
        shared.params.context_hint_state.as_ref(),
        shared.params.message_rollout.as_ref(),
    ) else {
        return;
    };
    if !state.has_pending_record() {
        return;
    }
    let Ok(_owner) = shared.persistence_in_flight.enter() else {
        return;
    };
    let admission = match rollout.prepare_append().await {
        Ok(admission) => admission,
        Err(error) => {
            shared
                .q
                .fail_persistence(format!("Context hint persistence failed: {error}").into());
            shared.params.signal.cancel();
            return;
        }
    };
    if let Some(payload) = state.take_pending_record() {
        admission.commit(crate::schemas::rollout::RolloutItem::Event(
            crate::schemas::rollout::EventItem {
                event: crate::context::context_hints::CONTEXT_HINT_ROLLOUT_EVENT.into(),
                turn_id: Some(shared.params.turn_id.clone()),
                payload: Some(payload),
            },
        ));
    }
}

struct StreamRequestIdentity<'a> {
    request_attempt: u32,
    assistant_uuid: &'a str,
}

fn cancelled_before_provider_call(assistant_uuid: &str) -> Outcome {
    Outcome::Error {
        error: ProviderError {
            kind: ProviderErrorKind::Network,
            message: "provider request cancelled before transport admission".into(),
            status: None,
            code: None,
            error_type: None,
            media_origins: None,
            media_repair_complete: false,
            credential_recovery: None,
            retry_after_ms: None,
            request_id: None,
        },
        emitted_text: false,
        partial_uuid: assistant_uuid.into(),
    }
}

async fn stream_and_dispatch_inner(
    shared: &Arc<Shared>,
    req: &ModelRequest,
    profile: &ProviderBehaviorProfile,
    mut loop_detector: Option<&mut LoopDetector>,
    request_identity: StreamRequestIdentity<'_>,
    timing: &mut RequestTiming,
    goal_usage: &GoalRequestUsage,
) -> Outcome {
    let StreamRequestIdentity {
        request_attempt,
        assistant_uuid,
    } = request_identity;
    let mut text_buf = Vec::new();
    let mut partial: HashMap<RuntimeString, PartialTool> = HashMap::new();
    let mut assembly: Vec<Assembly> = Vec::new();
    let mut reasoning_count = 0usize;
    let mut active_reasoning: Option<usize> = None;
    let mut usage: Option<Usage> = None;
    let mut stop_reason = ModelStopReason::EndTurn;
    let mut saw_message_stop = false;
    let mut text_messages: Vec<DisplayedTextMessage> = Vec::new();
    let mut active_text_message: Option<usize> = None;
    let mut prestarted: HashMap<RuntimeString, JoinHandle<ToolCallResult>> = HashMap::new();
    let mut resolved_calls: HashMap<RuntimeString, ResolvedToolCall> = HashMap::new();

    shared.observe_fixed_prefix_pressure(req);
    if let Err(error) = shared.validate_media(req) {
        return Outcome::InputError { error: *error };
    }
    if request_contains_document(req) {
        if let Err(error) = shared.validate_pdf(&req.model) {
            return Outcome::InputError { error: *error };
        }
    }

    // Idle watchdog on a model stream: wrap EACH next() in a per-event timeout on
    // a PER-ATTEMPT signal mirrored from the turn signal, so it bounds inter-event
    // silence and cancels only this request on expiry.
    let attempt = shared.params.signal.child_token();
    let mut usage_receipt = match &shared.params.prepare_request_usage {
        Some(prepare) => {
            match prepare(assistant_uuid.into(), req.model.clone(), attempt.clone()).await {
                Ok(receipt) => Some(receipt),
                Err(message) => {
                    if attempt.is_cancelled() {
                        return cancelled_before_provider_call(assistant_uuid);
                    }
                    return Outcome::ProviderAdmissionError { message };
                }
            }
        }
        None => None,
    };
    let goal_lease = if let (Some(goal), Some(identity)) =
        (&shared.params.goal, &shared.params.goal_request_identity)
    {
        match goal
            .prepare_provider_request_with_signal(
                &req.messages,
                identity,
                shared.params.goal_pending_input.as_ref(),
                shared.params.goal_execution_enabled.as_ref(),
                Some(&attempt),
            )
            .await
        {
            Ok(lease) => Some(lease),
            Err(error) => {
                return Outcome::ProviderAdmissionError {
                    message: format!("goal admission superseded: {error}").into(),
                }
            }
        }
    } else {
        None
    };
    if let (Some(goal), Some(identity)) =
        (&shared.params.goal, &shared.params.goal_request_identity)
    {
        if !goal.claim_provider_now(
            identity,
            shared.params.goal_pending_input.as_ref(),
            shared.params.goal_execution_enabled.as_ref(),
        ) {
            return Outcome::ProviderAdmissionError {
                message: "goal admission superseded at the final provider claim".into(),
            };
        }
    }
    if attempt.is_cancelled() {
        return cancelled_before_provider_call(assistant_uuid);
    }
    let provider_call_admission = shared.params.provider_call_admission.as_ref();
    if let Some(admission) = provider_call_admission {
        let before_call =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| admission.before_call()));
        match before_call {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                attempt.cancel();
                return Outcome::ProviderAdmissionError {
                    message: provider_admission_failure(
                        "acknowledging the pre-call boundary",
                        error,
                    ),
                };
            }
            Err(payload) => {
                attempt.cancel();
                return Outcome::ProviderAdmissionError {
                    message: provider_admission_failure(
                        "acknowledging the pre-call boundary",
                        panic_payload_detail(payload.as_ref()),
                    ),
                };
            }
        }
    }

    let mut wire_req = req.clone();
    if let Some(admission) = provider_call_admission {
        let preparation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            admission.prepare_request(&mut wire_req)
        }));
        let error = match preparation {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(payload) => Some(panic_payload_detail(payload.as_ref())),
        };
        if let Some(error) = error {
            attempt.cancel();
            return Outcome::ProviderAdmissionError {
                message: provider_admission_failure("preparing the complete request", error),
            };
        }
    }
    if let Err(error) = super::tool_dispatch::project_request_media(&mut wire_req) {
        return Outcome::InputError { error: *error };
    }
    if attempt.is_cancelled() {
        return cancelled_before_provider_call(assistant_uuid);
    }
    let deps = shared.deps();
    let provider_stream = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        (deps.call_model)(wire_req, attempt.clone())
    }));
    let mut stream = match provider_stream {
        // Scope the stream so the transport-level records a provider writes
        // while it is polled can name the session, and therefore the sub-agent
        // scope, that issued the request.
        Ok(stream) => crate::observability::provider_diagnostics::scoped_trace_stream(
            shared.params.thread_id.clone(),
            stream,
        ),
        Err(payload) => {
            attempt.cancel();
            return Outcome::ProviderAdmissionError {
                message: settle_provider_factory_panic(provider_call_admission, payload).await,
            };
        }
    };

    if let Some(identity) = &shared.params.goal_request_identity {
        identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .provider_admitted = true;
    }

    // This signal is synchronous and precedes every await (including the
    // pre-existing Snapshot callback). Cancellation after `call_model` returns
    // can therefore never make a provider-accepted Setup context look released.
    let provider_receipt = match provider_call_admission {
        Some(admission) => {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                admission.call_returned()
            })) {
                Ok(receipt) => Some(receipt),
                Err(payload) => {
                    attempt.cancel();
                    let phase_error = provider_admission_failure(
                        "recording the returned provider call",
                        panic_payload_detail(payload.as_ref()),
                    );
                    let conservative =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            admission.call_panicked()
                        }));
                    let message = match conservative {
                        Ok(receipt) => match settle_provider_admission_receipt(
                            "persisting an unknown provider-call outcome",
                            receipt,
                        )
                        .await
                        {
                            Ok(()) => phase_error,
                            Err(error) => {
                                concat_runtime_strings(&[phase_error, "; ".into(), error])
                            }
                        },
                        Err(payload) => concat_runtime_strings(&[
                            phase_error,
                            "; ".into(),
                            provider_admission_failure(
                                "recording an unknown provider-call outcome",
                                panic_payload_detail(payload.as_ref()),
                            ),
                        ]),
                    };
                    return Outcome::ProviderAdmissionError { message };
                }
            }
        }
        None => None,
    };

    let request_admitted_receipt = match shared.params.on_provider_request_admitted.as_ref() {
        Some(on_admitted) => {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| on_admitted())) {
                Ok(receipt) => Some(receipt),
                Err(payload) => {
                    attempt.cancel();
                    let callback_error = provider_admission_failure(
                        "starting accepted-request persistence",
                        panic_payload_detail(payload.as_ref()),
                    );
                    let message = match settle_provider_admission_receipts(provider_receipt, None)
                        .await
                    {
                        Ok(()) => callback_error,
                        Err(error) => concat_runtime_strings(&[callback_error, "; ".into(), error]),
                    };
                    return Outcome::ProviderAdmissionError { message };
                }
            }
        }
        None => None,
    };
    if let (Some(lease), Some(goal)) = (goal_lease, &shared.params.goal) {
        if let Err(error) = lease.accepted(goal).await {
            attempt.cancel();
            return Outcome::ProviderAdmissionError {
                message: format!("goal admission receipt failed: {error}").into(),
            };
        }
    }

    if let Err(message) =
        settle_provider_admission_receipts(provider_receipt, request_admitted_receipt).await
    {
        attempt.cancel();
        return Outcome::ProviderAdmissionError { message };
    }
    let idle = shared.config().stream_idle_timeout_ms;
    let total = shared.config().stream_total_timeout_ms;
    let stream_deadline = tokio::time::Instant::now() + Duration::from_millis(total);

    'stream: loop {
        let remaining = stream_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            attempt.cancel();
            timing.finish("total_timeout");
            if let Some(receipt) = &mut usage_receipt {
                receipt.settle();
            }
            persist_aborted_partial_message(shared, req, assistant_uuid, &assembly, &usage).await;
            return Outcome::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Timeout,
                    message: format!("stream total timeout: model response exceeded {total}ms")
                        .into(),
                    status: None,
                    code: None,
                    error_type: None,
                    media_origins: None,
                    media_repair_complete: crate::context::media_budget::request_media_repaired(),
                    credential_recovery: None,
                    retry_after_ms: None,
                    request_id: None,
                },
                emitted_text: !text_buf.is_empty(),
                partial_uuid: assistant_uuid.into(),
            };
        }
        let step = timing.next_event(&mut stream, stream_deadline, false).await;
        let ev = match step {
            StreamStep::Timeout {
                total: total_deadline_is_next,
            } => {
                attempt.cancel();
                timing.finish(if total_deadline_is_next {
                    "total_timeout"
                } else {
                    "idle_timeout"
                });
                ModelStreamEvent::Error {
                    error: ProviderError {
                        kind: if total_deadline_is_next {
                            ProviderErrorKind::Timeout
                        } else {
                            ProviderErrorKind::Network
                        },
                        message: if total_deadline_is_next {
                            format!("stream total timeout: model response exceeded {total}ms")
                                .into()
                        } else {
                            format!("stream idle timeout: no model event for {idle}ms").into()
                        },
                        status: None,
                        code: None,
                        error_type: None,
                        media_origins: None,
                        media_repair_complete: crate::context::media_budget::request_media_repaired(
                        ),
                        credential_recovery: None,
                        retry_after_ms: None,
                        request_id: None,
                    },
                }
            }
            StreamStep::Event(None) => break 'stream,
            StreamStep::Event(Some(ev)) => ev,
            StreamStep::Cancelled => unreachable!("normal request cancellation is provider-owned"),
        };
        match ev {
            ModelStreamEvent::TextMessageCompleted { .. } => {}
            ModelStreamEvent::TextMessage { id, phase } => {
                let index = select_displayed_text_message(
                    shared,
                    &mut text_messages,
                    assistant_uuid,
                    Some(id),
                    phase,
                );
                let message = &text_messages[index];
                for item in &mut assembly {
                    if let Assembly::Text {
                        message_id, phase, ..
                    } = item
                    {
                        if *message_id == message.id {
                            *phase = message.phase.clone();
                        }
                    }
                }
                active_text_message = Some(index);
            }
            ModelStreamEvent::TextDelta { text } => {
                let index = *active_text_message.get_or_insert_with(|| {
                    select_displayed_text_message(
                        shared,
                        &mut text_messages,
                        assistant_uuid,
                        None,
                        None,
                    )
                });
                let message = &mut text_messages[index];
                message.text.extend_from_slice(text.as_utf16());
                text_buf.extend_from_slice(text.as_utf16());
                match assembly.last_mut() {
                    Some(Assembly::Text {
                        text: current,
                        message_id,
                        ..
                    }) if *message_id == message.id => {
                        current.extend_from_slice(text.as_utf16());
                    }
                    _ => assembly.push(Assembly::Text {
                        text: text.as_utf16().to_vec(),
                        citations: Vec::new(),
                        message_id: message.id.clone(),
                        phase: message.phase.clone(),
                    }),
                }
                shared.q.push(EngineEvent::TextDelta {
                    inference: None,
                    message_id: Some(message.id.clone()),
                    phase: message.phase.clone(),
                    text: text.clone(),
                });
                if let Some(ld) = loop_detector.as_deref_mut() {
                    ld.add_text(&text);
                }
            }
            ModelStreamEvent::ThinkingDelta { text } => {
                let index = *active_reasoning.get_or_insert_with(|| {
                    let index = assembly.len();
                    assembly.push(Assembly::Thinking {
                        text: Vec::new(),
                        message_id: format!("{assistant_uuid}-reasoning-{reasoning_count}").into(),
                        signature: None,
                        encrypted_content: None,
                    });
                    reasoning_count += 1;
                    index
                });
                let Assembly::Thinking {
                    text: current,
                    message_id,
                    ..
                } = &mut assembly[index]
                else {
                    unreachable!("active reasoning must own its assembly entry")
                };
                current.extend_from_slice(text.as_utf16());
                shared.q.push(EngineEvent::ThinkingDelta {
                    text,
                    message_id: Some(message_id.clone()),
                    inference: None,
                });
            }
            ModelStreamEvent::ThinkingBlock {
                text,
                signature,
                encrypted_content,
                is_summary,
            } => {
                if is_summary && text.try_to_utf8().is_ok_and(|text| !text.trim().is_empty()) {
                    if let Some(goal) = &shared.params.goal {
                        goal.note_activity();
                    }
                }
                if let Some(index) = active_reasoning.take() {
                    let Assembly::Thinking {
                        text: current,
                        signature: saved_signature,
                        encrypted_content: saved_encrypted,
                        ..
                    } = &mut assembly[index]
                    else {
                        unreachable!("active reasoning must own its assembly entry")
                    };
                    // Opaque replay metadata must not erase already streamed reasoning.
                    let metadata_only = !is_summary
                        && text.is_empty()
                        && (signature.as_ref().is_some_and(|value| !value.is_empty())
                            || encrypted_content
                                .as_ref()
                                .is_some_and(|value| !value.is_empty()));
                    if !metadata_only {
                        *current = text.as_utf16().to_vec();
                    }
                    *saved_signature = signature.filter(|s| !s.is_empty());
                    *saved_encrypted = encrypted_content.filter(|s| !s.is_empty());
                } else {
                    assembly.push(Assembly::Thinking {
                        text: text.as_utf16().to_vec(),
                        message_id: format!("{assistant_uuid}-reasoning-{reasoning_count}").into(),
                        signature: signature.filter(|s| !s.is_empty()),
                        encrypted_content: encrypted_content.filter(|s| !s.is_empty()),
                    });
                    reasoning_count += 1;
                }
            }
            ModelStreamEvent::RedactedThinkingBlock { data } => {
                assembly.push(Assembly::Redacted { data });
            }
            ModelStreamEvent::ToolUseStart {
                id,
                name,
                thought_signature,
                tool_kind,
                input_format,
            } => {
                // First start wins for a call id. Provider adapters may replay a
                // start event; replacing the partial and appending another assembly
                // entry would dispatch the same resolved call twice.
                if partial.contains_key(&id) {
                    continue;
                }
                partial.insert(
                    id.clone(),
                    PartialTool {
                        name,
                        raw_input: RuntimeString::default(),
                        parsed_input: None,
                        thought_signature: thought_signature.filter(|s| !s.is_empty()),
                        tool_kind,
                        input_format: input_format.unwrap_or(ToolInputFormat::Json),
                    },
                );
                assembly.push(Assembly::Tool { id });
            }
            ModelStreamEvent::ServerSearchQuery { id, query, action } => {
                assembly.push(Assembly::Search {
                    id,
                    query,
                    action,
                    results: Vec::new(),
                    error: None,
                });
            }
            ModelStreamEvent::WebSearchResult { id, results, error } => {
                let idx = assembly
                    .iter()
                    .position(|a| matches!(a, Assembly::Search { id: sid, .. } if *sid == id));
                match idx {
                    Some(i) => {
                        if let Assembly::Search {
                            results: r,
                            error: e,
                            ..
                        } = &mut assembly[i]
                        {
                            *r = results;
                            if let Some(err) = error.filter(|s| !s.is_empty()) {
                                *e = Some(err);
                            }
                        }
                    }
                    None => assembly.push(Assembly::Search {
                        id,
                        query: RuntimeString::default(),
                        action: None,
                        results,
                        error: error.filter(|s| !s.is_empty()),
                    }),
                }
            }
            ModelStreamEvent::Citation { citation } => {
                let mut attached = false;
                for a in assembly.iter_mut().rev() {
                    if let Assembly::Text { citations, .. } = a {
                        citations.push(citation.clone());
                        attached = true;
                        break;
                    }
                }
                if !attached {
                    assembly.push(Assembly::Text {
                        text: Vec::new(),
                        citations: vec![citation],
                        message_id: active_text_message
                            .map(|index| text_messages[index].id.clone())
                            .unwrap_or_else(|| assistant_uuid.into()),
                        phase: active_text_message
                            .and_then(|index| text_messages[index].phase.clone()),
                    });
                }
            }
            ModelStreamEvent::ToolUseInputDelta { id, partial_json } => {
                if let Some(p) = partial.get_mut(&id) {
                    p.raw_input = p.raw_input.concat(&partial_json);
                    p.parsed_input = None;
                }
            }
            ModelStreamEvent::ToolUseStop { id } => {
                // First completion wins for this stream attempt. Resolution and every
                // completion side effect are cached together so duplicate stop events
                // cannot overwrite a prestarted handle or execute the call twice.
                if resolved_calls.contains_key(&id) {
                    continue;
                }
                if let Some(p) = partial.get_mut(&id) {
                    p.finalize_input();
                    let call = ResolvedToolCall::new(p.to_tool_use_block(&id), profile.clone());
                    let tools = shared.tools.lock_recover().clone();
                    let tool = find_runtime_tool(&tools, &call.internal_name);
                    let source = tool
                        .as_ref()
                        .map(|found| tool_lifecycle_source_for_tool(found.as_ref()));
                    if let Err(error) = record_tool_lifecycle_parts(
                        shared,
                        ToolLifecycleRecordParts {
                            tool_use_id: &id,
                            tool_name: &call.internal_name,
                            phase: ToolLifecyclePhase::Requested,
                            source,
                            arguments: p
                                .arguments_are_parseable()
                                .then(|| call.initial_internal_input.clone()),
                            is_error: None,
                            error: None,
                        },
                    )
                    .await
                    {
                        shared
                            .params
                            .retained_tool_tasks
                            .as_ref()
                            .expect("query owns retained tool tasks")
                            .record_failure(error.message.clone());
                        return Outcome::Fatal { error };
                    }
                    if !shared.params.signal.is_cancelled() {
                        if let Some(ld) = loop_detector.as_deref_mut() {
                            ld.observe_tool_call();
                        }
                        if let Some(tool) = tool {
                            if p.arguments_are_parseable()
                                && tool.mcp_server().is_none()
                                && call.internal_name != "advisor"
                                && safe_is_read_only(tool.as_ref(), &call.initial_internal_input)
                            {
                                let _handoff = match shared.persistence_in_flight.enter() {
                                    Ok(owner) => owner,
                                    Err(error) => return Outcome::Fatal { error },
                                };
                                let registration = match shared
                                    .params
                                    .retained_tool_tasks
                                    .as_ref()
                                    .expect("query owns retained tool tasks")
                                    .reserve()
                                {
                                    Ok(registration) => registration,
                                    Err(error) => return Outcome::Fatal { error },
                                };
                                if let Err(error) = push_tool_call_started(shared, &call).await {
                                    return Outcome::Fatal { error };
                                }
                                prestarted.insert(
                                    id.clone(),
                                    registration.spawn(execute_tool_call_after_started(
                                        Arc::clone(shared),
                                        call.clone(),
                                    )),
                                );
                                // No explicit yield here: forcing a scheduler
                                // yield after each prestart let a fast
                                // read-only sibling run to completion before
                                // the next sibling's ToolUseStop was processed,
                                // so its item.completed could precede a later
                                // sibling's item.started and the observed
                                // concurrency undercounted. Prestarted bodies
                                // still make progress at the stream's own await
                                // points (the next chunk, or a scripted turn's
                                // await), which preserves eager execution while
                                // keeping every sibling's start ahead of any
                                // completion within one assistant message.
                            }
                        }
                    }
                    resolved_calls.insert(id, call);
                }
            }
            ModelStreamEvent::MessageStart { .. } => {}
            ModelStreamEvent::UsageSnapshot { usage: sample } => {
                if let Some(receipt) = &mut usage_receipt {
                    receipt.observe(&sample);
                }
                goal_usage.observe(&sample);
                usage = Some(sample);
            }
            ModelStreamEvent::ContextEditsApplied {
                cleared_tool_uses,
                cleared_input_tokens,
                cleared_thinking_turns,
            } => {
                if let Some(state) = &shared.params.context_edit_state {
                    state.record(
                        cleared_tool_uses,
                        cleared_input_tokens,
                        cleared_thinking_turns,
                    );
                }
                tracing::debug!(
                    cleared_tool_uses,
                    cleared_input_tokens,
                    cleared_thinking_turns,
                    "server-side context editing applied"
                );
            }
            ModelStreamEvent::MessageStop {
                stop_reason: sr,
                usage: u,
            } => {
                if sr == ModelStopReason::EndTurn
                    && String::from_utf16(&text_buf).is_ok_and(|text| text.trim().is_empty())
                {
                    if let Some(goal) = &shared.params.goal {
                        goal.note_empty_final();
                    }
                }
                saw_message_stop = true;
                if let Some(receipt) = &mut usage_receipt {
                    receipt.observe(&u);
                }
                goal_usage.observe(&u);
                if let Err(message) = goal_usage.settle(shared).await {
                    return Outcome::ProviderAdmissionError { message };
                }
                usage = Some(u);
                stop_reason = sr;
            }
            ModelStreamEvent::Error { error } => {
                if let Some(receipt) = &mut usage_receipt {
                    receipt.settle();
                }
                persist_aborted_partial_message(shared, req, assistant_uuid, &assembly, &usage)
                    .await;
                return Outcome::Error {
                    error,
                    emitted_text: !text_buf.is_empty(),
                    partial_uuid: assistant_uuid.into(),
                };
            }
            ModelStreamEvent::RetryScheduled {
                attempt,
                max_attempts,
                delay_ms,
                error,
                network_wait,
            } => {
                // Establishment retry inside the provider: surface it while the
                // backoff sleeps so hosts never sit on a silent stream. It is
                // pure progress reporting — nothing was assembled yet.
                let metadata = provider_metadata(shared, req);
                let phase = if network_wait {
                    provider_observability::RetryPhase::NetworkWait
                } else {
                    provider_observability::RetryPhase::Connection
                };
                shared.q.push(EngineEvent::ProviderDecision {
                    inference: None,
                    event: provider_observability::retry_scheduled(
                        &metadata,
                        attempt,
                        max_attempts,
                        phase,
                        delay_ms,
                        &error,
                    ),
                });
                let message = if network_wait {
                    "Reconnecting... waiting for network".to_string()
                } else {
                    format!(
                        "provider connect error ({}); retry {}/{} in {}ms",
                        error.kind.as_str(),
                        attempt + 1,
                        max_attempts,
                        delay_ms
                    )
                };
                notify(&shared.q, NotificationLevel::Warn, message);
            }
        }
    }

    if !saw_message_stop {
        if let Some(receipt) = &mut usage_receipt {
            receipt.settle();
        }
        persist_aborted_partial_message(shared, req, assistant_uuid, &assembly, &usage).await;
        return Outcome::Error {
            error: ProviderError {
                kind: ProviderErrorKind::Network,
                message: "provider stream ended before message_stop".into(),
                status: None,
                code: None,
                error_type: None,
                media_origins: None,
                media_repair_complete: false,
                credential_recovery: None,
                retry_after_ms: None,
                request_id: None,
            },
            emitted_text: !text_buf.is_empty(),
            partial_uuid: assistant_uuid.into(),
        };
    }

    timing.finish(if shared.params.signal.is_cancelled() {
        "cancelled"
    } else {
        "completed"
    });

    let text_buf = RuntimeString::from_utf16(text_buf);

    // Assemble the assistant message in stream order.
    let mut blocks: Vec<ContentBlock> = Vec::new();
    for item in assembly {
        match item {
            Assembly::Text {
                text,
                citations,
                message_id,
                phase,
            } => {
                if !text.is_empty() || !citations.is_empty() {
                    blocks.push(ContentBlock::Text(TextBlock {
                        text: RuntimeString::from_utf16(text),
                        citations: if citations.is_empty() {
                            None
                        } else {
                            Some(citations)
                        },
                        harness_envelope: None,
                        message_id: Some(message_id),
                        phase,
                    }));
                }
            }
            Assembly::Thinking {
                text,
                message_id,
                signature,
                encrypted_content,
            } => {
                blocks.push(ContentBlock::Thinking(ThinkingBlock {
                    text: RuntimeString::from_utf16(text),
                    message_id: Some(message_id),
                    signature,
                    encrypted_content,
                }));
            }
            Assembly::Redacted { data } => {
                blocks.push(ContentBlock::RedactedThinking(RedactedThinkingBlock {
                    data,
                }));
            }
            Assembly::Search {
                id,
                query,
                action,
                results,
                error,
            } => {
                blocks.push(ContentBlock::WebSearchCall(WebSearchCallBlock {
                    id,
                    query,
                    action,
                    results,
                    error,
                }));
            }
            Assembly::Tool { id } => {
                if let Some(p) = partial.get(&id) {
                    if !resolved_calls.contains_key(&id) {
                        let call = ResolvedToolCall::new(p.to_tool_use_block(&id), profile.clone());
                        let tools = shared.tools.lock_recover().clone();
                        let tool = find_runtime_tool(&tools, &call.internal_name);
                        let source = tool
                            .as_ref()
                            .map(|tool| tool_lifecycle_source_for_tool(tool.as_ref()));
                        if let Err(error) = record_tool_lifecycle_parts(
                            shared,
                            ToolLifecycleRecordParts {
                                tool_use_id: &id,
                                tool_name: &call.internal_name,
                                phase: ToolLifecyclePhase::Requested,
                                source,
                                arguments: p
                                    .arguments_are_parseable()
                                    .then(|| call.initial_internal_input.clone()),
                                is_error: None,
                                error: None,
                            },
                        )
                        .await
                        {
                            shared
                                .params
                                .retained_tool_tasks
                                .as_ref()
                                .expect("query owns retained tool tasks")
                                .record_failure(error.message.clone());
                            return Outcome::Fatal { error };
                        }
                        if !shared.params.signal.is_cancelled() {
                            if let Some(detector) = loop_detector.as_deref_mut() {
                                detector.observe_tool_call();
                            }
                        }
                        resolved_calls.insert(id.clone(), call);
                    }
                    if let Some(call) = resolved_calls.get(&id) {
                        blocks.push(ContentBlock::ToolUse(call.visible.clone()));
                    }
                }
            }
        }
    }

    // Truncation footnote, written into the message body BEFORE it is persisted.
    //
    // The rollout store is append-only, so a footnote added later cannot join the
    // message that already landed: it would arrive as a second assistant message
    // (which `project_for_prompt` appends without deduplication, so a resume would
    // show the answer twice) or as a live-only notice that a reload erases. Both
    // leave a reader with a sentence that simply stops.
    //
    // Deciding here is what makes the body authoritative. The ladder count lives
    // on `Shared` because only the turn loop knows how many continuations are
    // left, while only this function knows when the message is about to be
    // written. A response cut off WITH visible text on screen and no continuation
    // left is the exhausted case; a response with no visible text at all is the
    // reasoning-over-limit case, which blocks on the user instead and never
    // silently settles.
    let has_tool_call = blocks
        .iter()
        .any(|block| matches!(block, ContentBlock::ToolUse(_)));
    let private_review = shared
        .params
        .agent_type
        .as_ref()
        .is_some_and(|kind| kind == "approval-review");
    let ladder_exhausted =
        !private_review && stop_reason == ModelStopReason::MaxTokens && !has_tool_call && {
            let truncation = shared.truncation.lock_recover();
            truncation.continuations >= MAX_TRUNCATION_CONTINUATIONS
        };
    let footnote = if ladder_exhausted && !trim_ecmascript_whitespace(&text_buf).is_empty() {
        shared.truncation.lock_recover().footnote_written = true;
        Some(super::request::truncation_footnote(
            MAX_TRUNCATION_CONTINUATIONS,
        ))
    } else {
        None
    };
    if let Some(footnote) = &footnote {
        if let Some(ContentBlock::Text(text_block)) = blocks
            .iter_mut()
            .rev()
            .find(|block| matches!(block, ContentBlock::Text(text) if !text.text.is_empty()))
        {
            text_block.text = text_block.text.concat(footnote);
        }
        // The live `item.completed` below is built from `text_messages`, not from
        // `blocks`, so it needs the same footnote or the screen and the transcript
        // would disagree about how the answer ended.
        if let Some(message) = text_messages
            .iter_mut()
            .rev()
            .find(|message| !message.text.is_empty())
        {
            message.text.extend_from_slice(footnote.as_utf16());
            // And the live screen renders text from deltas, not from the completed
            // item, so without this the footnote would only appear after a reload —
            // the reader watching the turn would still see a sentence that stops.
            shared.q.push(EngineEvent::TextDelta {
                inference: None,
                message_id: Some(message.id.clone()),
                phase: message.phase.clone(),
                text: footnote.clone(),
            });
        }
    }

    let assistant = AssistantMessage {
        uuid: assistant_uuid.into(),
        content: blocks.clone(),
        model: req.model.clone(),
        usage: usage.clone(),
    };
    if let Some(receipt) = &mut usage_receipt {
        receipt.settle();
    }
    if let Some(cb) = &shared.params.on_message {
        if let Err(message) = cb(Message::Assistant(assistant.clone())).await {
            return Outcome::ProviderAdmissionError { message };
        }
    }
    for (position, block) in blocks.iter().enumerate() {
        if let ContentBlock::Thinking(thinking) = block {
            shared.q.push(EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::Reasoning {
                    id: thinking
                        .message_id
                        .clone()
                        .unwrap_or_else(|| format!("{assistant_uuid}-{position}").into()),
                    text: thinking.text.clone(),
                },
            });
        }
    }
    for message in text_messages {
        if !message.text.is_empty() {
            shared.q.push(EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::AgentMessage {
                    id: message.id,
                    text: RuntimeString::from_utf16(message.text),
                    phase: message.phase,
                },
            });
        }
    }

    // Provider usage settles before tool execution; a later fatal tool error
    // must not turn a completed, billed model request into a provider failure.
    shared.q.push(EngineEvent::ProviderDecision {
        inference: None,
        event: provider_observability::request_finished(
            &provider_metadata(shared, req),
            request_attempt,
            usage.as_ref(),
        ),
    });
    let mut messages: Vec<Message> = req.messages.clone();
    messages.push(Message::Assistant(assistant));
    let calls: Vec<ResolvedToolCall> = blocks
        .iter()
        .filter_map(|b| match b {
            ContentBlock::ToolUse(t) => resolved_calls.get(&t.id).cloned(),
            _ => None,
        })
        .collect();
    if calls.is_empty() {
        return Outcome::Ok {
            messages,
            usage,
            tool_results: Vec::new(),
            tool_result_handoff: None,
            stop_reason,
            final_text: text_buf.clone(),
            sleep_ran: false,
            truncated_tool_calls: Vec::new(),
        };
    }

    let tool_result_handoff = match shared.persistence_in_flight.enter() {
        Ok(guard) => Some(guard),
        Err(error) => return Outcome::Fatal { error },
    };
    let output_limit_reached = stop_reason == ModelStopReason::MaxTokens
        || (req.max_output_tokens > 0
            && usage
                .as_ref()
                .is_some_and(|usage| usage.output_tokens >= req.max_output_tokens));

    // A call whose arguments cannot be read is answered as a failure and never
    // dispatched. Unparseable means the stream ended mid-call, so what arrived is a
    // prefix; dispatching it would hand the tool an EMPTY object (see
    // `PartialTool::provider_input`), i.e. an edit with no path or a command with no
    // command. The model is told, so it reissues the call — the loop continues on
    // these results rather than losing the work. A write missing required fields
    // at the output ceiling follows the same safe retry path, even when a gateway
    // returned a syntactically valid empty object or no argument deltas at all.
    let (dropped, calls): (Vec<ResolvedToolCall>, Vec<ResolvedToolCall>) =
        calls.into_iter().partition(|call| {
            partial
                .get(&call.visible.id)
                .is_some_and(|p| !p.arguments_are_parseable())
                || (output_limit_reached
                    && call.internal_name == "write"
                    && ["path", "content"].iter().any(|field| {
                        call.initial_internal_input
                            .get(*field)
                            .and_then(RuntimeJson::as_string)
                            .is_none()
                    }))
        });

    // Salvage a `write` the output ceiling cut off, and only once truncation is
    // confirmed. Two gates, both load-bearing:
    //
    // - `stop_reason == MaxTokens`: without it, a merely malformed call would be
    //   repaired and written, inventing content the model never asked for.
    // - the tool is `write`: a value cut short mid-string is recoverable for a
    //   file's CONTENTS (the rest arrives via the append protocol below) and is
    //   not recoverable for, say, a shell command — a truncated command that runs
    //   is worse than one that does not.
    //
    // A salvaged call goes through the normal dispatch chain, so permission,
    // path-safety and file-state recording all still apply to it.
    let (calls, dropped, recovered_write_paths): (
        Vec<ResolvedToolCall>,
        Vec<ResolvedToolCall>,
        Vec<(RuntimeString, RuntimeString)>,
    ) = if stop_reason == ModelStopReason::MaxTokens {
        let mut calls = calls;
        let mut dropped_kept: Vec<ResolvedToolCall> = Vec::new();
        let mut recovered: Vec<(RuntimeString, RuntimeString)> = Vec::new();
        for call in dropped {
            let id = call.visible.id.clone();
            let is_write = call.internal_name == "write";
            let salvaged = is_write
                && partial
                    .get_mut(&id)
                    .and_then(|p| p.salvage_truncated_input())
                    .is_some();
            if salvaged {
                let rebuilt = partial
                    .get(&id)
                    .map(|p| ResolvedToolCall::new(p.to_tool_use_block(&id), profile.clone()));
                let path = rebuilt.as_ref().and_then(|call| {
                    call.initial_internal_input
                        .get("path")
                        .and_then(RuntimeJson::as_string)
                        .cloned()
                        .filter(|value| !value.is_empty())
                });
                match (rebuilt, path) {
                    (Some(call), Some(path))
                        if truncated_write_mode_is_known(shared, &call, &path).await =>
                    {
                        recovered.push((call.visible.id.clone(), path));
                        calls.push(call);
                    }
                    _ => dropped_kept.push(call),
                }
            } else {
                dropped_kept.push(call);
            }
        }
        (calls, dropped_kept, recovered)
    } else {
        (calls, dropped, Vec::new())
    };

    // Some gateways report tool completion even when an invalid argument stream
    // exhausted the requested output budget. Usage can enable safe retry guidance,
    // but only the explicit MaxTokens reason above can authorize a partial write.
    let tool_output_over_limit = !dropped.is_empty() && output_limit_reached;
    let truncated_tool_calls: Vec<RuntimeString> = if tool_output_over_limit {
        dropped
            .iter()
            .map(|call| call.visible.name.clone())
            .collect()
    } else {
        Vec::new()
    };
    // The retry instruction rides ON the dropped call's error result rather than
    // arriving as its own message. That is not a style choice: the error result is
    // persisted here, so a separate nudge would be persisted before it and a
    // resumed session would see the assistant's `tool_use` followed by a user
    // message with the `tool_result` only after — a pairing a strict provider
    // rejects, and one that makes the instruction look like it preceded the
    // failure it explains.
    //
    // Ordinary malformed calls below the budget receive no budget promise.
    let tool_over_limit_attempt = if tool_output_over_limit {
        let mut truncation = shared.truncation.lock_recover();
        if truncation.tool_over_limit_retries < MAX_TOOL_OVER_LIMIT_RETRIES {
            truncation.tool_over_limit_retries += 1;
            Some(truncation.tool_over_limit_retries)
        } else {
            None
        }
    } else {
        None
    };
    // When the cut is confirmed to be the output token limit, say
    // so explicitly — the call did not run, nothing of it was kept, and the
    // retry should be shorter and split. An ordinary malformed call (no limit
    // involved) keeps its own wording.
    let dropped_base_message = if tool_output_over_limit {
        RuntimeString::from(
            "Your response was cut off because it exceeded the output token limit. The cut fell inside a tool call, so that call did not run and nothing of it was kept. Make the call again in a shorter response: split long content across several smaller tool calls. For a long file, write the first part and then append the rest in later calls.",
        )
    } else {
        RuntimeString::from(
            "This tool call was dropped: its arguments arrived incomplete, so they cannot be read. Issue the call again, on its own if it is long.",
        )
    };
    let mut tool_results = Vec::with_capacity(dropped.len());
    for (index, call) in dropped.iter().enumerate() {
        // Keep the original lifecycle order while awaiting each acknowledgement.
        let extra = match (tool_over_limit_attempt, index) {
            (Some(1), 0) => vec![tool_over_limit_raise_text(&call.visible.name)],
            (Some(_), 0) => vec![tool_over_limit_halve_text()],
            _ => Vec::new(),
        };
        tool_results.push(truncated_call_result(
            call,
            shared,
            dropped_base_message.clone(),
            &extra,
        ).await);
    }
    if calls.is_empty() {
        messages.extend(tool_results.iter().cloned().map(Message::ToolResult));
        return Outcome::Ok {
            messages,
            usage,
            tool_results,
            tool_result_handoff,
            stop_reason,
            final_text: text_buf,
            sleep_ran: false,
            truncated_tool_calls,
        };
    }

    // Publish exactly what the model was shown, so a tool that names messages
    // resolves against the same history rather than a later mutation of it.
    // Context-dependent consultation starts only after the response is assembled.
    // This includes the current assistant, not a stale session transcript.
    *shared.advisor_request.lock_recover() = if req.tools.iter().any(|tool| tool.name == "advisor")
        && calls.iter().any(|call| call.internal_name == "advisor")
    {
        let mut advisor_request = req.clone();
        advisor_request.messages = messages.clone();
        Some(advisor_request)
    } else {
        None
    };
    match dispatch_tool_calls(shared, &calls, prestarted).await {
        Ok(results) => tool_results.extend(results),
        Err(error) => return Outcome::Fatal { error },
    };
    // Tell the model how to finish a file it only got part of, and hand it the
    // file's ACTUAL tail rather than making it go look: the append protocol checks
    // `continuation_context` against those exact lines, so a guess would be
    // refused and cost another round trip.
    if !recovered_write_paths.is_empty() {
        append_write_continuation_instructions(shared, &mut tool_results, &recovered_write_paths)
            .await;
    }
    messages.extend(tool_results.iter().cloned().map(Message::ToolResult));
    let sleep_ran = calls.iter().any(|call| call.internal_name == "sleep");
    Outcome::Ok {
        messages,
        usage,
        tool_results,
        tool_result_handoff,
        stop_reason,
        final_text: text_buf,
        sleep_ran,
        truncated_tool_calls,
    }
}

async fn truncated_write_mode_is_known(
    shared: &Arc<Shared>,
    call: &ResolvedToolCall,
    path: &RuntimeString,
) -> bool {
    if call
        .initial_internal_input
        .get("append")
        .and_then(RuntimeJson::as_bool)
        .is_some()
    {
        return true;
    }
    // With an existing target, an append flag could still have followed the
    // unfinished content. Do not turn that undecided mode into an overwrite.
    // A new target has no earlier contents to lose; normal dispatch still checks
    // permissions and file freshness before writing any recovered prefix.
    let base = (shared.params.tool_context_base)();
    let Ok(abs) =
        crate::fs::node_path::resolve_runtime(&base.cwd, &[RuntimePath::from(path.clone())])
    else {
        return false;
    };
    let Ok(native) = crate::fs::node_path::project_runtime_path(&abs, "path") else {
        return false;
    };
    matches!(tokio::fs::symlink_metadata(native).await,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound)
}

pub(super) fn request_contains_document(req: &ModelRequest) -> bool {
    req.messages.iter().any(|message| match message {
        Message::User(user) => user
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Document(_))),
        Message::Assistant(assistant) => assistant
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Document(_))),
        Message::ToolResult(result) => result.extra_user_blocks.as_ref().is_some_and(|blocks| {
            blocks
                .iter()
                .any(|block| matches!(block, ContentBlock::Document(_)))
        }),
        Message::System(_) | Message::CompactBoundary(_) => false,
    })
}

/// Attach the append-protocol instruction to each salvaged `write` result.
///
/// It carries the file's ACTUAL last lines, read back from file state, rather
/// than asking the model to go look: the append protocol checks
/// `continuation_context` against exactly those lines, so a value the model
/// reconstructs from memory would be refused and cost another round trip.
///
/// Attached to the tool result as a system reminder rather than sent as its own
/// message, so it stays bound to the call it explains and is persisted with it.
async fn append_write_continuation_instructions(
    shared: &Arc<Shared>,
    tool_results: &mut [crate::schemas::message::ToolResultMessage],
    recovered: &[(RuntimeString, RuntimeString)],
) {
    let base = (shared.params.tool_context_base)();
    for (call_id, path) in recovered {
        let Some(result) = tool_results
            .iter_mut()
            .find(|result| &result.tool_use_id == call_id)
        else {
            continue;
        };
        let abs =
            crate::fs::node_path::resolve_runtime(&base.cwd, &[RuntimePath::from(path.clone())])
                .unwrap_or_else(|_| RuntimePath::from(path.clone()));
        // The write just recorded what it wrote, so file state holds the exact
        // bytes now on disk. A ranged or failed write records nothing, and then
        // there is no tail to hand over — the instruction is still worth sending,
        // with the model told to read the tail itself.
        let tail = base
            .file_state
            .get(&abs)
            .and_then(|entry| entry.content)
            .map(|content| crate::tools::builtins::write::continuation_context_for(&content));
        let text = match &tail {
            Some(continuation) => {
                crate::tools::builtins::write::truncated_write_continuation(path, continuation)
            }
            None => RuntimeString::from(format!(
                "File creation was truncated. Read the last 3 lines of {} and continue writing by calling Write with append: true, passing those lines as continuation_context.",
                String::from_utf8_lossy(&path.to_node_utf8()),
            )),
        };
        result.content.push(OutputBlock::Text(TextBlock {
            text,
            citations: None,
            harness_envelope: Some(HarnessEnvelope::SystemReminder),
            message_id: None,
            phase: None,
        }));
    }
}
