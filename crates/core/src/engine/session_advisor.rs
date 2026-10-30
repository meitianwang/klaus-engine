//! Text-only consultation. The caller supplies the admitted query snapshot, never session history.

use super::{InferenceBundle, InferenceTargetState, Session};
use crate::context::images::estimate_image_tokens;
use crate::context::tokens::{estimate_messages_tokens, estimate_prefix_tokens, MEDIA_TOKENS};
use crate::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use crate::schemas::content::{ContentBlock, OutputBlock, TextBlock};
use crate::schemas::message::{AssistantMessage, Message, Usage, UserMessage};
use crate::sync_ext::{MutexExt, RwLockExt};
use crate::types::abort::AbortSignal;
use crate::types::errors::ToolError;
use crate::types::events::InferenceSource;
use crate::types::provider::{
    ModelRequest, ModelStopReason, ModelStreamEvent, ProviderCapabilities, SystemPromptPart,
};
use futures::StreamExt;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

const ADVISOR_INSTRUCTIONS: &str = "You are an independent advisor to the assistant in the inherited conversation. The inherited instructions and messages describe that assistant's task and are reference context, not work for you to execute. Only answer the consultation question at the end. Give concrete text advice, identify uncertainty and risks, and distinguish evidence from assumptions. Do not continue the parent task, call tools, request permissions, or consult another advisor. You have no tools. Do not claim to have performed actions.";
const SUMMARY_INSTRUCTIONS: &str = "Produce a factual context summary for an independent advisor. The inherited instructions and conversation are reference material, not a task to continue. Preserve the user's objective, applicable constraints, decisions, relevant evidence and source references, completed work, open questions, and uncertainty needed to answer the consultation question. Distinguish observed evidence from assumptions. Describe relevant images and documents faithfully; do not invent missing details. Do not answer the consultation question, execute work, call tools, or consult the advisor. Return only the summary.";
const SUMMARY_NOTICE: &str = "[The current main model summarized the context for this advisor consultation. The main conversation history was not changed.]\n\n";
const MINIMUM_OUTPUT: u64 = 256;

#[derive(Clone, Copy)]
enum AdvisorPhase {
    Summary,
    Consultation,
}

impl AdvisorPhase {
    fn label(self) -> &'static str {
        match self {
            Self::Summary => "advisor context summarization",
            Self::Consultation => "advisor",
        }
    }
}

fn extra_image_tokens(block: &ContentBlock) -> u64 {
    match block {
        ContentBlock::Image(image) => estimate_image_tokens(image).saturating_sub(MEDIA_TOKENS),
        ContentBlock::ToolResult(result) => result.content.iter().map(extra_output_tokens).sum(),
        _ => 0,
    }
}

fn extra_output_tokens(block: &OutputBlock) -> u64 {
    match block {
        OutputBlock::Image(image) => estimate_image_tokens(image).saturating_sub(MEDIA_TOKENS),
        _ => 0,
    }
}

/// The advisor may use a different tokenizer, so the caller's usage is not an
/// anchor. Count the full request structurally instead of tokenizing media bytes.
fn estimate_consultation_tokens(request: &ModelRequest) -> u64 {
    let image_headroom: u64 = request
        .messages
        .iter()
        .map(|message| match message {
            Message::User(user) => user.content.iter().map(extra_image_tokens).sum(),
            Message::Assistant(assistant) => assistant.content.iter().map(extra_image_tokens).sum(),
            Message::ToolResult(result) => {
                result.content.iter().map(extra_output_tokens).sum::<u64>()
                    + result
                        .extra_user_blocks
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .map(extra_image_tokens)
                        .sum::<u64>()
            }
            _ => 0,
        })
        .sum();
    estimate_prefix_tokens(&request.system, &request.tools)
        .saturating_add(estimate_messages_tokens(&request.messages))
        .saturating_add(image_headroom)
        .saturating_add((request.messages.len() as u64).saturating_mul(8))
        .saturating_add(1_000)
}

fn available_output(request: &ModelRequest, caps: &ProviderCapabilities) -> u64 {
    let estimated = estimate_consultation_tokens(request);
    caps.context_window
        .saturating_sub(estimated)
        .saturating_sub(256_u64.max(estimated / 20))
}

fn text_message(uuid: RuntimeString, text: RuntimeString) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid,
        content: vec![ContentBlock::Text(text_block(text))],
        is_meta: Some(true),
    })
}

fn prepare_text_request(request: &mut ModelRequest) {
    request.tools.clear();
    request.tool_choice = None;
    request.web_search = None;
    request.output_schema = None;
    request.task_budget = None;
    request.context_management = None;
    request.context_hints = None;
    request.thinking_context = None;
    request.skip_cache_write = Some(true);
}

impl Session {
    /// Install before publishing a newly created session. Live changes use atomic inference publication.
    pub fn install_advisor_target(
        &self,
        mut target: Option<InferenceTargetState>,
    ) -> Result<(), String> {
        super::session_targets_part::prepare_targets_requirements_sync(&mut None, &mut target)?;
        let admission = self.inference_admission.lock_recover();
        if admission.closed || self.is_disposed() {
            return Err("cannot configure an advisor on a closing session".into());
        }
        let mut current = self.inference.write_recover();
        let mut next = (**current).clone();
        next.advisor_target = target;
        *current = Arc::new(next);
        Ok(())
    }

    pub fn advisor_target_runtime(&self) -> serde_json::Value {
        match &self.inference_snapshot().advisor_target {
            None => serde_json::Value::Null,
            Some(InferenceTargetState::Unavailable {
                source,
                model_id,
                code,
                message,
            }) => {
                serde_json::json!({"type":"unavailable","source":source,"modelId":model_id,"code":code,"message":message})
            }
            Some(InferenceTargetState::Ready(target)) => serde_json::json!({
                "type":"ready", "source":target.source, "modelId":target.model_id,
                "model":target.model.try_to_utf8().ok(), "capabilities":target.provider.capabilities(&target.model)
            }),
        }
    }

    pub(crate) async fn run_advisor_query(
        self: &Arc<Self>,
        captured: Arc<InferenceBundle>,
        mut request: ModelRequest,
        question: RuntimeString,
        parent_signal: AbortSignal,
    ) -> Result<RuntimeString, ToolError> {
        if parent_signal.is_cancelled() || self.is_disposed() {
            return Err(ToolError::new("advisor consultation cancelled"));
        }
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(captured.config.stream_total_timeout_ms.max(1));
        let target = match &captured.advisor_target {
            None => return Err(ToolError::new("advisor is disabled")),
            Some(InferenceTargetState::Unavailable {
                source,
                model_id,
                code,
                message,
            }) => {
                return Err(ToolError::new(format!(
                    "advisor unavailable: {code}: {source}/{model_id}: {message}"
                )))
            }
            Some(InferenceTargetState::Ready(target)) => target,
        };
        let caps = target.provider.capabilities(&target.model);
        if caps.context_window == 0 || caps.max_output_tokens == 0 {
            return Err(ToolError::new(
                "advisor has no usable context or output capacity",
            ));
        }
        self.check_advisor_spend_limit(&captured)?;
        // Retain the actual calling model and its admitted view before adapting
        // anything to the advisor's source or media capabilities.
        let main_request = request.clone();
        let admitted_history = request.messages.clone();
        request.messages = portable_history(request.messages, &caps)?;
        let question_message = text_message(
            (self.deps.uuid)().into(),
            RuntimeString::from("Advisor consultation question (answer only this question):\n")
                .concat(&question),
        );
        request.messages.push(question_message.clone());
        // Keep the consultation role inside the stable prefix, before volatile reminders.
        let boundary = request
            .system
            .iter()
            .position(|part| part.cacheable != Some(true))
            .unwrap_or(request.system.len());
        let advisor_role = SystemPromptPart {
            name: "advisor-role".into(),
            text: ADVISOR_INSTRUCTIONS.into(),
            cacheable: Some(true),
            cache_scope: None,
        };
        request.system.insert(boundary, advisor_role.clone());
        request.model = target.model.clone();
        request.bind_media_capabilities(&caps);
        let media_types = target
            .provider
            .model_metadata(&target.model)
            .media_input_mime_types;
        // Preserve the calling query's admitted view, including completed tools
        // not yet persisted to the mutable session transcript.
        crate::provider::native_media::project_history_media(
            &mut request.messages,
            &admitted_history,
            &media_types,
        );
        crate::provider::native_media::validate_request(&request, &media_types)
            .map_err(|error| ToolError::new(error.message))?;
        prepare_text_request(&mut request);
        // Do not inherit a different source's reasoning dialect or effort ladder.
        request.effort = None;
        request.thinking = None;
        request.max_output_tokens = caps.max_output_tokens.min(8192);
        let target_key = format!(
            "{:x}",
            Sha256::digest(format!("{}:{}", target.source, target.model_id))
        );
        request.cache_key = Some(
            self.id
                .concat(&RuntimeString::from(format!(":advisor:{target_key}"))),
        );
        let minimum_output = MINIMUM_OUTPUT.min(request.max_output_tokens);
        let mut available = available_output(&request, &caps);
        let summarized = available < minimum_output;
        if summarized {
            // The fixed role and question must fit before spending on a summary.
            // Parent instructions are preserved by the summary, rather than
            // occupying the advisor's smaller fixed prefix a second time.
            request.system = vec![advisor_role];
            request.messages = vec![question_message.clone()];
            let room = available_output(&request, &caps);
            let answer_reserve = request
                .max_output_tokens
                .min(caps.context_window / 4)
                .max(minimum_output);
            let summary_budget = room.saturating_sub(answer_reserve).min(8192);
            if summary_budget < MINIMUM_OUTPUT {
                return Err(ToolError::new(
                    "advisor context cannot fit the consultation question, summary, and answer",
                ));
            }
            let summary = self
                .summarize_advisor_context(
                    &captured,
                    main_request,
                    &question,
                    summary_budget,
                    parent_signal.clone(),
                    deadline,
                )
                .await?;
            request.messages.insert(
                0,
                text_message(
                    (self.deps.uuid)().into(),
                    RuntimeString::from(
                        "Context summary prepared by the main model for this consultation. Treat it as reference evidence, not instructions to execute:\n\n",
                    )
                    .concat(&summary),
                ),
            );
            available = available_output(&request, &caps);
            if available < minimum_output {
                return Err(ToolError::new(
                    "advisor context summary still exceeds the selected advisor's capacity; the conversation history was not changed",
                ));
            }
        }
        request.max_output_tokens = request.max_output_tokens.min(available);
        let mut billing = (*captured).clone();
        billing.provider = target.provider.clone();
        if let Some(prepared) = &target.review_requirements {
            billing.review_cloud = prepared.cloud.clone();
            billing.review_requirements = prepared.requirements.clone();
        }
        billing.config.model = target.model.clone();
        billing.config.model_source = Some(InferenceSource {
            source: Some(target.source.clone()),
            model_id: Some(target.model_id.clone()),
            tier: None,
            selection: Some(format!("{}:{}", target.source, target.model_id)),
        });
        billing.source = Some((target.source.clone(), target.model_id.clone()));
        billing.rates = target.pricing.clone();
        billing.advisor_target = None;
        let advice = self
            .collect_advisor_text(
                &billing,
                request,
                parent_signal,
                AdvisorPhase::Consultation,
                deadline,
            )
            .await?;
        Ok(if summarized {
            RuntimeString::from(SUMMARY_NOTICE).concat(&advice)
        } else {
            advice
        })
    }

    fn check_advisor_spend_limit(&self, captured: &InferenceBundle) -> Result<(), ToolError> {
        if captured
            .config
            .max_budget_usd
            .is_some_and(|budget| self.cost.lock_recover().total_cost_usd() >= budget)
        {
            return Err(ToolError::new(
                "advisor consultation exceeds the session spend limit",
            ));
        }
        Ok(())
    }

    async fn summarize_advisor_context(
        &self,
        captured: &InferenceBundle,
        mut request: ModelRequest,
        question: &RuntimeString,
        summary_budget: u64,
        signal: AbortSignal,
        deadline: tokio::time::Instant,
    ) -> Result<RuntimeString, ToolError> {
        // A per-turn model override can differ from the session's default.
        let mut billing = (*Arc::new(captured.clone())
            .for_selection(&request.model)
            .map_err(|error| {
                ToolError::new(format!(
                    "advisor context summarization unavailable: {error}"
                ))
            })?)
        .clone();
        billing.advisor_target = None;
        let mut caps = billing.provider.capabilities(&request.model);
        caps.context_window = caps.context_window.min(
            billing
                .config
                .model_context_window
                .unwrap_or(caps.context_window),
        );
        request.messages = portable_history(request.messages, &caps)?;
        prepare_text_request(&mut request);
        request.system.push(SystemPromptPart {
            name: "advisor-summary-role".into(),
            text: SUMMARY_INSTRUCTIONS.into(),
            cacheable: Some(true),
            cache_scope: None,
        });
        request.messages.push(text_message(
            (self.deps.uuid)().into(),
            RuntimeString::from(format!(
                "Summarize the full inherited context for the consultation below in at most {summary_budget} tokens. Include relevant system and project constraints as reference context. Preserve evidence references and uncertainty. Do not answer the question.\n\nConsultation question:\n"
            )).concat(question),
        ));
        request.bind_media_capabilities(&caps);
        request.cache_key = Some(self.id.concat(&RuntimeString::from(":advisor-summary")));
        request.context_hints = None;
        request.thinking = None;
        request.effort = None;
        request.max_output_tokens = summary_budget
            .min(caps.max_output_tokens)
            .min(
                billing
                    .config
                    .max_output_tokens
                    .unwrap_or(caps.max_output_tokens),
            )
            .min(available_output(&request, &caps));
        if request.max_output_tokens < MINIMUM_OUTPUT {
            return Err(ToolError::new(
                "advisor context summarization has insufficient capacity in the current main model",
            ));
        }
        self.collect_advisor_text(&billing, request, signal, AdvisorPhase::Summary, deadline)
            .await
    }

    async fn collect_advisor_text(
        &self,
        billing: &InferenceBundle,
        request: ModelRequest,
        parent_signal: AbortSignal,
        phase: AdvisorPhase,
        deadline: tokio::time::Instant,
    ) -> Result<RuntimeString, ToolError> {
        self.check_advisor_spend_limit(billing)?;
        let label = phase.label();
        if parent_signal.is_cancelled() || self.is_disposed() {
            return Err(ToolError::new(format!("{label} cancelled")));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ToolError::new(format!("{label} timed out")));
        }
        let signal = parent_signal.child_token();
        let _cancel_on_drop = CancelAdvisor(signal.clone());
        let request_id = (self.deps.uuid)().into();
        let slot = tokio::time::timeout_at(deadline, self.reserve_usage_receipt(&signal))
            .await
            .map_err(|_| ToolError::new(format!("{label} timed out")))?
            .map_err(|error| ToolError::new(format!("{label} usage admission failed: {error}")))?;
        let mut receipt = AdvisorUsageReceipt {
            session: self,
            billing,
            model: request.model.clone(),
            request_id,
            actual: None,
            slot,
        };
        let mut events = match phase {
            AdvisorPhase::Summary => (billing.deps.call_model)(request, signal.clone()),
            AdvisorPhase::Consultation => billing.provider.stream(request, signal.clone()),
        };
        let mut text = RuntimeString::default();
        let mut invalid_action = false;
        let mut stop = None;
        let total = tokio::time::sleep_until(deadline);
        tokio::pin!(total);
        loop {
            let event = tokio::select! {
                biased;
                _ = signal.cancelled() => return Err(ToolError::new(format!("{label} cancelled"))),
                _ = &mut total => return Err(ToolError::new(format!("{label} timed out"))),
                event = tokio::time::timeout(Duration::from_millis(billing.config.stream_idle_timeout_ms.max(1)), events.next()) =>
                    event.map_err(|_| ToolError::new(format!("{label} response stream timed out")))?,
            };
            match event {
                None => break,
                Some(ModelStreamEvent::TextDelta { text: delta }) => text = text.concat(&delta),
                Some(ModelStreamEvent::TextMessageCompleted {
                    text: completed, ..
                }) => text = completed,
                Some(ModelStreamEvent::UsageSnapshot { usage }) => receipt.actual = Some(usage),
                Some(
                    ModelStreamEvent::ToolUseStart { .. }
                    | ModelStreamEvent::ServerSearchQuery { .. },
                ) => invalid_action = true,
                Some(ModelStreamEvent::MessageStop { stop_reason, usage }) => {
                    if stop.is_some() {
                        return Err(ToolError::new(format!(
                            "{label} returned multiple completions"
                        )));
                    }
                    receipt.actual = Some(usage);
                    stop = Some(stop_reason);
                }
                Some(ModelStreamEvent::Error { error }) => {
                    return Err(ToolError::new(
                        RuntimeString::from(format!("{label} request failed: "))
                            .concat(&error.message),
                    ))
                }
                _ => {}
            }
        }
        if invalid_action || stop == Some(ModelStopReason::ToolUse) {
            return Err(ToolError::new(format!(
                "{label} returned a tool call; no tool was executed"
            )));
        }
        match stop {
            Some(ModelStopReason::EndTurn | ModelStopReason::StopSequence)
                if !super::runtime_trim(&text).is_empty() =>
            {
                Ok(text)
            }
            Some(ModelStopReason::MaxTokens) => Err(ToolError::new(format!(
                "{label} response exceeded its output limit"
            ))),
            Some(ModelStopReason::Refusal) => {
                Err(ToolError::new(format!("{label} declined the consultation")))
            }
            _ => Err(ToolError::new(format!(
                "{label} returned no complete text response"
            ))),
        }
    }
}

/// Keep the latest physical-request receipt even when cancellation or an error
/// drops a stream after usage arrived but before its terminal event.
struct AdvisorUsageReceipt<'a> {
    session: &'a Session,
    billing: &'a InferenceBundle,
    request_id: RuntimeString,
    model: RuntimeString,
    actual: Option<Usage>,
    slot: Option<crate::store::receipt_broker::ReceiptSlot>,
}

impl Drop for AdvisorUsageReceipt<'_> {
    fn drop(&mut self) {
        if let Some(usage) = self.actual.take() {
            self.session.record_reserved_assistant_usage_for(
                &AssistantMessage {
                    uuid: self.request_id.clone(),
                    model: self.model.clone(),
                    content: Vec::new(),
                    usage: Some(usage),
                },
                self.billing,
                self.slot.take(),
            );
        }
    }
}

struct CancelAdvisor(AbortSignal);
impl Drop for CancelAdvisor {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

fn text_block(text: RuntimeString) -> TextBlock {
    TextBlock {
        text,
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

/// Strip only provider replay state. Readable content and media remain intact.
fn portable_history(
    mut messages: Vec<Message>,
    caps: &ProviderCapabilities,
) -> Result<Vec<Message>, ToolError> {
    let mut results = HashSet::new();
    for message in &messages {
        match message {
            Message::ToolResult(result) => {
                results.insert(result.tool_use_id.clone());
            }
            Message::User(user) => {
                for block in &user.content {
                    if let ContentBlock::ToolResult(result) = block {
                        results.insert(result.tool_use_id.clone());
                    }
                }
            }
            _ => {}
        }
    }
    for message in &mut messages {
        match message {
            Message::User(user) => portable_blocks(&mut user.content, &results, caps)?,
            Message::Assistant(assistant) => {
                portable_blocks(&mut assistant.content, &results, caps)?;
                assistant.usage = None;
            }
            Message::ToolResult(result) => {
                portable_outputs(&mut result.content, caps)?;
                if result.content.is_empty() {
                    if let Some(native) = &result.native_tool_output {
                        result.content.push(OutputBlock::Text(text_block(
                            runtime_json::stringify(native),
                        )));
                    }
                }
                result.native_tool_output = None;
                result.tool_kind = None;
                result.output = None;
                if let Some(blocks) = &mut result.extra_user_blocks {
                    portable_blocks(blocks, &results, caps)?;
                }
            }
            _ => {}
        }
    }
    messages.retain(|message| match message {
        Message::Assistant(assistant) => !assistant.content.is_empty(),
        Message::User(user) => !user.content.is_empty(),
        _ => true,
    });
    Ok(messages)
}

fn portable_text(text: &mut TextBlock) -> Result<(), ToolError> {
    text.message_id = None;
    text.phase = None;
    if let Some(mut citations) = text.citations.take() {
        for citation in &mut citations {
            citation.encrypted_index = None;
        }
        if !citations.is_empty() {
            let sources = runtime_codec::encode_value(&citations)
                .map_err(|_| ToolError::new("advisor could not preserve source citations"))?;
            text.text = text
                .text
                .concat(&RuntimeString::from("\n\nSource citations: "))
                .concat(&runtime_json::stringify(&sources));
        }
    }
    Ok(())
}

fn portable_outputs(
    blocks: &mut [OutputBlock],
    caps: &ProviderCapabilities,
) -> Result<(), ToolError> {
    for block in blocks {
        match block {
            OutputBlock::Image(_) if !caps.vision => {
                return Err(ToolError::new(
                    "advisor model cannot read images in the current context",
                ))
            }
            OutputBlock::Text(text) => portable_text(text)?,
            _ => {}
        }
    }
    Ok(())
}

fn portable_blocks(
    blocks: &mut Vec<ContentBlock>,
    results: &HashSet<RuntimeString>,
    caps: &ProviderCapabilities,
) -> Result<(), ToolError> {
    let mut portable = Vec::with_capacity(blocks.len());
    for mut block in std::mem::take(blocks) {
        match &mut block {
            ContentBlock::Thinking(thinking) => {
                if !thinking.text.is_empty() {
                    portable.push(ContentBlock::Text(text_block(thinking.text.clone())));
                }
                continue;
            }
            ContentBlock::RedactedThinking(_) => continue,
            ContentBlock::Text(text) => portable_text(text)?,
            ContentBlock::ToolUse(call) => {
                call.thought_signature = None;
                call.tool_kind = None;
                if !results.contains(&call.id) {
                    // Preserve other pending work without sending dangling function calls.
                    portable.push(ContentBlock::Text(text_block(
                        RuntimeString::from("Pending tool call (not yet completed): ")
                            .concat(&call.name)
                            .concat(&RuntimeString::from(" "))
                            .concat(&runtime_json::stringify(&call.input)),
                    )));
                    continue;
                }
            }
            ContentBlock::ToolResult(result) => portable_outputs(&mut result.content, caps)?,
            ContentBlock::Image(_) if !caps.vision => {
                return Err(ToolError::new(
                    "advisor model cannot read images in the current context",
                ))
            }
            ContentBlock::Document(_) if !caps.native_pdf => {
                return Err(ToolError::new(
                    "advisor model cannot read documents in the current context",
                ))
            }
            ContentBlock::WebSearchCall(search) => {
                for result in &mut search.results {
                    result.encrypted_token = None;
                }
                let value = runtime_codec::encode_value(&block).map_err(|_| {
                    ToolError::new("advisor could not preserve native search context")
                })?;
                portable.push(ContentBlock::Text(text_block(runtime_json::stringify(
                    &value,
                ))));
                continue;
            }
            _ => {}
        }
        portable.push(block);
    }
    *blocks = portable;
    Ok(())
}
