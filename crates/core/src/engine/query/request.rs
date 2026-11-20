//! Model-request assembly, retry/backoff helpers, and the provider-facing
//! continuation / recovery envelope rendering for one query turn.

use super::QueryParams;
use super::Shared;
use super::SystemPromptResolvable;
use super::TurnOverride;
use crate::provider::behavior::render_provider_text_envelope;
use crate::provider::behavior::render_system_prompt_for_behavior;
use crate::provider::behavior::render_tool_name;
use crate::provider::behavior::render_tool_specs_for_behavior;
use crate::provider::behavior::resolve_internal_tool_name;
use crate::provider::behavior::resolve_provider_behavior_profile;
use crate::provider::behavior::ProviderBehaviorProfile;
use crate::provider::behavior::ProviderTextEnvelopeKind;
use crate::provider::observability::ProviderRequestMetadata;
use crate::runtime_values::RuntimeJson;
use crate::runtime_values::RuntimeString;
use crate::schemas::content::ContentBlock;
use crate::schemas::content::HarnessEnvelope;
use crate::schemas::content::OutputBlock;
use crate::schemas::content::TextBlock;
use crate::schemas::message::Message;
use crate::schemas::message::UserMessage;
use crate::sync_ext::MutexExt;
use crate::tools::pool::to_tool_spec;
use crate::types::errors::ProviderError;
use crate::types::errors::ProviderErrorKind;
use crate::types::provider::ModelRequest;
use crate::types::provider::ProviderInfo;
use crate::types::provider::ProviderToolCallKind;
use crate::types::provider::SystemPromptPart;
use crate::types::provider::ThinkingConfig;
use crate::types::provider::ThinkingContextPolicy;
use crate::types::provider::ToolChoice;
use crate::types::provider::ToolSpec;
use crate::types::tool::Tool;
use crate::types::tool::ToolCatalogEntry;
use crate::types::tool::ToolExposure;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
pub(super) fn is_retryable(error: &ProviderError) -> bool {
    matches!(
        error.kind,
        ProviderErrorKind::RateLimit
            | ProviderErrorKind::Overloaded
            | ProviderErrorKind::Network
            | ProviderErrorKind::Timeout
    )
}

pub(super) fn backoff_ms(attempt: u32, retry_after_ms: Option<u64>) -> u64 {
    if let Some(retry) = retry_after_ms {
        return retry.min(30_000);
    }
    (100u64.saturating_mul(1u64 << (attempt.saturating_sub(1)).min(20))).min(5_000)
}

pub(super) fn provider_metadata(shared: &Shared, req: &ModelRequest) -> ProviderRequestMetadata {
    ProviderRequestMetadata::new(shared.deps().provider_id, req.model.clone())
}

/// Sleep for `ms`, resolving early if the turn is aborted.
pub async fn abortable_sleep(ms: u64, signal: &crate::types::abort::AbortSignal) {
    if signal.is_cancelled() {
        return;
    }
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(ms)) => {}
        _ = signal.cancelled() => {}
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum QueryEnvelopeProducer {
    HookAdditionalContext,
    MemoryFreshness,
    LoopReminder,
    TokenBudgetContinuation,
    EditedFilesDiff,
    StructuredOutputRetry,
    StopHookContinuation,
    TruncationContinuation,
    ReasoningOverLimit,
    EmptyClosingRetry,
}

/// One testable seam for Query-owned envelope producers.
pub(super) fn render_query_envelope_producer(
    producer: QueryEnvelopeProducer,
    text: RuntimeString,
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    match producer {
        QueryEnvelopeProducer::EditedFilesDiff => render_provider_text_envelope(
            ProviderTextEnvelopeKind::DiffOutput,
            text,
            profile,
            crate::provider::behavior::ProviderTextEnvelopeOptions {
                tool_name: Some("edited_text_file_diff".into()),
            },
        ),
        QueryEnvelopeProducer::HookAdditionalContext
        | QueryEnvelopeProducer::MemoryFreshness
        | QueryEnvelopeProducer::LoopReminder
        | QueryEnvelopeProducer::TokenBudgetContinuation
        | QueryEnvelopeProducer::StructuredOutputRetry
        | QueryEnvelopeProducer::StopHookContinuation
        | QueryEnvelopeProducer::TruncationContinuation
        | QueryEnvelopeProducer::ReasoningOverLimit
        | QueryEnvelopeProducer::EmptyClosingRetry => render_provider_text_envelope(
            ProviderTextEnvelopeKind::SystemReminder,
            text,
            profile,
            Default::default(),
        ),
    }
}

pub(super) fn render_token_budget_continuation(
    pct: i64,
    total_output_tokens: u64,
    token_budget: u64,
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    let text = format!(
        "You are at ~{pct}% of the token target for this task ({total_output_tokens} of {token_budget} output tokens). Continue productive work toward the full requested outcome while work remains. The target is a minimum for productive work on the requested task, not a reason to pad the answer or invent unrequested work. If the task is complete, report the outcome with the detail the request requires."
    );
    render_query_envelope_producer(
        QueryEnvelopeProducer::TokenBudgetContinuation,
        text.into(),
        profile,
    )
}

pub(super) fn render_structured_output_retry(profile: &ProviderBehaviorProfile) -> RuntimeString {
    render_query_envelope_producer(
        QueryEnvelopeProducer::StructuredOutputRetry,
        "Your previous response was not valid JSON. Respond again with ONLY a single valid JSON value matching the required schema — no prose, no code fences.".into(),
        profile,
    )
}

pub(super) fn render_stop_hook_continuation(
    text: RuntimeString,
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    render_query_envelope_producer(QueryEnvelopeProducer::StopHookContinuation, text, profile)
}

/// The continuation nudge for a response cut off WITH visible text on screen:
/// resume and finish, and re-open any code fence or table header the cut left
/// unclosed so the continued text stays well-formed.
pub(super) fn render_truncation_continuation(profile: &ProviderBehaviorProfile) -> RuntimeString {
    render_query_envelope_producer(
        QueryEnvelopeProducer::TruncationContinuation,
        "Your response was cut off because it exceeded the output token limit. Continue from where you left off and finish the response. Start a new code fence or table header first if you were inside one.".into(),
        profile,
    )
}

/// The reminder for a response cut off having produced NO visible text: the
/// whole budget went to reasoning the user never sees.
///
/// Injected as a reminder rather than as a bare instruction so the model treats
/// it as harness guidance about its own output shape. The wording states that
/// the lost reasoning is NOT part of the conversation, so the model resumes
/// from the visible context instead of trying to continue invisible thoughts.
pub(super) fn render_reasoning_over_limit_reminder(
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    render_query_envelope_producer(
        QueryEnvelopeProducer::ReasoningOverLimit,
        "Output token limit hit while you were still reasoning, before this response produced any text or tool call. That reasoning was not kept and is not part of this conversation, so there is nothing to resume. Continue from the conversation above: reason much more briefly and produce your answer or next tool call well within the output limit.".into(),
        profile,
    )
}

/// The nudge for a closing response that came back EMPTY but stopped normally:
/// the turn already has visible work on screen, so what is missing is the
/// closing answer, not the work itself. The wording tells the model to either
/// close the task in prose or keep working with tools — a bare "say something"
/// would invite a one-word filler instead of a real closing.
///
/// When `has_prior_work` is false the turn has produced nothing visible yet,
/// so the reminder asks for a direct answer rather than a closing summary.
pub(super) fn render_empty_closing_retry(
    profile: &ProviderBehaviorProfile,
    has_prior_work: bool,
) -> RuntimeString {
    render_query_envelope_producer(
        QueryEnvelopeProducer::EmptyClosingRetry,
        if has_prior_work {
            "Your previous response was empty. The work done so far is visible to the user, but this turn has no closing answer yet. Either write the closing summary of what is done and what remains, or continue with tool calls — do not respond empty again.".into()
        } else {
            "Your previous response contained only internal reasoning and no visible answer. Please respond directly to the user — do not respond empty again.".into()
        },
        profile,
    )
}

/// A tool call whose arguments were cut off mid-JSON, retried with a LARGER
/// budget. The point of naming the tool and forbidding a shorter output is that
/// the call was truncated by size, so shrinking it would silently lose content.
///
/// Plain text, not envelope-wrapped: it rides on the dropped call's error result
/// as `extra_context`, which the result builder already wraps in a system
/// reminder. Wrapping here too would nest a reminder inside a reminder.
pub(super) fn tool_over_limit_raise_text(tool_name: &RuntimeString) -> RuntimeString {
    RuntimeString::from(format!(
        "The single-turn model output has exceeded the token limit. In the next request, I will increase the token limit. Must make the {tool} tool call again and ensure to utilize the previous output to accelerate the next model's output. Must not to reduce the output content.",
        tool = String::from_utf8_lossy(&tool_name.to_node_utf8()),
    ))
}

/// Same situation, budget already raised and still not enough: ask for half the
/// output instead. The model is told to explain first so the user learns why the
/// work is being split. Plain text for the same reason as above.
pub(super) fn tool_over_limit_halve_text() -> RuntimeString {
    RuntimeString::from(
        "The single-turn model output has reached the token limit. Please explain the issue, then use the tool again and reduce the output content by half. For file writes, split the document across smaller calls without omitting content; append later chunks with append: true and continuation_context matching the saved file tail.",
    )
}

/// The footnote written into the assistant's own text when the continuation
/// ladder is exhausted.
///
/// It is part of the message body, not a side-channel notice: the body is what
/// gets persisted and replayed, so a reader returning to this conversation later
/// still learns the answer was cut short rather than seeing a sentence that
/// simply stops.
pub(crate) fn truncation_footnote(attempts: u32) -> RuntimeString {
    RuntimeString::from(format!(
        "\n\n---\n*Output was truncated due to token limit. Automatic recovery was attempted {attempts} times.*"
    ))
}

pub(super) fn provider_meta_nudge(shared: &Shared, rendered_text: RuntimeString) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: (shared.deps.uuid)().into(),
        is_meta: Some(true),
        content: vec![ContentBlock::Text(TextBlock {
            text: rendered_text,
            citations: None,
            harness_envelope: Some(HarnessEnvelope::SystemReminder),
            message_id: None,
            phase: None,
        })],
    })
}

// ---------------------------------------------------------------------------
// Request assembly + tool-spec visibility (pure, testable).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(super) struct PreparedModelRequest {
    pub(super) request: ModelRequest,
    pub(super) profile: ProviderBehaviorProfile,
}

pub(super) struct RequestInference<'a> {
    pub config: &'a crate::schemas::config::ResolvedConfig,
    pub provider_info: Option<&'a ProviderInfo>,
    pub thinking: Option<ThinkingConfig>,
    pub model_messages: Option<&'a crate::runtime_values::RuntimeJson>,
}

pub(super) fn render_query_owned_system_parts(
    parts: &[SystemPromptPart],
    profile: &ProviderBehaviorProfile,
) -> Vec<SystemPromptPart> {
    parts
        .iter()
        .map(|part| {
            let producer = match () {
                _ if part.name == "hook-context" => {
                    Some(QueryEnvelopeProducer::HookAdditionalContext)
                }
                _ if part.name == "reminder" || part.name == "task-plan-reminder" => {
                    Some(QueryEnvelopeProducer::MemoryFreshness)
                }
                _ if part.name == "edited-files" => Some(QueryEnvelopeProducer::EditedFilesDiff),
                _ if part.name == "loop-recovery" => Some(QueryEnvelopeProducer::LoopReminder),
                _ => None,
            };
            let Some(producer) = producer else {
                return part.clone();
            };
            let mut rendered = part.clone();
            rendered.text = render_query_envelope_producer(producer, part.text.clone(), profile);
            rendered
        })
        .collect()
}

/// Assemble live policy and provider envelopes through the same path for
/// conversational requests and explicitly requested summaries.
#[allow(clippy::too_many_arguments)]
pub(crate) fn finalize_instruction_system(
    mut instruction_system: Vec<SystemPromptPart>,
    permission_context: &crate::types::tool::ToolContext,
    approvals_available: bool,
    ultra_mode: bool,
    multi_agent_instructions: bool,
    tool_specs: &[ToolSpec],
    agent_id: Option<&RuntimeString>,
    profile: &ProviderBehaviorProfile,
) -> Vec<SystemPromptPart> {
    let permission_index = instruction_system
        .iter()
        .position(|part| part.name == "total-tokens")
        .unwrap_or(instruction_system.len());
    instruction_system.insert(
        permission_index,
        crate::context::instruction_policy::permission_instructions(
            permission_context,
            approvals_available,
        ),
    );
    let current_system = super::orchestration::request_policy_with_model_messages(
        &instruction_system,
        ultra_mode,
        tool_specs,
        agent_id,
        multi_agent_instructions,
        None,
        None,
    );
    render_system_prompt_for_behavior(
        &render_query_owned_system_parts(&current_system, profile),
        profile,
    )
}

pub(super) fn prepare_model_request(
    messages: &[Message],
    params: &QueryParams,
    canonical_system_prompt: &[SystemPromptPart],
    max_output_tokens: u64,
    internal_visible_tool_specs: &[ToolSpec],
    turn_override: Option<&TurnOverride>,
) -> PreparedModelRequest {
    prepare_with_inference(
        messages,
        params,
        RequestInference {
            config: &params.config,
            provider_info: params.provider_info.as_ref(),
            thinking: params.thinking,
            model_messages: None,
        },
        canonical_system_prompt,
        max_output_tokens,
        internal_visible_tool_specs,
        turn_override,
    )
}

pub(super) fn prepare_with_inference(
    messages: &[Message],
    params: &QueryParams,
    inference: RequestInference<'_>,
    canonical_system_prompt: &[SystemPromptPart],
    max_output_tokens: u64,
    internal_visible_tool_specs: &[ToolSpec],
    turn_override: Option<&TurnOverride>,
) -> PreparedModelRequest {
    let RequestInference {
        config,
        provider_info,
        thinking,
        model_messages,
    } = inference;
    let model = turn_override
        .map(|turn_override| turn_override.model.clone())
        .unwrap_or_else(|| params.model.clone());
    let profile = resolve_provider_behavior_profile(provider_info, &model);
    let effort = match turn_override {
        Some(turn_override) => turn_override.effort.clone(),
        None => params.effort.clone(),
    };
    // Raw Ultra activates the default collaboration policy.
    let ultra_mode = match turn_override {
        Some(turn_override) => turn_override.ultra_mode,
        None => params.ultra_mode,
    };
    let effort = super::orchestration::resolve_effort(effort, &Default::default());
    let (instruction_system, mut request_messages) =
        crate::context::instruction_policy::project_workspace_instructions(
            canonical_system_prompt,
            messages,
        );
    let permission_context = (params.tool_context_base)();
    let system = finalize_instruction_system(
        instruction_system,
        &permission_context,
        params.approval_handler.is_some(),
        ultra_mode,
        params.multi_agent_instructions,
        internal_visible_tool_specs,
        params.agent_id.as_ref(),
        &profile,
    );
    let system = super::orchestration::request_policy_with_model_messages(
        &system,
        ultra_mode,
        internal_visible_tool_specs,
        params.agent_id.as_ref(),
        params.multi_agent_instructions,
        model_messages,
        config.multi_agent_mode_hint_text.as_ref(),
    );
    let tools = render_tool_specs_for_behavior(internal_visible_tool_specs, &profile);
    let tool_choice = (!tools.is_empty()).then_some(ToolChoice::Auto);
    if params.agent_id.is_none() {
        if let Some(state) = &params.context_hint_state {
            state.apply_frozen(&model, &mut request_messages);
        }
    }
    let context_hints = if config.context_hints && params.agent_id.is_none() {
        params.context_hint_state.as_ref().map(|state| {
            crate::context::context_hints::ContextHintRequest {
                enabled: true,
                main_thread: true,
                artifact_dir: Some(crate::runtime_process::path_join(
                    super::query_path_dialect(),
                    &[
                        permission_context.cwd.clone(),
                        crate::runtime_values::RuntimePath::from(".klaus"),
                        crate::runtime_values::RuntimePath::from("tool-results"),
                    ],
                )),
                cwd: permission_context.cwd.clone(),
                read_state: params.file_state.clone(),
                state: state.clone(),
            }
        })
    } else {
        None
    };
    let isolate_gemini_session = crate::context::runtime_reminders::is_antigravity_source(config)
        && profile.family == Some("gemini");
    let (cache_key, cache_domain) = if isolate_gemini_session {
        (
            (!params.thread_id.is_empty())
                .then(|| params.thread_id.clone())
                .or_else(|| params.cache_key.clone()),
            None,
        )
    } else {
        (params.cache_key.clone(), params.cache_domain.clone())
    };
    let mut request = ModelRequest {
        model,
        image_limits: None,
        max_media_blocks: None,
        supports_image_detail_original: false,
        system,
        messages: crate::engine::resume_repair::normalize_tool_history_owned(request_messages),
        tools,
        // Native deferred-loading lists are bound per-request inside
        // `Shared::prepare`, which owns the exposure/surfaced state.
        native_deferred_tools: None,
        deferred_tools: Vec::new(),
        surfaced_deferred_names: Vec::new(),
        wire_caps: None,
        frozen_tools: None,
        frozen_instructions: None,
        tool_inventory_events: Vec::new(),
        tool_choice,
        max_output_tokens,
        task_budget: config.task_budget_tokens,
        output_schema: params.output_schema.clone(),
        web_search: params.web_search.clone(),
        thinking,
        effort,
        model_verbosity: config.model_verbosity,
        reasoning_summary: config.reasoning_summary,
        skip_cache_write: if params.skip_cache_write.unwrap_or(false) {
            Some(true)
        } else {
            None
        },
        cache_ttl: params.cache_ttl,
        cache_key,
        cache_domain,
        context_management: adaptive_context_management(
            config.context_management.as_ref(),
            params.context_edit_state.as_deref(),
        ),
        context_hints,
        thinking_context: resolve_thinking_context(
            config.manage_thinking_context,
            thinking,
            params.thinking_context_cold,
        ),
    };
    crate::context::runtime_reminders::project_for_source(&mut request, config, provider_info);
    PreparedModelRequest { request, profile }
}

/// Preserve prior-turn reasoning whenever thinking is active. While the prompt
/// cache can still be warm every thinking turn is pinned: shedding one would
/// rewrite a prefix that is otherwise served from cache. Once the session has
/// been idle past the longest cache lifetime (`cold`), the prefix has to be
/// rewritten at full price anyway, so aged thinking turns are dropped down to
/// the most recent one — pure weight saved on a rewrite that cannot be avoided.
fn resolve_thinking_context(
    manage: bool,
    thinking: Option<ThinkingConfig>,
    cold: bool,
) -> Option<ThinkingContextPolicy> {
    if !manage || thinking.is_none() {
        return None;
    }
    Some(if cold {
        ThinkingContextPolicy::KeepMostRecentTurn
    } else {
        ThinkingContextPolicy::KeepAll
    })
}

/// Raise a count-based clearing trigger by the tool uses the server has already
/// cleared for this session. The client resends its full unrewritten history on
/// every request, so a fixed count trigger would re-clear on every request once
/// crossed; anchoring at the cleared total makes the next clear wait for a fresh
/// batch of roughly `trigger_tool_uses` tool calls, amortizing the cache rewrite
/// each clear costs. Token-based triggers are passed through as configured.
fn adaptive_context_management(
    config: Option<&crate::schemas::config::ContextManagementConfig>,
    state: Option<&crate::context::context_editing::ContextEditState>,
) -> Option<crate::schemas::config::ContextManagementConfig> {
    let config = config?.clone();
    let (Some(base), Some(state)) = (config.trigger_tool_uses, state) else {
        return Some(config);
    };
    let cleared = state.cleared_tool_uses();
    if cleared == 0 {
        return Some(config);
    }
    Some(crate::schemas::config::ContextManagementConfig {
        trigger_tool_uses: Some(base + cleared),
        ..config
    })
}

impl Shared {
    pub(super) fn prepare(
        &self,
        messages: &[Message],
        canonical_system_prompt: &[SystemPromptPart],
        max_output_tokens: u64,
        internal_visible_tool_specs: &[ToolSpec],
        turn_override: Option<&TurnOverride>,
    ) -> PreparedModelRequest {
        self.prepare_inner(
            messages,
            canonical_system_prompt,
            max_output_tokens,
            internal_visible_tool_specs,
            turn_override,
            true,
        )
    }

    pub(super) fn prepare_without_tool_freeze(
        &self,
        messages: &[Message],
        canonical_system_prompt: &[SystemPromptPart],
        max_output_tokens: u64,
        internal_visible_tool_specs: &[ToolSpec],
        turn_override: Option<&TurnOverride>,
    ) -> PreparedModelRequest {
        self.prepare_inner(
            messages,
            canonical_system_prompt,
            max_output_tokens,
            internal_visible_tool_specs,
            turn_override,
            false,
        )
    }

    fn prepare_inner(
        &self,
        messages: &[Message],
        canonical_system_prompt: &[SystemPromptPart],
        max_output_tokens: u64,
        internal_visible_tool_specs: &[ToolSpec],
        turn_override: Option<&TurnOverride>,
        update_tool_freeze: bool,
    ) -> PreparedModelRequest {
        let Some(bundle) = self.bundle() else {
            let mut prepared = prepare_model_request(
                messages,
                &self.params,
                canonical_system_prompt,
                max_output_tokens,
                internal_visible_tool_specs,
                turn_override,
            );
            if update_tool_freeze {
                if let Some(specs) = self.defer_oversized_inventory(&prepared) {
                    return self.prepare_inner(
                        messages,
                        canonical_system_prompt,
                        max_output_tokens,
                        &specs,
                        turn_override,
                        update_tool_freeze,
                    );
                }
                prepared.request.deferred_tools = self.deferred_request_specs(&prepared.profile);
                prepared.request.surfaced_deferred_names =
                    self.surfaced_deferred_request_names(messages, &prepared.profile);
            }
            self.observe_approval_context(
                messages,
                canonical_system_prompt,
                &prepared.request,
                None,
            );
            self.freeze_subagent_context(canonical_system_prompt);
            return prepared;
        };
        let turn_override = turn_override.cloned().unwrap_or_else(|| {
            let context = self.turn_context.lock_recover();
            TurnOverride {
                model: context.model.clone(),
                effort: context.effort.clone(),
                ultra_mode: context.ultra_mode,
            }
        });
        *self.step_model.lock_recover() = turn_override.model.clone();
        let config = self.config();
        let thinking = if self
            .inference
            .as_ref()
            .is_some_and(|capture| capture.next.is_some())
        {
            config.thinking.then_some(ThinkingConfig::Adaptive)
        } else {
            self.params.thinking
        };
        let capabilities = bundle.provider.capabilities(&turn_override.model);
        let metadata = bundle.provider.model_metadata(&turn_override.model);
        let mut prepared = prepare_with_inference(
            messages,
            &self.params,
            RequestInference {
                config: &config,
                provider_info: Some(bundle.provider.info()),
                thinking,
                model_messages: metadata.model_messages.as_ref(),
            },
            canonical_system_prompt,
            max_output_tokens,
            internal_visible_tool_specs,
            Some(&turn_override),
        );
        prepared.request.effort =
            super::orchestration::resolve_effort(turn_override.effort.clone(), &metadata);
        if turn_override
            .effort
            .as_ref()
            .is_some_and(crate::schemas::effort::EffortValue::is_ultra)
            && capabilities.thinking
        {
            prepared.request.thinking = Some(ThinkingConfig::Adaptive);
            prepared.request.thinking_context = resolve_thinking_context(
                config.manage_thinking_context,
                prepared.request.thinking,
                self.params.thinking_context_cold,
            );
        }
        if self
            .inference
            .as_ref()
            .is_some_and(|capture| capture.next.is_some())
        {
            if self
                .params
                .agent_tool_policy
                .as_ref()
                .is_none_or(|policy| policy.allows_tool(&RuntimeString::from("web_search")))
            {
                prepared.request.web_search = crate::engine::web_search_select::select_web_search(
                    Vec::new(),
                    Some(&config.web_search),
                    capabilities.web_search,
                )
                .request;
            } else {
                prepared.request.web_search = None;
            }
        } else if let Some(search) = prepared.request.web_search.as_mut() {
            search.access = config.web_search.access;
            if search.access == crate::schemas::config::WebSearchAccess::Disabled {
                prepared.request.web_search = None;
            }
        }
        prepared.request.bind_media_capabilities(&capabilities);
        prepared.request.native_deferred_tools = prepared
            .request
            .native_deferred_tools
            .or(metadata.native_deferred_tools);
        prepared.request.wire_caps.get_or_insert_with(|| {
            crate::types::provider::WireCapabilityPolicy::from_capabilities(&metadata)
        });
        prepared.request.max_output_tokens = prepared
            .request
            .max_output_tokens
            .min(capabilities.max_output_tokens);
        if !capabilities.tools {
            prepared.request.tools.clear();
            prepared.request.tool_choice = None;
        }
        if !capabilities.vision {
            omit_request_images(&mut prepared.request.messages);
        }
        if !capabilities.native_pdf {
            omit_historical_request_documents(
                &mut prepared.request.messages,
                self.params.input_pdf_documents > 0,
            );
        }
        crate::provider::native_media::project_history_media(
            &mut prepared.request.messages,
            messages,
            &metadata.media_input_mime_types,
        );
        super::tool_dispatch::gate_request_images(&mut prepared.request);
        if update_tool_freeze {
            if let Some(specs) = self.defer_oversized_inventory(&prepared) {
                return self.prepare_inner(
                    messages,
                    canonical_system_prompt,
                    max_output_tokens,
                    &specs,
                    Some(&turn_override),
                    update_tool_freeze,
                );
            }
            if capabilities.tools {
                prepared.request.deferred_tools = self.deferred_request_specs(&prepared.profile);
                prepared.request.surfaced_deferred_names =
                    self.surfaced_deferred_request_names(messages, &prepared.profile);
            }
            self.attach_frozen_tool_projection(&mut prepared.request, messages);
        }
        self.observe_approval_context(
            messages,
            canonical_system_prompt,
            &prepared.request,
            Some(bundle.clone()),
        );
        self.freeze_subagent_context(canonical_system_prompt);
        prepared
    }

    fn freeze_subagent_context(&self, canonical: &[SystemPromptPart]) {
        let tools = self.tools.lock_recover().as_ref().clone();
        let context = (self.params.tool_context_base)();
        let clients = context
            .mcp_clients
            .map(|clients| clients())
            .unwrap_or_default();
        let mcp = crate::engine::session::SubagentMcpCapture::capture(&tools, &clients);
        *self.step_subagent_context.lock_recover() = Some(std::sync::Arc::new(
            crate::types::tool::InheritedSubagentContext {
                system_prompt: canonical.to_vec(),
                tools,
                mcp,
            },
        ));
    }

    fn input_window(&self, request: &ModelRequest) -> u64 {
        let configured = self.config().context_window;
        let window = self.bundle().map_or(configured, |bundle| {
            configured.min(bundle.provider.capabilities(&request.model).context_window)
        });
        window.saturating_sub(request.max_output_tokens)
    }

    pub(super) fn uses_static_deferred_tool_dispatch(&self) -> bool {
        let config = self.config();
        let model = self.turn_context.lock_recover().model.clone();
        let tools = self.tools.lock_recover().clone();
        let tool_search_allowed = self
            .params
            .agent_tool_policy
            .as_ref()
            .is_none_or(|policy| policy.allows_tool(&RuntimeString::from("tool_search")));
        let native_active = self
            .bundle()
            .is_some_and(|bundle| bundle.provider.native_deferred_loading_active(&model));
        static_deferred_dispatch_active(&config, &tools, tool_search_allowed, native_active)
    }

    pub(super) fn visible_tool_specs_for_step(&self) -> Vec<ToolSpec> {
        let tools = self.tools.lock_recover().clone();
        if self.uses_static_deferred_tool_dispatch() {
            return static_deferred_visible_tool_specs(&tools, &self.config().disabled_tools);
        }
        let precomp = self.tool_specs.lock_recover().clone();
        let surfaced = self.surfaced.lock_recover();
        visible_tool_specs(&tools, &surfaced, &precomp)
    }

    /// Deferred-exposure specs that have not been surfaced yet, rendered for
    /// the request's provider profile. Providers with native deferred-loading
    /// wire support send these alongside the immediate tools; every other
    /// provider ignores the field.
    pub(super) fn deferred_request_specs(
        &self,
        profile: &ProviderBehaviorProfile,
    ) -> Vec<ToolSpec> {
        if self.uses_static_deferred_tool_dispatch() {
            return Vec::new();
        }
        let tools = self.tools.lock_recover().clone();
        let surfaced = self.surfaced.lock_recover();
        let internal = deferred_tool_specs(&tools, &surfaced);
        drop(surfaced);
        if internal.is_empty() {
            return Vec::new();
        }
        render_tool_specs_for_behavior(&internal, profile)
    }

    /// Names of already-surfaced deferred tools rendered for the request's
    /// provider profile. When the history contains a compaction boundary, only
    /// tools surfaced after the last boundary still have search references on
    /// the wire; earlier surfaced tools stay as direct definitions instead of
    /// orphaned `defer_loading: true` entries.
    pub(super) fn surfaced_deferred_request_names(
        &self,
        messages: &[Message],
        profile: &ProviderBehaviorProfile,
    ) -> Vec<RuntimeString> {
        if self.uses_static_deferred_tool_dispatch() {
            return Vec::new();
        }
        let tools = self.tools.lock_recover().clone();
        let surfaced = self.surfaced.lock_recover().clone();
        if surfaced.is_empty() {
            return Vec::new();
        }
        let active_names: HashSet<RuntimeString> = match messages
            .iter()
            .rposition(|message| matches!(message, Message::CompactBoundary(_)))
        {
            Some(boundary_index) => {
                let post_boundary = surfaced_tools_from_message_results(
                    &messages[boundary_index + 1..],
                    profile,
                    Some(&tools),
                );
                surfaced.intersection(&post_boundary).cloned().collect()
            }
            None => surfaced,
        };
        if active_names.is_empty() {
            return Vec::new();
        }
        let mut out: Vec<RuntimeString> = Vec::new();
        let mut seen_internal: HashSet<RuntimeString> = HashSet::new();
        for tool in tools.iter() {
            let name = tool.name();
            if tool.exposure() == crate::types::tool::ToolExposure::Deferred
                && active_names.contains(&name)
            {
                seen_internal.insert(name.clone());
                out.push(crate::provider::behavior::render_tool_name(&name, profile));
            }
        }
        let present_names: HashSet<RuntimeString> = tools.iter().map(|tool| tool.name()).collect();
        let mut disconnected: Vec<RuntimeString> = active_names
            .into_iter()
            .filter(|name| !seen_internal.contains(name) && !present_names.contains(name))
            .collect();
        disconnected.sort();
        for name in disconnected {
            out.push(crate::provider::behavior::render_tool_name(&name, profile));
        }
        out
    }

    /// Maintain the frozen tool-inventory epoch for transports that announce
    /// inventory changes in the message stream, and attach the projection to
    /// the request. The baseline is re-captured whenever the persisted history
    /// stops extending it (compaction, migration, restore), the model changes,
    /// the static-fallback mode flips, or the oversized-inventory latch flips;
    /// recorded events are byte-frozen so replays stay identical within an
    /// epoch.
    pub(super) fn attach_frozen_tool_projection(
        &self,
        request: &mut ModelRequest,
        messages: &[Message],
    ) {
        if !self.inline_tool_change_projection_capable(&request.model)
            || (request.tools.is_empty() && request.deferred_tools.is_empty())
        {
            return;
        }
        let oversized = self
            .oversized_inventory_deferred
            .load(std::sync::atomic::Ordering::Acquire);
        let static_deferred_dispatch = self.uses_static_deferred_tool_dispatch();
        let compact_boundaries = messages
            .iter()
            .filter(|message| matches!(message, Message::CompactBoundary(_)))
            .count();
        let mut state = self.tool_freeze.lock_recover();
        let need_rebase = match state.as_ref() {
            None => true,
            Some(epoch) => {
                epoch.model != request.model
                    || epoch.oversized != oversized
                    || epoch.static_deferred_dispatch != static_deferred_dispatch
                    || epoch.compact_boundaries != compact_boundaries
                    || (epoch.prefix_len == 0 && !messages.is_empty())
                    || messages.len() < epoch.prefix_len
                    || super::precompute::history_hash(&messages[..epoch.prefix_len])
                        != Some(epoch.prefix_hash)
            }
        };
        if need_rebase {
            let Some(prefix_hash) = super::precompute::history_hash(messages) else {
                return;
            };
            *state = Some(super::ToolFreezeEpoch {
                model: request.model.clone(),
                specs: request.tools.clone(),
                prefix_len: messages.len(),
                prefix_hash,
                compact_boundaries,
                static_deferred_dispatch,
                oversized,
                events: Vec::new(),
                instructions: Some(crate::types::provider::stable_system_text(&request.system)),
            });
        }
        let epoch = state.as_mut().expect("epoch just established");
        let expected = fold_tool_inventory(&epoch.specs, &epoch.events);
        if let Some((added, removed)) = tool_inventory_diff(&expected, &request.tools) {
            epoch
                .events
                .push(crate::types::provider::ToolInventoryEvent {
                    anchor_uuid: messages.last().and_then(message_uuid),
                    added,
                    removed,
                });
        }
        request.frozen_tools = Some(epoch.specs.clone());
        request.tool_inventory_events = epoch.events.clone();
        request.frozen_instructions = epoch.instructions.clone();
    }

    /// Whether the current model could ride an in-place tool-change transport
    /// (Anthropic inline blocks or Responses cumulative batches). Resolution
    /// order: catalog-merged metadata, then the offline compatibility list.
    fn inline_tool_change_projection_capable(&self, model: &RuntimeString) -> bool {
        let Some(bundle) = self.bundle() else {
            return false;
        };
        let Ok(provider_name) = bundle.provider.info().name.try_to_utf8() else {
            return false;
        };
        let metadata = bundle.provider.model_metadata(model);
        let sys = metadata.mid_convo_system_messages.unwrap_or_else(|| {
            crate::provider::model_catalog::mid_convo_system_messages_default(&provider_name, model)
        });
        let changes = metadata.mid_convo_tool_changes.unwrap_or_else(|| {
            crate::provider::model_catalog::mid_convo_tool_changes_default(&provider_name, model)
        }) || metadata.additional_tools.unwrap_or_else(|| {
            crate::provider::model_catalog::additional_tools_default(&provider_name, model)
        });
        sys && changes
    }

    /// Budget the rendered prefix, rather than a tool count or a model-name
    /// heuristic. Activated deferred tools retain their declarations; everything
    /// else remains available to the existing tool-search activation path.
    fn defer_oversized_inventory(&self, prepared: &PreparedModelRequest) -> Option<Vec<ToolSpec>> {
        let request = &prepared.request;
        let bundle = self.bundle()?;
        if !bundle.provider.has_model_capabilities(&request.model)
            || !bundle.provider.uses_model_tool_budget(&request.model)
            || request.tools.is_empty()
        {
            return None;
        }
        if fixed_prefix_tokens(request) <= self.input_window(request) {
            return None;
        }
        let config = self.config();
        if config
            .disabled_tools
            .contains(&RuntimeString::from("tool_search"))
            || self
                .params
                .agent_tool_policy
                .as_ref()
                .is_some_and(|policy| !policy.allows_tool(&RuntimeString::from("tool_search")))
        {
            return None;
        }
        let tools = self.tools.lock_recover().clone();
        let deferred =
            crate::tools::builtins::tool_search::defer_direct_inventory(tools.as_ref().clone());
        let specs = if self.uses_static_deferred_tool_dispatch() {
            static_deferred_visible_tool_specs(&deferred, &config.disabled_tools)
        } else {
            let all_specs = deferred
                .iter()
                .map(|tool| to_tool_spec(tool.as_ref()))
                .collect::<Vec<_>>();
            let surfaced = self.surfaced.lock_recover();
            visible_tool_specs(&deferred, &surfaced, &all_specs)
        };
        if render_tool_specs_for_behavior(&specs, &prepared.profile) == request.tools {
            return None;
        }
        self.oversized_inventory_deferred
            .store(true, std::sync::atomic::Ordering::Release);
        *self.tools.lock_recover() = Arc::new(deferred);
        Some(specs)
    }

    pub(super) fn observe_fixed_prefix_pressure(&self, request: &ModelRequest) {
        // An uncaptured model has no numeric window. Let the existing provider
        // admission path report its binding error before any transport is called.
        if self
            .bundle()
            .is_some_and(|bundle| !bundle.provider.has_model_capabilities(&request.model))
        {
            return;
        }
        let estimate = fixed_prefix_tokens(request);
        let limit = self.input_window(request);
        if estimate <= limit {
            return;
        }
        // This is a heuristic. Keep available context recovery and the real
        // provider admission path reachable rather than rejecting on an estimate.
        crate::observability::provider_diagnostics::record_named(
            crate::observability::provider_diagnostics::TIMING_LOG_FILE_NAME,
            "fixed_prefix_pressure",
            serde_json::json!({
                "sessionId": self.params.thread_id.try_to_utf8().ok(),
                "model": request.model.try_to_utf8().ok(),
                "estimatedTokens": estimate,
                "inputLimitTokens": limit,
                "recoveryAvailable": true,
            }),
        );
    }

    fn observe_approval_context(
        &self,
        messages: &[Message],
        canonical_system: &[SystemPromptPart],
        request: &ModelRequest,
        bundle: Option<std::sync::Arc<crate::engine::session::InferenceBundle>>,
    ) {
        if let Some(reviewer) = &self.params.approval_reviewer {
            let queue = self.q.clone();
            reviewer.observe_context(crate::types::permission::ApprovalReviewContext {
                turn_id: self.params.turn_id.clone(),
                agent_id: self.params.agent_id.clone(),
                request: request.clone(),
                canonical_system: canonical_system.to_vec(),
                history: messages.to_vec(),
                config: self.config(),
                provider: bundle.as_ref().map(|bundle| bundle.provider.clone()),
                accounting: bundle,
                emit: std::sync::Arc::new(move |event| queue.push(event)),
            });
        }
    }
}

/// Count the fully rendered request without charging projected instructions twice.
pub(super) fn rendered_context_tokens(request: &ModelRequest) -> u64 {
    crate::context::tokens::estimate_context_tokens_with_prefix(
        &request.messages,
        crate::context::tokens::estimate_prefix_tokens(&request.system, &request.tools),
    )
}

/// Workspace instructions are deliberately projected into request-local user
/// messages. Their role does not make them removable conversation history.
pub(super) fn fixed_prefix_tokens(request: &ModelRequest) -> u64 {
    let instructions = request
        .messages
        .iter()
        .filter(|message| {
            crate::context::instruction_policy::is_workspace_instruction_projection(message)
        })
        .map(crate::context::tokens::estimate_message_tokens)
        .sum::<u64>();
    crate::context::tokens::estimate_prefix_tokens(&request.system, &request.tools)
        .saturating_add(instructions)
}

/// Project capability omissions only into the request, never rewrite history.
/// Reuse the session's omission wording so historical and fresh images follow
/// the same contract, including tool-result and extra-user image parts.
pub(crate) fn omit_request_images(messages: &mut [Message]) {
    for message in messages {
        match message {
            Message::User(user) => omit_content_images(&mut user.content),
            Message::Assistant(assistant) => omit_content_images(&mut assistant.content),
            Message::ToolResult(result) => {
                let dropped = result
                    .content
                    .iter()
                    .filter(|block| matches!(block, OutputBlock::Image(_)))
                    .count();
                if let Some(note) = image_omission_note(dropped) {
                    result
                        .content
                        .retain(|block| !matches!(block, OutputBlock::Image(_)));
                    result.content.push(OutputBlock::Text(note));
                }
                if let Some(blocks) = &mut result.extra_user_blocks {
                    omit_content_images(blocks);
                }
            }
            _ => {}
        }
    }
}

fn omit_content_images(blocks: &mut Vec<ContentBlock>) {
    for block in blocks.iter_mut() {
        if let ContentBlock::ToolResult(result) = block {
            let dropped = result
                .content
                .iter()
                .filter(|part| matches!(part, OutputBlock::Image(_)))
                .count();
            if let Some(note) = image_omission_note(dropped) {
                result
                    .content
                    .retain(|part| !matches!(part, OutputBlock::Image(_)));
                result.content.push(OutputBlock::Text(note));
            }
        }
    }
    let dropped = blocks
        .iter()
        .filter(|block| matches!(block, ContentBlock::Image(_)))
        .count();
    if let Some(note) = image_omission_note(dropped) {
        blocks.retain(|block| !matches!(block, ContentBlock::Image(_)));
        blocks.push(ContentBlock::Text(note));
    }
}

/// Omit historical native PDF blocks when the selected model cannot read PDFs,
/// while preserving any fresh PDF attachment on the current user turn so
/// explicit input validation still surfaces a clear rejection.
pub(crate) fn omit_historical_request_documents(
    messages: &mut [Message],
    preserve_latest_user_documents: bool,
) {
    let protected_user = if preserve_latest_user_documents {
        messages.iter().rposition(|message| {
            matches!(
                message,
                Message::User(user)
                    if user.is_meta != Some(true)
                        && user
                            .content
                            .iter()
                            .any(|block| matches!(block, ContentBlock::Document(_)))
            )
        })
    } else {
        None
    };
    for (index, message) in messages.iter_mut().enumerate() {
        if Some(index) == protected_user {
            continue;
        }
        match message {
            Message::User(user) => omit_content_documents(&mut user.content),
            Message::Assistant(assistant) => omit_content_documents(&mut assistant.content),
            Message::ToolResult(result) => {
                if let Some(blocks) = &mut result.extra_user_blocks {
                    omit_content_documents(blocks);
                }
            }
            _ => {}
        }
    }
}

fn omit_content_documents(blocks: &mut Vec<ContentBlock>) {
    let mut dropped = 0usize;
    for block in blocks.iter() {
        if matches!(block, ContentBlock::Document(_)) {
            dropped += 1;
        }
    }
    if dropped == 0 {
        return;
    }
    blocks.retain(|block| !matches!(block, ContentBlock::Document(_)));
    let note = if dropped == 1 {
        "[1 PDF document was omitted from this model request: this model cannot read PDF attachments directly. The attachment remains in the conversation.]"
    } else {
        "[PDF documents were omitted from this model request: this model cannot read PDF attachments directly. The attachments remain in the conversation.]"
    };
    blocks.push(ContentBlock::Text(TextBlock {
        text: RuntimeString::from(note),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }));
}

fn image_omission_note(dropped: usize) -> Option<TextBlock> {
    if dropped == 0 {
        return None;
    }
    let parts = vec![
        crate::engine::session::UserContentPart::Image {
            mime_type: "image/png".into(),
            base64: RuntimeString::default(),
            source_path: None,
        };
        dropped.min(2)
    ];
    let omitted = crate::engine::session::omit_images_for_blind_model(&parts)?;
    omitted.into_iter().find_map(|part| match part {
        crate::engine::session::UserContentPart::Text { text } => Some(TextBlock {
            text,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        }),
        _ => None,
    })
}

/// Compatibility request-only view of [`prepare_model_request`]. The live query
/// loop retains the complete prepared request/profile pair; only tests use
/// this view.
#[cfg(test)]
pub fn build_model_request(
    messages: &[Message],
    params: &QueryParams,
    canonical_system_prompt: &[SystemPromptPart],
    max_output_tokens: u64,
    internal_visible_tool_specs: &[ToolSpec],
    turn_override: Option<&TurnOverride>,
) -> ModelRequest {
    prepare_model_request(
        messages,
        params,
        canonical_system_prompt,
        max_output_tokens,
        internal_visible_tool_specs,
        turn_override,
    )
    .request
}

/// Whether the session runs the static fallback tool protocol: `Deferred`
/// schemas never enter the wire tool list and never shift the leading `tools`
/// prefix when discovery finds tools or new MCP servers finish connecting.
/// Discovery goes through `tool_search` (catalog notifications ride the
/// message stream), execution through the fixed `call_mcp_tool` entry.
/// This is the floor for EVERY wire: a provider arms its native deferred
/// channel per model, and anything without one — plain function-calling
/// endpoints, compatibility profiles, managed cloud routes, or a native
/// channel the endpoint refused — lands here instead of inflating the
/// standing declaration list with full definitions.
pub(crate) fn static_deferred_dispatch_active(
    config: &crate::schemas::config::ResolvedConfig,
    tools: &[Arc<dyn Tool>],
    tool_search_allowed: bool,
    native_active: bool,
) -> bool {
    !native_active
        && tool_search_allowed
        && !config
            .disabled_tools
            .contains(&RuntimeString::from("tool_search"))
        && !config.disabled_tools.contains(&RuntimeString::from(
            crate::tools::builtins::tool_search::CALL_MCP_TOOL_NAME,
        ))
        && tools.iter().any(|tool| tool.name() == "tool_search")
}

/// Static name-sorted direct tool declarations plus `call_mcp_tool` so
/// `Deferred` schemas never enter `functionDeclarations` and never shift the
/// leading `tools` prefix when `tool_search` discovers tools or new MCP servers
/// finish connecting.
pub(crate) fn static_deferred_visible_tool_specs(
    tools: &[Arc<dyn Tool>],
    disabled_tools: &[RuntimeString],
) -> Vec<ToolSpec> {
    let mut visible: Vec<ToolSpec> = tools
        .iter()
        .filter(|tool| tool.exposure().is_model_visible(false))
        .map(|tool| to_tool_spec(tool.as_ref()))
        .collect();
    let dispatcher_name =
        RuntimeString::from(crate::tools::builtins::tool_search::CALL_MCP_TOOL_NAME);
    if !disabled_tools.contains(&dispatcher_name)
        && !visible.iter().any(|spec| spec.name == dispatcher_name)
    {
        let dispatcher = crate::tools::builtins::tool_search::create_call_mcp_tool();
        let spec = to_tool_spec(dispatcher.as_ref());
        let insert_at = visible
            .iter()
            .position(|existing| existing.name > spec.name)
            .unwrap_or(visible.len());
        visible.insert(insert_at, spec);
    }
    visible
}

/// Tool specs visible to the model this turn: direct tools always, deferred
/// tools only once surfaced via `tool_search`, hidden tools never. Falls back
/// to the precomputed specs when no tool carries an exposure (the common
/// case). A surfaced deferred tool is APPENDED after every always-visible
/// tool rather than inserted at its sorted position, so the cached prefix of
/// the leading list never shifts when discovery unlocks another tool.
pub(crate) fn visible_tool_specs(
    tools: &[Arc<dyn Tool>],
    surfaced: &HashSet<RuntimeString>,
    precomputed: &[ToolSpec],
) -> Vec<ToolSpec> {
    let has_exposure = tools.iter().any(|t| t.exposure() != ToolExposure::Direct);
    if !has_exposure {
        return precomputed.to_vec();
    }
    let is_surfaced = |tool: &Arc<dyn Tool>| {
        tool.exposure() == ToolExposure::Deferred && surfaced.contains(&tool.name())
    };
    let mut visible: Vec<ToolSpec> = tools
        .iter()
        .filter(|t| !is_surfaced(t) && t.exposure().is_model_visible(false))
        .map(|t| to_tool_spec(t.as_ref()))
        .collect();
    visible.extend(
        tools
            .iter()
            .filter(|t| is_surfaced(t))
            .map(|t| to_tool_spec(t.as_ref())),
    );
    visible
}

/// Deferred-exposure tool specs that have not been surfaced via `tool_search`
/// yet: the wire list for providers with native deferred tool loading. Walks
/// the name-sorted pool like [`visible_tool_specs`], never the surfaced set, so
/// requests that deferred the same tools emit them in the same order.
pub(crate) fn deferred_tool_specs(
    tools: &[Arc<dyn Tool>],
    surfaced: &HashSet<RuntimeString>,
) -> Vec<ToolSpec> {
    tools
        .iter()
        .filter(|tool| {
            tool.exposure() == ToolExposure::Deferred && !surfaced.contains(&tool.name())
        })
        .map(|tool| to_tool_spec(tool.as_ref()))
        .collect()
}

pub(crate) fn catalog_entry(
    tool: &dyn Tool,
    profile: &ProviderBehaviorProfile,
) -> ToolCatalogEntry {
    let internal = to_tool_spec(tool);
    let primary = render_tool_specs_for_behavior(&[internal], profile)
        .into_iter()
        .next();
    ToolCatalogEntry {
        name: tool.name(),
        description: primary
            .as_ref()
            .map(|spec| spec.description.clone())
            .unwrap_or_else(|| tool.description()),
        search_hint: tool.search_hint(),
        exposure: tool.exposure(),
        spec: primary,
    }
}

pub(super) fn build_tool_catalog(
    tools: &[Arc<dyn Tool>],
    profile: &ProviderBehaviorProfile,
) -> Vec<ToolCatalogEntry> {
    tools
        .iter()
        .map(|tool| catalog_entry(tool.as_ref(), profile))
        .collect()
}

pub(super) async fn resolve_system_prompt(s: &SystemPromptResolvable) -> Vec<SystemPromptPart> {
    // Copy so per-turn mutation (hook context) never leaks across turns.
    match s {
        SystemPromptResolvable::Parts(parts) => parts.clone(),
        SystemPromptResolvable::Fn(thunk) => thunk().await,
    }
}

/// Stable uuid of a persisted message, used as the announcement anchor of a
/// recorded tool-inventory change.
fn message_uuid(message: &Message) -> Option<RuntimeString> {
    match message {
        Message::User(m) => Some(m.uuid.clone()),
        Message::Assistant(m) => Some(m.uuid.clone()),
        Message::ToolResult(m) => Some(m.uuid.clone()),
        Message::System(m) => Some(m.uuid.clone()),
        Message::CompactBoundary(m) => Some(m.uuid.clone()),
    }
}

fn resolve_wire_tool_name(
    wire_name: &RuntimeString,
    namespace: Option<&str>,
    tools: Option<&[Arc<dyn Tool>]>,
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    let candidate = match wire_name.try_to_utf8() {
        Ok(raw) => crate::types::provider::internal_tool_name(namespace, &raw),
        Err(_) => wire_name.clone(),
    };
    if let Some(tools) = tools {
        if let Some(found) = tools.iter().find(|tool| {
            let spec_ns = to_tool_spec(tool.as_ref()).namespace;
            let ns_ok = match namespace {
                Some(ns) if ns == crate::types::provider::DEFAULT_TOOL_NAMESPACE => {
                    spec_ns.is_none()
                }
                Some(ns) => spec_ns.as_ref().is_some_and(|s| s == ns),
                None => true,
            };
            ns_ok
                && (tool.name() == *wire_name
                    || tool.name() == candidate
                    || render_tool_name(&tool.name(), profile) == *wire_name)
        }) {
            return found.name();
        }
    }
    if namespace.is_none() || namespace == Some(crate::types::provider::DEFAULT_TOOL_NAMESPACE) {
        resolve_internal_tool_name(wire_name, profile)
    } else {
        candidate
    }
}

/// Extract internal tool names materialized in a `ServerToolSearch` or
/// `tool_search` output payload (`tools` array with flat or namespaced items).
pub(crate) fn surfaced_tools_from_server_output(
    payload: &RuntimeJson,
    tools: Option<&[Arc<dyn Tool>]>,
    profile: &ProviderBehaviorProfile,
) -> Vec<RuntimeString> {
    let mut out: Vec<RuntimeString> = Vec::new();
    let Some(items) = payload.get("tools").and_then(RuntimeJson::as_array) else {
        return out;
    };
    let mut push_unique = |name: RuntimeString| {
        if !name.is_empty() && !out.contains(&name) {
            out.push(name);
        }
    };
    for item in items {
        if item
            .get("type")
            .and_then(RuntimeJson::as_str)
            .is_some_and(|kind| kind == "namespace")
        {
            let ns = item
                .get("name")
                .and_then(RuntimeJson::as_str)
                .and_then(|s| s.try_to_utf8().ok());
            if let Some(children) = item.get("tools").and_then(RuntimeJson::as_array) {
                for child in children {
                    if let Some(name) = child.get("name").and_then(RuntimeJson::as_string) {
                        push_unique(resolve_wire_tool_name(name, ns.as_deref(), tools, profile));
                    }
                }
            }
        } else if let Some(name) = item.get("name").and_then(RuntimeJson::as_string) {
            push_unique(resolve_wire_tool_name(name, None, tools, profile));
        }
    }
    out
}

fn collect_bullet_tool_lines(
    text: &str,
    marker: &str,
    profile: &ProviderBehaviorProfile,
    out: &mut HashSet<RuntimeString>,
) {
    let Some(pos) = text.find(marker) else {
        return;
    };
    let rest = &text[pos + marker.len()..];
    let Some(after_line) = rest.find('\n').map(|idx| &rest[idx + 1..]) else {
        return;
    };
    for line in after_line.lines() {
        let trimmed = line.trim();
        let Some(entry) = trimmed.strip_prefix("- ") else {
            if trimmed.is_empty() {
                continue;
            }
            break;
        };
        let raw_name = entry.split(':').next().unwrap_or("").trim();
        if !raw_name.is_empty() {
            out.insert(resolve_internal_tool_name(
                &RuntimeString::from(raw_name),
                profile,
            ));
        }
    }
}

/// Extract internal tool names surfaced by successful `tool_search` and
/// `ServerToolSearch` `ToolResult` messages in `messages`.
pub(crate) fn surfaced_tools_from_message_results(
    messages: &[Message],
    profile: &ProviderBehaviorProfile,
    tools: Option<&[Arc<dyn Tool>]>,
) -> HashSet<RuntimeString> {
    let mut legacy_search_ids: HashSet<RuntimeString> = HashSet::new();
    for message in messages {
        if let Message::Assistant(assistant) = message {
            for block in &assistant.content {
                if let ContentBlock::ToolUse(use_block) = block {
                    if use_block.tool_kind == Some(ProviderToolCallKind::ToolSearch)
                        || use_block.name == "tool_search"
                    {
                        legacy_search_ids.insert(use_block.id.clone());
                    }
                }
            }
        }
    }
    let mut found: HashSet<RuntimeString> = HashSet::new();
    for message in messages {
        let Message::ToolResult(tr) = message else {
            continue;
        };
        if tr.is_error {
            continue;
        }
        if tr.tool_kind == Some(ProviderToolCallKind::ServerToolSearch) {
            if let Some(payload) = &tr.native_tool_output {
                found.extend(surfaced_tools_from_server_output(payload, tools, profile));
            }
            continue;
        }
        if tr.tool_kind != Some(ProviderToolCallKind::ToolSearch)
            && !legacy_search_ids.contains(&tr.tool_use_id)
        {
            continue;
        }
        let mut from_json = false;
        if let Some(payload) = tr.native_tool_output.as_ref().or(tr.output.as_ref()) {
            if let Some(matched) = payload.get("matched").and_then(RuntimeJson::as_array) {
                for entry in matched {
                    if let Some(name) = entry.as_string() {
                        found.insert(resolve_internal_tool_name(name, profile));
                        from_json = true;
                    }
                }
            }
            if !from_json {
                let extracted = surfaced_tools_from_server_output(payload, tools, profile);
                if !extracted.is_empty() {
                    from_json = true;
                    found.extend(extracted);
                }
            }
        }
        if !from_json {
            for block in &tr.content {
                if let OutputBlock::Text(text_block) = block {
                    if let Ok(utf8) = text_block.text.try_to_utf8() {
                        collect_bullet_tool_lines(
                            &utf8,
                            "These tools are now available:",
                            profile,
                            &mut found,
                        );
                    }
                }
            }
        }
    }
    found
}

/// Extract all internal tool names surfaced in `messages`, including
/// post-compaction retained surfaced-tools sections.
pub(crate) fn surfaced_tools_from_messages(
    messages: &[Message],
    profile: &ProviderBehaviorProfile,
    tools: Option<&[Arc<dyn Tool>]>,
) -> HashSet<RuntimeString> {
    let mut found = surfaced_tools_from_message_results(messages, profile, tools);
    for message in messages {
        let Message::User(user) = message else {
            continue;
        };
        for block in &user.content {
            if let ContentBlock::Text(text_block) = block {
                if let Ok(utf8) = text_block.text.try_to_utf8() {
                    collect_bullet_tool_lines(
                        &utf8,
                        "These tools you previously found via tool_search remain available",
                        profile,
                        &mut found,
                    );
                }
            }
        }
    }
    found
}

/// The visible inventory implied by a frozen baseline plus its recorded events.
pub(crate) fn fold_tool_inventory(
    baseline: &[ToolSpec],
    events: &[crate::types::provider::ToolInventoryEvent],
) -> Vec<ToolSpec> {
    let mut state: Vec<ToolSpec> = baseline.to_vec();
    for event in events {
        for name in &event.removed {
            state.retain(|spec| &spec.name != name);
        }
        for spec in &event.added {
            match state.iter_mut().find(|slot| slot.name == spec.name) {
                Some(slot) => *slot = spec.clone(),
                None => state.push(spec.clone()),
            }
        }
    }
    state
}

/// Added/redefined and removed declarations between the implied inventory and
/// the request's actual visible list. `None` when they already agree.
fn tool_inventory_diff(
    expected: &[ToolSpec],
    actual: &[ToolSpec],
) -> Option<(Vec<ToolSpec>, Vec<RuntimeString>)> {
    let mut added = Vec::new();
    let mut removed = Vec::new();
    for spec in actual {
        match expected
            .iter()
            .find(|candidate| candidate.name == spec.name)
        {
            Some(previous) if previous == spec => {}
            _ => added.push(spec.clone()),
        }
    }
    for spec in expected {
        if !actual.iter().any(|candidate| candidate.name == spec.name) {
            removed.push(spec.name.clone());
        }
    }
    if added.is_empty() && removed.is_empty() {
        None
    } else {
        Some((added, removed))
    }
}

#[cfg(test)]
mod context_policy_tests {
    use super::*;

    #[test]
    fn thinking_policy_pins_every_turn_while_the_cache_can_be_warm() {
        assert_eq!(
            resolve_thinking_context(true, Some(ThinkingConfig::Adaptive), false),
            Some(ThinkingContextPolicy::KeepAll)
        );
    }

    #[test]
    fn thinking_policy_keeps_one_turn_once_the_cache_is_cold() {
        assert_eq!(
            resolve_thinking_context(true, Some(ThinkingConfig::Adaptive), true),
            Some(ThinkingContextPolicy::KeepMostRecentTurn)
        );
    }

    #[test]
    fn thinking_policy_needs_both_the_switch_and_wire_thinking() {
        assert_eq!(
            resolve_thinking_context(false, Some(ThinkingConfig::Adaptive), true),
            None
        );
        assert_eq!(resolve_thinking_context(true, None, true), None);
    }

    #[test]
    fn count_trigger_rises_by_what_the_server_already_cleared() {
        use crate::context::context_editing::ContextEditState;
        use crate::schemas::config::ContextManagementConfig;
        let config = ContextManagementConfig {
            trigger_tool_uses: Some(10),
            ..Default::default()
        };
        // No state or nothing cleared yet: the configured base is sent as-is.
        assert_eq!(
            adaptive_context_management(Some(&config), None),
            Some(config.clone())
        );
        let state = ContextEditState::default();
        assert_eq!(
            adaptive_context_management(Some(&config), Some(&state)),
            Some(config.clone())
        );
        // After a server report, the next trigger waits for a fresh batch.
        state.record(8, 40_000, 0);
        assert_eq!(
            adaptive_context_management(Some(&config), Some(&state)),
            Some(ContextManagementConfig {
                trigger_tool_uses: Some(18),
                ..Default::default()
            })
        );
    }

    #[test]
    fn token_trigger_is_never_adapted() {
        use crate::context::context_editing::ContextEditState;
        use crate::schemas::config::ContextManagementConfig;
        let config = ContextManagementConfig {
            trigger_input_tokens: Some(120_000),
            ..Default::default()
        };
        let state = ContextEditState::default();
        state.record(8, 40_000, 0);
        assert_eq!(
            adaptive_context_management(Some(&config), Some(&state)),
            Some(config)
        );
    }

    #[test]
    fn inventory_fold_and_diff_drive_anchored_events() {
        use crate::types::provider::{ToolInventoryEvent, ToolSpec, ToolSpecKind};
        fn spec(name: &str, description: &str) -> ToolSpec {
            ToolSpec {
                name: name.into(),
                description: description.into(),
                input_schema: crate::runtime_values::runtime_json::parse(&RuntimeString::from(
                    "{}",
                ))
                .unwrap(),
                kind: ToolSpecKind::Function,
                format: None,
                namespace: None,
            }
        }
        let baseline = vec![spec("read", "v1"), spec("bash", "run")];
        // A redefinition replaces in place; a removal drops the entry.
        let events = vec![ToolInventoryEvent {
            anchor_uuid: Some("u1".into()),
            added: vec![spec("read", "v2")],
            removed: vec!["bash".into()],
        }];
        let folded = super::fold_tool_inventory(&baseline, &events);
        assert_eq!(folded.len(), 1);
        assert_eq!(folded[0].name.try_to_utf8().unwrap(), "read");
        assert_eq!(folded[0].description.try_to_utf8().unwrap(), "v2");

        // The folded state agrees with the live inventory: no new event.
        assert!(super::tool_inventory_diff(&folded, &folded).is_none());
        // A tool that appeared live diffs as an addition.
        let live = vec![folded[0].clone(), spec("calculator", "add")];
        let (added, removed) = super::tool_inventory_diff(&folded, &live).expect("delta");
        assert_eq!(added.len(), 1);
        assert!(removed.is_empty());
        // A tool that vanished live diffs as a removal.
        let (added, removed) = super::tool_inventory_diff(&live, &folded).expect("delta");
        assert!(added.is_empty());
        assert_eq!(removed.len(), 1);
    }
}
