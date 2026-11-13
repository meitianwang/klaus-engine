//! Model-request assembly, retry/backoff helpers, and the provider-facing
//! continuation / recovery envelope rendering for one query turn.

use super::QueryParams;
use super::Shared;
use super::SystemPromptResolvable;
use super::TurnOverride;
use crate::provider::behavior::render_provider_text_envelope;
use crate::provider::behavior::render_system_prompt_for_behavior;
use crate::provider::behavior::render_tool_specs_for_behavior;
use crate::provider::behavior::resolve_provider_behavior_profile;
use crate::provider::behavior::ProviderBehaviorProfile;
use crate::provider::behavior::ProviderTextEnvelopeKind;
use crate::provider::observability::ProviderRequestMetadata;
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

/// The continuation nudge for a response cut off WITH visible text on screen.
///
/// The sentence is addressed to the model: it has to read "break your work into
/// smaller pieces" as an instruction to change shape, not just resume.
pub(super) fn render_truncation_continuation(profile: &ProviderBehaviorProfile) -> RuntimeString {
    render_query_envelope_producer(
        QueryEnvelopeProducer::TruncationContinuation,
        "Your response was cut off because it exceeded the output token limit. Please break your work into smaller pieces. Continue from where you left off.".into(),
        profile,
    )
}

/// The reminder for a response cut off having produced NO visible text: the
/// whole budget went to reasoning the user never sees.
///
/// Injected as a reminder rather than as a bare instruction so the model treats
/// it as harness guidance about its own output shape.
pub(super) fn render_reasoning_over_limit_reminder(
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    render_query_envelope_producer(
        QueryEnvelopeProducer::ReasoningOverLimit,
        "Please keep your thinking concise and avoid overly long reasoning chains. Focus on the key steps and respond directly.".into(),
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
        None,
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
    let request = ModelRequest {
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
        cache_key: params.cache_key.clone(),
        cache_domain: params.cache_domain.clone(),
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
        let Some(bundle) = self.bundle() else {
            let mut prepared = prepare_model_request(
                messages,
                &self.params,
                canonical_system_prompt,
                max_output_tokens,
                internal_visible_tool_specs,
                turn_override,
            );
            if let Some(specs) = self.defer_oversized_inventory(&prepared) {
                return self.prepare(
                    messages,
                    canonical_system_prompt,
                    max_output_tokens,
                    &specs,
                    turn_override,
                );
            }
            prepared.request.deferred_tools = self.deferred_request_specs(&prepared.profile);
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
        let mut prepared = prepare_with_inference(
            messages,
            &self.params,
            RequestInference {
                config: &config,
                provider_info: Some(bundle.provider.info()),
                thinking,
            },
            canonical_system_prompt,
            max_output_tokens,
            internal_visible_tool_specs,
            Some(&turn_override),
        );
        let capabilities = bundle.provider.capabilities(&prepared.request.model);
        let metadata = bundle.provider.model_metadata(&prepared.request.model);
        prepared.request.system = super::orchestration::request_policy_with_model_messages(
            &prepared.request.system,
            turn_override.ultra_mode,
            internal_visible_tool_specs,
            self.params.agent_id.as_ref(),
            self.params.multi_agent_instructions,
            metadata.model_messages.as_ref(),
            config.multi_agent_mode_hint_text.as_ref(),
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
            &bundle
                .provider
                .model_metadata(&prepared.request.model)
                .media_input_mime_types,
        );
        super::tool_dispatch::gate_request_images(&mut prepared.request);
        if let Some(specs) = self.defer_oversized_inventory(&prepared) {
            return self.prepare(
                messages,
                canonical_system_prompt,
                max_output_tokens,
                &specs,
                Some(&turn_override),
            );
        }
        if capabilities.tools {
            prepared.request.deferred_tools = self.deferred_request_specs(&prepared.profile);
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

    /// Deferred-exposure specs that have not been surfaced yet, rendered for
    /// the request's provider profile. Providers with native deferred-loading
    /// wire support send these alongside the immediate tools; every other
    /// provider ignores the field.
    pub(super) fn deferred_request_specs(
        &self,
        profile: &ProviderBehaviorProfile,
    ) -> Vec<ToolSpec> {
        let tools = self.tools.lock_recover().clone();
        let surfaced = self.surfaced.lock_recover();
        let internal = deferred_tool_specs(&tools, &surfaced);
        drop(surfaced);
        if internal.is_empty() {
            return Vec::new();
        }
        render_tool_specs_for_behavior(&internal, profile)
    }

    /// Budget the rendered prefix, rather than a tool count or a model-name
    /// heuristic. Activated deferred tools retain their declarations; everything
    /// else remains available to the existing tool-search activation path.
    fn defer_oversized_inventory(&self, prepared: &PreparedModelRequest) -> Option<Vec<ToolSpec>> {
        let request = &prepared.request;
        let bundle = self.bundle()?;
        if !bundle.provider.has_model_capabilities(&request.model)
            || !bundle.provider.uses_model_tool_budget(&request.model)
        {
            return None;
        }
        if request.tools.is_empty() || fixed_prefix_tokens(request) <= self.input_window(request) {
            return None;
        }
        if self
            .config()
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
        let all_specs = deferred
            .iter()
            .map(|tool| to_tool_spec(tool.as_ref()))
            .collect::<Vec<_>>();
        let surfaced = self.surfaced.lock_recover();
        let specs = visible_tool_specs(&deferred, &surfaced, &all_specs);
        drop(surfaced);
        if render_tool_specs_for_behavior(&specs, &prepared.profile) == request.tools {
            return None;
        }
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

/// Tool specs visible to the model this turn: direct tools always, deferred tools
/// only once surfaced via `tool_search`, hidden tools never. Falls back to the
/// precomputed specs when no tool carries an exposure (the common case).
///
/// A surfaced deferred tool is APPENDED after every always-visible tool rather
/// than placed at its sorted position. The tools array is the leading region of
/// the cached prefix, so inserting mid-array shifts every tool behind it and
/// costs the whole prefix — system and history included — on a session that has
/// merely found another tool to call. Appending only lengthens the prefix, so
/// the part already cached stays a hit.
///
/// Both passes walk the pool, which is sorted by name, never the surfaced set:
/// a `HashSet` has no stable iteration order, and reading it would let two
/// requests that surfaced the same tools emit them in a different order. That
/// is the same prefix invalidation as a mid-array insert, with no visible cause.
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
}
